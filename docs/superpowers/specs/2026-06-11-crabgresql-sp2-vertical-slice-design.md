# crabgresql SP2: Thin Vertical Slice (parser → catalog → KV engine → executor)

**Date:** 2026-06-11
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessor:** SP1 (wire protocol + conformance oracle) — merged.

## Goal

Replace SP1's canned `StubEngine` with a real SQL engine that takes
`CREATE TABLE` / `DROP TABLE` / `INSERT` / `SELECT` end-to-end through a genuine
parser → catalog → executor → KV pipeline, served over the existing PostgreSQL
wire protocol. Every statement routes through real code; nothing is canned. The
storage layer is KV-encoded from day one so it survives into SP3 (durable LSM)
and SP4 (range sharding). Three carried-over SP1 obligations are folded in:
SCRAM verifier storage, the libpg_query parser oracle, and the start of the
pg_regress conformance import.

Constraints unchanged from the program spec: `#![forbid(unsafe_code)]` in every
crate; pure-Rust shipped dependency tree (C dev/test dependencies — libpg_query,
the PostgreSQL test oracle — are exempt); parity baseline PostgreSQL 18.

## SQL surface (the "core slice")

In scope:
- `CREATE TABLE name (col type, ...)` — column types `int4`/`int8`/`text`/`bool`
  (and common aliases: `integer`, `bigint`, `boolean`). No constraints, no
  `PRIMARY KEY`, no `NOT NULL`, no defaults.
- `DROP TABLE name`.
- `INSERT INTO name [(cols)] VALUES (...), (...)` — literal expressions,
  multi-row.
- `SELECT proj[, ...] [FROM table] [WHERE expr] [ORDER BY expr [ASC|DESC] ...]
  [LIMIT n]` — projection with expressions and aliases (`expr AS name`),
  `SELECT *`, expressions over `+ - * /`, comparisons (`= <> < <= > >=`),
  `AND`/`OR`/`NOT`, parenthesization. `SELECT` with no `FROM` evaluates against a
  single synthetic empty row (`SELECT 1+1`).

Explicitly OUT (tracked gaps): `pg_catalog`/`information_schema` views (psql
`\d` will not work; plain SQL will); indexes, constraints, PK/FK, NOT NULL;
`UPDATE`/`DELETE`; transactions / MVCC (each statement autocommits into the
map); joins, aggregates, subqueries, `GROUP BY`, `DISTINCT`; numeric/float/
date/timestamp types; **parameterized-query parameter binding** — the extended
protocol still works, but `$1` placeholders evaluate to `0A000` (the slice is
literals-only). COPY, replication, GSSAPI, protocol 3.2 remain out per SP1.

## Architecture

Five new workspace crates, all `forbid(unsafe_code)`, joining `pgwire`,
`crabgresql`, `conformance`. Dependency direction is strictly downward; no lower
crate depends on `pgwire`.

| Crate | Responsibility | Depends on |
|---|---|---|
| `pgtypes` | `Datum` value type; text+binary wire encodings; type OIDs; literal typing; arithmetic/comparison operators with PG error semantics | — |
| `pgparser` | Hand-written lexer + recursive-descent/Pratt parser → crabgresql AST; multi-statement splitting | pgtypes (for type names/literals) |
| `catalog` | Table metadata (TableId, columns); name lookup; CREATE/DROP semantics; in-memory behind RwLock | pgtypes |
| `kv` | `Kv` trait; `MemKv` (RwLock<BTreeMap>); order-preserving key encoding + versioned value encoding | — |
| `executor` | AST execution; expression evaluator; implements `pgwire::Engine` | pgtypes, pgparser, catalog, kv, pgwire |

`pgwire`'s tiny `oids` module migrates into `pgtypes` (its real home).
`StubEngine` stays in `pgwire` for pgwire's own protocol tests; the binary
constructs `executor::SqlEngine`.

### pgtypes

`Datum` enum: `Null`, `Bool(bool)`, `Int4(i32)`, `Int8(i64)`, `Text(String)`.
Each non-null type provides text and binary wire encodings (populating SP1's
`Cell { text, binary }`), an OID, and a `FieldDescription` type-size/modifier.
Literal typing: integer literal fitting i32 → int4, else i64 → int8; overflow of
i64 → `22003`. String literal → text; `true`/`false` → bool. Operators
(`+ - * /`, comparisons, `AND`/`OR`/`NOT`) implement PG semantics including
NULL propagation (`NULL + 1 = NULL`; three-valued logic for boolean ops),
integer overflow (`22003`), and division by zero (`22012`). Operator type rules
follow PG: int4⊕int4→int4, mixed int4/int8→int8.

### pgparser

Hand-written lexer (keywords, quoted/unquoted identifiers, string and numeric
literals, operators, `--` line and `/* */` block comments) feeding a
recursive-descent parser with Pratt expression parsing. Produces a crabgresql
AST (`Vec<Statement>` per source string — multi-statement splitting is the
parser's job, replacing the stub's normalization hack). Parses exactly the core
slice grammar. Syntax errors → `42601` with a byte position. The crate depends
only on pgtypes (for type-name and literal recognition) — it parses to AST and
nothing more.

### catalog

`TableId` (u32, OID-style, allocated from a counter). Per table: name, and an
ordered column list (name, type OID, ordinal). Operations: `create_table`
(→`42P07` on duplicate), `drop_table` (→`42P01` if absent), `lookup` by name
(→`42P01`), column resolution (→`42703`). In-memory behind `RwLock`;
persistence is SP3.

### kv — the permanent seam

`Kv` trait: `get`, `put`, `delete`, `scan_prefix` (synchronous for SP2;
async/transactional comes with the distributed layer). `MemKv`:
`RwLock<BTreeMap<Vec<u8>, Vec<u8>>>`.

**Key encoding** (order-preserving):
```
key = table_prefix(table_id) || index_id(=1) || encode_u64_be(rowid)
```
Table/index ids and rowid use big-endian fixed-width integer encoding — already
order-preserving for unsigned values, which is all the slice needs (rowid is a
hidden monotonic u64, since the core slice has no PRIMARY KEY). The
CockroachDB-style sortable encoding for *arbitrary* PK column types is deferred
to when real `PRIMARY KEY` arrives; the key layout reserves the slot, so adding
it is additive.

**Value encoding** (compact, NOT order-preserving — values are never sorted by
raw bytes):
```
value = row_version(u8 = 1) || for each column: encode_datum(datum)
encode_datum = type_tag(u8, covers NULL) || payload
  bool   → 1 byte
  int4   → 4 bytes BE
  int8   → 8 bytes BE
  text   → u32 length prefix || UTF-8 bytes
```
The `row_version` byte lets SP3 evolve the value format without a migration
scramble.

### executor

Implements `pgwire::Engine`. `simple_query(sql)`:
1. `pgparser::parse(sql)` → statements (or `42601`).
2. Dispatch each statement; produce one `QueryResult` per statement (SP1's
   multi-statement contract).
3. Lower-crate error enums are mapped to `PgError` at this boundary.

`describe(sql)` parses and type-checks the single statement against the catalog,
returning `FieldDescription`s without executing — real lazy describe (fixes
SP1's eager-describe note).

Per-statement execution:
- **CREATE TABLE** → catalog insert; `QueryResult::Command { tag: "CREATE TABLE" }`.
- **DROP TABLE** → catalog remove; tag `"DROP TABLE"`.
- **INSERT** → resolve table; per VALUES row evaluate expressions to Datums,
  coerce/type-check against column types (`22P02`/`22003`/`42804`), assign next
  rowid, encode, `put`. Tag `INSERT 0 <n>`.
- **SELECT** → resolve FROM (or synthetic single empty row); `scan_prefix` the
  table; decode rows to Datums; evaluate WHERE (must be boolean; NULL → row
  excluded); evaluate projection building output Datums + inferred
  FieldDescriptions; apply ORDER BY (multi-key, ASC/DESC, NULLS LAST for ASC /
  NULLS FIRST for DESC per PG default); apply LIMIT; convert each output Datum
  to `Cell { text, binary }`. Tag `SELECT <n>`.

The expression evaluator lives in executor, operating on pgtypes Datums and
operators.

## Error handling

Every failure surfaces as a `PgError` carrying the SQLSTATE real PostgreSQL
uses (the conformance harness diffs codes). Slice taxonomy: `42601` syntax
(with position), `42P01` undefined table, `42P07` duplicate table, `42703`
undefined column, `42804`/`42P10` type mismatch, `22P02` invalid text input,
`22003` numeric overflow, `22012` division by zero, `0A000` for
in-grammar-but-unimplemented features (keeps the boundary honest — e.g. `$1`
parameters). pgtypes/catalog/kv expose their own rich error enums; executor maps
them to `PgError`, so lower crates never depend on pgwire. Per-statement errors
are non-fatal (the session survives) via SP1's existing machinery.

## Testing

Per-crate isolation:
- **pgtypes** — encoding roundtrips (every Datum incl. NULL, `i32::MIN`,
  `i64::MIN`); operator-semantics tests vs known PG results.
- **pgparser** — AST-shape unit tests + the **libpg_query differential oracle**:
  parse the same SQL through pgparser and libpg_query (dev-only C dependency,
  exempt from zero-C), compare structure. This is pgparser's dedicated parity
  harness (program-spec commitment).
- **kv** — the order-preserving property tests: key encode→decode roundtrip and
  **`a < b` (logical) ⟺ `encode(a) < encode(b)` (bytewise)** for the key integer
  encoders; value roundtrips for all Datums. Heaviest test weight — this module
  must be right forever.
- **catalog** — CRUD + each error code.
- **executor** — per-operator unit tests.

End-to-end: a `tests/` suite drives real `tokio-postgres` through
`CREATE TABLE → INSERT → SELECT … WHERE … ORDER BY … LIMIT`, asserting rows and
types.

Conformance: fix the statement splitter to handle dollar-quoting, then vendor
the first `pg_regress` files (`int4.sql`, `boolean.sql`, and a `select.sql`
subset) into the corpus so the parity number reflects real PostgreSQL tests.
The dashboard will show parity climb from SP1's ~20% smoke baseline; no specific
number is committed.

CI gates unchanged and must stay green: `forbid(unsafe_code)`, `cargo-deny`
(pure-Rust shipped tree), `check-no-native.sh`, fmt, clippy `-D warnings`,
the conformance job.

## Carry-over tasks (sequenced in the plan)

1. **SCRAM verifiers** (first; independent of the engine). Replace
   `AuthMode::ScramSha256 { users: HashMap<String,String> }` (plaintext) with
   stored verifiers (`StoredKey`/`ServerKey`/salt/iterations); add **mock
   authentication** for unknown users (eliminates the username-enumeration
   oracle); binary takes verifier config. Closes the documented SP1 security
   trio (plaintext at rest, per-connection PBKDF2 DoS lever, enumeration oracle).
2. **libpg_query oracle** — lands with pgparser (its test harness).
3. **pg_regress import** — lands with the conformance work, after the executor
   exists to run the corpus against.

## Success criteria

1. `psql` (and tokio-postgres) run `CREATE TABLE t (id int4, name text);
   INSERT INTO t VALUES (1,'a'),(2,'b'); SELECT name FROM t WHERE id > 1 ORDER BY
   id DESC LIMIT 5;` and get correct rows with correct types — through the real
   pipeline, no canned answers.
2. The KV key encoding's order-preservation property holds under property tests.
3. pgparser matches libpg_query on the slice grammar (differential oracle green).
4. SCRAM authenticates against stored verifiers; unknown users get mock auth (no
   enumeration oracle).
5. The conformance corpus includes real pg_regress files; the parity dashboard
   shows a measured increase over SP1's baseline.
6. All SP1 CI gates remain green: zero unsafe, pure-Rust shipped tree, fmt,
   clippy, conformance job.
