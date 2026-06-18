# SP40 — Kafka FDW (query crabka topics as SQL tables)

**Date:** 2026-06-18
**Status:** Approved (design)

## Problem / motivation

crabgresql (a pure-Rust PostgreSQL-parity engine) and its sibling **crabka** (a
pure-Rust, byte-exact Apache Kafka reimplementation) share a language, an era, and
a philosophy — both are wire-compatible reimplementations validated by differential
testing against the reference, both lean on Stateright model-checking, both are
greenfield. They have never *depended on each other*. This slice creates the first
real product-level synergy: a built-in PostgreSQL **foreign-data wrapper** that
exposes crabka topics as SQL tables, so any `psql`/JDBC client can run
`SELECT … FROM a_kafka_topic` and get typed rows decoded from the topic's Schema
Registry schema — SQL over Kafka in one pure-Rust stack, no JVM, no ksqlDB.

This also exercises crabka's investment in *published, reusable crates*
(`crabka-client-*`, `crabka-schema-serde`): crabgresql consumes them as a
downstream, validating that they stand alone outside the broker.

**Scope: phase 1 is federated (live read-through) only.** A Kafka topic-table is a
**foreign table**; a `SELECT` opens an ephemeral consumer, reads the topic's
currently-committed records, decodes them, and returns rows. crabgresql stores
nothing. The hybrid end state (continuously-maintained **materialized views** over
topics) is *designed for* here but built in a later slice (SP41); phase 1
deliberately lands the shared decode + type-mapping + transport foundation that
phase 2 reuses wholesale.

## Scope decisions (and why)

### A feature-gated optional crate (the zero-C boundary)

crabgresql ships a hard **zero-C guarantee** (enforced by `scripts/check-no-native.sh`
and `cargo-deny` bans; paired with `rustls-rustcrypto`). crabka's client stack
authenticates via `crabka-security`, which pulls `ring`/`aws-lc-rs` — C code. Rather
than reimplement a pure-Rust Kafka client (rejected: large surface, duplicative) or
relax zero-C project-wide (rejected: abandons a core differentiator for users who
never touch Kafka), the integration lives in a **new crate `kafka_fdw` behind a
`kafka` cargo feature, off by default**:

- The **default build stays pure Rust.** `check-no-native.sh` + `cargo-deny` run on
  the default feature set and stay green. The pure-Rust guarantee survives for
  everyone not querying Kafka.
- A **separate CI lane** builds and tests `--features kafka` and skips the no-native
  check. Opting into Kafka explicitly opts into C — a documented, scoped exception.
- The `crabgresql` node binary gains a `kafka` feature that, when enabled, links the
  FDW; the default binary does not.

### Depend on crabka's client crates as-is

The FDW depends on the **published** crabka crates (crates.io `0.3.x`, with a
`[patch]`/path override for local cross-repo dev against the sibling checkout):
`crabka-client-core` (the bounded federated scan — connect, `ListOffsets`,
`fetch_partition_with_isolation`), `crabka-client-admin` (topic/partition metadata),
and `crabka-schema-serde` (Confluent wire framing + Schema Registry client + cache).
`crabka-client-consumer` is **not** used for the federated read — it is a
subscribe/group consumer with no manual `assign()`, whereas a bounded snapshot scan
needs per-partition seek+fetch, which `crabka-client-core` exposes directly.
`crabka-client-producer` is reserved for a future write path. No fork, no vendoring —
the synergy *is* the dependency.

### rustls CryptoProvider coexistence (flagged risk — early spike)

With `--features kafka`, two rustls crypto backends are in the tree: crabka's `ring`
(via `crabka-client-core`) for the Kafka connection and crabgresql's `rustcrypto`
for the pgwire frontend. rustls 0.23 resolves the provider for a builder-constructed
`ClientConfig`/`ServerConfig` from the **process-global default**, and
`crabka-client-core` constructs its `ClientConfig` that way (it exposes **no**
`builder_with_provider` hook). So the spec's original "explicit provider per
connection" cannot be done from crabgresql's side alone. The resolution, validated by
an **early spike (plan Task 2) before any other Kafka work**: crabgresql's binary
installs the **`rustcrypto` provider as the process default** at startup, and
crabka-client rides on that same default (its TLS code is provider-agnostic — it asks
for the default, it does not hard-pin ring's primitives). The spike stands up pgwire
TLS *and* a Kafka TLS connection in one process and asserts both handshake. **If** the
spike shows crabka-client implicitly depends on ring-specific behavior, the fallback
is a minimal upstream `crabka-client-core` change to accept an injected provider
(`ClientSecurity { crypto_provider: Option<Arc<CryptoProvider>> }`) — small, additive,
and a genuine shared-code win. Tested invariant: Component H, #7.

### Registry-typed decoding, with a raw fallback

A topic's value (and optionally key) is decoded via `crabka-schema-serde` assuming
the **Confluent wire format** (`0x00` magic byte + 4-byte big-endian schema id →
fetch schema from the registry → decode **Avro / JSON Schema / Protobuf**). Decoded
fields project by name onto the foreign table's declared columns. **Graceful
fallback:** a record with no magic byte (a non-registry topic) is not an error — its
value is surfaced as `bytea`, so a raw topic is always queryable. The registry
endpoint is a `CREATE SERVER` option; schemas are cached by id within a scan.

### PostgreSQL FDW DDL (parity-faithful), crabgresql's first foreign data

The declaration surface is the **standard Postgres FDW grammar**, not a bespoke
`CREATE KAFKA TABLE`. This matches crabgresql's parity north star, is understood by
real `psql`/JDBC tooling, and — crucially — pairs `IMPORT FOREIGN SCHEMA` with the
Schema Registry: importing a server generates one foreign table per registry subject
from its latest schema. Because Postgres *has* this grammar, the new parser
statements are differential-tested against `libpg_query` like everything else.

This is crabgresql's **first foreign-data infrastructure**: a builtin
foreign-data-wrapper (`kafka_fdw`), foreign servers, user mappings, and foreign
tables become catalog objects (mirroring `pg_foreign_data_wrapper`,
`pg_foreign_server`, `pg_user_mapping`, `pg_foreign_table`), and the planner/executor
learns to route a scan of a foreign table to its wrapper.

### Bounding the infinite stream (read semantics)

A topic is unbounded; a SQL table is finite. The federated scan resolves this with a
**snapshot rule**: an unbounded `SELECT` reads from each partition's earliest offset
up to the **high-water mark captured at query start**, then stops. The result is
well-defined — "the topic's currently-committed contents at the instant the query
began." Continuous *tailing* (a streaming SELECT that never ends) is **not** phase 1.
Isolation is **`read_committed` by default** (aborted/uncommitted transactional
records are skipped), matching SQL expectations.

**Predicate pushdown** keeps an unbounded scan from reading a huge topic:
`_partition = N` (restrict assignment), `_offset` range predicates (seek), and
`_timestamp` range predicates (via `offsetsForTimes`/`ListOffsets`), plus `LIMIT k`
(stop after `k` records). Predicates the wrapper cannot push are applied locally by
the existing executor above the foreign scan — joins, aggregates, and residual
filters compose over foreign-table rows exactly as over base-table rows.

### Why no Stateright model in phase 1

A federated read is a **pure, stateless fold** over a consumed record set under a
single high-water-mark snapshot — no lock, no write path, no replicated state, no
interleaving. Correctness is a decode/type-mapping/pushdown property proven by unit
tests + the round-trip differential oracle (Component H). It fits the same carve-out
as the SQL-breadth waves. The model-checking target is **phase 2's** durable
offset-checkpoint / resume / upsert state machine, called out in Non-goals.

### Auth model (phase 1)

A `CREATE USER MAPPING` holds a **static per-`(pguser, server)` service credential**
(SASL mechanism + username/password, or OAUTHBEARER token config). crabka enforces
its own ACLs (`crabka-authz`) against that identity; crabgresql merely presents it.
Mapping options are superuser-visible, matching PostgreSQL's `pg_user_mapping`
behavior; encrypted secret storage and Postgres-role→Kafka-principal pass-through are
noted future enhancements.

## Components

- **A. New crate + build/CI (`kafka_fdw`, workspace `Cargo.toml`, `deny.toml`,
  `ci.yml`).** New optional workspace member behind a `kafka` feature. Default build
  excludes it; `check-no-native.sh` + `cargo-deny` run on the default set. A new CI
  job builds/tests `--features kafka` (no-native check skipped, C permitted).
  `crabgresql` binary gains a `kafka` passthrough feature. Dependencies:
  `crabka-client-core`, `crabka-client-admin`, `crabka-schema-serde` (crates.io
  `0.3.x` + local `[patch]`), the decode libs `apache-avro` / `serde_json` /
  `prost-reflect`, and the crabgresql crates `pgtypes`, `executor`, `catalog`,
  `pgparser`, `kv`.
- **B. Parser — FDW DDL (`pgparser`).** New keywords/statements + AST for
  `CREATE/DROP FOREIGN DATA WRAPPER`, `CREATE/DROP/ALTER SERVER`,
  `CREATE/DROP/ALTER USER MAPPING`, `CREATE/DROP FOREIGN TABLE`, and
  `IMPORT FOREIGN SCHEMA … [LIMIT TO|EXCEPT (…)] FROM SERVER … INTO …`. Generic
  `OPTIONS (k 'v', …)` option lists. Differential-tested against `libpg_query`.
- **C. Catalog — foreign objects (`catalog`).** New KV-backed metadata for
  foreign-data-wrappers, servers, user mappings, and foreign tables (column defs +
  per-table options), with PG-faithful SQLSTATEs (`42704` undefined object, `42710`
  duplicate, dependency errors on `DROP`). A foreign table carries enough to plan a
  scan without a network round-trip: topic name, value/key formats, declared columns.
- **D. Decode + type mapping (`kafka_fdw::decode`, `kafka_fdw::types`).** crabka's
  `crabka-schema-serde` serdes are generic over a *compile-time* Rust type, so the FDW
  decodes **dynamically**: `crabka_schema_serde::wire::decode` strips the Confluent
  envelope (`0x00` + 4-byte schema id), `RegistryClient::schema_by_id` fetches the
  schema text (cached), then the value is decoded against it with the underlying libs —
  `apache_avro::from_avro_datum` → `avro::types::Value`, `serde_json` → `Value`, or
  `prost_reflect::DynamicMessage` — and mapped to `Datum`. Schema → `ColumnType`
  mapping: Avro (`int`→`int4`, `long`→`int8`, `string`→`text`, `boolean`→`bool`,
  `float`→`float4` (note: pgtypes has no `Float4` yet — see Global Constraints,
  decode→`float8`), `double`→`float8`, `bytes`→`bytea`, logical
  `timestamp-millis/micros`→`timestamptz`, `date`→`date`, `decimal`→`numeric`,
  union-with-`null`→nullable, `record`/`array`/`map`→`jsonb`); analogous JSON Schema
  and Protobuf mappings. Drives both `IMPORT FOREIGN SCHEMA` column generation and
  per-record value→`Datum` projection. Fields absent from a record → `NULL`; an
  irreconcilable type clash → a clear decode error. Avro + JSON land first; Protobuf
  is a follow-on task in this slice.
- **E. Transport / source (`kafka_fdw::source`).** Uses **`crabka-client-core`
  directly** (not the subscribe-only `crabka-client-consumer`): `crabka-client-admin`
  resolves the topic's partitions; a `ListOffsets` send captures each partition's
  earliest + high-water mark at scan start; `fetch_partition_with_isolation(...,
  ReadCommitted, ...)` reads each partition from the lower bound up to the captured
  HWM. Builds the Kafka client from the resolved server + user mapping (SASL/TLS),
  with the process-default `rustcrypto` provider established at startup (see risk
  above). Holds the `SchemaCache` (registry client + schema-id cache).
- **F. Foreign-scan executor + pushdown (`executor`, `cluster::range::router`).** A
  new **foreign-scan node**: when a scanned relation resolves to a `kafka_fdw`
  foreign table, the executor delegates to the wrapper instead of `scan_live`.
  Pushdown analysis extracts `_partition`/`_offset`/`_timestamp` constraints and
  `LIMIT` into a `ScanBounds` passed to Component E; residual predicates, projection,
  joins, aggregates run locally over the produced rows. Envelope columns
  (`_partition int4`, `_offset int8`, `_timestamp timestamptz`, `_key`, `_headers`)
  are populated from record metadata. DDL statements (B/C) route to catalog handlers.
- **G. Config & auth wiring (`kafka_fdw::config`).** Resolve `(foreign table → server
  → user mapping)` into a connection profile: bootstrap servers, registry URL,
  `security_protocol`, `sasl_mechanism`, credentials. Validate option keys at DDL
  time with PG-style errors for unknown/missing required options.
- **H. Conformance / testing (`kafka_fdw` tests, `conformance`).** Round-trip
  differential: stand up a crabka broker + Schema Registry **in-process** via crabka
  dev-dependencies (`crabka-broker`, `crabka-schema-registry`, `crabka-client-producer`)
  — matching crabgresql's spawn-in-process, condition-driven (no-`sleep`) test style;
  produce known Avro/JSON/raw records, query via the FDW, assert rows equal what was
  produced. (Child-process broker/registry binaries are the documented fallback if
  in-process bring-up proves heavy.) Pushdown property tests. FDW-grammar conformance
  vs `libpg_query`.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | Default build remains zero-C; `--features kafka` builds and links the FDW; `check-no-native.sh`/`cargo-deny` gate the default set only. | CI default lane (no-native green) + new `kafka` lane (builds/tests with C). |
| 2 | FDW DDL parses: `FOREIGN DATA WRAPPER`, `SERVER`, `USER MAPPING`, `FOREIGN TABLE`, `IMPORT FOREIGN SCHEMA` with `OPTIONS`. | `pgparser` parser unit tests. |
| 3 | FDW-grammar parity with PostgreSQL. | `pgparser` `libpg_query` oracle test. |
| 4 | Catalog CRUD for wrapper/server/user-mapping/foreign-table objects; `42704`/`42710`/dependency errors. | `catalog` unit tests. |
| 5 | Registry schema → `ColumnType` and decoded value → `Datum` for Avro/JSON/Protobuf incl. logical types, nullability, nested→`jsonb`; non-registry value → `bytea` fallback. | `kafka_fdw::types` unit tests. |
| 6 | Manual assign+seek bounded scan: earliest→HWM-at-start snapshot; `read_committed` skips aborted records; per-partition assignment. | `kafka_fdw::source` tests vs a real broker. |
| 7 | Two rustls providers coexist (`kafka` on): explicit per-connection provider, no `install_default`, no panic/swap. | `kafka_fdw` integration test. |
| 8 | Pushdown: `_partition`/`_offset`/`_timestamp`/`LIMIT` return exactly what full-scan-then-filter would; envelope columns populated. | `executor` pushdown property tests. |
| 9 | End-to-end over the wire: `CREATE SERVER`/`USER MAPPING`/`FOREIGN TABLE`, `IMPORT FOREIGN SCHEMA` generates tables from registry subjects, `SELECT` returns typed rows, joins/aggregates compose over a foreign table. | `kafka_fdw` round-trip differential (testcontainers crabka + registry). |
| 10 | No regression across the workspace; `pgparser` stays mutation-clean under the new DDL grammar. | full `cargo nextest run --workspace` + doctests; `cargo mutants` on `pgparser`. |

## Success criteria

1. With `--features kafka`, a user can `CREATE SERVER` (crabka bootstrap + registry),
   `CREATE USER MAPPING` (SASL creds), and either declare `CREATE FOREIGN TABLE`s or
   `IMPORT FOREIGN SCHEMA` to auto-generate them, then `SELECT … FROM <topic>` and get
   typed rows decoded from the registry schema (raw topics → `bytea`). — (A–G)
2. Offset/partition/timestamp predicates and `LIMIT` push down to the consumer; the
   unbounded-scan snapshot is earliest→HWM-at-query-start under `read_committed`;
   joins/aggregates compose locally. — (#6, #8, #9)
3. The default crabgresql build remains zero-C and the FDW grammar diffs clean against
   PostgreSQL; the two rustls providers coexist. — (#1, #3, #7)
4. No regression across the workspace; `pgparser` remains mutation-clean. — (#10)

## Non-goals (deferred)

- **Phase 2 — materialized views (SP41).** `CREATE MATERIALIZED VIEW … AS SELECT …
  FROM <topic>` backed by a background consumer that tails into a real MVCC table with
  **durable offset checkpoints** (resume on restart) and key-based upsert for
  compacted topics. Reuses this slice's decode + type-mapping + transport. Its
  checkpoint/resume/upsert state machine is the **Stateright** target.
- **Writes / `INSERT → produce`** — phase 1 foreign tables are read-only; the write
  path (and `crabka-client-producer`) is a later slice.
- **Continuous / streaming `SELECT`** (tailing past the start-of-query high-water
  mark) — phase 1 reads a finite snapshot.
- **Consumer-group offset commit** — federated scans are ephemeral and group-less.
- **Postgres-role → Kafka-principal pass-through** and **encrypted secret storage** —
  static per-mapping service credential, superuser-visible, for now.
- **Cross-system distributed joins / pushing SQL into Kafka** — Kafka has no join
  engine; joins are executed locally by crabgresql over consumed rows.
- **Other product integrations** (Postgres→Kafka CDC, transactional outbox, a
  Kafka→Postgres durable *sink*) — separate specs; this slice is read-from-Kafka only.
