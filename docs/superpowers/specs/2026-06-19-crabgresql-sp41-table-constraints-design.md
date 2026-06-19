# SP41 — Table Constraints (NOT NULL, DEFAULT, CHECK, UNIQUE, PRIMARY KEY)

**Date:** 2026-06-19
**Status:** Approved (design)
**Slice:** SP41

## Problem / Motivation

crabgresql has a broad read-side SQL surface (aggregates, joins, subqueries, set
operations, CTEs, VALUES, the full type/function library) and deep distributed
2PC machinery, but the **write/schema side is bare**: `CREATE TABLE` parses only
`ColumnDef { name, ty }` — there are **no constraints at all**. A
PostgreSQL-compatible database without `NOT NULL`, `DEFAULT`, `CHECK`, `UNIQUE`,
or `PRIMARY KEY` cannot model real tables.

This slice adds the full **non-foreign-key** constraint set, enforced at write
time, matching PostgreSQL's observable behavior (syntax, semantics, SQLSTATEs,
message text) as identically as possible. `FOREIGN KEY` is a separate follow-up.

The notable design depth is `UNIQUE`/`PRIMARY KEY`: enforcing "no two live rows
share a key" must serialize concurrent inserts of the same value and survive a
leader failover that occurs mid-insert. That earns this slice two Stateright
models with teeth and a multi-process kill nemesis, consistent with the
project's distributed-correctness discipline.

## Scope

**In:**

- Column-level constraints: `NOT NULL` / `NULL`, `DEFAULT <expr>`, `PRIMARY KEY`,
  `UNIQUE`, `CHECK (<expr>)`, optional `CONSTRAINT name`.
- Table-level constraints: `[CONSTRAINT name] PRIMARY KEY (cols)`,
  `[CONSTRAINT name] UNIQUE (cols)`, `[CONSTRAINT name] CHECK (<expr>)`.
- Composite (multi-column) `PRIMARY KEY` / `UNIQUE` via the table-level form.
- Write-time enforcement on `INSERT` and `UPDATE`.
- A **durable, enforcement-only unique index** keyspace (per unique/PK
  constraint) used solely for write-time duplicate detection.
- Concurrency: a transaction-scoped **value lock** serializing same-key writers
  (first-committer-wins).
- **Leader-failover hardening** for uniqueness (single-failover scope): the
  leadership-rise sweep reconstructs value locks for in-doubt unique keys before
  opening the write gate.

**Deferred / non-goals (documented):**

- `FOREIGN KEY` / `REFERENCES` and `ON DELETE` / `ON UPDATE` actions.
- `ALTER TABLE ADD/DROP CONSTRAINT` — constraints are declarable only at
  `CREATE TABLE`.
- `EXCLUDE` constraints; `NULLS NOT DISTINCT`; `DEFERRABLE` / `INITIALLY
  DEFERRED`; `NOT VALID`.
- `GENERATED` / identity columns; `ON CONFLICT` (upsert).
- `CREATE INDEX`, non-unique indexes, and any index-based query acceleration —
  the unique index is **not** consulted by SELECT planning (scans stay
  full-table).
- Subquery / aggregate inside `CHECK` → `0A000`.
- **Overlapping / cascading failover** during an in-flight unique insert
  (mirrors the SP23/SP25/SP26 single-failover scoping). The single-failover
  case (one leader killed mid-insert, full drain before the next) is in scope.

## SQL Surface & Semantics

### Syntax

Column-level (any order, repeatable) after a column's type:

```
col TYPE [NOT NULL | NULL] [DEFAULT <expr>] [PRIMARY KEY] [UNIQUE]
         [CHECK (<expr>)] [CONSTRAINT name ...]
```

Table-level (after the column list):

```
[CONSTRAINT name] PRIMARY KEY (col, ...)
[CONSTRAINT name] UNIQUE (col, ...)
[CONSTRAINT name] CHECK (<expr>)
```

Unnamed constraints get PostgreSQL-style auto names (`t_pkey`, `t_col_key`,
`t_col_check`) for the catalog and error text.

### Per-constraint semantics (PostgreSQL-faithful)

- **`NOT NULL`** — write-time check; a NULL in the column → `23502`. Stored as a
  column attribute (`attnotnull`), not a constraint object.
- **`DEFAULT <expr>`** — evaluated **per-row at INSERT** for omitted columns and
  for an explicit `DEFAULT` token in `VALUES`. May reference constants and
  functions (volatile ones re-evaluated per row); **may not reference other
  columns** (PG rejects that → `42703` at DDL time). Stored as **source text**,
  re-parsed on table load.
- **`CHECK (<expr>)`** — boolean row predicate over same-row columns; **no
  subqueries, no aggregates**. Evaluated on INSERT and UPDATE. PG quirk
  replicated: the constraint **passes when the result is TRUE *or* NULL**; only
  FALSE → `23514`.
- **`PRIMARY KEY`** — implies `NOT NULL` on every key column **and** `UNIQUE`. At
  most one per table (a second → `42P16`).
- **`UNIQUE`** — no two live rows share the key. **NULLs are distinct** (PG
  default): a row with NULL in any key column is exempt from the conflict check
  (multiple NULLs allowed). Violation → `23505`.

## Catalog Model & Serialization

`catalog::Column` gains two attributes:

```rust
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,          // false ⇒ NOT NULL
    pub default: Option<String>, // source text of the DEFAULT expr
}
```

`catalog::Table` gains a constraint list:

```rust
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign: Option<ForeignTableMeta>,
    pub constraints: Vec<Constraint>,
}

pub struct Constraint {
    pub name: String,   // explicit or auto-generated PG-style
    pub id: u32,        // stable per-table id → index keyspace discriminator
    pub kind: ConstraintKind,
}

pub enum ConstraintKind {
    PrimaryKey { columns: Vec<usize> }, // column ordinals
    Unique     { columns: Vec<usize> },
    Check      { expr: String },        // source text, re-parsed on load
}
```

Column-level `UNIQUE` / `PRIMARY KEY` / `CHECK` **desugar** into table-level
`Constraint`s at parse→catalog time (as PG normalizes them). Column-level
`NOT NULL` / `DEFAULT` set the column attributes directly. `PRIMARY KEY` also
flips `nullable = false` on its columns. `Constraint.id` is the discriminator in
the index keyspace, so each unique/PK constraint owns a disjoint index range.

**Serialization (`catalog::serde`):** bump `SCHEMA_VERSION` 2 → 3. The v3
per-column payload appends a `nullable` byte and an optional default-text field;
after the column list comes a constraints section: count, then per constraint
`{id, name, kind tag, kind payload}`. A **v2 payload still decodes** (forward-read
safety: all columns nullable, no defaults, no constraints), so the bump is
non-destructive. This is a genuine format change (not an append-only tag add), so
the version bump is the honest move; greenfield ⇒ no migration burden.

**Validation at `CREATE TABLE`** (`catalog::create_table_ops`): reject a second
`PRIMARY KEY` (`42P16`), a constraint/PK referencing an unknown column (`42703`),
and re-parse + type-check each `DEFAULT` / `CHECK` expression at create time so a
bad expression fails `CREATE TABLE` rather than the first `INSERT` (PG validates
at DDL time).

## Implementation Waves

The feature lands as three sequenced, independently-mergeable waves under this
one spec.

### Wave A — pure-data constraints (NOT NULL, DEFAULT, CHECK)

No index, no locks, no concurrency, **no Stateright model** (the SP27–SP39
"pure-data / single-node" carve-out applies).

**Parser.** New keywords as needed (`PRIMARY`, `KEY`, `DEFAULT`, `CHECK`,
`CONSTRAINT`; `NULL`/`UNIQUE` already present). `create_table` parses
column-level and table-level constraints. AST: `ColumnDef` gains
`constraints: Vec<ColumnConstraint>`; a table-level `Vec<TableConstraint>` on the
`CreateTable` statement. `DEFAULT` / `CHECK` expressions reuse `expr(0)` and are
also captured as **source text** for catalog storage.

**INSERT enforcement** (extending the `Statement::Insert` arm), per row, in
order:

1. **DEFAULT fill** — for each column not supplied (or supplied as the `DEFAULT`
   token), evaluate its `default` expr (re-parsed, cached on the loaded `Table`);
   no default ⇒ `Null`.
2. **Coerce** each value to the column type (existing `coerce`).
3. **NOT NULL** — any `nullable == false` column holding `Null` →
   `NotNullViolation` (`23502`).
4. **CHECK** — evaluate each `Check` over the row's `Scope`; **fail only on
   FALSE** (TRUE/NULL pass) → `CheckViolation` (`23514`).

**UPDATE enforcement** (after EvalPlanQual produces the new row image): re-run
NOT NULL + CHECK on the updated row. `SET col = DEFAULT` resolves to the column
default.

**Expression caching.** A loaded `Table` lazily parses its `DEFAULT` / `CHECK`
source text into `Expr` once; re-parsing on load keeps the catalog free of
AST-versioning concerns.

**New `ExecError`:** `NotNullViolation { column, table }` → `23502`,
`CheckViolation { constraint, table }` → `23514`.

### Wave B — uniqueness on a stable leader

Three new pieces plus a model with teeth.

**1. Durable unique-index keyspace.** A new
`kv::key::index_key(table_id, constraint_id, key_bytes)` namespace, disjoint from
`row_key`, on the **same range** as the table (range routing is by `table_id`, so
index and heap co-reside — uniqueness is always single-range). `key_bytes` is an
**equality-canonical** encoding of the key datums (extending `kv::keyenc`)
respecting `Datum`'s grouping equality (`1.0 == 1.00`, `-0.0 == 0.0`, …), so the
byte key collides iff the values are "the same" by PG's unique semantics. Entry:
`index_key(...) → rowid`. The index is **non-MVCC** (a bare heap pointer, like a
PG btree); visibility is resolved by reading the pointed row's versions. A NULL in
any key column ⇒ **no entry** (NULLs distinct).

**2. Value lock.** Extend `RowLockManager` with a parallel value-lock table keyed
by `(table_id, constraint_id, key_bytes)`, exclusive, transaction-scoped, reusing
the existing `Notify` / wait-for-graph / deadlock machinery. The wait-for graph
now spans both row and value locks, so a deadlock through a value lock is
detected → `40P01`. Released at COMMIT/ROLLBACK with the row locks.

**3. Enforcement protocol** (per unique/PK constraint touched by an INSERT, or an
UPDATE that changes a key column):

1. Compute `key_bytes`; if any key column is NULL → skip (distinct).
2. **Acquire the exclusive value lock** `(table, constraint, key_bytes)` — this
   serializes concurrent same-key writers (first-committer-wins).
3. **Look up** `index_key` → candidate `rowid`. Absent ⇒ no conflict.
4. If present, **read the pointed row's latest version** and decide:
   - committed & live with this key → **`23505`**;
   - dead / key-changed / pointer stale → no conflict (lazily overwrite);
   - being modified by an **in-flight** xid (xmin or xmax in-progress) → **wait
     on that xid** via the procarray (the existing EvalPlanQual / lock-wait
     primitive), then re-decide. (The value lock blocks concurrent *inserts* of
     K; this xid-wait handles a concurrent *delete/update* of the existing K row,
     matching PG's "wait on the in-progress xid.")
5. No conflict → emit, in the **same commit batch**, the row `Put` and the index
   `Put(key → new rowid)`. Hold the value lock to end of txn.

**UPDATE** changing a key column = remove-old-key + insert-new-key check; the old
index entry is left **stale** (a harmless pointer, overwritten on next insert of
that key — lazy cleanup, no tombstone). **DELETE** leaves the entry stale
likewise; the next inserter reads the pointed row as dead → overwrites.

**Stateright model — `unique_value_lock_model`** (mirrors
`mvcc_write_conflict_model`). Abstract the *logic*: N transactions
inserting/deleting keys from a small domain against a shared index + value-lock
table; canonical sorted state; bounded steps. **Invariants:**
*at-most-one-live-row-per-key* and *no-lost-insert*. **Boolean toggle**
`value_lock` (true = take the lock before the index check; false = skip it → the
TOCTOU lets two inserts of K both commit). **Teeth test (mandatory):** the
`value_lock = false` variant is caught and `discoveries()` names
`at_most_one_live_row_per_key`; plus `unique_state_count() > 1`.

### Wave C — leader-failover hardening

The hazard: the value lock is in-memory, so a leader dying mid-insert leaves a
**durable in-doubt row + index entry** but no lock. If the risen leader opened its
write gate without accounting for that in-doubt unique key, a new insert of the
same key could miss the conflict — and if the in-doubt row later commits, two live
rows share the key. This is the uniqueness analogue of SP24's abort-atomicity
half-leak, and plugs into the same settle-before-serve machinery (`RecoveryGate`
+ `resolve_in_doubt_on_leadership`).

**Mechanism.** Extend the leadership-rise sweep
(`server_node::resolve_in_doubt_on_leadership`, which already re-acquires in-doubt
**row** locks per SP24 D3c). Before `mark_served` opens the write gate, the sweep
— while scanning the in-doubt rows it already visits — for each in-doubt row that
carries a unique key, **reconstructs the exclusive value lock**
`(table, constraint, key_bytes)` and holds it until that row's `g` resolves to a
global decision (`executor::reacquire_in_doubt_unique_locks`, alongside
`reacquire_in_doubt_locks`). Between rise and resolution, a new same-key insert
blocks on the reconstructed value lock exactly as it would have on the original
leader; once the in-doubt `g` commits (key live → new insert sees `23505`) or
aborts (key gone → new insert proceeds), the lock releases. Reuses SP26's
*settle-COMPLETE-before-serve*: the gate opens only after every in-doubt marker is
resolved/locked.

**Aborted in-doubt insert's index entry** points at a now-dead row — already
handled by Wave B's lazy-pointer rule (the next inserter reads the dead row → no
conflict → overwrites). No separate cleanup path.

**Stateright model — `unique_failover_settle_model`** (mirrors
`crossrange_2pc_abort_atomicity_model` / `crossrange_2pc_overlap_settle_model`).
Abstract the rise-sweep + gate + a concurrent same-key insert across a leadership
change; canonical sorted state; bounded steps. **Invariant:**
*at-most-one-live-row-per-key survives a failover*. **Boolean toggle**
`reacquire_unique_locks` (true = reconstruct value locks before `mark_served`;
false = open the gate without them → the tear appears). **Teeth test
(mandatory):** the `false` variant is caught and `discoveries()` names
`at_most_one_live_row_per_key_across_failover`; plus `unique_state_count() > 1`.

**Multi-process kill nemesis — `crabgresql::unique_failover_bank`** (mirrors
`participant_kill_bank` / `range0_leader_kill_drain`; UAC-safe target name). A
workload inserts contended unique keys into a single-range table while a nemesis
kills the table's range leader mid-insert each round, with a full drain between
kills (single-failover scope). The oracle asserts **no key ever has two live
rows** and **every acknowledged insert is durable**. Runs un-`#[ignore]`'d, must
pass repeatedly non-flaky.

## Error Surface (all PostgreSQL-faithful)

| Condition | SQLSTATE | Name |
|---|---|---|
| NULL into NOT NULL column | `23502` | not_null_violation |
| Duplicate unique/PK key | `23505` | unique_violation |
| CHECK predicate is FALSE | `23514` | check_violation |
| Second PRIMARY KEY on a table | `42P16` | invalid_table_definition |
| Constraint references unknown column | `42703` | undefined_column |
| Deadlock through a value lock | `40P01` | deadlock_detected |
| Subquery/aggregate in CHECK, or a deferred form | `0A000` | feature_not_supported |

Message text matches PG (e.g. `duplicate key value violates unique constraint
"t_pkey"`, `null value in column "c" of relation "t" violates not-null
constraint`), since the conformance oracle diffs output.

## Testing

- **Parser:** unit tests for every constraint form (column- and table-level,
  named/unnamed, composite) + libpg_query oracle on accepted forms.
- **Catalog:** v3 serde round-trip + v2 back-read; `CREATE TABLE` validation
  (`42P16`, `42703`, bad DEFAULT/CHECK expr at DDL time).
- **Executor unit tests:** DEFAULT fill (incl. per-row volatile default), NOT
  NULL on insert/update, CHECK true/false/NULL semantics; uniqueness (insert dup
  → 23505, insert-after-delete OK, update-into-dup → 23505, composite key,
  multiple NULLs OK, concurrent same-key serialize → one 23505).
- **Stateright:** `unique_value_lock_model` and `unique_failover_settle_model`,
  **each with a teeth test** that names the specific safety property and asserts
  `unique_state_count() > 1`.
- **Multi-process nemesis:** `crabgresql::unique_failover_bank`, un-`#[ignore]`'d,
  non-flaky.
- **Over-the-wire integration:** `executor::constraints` (UAC-safe).
- **Conformance corpus:** `constraints_basic.sql` (Wave A) and
  `constraints_unique.sql` (Wave B), validated locally vs a real PostgreSQL and
  diffed vs PG 18 in CI.
- **Mutation testing:** new pure-data code (`catalog`, `kv::keyenc`) folds into
  the cargo-mutants nightly, driven toward zero survivors.

## UAC-safe target names

New test binaries — `cluster::unique_value_lock_model`,
`cluster::unique_failover_settle_model`, `crabgresql::unique_failover_bank` —
contain none of `setup` / `install` / `update` / `patch` / `upgrad`. The guard
`git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
must stay empty.

## Documented Deviations / Non-goals

- **Deferred features:** `FOREIGN KEY` / `REFERENCES`, `ALTER TABLE ADD/DROP
  CONSTRAINT`, `EXCLUDE`, `NULLS NOT DISTINCT`, `DEFERRABLE` / `INITIALLY
  DEFERRED`, `NOT VALID`, `GENERATED` / identity, `ON CONFLICT`, `CREATE INDEX` /
  non-unique / query-accelerating indexes.
- **Failover scope:** single-failover only; overlapping/cascading failover during
  an in-flight unique insert is deferred (SP23/SP25/SP26 precedent).
- **Index not used for reads:** SELECT planning still full-scans; the index exists
  solely for write-time uniqueness.
- **Constraint naming:** auto-generated names follow PG's convention, but a
  pathological long/duplicate auto-name is not de-collided exactly as PG does
  (cosmetic).
- **CHECK:** no subqueries/aggregates (PG also forbids these); a non-immutable
  function in CHECK is evaluated but not rejected at DDL time.
