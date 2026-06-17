# SP38 — set operations: UNION / INTERSECT / EXCEPT (SQL breadth wave 10)

**Date:** 2026-06-17
**Status:** Approved (design)

## Problem / motivation

Every breadth wave so far (SP27–SP37) operates on a single query block: one `SELECT`
producing one result set. SQL's set operators — `UNION`, `INTERSECT`, `EXCEPT` — are
the obvious remaining gap: they combine the *outputs* of two or more query blocks into
one result. This slice adds them at the top level of a statement, with PostgreSQL's
precedence, duplicate semantics, type unification, and result-level
`ORDER BY` / `LIMIT` / `OFFSET`.

## Scope

**In:**
- `UNION [ALL]`, `INTERSECT [ALL]`, `EXCEPT [ALL]` combining two or more `SELECT`
  branches at the **top level** of a statement.
- PostgreSQL precedence: `INTERSECT` binds **tighter** than `UNION` / `EXCEPT`;
  `UNION` and `EXCEPT` share precedence and are **left-associative**. Parentheses
  override.
- `ORDER BY` / `LIMIT` / `OFFSET` applied to the **combined** result, where the sort
  key is an output-column **name**, a **1-based ordinal position**, or an expression
  over the output columns.
- A parenthesized **single-`SELECT`** branch may carry its own `ORDER BY` / `LIMIT` /
  `OFFSET` (the "top-N per branch" idiom) — representable for free because a branch
  leaf *is* a `SelectStmt` and `select_to_relation` already honors those fields.
- Cross-branch **column-count** check and **type unification** (the numeric tower +
  identical types), with PG's NULL-equal duplicate semantics.

**Deferred (documented non-goals, §"Documented deviations / non-goals"):**
- Set operations nested **inside** a derived table (`FROM (SELECT … UNION …) t`) or a
  subquery expression (`x IN (SELECT … UNION …)`) — those positions stay single-
  `SELECT`; a set-op keyword there is an honest parse error.
- A trailing `ORDER BY` / `LIMIT` on a parenthesized **multi-branch** subtree
  (`(SELECT … UNION SELECT … ORDER BY …) UNION …`).
- `FOR UPDATE` / `FOR SHARE` with a set operation (PostgreSQL also rejects this).
- `VALUES` as a set-op branch (no `VALUES` query form exists yet).
- Cross-range *distributed* set operations → `0A000` (mirrors SP33/SP34).

## Keystone — a separate statement variant keeps the single-SELECT path untouched

A plain `SELECT … ORDER BY … LIMIT` (no set-op keyword) stays exactly
`Statement::Select(SelectStmt)`. Set operations get their **own** statement variant
and a recursive query-expression tree whose leaves are the existing `SelectStmt`:

```rust
// pgparser::ast
Statement::SetOperation(SetQuery)

pub struct SetQuery {
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,   // applies to the COMBINED result
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub enum SetExpr {
    Select(Box<SelectStmt>),        // a leaf branch
    SetOp {
        op: SetOp,
        all: bool,                  // ALL vs (default) distinct
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

pub enum SetOp { Union, Intersect, Except }
```

**Why a separate variant (not a tail on `SelectStmt`).** The single-`SELECT` path
(`execute_read`, `agg`, `subquery`, derived tables, the extended-protocol `describe`)
is heavily used and well-tested; a new variant leaves all of it byte-for-byte
unchanged and confines the new logic to a new module + a few new `match` arms. A flat
`Vec` tail on `SelectStmt` was rejected: it cannot express
`A UNION B INTERSECT C` = `A UNION (B INTERSECT C)` — precedence needs a tree, not a
list. The new variant does force every **exhaustive** `match Statement` (the engine
dispatch in `session.rs`, the router's `pinning_range`, `describe`) to grow one arm —
which is the desired outcome: the compiler enumerates exactly the sites that must
handle a set-op query.

## Parser (`crates/pgparser`)

- **Lexer / keywords:** add `Union`, `Intersect`, `Except` keywords. `All` already
  exists (SP28); `Distinct` already exists (SP27) and is the default modifier.
- **Refactor `select_inner` → `select_core` + tail.** `select_core` parses projection
  through `HAVING` and leaves `order_by` / `limit` / `offset` / `locking` empty. The
  existing single-`SELECT` behavior is reassembled as `select_core` + a tail parse, so
  the recursive callers (`select_inner`, used by derived tables and subquery
  expressions) keep returning a fully-populated `SelectStmt` exactly as today.
- **New precedence-climbing `set_expr(min_prec)`** builds the `SetExpr` tree.
  Precedence: `INTERSECT` = 2, `UNION` / `EXCEPT` = 1; left-associative (recurse for
  the RHS with `prec + 1`). A *primary* is `( query )` (recurse, with its own internal
  tail consumed inside the parens) or `select_core` → `SetExpr::Select`.
- **New top-level `query()`** parses `set_expr(0)` then the query-level tail
  (`ORDER BY` / `LIMIT` / `OFFSET`). If the body is a lone `SetExpr::Select` with no
  set-op, it returns `Statement::Select` with the tail (and `FOR UPDATE`) attached to
  the `SelectStmt` — **identical shape to today**. Otherwise it returns
  `Statement::SetOperation(SetQuery)`; a `FOR UPDATE` after a multi-branch set op is a
  parse error (PG-faithful). The statement dispatcher routes a leading `SELECT` *or* a
  leading `(` to `query()`.

## Executor — new `executor::setops` module

`execute_set_operation(... , &SetQuery, ctx) -> QueryResult`:

1. **Evaluate every leaf** via the existing `exec::select_to_relation` →
   `Relation { scope, rows }`. Full reuse: a branch's own WHERE / GROUP BY / joins /
   uncorrelated subqueries / (parenthesized) ORDER BY+LIMIT already work. A small
   `fold_set_expr` recurses the tree, combining child relations.
2. **Column count** must agree across the two sides of every `SetOp` node → else
   `42601` ("each UNION/INTERSECT/EXCEPT query must have the same number of columns").
   *(Confirm the exact SQLSTATE + message against the local PG oracle before commit.)*
3. **Type unification** per output column: fold `eval::unify_types` across the two
   child column types at each `SetOp` node (numeric tower + identical-type; an
   incompatible pair → `42804`). Output **names** come from the **left/first** branch.
   An unknown-typed column (a bare `NULL` / untyped literal in a branch) unifies to the
   other side rather than forcing a type clash — see the NULL note below.
4. **Coerce** each child's cells to the unified per-column type via `pgtypes::cast::cast`
   on the unify-approved pairs (widening within the numeric tower, or `NULL → NULL`);
   identical types are a no-op.
5. **Combine** with multiplicity, reusing `Datum`'s hand-written grouping `Eq` / `Hash`
   (NULL = NULL, `-0.0` = `+0.0`, `NaN` = `NaN`) — exactly set-op "not distinct"
   semantics:
   - `UNION` → concat both sides then dedup; `UNION ALL` → concat, keep all.
   - `INTERSECT` → rows present in both (distinct: once; `ALL`: `min(Lₙ, Rₙ)`).
   - `EXCEPT` → rows in left not in right (distinct: once; `ALL`: `max(0, Lₙ − Rₙ)`).
6. **Query-level `ORDER BY`** over the combined output: resolve each key against the
   **output** scope — a bare integer literal is a **1-based ordinal position**
   (PG rule; required because output columns may be unnamed `?column?`), otherwise an
   output-column name or an expression over output columns; reuse `order_cmp` and
   `apply_offset_limit` for `OFFSET` / `LIMIT`.
7. **Encode** to `QueryResult::Rows` via `rows_result` with the first-branch field
   names + unified types; the tag is `SELECT <n>`.

`session.rs` gains a `Statement::SetOperation` dispatch arm calling
`execute_set_operation`; `describe` gains a parallel arm that produces the
`RowDescription` (first-branch names + unified types) from a schema-only pass
(`build_from_schema` per leaf, no execution).

New `ExecError` variants as needed: `SetOpColumnCount (42601)` (and reuse the existing
`TypeMismatch → 42804` for incompatible unification).

## Cross-range co-location (router)

`pinning_range` gains a `Statement::SetOperation` arm; a new `collect_set_expr_ranges`
recurses the tree, calling the existing `collect_select_ranges` on each leaf. All leaf
ranges are deduped: a single range routes locally; more than one is rejected `0A000`
("set operations spanning ranges are not supported"). This preserves the invariant
that one statement never spans ranges (SP33/SP34). In the default single-range
deployment every set operation works.

## Why no Stateright model (consistent with SP27–SP37)

A set operation is a pure relational fold over already-correct, MVCC-visible rows:
every branch runs under **one** statement snapshot inside **one** `execute_read` /
`select_to_relation` on **one** engine (cross-range rejected at the router). It
introduces no new lock, write path, visibility rule, or cross-range interleaving — the
same "pure-data / single-node refactor with no concurrency/fault dimension" carve-out
the last eleven breadth waves used. The semantics (precedence, multiplicity, NULL-equal
dedup, type unification, positional ORDER BY) are **value** properties with no event
ordering to explore; they are proven exhaustively by unit tests + the conformance
oracle.

## Testing

- **`pgparser`**: parser tests — precedence (`INTERSECT` tighter than `UNION`),
  left-associativity, `ALL` vs distinct, parentheses for grouping, `ORDER BY` / `LIMIT`
  binding to the whole query vs a parenthesized single-`SELECT` branch, and the
  `FOR UPDATE`-with-set-op parse error. **libpg_query oracle**: add the accepted set-op
  forms to the ACCEPTED list; the column-count mismatch is an *analysis* error (not a
  raw-grammar error) — per the libpg_query-oracle rule it stays a unit test, **not** an
  oracle REJECTED entry.
- **`executor::setops` unit tests**: every combine (`UNION`/`ALL`, `INTERSECT`/`ALL`,
  `EXCEPT`/`ALL`) including multiplicity; NULL-equal dedup/match; cross-branch type
  unification (int4∪int8→int8, int∪numeric→numeric, int∪float8→float8, identical
  types) and coercion of values; column-count mismatch → `42601`; incompatible types →
  `42804`; positional and named `ORDER BY` over the output; precedence evaluation
  (`A UNION B INTERSECT C`).
- **`executor::set_operations` wire test** (end-to-end over the wire): the three
  operators + `ALL`, result column names/types/OIDs, ordering, `LIMIT`/`OFFSET`,
  top-N-per-parenthesized-branch, and the `42601`/`42804`/`0A000` error surface.
  Target name `set_operations` is **UAC-safe** (no `setup`/`install`/`update`/`patch`/
  `upgrad` substring).
- **`cluster` router test**: a set operation spanning ranges is rejected `0A000` while
  a co-located one runs (mirror SP34's
  `a_cross_range_subquery_is_rejected_while_colocated_runs`).
- **`conformance/corpus/set_operations.sql`**: validated locally against the PG oracle
  (every query carries an explicit `ORDER BY` for deterministic row order — PG does not
  guarantee set-op output order otherwise); diffed vs PG 18 in CI.

## File-by-file change list

- `crates/pgparser/src/{token.rs,lexer.rs}` — `Union`/`Intersect`/`Except` keywords.
- `crates/pgparser/src/ast.rs` — `Statement::SetOperation`, `SetQuery`, `SetExpr`,
  `SetOp`.
- `crates/pgparser/src/parser.rs` — `select_core` + tail refactor; `set_expr`,
  `query`; route `SELECT`/`(` at statement level; parser tests.
- `crates/executor/src/setops.rs` — **new** module (`execute_set_operation`,
  `fold_set_expr`, the combine + unify + coerce + positional-ORDER-BY helpers); unit
  tests.
- `crates/executor/src/{lib.rs,session.rs,exec.rs}` — wire the module; `session.rs`
  dispatch arm; `exec::describe` arm; reuse `select_to_relation` / `build_from_schema`
  / `unify_types` / `order_cmp` / `apply_offset_limit` / `rows_result`.
- `crates/executor/src/error.rs` — `SetOpColumnCount (42601)`.
- `crates/cluster/src/range/router.rs` — `Statement::SetOperation` arm in
  `pinning_range`; `collect_set_expr_ranges`; router test.
- `crates/executor/tests/set_operations.rs` — **new** wire test.
- `crates/conformance/corpus/set_operations.sql` — **new** corpus file.
- `CLAUDE.md` — SP38 entry (UAC-safe target audit; the `executor` integration-test
  list gains `set_operations`).

## Documented deviations / non-goals

- **Set-ops inside a subquery / derived table** — deferred; a set-op keyword inside
  `FROM (…)` or `IN (…)` is a parse error (the honest deferral, never a silent single-
  branch result).
- **Trailing `ORDER BY`/`LIMIT` on a parenthesized multi-branch subtree** — deferred
  (the `SetExpr::SetOp` node carries no tail of its own); the outermost query and a
  parenthesized single-`SELECT` branch are the supported tail positions.
- **`FOR UPDATE` / `FOR SHARE` with a set operation** — parse error (PG-faithful).
- **`VALUES` as a branch** — no `VALUES` query form exists yet.
- **Positional `ORDER BY` on a plain (non-set-op) `SELECT`** — unchanged from today
  (the new positional handling is scoped to the set-op output path); revisit if a later
  slice wants `SELECT a ORDER BY 1` to be positional.
- **Untyped `NULL` / unknown-type branch column** — unifies to the other branch's type
  rather than PG's full `unknown`-type resolution; an all-`NULL` column across every
  branch resolves to `text` (matching crabgresql's existing `infer_type` default).
  *(Confirm `SELECT NULL UNION SELECT 1` behavior against the oracle; document the exact
  result during implementation.)*
- **Cross-range distributed set operations** → `0A000`.
- **Set-op output order without `ORDER BY`** is unspecified (as in PG); the corpus
  always sorts.
