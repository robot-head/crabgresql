# SP39 — plain SELECT ORDER BY parity (SQL breadth query-expression runway)

**Date:** 2026-06-18
**Status:** Approved (design)

## Problem / motivation

SP38 added PostgreSQL-style output ordering for set-operation results, including
`ORDER BY 1` as a 1-based output-column position. It deliberately left plain
`SELECT` unchanged: in the existing row path, non-`DISTINCT` ordering evaluates each
`ORDER BY` expression against the source row before projection, so `ORDER BY 1` is a
constant-sort no-op and `ORDER BY alias` is an undefined/ambiguous source-column
lookup instead of an output-label lookup.

This slice closes that documented SP38 deferral for ordinary `SELECT`, including the
edge cases that make the behavior PostgreSQL-observable: output aliases and names,
positions, `SELECT DISTINCT`, and aggregate/grouped queries.

## Scope

**In:**
- Plain non-set-operation `SELECT ... ORDER BY ...`:
  - `ORDER BY 1`, `ORDER BY 2 DESC`, etc. are 1-based output positions.
  - A bare unqualified name that matches an output alias/name orders by that output
    column.
  - A bare unqualified name that matches more than one output column is ambiguous
    (`42702`), matching PostgreSQL 18.
  - A bare unqualified name that does not match an output column falls back to the
    existing source-scope expression path, preserving `SELECT a FROM t ORDER BY b`.
  - A non-bare expression, including `b + 0`, remains a source-scope expression; output
    labels are not expression variables.
- `SELECT DISTINCT`:
  - `ORDER BY` positions and output aliases/names are allowed.
  - Non-output ordering expressions are rejected with PostgreSQL's `42P10`:
    "for SELECT DISTINCT, ORDER BY expressions must appear in select list".
- Aggregate/grouped queries:
  - `ORDER BY` output positions, projected aggregate aliases, and projected grouped
    aliases work.
  - Legal grouped/aggregate expressions that are not projected keep working through the
    existing grouped evaluator.
- Oracle-pinned error surface for the supported cases:
  - `ORDER BY 0` / out-of-range position -> `42P10`.
  - Too-large integer constant in ordinal position -> `42601`
    ("non-integer constant in ORDER BY").
  - Duplicate output label selected by bare name -> `42702`
    (`ORDER BY "x" is ambiguous`).

**Out:**
- `VALUES`, CTEs, nested set operations in derived/subquery positions, window
  functions, `NULLS FIRST` / `NULLS LAST`, collations, and ordered-set aggregate syntax.
- Planner/performance work; this is a semantic parity slice over the existing execution
  pipeline.
- Any cross-range execution change.

## PostgreSQL 18 oracle findings

The design was pinned against a local PostgreSQL 18 oracle for the behavior that is
easy to get subtly wrong:

- `SELECT a FROM ob ORDER BY 1` sorts by output column `a`.
- `SELECT a AS x FROM ob ORDER BY x DESC` sorts by output alias `x`.
- `SELECT DISTINCT a FROM ob ORDER BY b` errors `42P10`.
- `SELECT DISTINCT a AS x FROM ob ORDER BY x` succeeds.
- `SELECT a AS x, b AS x FROM ob ORDER BY x` errors `42702`.
- `SELECT a AS b, b FROM ob ORDER BY b` errors `42702`.
- `SELECT a AS b FROM ob ORDER BY b` uses the output alias, while
  `ORDER BY ob.b` and `ORDER BY b + 0` use the source column.
- `ORDER BY 0` and `ORDER BY 9` on a one-column output error `42P10`.
- An integer literal too large to be a positional reference errors `42601`
  ("non-integer constant in ORDER BY").

## Keystone — resolve ORDER BY keys as SQL92 output references first

PostgreSQL treats a simple `ORDER BY` item differently from a general expression:

1. A bare integer constant is a 1-based output column position.
2. A bare unqualified name can refer to an output column label.
3. A general expression is evaluated normally against the input/source scope.

crabgresql should encode that distinction directly instead of trying to make aliases
visible to the whole expression evaluator. The core helper is an output-aware resolver
that receives the parsed `OrderItem`, the already-resolved output fields/expressions,
and whichever row representation the caller has available:

```rust
enum SelectOrderKey {
    Output(usize),
    SourceExpr(Expr),
}
```

The resolver returns `Output(i)` for positional and output-name references, and
`SourceExpr(expr)` for the preserved source-expression path. Duplicate output-name
matches produce `ExecError::AmbiguousColumn` with the PostgreSQL-style
`ORDER BY "name" is ambiguous` message. Out-of-range positions use the existing
`ExecError::InvalidColumnReference`.

This helper should live near the existing projection/order helpers in
`executor::exec`, because both row queries and aggregate queries need it. The SP38
set-operation path may share the ordinal/name helper once the plain-SELECT path is
green, but set-op behavior must not change in this slice.

## Executor data flow

### Non-DISTINCT row queries

`project_rows_ordered` currently sorts source rows before projection, then projects.
Keep that shape, but pre-resolve each `OrderItem`:

- `Output(i)` keys are evaluated by computing the corresponding projected expression
  for the current source row.
- `SourceExpr(expr)` keys are evaluated with the existing `eval(expr, source_scope,
  source_row, ctx)` path.

Then sort, apply `OFFSET` / `LIMIT`, and project exactly as today. This preserves
`ORDER BY` on non-projected source columns while fixing output aliases and positions.

### SELECT DISTINCT row queries

The distinct path already projects first, deduplicates projected rows, then sorts.
For this path, every `ORDER BY` item must resolve to an output column. Positions and
output labels are allowed; source expressions are rejected with `42P10`.

The existing `distinct_order_indices` should be replaced or widened so it understands
positions and aliases, and so it emits `42P10` rather than the current `0A000`
unsupported error.

### Aggregate/grouped queries

`aggregate_rows` already finalizes each group as `(order_keys, projected)`. Adjust the
per-group finalization so the ordering resolver runs before evaluation:

- `Output(i)` reads the projected datum at index `i`.
- `SourceExpr(expr)` continues through `eval_grouped`, preserving legal grouped and
  aggregate expressions.

For `SELECT DISTINCT` aggregate outputs, the same "only output ordering keys" rule
applies after projected rows are deduplicated.

## Error handling

Add only the error surface needed to match PostgreSQL:

- Reuse `ExecError::InvalidColumnReference` for bad positions and `SELECT DISTINCT`
  non-output ordering, both `42P10`.
- Add a narrow `ExecError::Syntax(String)` -> `42601` for the PostgreSQL
  "non-integer constant in ORDER BY" overflow case.
- Add `ExecError::AmbiguousOrderBy(String)` -> `42702` so duplicate output-label
  references render as `ORDER BY "x" is ambiguous` rather than the generic source-column
  ambiguity message.

Do not remap ordinary source expression failures: undefined columns remain `42703`,
ambiguous source columns remain `42702`, and missing qualified FROM entries remain
`42P01`.

## Why no Stateright model

This is a pure output-ordering/value-resolution slice. It runs inside one statement
snapshot on one engine, introduces no new write path, lock, MVCC visibility rule,
router rule, leadership interaction, or cross-range interleaving. The correctness
properties are deterministic value/SQLSTATE properties, covered by unit tests and the
PostgreSQL conformance oracle.

## Testing

- **`executor::exec` unit tests** for the ordering resolver and row path:
  - ordinal ordering (`ORDER BY 1`, `ORDER BY 2 DESC`);
  - alias / output-name ordering;
  - output alias beats source column for a bare name;
  - qualified source expression and expression source fallback still work;
  - duplicate output aliases -> `42702`;
  - bad positions -> `42P10`;
  - integer overflow -> `42601`;
  - `SELECT DISTINCT` accepts output positions/aliases and rejects source-only keys
    with `42P10`.
- **`executor::agg` unit tests**:
  - grouped projection alias (`SELECT k AS g ... GROUP BY k ORDER BY g`);
  - aggregate alias (`count(*) AS c ORDER BY c DESC`);
  - ordinal aggregate ordering (`ORDER BY 2 DESC`);
  - legal non-projected grouped/aggregate expression remains supported;
  - `SELECT DISTINCT` aggregate output ordering obeys the same output-only rule.
- **Wire-level executor test**:
  - extend an existing UAC-safe test target if the additions stay small, or add a new
    `executor::ordering` integration target if the cases are clearer there.
- **Conformance corpus**:
  - add `crates/conformance/corpus/order_by.sql`, with deterministic successful
    queries and SQLSTATE-only error cases diffed against PostgreSQL 18.

## File-by-file change list

- `crates/executor/src/exec.rs` — output-aware `ORDER BY` resolver, row-query ordering
  changes, `SELECT DISTINCT` error behavior, unit tests.
- `crates/executor/src/agg.rs` — aggregate/grouped query ordering changes and tests.
- `crates/executor/src/error.rs` — add `Syntax` (`42601`) and `AmbiguousOrderBy`
  (`42702`) variants for PostgreSQL-exact messages.
- `crates/executor/tests/{end_to_end.rs or ordering.rs}` — wire-level coverage.
- `crates/conformance/corpus/order_by.sql` — PostgreSQL 18 differential cases.
- `CLAUDE.md` — SP39 audit entry after implementation, including UAC-safe target audit
  if a new integration target is added.

## Success criteria

1. Plain `SELECT` supports PostgreSQL-style output positional and alias/name
   ordering without regressing source-expression ordering.
2. `SELECT DISTINCT` and aggregate/grouped queries match PostgreSQL for the in-scope
   `ORDER BY` success and error cases.
3. `crates/conformance/corpus/order_by.sql` diff-checks cleanly against PostgreSQL 18.
4. No set-operation regression; any helper sharing keeps SP38 behavior unchanged.
5. Workspace tests for the touched crates pass, and the full workspace test command is
   run before shipping the implementation.

## Non-goals (deferred)

- `VALUES` query expressions and `VALUES` as a set-operation branch.
- Set operations nested inside derived tables or subquery expressions.
- CTEs / `WITH`, recursive queries, window functions, `ORDER BY ... NULLS FIRST/LAST`,
  collation-sensitive ordering, ordered-set aggregates, and planner optimizations.
- Deep expression-equivalence beyond the existing AST equality rules where PostgreSQL's
  parse analysis recognizes a select-list expression by identity. If the oracle exposes
  a case that needs expression canonicalization beyond the current AST representation,
  document that case and defer it rather than adding broad canonicalization here.
