# SP34 — uncorrelated subquery expressions (SQL breadth wave 8)

**Date:** 2026-06-16
**Status:** Approved (design) — as-built (re-ported onto the SP33 joins foundation)

## Problem / motivation

SP33's joins note explicitly deferred "scalar / `IN` subqueries (in WHERE/SELECT/
HAVING)". This slice fills that gap: **uncorrelated subquery expressions** — scalar
`(SELECT …)`, `x [NOT] IN (SELECT …)`, `[NOT] EXISTS (…)`, and
`x op ANY|SOME|ALL (…)`. (Derived tables in FROM already landed with SP33's joins;
*correlated* subqueries remain deferred.)

This design was originally written against the pre-joins single-table foundation
(`SelectStmt.from: Option<String>`, `eval(expr, table, values)`). The SP33 joins
slice landed first and rewrote that foundation — `from: Vec<TableExpr>`, a `Scope`
name-resolution abstraction, a `Relation { scope, rows }` + `select_to_relation`
read primitive. The slice was **re-ported onto that foundation**; this doc reflects
the as-built result.

## Keystone — eager substitution keeps the evaluator pure

An **uncorrelated** subquery's result is identical for every outer row, so it is
evaluated **once**, before the outer row loop, and the subquery node is rewritten
into already-supported nodes. The pure evaluator (`eval`/`agg::eval_grouped`) then
runs unchanged with a single new `Expr::Const { value, ty }` arm.

The new `executor::subquery` module:

- `resolve_in_select(ctx, &SelectStmt) -> SelectStmt` rewrites every subquery in the
  expression clauses (projection / WHERE / HAVING / GROUP BY / ORDER BY). The FROM
  clause is untouched (base tables / joins / derived tables are the SP33 join read
  path's job).
- `resolve_expr` recurses bottom-up; the four subquery nodes fold:
  - **scalar** → `Const { value, ty }` — run via `exec::select_to_relation`; the
    column count + type come off `relation.scope` (`width()` / `ty_at(0)`); 0 rows →
    typed NULL; >1 row → `21000`; ≠1 column → `42601`.
  - **EXISTS** → `Const { Bool(!rows.is_empty()), Bool }`.
  - **IN-subquery** → `InList { expr, list: [Const…], negated }` (reuses SP28's
    three-valued `eval_in_list`).
  - **quantified** → an `OR` (ANY/SOME) / `AND` (ALL) fold of `Binary(op, lhs,
    Const(elem))`; empty set → `Const(Bool(all))` (ANY→false, ALL→true). The NULL
    three-valued logic falls out of the existing `ops::or`/`and`/`compare`.
- `resolve_in_select` runs at the top of `execute_read`, `execute_read_locking`, and
  `select_to_relation` (so nested subqueries and derived-table subqueries resolve
  recursively, under the outer query's snapshot handles via a `SubCtx` bundle).
- `describe` (extended protocol, no execution) types a scalar-subquery projection
  column via a catalog-only pass over `build_from_schema`'s schema (`infer_type`
  types EXISTS/IN/quantified as boolean directly).

New `ExecError::{CardinalityViolation (21000), SubqueryColumns (42601)}`.

## Cross-range co-location

The router's `collect_select_ranges` now also walks a SELECT's expression clauses
(new `collect_expr_ranges`) in addition to its FROM tree, so a subquery referencing
a table on another range is rejected `0A000` ("cross-range joins or subqueries are
not supported"). Same-range subqueries (and the SP33 same-range-join routing) are
unaffected — the invariant that one statement never spans ranges is preserved.

## Why no Stateright model

Identical to SP27–SP33: an uncorrelated subquery is a pure nested read against the
same engine under the same MVCC snapshot, folded to a constant before the outer row
loop; the whole statement runs inside one `execute_read` on one engine (cross-range
rejected). No new lock, write path, visibility rule, or interleaving. The semantics
(cardinality, single-column, three-valued IN/ANY/ALL, EXISTS, empty-set) are value
properties proven by unit tests. CLAUDE.md's "pure-data / single-node refactor"
carve-out.

## Testing

`pgparser` parser tests; 10 `executor::subquery` unit tests (cardinality, single-
column, three-valued NOT-IN-with-NULL, empty-set ANY/ALL, the describe type pass);
the `executor::subqueries` wire test; a `cluster` router co-location test
(`a_cross_range_subquery_is_rejected_while_colocated_runs`); and
`conformance/corpus/subqueries.sql` (diffed vs PG 18 in CI). The new test binary
`executor::subqueries` is UAC-safe.

## Documented deviations / non-goals

- **Correlated subqueries** — a reference to an outer column inside a subquery
  raises `42703` (the honest deferral; never silent wrong results).
- Subqueries in `UPDATE`/`DELETE`/`INSERT` — read path only (they surface `0A000`).
- Row-valued `(a, b) IN (…)`; `FOR UPDATE` inside a subquery (`0A000`); cross-range
  subqueries (`0A000`).
- A scalar subquery nested inside a scalar function / `CASE` / predicate in the
  **projection** is not type-substituted by the extended-protocol `Describe` type
  pass (returns `0A000` on Describe-without-execute of that shape) — the simple-query
  execution path is exhaustive and computes the value correctly.
