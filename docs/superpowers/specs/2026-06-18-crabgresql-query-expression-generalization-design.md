# crabgresql query-expression generalization design

Date: 2026-06-18

## Summary

Generalize crabgresql's row-producing SQL expressions so top-level queries,
derived tables, and uncorrelated expression subqueries all share one AST and
executor contract. Today `SELECT`, standalone `VALUES`, and set operations run
through parallel top-level paths, while nested positions still mostly accept only
plain `SELECT` or bare `VALUES`. This slice replaces those parallel paths with a
single query-expression model that can represent:

- plain `SELECT`
- standalone `VALUES`
- `UNION` / `INTERSECT` / `EXCEPT` trees with `VALUES` or `SELECT` leaves
- derived table query expressions
- uncorrelated scalar / `IN` / `EXISTS` / `ANY` / `ALL` query expressions

The observable goal is PostgreSQL-compatible composition for query expressions
without opening larger feature fronts such as CTEs, correlated subqueries,
windows, collations, or distributed cross-range query execution.

## Goals

- Replace top-level `Statement::Select`, `Statement::Values`, and
  `Statement::SetOperation` with one `Statement::Query(QueryExpr)` shape.
- Allow set-operation and `VALUES` query expressions in derived table positions:
  `FROM (SELECT ... UNION SELECT ...) AS t` and
  `FROM (VALUES (...) ORDER BY 1 LIMIT 1) AS v`.
- Allow set-operation and `VALUES` query expressions in uncorrelated expression
  subquery positions: scalar subqueries, `IN`, `EXISTS`, and `ANY` / `ALL`.
- Preserve one statement snapshot and existing linearizable-read gates for all
  nested query expression execution.
- Preserve existing PostgreSQL-compatible error classes for set operations,
  scalar subqueries, derived column aliases, and cross-range rejection.
- Consolidate describe, routing, and execution around one query-expression
  relation contract.

## Non-Goals

- CTEs / `WITH`.
- Correlated subqueries. Nested query scopes remain independent; outer references
  continue to fail name resolution.
- Window functions.
- Collations, `NULLS FIRST` / `NULLS LAST`, or ordered-set aggregate work.
- Distributed cross-range query execution. Query expressions that reference more
  than one range still return `0A000`.
- New locking semantics. `FOR UPDATE` / `FOR SHARE` remains supported only for a
  lone plain `SELECT`, as it is today.

## AST Design

Introduce a reusable `QueryExpr` AST node:

```rust
pub struct QueryExpr {
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub locking: Option<RowLockStrength>,
}
```

`SetExpr` remains the recursive set-operation tree:

```rust
pub enum SetExpr {
    Query(QueryBody),
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

pub enum QueryBody {
    Select(Box<SelectStmt>),
    Values(ValuesStmt),
}
```

The `QueryExpr` tail applies to the complete query expression. A plain top-level
`SELECT` is a `QueryExpr` with a single `Select` leaf. A standalone `VALUES` is a
`QueryExpr` with a single `Values` leaf. A set operation is a `QueryExpr` whose
body is a `SetOp` tree. Parenthesized query expressions also carry their own
`QueryExpr` tail, so `(SELECT ... UNION SELECT ... ORDER BY 1 LIMIT 1)` and
`(VALUES (...) ORDER BY 1 LIMIT 1)` are represented directly instead of being
special-cased as unsupported branch tails.

Update statement and nested-expression nodes:

```rust
pub enum Statement {
    Query(QueryExpr),
    // DDL, DML, transaction, and GUC statements unchanged.
}

pub enum Expr {
    ScalarSubquery(Box<QueryExpr>),
    Exists(Box<QueryExpr>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryExpr>,
        negated: bool,
    },
    Quantified {
        expr: Box<Expr>,
        op: BinaryOp,
        all: bool,
        subquery: Box<QueryExpr>,
    },
    // other expression variants unchanged
}

pub enum TableExpr {
    Derived {
        subquery: QueryExpr,
        alias: String,
        columns: Option<Vec<String>>,
    },
    // other table expressions unchanged
}
```

## Parser Design

Route every row-producing query through one `query_expr()` parser. The existing
set-operation precedence rules remain:

- `INTERSECT` binds tighter than `UNION` / `EXCEPT`.
- `UNION` / `EXCEPT` are left-associative.
- `ALL` preserves duplicates; `DISTINCT` is the default.

Parser contexts that currently call `select_inner()`, `values_stmt()`, or
`query_stmt()` change to parse a `QueryExpr`:

- top-level statement dispatch for `SELECT`, `VALUES`, and leading `(`
- derived table factors after `FROM (`
- scalar subquery prefix `(SELECT ... )` and `(VALUES ... )`
- `EXISTS (...)`
- `IN (...)`
- quantified `ANY` / `SOME` / `ALL` subqueries

Parenthesized query expressions may own `ORDER BY` / `LIMIT` / `OFFSET` tails
regardless of whether their body is a lone `SELECT`, a lone `VALUES`, or a
multi-branch set-operation tree. This intentionally retires the current
parenthesized branch-tail deferrals rather than preserving them.

Locking remains special. `FOR UPDATE` / `FOR SHARE` may attach only when the
`QueryExpr` body is a lone `Select` leaf. A locking clause on a set operation,
`VALUES`, or nested query expression is rejected with the existing PostgreSQL-style
unsupported/syntax surface.

The existing parser recursion-depth protections must cover the generalized
query-expression recursion. A flat set-operation chain and nested parenthesized
query expression must still return `54001` rather than constructing an
over-deep AST.

## Executor Design

Introduce one materialization helper:

```rust
query_to_relation(
    catalog_kv,
    kv,
    global,
    gsnap,
    snapshot,
    own,
    &QueryExpr,
    ctx,
) -> Result<Relation, ExecError>
```

This helper returns a `Relation` with final output scope and materialized rows.
All row-producing contexts call this helper:

- top-level `run_query` renders the relation as wire rows
- derived tables call it and then `values::requalify_derived`
- scalar subqueries enforce exactly one output column and at most one row
- `IN` / `ANY` / `ALL` enforce exactly one output column
- `EXISTS` checks whether the relation has any rows

Inside `query_to_relation`:

- A lone plain `SELECT` reuses the current `select_to_relation` logic:
  uncorrelated subquery resolution, FROM construction, filtering, grouping,
  aggregation, projection, `DISTINCT`, ordering, offset, and limit.
- A lone `VALUES` reuses the current `values_to_relation` logic and then applies
  the query tail.
- A `SetOp` tree reuses the current set-operation fold: resolve common output
  columns once, coerce every leaf to those output types, combine rows according
  to the operator and `ALL` flag, then apply the `QueryExpr` tail.

Snapshot and read-gate behavior remain unchanged. The top-level query obtains the
read context once. Nested query expressions use the same `gsnap`, `snapshot`, and
`own` handles as their containing statement.

## Describe Design

Add a schema-only sibling:

```rust
describe_query_expr(catalog_kv, &QueryExpr) -> Result<Vec<FieldDescription>, ExecError>
```

This replaces the current top-level branches for SELECT, VALUES, and set
operations. It also gives derived-table schema construction and scalar-subquery
type resolution one shared path.

The describe path must preserve current type behavior:

- set operations resolve common output types across all branches, including
  unknown-literal handling
- `VALUES` resolves per-column common types and default names
- scalar subqueries in projection are type-resolved without executing
- output column names for set operations come from the leftmost branch

## Routing Design

Replace separate statement walkers with one query-expression range walker:

```rust
collect_query_expr_ranges(router, &QueryExpr, &mut BTreeSet<RangeId>)
```

The walker descends through:

- set-operation leaves
- SELECT FROM clauses
- derived query expressions
- uncorrelated expression subqueries in projection, WHERE, GROUP BY, HAVING, and
  ORDER BY

`VALUES` is range-neutral. Query expressions referencing no tables are unpinned
and route to range 0. Query expressions referencing one range route to that range.
Query expressions referencing more than one range return `0A000`, preserving the
current one-statement-one-range invariant.

## Error Behavior

Reuse existing error variants and SQLSTATEs where possible:

- set-operation column-count mismatch: `42601`
- incompatible set-operation or `VALUES` common types: `42804`
- scalar subquery with multiple columns: `42601`
- scalar subquery with multiple rows: `21000`
- derived column alias count mismatch: `42601`
- cross-range query expression: `0A000`
- unsupported query-expression locking form: existing unsupported/syntax surface

Correlated subqueries remain unsupported by scope isolation, not by a new ad hoc
check. A nested query expression cannot see outer columns, so an outer reference
continues to fail as an undefined column.

## Test Plan

Parser:

- top-level SELECT, VALUES, and set operations all parse to
  `Statement::Query(QueryExpr)`
- `FROM (SELECT ... UNION SELECT ...) AS t`
- `FROM (VALUES (...) ORDER BY 1 LIMIT 1) AS v`
- scalar `(SELECT ... UNION SELECT ...)`
- `IN (VALUES (...))`
- `EXISTS (SELECT ... EXCEPT SELECT ...)`
- `ANY` / `ALL` with set-operation query expressions
- locking accepted only for a lone SELECT query expression
- recursion-depth guard still returns `54001` for deep query-expression nesting

Executor:

- derived set operation returns rows and output column names from the left branch
- derived VALUES with `ORDER BY` / `LIMIT` returns the top-N relation
- scalar set-operation subquery enforces one column and cardinality
- `IN (VALUES (...))`, `EXISTS (set-op)`, and quantified set-op subqueries match
  PostgreSQL truth-table behavior
- nested query expressions run under one statement snapshot and preserve
  read-your-writes
- extended-protocol Describe returns correct OIDs and names for nested query
  expressions

Conformance:

- Add `crates/conformance/corpus/nested_query_expressions.sql`, diffed against
  PostgreSQL 18.

Router:

- same-range nested query expressions run
- cross-range nested query expressions return `0A000`
- VALUES-only nested query expressions remain range-neutral

Regression:

- Existing SELECT, VALUES, set-operation, ORDER BY, aggregate, join, subquery, and
  recursion-guard suites stay green.
- The UAC-safe target-name guard remains empty if a new integration test binary is
  added.

## Model-Checking

No Stateright model is required. This slice is a parser/executor query-expression
refactor over already-existing single-statement read semantics. It introduces no
new write path, lock protocol, MVCC visibility rule, leadership interaction,
recovery behavior, or distributed interleaving. Cross-range query expressions stay
rejected at the router.

## Risks And Mitigations

- **Top-level behavior drift.** Mitigate with regression tests for existing plain
  SELECT, VALUES, and set-operation behavior, including ORDER BY parity.
- **Describe divergence.** Mitigate by using `describe_query_expr` in both
  top-level Describe and nested type-resolution paths.
- **Tail binding mistakes.** Keep PostgreSQL's distinction between a branch tail
  and a whole-query tail explicit in parser tests.
- **Cross-range leak.** Route by walking the whole `QueryExpr`, including nested
  expression subqueries and derived query expressions, before execution.
- **Recursive AST depth.** Preserve parser and executor defense-in-depth guards on
  generalized query-expression trees.
