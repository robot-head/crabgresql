# CTEs / WITH SQL Support

**Date:** 2026-06-18
**Status:** Approved (design)

## Problem / Motivation

crabgresql already has most of the relation-building pieces needed for common
table expressions: `SELECT`, `VALUES`, set operations, derived tables, and
subqueries all materialize through `Relation { scope, rows }`. The missing SQL
surface is `WITH`, which lets users name one or more query results and reference
them from a following query.

This slice adds non-recursive, read-only CTEs for query statements. It should
match PostgreSQL's ordinary CTE visibility behavior while staying inside the
current single-statement, single-range query execution model.

## Scope

**In:**

- `WITH name [(col [, ...])] AS (<query>) <query>` for read-only query bodies.
- CTE bodies may be `SELECT`, `VALUES`, or set-operation query expressions.
- Multiple CTEs in one `WITH` clause.
- Later CTEs may reference earlier CTEs.
- The main query may reference every CTE in its `WITH` clause.
- CTE names shadow base table names within the query.
- Nested `WITH` clauses inside CTE definitions, derived tables, and subqueries.
- CTE output column alias lists.
- Reusing one CTE multiple times in a query.

**Deferred / non-goals:**

- `WITH RECURSIVE` execution.
- Data-modifying CTEs.
- `WITH` before `INSERT`, `UPDATE`, or `DELETE`.
- PostgreSQL materialization hints (`MATERIALIZED` / `NOT MATERIALIZED`).
- Cross-range distributed CTE execution.
- Planner optimizations or CTE inlining.

`WITH RECURSIVE` is parsed so the feature fails intentionally with `0A000`
rather than as an accidental syntax error.

## Design Summary

Use a query-local CTE context. The parser records the `WITH` clause on query
expressions. The executor evaluates CTEs left-to-right into materialized
`Relation`s, stores them in a local map, and resolves `FROM cte_name` against
that map before falling back to the catalog.

This keeps the feature aligned with existing relation evaluation and preserves
the current table-catalog/storage boundaries. A CTE is not a temporary table and
does not touch durable storage.

## Parser And AST

Add `WITH` and `RECURSIVE` keywords.

Add AST nodes:

```rust
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

pub struct Cte {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: QueryExpr,
}

pub enum QueryExpr {
    Select(Box<SelectStmt>),
    Values(ValuesQuery),
    SetOperation(SetQuery),
}
```

`QueryExpr` wraps the already-supported top-level query forms so a CTE body and
a nested query can share one representation. Existing plain queries should keep
their current statement shape where possible: a normal `SELECT` without `WITH`
continues to be `Statement::Select(SelectStmt)`, a standalone `VALUES` continues
to be `Statement::Values(ValuesQuery)`, and a set operation continues to be
`Statement::SetOperation(SetQuery)`.

Attach `with: Option<WithClause>` to the query forms that can be preceded by
`WITH`. If implementation proves cleaner with `QueryExpr { with, body }` instead
of adding a field to every query form, that is acceptable as long as plain
no-`WITH` statement shapes remain stable.

Parsing rules:

- Parse an optional leading `WITH [RECURSIVE]` before a top-level or nested query
  expression.
- Each CTE is `name [(col [, ...])] AS (<query expression>)`.
- A CTE body must be a query expression: `SELECT`, `VALUES`, or a set operation.
- CTE names in the same `WITH` list must be unique.
- `WITH` is supported before `SELECT`, `VALUES`, and parenthesized/set-operation
  query expressions. It is not supported before DML in this slice.

## Execution

Introduce a `CteContext`:

```rust
pub(crate) struct CteContext {
    entries: Vec<(String, Relation)>,
}
```

The concrete data structure can be a `BTreeMap` or insertion-ordered vector. It
must support case-insensitive lookup in the same style as existing identifier
resolution, duplicate detection within one `WITH` list, and child scopes for
nested `WITH` clauses.

Evaluation rules:

1. Start a query with the outer visible CTE context, or an empty context for a
   top-level statement.
2. If the query has a `WITH` clause, reject `recursive: true` with `0A000`.
3. Evaluate each CTE left-to-right.
4. While evaluating CTE `b`, the context contains only outer CTEs plus earlier
   CTEs from the same `WITH` list. This permits references to earlier CTEs and
   rejects forward references as undefined relation/table.
5. Materialize each CTE body once into a `Relation`.
6. Apply the optional CTE column alias list by renaming output columns. The alias
   count must match relation width.
7. Add the materialized relation to the context.
8. Evaluate the main query with the completed context.

`build_table_expr` changes so `TableExpr::Table { name, alias }` checks the CTE
context before `catalog::get_table`. If a CTE matches:

- clone the materialized relation;
- apply the table alias if present, otherwise qualify columns with the CTE name;
- return the requalified relation.

This implements query-local shadowing: a CTE named `users` hides a base table
named `users` inside that query expression.

Reusing a CTE multiple times reuses the materialized relation. The query body is
not re-executed for each reference.

## Nested Query Scope

Derived tables and subquery expressions receive the current `CteContext`, so
these forms work:

```sql
WITH c AS (VALUES (1))
SELECT * FROM (SELECT * FROM c) AS d(x);

WITH c AS (VALUES (1))
SELECT EXISTS (SELECT 1 FROM c);
```

A nested `WITH` creates a child context:

- it starts with the outer visible CTEs;
- it evaluates its own CTEs left-to-right;
- names in the nested `WITH` shadow outer CTE names only for that nested query;
- after the nested query completes, the outer context is unchanged.

## Describe / Schema-Only Paths

The extended-protocol `Describe` path must produce RowDescription for CTE queries
without scanning base tables. It should use a schema-only CTE context built from
the existing schema builders:

- `SELECT` CTE bodies use `build_from_schema` and `resolve_projection`.
- `VALUES` CTE bodies use `describe_values`.
- set-operation CTE bodies use `describe_set_query` or the same underlying
  schema resolver.

The schema context mirrors the execution context but stores relations with empty
rows. This keeps Describe aligned with current schema-only behavior.

## Router / Range Collection

Range collection must understand CTE scopes, not just table names.

Rules:

- Walk CTE definitions left-to-right before the main query.
- A CTE reference resolves against earlier CTEs before the catalog, matching
  executor shadowing.
- `VALUES`-only CTEs are range-neutral.
- The union of ranges referenced by all CTE definitions and the main query must
  contain at most one data range.
- If more than one data range is referenced, reject with `0A000`.

This preserves the existing invariant that one statement runs on at most one data
range.

## Errors

- `WITH RECURSIVE` returns `0A000` unsupported.
- Duplicate CTE names in one `WITH` list return a duplicate-alias style error.
- Forward references fail as undefined relation/table.
- CTE column alias count mismatch returns the existing `42601`-style derived
  alias count error.
- Unsupported `WITH` before DML remains unsupported in this slice.
- Data-modifying CTE bodies are not accepted as query bodies.

Where message text differs from PostgreSQL, conformance should at least pin the
SQLSTATE and the observable query result behavior.

## Why No Stateright Model

This feature is a pure read/query-expression feature inside one statement
snapshot. CTE materialization is a deterministic relation-building step over
already-visible rows, and cross-range execution remains rejected by the router.
The slice adds no write path, lock behavior, MVCC visibility rule, replication
logic, recovery path, or leadership interleaving.

The relevant properties are parser, name-resolution, scope, type/schema, and row
value properties. Unit tests plus PostgreSQL conformance cover those directly,
consistent with prior pure query-expression breadth slices.

## Testing

Parser tests:

- single CTE;
- multiple CTEs;
- CTE column aliases;
- `VALUES` CTE;
- set-operation CTE;
- nested `WITH`;
- duplicate CTE names;
- `WITH RECURSIVE` parses.

Executor tests:

- simple CTE scan;
- later CTE references earlier CTE;
- forward reference error;
- CTE shadows a base table;
- one CTE referenced twice reuses one materialized relation;
- column alias rename;
- column alias count mismatch;
- CTE used inside a derived table;
- CTE used inside a subquery;
- nested `WITH` shadows an outer CTE only inside the nested query.

Router tests:

- same-range CTE plus main query succeeds;
- `VALUES`-only CTE is range-neutral;
- CTE/main-query range union spanning more than one data range is rejected with
  `0A000`;
- CTE shadowing is honored by range collection.

Conformance:

- Add `crates/conformance/corpus/ctes.sql`.
- Use explicit `ORDER BY` whenever row order matters.
- Diff against PostgreSQL 18 in CI.

## Implementation Notes

The implementation should minimize churn in existing no-`WITH` paths. Plain
`SELECT`, `VALUES`, and set-operation statements should continue to exercise the
same statement variants and dispatch arms. New context parameters should thread
through relation-building helpers in a scoped way rather than becoming global
executor state.
