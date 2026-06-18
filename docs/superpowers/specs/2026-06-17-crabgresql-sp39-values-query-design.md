# SP39 - VALUES query expressions and derived row sources

**Date:** 2026-06-17
**Status:** Approved (design)

## Problem / motivation

SP38 added top-level set operations but deliberately deferred `VALUES` branches
because crabgresql does not yet have a `VALUES` query form. PostgreSQL treats
`VALUES` as a query expression: it can stand alone, feed set operations, and appear as
a derived table in `FROM`. Adding it is a compact SQL breadth slice that improves
interactive ergonomics and gives future features a simple row-source primitive.

This slice adds `VALUES` as a first-class query body instead of lowering it to a fake
`SELECT` or special-casing it in only one execution path. That keeps set operations,
derived tables, extended-protocol describe, and future query-expression work on the
same shape.

## Scope

**In:**
- Standalone `VALUES (expr [, ...]) [, ...]` statements.
- `ORDER BY` / `LIMIT` / `OFFSET` on a standalone `VALUES` result.
- `VALUES` as a branch in `UNION` / `INTERSECT` / `EXCEPT`, including `[ALL]`.
- `FROM (VALUES (...), (...)) AS alias(col1, col2, ...)` as a non-correlated derived
  table row source.
- PostgreSQL-style default output names: `column1`, `column2`, ...
- Per-column common-type resolution across all rows, including SP38-style handling for
  bare `NULL` and unknown string literals.
- Extended-protocol `Describe` support without evaluating the row expressions.

**Deferred / non-goals:**
- Correlated `VALUES` in a derived table, such as `FROM t, (VALUES (t.id)) v(x)`.
- `INSERT ... SELECT` and `INSERT ... VALUES ... RETURNING`.
- General CTE/query-expression cleanup beyond the minimal AST generalization this
  slice needs.
- Plain non-set-op `SELECT ORDER BY 1` positional ordering. This slice keeps that
  behavior unchanged and scopes positional ordering to `VALUES` and set-operation
  result paths.
- Cross-range distributed execution beyond the existing single-range router invariant.

## Approach

Use a first-class query-body representation:

```rust
pub enum QueryBody {
    Select(Box<SelectStmt>),
    Values(ValuesStmt),
}

pub struct ValuesStmt {
    pub rows: Vec<Vec<Expr>>,
}

pub enum SetExpr {
    Query(QueryBody),
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}
```

A plain `SELECT` with no set operator continues to collapse to
`Statement::Select(SelectStmt)`. A plain `VALUES` statement becomes a values-query
statement carrying the body plus result-level `ORDER BY` / `LIMIT` / `OFFSET`.
Set-operation leaves become query bodies, so `VALUES` and `SELECT` share the same
combine path.

Rejected alternatives:
- **Standalone-only `Statement::Values`:** smaller initially, but it duplicates
  relation-building when the same rows need to appear in set operations and derived
  tables.
- **Rewrite `VALUES` to synthetic `SELECT`s:** superficially compact, but awkward for
  default column names, row arity checks, common-type inference, derived-table aliases,
  and PostgreSQL-compatible errors.

## Parser

- Add a `VALUES` keyword.
- Parse `VALUES (expr [, ...]) [, ...]` into `ValuesStmt`. The parser enforces that
  there is at least one row and each row has at least one expression. Cross-row arity
  is validated by the executor/type pass so the error can use the same SQLSTATE as
  PostgreSQL's analysis error.
- Extend query-primary parsing so a primary may be `SELECT`, `VALUES`, or a
  parenthesized query expression.
- Extend set-operation parsing so either `SELECT` or `VALUES` may appear on either
  side of `UNION` / `INTERSECT` / `EXCEPT`.
- Extend derived-table parsing so `FROM (VALUES (...)) AS v(...)` is accepted. The
  alias remains required, matching the existing derived-table rule.
- Keep existing `SELECT` parsing behavior intact for all non-`VALUES` statements.

## Executor

Add `executor::values`, centered on two helpers:

- `values_to_relation(..., &ValuesStmt, &EvalCtx) -> Relation`
- `describe_values(..., &ValuesStmt) -> RelationSchema`

The execution helper:
1. Determines the column count from the first row and rejects any row with a different
   count using `42601`.
2. Infers one common output type per column across every row. Bare `NULL` and string
   literals participate as PostgreSQL-style `unknown` values: they resolve to the
   concrete peer type when possible, and an all-unknown column resolves to `text`.
3. Evaluates row expressions through the existing scalar evaluator under the current
   statement `EvalCtx`.
4. Coerces each evaluated cell to the resolved column type using existing cast logic.
5. Builds a `Relation` with field names `column1`, `column2`, ... and the resolved
   column types.

`Describe` uses the same column-count and type-resolution rules, but does not evaluate
the expressions.

## Integration

- **Standalone statements:** `session.rs` routes a `VALUES` query through the same
  read/statement context used by `SELECT`: one statement timestamp, one snapshot, no
  write transaction.
- **Set operations:** `executor::setops` evaluates `QueryBody::Select` through the
  existing `select_to_relation` path and `QueryBody::Values` through
  `values_to_relation`; the combine logic stays unchanged after leaf materialization.
- **Derived tables:** `TableExpr::Derived` is generalized to hold a query body. A
  derived `VALUES` relation is built with `values_to_relation`; table aliases and
  optional column aliases override exposed names in the surrounding scope.
- **Ordering:** standalone `VALUES` applies result-level `ORDER BY` / `LIMIT` /
  `OFFSET` after row materialization. Positional `ORDER BY` works for values-query
  results and set-operation results.
- **Router:** a standalone `VALUES` query is range-neutral and can run on the current
  gateway/default engine, like a FROM-less `SELECT`. A `VALUES` derived table inherits
  the surrounding query's range decision. `VALUES` branches in a set operation add no
  table ranges; any `SELECT` branches still drive the existing co-location check.

## Semantics and errors

- Row expressions evaluate left-to-right in row order.
- Default output names are `column1`, `column2`, and so on.
- A derived-table alias may rename output columns:
  `SELECT id FROM (VALUES (1)) AS v(id)`.
- Column-count mismatches return `42601` with PostgreSQL-like wording.
- Incompatible common types return the existing `42804` type-mismatch surface.
- Failed unknown-literal coercions return the existing conversion SQLSTATE, such as
  `22P02` for invalid integer input.
- Derived-table column-alias count mismatches return `42601`
  (`syntax_error`) with PostgreSQL-like wording.
- Unsupported correlation fails as an ordinary unresolved-column error.

## Testing

- **Parser tests:** standalone `VALUES`; multi-row and multi-column forms; `VALUES`
  in each set-operation position; parenthesized values queries; derived-table aliases;
  malformed syntax rejection.
- **libpg_query oracle:** add accepted grammar forms for standalone `VALUES`, set-op
  branches, and derived-table usage. Analysis errors such as row arity mismatches stay
  executor tests, not grammar-oracle rejected entries.
- **Executor unit tests:** relation building; row arity error; default column names;
  common-type resolution; unknown `NULL`/string behavior; bad coercions; describe
  schema; alias renaming.
- **Wire test:** new `executor::values_query` target covering standalone `VALUES`,
  `ORDER BY` / `LIMIT` / `OFFSET`, `VALUES UNION SELECT`, and
  `SELECT ... FROM (VALUES ...) AS v(...)`. The target name is UAC-safe.
- **Cluster/router test:** `VALUES` alone is range-neutral; set operations containing
  values and a single table range route correctly; set operations whose `SELECT`
  branches span ranges remain rejected `0A000`.
- **Conformance corpus:** add `crates/conformance/corpus/values.sql`, validated
  locally against PostgreSQL with explicit `ORDER BY` whenever row order matters.

## Why no Stateright model

This is a pure query parsing and expression-evaluation slice. It adds no lock path,
write path, MVCC visibility rule, recovery behavior, leadership interaction, or
cross-range interleaving. Every row is evaluated under one statement context on one
engine, and cross-range table access remains governed by the existing router checks.
Unit, wire, router, and PostgreSQL-oracle conformance tests are the right proof shape.

## File-by-file change list

- `crates/pgparser/src/{token.rs,lexer.rs}` - add `VALUES` keyword support.
- `crates/pgparser/src/ast.rs` - add `ValuesStmt` and a query-body abstraction; update
  set-operation and derived-table AST nodes.
- `crates/pgparser/src/parser.rs` - parse values rows, query primaries, set-op values
  branches, and derived-table values queries.
- `crates/executor/src/values.rs` - new relation builder and describe/type pass.
- `crates/executor/src/setops.rs` - evaluate `VALUES` leaves via `values_to_relation`.
- `crates/executor/src/exec.rs` - route derived-table values queries and describe
  standalone values queries.
- `crates/executor/src/session.rs` - dispatch standalone `VALUES`.
- `crates/executor/src/error.rs` - add or reuse analysis errors for row and alias
  arity.
- `crates/cluster/src/range/router.rs` - treat standalone/derived values as
  range-neutral while preserving set-op co-location checks.
- `crates/executor/tests/values_query.rs` - new wire test target.
- `crates/conformance/corpus/values.sql` - new PostgreSQL parity corpus.
- `CLAUDE.md` - SP39 completion note and UAC-safe target audit when implementation
  ships.

## Design checks

- No silent behavior changes to plain `SELECT`; it remains on the established
  `Statement::Select` path.
- `VALUES` has one relation-building implementation shared by standalone statements,
  set operations, and derived tables.
- Unknown-literal resolution follows the SP38 set-operation behavior instead of
  introducing a second common-type implementation.
- The router continues to reject multi-range table access and treats table-free values
  rows as range-neutral.
