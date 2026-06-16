# SP33 — JOINs (same-range) (SQL breadth wave 7)

**Date:** 2026-06-16
**Status:** Approved (design)

## Problem / motivation

SP27–SP32 were six SQL-breadth waves that grew the single-table query surface
(aggregates, predicates, scalar functions, `float8`, casts, `numeric`). Every one
of them executes over the rows of **one** table: `SelectStmt.from` is an
`Option<String>` — a single table name — and `eval` resolves `Expr::Column(name)`
by index against one `catalog::Table`. The query language has no way to combine
two tables.

This slice adds **SQL joins**: the full join surface a real query workload needs —
`INNER JOIN … ON`, `LEFT`/`RIGHT`/`FULL [OUTER] JOIN … ON`, `CROSS JOIN`, the
comma form (`FROM a, b WHERE …`), `USING (cols)`, `NATURAL JOIN`, **subqueries in
`FROM` (derived tables)**, table aliases (`a` / `a AS x` / `a x`), qualified column
references (`a.col`, `a.*`), self-joins, and multi-way joins (`a JOIN b JOIN c`).

**Scope: same-range only.** Tables map to ranges by `table_id`
(`RangeMap::range_for_table`). A join whose tables all live on one range executes
entirely inside one engine's `execute_read`, exactly like every prior breadth wave.
In the default single-range deployment (`RangeMap::single()`) every table is on
range 0, so **all** joins work. A join spanning two ranges is rejected with a clear
`0A000` — cross-range *distributed* join execution (scatter/ship/shuffle) is a
separate distribution-depth effort, not a breadth wave.

## Scope decisions (and why)

### Why not a Stateright model

Same carve-out as SP27–SP32: a join is a **pure relational fold** over data. All
referenced tables are scanned under **one statement snapshot** (already how
`execute_read` works — see `crate::exec::execute_read`), so the cross-table view is
consistent by construction; nested-loop join, NULL-extension for outer joins,
`USING`/`NATURAL` column coalescing, and derived-table materialization are all
deterministic transforms of the already-correct, MVCC-visible row sets, composed
inside one `execute_read` on one engine. No new lock, write path, visibility rule,
or interleaving is introduced — correctness is a *relational-algebra* property,
proven by unit tests plus the differential conformance oracle. A `Model` of a join
would have an interleaving-free state space and merely restate the unit tests
(CLAUDE.md's "pure-data / single-node refactor with no concurrency/fault dimension
may not warrant one" carve-out).

### Nested-loop join (not hash)

The executor joins two relations with a **nested-loop**. It is correct-by-
construction for every join type and *arbitrary* `ON` predicates (not just
equi-joins), handles outer-join NULL-extension naturally, and is the simplest thing
that is faithful. A hash-join fast path for equi-joins is a performance
optimization with no observable behavioral difference, so it is deferred — the
conformance corpus and tests assert *results*, not plans or timing.

### The `Relation` / `Scope` abstraction (and the `Expr::Column` change)

A join and a derived table both *produce* and *consume* the same thing: a set of
rows with a named, ordered schema. The slice introduces that as the unifying
abstraction rather than special-casing each:

- `ColumnBinding { qualifier: Option<String>, name: String, ty: ColumnType }`.
- `Scope { columns: Vec<ColumnBinding> }` — the ordered schema of a relation;
  resolves a column reference to a flat row index.
- `Relation { scope: Scope, rows: Vec<Vec<Datum>> }` — a combined row aligns
  positionally with `scope.columns`.

This forces a change at the root of expression evaluation: `Expr::Column(String)`
becomes `Expr::Column { table: Option<String>, name: String }` (qualified refs),
and everything that threads `table: Option<&Table>` today
(`eval`, `agg`'s four `Expr` traversals, `infer_type`, `exec`, `describe`) takes a
`&Scope` instead. This is wide but mechanical, and is the correct long-term shape —
the alternative (synthesizing a transient `catalog::Table` with dotted column names)
cannot represent derived-table schemas, makes ambiguity handling awkward, and turns
`USING`/`NATURAL` column-merging into string surgery.

### Column-resolution error surface (PG-faithful)

- Unqualified `name`: **0** matching bindings → `42703` (undefined column); **>1** →
  `42702` (ambiguous column); exactly 1 → its flat index.
- Qualified `t.name`: qualifier `t` **not in scope** → `42P01` ("missing FROM-clause
  entry for table `t`"); in scope but no such column → `42703`.

### `USING` / `NATURAL` output shape

PostgreSQL's rule, faithfully: a `USING`/`NATURAL` join column appears **once** in
the output, **coalesced** (`COALESCE(left, right)` — matters for outer joins),
positioned **first** (join columns in `USING` order / common-name order), then the
remaining left columns, then the remaining right columns. The merged column is
**unqualified** (referenceable as bare `col`, not `a.col`). `NATURAL` joins on all
columns sharing a name between the two inputs (a `NATURAL` join with no common
column degenerates to a cross join, per PG).

### Distribution: route or reject (no 2PC)

A `SELECT` is read-only and writes no `Prepared(→ g)` markers, so a join never
escalates to cross-range 2PC. `RangeRouter::pinning_range` walks the whole `FROM`
tree (recursing through join nodes and derived subqueries), collects every base
table name, and resolves each to a range: all on one range → route there; spanning
ranges → `0A000` "cross-range joins are not supported". The stale router comment
("the grammar has no joins and every DML carries one table") is corrected.

### Documented deviations / non-goals

- **Cross-range joins** → `0A000` (distributed join execution is out of scope).
- **Scalar / `IN` / correlated subqueries** in `WHERE`/`SELECT`/`HAVING`, and
  **`LATERAL`** derived tables — deferred. Derived tables here are independent
  (non-correlated): the subquery cannot reference the outer query's tables.
- **`UPDATE`/`DELETE` with a `FROM`/`USING` join** — deferred (this slice is
  read-path only).
- **Hash join / any join optimization** — nested-loop only.
- A derived table's **alias is required** (PG rule: `subquery in FROM must have an
  alias`).
- An `a.col` projection's output column is named **`col`** (PG names it after the
  column, not the qualifier).
- Lexer corner: `t.5` lexes as `t` then the float `.5` (a syntax error downstream),
  matching PG's rejection of `t.5`; `a.col` is the clean common path.

## Components

- **A. Lexer / tokens (`pgparser`).** New `Token::Dot` (a `.` that does not begin a
  number lexeme). New keywords `JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS, ON,
  USING, NATURAL` (`AS` already exists). The numeric lexer is unchanged — it already
  claims `.` only for `.5` / `2.` forms.
- **B. AST (`pgparser::ast`).** `Expr::Column(String)` → `Expr::Column { table:
  Option<String>, name: String }`. `SelectStmt.from: Option<String>` → `from:
  Vec<TableExpr>` (comma items = implicit cross-join; each item a join tree). New
  `TableExpr` (`Table { name, alias }` | `Derived { subquery: Box<SelectStmt>,
  alias: String }` | `Join { left, right, kind, constraint }`), `JoinKind`
  (`Inner`/`Left`/`Right`/`Full`/`Cross`), `JoinConstraint`
  (`On(Expr)`/`Using(Vec<String>)`/`Natural`/`None`). `SelectItem` gains
  `QualifiedWildcard(String)` for `a.*`.
- **C. Parser (`pgparser::parser`).** `parse_from()` parses a comma list of
  left-associative join trees; a table-factor is a base table (`t` / `t AS x` /
  `t x`), a derived table (`(SELECT …) alias`), or a parenthesized join. `JOIN`
  binds tighter than comma. Join prefixes `[INNER | LEFT|RIGHT|FULL [OUTER] |
  CROSS | NATURAL] JOIN factor [ON expr | USING (cols)]`. The atom parser consumes
  `ident . ident` (qualified column) and the projection parser consumes `ident . *`.
- **D. Executor — scope/relation (`executor`).** New `Scope`/`ColumnBinding`/
  `Relation`. `eval`/`infer_type`/`exec`/`agg`/`describe` change from
  `Option<&Table>` to `&Scope`. `Expr::Column` resolution implements the 42703/
  42702/42P01 surface. `SELECT *` expands all bindings in scope order; `a.*`
  expands one qualifier's bindings.
- **E. Executor — join (`executor::join`).** `build_relation(&[TableExpr])` folds
  comma items as cross joins; `build_table_expr` builds a base table (`scan_live` +
  catalog schema qualified by alias-or-name), a derived table (recursive read →
  rows + inferred schema, qualified by its alias), or a `Join` (recurse, then
  `join_relations`). `join_relations` is the nested-loop with per-kind emission and
  the `On`/`Using`/`Natural`/cross predicate; `USING`/`NATURAL` coalesce + reorder
  per PG. WHERE/GROUP/HAVING/projection/ORDER/LIMIT then run over the final relation
  unchanged.
- **F. Router (`cluster::range::router`).** `pinning_range(Select)` walks the FROM
  tree, dedups ranges, routes or `0A000`. `describe` builds the Scope from the
  catalog via a shared scope-builder. Corrected comment.
- **G. Conformance.** `crates/conformance/corpus/joins.sql` — every join type, the
  comma form, `USING`/`NATURAL`, aliases, qualified refs, self-joins, multi-way,
  derived tables, NULL-extension, and the `42702`/`42703`/`42P01`/`0A000` error
  surface, diffed against real PostgreSQL in CI.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | `Dot` token; `a.col` lexes; `.5`/`2.` floats preserved; `t.5` corner. | `pgparser` lexer unit tests. |
| 2 | Parse every join type, comma form, `ON`/`USING`/`NATURAL`, `AS`/bare aliases, derived tables, multi-way, self-join, `a.*`, JOIN-binds-tighter-than-comma. | `pgparser` parser unit tests. |
| 3 | Parser parity with PostgreSQL's grammar over the join surface. | `pgparser` libpg_query oracle test. |
| 4 | Scope resolution: unqualified unique / 42703 / 42702; qualified / 42P01 / 42703; `*` and `a.*` expansion in scope order. | `executor` scope-resolution unit tests. |
| 5 | Nested-loop per kind (INNER/LEFT/RIGHT/FULL/CROSS); outer-join NULL-extension; `USING`/`NATURAL` coalesce + column order; comma cross-join. | `executor::join` unit tests. |
| 6 | Derived tables (materialize + schema), multi-way joins, self-joins, aggregates/GROUP BY/ORDER BY/DISTINCT composed over a joined relation. | `executor::join` + `executor::{agg,eval}` unit tests. |
| 7 | End-to-end over the wire: each join type, qualified projection, USING/NATURAL, derived tables, self/multi-way joins, result row description, and the 42702/42703/42P01 error surface. | `executor::joins` integration test. |
| 8 | Router routes a same-range join to its range and rejects a cross-range join `0A000`; `describe` resolves joined field types. | `cluster::range::router` unit tests. |
| 9 | Differential parity against PostgreSQL for the join surface (diffed vs PG 18 in CI; validated locally vs real PG if available). | `conformance/corpus/joins.sql`. |
| 10 | No regression; `pgparser` stays mutation-clean (zero-survivor baseline crate) under the new `Column`/FROM grammar. | full `cargo nextest run --workspace` + doctests; `cargo mutants` on `pgparser`. |

## Success criteria

1. `INNER`/`LEFT`/`RIGHT`/`FULL [OUTER]`/`CROSS` joins, the comma form, `USING`,
   `NATURAL`, table aliases, qualified column refs (`a.col` / `a.*`), self-joins,
   multi-way joins, and non-correlated derived tables work end-to-end over the wire
   with PG-faithful semantics (incl. three-valued `ON` matching and outer-join
   NULL-extension). — (A–F)
2. The column-resolution error surface matches PostgreSQL SQLSTATEs
   (`42702`/`42703`/`42P01`), and a cross-range join is `0A000`. — (#4, #7, #8)
3. The conformance corpus diffs clean against PostgreSQL for the join surface. — (#9)
4. No regression across the workspace; `pgparser` remains mutation-clean. — (#10)

## Non-goals (deferred)

- **Cross-range distributed join execution** (scatter/ship/shuffle) — a same-range
  join routes/works; a cross-range join is `0A000`.
- **Scalar / `IN` / correlated subqueries** in `WHERE`/`SELECT`/`HAVING`, and
  **`LATERAL`** derived tables — derived tables here are independent only.
- **`UPDATE`/`DELETE` with a join** (`FROM`/`USING`) — read path only.
- **Hash join / join reordering / any optimization** — nested-loop only.
- Sending per-column `table_oid`/`column_id` in `RowDescription` for joined columns
  (the text-diffing oracle does not check it).
