# SP27 — aggregates + GROUP BY (SQL breadth wave 1)

**Date:** 2026-06-16
**Status:** Approved (design)

## Problem / motivation

SP16–SP26 spent eleven consecutive slices on distribution *depth* — cross-range 2PC
fault recovery — and closed it out (the residual deferrals are narrow: lagging-follower
read staleness and range-0-as-participant). The program roadmap is explicit that
sub-projects "alternate between depth (distribution) and breadth (SQL surface)." After
eleven depth slices the honest next move is breadth, and the most foundational breadth
gap after basic DML is **aggregation**: today a `SELECT` can scan, filter (`WHERE`),
sort (`ORDER BY`), and `LIMIT` a single table, but there is no `GROUP BY`, no `HAVING`,
and no aggregate functions. Analytics — the single largest class of real SQL — is
entirely absent.

This slice adds aggregate functions and grouping:

- `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)`
- `SUM(expr)`, `MIN(expr)`, `MAX(expr)` (and their `DISTINCT` forms)
- `GROUP BY <expr-list>`
- `HAVING <predicate>`
- Whole-table aggregation (aggregates with no `GROUP BY` → one row)
- Correct interaction with the existing `WHERE` (filter *before* grouping), `ORDER BY`
  (order the group rows, over grouping exprs and aggregates), and `LIMIT`.

## Scope decisions (and why)

### Why not a Stateright model

CLAUDE.md mandates an exhaustive Stateright model for **"anything touching 2PC,
replication, recovery, leadership, locking, MVCC visibility, or cross-range
consistency,"** and explicitly carves out that **"a pure-data or single-node refactor
with no concurrency/fault dimension may not warrant one."**

Aggregation is exactly that pure-data case, and it is *structurally* single-range:

- A whole table maps to a single range (`RangeMap::range_for_table`), and a table-bearing
  `SELECT` is routed to that one range's leader (`range::router::dispatch` →
  `run_on`). Aggregation therefore executes inside one `execute_read` on one engine —
  there is **no cross-range scatter/gather**, so no distributed dimension to model.
- The aggregate is a **deterministic fold over the already-correct visible row set**.
  Snapshot isolation and the at-most-one-live MVCC invariant — which *do* have models
  (SP24 `mvcc_write_conflict_model`) and a runtime `debug_assert!` in `scan_live` — fix
  that set before aggregation runs. Aggregation introduces no new lock, no new write, no
  new visibility rule, and no new interleaving; it cannot violate an invariant those
  layers already uphold.

A Stateright model of a pure fold would have a one-dimensional, interleaving-free state
space — it would restate the unit tests, not find an adversarial ordering. So this slice
**deliberately ships no model**, consistent with the carve-out, and instead over-invests
in deterministic empirical proof (unit + integration + the differential conformance
oracle). This decision is called out here precisely because the default expectation is a
model; the justification is the single-range/pure-data nature, not expedience.

### AVG is deferred

PostgreSQL's `avg(int)` and `sum(int8)` return `numeric`; `avg` is inherently
fractional. crabgresql has no `numeric`/floating type yet (only `int4`, `int8`, `text`,
`bool`). Implementing `AVG` now would force either a wrong integer-truncating result or a
fake type — a parity lie. `AVG` is therefore **deferred to the slice that introduces a
`numeric`/float type**, and is a documented non-goal here. `COUNT`/`SUM`/`MIN`/`MAX` all
have exact, PG-faithful results within the existing type system (see Result types).

### Result types (PG parity within our type system)

| Aggregate | Input | crabgresql result | PostgreSQL | Parity note |
|---|---|---|---|---|
| `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)` | any | `int8` | `bigint` | exact |
| `SUM(int4)` | `int4` | `int8` | `bigint` | exact |
| `SUM(int8)` | `int8` | `int8` | `numeric` | **deviation**: in-range sums print identically; out-of-`i64` → `22003` where PG would not overflow. Accepted until `numeric` exists. |
| `MIN(x)` / `MAX(x)` | `int4`/`int8`/`text`/`bool` | same as input | same as input | exact |

`SUM` accumulates in a checked `i64` (so `SUM(int4)` never overflows prematurely at
`int4` width); overflow past `i64` raises `22003`. `SUM` of a non-integer argument is
`42883` ("function does not exist"). `MIN`/`MAX` accept any of the four comparable types.

### Semantics (PostgreSQL-faithful)

- **Aggregate query detection.** A `SELECT` is an *aggregate query* iff it has a
  `GROUP BY`, a `HAVING`, or an aggregate call anywhere in the projection or `ORDER BY`.
- **Grouping key equality.** Rows group by the tuple of `GROUP BY` expression values;
  `NULL` groups with `NULL` (grouping uses *not-distinct*, unlike `WHERE`'s `=`). Output
  group order is first-appearance of the group (deterministic given `scan_live`'s
  rowid-sorted scan); tests that diff against PG use `ORDER BY` for a defined order.
- **Empty input.** No `GROUP BY` + zero input rows → still **one** row
  (`COUNT`=0, `SUM`/`MIN`/`MAX`=`NULL`). With `GROUP BY` + zero rows → **zero** rows.
- **NULL handling.** `COUNT(*)` counts all rows; `COUNT(x)`/`SUM`/`MIN`/`MAX` ignore
  `NULL` arguments. An all-`NULL` (or empty) group yields `COUNT`=0, others `NULL`.
- **`DISTINCT`.** `agg(DISTINCT x)` folds only the distinct non-null argument values
  (`MIN`/`MAX(DISTINCT x)` ≡ `MIN`/`MAX(x)`; the keyword is accepted and is a no-op for
  those two).
- **Validation (`42803`).** In an aggregate query, every projection / `HAVING` /
  `ORDER BY` expression must be built only from aggregate calls, the `GROUP BY`
  expressions (matched structurally — `GROUP BY a` makes `a` and `a+1` valid), and
  constants. A bare column that is neither grouped nor inside an aggregate →
  `42803` ("column must appear in the GROUP BY clause or be used in an aggregate
  function"). Validation is data-independent (errors even on an empty table). Nested
  aggregates and aggregates inside `GROUP BY` → `42803`.
- **`HAVING`** is evaluated per group (after folding); a group whose `HAVING` is false or
  `NULL` is dropped. `HAVING` may reference aggregates and grouping exprs.
- **`FOR UPDATE/SHARE` + aggregation** → `0A000` (PostgreSQL: "FOR UPDATE is not allowed
  with aggregate functions / GROUP BY clause").
- **Unknown function.** Any function call whose name is not a known aggregate →
  `42883`. (No scalar functions exist yet; that is a later breadth wave.)

## Components

- **A. Parser/AST (`pgparser`).** New keywords `GROUP`, `HAVING`, `DISTINCT`, `ALL`
  (lexer + `token`). `SelectStmt` gains `group_by: Vec<Expr>` and `having: Option<Expr>`.
  A new `Expr::Func(FuncCall)` (`FuncCall { name, distinct, args: FuncArgs }`,
  `FuncArgs ∈ { Star, Exprs(Vec<Expr>) }`) parsed from `ident '(' …')'` in the Pratt
  prefix position. `GROUP BY`/`HAVING` parse between `WHERE` and `ORDER BY`.
- **B. Types (`pgtypes`).** Derive `Eq + Hash` on `Datum` (sound — no float/NaN), so it
  can key grouping maps and `DISTINCT` sets.
- **C. Aggregate executor (`executor::agg`).** A new module: aggregate-function registry,
  per-group accumulators (`Count`/`Sum`/`Min`/`Max`, each with an optional `DISTINCT`
  set), insertion-ordered grouping, the grouped-expression validator + evaluator, and
  `execute_aggregate(select, table, rows) -> QueryResult`. `eval::eval` and
  `eval::infer_type` learn an `Expr::Func` arm (aggregate result types for
  RowDescription; `42883`/`42803` otherwise). `exec::execute_read` routes aggregate
  queries to `execute_aggregate` after `WHERE`; `execute_read_locking` rejects
  aggregation with `0A000`. New `ExecError::Grouping`(`42803`) /
  `UndefinedFunction`(`42883`).
- **D. Conformance corpus.** `crates/conformance/corpus/aggregates.sql` — `COUNT/SUM/
  MIN/MAX`, `GROUP BY`, `HAVING`, `DISTINCT`, empty-input, and the `42803`/`42883`
  error cases, all `ORDER BY`-stable, diffed against real PG 18 in CI.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | Parser accepts the new grammar (aggregate calls incl. `*`/`DISTINCT`, `GROUP BY`, `HAVING`) and rejects malformed forms. | `pgparser` unit tests. |
| 2 | Each aggregate folds correctly incl. `NULL`-skip, `DISTINCT`, empty/all-null group, `SUM` overflow → `22003`. | `executor::agg` unit tests. |
| 3 | `GROUP BY` groups (incl. `NULL` grouping), `HAVING` filters groups, `ORDER BY`/`LIMIT` over groups, empty-table behaviors (one row bare / zero rows grouped). | `executor::agg` unit tests + `executor` integration test over the wire. |
| 4 | Validation: ungrouped column `42803`; unknown function `42883`; `FOR UPDATE` + agg `0A000`; nested aggregate `42803`. | `executor` unit/integration tests (assert SQLSTATEs). |
| 5 | RowDescription (extended protocol `Describe`) reports the correct aggregate result types without executing. | `executor` integration test (binary results / `describe`). |
| 6 | Differential parity against PostgreSQL 18 for the aggregate surface. | `conformance/corpus/aggregates.sql` (CI diff; matches for in-range values). |
| 7 | No regression of the existing scan/filter/sort/DML/2PC suites. | full `cargo nextest run --workspace` + doctests. |

## Success criteria

1. `COUNT/SUM/MIN/MAX`, `GROUP BY`, `HAVING`, and `DISTINCT` work end-to-end over the wire
   with PG-faithful semantics (within the documented type-system deviations). — (A–C)
2. The validation and error surface matches PostgreSQL SQLSTATEs. — (#4)
3. The conformance corpus diffs clean against PG 18 for the in-range aggregate surface. — (#6)
4. No regression. — (#7)

## Non-goals (deferred)

- **`AVG`** and any fractional/`numeric` aggregate — pending a `numeric`/float type slice.
- **`GROUPING SETS`/`ROLLUP`/`CUBE`**, `FILTER (WHERE …)`, ordered-set/`WITHIN GROUP`
  aggregates, `DISTINCT` at the `SELECT` level, window functions.
- **Scalar (non-aggregate) functions** — a separate breadth wave.
- **Cross-range aggregation.** Not reachable today (a table lives on one range); becomes
  relevant only once range splits put one table's rows on multiple ranges, at which point
  partial-aggregate merge gets its own slice (and, being distributed, its own model).
