# SP27 plan — aggregates + GROUP BY (SQL breadth wave 1)

Design: `docs/superpowers/specs/2026-06-16-crabgresql-sp27-aggregates-group-by-design.md`.
Branch: `claude/hopeful-dijkstra-8eyy6g`.

First breadth slice after eleven distribution-depth slices. `COUNT/SUM/MIN/MAX`,
`GROUP BY`, `HAVING`, `DISTINCT`. Single-range, pure-data → **no Stateright model** (see
spec "Why not a Stateright model"); proven by unit + integration + the differential
conformance oracle. `AVG` deferred (needs `numeric`).

## Task 1 — Parser/AST (`pgparser`)
- [x] `token.rs`: add keywords `Group`, `Having`, `Distinct`, `All`. `lexer.rs`: map the
  four words (lowercased) in `Keyword::from_word`.
- [x] `ast.rs`: add `Expr::Func(FuncCall)`; `FuncCall { name, distinct, args }`;
  `FuncArgs ∈ { Star, Exprs(Vec<Expr>) }` (Clone/Debug/PartialEq). `SelectStmt` gains
  `group_by: Vec<Expr>` and `having: Option<Expr>`.
- [x] `parser.rs`: in the Pratt `prefix()` `Ident` arm, if `(` follows, parse a function
  call (`f(*)` → `Star`; optional leading `DISTINCT`/`ALL`; comma-separated `Exprs`).
  In `select()`, parse `GROUP BY <expr-list>` then `HAVING <expr>` between `WHERE` and
  `ORDER BY`; set the new `SelectStmt` fields.
- [x] Parser unit tests: `count(*)`, `count(distinct x)`, `sum(a+1)`, `GROUP BY a, b`,
  `HAVING count(*) > 1`, and malformed (`count(distinct *)` rejected).

## Task 2 — Types (`pgtypes`)
- [x] Derive `Eq, Hash` on `Datum` (sound: no float). Keep a one-line comment on why.

## Task 3 — Aggregate executor (`executor::agg`) + eval/exec wiring
- [x] `error.rs`: add `ExecError::Grouping(String)`→`42803`,
  `UndefinedFunction(String)`→`42883`; map in `into_pg`.
- [x] `eval.rs`: `Expr::Func` arm in `eval` (aggregate-in-scalar-context → `42803`;
  unknown → `42883`) and in `infer_type` (`count/sum`→`int8`, `min/max`→`infer_type(arg)`;
  `sum` non-integer arg → `42883`; unknown → `42883`). `derived_name`(Func)=lowercased name.
- [x] New `agg.rs`:
  - `AggFunc { Count, Sum, Min, Max }` + `aggregate_func(name) -> Option<AggFunc>`.
  - `contains_aggregate(&Expr)`, `is_aggregate_query(&SelectStmt)`.
  - `AggSpec { func, arg: Option<Expr>, star, distinct }` collected (deduped) from
    projection out-exprs + having + order-by; arity/type validation (→ `42883`).
  - accumulators with optional `DISTINCT` `HashSet<Datum>`; `NULL`-skip; checked-`i64`
    `SUM` (`22003`); `MIN`/`MAX` via `ops::compare`.
  - insertion-ordered grouping (`Vec<(key, accs)>` + `HashMap<key, idx>`); empty-input
    rule (one bare group / zero grouped rows).
  - `validate_grouped` (data-independent `42803`) + `eval_grouped` (agg-result /
    grouping-match / recurse / column→`42803`).
  - `execute_aggregate(s, table, rows) -> QueryResult` (fields via `resolve_projection`,
    fold, finalize, `HAVING`, order via existing `order_cmp`, `LIMIT`, project).
- [x] `exec.rs`: `execute_read` routes to `agg::execute_aggregate` when
  `is_aggregate_query`; `execute_read_locking` returns `0A000` for an aggregate query.
- [x] `agg.rs` unit tests (Task-2/3/4 claims in the spec traceability table).

## Task 4 — Integration + conformance
- [x] `executor` integration test (over the wire, extended + simple protocol): grouped
  counts/sums, `HAVING`, `DISTINCT`, empty table, `42803`/`42883`/`0A000`, and a
  `Describe`/binary-result check for aggregate result types.
- [x] `crates/conformance/corpus/aggregates.sql` — `ORDER BY`-stable aggregate corpus +
  the error cases, diffed against PG 18 in CI.

## Task 5 — Validate + document + finish
- [x] `cargo fmt`, `cargo clippy --workspace --all-targets`, `cargo nextest run
  --workspace`, `cargo test --workspace --doc`. UAC guard returns empty (no new test
  target name with `setup/install/update/patch/upgrad`; no new `[[test]]`/`[[bin]]`).
- [x] CLAUDE.md SP27 audit paragraph (breadth slice; no new test binary; no new
  dependency; UAC guard empty).
- [x] Commit, push `-u origin claude/hopeful-dijkstra-8eyy6g`, open a ready-for-review PR.

## Non-goals (deferred — see spec)
`AVG`/`numeric`; `GROUPING SETS`/`ROLLUP`/`CUBE`/`FILTER`/`WITHIN GROUP`; `SELECT DISTINCT`;
window functions; scalar functions; cross-range aggregation (one table = one range today).
