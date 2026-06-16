# SP30 plan — `double precision` (float8) + `AVG` (SQL breadth wave 4)

Design: `docs/superpowers/specs/2026-06-16-crabgresql-sp30-double-precision-avg-design.md`.
Branch: `claude/sleepy-lovelace-le00od`.

Fourth breadth slice. Adds PostgreSQL `double precision` (`float8`, OID 701, f64) and the
`AVG` aggregate (+ float-aware `SUM`/`MIN`/`MAX`/`abs`/arithmetic). Pure-data / single
range → **no Stateright model** (see spec "Why not a Stateright model"); proven by unit +
integration + the differential conformance oracle. `real`/`float4`, `numeric`, and
`CAST`/`::` are deferred (non-goals).

## Task 1 — Types (`pgtypes`)
- [x] `datum.rs`: `oids::FLOAT8 = 701`; `ColumnType::Float8`; `from_sql_name`
  (`float8`/`float`/`double precision`); `oid`/`name`(`double precision`)/`type_size`(8);
  `Datum::Float8(f64)`; `column_type`. Replace `derive(PartialEq,Eq,Hash)` with
  hand-written impls (float grouping: `NaN==NaN`, `-0.0==+0.0`, canonical-bits `Hash`).
- [x] `encoding.rs`: `encode_text` (specials `Infinity`/`-Infinity`/`NaN`, else `{f}`);
  `encode_binary` (IEEE big-endian `to_be_bytes`).
- [x] `ops.rs`: `float_literal` (parse f64; `inf`→`22003`); `as_f64`; numeric promotion in
  `add`/`sub`/`mul`/`div` (finite-overflow→`22003`, float `/0`→`22012`); float ordering in
  `compare` (NaN greatest/equal, `-0.0==0.0`).
- [x] Unit tests: literal/overflow, promotion, `/0`, overflow, NaN/±Inf order+eq, `Eq`/`Hash`
  grouping, text/binary encoding.

## Task 2 — Storage (`kv`, `catalog`)
- [x] `kv/rowenc.rs`: `tag::FLOAT8 = 5` in encode/decode; extend roundtrip + `arb_datum`.
- [x] `catalog/serde.rs`: `type_tag::FLOAT8 = 4` in `tag_of`/`type_of`; extend roundtrip.

## Task 3 — Parser/AST (`pgparser`)
- [x] `token.rs`: `Token::FloatLit(String)`.
- [x] `lexer.rs`: numeric lexer splits int vs float (`.`-fraction, leading `.`, `e`/`E`
  exponent w/ optional sign); leading-dot needs a following digit.
- [x] `ast.rs`: `Expr::FloatLiteral(String)`.
- [x] `parser.rs`: `prefix()` `FloatLit` arm; `create_table` two-word `double precision`.
- [x] Unit tests: float-literal forms, `double precision` column, precedence unchanged.

## Task 4 — Executor (`eval`, `exec`, `agg`, `func`)
- [x] `eval.rs`: `eval`/`infer_type` `FloatLiteral`; float promotion in arithmetic
  `infer_type`; `unify_types` int+float→float8.
- [x] `exec.rs`: `coerce` int→float8, float8→float8, float8→int (`round_ties_even`,
  range-check `22003`).
- [x] `agg.rs`: `AggFunc::Avg`; `aggregate_func`(`avg`); `func_result_type`
  (`sum(float8)`→float8, `avg`→float8); `AggSpec.arg_type`; `spec_of` accepts float for
  sum/avg; `Acc::{SumF, Avg}`; fold/finish.
- [x] `func.rs`: `abs(float8)`→float8 (eval + `scalar_result_type` via numeric helper).
- [x] Unit tests: avg(float8)/avg(int)→float8, sum/min/max(float8), float DISTINCT/GROUP BY,
  abs(float8), coerce.

## Task 5 — Integration + conformance
- [x] `crates/executor/tests/floating_point.rs` (UAC-safe target name) — end-to-end over the
  wire: float8 table CRUD, arithmetic, comparison/order incl. specials, `avg/sum/min/max`,
  GROUP BY/DISTINCT over floats, RowDescription OID 701, error SQLSTATEs.
- [x] `crates/conformance/corpus/floating_point.sql` — ORDER BY-stable, agreeing-magnitude
  float surface + error cases, diffed against PG 18 in CI.

## Task 6 — Validate + document + finish
- [x] `cargo fmt`, `cargo clippy --workspace --all-targets`, `cargo nextest run --workspace`,
  `cargo test --workspace --doc`. UAC guard returns empty (no new target name with
  `setup/install/update/patch/upgrad`).
- [x] CLAUDE.md SP30 audit paragraph (breadth slice; one new test binary
  `executor::floating_point`; no new dependency; UAC guard empty).
- [x] Commit, push `-u origin claude/sleepy-lovelace-le00od`, open a ready-for-review PR.

## Non-goals (deferred — see spec)
`real`/`float4`; `numeric`/`decimal`; `CAST`/`::`; math functions beyond `abs`; float
`mod`/`%`; scientific-notation text for extreme magnitudes; cross-range float aggregation.
