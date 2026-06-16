# SP31 plan — explicit casts: `CAST(expr AS type)` + `expr::type` (SQL breadth wave 5)

Design: `docs/superpowers/specs/2026-06-16-crabgresql-sp31-explicit-casts-design.md`.
Branch: `claude/eager-volta-dd4izk`.

Fifth breadth slice. Adds explicit type conversion — the SQL-standard
`CAST(expr AS type)` and the PostgreSQL `expr::type` operator — over the five existing
runtime types (`bool`, `int4`, `int8`, `text`, `float8`); no new type. Headline: parse
`text` → number/bool and render anything → `text`. Pure-data / single range → **no
Stateright model** (see spec "Why not a Stateright model"); proven by unit + integration
+ the differential conformance oracle. New types, implicit/assignment-cast unification,
and length-qualified target spellings are deferred (non-goals).

## Task 1 — Types (`pgtypes`)
- [x] `error.rs`: `TypeError::CannotCast { from, to }` → `42846`; sqlstate + test.
- [x] `cast.rs` (new): `cast_allowed(from, to)` (static PG explicit matrix) and
  `cast(value, to)` (runtime transform) sharing one matrix; helpers for text→int
  (22P02 syntax vs 22003 range), text→float8 (specials + finite-overflow 22003),
  text→bool (PG `boolin` prefixes, `on`-before-`off`), float→int (`round_ties_even`
  + range), and `*`→text (`bool`→`true`/`false`). `lib.rs`: `pub mod cast`.
- [x] Unit tests (mutation-baseline crate → exhaustive): matrix, NULL→NULL, every
  conversion's boundaries, undefined cast → 42846.

## Task 2 — Parser/AST (`pgparser`)
- [x] `token.rs`: `Token::TypeCast` (`::`); `Keyword::Cast` (+ round-trip test).
- [x] `lexer.rs`: lex `::` → `TypeCast` (lone `:` stays "unexpected character"); test.
- [x] `ast.rs`: `Expr::Cast { expr, ty: ColumnType }`.
- [x] `parser.rs`: shared `parse_type_name` (extracted from `create_table`, folds
  two-word `double precision`); unconditional `::` postfix in `expr` (tightest, no
  `min_bp` gate, left-assoc); `CAST(_ AS _)` prefix arm (`cast_expr`).
- [x] Unit tests: both forms → one node, `::` precedence vs unary minus / `+`,
  left-assoc chaining, unknown type → 42601.

## Task 3 — Executor (`eval`, `agg`)
- [x] `eval.rs`: `eval` `Expr::Cast` (convert via `pgtypes::cast::cast`);
  `infer_type` `Expr::Cast` (target type; `cast_allowed` gate → 42846 at plan time).
- [x] `agg.rs`: `Expr::Cast` arms in `contains_aggregate`/`collect_specs`/
  `validate_grouped`/`eval_grouped` (recurse the operand; grouped eval converts).
- [x] Assignment-`coerce` (`exec.rs`) left untouched (different PG cast context).
- [x] Unit tests: each conversion via `eval`; `infer_type` target + plan-time 42846.

## Task 4 — Integration + conformance
- [x] `crates/executor/tests/casts.rs` (UAC-safe target name) — end-to-end over the
  wire: both spellings, the matrix, result-type OIDs, casts through a column,
  precedence/chaining, and the 22P02/22003/42846 surface.
- [x] `crates/conformance/corpus/cast.sql` — float→int through float8 columns
  (numeric-deviation discipline), both spellings, matrix, specials, error cases,
  diffed against PG 18 in CI.

## Task 5 — Validate + document + finish
- [x] `cargo fmt`, `cargo clippy ... --all-targets` (-D warnings clean),
  `cargo test` (touched + dependent crates), `cargo test --workspace --doc`. UAC
  guard returns empty (`casts` has no `setup/install/update/patch/upgrad` substring).
- [x] CLAUDE.md SP31 audit paragraph (breadth slice; one new test binary
  `executor::casts`; no new dependency; UAC guard empty).
- [x] Commit, push `-u origin claude/eager-volta-dd4izk`, open a ready-for-review PR.

## Non-goals (deferred — see spec)
New types (`real`/`float4`, `numeric`, date/time); casts to/from absent types and
length-qualified target spellings (`varchar(n)`); implicit/assignment-cast
unification; `typname`-based cast output column naming.
