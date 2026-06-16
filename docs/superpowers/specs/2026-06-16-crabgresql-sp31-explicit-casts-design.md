# SP31 — explicit casts: `CAST(expr AS type)` + `expr::type` (SQL breadth wave 5)

**Date:** 2026-06-16
**Status:** Approved (design)

## Problem / motivation

SP27–SP30 were four SQL-breadth waves (aggregates, predicates, scalar functions,
`double precision`). SP30's "Non-goals (deferred)" names the next one outright:

> **`CAST(expr AS type)` / `expr::type`** — not needed to *use* `float8` (float
> literals + `int⊕float` promotion suffice); its own breadth slice.

That is the same kind of self-documented "next" signal SP30 itself followed (the
twice-deferred `AVG`). This slice adds **explicit type conversion** — both the SQL-
standard `CAST(expr AS type)` functional form and the PostgreSQL `expr::type`
operator — over the slice's five runtime types (`bool`, `int4`, `int8`, `text`,
`float8`). No new type is introduced; casts only convert *between* the existing
ones. The headline capability is the one nothing before this slice could do:
**parse a `text` value into a number/bool, and render any value as `text`**, e.g.
`'42'::int4`, `'1.5'::float8`, `'t'::bool`, `n::text`, `flag::text`.

## Scope decisions (and why)

### Why not a Stateright model

CLAUDE.md mandates an exhaustive model for "anything touching 2PC, replication,
recovery, leadership, locking, MVCC visibility, or cross-range consistency," and
carves out "a pure-data or single-node refactor with no concurrency/fault dimension
may not warrant one." This slice is squarely the carve-out, identical in structure
to SP27/SP28/SP29/SP30:

- A cast is a **pure value transform** of one already-evaluated `Datum` to a target
  type — no lock, no write path, no visibility rule, no I/O, no interleaving. It
  composes into expression evaluation exactly like arithmetic or a scalar function,
  inside one `execute_read`/`eval` on one engine (a whole table lives on one range).
- Its correctness is a *value* property — a finite cast matrix plus per-conversion
  parsing/rounding rules — proven exhaustively by boundary-value unit tests. A
  `Model` of a cast would have an interleaving-free state space and merely restate
  those unit tests.

So SP31 ships **no model**, consistent with the four prior breadth waves, and over-
invests in deterministic empirical proof (unit + integration + the differential
conformance oracle).

### The cast matrix is PostgreSQL's *explicit* context

PostgreSQL has three cast contexts (implicit / assignment / explicit); `CAST`/`::`
use the broadest, **explicit**. Among the five types the defined explicit casts are
(NULL → NULL for every one):

| from \ to | bool | int4 | int8 | float8 | text |
|-----------|------|------|------|--------|------|
| bool      | id   | ✓ 0/1 | —   | —      | ✓ `true`/`false` |
| int4      | ✓ 0/≠0 | id | widen | widen | ✓ |
| int8      | —    | ✓ 22003 | id | widen | ✓ |
| float8    | —    | ✓ rint | ✓ rint | id | ✓ |
| text      | ✓ parse | ✓ parse | ✓ parse | ✓ parse | id |

The asymmetries are PostgreSQL's, kept faithfully: **bool↔int exists only for
`int4`** (so `true::int8`, `1::int8::bool`, `1.5::bool` are all *undefined*), and
there is **no `float8`/`int8` ↔ `bool` cast**. An undefined `(from, to)` pair is
**42846** (`cannot_coerce`), reported at *plan time* (`infer_type`) so it is caught
before any row is produced — and the cast result column type is known for
`RowDescription`. The whole matrix is one source of truth: a static
`cast_allowed(from, to)` predicate (plan time) and a runtime `cast(value, to)` value
transform, in a new `pgtypes::cast` module.

The conversions themselves match PostgreSQL:

- **numeric ↔ numeric** — int widening, range-checked narrowing (`int8→int4` over
  range → 22003), and **`float8 → int` round-half-to-even** (PG's `rint`/`dtoi4`).
- **bool ↔ int4** — `false`/`true` ↔ `0`/`1`; `0`/non-zero → `false`/`true`.
- **`* → text`** — the type's output text (reusing the wire text encoder), **except
  `bool → text` is `true`/`false`** (PG's `booltext` cast), NOT the `t`/`f` of a
  bool column's `boolout`.
- **`text → bool`** — PG `boolin`: trimmed, case-insensitive, a non-empty prefix of
  `true`/`false`/`yes`/`no`/`on`/`off` or the single chars `1`/`0` (the ambiguous
  `o` resolves to `on` → true, as PG tests `on` before `off`); else 22P02.
- **`text → int`** — trimmed `[+-]?[0-9]+`; bad syntax (decimal point, letters,
  empty, lone sign) → 22P02, **well-formed-but-out-of-range → 22003** (the syntax
  check is split from the width parse so the two SQLSTATEs are distinguished).
- **`text → float8`** — trimmed `float8in`: decimal/exponent forms and the IEEE
  specials `Infinity`/`-Infinity`/`NaN`/`inf` (case-insensitive); a *finite*
  literal overflowing to ∞ (`'1e400'`) → 22003, but an explicit infinity spelling
  is the **value** Infinity (so it cannot just reuse `ops::float_literal`, whose
  grammar has no infinity spelling).

### Parsing: `::` is the tightest operator

`::` is added to the Pratt loop as an **unconditional postfix** (no `min_bp` gate),
because it binds tighter than every other operator — tighter than unary minus and
arithmetic. So `-2::int8` parses `-(2::int8)` (the unary-minus prefix recurses into
`expr`, whose innermost frame grabs the `::` before the minus applies), `1 + 2::int8`
is `1 + (2::int8)`, and `a::int4::text` is left-associative `(a::int4)::text`.
Because `::` is unconditional there is **no binding-power comparison to mutate**
(unlike the existing `l_bp < min_bp` check). `CAST(expr AS type)` adds the reserved
keyword `CAST` and parses the functional form. Both forms resolve the target type
name with the **shared `parse_type_name` helper** extracted from `CREATE TABLE`
(which folds the two-word `double precision`); an unknown type name is a 42601
parse error (consistent with the column-type path — a documented deviation from
PG's 42704).

### Documented deviations

- **Bare decimal literals are `float8`, not `numeric`** (the standing SP30
  deviation), so a bare-literal `float8→int` cast rounds half-to-even where PG's
  `numeric` rounds half-away-from-zero — `2.5::int` is `2` here, `3` in PG. The
  conformance corpus therefore exercises `float8→int` through float8 **columns**
  (where PG also uses `float8→int`/`rint`), and the bare-literal rounding lives in
  unit tests. (Identical discipline to SP30's corpus.)
- An **unknown cast target type** is 42601 (PG: 42704), matching the existing
  `CREATE TABLE` column-type behavior.
- A cast's **output column name** is `?column?` (PG names it after the target
  type's `typname`); this matches SP28's precedent for predicate/CASE outputs and
  does not affect conformance (the oracle diffs row values + SQLSTATE, not headers).
- `float8 ↔ int8` at the `±2^63` boundary saturates rather than erroring (the
  existing assignment-`coerce` behavior, kept consistent; an extreme edge, not in
  the corpus).

## Components

- **A. Types (`pgtypes`).** New `pgtypes::cast` module: `cast_allowed(from, to)`
  (static matrix) and `cast(value, to)` (runtime transform) plus the private
  text-parse / float→int / bool helpers. New `TypeError::CannotCast { from, to }`
  → 42846.
- **B. Parser/AST (`pgparser`).** `Token::TypeCast` (`::`); `Keyword::Cast`; the
  lexer lexes `::` (a lone `:` stays an error); `Expr::Cast { expr, ty }`; the
  shared `parse_type_name` helper (reused by `CREATE TABLE`); the unconditional
  `::` postfix in `expr`; the `CAST(_ AS _)` prefix arm.
- **C. Executor.** `eval`/`infer_type` learn `Expr::Cast` (runtime conversion via
  `pgtypes::cast::cast`; plan-time `cast_allowed` gate → 42846); `agg`'s four
  traversals (`contains_aggregate`/`collect_specs`/`validate_grouped`/`eval_grouped`)
  recurse through a cast's operand, so a cast may wrap or be wrapped by an aggregate
  (`sum(x)::int8`, `avg(x::float8)`). Assignment-`coerce` is left untouched
  (different PG cast context).
- **D. Conformance.** `crates/conformance/corpus/cast.sql` — both spellings, the
  cast matrix, precedence/chaining, casts through a column, and the 22P02 / 22003 /
  42846 error surface, diffed against real PG 18 in CI.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | Lexer lexes `::` (lone `:` is an error); `CAST` is a keyword. | `pgparser` lexer + `token` round-trip tests. |
| 2 | Both forms parse to one `Cast` node; `::` binds tighter than unary minus / `+` and chains left-assoc; unknown type → 42601. | `pgparser` parser tests. |
| 3 | The static cast matrix matches PG (incl. the bool/int4-only and no-`*↔bool` asymmetries). | `pgtypes::cast` `cast_allowed` test. |
| 4 | Each conversion's value semantics: numeric widen/narrow (22003), float→int rint, bool↔int4, *→text (`bool`→`true`/`false`), text→bool (PG spellings), text→int (22P02 vs 22003), text→float8 (specials, overflow). | `pgtypes::cast` unit tests. |
| 5 | NULL casts to NULL for every target; undefined cast → 42846. | `pgtypes::cast` unit tests. |
| 6 | `eval` performs the conversion; `infer_type` returns the target type and rejects an undefined cast at plan time (42846); a cast composes with aggregates/`GROUP BY`. | `executor::eval` unit tests. |
| 7 | End-to-end over the wire: both spellings, the matrix, result-type OIDs, casts through a column, and the 22P02/22003/42846 surface. | `executor::casts` integration test. |
| 8 | Differential parity against PostgreSQL 18 for the cast surface. | `conformance/corpus/cast.sql` (CI diff). |
| 9 | No regression of the existing scan/filter/sort/DML/aggregate/predicate/scalar/2PC suites. | full `cargo nextest run --workspace` + doctests. |

## Success criteria

1. `CAST(expr AS type)` and `expr::type` work end-to-end for the explicit cast
   matrix over the five types, with PG-faithful conversion semantics (within the
   documented `numeric`-absence deviation). — (A–C)
2. The error surface matches PostgreSQL SQLSTATEs (`22P02`/`22003`/`42846`). — (#4,#7)
3. The conformance corpus diffs clean against PG 18 for the in-range cast surface. — (#8)
4. No regression. — (#9)

## Non-goals (deferred)

- **New types** — `real`/`float4`, `numeric`/`decimal` (a `numeric` is what would
  make a bare-literal `float8→int` cast round PG-identically and `avg(int)` text-
  exact), `date`/`timestamp`, etc. SP31 only converts *between the existing five*.
- **`CAST` to/from types this slice lacks**, and PG's length-qualified target
  spellings (`varchar(n)`, `numeric(p,s)`).
- **Implicit/assignment cast unification** — `INSERT`/`UPDATE` assignment `coerce`
  stays its own (narrower) path; this slice adds only the explicit context.
- **`typname`-based cast output column naming** (cosmetic; `?column?` for now).
