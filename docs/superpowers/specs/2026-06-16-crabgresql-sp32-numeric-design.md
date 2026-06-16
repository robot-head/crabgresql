# SP32 — `numeric` / `decimal` (arbitrary-precision exact decimal) (SQL breadth wave 6)

**Date:** 2026-06-16
**Status:** Approved (design)

## Problem / motivation

SP27–SP31 were five SQL-breadth waves. Three of them (SP27 `AVG`, SP30 `double precision`,
SP31 casts) repeatedly named the *same* missing type as the thing that would retire their
standing deviations: **`numeric`**. SP30's spec says it directly — "`numeric` is what would
make `avg(int)` text-exact and decimal literals scale-faithful" — and SP31 echoes it. This
slice adds PostgreSQL **`numeric`/`decimal`** (OID 1700): an arbitrary-precision, exact
decimal with a tracked display scale, plus the `numeric(precision, scale)` type modifier.

It retires three documented deviations at once:

- **Bare decimal/exponent literals are now `numeric`**, not `float8` (SP30's deviation). So
  `SELECT 1.50` is the scale-faithful numeric `1.50`, `SELECT 1.0/3` is the exact
  `0.33333333333333333333`, and `1.5 + 1.5` is `3.0` — matching PostgreSQL. `float8` is now
  reached only through a `float8` column, a `::float8` cast, or a float-returning function.
- **`avg(int)` / `avg(numeric)` return `numeric`** (SP27/SP30's float8 deviation), text-exact.
- **`sum(numeric)`** and exact numeric arithmetic exist.

## Scope decisions (and why)

### Why not a Stateright model

Same carve-out as SP27–SP31: `numeric` is **data**. A value, two wire encodings, the
arithmetic/scale rules, and the casts are pure transforms of one already-evaluated `Datum`,
composing into expression evaluation inside one `execute_read`/`eval` on one engine (a whole
table lives on one range). No lock, write path, visibility rule, or interleaving is
introduced — its correctness is a *value* property, proven exhaustively by unit tests plus
the differential conformance oracle. A `Model` would have an interleaving-free state space.

### Backed by `bigdecimal` (the one new dependency)

Arbitrary-precision base-10000 decimal arithmetic (PostgreSQL's `numeric.c`) is a large,
error-prone thing to hand-roll. `bigdecimal` (0.4, pure Rust over `num-bigint` — no native
`cc`, so the shipped tree stays pure Rust) provides exact decimal arithmetic with **value-
based `==`/`Ord`** (so `1.0 == 1.00` for `GROUP BY`/`DISTINCT`), scale-preserving storage,
and half-away-from-zero rounding — exactly PostgreSQL's numeric semantics. The slice's own
code is the PostgreSQL-faithful *policy* on top: the display-scale rules, `numeric_out`/
`numeric_send`, typmod enforcement, and the casts.

### Canonical dscale, and a hand-written `numeric_out`

PostgreSQL's display scale (dscale) is never negative; `bigdecimal` keeps `1e3` at scale −3.
Every numeric `Datum` is **canonicalized** to `dscale >= 0` on entry (literal parse, text
cast), so result scales (`+`/`−` → max, `*` → sum) match PostgreSQL. `bigdecimal`'s `Display`
also switches to scientific notation for small magnitudes (`1.5E-10`), which PostgreSQL never
does, so `numeric_out` is **hand-written** (plain decimal, exactly `dscale` fractional digits).

### Division / AVG display scale (`select_div_scale`)

The one genuinely intricate rule. PostgreSQL chooses a division result scale that guarantees
~16 significant digits: in base-10000 units, `rscale = clamp(max(16 − qweight·4, s1 + s2), 0,
1000)`, where `qweight` is the quotient's leading-digit weight estimate (`w1 − w2`, decremented
when the leading base-10000 group of the dividend is smaller than the divisor's). This is
implemented from `BigDecimal::as_bigint_and_exponent()` and **validated against a real
PostgreSQL oracle** across a battery of cases (incl. multi-group weights and sub-1 values).
`avg` reuses it (sum/count), making `avg(int)` text-exact.

### `numeric(p, s)`, rounding, and casts

The `numeric(precision, scale)` modifier is parsed, persisted (catalog), and **enforced on
store/cast**: round to `scale` (half-away-from-zero — PostgreSQL's numeric rounding, distinct
from `float8 → int`'s half-to-even), then a precision overflow (more integer digits than
`precision − scale`) is `22003` ("numeric field overflow"). The cast matrix gains the
**numeric family** — `int4`/`int8`/`float8`/`numeric` all interconvert (`numeric → int` rounds
half-away; `float8 → numeric` uses the float's shortest text, so `0.1::float8::numeric` is
`0.1`); `text ↔ numeric` parses/renders; there is **no `numeric ↔ bool` cast** (`42846`).

### Documented deviations / non-goals

- **`NaN` / `±Infinity`** numeric specials are deferred (so no special-value propagation);
  `'NaN'::numeric` is `22P02` here (PG accepts it). They have no SQL literal, so they never
  reach the corpus.
- `sum(int8)` still returns `int8` (PG: `numeric`) — a remaining, separately-scoped deviation.
- The binary wire format (`numeric_send`) is implemented and unit-tested against hand-computed
  byte vectors, but is not exercised by the corpus/integration tests (those read in text mode).

## Components

- **A. Types (`pgtypes`).** New `numeric` module (`BigDecimal`-backed): `parse`, `to_text`
  (`numeric_out`), `binary` (`numeric_send` NBASE), `add`/`sub`/`mul`/`div`/`rem`/`abs`,
  `select_div_scale`, `to_i32`/`to_i64` (half-away + range), `from_i64`/`from_f64`/`to_f64`,
  `apply_typmod`, and `Typmod`. `ColumnType::Numeric(Option<Typmod>)` (OID 1700, name
  `numeric`, typlen −1). `Datum::Numeric(BigDecimal)` with value-based `Eq`/`Hash`
  (scale-ignoring, for grouping). `ops` gains the **numeric tier** (int < numeric < float8)
  in `add`/`sub`/`mul`/`div`/`rem`/`compare`. `cast` gains the numeric family. `encoding`
  routes numeric text/binary.
- **B. Storage (`kv`, `catalog`).** `kv::rowenc` tag `NUMERIC = 6` (canonical decimal text,
  round-tripping scale). `catalog::serde` tag `NUMERIC = 5` + an inline typmod payload. Both
  append-only.
- **C. Parser/AST (`pgparser`).** `Expr::FloatLiteral` → `Expr::NumericLiteral` (bare decimals
  are numeric); `parse_type_name` parses `numeric(p[,s])`.
- **D. Executor.** `eval`/`infer_type` learn `NumericLiteral` + the numeric tier in
  `numeric_result_type`/`unify_types`; `coerce` rounds/overflows numeric-family assignments;
  `agg` adds numeric `SUM`/`AVG` accumulators (exact, `select_div_scale`) and numeric
  `MIN`/`MAX`; `func` extends `abs`/`mod` to numeric.
- **E. Conformance.** `crates/conformance/corpus/numeric.sql` — literals, exact arithmetic,
  division/avg scale, `numeric(p,s)` rounding/overflow, casts, grouping, abs/mod, and the
  `22012`/`22003`/`42846`/`22P02` error surface, diffed against real PostgreSQL in CI.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | Literal parse/canonical scale; `numeric_out` is plain decimal (never scientific); `numeric_send` NBASE bytes. | `pgtypes::numeric` unit tests. |
| 2 | `+`/`−`/`*` scale rules; division/AVG `select_div_scale` (validated vs a PG oracle); `mod`/`abs`; div-by-zero `22012`. | `pgtypes::numeric` unit tests. |
| 3 | `numeric → int` rounds half-away (vs `float8 → int` half-even); `numeric(p,s)` rounds + overflows `22003`; the numeric-family cast matrix; no `numeric↔bool`. | `pgtypes::{numeric,cast}` unit tests. |
| 4 | Value-based grouping equality (`1.0 == 1.00`) + consistent hash. | `pgtypes::{datum,numeric}` unit tests. |
| 5 | Row + schema (de)serialization round-trip numeric (value + scale + typmod). | `kv::rowenc` + `catalog::serde` unit tests. |
| 6 | Bare literal → numeric; numeric tier in arithmetic/`unify`; `SUM`/`AVG`/`MIN`/`MAX`/`abs`/`mod` over numeric; `avg(int)` → numeric. | `executor::{eval,agg,func}` unit tests. |
| 7 | End-to-end over the wire: numeric columns/`numeric(p,s)`, literals, arithmetic/division, aggregates, casts, grouping, result type OID 1700, error SQLSTATEs. | `executor::numeric` integration test. |
| 8 | Differential parity against PostgreSQL for the numeric surface (validated locally vs PG 16, diffed vs PG 18 in CI). | `conformance/corpus/numeric.sql`. |
| 9 | No regression (SP30 float tests adjusted to use `::float8` where they relied on bare-literal-float8). | full `cargo nextest run --workspace` + doctests. |

## Success criteria

1. `numeric`/`decimal`/`numeric(p,s)` columns, bare-decimal numeric literals, exact arithmetic
   (incl. division/AVG display scale), the numeric-family casts, and `SUM`/`AVG`/`MIN`/`MAX`/
   `abs`/`mod` over numeric work end-to-end with PG-faithful semantics. — (A–D)
2. The error surface matches PostgreSQL SQLSTATEs (`22012`/`22003`/`42846`/`22P02`). — (#2,#3,#7)
3. The conformance corpus diffs clean against PostgreSQL for the numeric surface. — (#8)
4. No regression; SP30's bare-literal-float8 assumptions are updated to `::float8`. — (#9)

## Non-goals (deferred)

- **`NaN` / `±Infinity`** numeric specials (and `'NaN'`/`'Infinity'` text input).
- **`real` / `float4`** (the other deferred float type), date/time types.
- `sum(int8) → numeric` parity (still `int8`).
- Binary-format numeric **clients** in the test suite (the encoder ships + is unit-tested;
  the wire/conformance tests read text).
- Sending the `numeric(p,s)` typmod in `RowDescription` (the value is exact; only the
  advertised typmod is omitted, which the text-diffing oracle does not check).
