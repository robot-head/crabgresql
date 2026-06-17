# SP33 — Math & string function breadth (SQL breadth wave 7)

**Date:** 2026-06-16
**Status:** Approved (design)

## Goal

Add a broad set of scalar **math** and **string** functions to the executor —
the next increment of SQL expressiveness after SP32 closed the `numeric` type.
This is a pure-data, single-node breadth slice: every function is a deterministic
transform over one MVCC-resolved row, executed entirely inside one
`execute_read`/`eval` on one engine (a whole table lives on one range), so it
introduces no lock, write path, visibility rule, or interleaving.

## Scope — 21 functions (counting aliases)

### Math — type-preserving rounding family

These preserve the input type across `int4`/`int8`/`float8`/`numeric`, exactly
mirroring the existing `abs`. An `int` input is returned unchanged (the floor of
an integer is that integer); `sign` returns `-1`/`0`/`1` in the input's type.

| Function | Behavior |
|---|---|
| `floor(x)` | round toward −∞ |
| `ceil(x)` / `ceiling(x)` | round toward +∞ |
| `round(x)` | nearest; **float8 = half-to-even** (PG `rint`), **numeric = half-away-from-zero** (PG `numeric_round`) |
| `round(x, n)` | round to `n` decimal places (`n` may be negative); **result `numeric`**, `n` is int |
| `trunc(x)` | round toward 0 |
| `trunc(x, n)` | truncate to `n` decimal places; **result `numeric`**, `n` is int |
| `sign(x)` | −1 / 0 / 1, preserving the input numeric type |

**Two-arg rule:** `round(x, n)` / `trunc(x, n)` accept a `numeric` first arg, or
an `int` first arg promoted to `numeric`, and always return `numeric`. **A
`float8` first arg with two args → `42883`** (matches PG — there is no
`round(double precision, int)`).

### Math — transcendental family (always `float8`)

These accept `int`/`float8`/`numeric` args (promoted to `f64`) and **always
return `float8`**, computed via `f64`. This is a documented deviation from PG
(which returns `numeric` for `numeric` input); we use one uniform rule to avoid
arbitrary-precision transcendental math.

| Function | Behavior | Domain error |
|---|---|---|
| `sqrt(x)` | square root | `x < 0` → `2201F` |
| `power(x, y)` / `pow(x, y)` | `x` raised to `y` | `0` to a negative power → `2201F`; negative base to a non-integer power → `2201F` |
| `exp(x)` | eˣ | overflow → `22003` |
| `ln(x)` | natural log | `x ≤ 0` → `2201E` |
| `log(x)` | base-10 log (one-arg form only) | `x ≤ 0` → `2201E` |
| `pi()` | π constant (no args) | — |

### String functions

| Function | Behavior |
|---|---|
| `lpad(s, n [, fill])` | left-pad `s` to width `n` with `fill` (default `' '`); if `s` longer than `n`, truncate to the first `n` chars |
| `rpad(s, n [, fill])` | right-pad symmetrically; truncate from the front-kept first `n` chars when longer |
| `left(s, n)` | first `n` chars; `n < 0` → all but the last `|n|` |
| `right(s, n)` | last `n` chars; `n < 0` → all but the first `|n|` |
| `repeat(s, n)` | `s` repeated `n` times; `n ≤ 0` → `''` |
| `reverse(s)` | reverse, Unicode char by char |
| `strpos(s, sub)` | 1-based index of first `sub` in `s`; `0` if absent; empty `sub` → `1`; result `int4` |
| `initcap(s)` | capitalize the first letter of each word (ASCII word boundaries), lowercase the rest |
| `ascii(s)` | code point of the first char; empty string → `0`; result `int4` |
| `chr(n)` | the one-character string for Unicode code point `n` (`int`); `0` or out-of-range → `54000` |

## Type rules summary

- **Rounding family** (`floor`/`ceil`/`ceiling`/`round`/`trunc`/`sign`): input
  type preserved (`int4`→`int4`, `int8`→`int8`, `float8`→`float8`,
  `numeric`→`numeric`). Two-arg `round`/`trunc` → `numeric`.
- **Transcendental family** (`sqrt`/`power`/`pow`/`exp`/`ln`/`log`/`pi`): always
  `float8`.
- **String functions**: `length`-style integer results are `int4`; everything
  else is `text`.

All functions are **strict** (any `NULL` argument → `NULL`) except where noted by
PG semantics (none in this slice produce a non-NULL result from a NULL arg —
`coalesce`-style functions were SP29).

## Error surface

| Condition | SQLSTATE |
|---|---|
| Unknown name / bad arity / bad arg type | `42883` (`UndefinedFunction`) |
| `round(float8, int)` (two-arg float8) | `42883` |
| `sqrt(negative)`, `power` domain | `2201F` |
| `ln`/`log` of a non-positive number | `2201E` |
| `chr(0)` / `chr(out-of-range code point)` | `54000` |
| `exp` overflow, `abs`/`length`/`ascii` int overflow, `repeat`/`lpad` size overflow | `22003` |

`2201E`, `2201F`, and `54000` are **new** SQLSTATEs. They are introduced via a
single new, code-carrying `TypeError::Domain { sqlstate, message }` variant in
`pgtypes` (rather than one enum variant per math domain), so the executor and the
numeric module share one error path and the exact codes are confirmed empirically
by the conformance corpus diff against PostgreSQL.

## Architecture / files touched

- **`crates/pgtypes/src/numeric.rs`** — add value transforms `floor`, `ceil`,
  `round(bd, n)`, `trunc(bd, n)`, `sign` (no `sqrt`: the transcendental family is
  computed in `float8`). pgtypes is the mutation-baseline crate at zero
  survivors, so each comes with boundary-value unit tests.
- **`crates/pgtypes/src/error.rs`** — add `TypeError::Domain { sqlstate, message }`
  (+ its `sqlstate()` arm and a unit test).
- **`crates/executor/src/func.rs`** — register all 21 functions in `ScalarFunc`,
  extend `scalar_func` name resolution, `scalar_result_type` (arity + type
  validation + result type), and `eval_eager`/`eval_scalar` dispatch. The
  transcendental computations (f64 math + domain checks) live here.
- **`crates/executor/tests/math_string_functions.rs`** — new end-to-end wire test
  (UAC-safe name: no `setup`/`install`/`update`/`patch`/`upgrad` substring).
- **`crates/conformance/corpus/math_string_functions.sql`** — new corpus file,
  diffed against PostgreSQL 18 in CI.

**No parser change** (every function name is a plain identifier resolved by the
executor via the existing `Expr::Func(FuncCall)` node; `pi()` is the zero-arg
form, `power`/`pow` two-arg). **No new dependency** (`f64` is built-in;
`bigdecimal` already provides `with_scale_round`/`RoundingMode`).

## No Stateright model — deliberate and justified

Identical to SP27–SP32: every function is a pure, deterministic scalar transform
over the already-correct, MVCC-visible, single-range row set inside one
`execute_read`/`eval` on one engine — no lock, write path, visibility rule, or
interleaving. Even the subtle bits (rounding mode per type, domain errors) are
*value* properties with no event ordering to explore. This is CLAUDE.md's
"pure-data / single-node refactor" carve-out. Proven instead by:

- `pgtypes::numeric` unit tests (the new rounding/sign primitives: floor/ceil
  toward ±∞, half-away `round`, negative-`n` round/trunc, `sign` of ±/0).
- `pgtypes::error` unit test (the new `Domain` variant returns its carried code).
- `executor::func` unit tests (every function, NULL strictness, the type-preserving
  vs float8 result types, the full error surface).
- the `executor::math_string_functions` wire test (over the wire, result OIDs).
- `conformance/corpus/math_string_functions.sql` diffed against PG 18 in CI.

## Documented deviations

1. **Transcendental functions return `float8` for every numeric input** (PG
   returns `numeric` for `numeric` input). Magnitude-equivalent; only the
   type/scale differs. The corpus exercises them through `float8` columns / casts
   where PG also computes in `float8`.
2. **`round`/`trunc` two-arg form requires a `numeric` (or int-promoted) first
   arg**; a `float8` first arg with two args is `42883` (PG-faithful).
3. **`round(float8)` uses half-to-even**, `round(numeric)` uses
   half-away-from-zero — matching PG's `rint` vs `numeric_round` split (and the
   existing SP31 cast rounding split).
4. A bare decimal literal is `numeric` (SP32), so `floor(2.5)` is numeric
   arithmetic; float8 rounding is reached via a `::float8` cast / float8 column.
   The corpus exercises float8 rounding through float8 columns.
5. Output column names for function results are `?column?` (pre-existing engine
   behavior; PG names them after the function — cosmetic, not diffed).

## Non-goals (deferred)

- `real`/`float4`, date/time types.
- Numeric-precision transcendental functions (numeric `sqrt`/`ln`/`exp`/`power`).
- Two-arg `log(base, x)`, `width_bucket`, `gcd`/`lcm`, `factorial`.
- Regex (`regexp_match`/`regexp_replace`), `to_char`/`to_number`, `split_part`,
  `translate`, `format`, `position(sub IN s)` syntactic form (we ship the
  `strpos` function spelling only).
- `encode`/`decode`, `md5`, `to_hex`.
