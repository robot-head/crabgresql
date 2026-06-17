# SP34 — PG-faithful numeric transcendentals (retire SP33's float8 deviation)

**Date:** 2026-06-16
**Status:** Approved (design)
**Branch:** continues on `claude/pensive-greider-f57829` / PR #52 (SP33 not yet merged)

## Goal

Make the transcendental functions `sqrt`, `ln`, `log`, `exp`, `power`/`pow`
return **`numeric` for `numeric` input**, matching PostgreSQL exactly (value AND
display scale), retiring the single documented SP33 deviation ("we uniformly
return float8 to avoid arbitrary-precision transcendental math"). Integer and
`float8` inputs are unchanged — PostgreSQL resolves `sqrt(4)` (integer) and
`sqrt(x::float8)` to `float8`; only a `numeric` argument changes the result type.

This is a pure-value slice (no lock/write-path/visibility/interleaving), the same
carve-out as SP27–SP33, so it ships **no Stateright model**.

## Type rules

- **Unary** (`sqrt`, `ln`, `log`, `exp`): `numeric` in → `numeric` out; `int4`/
  `int8`/`float8` in → `float8` out (unchanged from SP33).
- **`power(x, y)`**: `float8` if **any** argument is `float8`; else `numeric` if
  **any** argument is `numeric`; else `float8` (all-integer — PostgreSQL's
  preferred type for the `power(int, int)` resolution). An `int` argument
  alongside a `numeric` one is promoted to `numeric`.

The float8 paths (and all the int-literal cases) are exactly today's behavior;
the only new code path is "at least one numeric operand, no float8 operand".

## Value computation — `pgtypes::numeric`

**Finding:** `bigdecimal` 0.4.10 has `exp`/`sqrt`/`powi`/`cbrt`/`inverse` but **no
`ln`/`log10`** — so it cannot supply the logarithm `log`/`ln`/non-integer-`power`
need. Per the "prefer vetted crates for hard subsystems" rule, SP34 adds **one new
pure-Rust dependency, `dashu-float`** (no C/`cc` — keeps the shipped tree pure
Rust), whose `DBig` is a base-10, **HalfAway**-rounding arbitrary-precision float
— matching PostgreSQL `numeric`'s base and rounding exactly. Its `Context` API:

```rust
let ctx = dashu_float::Context::<dashu_float::round::mode::HalfAway>::new(prec); // prec = sig digits
ctx.exp(x.repr())   -> Rounded<DBig>
ctx.ln(x.repr())    -> Rounded<DBig>
ctx.sqrt(x.repr())  -> Rounded<DBig>
ctx.powf(b.repr(), e.repr()) -> Rounded<DBig>   // Rounded::value() -> DBig
```

Values flow `BigDecimal → DBig` (via the plain-decimal text from `numeric::to_text`
→ `DBig::from_str`), compute, then `DBig → BigDecimal` (via `DBig::to_string` →
`numeric::parse`). Functions:

- `sqrt(x)` → `ctx.sqrt` (negative → `2201F`).
- `exp(x)` → `ctx.exp`.
- `ln(x)` → `ctx.ln` (x ≤ 0 → `2201E`).
- `log(x)` (base-10, one-arg) = `ctx.ln(x) / LN10` where `LN10 = ctx.ln(10)`
  computed at the same precision (x ≤ 0 → `2201E`).
- `power(x, y)`:
  - `y` an integer → `BigDecimal::powi` (EXACT BigInt power; handles a negative
    base, e.g. `power(-2,3)=-8`, and exact large results like `power(2,100)`).
  - else `x > 0` → `ctx.powf(x, y)`.
  - `x = 0, y > 0` → `0`; `x = 0, y < 0` → `2201F`; `x = 0, y = 0` → `1`.
  - `x < 0` with non-integer `y` → `2201F` (complex result).

The dashu computation uses a generous **precision** (significant digits) =
`result_integer_digits_estimate + rscale + GUARD` (GUARD = 16), derived from the
same result-weight estimate that drives rscale; the `DBig` result is then
converted to `BigDecimal` and **rounded half-away-from-zero to PostgreSQL's
rscale** (below) via the existing `with_scale_round` path, then `canonical`-ized.
(dashu's own rounding is at a *significant-digit* boundary, so the final
`with_scale_round(rscale, HalfUp)` is what guarantees PG's exact decimal scale.)

## Display-scale (rscale) selection — the crux

PostgreSQL's `numeric.c` chooses a result display scale per function from cheap
*estimates* of the result's decimal weight. These are reverse-engineered from the
PG source (`sqrt_var`) and validated against a live PostgreSQL 17.10 oracle across
a magnitude battery. Constants: `NUMERIC_MIN_SIG_DIGITS = 16`, `DEC_DIGITS = 4`,
`MIN_DISPLAY_SCALE = 0`, `MAX_DISPLAY_SCALE = 1000`.

Let `w` = the argument's **base-10000** weight (via the existing
`nbase_weight_and_lead`), and `dw` = the argument's **decimal** weight of its
leading digit (positive for ≥ 1, negative for < 1; `dw = (w+1)·DEC_DIGITS − 1 −
(leading-group trailing-zero adjustment)` — computed from the compact form, like
`nbase_weight_and_lead` already does for the leading group).

- **sqrt** (verbatim from `sqrt_var`):
  `sweight = w · DEC_DIGITS / 2 + 1;  rscale = clamp(16 − sweight, max(arg.dscale,0), 1000)`.
  Validated: `sqrt(2)`→15 dec, `sqrt(1e6)`→13, `sqrt(0.04)`→17. ✓
- **exp** (from `exp_var`): `ln_dweight = trunc(double(arg) · 0.4342944819)` (C
  truncation toward zero); `rscale = clamp(16 − ln_dweight, 0, 1000)`. Validated:
  `exp(1)`→16, `exp(2.5)`→15, `exp(10)`→12, `exp(100)`→0, `exp(-5)`→18. ✓
- **ln** and **log** (one-arg, base 10): `rscale = clamp(16 −
  estimate_ln_dweight(arg), max(arg.dscale,0), 1000)`, where

  ```
  estimate_ln_dweight(arg):
      dw = decimal weight of arg's leading digit
      if dw == 0:  return 0
      else:        return max(0, floor(log10(|dw| · 2.302585092994046)))
  ```

  Validated across all 20 sampled ln/log cases (`ln(2)`→16, `ln(1e6)`→15,
  `ln(1e-6)`→15, `log(100)`→16, `log(1e6)`→15, `log(0.5)`→16, …). ✓ For `arg`
  within `[0.9, 1.1]` PostgreSQL takes a direct-computation branch; those
  near-1 inputs are validated explicitly and, if any resists the estimate,
  excluded from the corpus with a note.
- **power**:
  - integer exponent (`power_var_int`): rscale from the estimated result weight
    `≈ base_dweight · exp`; finalized against the oracle (validated:
    `power(2,10)`→13, `power(2,3)`→16, `power(3,4)`→15, `power(5,-2)`→17).
  - non-integer exponent: computed as `exp(y · ln x)` and scaled by the `exp`
    rscale rule on `y · ln x`; finalized against the oracle.

## Validation harness — the correctness gate

A dedicated oracle-validation test (mirroring SP32's `select_div_scale`
validation) generates a **broad battery** of inputs per function (varying weight,
sign, dscale, and the near-1 region) and asserts our `(value, text)` equals
PostgreSQL's. Run locally against PG 17.10 (`C:\Program Files\PostgreSQL\17\bin`,
`localhost:5432`, password `postgres`); the rscale formulas are iterated until the
battery is **100%**. In CI the same parity is enforced via the conformance corpus
diffed against PG 18.

Any irreducible last-digit divergence (the table-maker's-dilemma: bigdecimal and
PG rounding the final ULP differently) is **excluded from the corpus and
documented**, never silently shipped.

## Error surface (unchanged from SP33)

`2201E` (ln/log of non-positive), `2201F` (sqrt of negative; `0^negative`;
negative base to a non-integer power), via the existing `TypeError::Domain`. The
float8 paths keep their `22003` overflow behavior.

## Files touched

- `crates/pgtypes/src/numeric.rs` — new `sqrt`/`ln`/`log10`/`exp`/`power` value
  functions + the rscale helpers (`estimate_ln_dweight`, the per-function rscale)
  + their unit tests. (pgtypes is the mutation-baseline crate — exhaustive
  boundary unit tests.)
- `crates/executor/src/func.rs` — `scalar_result_type` returns `numeric` for a
  numeric argument (and the `power` type rule); `eval_eager` dispatches the
  numeric path to the new `pgtypes::numeric` functions, keeping the `f64` path for
  int/float8. `as_f64` stays for the float8 path.
- `crates/conformance/corpus/numeric_transcendental.sql` — new corpus (numeric
  inputs), validated locally vs PG 17.10, diffed vs PG 18 in CI.
- `crates/executor/tests/` — extend the existing `math_string_functions` (or a new
  oracle-gated unit test in pgtypes) for the numeric-typed results + OIDs.
- `Cargo.toml` (workspace) + `crates/pgtypes/Cargo.toml` — add the `dashu-float`
  dependency (pure Rust).
- `CLAUDE.md` — append the SP34 audit; update SP33's deviation note to "retired in
  SP34".

No parser change. **One new dependency: `dashu-float`** (pure Rust, no C/`cc`).

## No Stateright model — deliberate

Identical justification to SP27–SP33: every function is a pure, deterministic
value transform of one already-evaluated `Datum` inside one `eval` on one engine —
no lock, write path, visibility rule, or interleaving. The subtle parts (rscale,
rounding) are *value* properties proven by unit tests + the PG oracle, with no
event ordering to explore.

## Documented deviations / non-goals

- `numeric` `NaN`/`±Infinity` specials remain deferred (SP32 non-goal) — e.g.
  `exp` of a huge value that overflows `bigdecimal`'s context is handled as a
  numeric-format overflow (`22003`), not `Infinity`.
- Two-arg `log(base, x)` remains deferred (only one-arg base-10 `log` ships).
- Any table-maker's-dilemma last-digit case is corpus-excluded + documented.
- `real`/`float4`, date/time types remain deferred.

## Where it lands

Continues on the current branch (PR #52). SP34 is additive commits; the SP33
deviation paragraph in `CLAUDE.md` is updated to point at SP34.
