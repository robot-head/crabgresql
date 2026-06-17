# SP34 — PG-faithful numeric transcendentals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `sqrt`/`ln`/`log`/`exp`/`power` return `numeric` for `numeric` input, matching PostgreSQL's value AND display scale exactly — retiring SP33's float8 deviation.

**Architecture:** Add the pure-Rust `dashu-float` crate (base-10, HalfAway `DBig`) for the arbitrary-precision values; port PostgreSQL's per-function display-scale (rscale) rules (validated against a local PG 17.10 oracle); compute in `pgtypes::numeric`, dispatch from `executor::func` only for numeric inputs (int/float8 keep the f64 path). No Stateright (pure-value carve-out, like SP27–SP33).

**Tech Stack:** Rust 2024, `dashu-float` (new, pure Rust), `bigdecimal` 0.4.10 (existing, for exact integer `powi`), cargo-nextest, conformance corpus vs PostgreSQL.

**Spec:** `docs/superpowers/specs/2026-06-16-crabgresql-sp34-numeric-transcendentals-design.md`

**Local oracle (for validation tasks):** PostgreSQL 17.10 at `C:\Program Files\PostgreSQL\17\bin\psql.exe`, `localhost:5432`, user `postgres`, password `postgres`, db `postgres`. Set `$env:PGPASSWORD="postgres"`.

---

## PostgreSQL reference values (captured from the live oracle — use as expected values)

```
sqrt(2)   = 1.414213562373095      sqrt(4)   = 2.000000000000000
sqrt(200) = 14.142135623730950     sqrt(0.04)= 0.20000000000000000
sqrt(1000000) = 1000.0000000000000
ln(2)  = 0.6931471805599453        ln(10) = 2.3025850929940457
ln(0.5)= -0.6931471805599453       ln(1000000) = 13.815510557964274
ln(0.000001) = -13.815510557964274 ln(0.0001) = -9.2103403719761827
log(100) = 2.0000000000000000      log(2) = 0.3010299956639812
log(1000000) = 6.000000000000000   log(0.5) = -0.3010299956639812
exp(0) = 1.0000000000000000        exp(1) = 2.7182818284590452
exp(2.5) = 12.182493960703473      exp(10) = 22026.465794806717
exp(100) = 26881171418161354484126255515800135873611119
exp(-5) = 0.006737946999085467
power(2,10) = 1024.0000000000000   power(2,3) = 8.0000000000000000
power(3,4) = 81.000000000000000    power(0.5,3) = 0.1250000000000000
power(5,-2) = 0.04000000000000000  power(-2,3) = -8.0000000000000000
power(2,0.5) = 1.4142135623730950  power(2,100) = 1267650600228229401496703205376
power(1.5,2.5) = 2.7556759606310754
```

---

## Task 1: Add dashu-float and a thin arbitrary-precision wrapper

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`), `crates/pgtypes/Cargo.toml`
- Modify: `crates/pgtypes/src/numeric.rs`

- [ ] **Step 1: Add the dependency**

In the root `Cargo.toml` `[workspace.dependencies]` (where `bigdecimal` is), add:

```toml
dashu-float = "0.4"
```

In `crates/pgtypes/Cargo.toml` `[dependencies]` (where `bigdecimal.workspace = true` is), add:

```toml
dashu-float.workspace = true
```

Run `cargo build -p pgtypes` and confirm it resolves a pure-Rust build (no `cc`/native step). If `0.4` does not expose `Context::<HalfAway>::new` + `exp`/`ln`/`sqrt`/`powf`, use the latest `dashu-float` that does and note the version.

- [ ] **Step 2: Write the failing test** (known mathematical values — independent of PG's display scale)

Add to the `tests` module in `crates/pgtypes/src/numeric.rs`:

```rust
    #[test]
    fn dashu_wrappers_compute_known_values() {
        // 40 significant digits is plenty for these checks.
        let p = 40;
        // ln(e) = 1, exp(0) = 1, exp(1) = e, sqrt(2)^2 = 2, ln(1) = 0.
        assert_eq!(bf_to_text(&bf_exp(&num_to_bf("0", p), p)), "1");
        // sqrt(2) starts 1.41421356237309504880…
        let s2 = bf_to_text(&bf_sqrt(&num_to_bf("2", p), p).expect("sqrt"));
        assert!(s2.starts_with("1.4142135623730950488"), "got {s2}");
        // ln(2) starts 0.69314718055994530941…
        let l2 = bf_to_text(&bf_ln(&num_to_bf("2", p), p).expect("ln"));
        assert!(l2.starts_with("0.6931471805599453094"), "got {l2}");
        // powf(2, 0.5) ≈ sqrt(2)
        let p2 = bf_to_text(&bf_powf(&num_to_bf("2", p), &num_to_bf("0.5", p), p).expect("powf"));
        assert!(p2.starts_with("1.4142135623730950488"), "got {p2}");
        // ln of a non-positive value is rejected.
        assert!(bf_ln(&num_to_bf("0", p), p).is_none());
        assert!(bf_sqrt(&num_to_bf("-1", p), p).is_none());
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo nextest run -p pgtypes dashu_wrappers_compute_known_values`
Expected: FAIL to compile (`bf_*` not defined).

- [ ] **Step 4: Implement the wrapper**

Add to `crates/pgtypes/src/numeric.rs` (top-level). This isolates the dashu-float API behind small `BigDecimal`-free helpers operating on `dashu_float::DBig`. Adjust the exact dashu calls to the installed version's API if they differ — the test above is the contract.

```rust
use dashu_float::DBig;
use dashu_float::round::mode::HalfAway;
type DCtx = dashu_float::Context<HalfAway>;

/// Parse a plain-decimal string into a DBig (used to bridge from BigDecimal text).
fn num_to_bf(s: &str, _prec: usize) -> DBig {
    use core::str::FromStr;
    DBig::from_str(s).expect("valid decimal text")
}

/// A DBig back to a plain-decimal string (DBig's Display is plain decimal).
fn bf_to_text(x: &DBig) -> String {
    x.to_string()
}

fn dctx(prec: usize) -> DCtx {
    DCtx::new(prec)
}

/// exp(x) at `prec` significant digits.
fn bf_exp(x: &DBig, prec: usize) -> DBig {
    dctx(prec).exp(x.repr()).value()
}

/// ln(x) at `prec` sig digits; None for x <= 0.
fn bf_ln(x: &DBig, prec: usize) -> Option<DBig> {
    if x <= &DBig::ZERO {
        return None;
    }
    Some(dctx(prec).ln(x.repr()).value())
}

/// sqrt(x) at `prec` sig digits; None for x < 0.
fn bf_sqrt(x: &DBig, prec: usize) -> Option<DBig> {
    if x < &DBig::ZERO {
        return None;
    }
    Some(dctx(prec).sqrt(x.repr()).value())
}

/// powf(base, exp) at `prec` sig digits (caller guarantees base > 0).
fn bf_powf(base: &DBig, exp: &DBig, prec: usize) -> Option<DBig> {
    if base <= &DBig::ZERO {
        return None;
    }
    Some(dctx(prec).powf(base.repr(), exp.repr()).value())
}
```

If `Rounded` has no `.value()` in the installed version, unwrap via its `Approximation` pattern (`Exact(v) | Inexact(v, _) => v`). If `DBig::ZERO` is absent, use `DBig::from_str("0")`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo nextest run -p pgtypes dashu_wrappers_compute_known_values`
Expected: PASS. Then `cargo nextest run -p pgtypes` (no regressions).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p pgtypes
git add Cargo.toml crates/pgtypes/Cargo.toml crates/pgtypes/src/numeric.rs
git commit -m "SP34: add dashu-float + arbitrary-precision exp/ln/sqrt/powf wrappers"
```

---

## Task 2: rscale helpers (decimal weight + estimate_ln_dweight + per-function rscale)

**Files:**
- Modify: `crates/pgtypes/src/numeric.rs`

- [ ] **Step 1: Write the failing test** (validated against PG; see reference table)

Add to the `tests` module:

```rust
    #[test]
    fn rscale_rules_match_postgres() {
        let n = |s: &str| parse(s).expect("parse");
        // sqrt: rscale = clamp(16 - (w*2 + 1), max(dscale,0), 1000)
        assert_eq!(sqrt_rscale(&n("2")), 15);
        assert_eq!(sqrt_rscale(&n("1000000")), 13);
        assert_eq!(sqrt_rscale(&n("0.04")), 17);
        // exp: rscale = clamp(16 - trunc(val * 0.4342944819), 0, 1000)
        assert_eq!(exp_rscale(&n("1")), 16);
        assert_eq!(exp_rscale(&n("2.5")), 15);
        assert_eq!(exp_rscale(&n("10")), 12);
        assert_eq!(exp_rscale(&n("100")), 0);
        assert_eq!(exp_rscale(&n("-5")), 18);
        // ln/log: rscale = clamp(16 - estimate_ln_dweight, max(dscale,0), 1000)
        assert_eq!(ln_rscale(&n("2")), 16);
        assert_eq!(ln_rscale(&n("1000000")), 15);
        assert_eq!(ln_rscale(&n("0.000001")), 15);
        assert_eq!(ln_rscale(&n("0.0001")), 16);
        assert_eq!(ln_rscale(&n("1000000000000")), 15);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo nextest run -p pgtypes rscale_rules_match_postgres`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the rscale helpers**

Add to `crates/pgtypes/src/numeric.rs`. `DEC_DIGITS` and `nbase_weight_and_lead` already exist. `MIN_SIG_DIGITS = 16` already exists. Reuse them.

```rust
/// PostgreSQL clamp bounds for a transcendental result display scale.
const TRANSC_MAX_SCALE: i64 = 1000;

/// The decimal weight of a value's leading significant digit (position of the
/// leading nonzero digit as a power of ten): e.g. 1234 -> 3, 0.0067 -> -3, 0 -> 0.
fn decimal_weight(bd: &BigDecimal) -> i64 {
    if is_zero(bd) {
        return 0;
    }
    let (mant, scale) = bd.as_bigint_and_exponent();
    let digits = mant.to_string();
    let len = digits.trim_start_matches('-').len() as i64;
    len - 1 - scale
}

/// sqrt rscale (PostgreSQL sqrt_var): sweight = w*DEC_DIGITS/2 + 1.
fn sqrt_rscale(arg: &BigDecimal) -> i64 {
    let (w, _) = nbase_weight_and_lead(arg);
    let sweight = w * DEC_DIGITS / 2 + 1;
    (MIN_SIG_DIGITS - sweight)
        .max(arg.fractional_digit_count().max(0))
        .max(0)
        .min(TRANSC_MAX_SCALE)
}

/// exp rscale (PostgreSQL exp_var): ln_dweight = trunc(val * log10(e)).
fn exp_rscale(arg: &BigDecimal) -> i64 {
    let val = arg.to_f64().unwrap_or(0.0);
    let ln_dweight = (val * 0.4342944819032518) as i64; // C-style truncation toward zero
    (MIN_SIG_DIGITS - ln_dweight).max(0).min(TRANSC_MAX_SCALE)
}

/// PostgreSQL estimate_ln_dweight: an estimate of the decimal weight of ln(arg).
fn estimate_ln_dweight(arg: &BigDecimal) -> i64 {
    let dw = decimal_weight(arg);
    if dw == 0 {
        0
    } else {
        let est = ((dw.unsigned_abs() as f64) * 2.302585092994046).log10().floor() as i64;
        est.max(0)
    }
}

/// ln/log (base-10) rscale (PostgreSQL ln_var / log_var with base 10).
fn ln_rscale(arg: &BigDecimal) -> i64 {
    (MIN_SIG_DIGITS - estimate_ln_dweight(arg))
        .max(arg.fractional_digit_count().max(0))
        .max(0)
        .min(TRANSC_MAX_SCALE)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo nextest run -p pgtypes rscale_rules_match_postgres`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p pgtypes
git add crates/pgtypes/src/numeric.rs
git commit -m "SP34: PG-faithful rscale rules for sqrt/exp/ln/log (oracle-validated)"
```

---

## Task 3: numeric transcendental functions (value + rscale)

**Files:**
- Modify: `crates/pgtypes/src/numeric.rs`

- [ ] **Step 1: Write the failing test** (expected = PG reference values above)

Add to the `tests` module:

```rust
    #[test]
    fn numeric_transcendentals_match_postgres() {
        let t = |bd: &BigDecimal| to_text(bd);
        let n = |s: &str| parse(s).expect("parse");
        assert_eq!(t(&num_sqrt(&n("2")).expect("sqrt")), "1.414213562373095");
        assert_eq!(t(&num_sqrt(&n("4")).expect("sqrt")), "2.000000000000000");
        assert_eq!(t(&num_sqrt(&n("0.04")).expect("sqrt")), "0.20000000000000000");
        assert!(num_sqrt(&n("-1")).is_none());
        assert_eq!(t(&num_ln(&n("2")).expect("ln")), "0.6931471805599453");
        assert_eq!(t(&num_ln(&n("1000000")).expect("ln")), "13.815510557964274");
        assert!(num_ln(&n("0")).is_none());
        assert_eq!(t(&num_log10(&n("100")).expect("log")), "2.0000000000000000");
        assert_eq!(t(&num_log10(&n("1000000")).expect("log")), "6.000000000000000");
        assert_eq!(t(&num_exp(&n("0"))), "1.0000000000000000");
        assert_eq!(t(&num_exp(&n("1"))), "2.7182818284590452");
        assert_eq!(t(&num_exp(&n("10"))), "22026.465794806717");
        // power: exact integer exponent, and non-integer via powf
        assert_eq!(t(&num_power(&n("2"), &n("10")).expect("pow")), "1024.0000000000000");
        assert_eq!(t(&num_power(&n("2"), &n("3")).expect("pow")), "8.0000000000000000");
        assert_eq!(t(&num_power(&n("-2"), &n("3")).expect("pow")), "-8.0000000000000000");
        assert_eq!(t(&num_power(&n("5"), &n("-2")).expect("pow")), "0.04000000000000000"); // negative integer exponent
        assert_eq!(t(&num_power(&n("3"), &n("4")).expect("pow")), "81.000000000000000");
        assert_eq!(t(&num_power(&n("2"), &n("100")).expect("pow")), "1267650600228229401496703205376");
        assert_eq!(t(&num_power(&n("2"), &n("0.5")).expect("pow")), "1.4142135623730950");
        assert!(num_power(&n("0"), &n("-1")).is_none()); // 0^negative
        assert!(num_power(&n("-2"), &n("0.5")).is_none()); // negative^non-integer
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo nextest run -p pgtypes numeric_transcendentals_match_postgres`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the functions**

Add to `crates/pgtypes/src/numeric.rs`. Each computes the dashu value at a precision derived from the rscale, converts to `BigDecimal`, and rounds to the rscale half-away. A `None` return means a domain error (the executor maps it to `2201E`/`2201F`).

```rust
/// Significant-digit precision for the dashu computation: enough to cover the
/// result's integer digits plus the requested fractional rscale plus guard.
fn transc_prec(result_dweight: i64, rscale: i64) -> usize {
    (result_dweight.max(0) + rscale + 16).max(24) as usize
}

/// Round a dashu DBig result to `rscale` fractional digits, half-away.
fn finish_transc(value: DBig, rscale: i64) -> BigDecimal {
    let bd = parse(&bf_to_text(&value)).unwrap_or_else(|| BigDecimal::from(0));
    canonical(bd.with_scale_round(rscale, RoundingMode::HalfUp))
}

/// numeric sqrt; None for a negative argument (caller -> 2201F).
pub fn num_sqrt(arg: &BigDecimal) -> Option<BigDecimal> {
    if is_zero(arg) {
        return Some(canonical(BigDecimal::from(0).with_scale_round(sqrt_rscale(arg), RoundingMode::HalfUp)));
    }
    let rscale = sqrt_rscale(arg);
    let prec = transc_prec(decimal_weight(arg) / 2, rscale);
    let v = bf_sqrt(&num_to_bf(&to_text(arg), prec), prec)?;
    Some(finish_transc(v, rscale))
}

/// numeric ln; None for arg <= 0 (caller -> 2201E).
pub fn num_ln(arg: &BigDecimal) -> Option<BigDecimal> {
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale);
    let v = bf_ln(&num_to_bf(&to_text(arg), prec), prec)?;
    Some(finish_transc(v, rscale))
}

/// numeric log base 10; None for arg <= 0 (caller -> 2201E).
pub fn num_log10(arg: &BigDecimal) -> Option<BigDecimal> {
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale) + 8;
    let x = num_to_bf(&to_text(arg), prec);
    let ten = num_to_bf("10", prec);
    let lnx = bf_ln(&x, prec)?;
    let ln10 = bf_ln(&ten, prec).expect("ln(10) defined");
    let v = dctx(prec).div(lnx.repr(), ln10.repr()).value(); // lnx / ln10
    Some(finish_transc(v, rscale))
}

/// numeric exp (never a domain error; a magnitude beyond numeric format -> caller maps overflow).
pub fn num_exp(arg: &BigDecimal) -> BigDecimal {
    let rscale = exp_rscale(arg);
    // result decimal weight ~ trunc(val * log10(e))
    let result_dweight = (arg.to_f64().unwrap_or(0.0) * 0.4342944819032518) as i64;
    let prec = transc_prec(result_dweight, rscale);
    let v = bf_exp(&num_to_bf(&to_text(arg), prec), prec);
    finish_transc(v, rscale)
}

/// numeric power; None on a domain error (0^neg, negative^non-integer -> caller -> 2201F).
pub fn num_power(base: &BigDecimal, exp: &BigDecimal) -> Option<BigDecimal> {
    // x = 0 cases
    if is_zero(base) {
        if exp.sign() == bigdecimal::num_bigint::Sign::Minus { return None; } // 0^negative
        if is_zero(exp) { return Some(power_finish(BigDecimal::from(1), base, exp)); }
        return Some(power_finish(BigDecimal::from(0), base, exp));
    }
    // integer exponent -> exact powi (handles negative base)
    if let Some(e) = exp_as_i64(exp) {
        let exact = base.powi(e); // BigDecimal::powi handles negative exponent via inverse
        return Some(power_finish(exact, base, exp));
    }
    // non-integer exponent: base must be > 0
    if base.sign() == bigdecimal::num_bigint::Sign::Minus {
        return None; // negative^non-integer -> 2201F
    }
    // result weight estimate ~ exp * decimal_weight(base)
    let rweight = (exp.to_f64().unwrap_or(0.0) * decimal_weight(base) as f64) as i64;
    let rscale = (MIN_SIG_DIGITS - rweight).max(0).min(TRANSC_MAX_SCALE);
    let prec = transc_prec(rweight, rscale);
    let v = bf_powf(&num_to_bf(&to_text(base), prec), &num_to_bf(&to_text(exp), prec), prec)?;
    Some(finish_transc(v, rscale))
}

/// Is `exp` an exact integer? Returns it as i64 if so (bounded).
fn exp_as_i64(exp: &BigDecimal) -> Option<i64> {
    if exp.fractional_digit_count() <= 0 || exp.is_integer() {
        to_i64(exp).ok()
    } else {
        None
    }
}

/// Round an exact power result to PG's rscale (result-weight based).
fn power_finish(value: BigDecimal, _base: &BigDecimal, _exp: &BigDecimal) -> BigDecimal {
    let rweight = decimal_weight(&value);
    let rscale = (MIN_SIG_DIGITS - rweight).max(0).min(TRANSC_MAX_SCALE);
    canonical(value.with_scale_round(rscale, RoundingMode::HalfUp))
}
```

NOTE: `BigDecimal::is_integer` / `sign` / `num_bigint::Sign` — if a name differs in 0.4.10, use the equivalent (`exp.fractional_digit_count() <= 0` already detects integers; sign via comparison to `BigDecimal::from(0)`). `dctx(prec).div(...)` — if dashu's `Context` lacks `div`, divide via `lnx / ln10` using `DBig`'s `Div` then re-round, or compute `ln10` once and reuse. The unit-test expected values are the contract; adjust the API calls to satisfy them.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo nextest run -p pgtypes numeric_transcendentals_match_postgres`
Expected: PASS. Iterate the `power` rscale (the result-weight rule) until the `power(...)` assertions pass; if `power(2,3)` etc. disagree, adjust `power_finish`'s rweight handling (e.g. `power(0.5,3)=0.1250000000000000` is 16 decimals, weight −1 → rscale 16, so use `16 - max(rweight, ...)`; finalize against these captured values).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p pgtypes
git add crates/pgtypes/src/numeric.rs
git commit -m "SP34: numeric sqrt/ln/log/exp/power values at PG display scale"
```

---

## Task 4: oracle-validation harness (the correctness gate)

**Files:**
- Create: `crates/pgtypes/tests/numeric_transcendental_oracle.rs` (ignored by default; run locally)

- [ ] **Step 1: Write the oracle test**

Create `crates/pgtypes/tests/numeric_transcendental_oracle.rs`. It is `#[ignore]` (needs a live PG); run it locally to finalize/confirm the rscale rules. It shells out to `psql` and compares text.

```rust
//! SP34: validate numeric transcendentals against a live PostgreSQL oracle.
//! Ignored by default (needs PG). Run locally:
//!   $env:PGPASSWORD="postgres"; cargo nextest run -p pgtypes --test numeric_transcendental_oracle --run-ignored all
use std::process::Command;

fn pg(sql: &str) -> String {
    let out = Command::new(r"C:\Program Files\PostgreSQL\17\bin\psql.exe")
        .args(["-U", "postgres", "-h", "localhost", "-d", "postgres", "-t", "-A", "-c", sql])
        .output()
        .expect("psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn ours(expr: &str, arg: &str) -> String {
    use pgtypes::numeric::*; // re-exported helpers; expose pub(crate)->pub or a test shim
    let a = parse(arg).expect("parse");
    let bd = match expr {
        "sqrt" => num_sqrt(&a).expect("sqrt"),
        "ln" => num_ln(&a).expect("ln"),
        "log" => num_log10(&a).expect("log"),
        "exp" => num_exp(&a),
        _ => unreachable!(),
    };
    to_text(&bd)
}

#[test]
#[ignore]
fn transcendentals_match_oracle_battery() {
    let args = [
        "2", "3", "4", "5", "10", "50", "200", "1000", "1000000", "1000000000000",
        "0.5", "0.04", "0.0001", "0.000001", "1.5", "2.5", "1.05", "0.99",
    ];
    let mut mismatches = Vec::new();
    for f in ["sqrt", "ln", "log", "exp"] {
        for a in args {
            // skip domain-invalid combos
            let want = pg(&format!("SELECT {f}({a}::numeric)::text"));
            if want.is_empty() { continue; }
            let got = ours(f, a);
            if want != got {
                mismatches.push(format!("{f}({a}): pg={want} ours={got}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "oracle mismatches:\n{}", mismatches.join("\n"));
}
```

- [ ] **Step 2: Run it locally and iterate to zero mismatches**

Run: `$env:PGPASSWORD="postgres"; cargo nextest run -p pgtypes --test numeric_transcendental_oracle --run-ignored all`
Expected: PASS (0 mismatches). If any mismatch is a stable rscale gap, fix the rscale rule in Task 2/3 and re-run. If any is an irreducible last-digit (table-maker's-dilemma) case, record it for corpus exclusion (do NOT weaken a rule to chase one ULP). Add the `power` function to the battery similarly once Task 3's power passes the unit values.

- [ ] **Step 3: Commit**

```bash
cargo fmt -p pgtypes
git add crates/pgtypes/tests/numeric_transcendental_oracle.rs crates/pgtypes/src/numeric.rs
git commit -m "SP34: oracle-validation harness for numeric transcendentals (ignored; gates rscale)"
```

---

## Task 5: executor wiring — numeric result type + dispatch

**Files:**
- Modify: `crates/executor/src/func.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/executor/src/func.rs`:

```rust
    #[test]
    fn transcendentals_are_numeric_for_numeric_input() {
        let num = |s: &str| Datum::Numeric(pgtypes::numeric::parse(s).expect("n"));
        // numeric in -> numeric out (PG display scale)
        assert_eq!(ev("sqrt(2.0)"), num("1.41421356237310")); // 2.0 dscale 1 -> rscale per rule
        assert_eq!(ev("sqrt(4.0)"), num("2.0000000000000000")); // value check below via wire/oracle
        assert_eq!(ev("exp(1.0)"), num("2.7182818284590452"));
        assert_eq!(ev("ln(2.0)"), num("0.6931471805599453"));
        assert_eq!(ev("power(2.0, 3.0)"), num("8.0000000000000000"));
        // int in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        // float8 in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4.0::float8)"), Datum::Float8(2.0));
    }

    #[test]
    fn transcendental_result_types() {
        let t = table_n(); // table with a numeric column
        let ty = |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), Some(&t)).expect("ty");
        assert_eq!(ty("sqrt(qn)"), ColumnType::Numeric(None)); // qn is numeric
        assert_eq!(ty("ln(qn)"), ColumnType::Numeric(None));
        assert_eq!(ty("sqrt(4)"), ColumnType::Float8);        // int literal
        assert_eq!(ty("sqrt(4.0::float8)"), ColumnType::Float8);
        assert_eq!(ty("power(qn, 2)"), ColumnType::Numeric(None)); // numeric base
        assert_eq!(ty("power(2, 3)"), ColumnType::Float8);          // all-int
    }
```

Add a `table_n()` helper near `table()` in the tests module:

```rust
    fn table_n() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column { name: "qn".into(), ty: ColumnType::Numeric(None) }],
        }
    }
```

(For the exact expected `sqrt(2.0)` / `sqrt(4.0)` strings, use the value the PG-validated `pgtypes::numeric` functions return — confirm by computing `pgtypes::numeric::num_sqrt(&parse("2.0"))` once; the point of this test is the TYPE and the numeric-vs-float8 routing, so keep value asserts to the few stable ones and rely on Task 6's wire/corpus for exhaustive value parity.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo nextest run -p executor transcendental`
Expected: FAIL (currently returns float8 for numeric input).

- [ ] **Step 3: Implement the type rule + dispatch**

In `crates/executor/src/func.rs`, change `scalar_result_type` for the transcendental arms so a numeric argument yields numeric, and `power`'s rule per the spec:

```rust
        ScalarFunc::Sqrt | ScalarFunc::Exp | ScalarFunc::Ln | ScalarFunc::Log => {
            require_arity(fc, n == 1)?;
            let at = require_numeric(&args[0], table)?;
            Ok(if at.is_numeric() { ColumnType::Numeric(None) } else { ColumnType::Float8 })
        }
        ScalarFunc::Power => {
            require_arity(fc, n == 2)?;
            let a = require_numeric(&args[0], table)?;
            let b = require_numeric(&args[1], table)?;
            Ok(power_result_type(a, b))
        }
        ScalarFunc::Pi => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Float8)
        }
```

Add `power_result_type` (module scope in func.rs):

```rust
/// PostgreSQL power result type: float8 if any operand is float8; else numeric if
/// any operand is numeric; else float8 (all-int, PG's preferred type).
fn power_result_type(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Float8 || b == ColumnType::Float8 {
        ColumnType::Float8
    } else if a.is_numeric() || b.is_numeric() {
        ColumnType::Numeric(None)
    } else {
        ColumnType::Float8
    }
}
```

In `eval_eager`, route a numeric argument to the new `pgtypes::numeric` functions, keeping the f64 path otherwise. Replace the SP33 transcendental arms with:

```rust
        ScalarFunc::Sqrt => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return pgtypes::numeric::num_sqrt(d)
                    .map(Datum::Numeric)
                    .ok_or_else(|| domain("2201F", "cannot take square root of a negative number"));
            }
            let x = as_f64(&vals[0])?;
            if x < 0.0 { return Err(domain("2201F", "cannot take square root of a negative number")); }
            Ok(Datum::Float8(x.sqrt()))
        }
        ScalarFunc::Exp => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return Ok(Datum::Numeric(pgtypes::numeric::num_exp(d)));
            }
            finite_or_overflow(as_f64(&vals[0])?.exp())
        }
        ScalarFunc::Ln => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return pgtypes::numeric::num_ln(d)
                    .map(Datum::Numeric)
                    .ok_or_else(|| domain("2201E", "cannot take logarithm of a non-positive number"));
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 { return Err(domain("2201E", "cannot take logarithm of a non-positive number")); }
            Ok(Datum::Float8(x.ln()))
        }
        ScalarFunc::Log => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return pgtypes::numeric::num_log10(d)
                    .map(Datum::Numeric)
                    .ok_or_else(|| domain("2201E", "cannot take logarithm of a non-positive number"));
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 { return Err(domain("2201E", "cannot take logarithm of a non-positive number")); }
            Ok(Datum::Float8(x.log10()))
        }
        ScalarFunc::Power => {
            require_arity(fc, vals.len() == 2)?;
            if matches!(&vals[0], Datum::Numeric(_)) || matches!(&vals[1], Datum::Numeric(_)) {
                if !matches!(&vals[0], Datum::Float8(_)) && !matches!(&vals[1], Datum::Float8(_)) {
                    let b = to_numeric(&vals[0])?;
                    let e = to_numeric(&vals[1])?;
                    return pgtypes::numeric::num_power(&b, &e)
                        .map(Datum::Numeric)
                        .ok_or_else(|| domain("2201F", "invalid argument for power function"));
                }
            }
            power(as_f64(&vals[0])?, as_f64(&vals[1])?)
        }
        ScalarFunc::Pi => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Float8(std::f64::consts::PI))
        }
```

Add `to_numeric` (promotes an int Datum to numeric for the mixed numeric/int power path):

```rust
/// Promote an int4/int8/numeric Datum to a numeric BigDecimal (for the numeric
/// power path, where one operand may be an integer).
fn to_numeric(d: &Datum) -> Result<bigdecimal::BigDecimal, ExecError> {
    match d {
        Datum::Int4(n) => Ok(pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int8(n) => Ok(pgtypes::numeric::from_i64(*n)),
        Datum::Numeric(d) => Ok(d.clone()),
        other => Err(type_error("power", other)),
    }
}
```

Ensure the `pgtypes::numeric` functions used (`num_sqrt`/`num_ln`/`num_log10`/`num_exp`/`num_power`/`from_i64`/`parse`/`to_text`) are `pub`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo nextest run -p executor transcendental`
Expected: PASS. Then `cargo nextest run -p executor --lib` (no regressions) and `cargo clippy -p executor --all-targets -- -D warnings`.

Note: SP33's `transcendental_family_returns_float8` test asserts `sqrt(2.25)` etc. on bare numeric literals returning `Float8`. Those bare literals are now numeric, so UPDATE that SP33 test to use `::float8` casts (e.g. `sqrt(2.25::float8)`) where it intends the float8 path, matching how SP32 updated SP30's float8 assumptions.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p executor
git add crates/executor/src/func.rs
git commit -m "SP34: route numeric-input transcendentals to numeric results (executor)"
```

---

## Task 6: conformance corpus + wire test

**Files:**
- Create: `crates/conformance/corpus/numeric_transcendental.sql`
- Modify: `crates/executor/tests/math_string_functions.rs`

- [ ] **Step 1: Create the corpus** (numeric inputs; validated locally against PG)

Create `crates/conformance/corpus/numeric_transcendental.sql`:

```sql
-- SP34: numeric transcendentals (sqrt/ln/log/exp/power) return numeric for
-- numeric input, matching PostgreSQL's value AND display scale. Diffed vs PG 18
-- in CI; validated locally vs PG 17.10. ASCII + ORDER BY-stable.
CREATE TABLE nt (id int4, x numeric);
INSERT INTO nt VALUES (1, 2), (2, 4), (3, 100), (4, 0.04), (5, 1000000);

SELECT sqrt(2::numeric), sqrt(4::numeric), sqrt(0.04::numeric);
SELECT ln(2::numeric), ln(10::numeric), ln(1000000::numeric);
SELECT log(100::numeric), log(1000000::numeric);
SELECT exp(0::numeric), exp(1::numeric), exp(10::numeric);
SELECT power(2::numeric, 10::numeric), power(2::numeric, 3::numeric);
SELECT power(2::numeric, 100::numeric), power(-2::numeric, 3::numeric);
SELECT power(2::numeric, 0.5::numeric);
SELECT id, sqrt(x), ln(x) FROM nt ORDER BY id;
```

- [ ] **Step 2: Validate the corpus locally against PG 17.10**

Build and run the differential (start the server, stage the file, run conformance) exactly as recorded in the memory note "Validate conformance corpus locally vs PG 17":

```
cargo build -p crabgresql -p conformance
# start: Start-Process .\target\debug\crabgresql.exe -ArgumentList "--listen","127.0.0.1:54333" -WindowStyle Hidden ; wait for port 54333
# stage only numeric_transcendental.sql into a temp dir, then:
.\target\debug\conformance.exe --oracle-url "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres" --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" --corpus <tempdir> --out p.json --summary p.md
```

Expected: `parity: 100.0%`. If a row diverges by a last digit (table-maker's-dilemma), remove that specific value from the corpus and document it; if a row diverges by scale, fix the rscale rule (Task 2/3) and re-run.

- [ ] **Step 3: Add wire assertions for the numeric result type**

Append to `crates/executor/tests/math_string_functions.rs` a test:

```rust
#[tokio::test]
async fn numeric_transcendentals_over_the_wire() {
    let port = spawn().await;
    let client = connect(port).await;
    // numeric in -> numeric out, PG display scale, via text protocol
    assert_eq!(scalar(&client, "SELECT sqrt(2::numeric)").await.as_deref(), Some("1.414213562373095"));
    assert_eq!(scalar(&client, "SELECT ln(2::numeric)").await.as_deref(), Some("0.6931471805599453"));
    assert_eq!(scalar(&client, "SELECT exp(1::numeric)").await.as_deref(), Some("2.7182818284590452"));
    assert_eq!(scalar(&client, "SELECT power(2::numeric, 100::numeric)").await.as_deref(), Some("1267650600228229401496703205376"));
    // result OID is numeric (1700) for numeric input, float8 (701) for int input
    let rows = client.query("SELECT sqrt(2::numeric), sqrt(4)", &[]).await.expect("q");
    assert_eq!(rows[0].columns()[0].type_().oid(), 1700);
    assert_eq!(rows[0].columns()[1].type_().oid(), 701);
}
```

- [ ] **Step 4: Run the wire test**

Run: `cargo nextest run -p executor --test math_string_functions`
Expected: PASS (all tests, including the new one).

- [ ] **Step 5: Commit**

```bash
git add crates/conformance/corpus/numeric_transcendental.sql crates/executor/tests/math_string_functions.rs
git commit -m "SP34: numeric-transcendental conformance corpus + wire test"
```

---

## Task 7: CLAUDE.md audit + full gauntlet

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the SP33 deviation note + add the SP34 paragraph**

In `CLAUDE.md`, in the SP33 paragraph, change the deviation sentence "transcendental functions return float8 for every numeric input (PG returns numeric for numeric input — …)" to note "(retired in SP34 — numeric input now returns numeric)". Then append a new paragraph after SP33:

```markdown
**SP34 (2026-06-16):** retires SP33's one deviation — **`sqrt`/`ln`/`log`/`exp`/`power` now return `numeric` for `numeric` input** (PG-faithful value AND display scale); int/float8 inputs keep the float8 path (PG resolves `sqrt(4)`/`sqrt(x::float8)` to float8). **One new dependency: `dashu-float`** (pure Rust, no C/`cc`) — its base-10 HalfAway `DBig` supplies arbitrary-precision `exp`/`ln`/`sqrt`/`powf` (bigdecimal 0.4.10 lacks `ln`/`log10`); chosen over hand-rolling per the "prefer vetted crates for hard subsystems" rule. `pgtypes::numeric` gains `num_sqrt`/`num_ln`/`num_log10`/`num_exp`/`num_power` (value via dashu at a derived precision, then rounded half-away to PG's rscale) plus the rscale rules (`sqrt_var`/`exp_var`/`ln_var`/`log_var`/`power_var`, reverse-engineered from PG source + validated against a live PostgreSQL 17.10 oracle: sqrt `16-(w*2+1)`, exp `16-trunc(val*log10(e))`, ln/log `16-estimate_ln_dweight`; integer-exponent `power` is exact via `BigDecimal::powi`, non-integer via dashu `powf`). Executor: `scalar_result_type` returns numeric for a numeric arg (and the `power` numeric/float8/int resolution); `eval_eager` routes numeric inputs to the `pgtypes::numeric` functions, keeping the f64 path for int/float8. Error surface unchanged (`2201E`/`2201F` via `TypeError::Domain`). **NO Stateright model** (pure-value carve-out, identical to SP27–SP33). Proven by `pgtypes::numeric` unit tests (rscale rules + values vs PG-captured references), the ignored `pgtypes::numeric_transcendental_oracle` battery (validated 100% locally vs PG 17.10), `executor::func` unit tests (numeric-vs-float8 routing + result types), the `executor::math_string_functions` wire test (numeric OIDs), and `conformance/corpus/numeric_transcendental.sql` (validated locally; diffed vs PG 18 in CI). **Documented deviations:** numeric `NaN`/`±Infinity` specials remain deferred (an `exp` overflow beyond numeric format is `22003`); two-arg `log(base,x)` deferred; any irreducible table-maker's-dilemma last-digit case is corpus-excluded + documented. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.
```

- [ ] **Step 2: Run the full gauntlet**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
```

Expected: fmt clean, clippy clean, all nextest tests pass (the oracle test is `#[ignore]`, so it does not run here), doctests pass.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "SP34: CLAUDE.md audit — numeric transcendentals retire the SP33 float8 deviation"
```

---

## Self-review notes

- **Spec coverage:** dependency + wrapper (Task 1), rscale rules (Task 2), values (Task 3), oracle gate (Task 4), executor type/dispatch (Task 5), corpus + wire (Task 6), audit + gauntlet (Task 7). Type rules (numeric-in→numeric, power resolution) in Task 5; error codes preserved in Task 5.
- **Type consistency:** `pgtypes::numeric` exposes `num_sqrt`/`num_ln`/`num_log10`/`num_exp`/`num_power` (returning `Option<BigDecimal>`, `None` = domain error; `num_exp` returns `BigDecimal`); helpers `sqrt_rscale`/`exp_rscale`/`ln_rscale`/`estimate_ln_dweight`/`decimal_weight`/`transc_prec`/`finish_transc`; executor `power_result_type`/`to_numeric`. Names match across tasks.
- **Known-empirical items (resolved by tests, not placeholders):** the exact dashu-float API calls (Task 1, gated by known-value test) and the `power` rscale (Task 3/4, gated by PG-captured values + oracle). Both have concrete acceptance criteria.
- **Risk:** table-maker's-dilemma last-digit mismatches — handled by corpus exclusion + documentation, never by weakening a rule.
```
