//! SP32: arbitrary-precision exact `numeric` / `decimal` (OID 1700), backed by
//! `bigdecimal::BigDecimal`. This module is the value layer for numeric: parsing,
//! PostgreSQL-faithful text + binary output, the arithmetic scale rules
//! (`select_div_scale` for division/AVG), rounding, `numeric(p,s)` typmod
//! enforcement, and the casts to/from the other types.
//!
//! Invariant: every numeric `Datum` is **canonical** — its display scale (dscale)
//! is `>= 0`, matching PostgreSQL (a literal like `1e3` parses to scale 0, not the
//! negative scale `bigdecimal` would otherwise keep). The deferred non-goals are
//! the `NaN`/`±Infinity` specials (so no special-value propagation here).

use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};

use crate::TypeError;

/// PostgreSQL `numeric` type OID.
pub const OID: u32 = 1700;

/// PostgreSQL division/AVG significant-digit floor (`NUMERIC_MIN_SIG_DIGITS`) and
/// the base-10000 digit width (`DEC_DIGITS`).
const MIN_SIG_DIGITS: i64 = 16;
const DEC_DIGITS: i64 = 4;
const MAX_DISPLAY_SCALE: i64 = 1000;

/// PostgreSQL's hard numeric-format limits: at most `131072` digits before the
/// decimal point (leading-digit weight ≤ `131071`) and `16383` after it. A value
/// outside these "overflows numeric format" — PostgreSQL rejects it, and so do we
/// (which ALSO bounds materialization: a literal like `8e88888888` would otherwise
/// expand to ~88M digits and OOM, as the `decode_row` fuzzer found).
const MAX_WEIGHT: i64 = 131071;
const MAX_DSCALE: i64 = 16383;

/// Optional `numeric(precision, scale)` type modifier. Absent = unconstrained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Typmod {
    pub precision: u16,
    pub scale: u16,
}

/// Canonicalize a `BigDecimal` to a PostgreSQL dscale (`>= 0`). A negative scale
/// (e.g. from `1e3`) is materialized to scale 0 (exact — only appends zeros).
pub fn canonical(bd: BigDecimal) -> BigDecimal {
    if bd.fractional_digit_count() < 0 {
        bd.with_scale(0)
    } else {
        bd
    }
}

/// Parse a numeric literal / text value (PostgreSQL `numeric_in`, minus the
/// deferred `NaN`/`Infinity` spellings). Leading/trailing whitespace is trimmed.
/// Returns `None` on bad syntax OR a value that overflows the numeric format
/// (the caller maps either to an error). The overflow check runs BEFORE
/// [`canonical`], whose `with_scale` would otherwise materialize an adversarial
/// exponent's digits and OOM.
pub fn parse(s: &str) -> Option<BigDecimal> {
    use std::str::FromStr;
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    let bd = BigDecimal::from_str(t).ok()?;
    if !within_format_limits(&bd) {
        return None;
    }
    Some(canonical(bd))
}

/// Is `bd` within PostgreSQL's numeric-format limits (weight ≤ 131071, dscale ≤
/// 16383)? Computed from the compact `(mantissa, exponent)` form WITHOUT
/// materializing, so an extreme exponent is rejected cheaply.
fn within_format_limits(bd: &BigDecimal) -> bool {
    let (mant, exp) = bd.as_bigint_and_exponent();
    // dscale = displayed fractional digits = max(0, exp).
    if exp > MAX_DSCALE {
        return false;
    }
    // Decimal weight of the leading digit = (#mantissa digits) − 1 − exp.
    let ndigits = mant.to_string().trim_start_matches('-').len() as i64;
    ndigits - 1 - exp <= MAX_WEIGHT
}

/// PostgreSQL `numeric_out`: a plain decimal string (never scientific notation),
/// with exactly `dscale` fractional digits. (`bigdecimal`'s own `Display` switches
/// to `E` notation for small magnitudes, so this is hand-written.)
pub fn to_text(bd: &BigDecimal) -> String {
    let (mant, scale) = bd.as_bigint_and_exponent();
    let s = mant.to_string();
    let neg = s.starts_with('-');
    let digits = s.trim_start_matches('-');
    let scale = scale.max(0) as usize;
    let body = if scale == 0 {
        digits.to_string()
    } else if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if neg && digits != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// PostgreSQL `numeric_send` (binary): `int16 ndigits`, `int16 weight`,
/// `uint16 sign` (0x0000 +, 0x4000 −), `int16 dscale`, then `ndigits` base-10000
/// groups (`int16`, most significant first), with leading/trailing zero groups
/// stripped. Exercised only by binary-format clients (the text path covers the
/// wire tests + conformance), so it is proven by unit tests over hand-computed
/// vectors.
pub fn binary(bd: &BigDecimal) -> Vec<u8> {
    let (mant, scale) = bd.as_bigint_and_exponent();
    let dscale = scale.max(0) as u16;
    let s = mant.to_string();
    let neg = s.starts_with('-');
    let digits = s.trim_start_matches('-');
    let scale_u = scale.max(0) as usize;

    // Split into integer and fractional decimal-digit strings.
    let (int_str, frac_str) = if digits.len() > scale_u {
        (
            digits[..digits.len() - scale_u].to_string(),
            digits[digits.len() - scale_u..].to_string(),
        )
    } else {
        (
            String::new(),
            format!("{}{}", "0".repeat(scale_u - digits.len()), digits),
        )
    };

    // Base-10000 groups, aligned at the decimal point: integer part left-padded,
    // fractional part right-padded, to a multiple of 4.
    let mut nbase: Vec<i16> = Vec::new();
    let int_pad = (DEC_DIGITS as usize - int_str.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    let int_padded = format!("{}{}", "0".repeat(int_pad), int_str);
    let int_group_count = int_padded.len() / DEC_DIGITS as usize;
    for g in 0..int_group_count {
        let chunk = &int_padded[g * 4..g * 4 + 4];
        nbase.push(chunk.parse::<i16>().unwrap_or(0));
    }
    let frac_pad =
        (DEC_DIGITS as usize - frac_str.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    let frac_padded = format!("{}{}", frac_str, "0".repeat(frac_pad));
    for g in 0..frac_padded.len() / DEC_DIGITS as usize {
        let chunk = &frac_padded[g * 4..g * 4 + 4];
        nbase.push(chunk.parse::<i16>().unwrap_or(0));
    }

    // Weight of the first group, then strip leading/trailing zero groups.
    let mut weight = int_group_count as i64 - 1;
    while nbase.first() == Some(&0) {
        nbase.remove(0);
        weight -= 1;
    }
    while nbase.last() == Some(&0) {
        nbase.pop();
    }
    let sign: u16 = if nbase.is_empty() {
        weight = 0;
        0x0000
    } else if neg {
        0x4000
    } else {
        0x0000
    };

    let mut out = Vec::with_capacity(8 + nbase.len() * 2);
    out.extend_from_slice(&(nbase.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as i16).to_be_bytes());
    for d in nbase {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

/// `a + b` (result dscale = max input dscale — `bigdecimal` matches PostgreSQL).
pub fn add(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a + b)
}
/// `a - b` (result dscale = max input dscale).
pub fn sub(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a - b)
}
/// `a * b` (result dscale = sum of input dscales).
pub fn mul(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a * b)
}

/// `a / b` with PostgreSQL's display-scale rule (`select_div_scale`), rounded
/// half-away-from-zero. A zero divisor is 22012.
pub fn div(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(b) {
        return Err(TypeError::DivisionByZero);
    }
    let rscale = select_div_scale(a, b);
    Ok((a / b).with_scale_round(rscale, RoundingMode::HalfUp))
}

/// `mod(a, b)` for numeric (the remainder takes the dividend's sign, like PG). A
/// zero divisor is 22012.
pub fn rem(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(b) {
        return Err(TypeError::DivisionByZero);
    }
    Ok(canonical(a % b))
}

pub fn abs(bd: &BigDecimal) -> BigDecimal {
    bd.abs()
}

/// `floor(x)` — round toward −∞ (PostgreSQL `numeric_floor`); scale 0.
pub fn floor(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Floor))
}

/// `ceil(x)` / `ceiling(x)` — round toward +∞ (PostgreSQL `numeric_ceil`); scale 0.
pub fn ceil(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Ceiling))
}

/// `round(x, n)` — round to `n` decimal places, half-away-from-zero (PostgreSQL
/// `numeric_round`). `n` may be negative (round to tens/hundreds/…). The result
/// carries scale `max(n, 0)`. `n` is clamped to `MAX_DSCALE` so an adversarial
/// huge scale can't materialize billions of fractional digits and OOM — the same
/// format-limit discipline [`within_format_limits`] enforces on `parse`.
pub fn round(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::HalfUp))
}

/// `trunc(x, n)` — truncate to `n` decimal places, toward zero (PostgreSQL
/// `numeric_trunc`). `n` may be negative; clamped to `MAX_DSCALE` (see [`round`]).
pub fn trunc(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::Down))
}

/// `sign(x)` — −1 / 0 / 1 as a numeric (PostgreSQL `numeric_sign`).
pub fn sign(bd: &BigDecimal) -> BigDecimal {
    use std::cmp::Ordering;
    match bd.cmp(&BigDecimal::from(0)) {
        Ordering::Less => BigDecimal::from(-1),
        Ordering::Equal => BigDecimal::from(0),
        Ordering::Greater => BigDecimal::from(1),
    }
}

fn is_zero(bd: &BigDecimal) -> bool {
    bd.as_bigint_and_exponent()
        .0
        .to_string()
        .trim_start_matches('-')
        == "0"
}

/// PostgreSQL `select_div_scale`: the division/AVG display scale. In base-10000
/// units, `rscale = clamp(max(16 − qweight·4, s1 + s2), 0, 1000)` where `qweight`
/// is the quotient's leading-digit weight estimate.
fn select_div_scale(a: &BigDecimal, b: &BigDecimal) -> i64 {
    let (w1, f1) = nbase_weight_and_lead(a);
    let (w2, f2) = nbase_weight_and_lead(b);
    let mut qweight = w1 - w2;
    if f1 < f2 {
        qweight -= 1;
    }
    let s1 = a.fractional_digit_count().max(0);
    let s2 = b.fractional_digit_count().max(0);
    (MIN_SIG_DIGITS - qweight * DEC_DIGITS)
        .max(s1 + s2)
        .clamp(0, MAX_DISPLAY_SCALE)
}

/// The base-10000 weight of the leading digit, and that leading group's value
/// (right-padded to four decimal digits) — the two inputs `select_div_scale`
/// needs. Zero has weight 0 and leading group 0.
fn nbase_weight_and_lead(bd: &BigDecimal) -> (i64, u64) {
    let (mant, scale) = bd.as_bigint_and_exponent();
    let s = mant.to_string();
    let digits = s.trim_start_matches('-');
    if digits == "0" {
        return (0, 0);
    }
    let dweight = digits.len() as i64 - 1 - scale; // decimal weight of leading digit
    let w = dweight.div_euclid(DEC_DIGITS); // base-10000 weight (floor division)
    let count = (dweight - DEC_DIGITS * w + 1) as usize; // 1..=4 leading decimal digits
    let mut lead: String = digits.chars().take(count).collect();
    while lead.len() < count {
        lead.push('0');
    }
    (w, lead.parse::<u64>().unwrap_or(0))
}

/// PostgreSQL clamp bound for a transcendental result display scale.
const TRANSC_MAX_SCALE: i64 = 1000;

/// The decimal weight of a value's leading significant digit (its position as a
/// power of ten): 1234 -> 3, 0.0067 -> -3, 0 -> 0.
fn decimal_weight(bd: &BigDecimal) -> i64 {
    if is_zero(bd) {
        return 0;
    }
    let (mant, scale) = bd.as_bigint_and_exponent();
    let len = mant.to_string().trim_start_matches('-').len() as i64;
    len - 1 - scale
}

/// sqrt rscale (PostgreSQL `sqrt_var`): `sweight = w*DEC_DIGITS/2 + 1`.
fn sqrt_rscale(arg: &BigDecimal) -> i64 {
    let (w, _) = nbase_weight_and_lead(arg);
    let sweight = w * DEC_DIGITS / 2 + 1;
    (MIN_SIG_DIGITS - sweight)
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// exp rscale (PostgreSQL `exp_var`): `ln_dweight = trunc(val * log10(e))`.
fn exp_rscale(arg: &BigDecimal) -> i64 {
    let val = arg.to_f64().unwrap_or(0.0);
    let ln_dweight = (val * std::f64::consts::LOG10_E) as i64; // C-style truncation toward zero
    // PostgreSQL also floors rscale at the input's own dscale, so e.g.
    // exp(123.456) keeps 3 fractional digits even though the integer part is huge.
    (MIN_SIG_DIGITS - ln_dweight)
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// PostgreSQL `estimate_ln_dweight`: an estimate of the decimal weight of `ln(arg)`.
fn estimate_ln_dweight(arg: &BigDecimal) -> i64 {
    let dw = decimal_weight(arg);
    if dw == 0 {
        0
    } else {
        let est = ((dw.unsigned_abs() as f64) * std::f64::consts::LN_10)
            .log10()
            .floor() as i64;
        est.max(0)
    }
}

/// ln/log (base-10) rscale (PostgreSQL `ln_var` / `log_var` with base 10).
fn ln_rscale(arg: &BigDecimal) -> i64 {
    (MIN_SIG_DIGITS - estimate_ln_dweight(arg))
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// `numeric → int4` / `int8`: round half-away-from-zero (PostgreSQL `numeric_int4`,
/// distinct from `float8 → int`'s round-half-to-even), then range-check (22003).
pub fn to_i32(bd: &BigDecimal) -> Result<i32, TypeError> {
    bd.with_scale_round(0, RoundingMode::HalfUp)
        .to_i32()
        .ok_or(TypeError::Overflow)
}
pub fn to_i64(bd: &BigDecimal) -> Result<i64, TypeError> {
    bd.with_scale_round(0, RoundingMode::HalfUp)
        .to_i64()
        .ok_or(TypeError::Overflow)
}

pub fn from_i64(n: i64) -> BigDecimal {
    BigDecimal::from(n)
}

/// `float8 → numeric` via the float's shortest round-tripping text (PostgreSQL
/// `float8_numeric`), so `0.1::float8::numeric` is `0.1`, not the exact binary
/// expansion. A non-finite float has no numeric value here (deferred specials).
pub fn from_f64(f: f64) -> Result<BigDecimal, TypeError> {
    if !f.is_finite() {
        return Err(TypeError::Overflow);
    }
    parse(&format!("{f}")).ok_or(TypeError::Overflow)
}

/// `numeric → float8`. A magnitude beyond `f64` range becomes `±Infinity`, like
/// PostgreSQL's `numeric_float8`.
pub fn to_f64(bd: &BigDecimal) -> f64 {
    bd.to_f64().unwrap_or(f64::INFINITY)
}

/// Apply a `numeric(precision, scale)` type modifier: round to `scale`
/// (half-away-from-zero) then check the integer-digit budget `precision − scale`;
/// an overflow is 22003 ("numeric field overflow").
pub fn apply_typmod(bd: &BigDecimal, tm: Typmod) -> Result<BigDecimal, TypeError> {
    let r = bd.with_scale_round(i64::from(tm.scale), RoundingMode::HalfUp);
    if !is_zero(&r) {
        let (mant, scale) = r.as_bigint_and_exponent();
        let len = mant.to_string().trim_start_matches('-').len() as i64;
        let int_digits = len - scale; // integer-part digit count
        if int_digits > i64::from(tm.precision) - i64::from(tm.scale) {
            return Err(TypeError::Overflow);
        }
    }
    Ok(canonical(r))
}

// ---------------------------------------------------------------------------
// dashu-float wrappers: arbitrary-precision exp / ln / sqrt / powf
//
// These thin helpers isolate the dashu API behind a stable interface. Later
// tasks in SP34 call them from within this module to implement the SQL
// math functions `exp`, `ln`, `log10`, `sqrt`, and `power`.
//
// `DBig` (= `FBig<HalfAway, 10>`) is a decimal arbitrary-precision float.
// Precision is set at construction time via `.with_precision(prec).value()`.
// The method forms (`.exp()`, `.ln()`, `.sqrt()`, `.powf()`) use the embedded
// context, so we carry `prec` only to the `num_to_bf` constructor.
// ---------------------------------------------------------------------------
use dashu_float::DBig;
use dashu_float::ops::SquareRoot;

/// Parse a plain-decimal string into a `DBig` with `prec` significant digits.
fn num_to_bf(s: &str, prec: usize) -> DBig {
    use core::str::FromStr;
    DBig::from_str(s)
        .expect("valid decimal text")
        .with_precision(prec)
        .value()
}

/// Render a `DBig` to a plain-decimal string.
/// `DBig`'s `Display` is plain decimal (never scientific notation for finite
/// values), so `to_string()` is correct.
fn bf_to_text(x: &DBig) -> String {
    x.to_string()
}

/// `exp(x)` at `prec` significant digits.
fn bf_exp(x: &DBig, _prec: usize) -> DBig {
    x.exp()
}

/// `ln(x)` at `prec` significant digits; `None` for `x <= 0`.
/// `DBig::ln` panics on non-positive input, so we guard first.
fn bf_ln(x: &DBig, _prec: usize) -> Option<DBig> {
    // Use comparison to DBig::ZERO: PartialOrd is implemented for DBig.
    // is_zero() is on Repr, so check sign via comparison.
    if *x <= DBig::ZERO {
        return None;
    }
    Some(x.ln())
}

/// `sqrt(x)` at `prec` significant digits; `None` for `x < 0`.
/// `DBig::sqrt` (via the `SquareRoot` trait) panics on negative input, so we guard first.
fn bf_sqrt(x: &DBig, _prec: usize) -> Option<DBig> {
    if *x < DBig::ZERO {
        return None;
    }
    Some(x.sqrt())
}

/// `pow(base, exp)` at `prec` significant digits; `None` for non-positive base.
/// `DBig::powf` panics on non-positive base, so we guard first.
fn bf_powf(base: &DBig, exp: &DBig, _prec: usize) -> Option<DBig> {
    if *base <= DBig::ZERO {
        return None;
    }
    Some(base.powf(exp))
}

// ---------------------------------------------------------------------------
// Public transcendental functions (SP34 Task 3)
// ---------------------------------------------------------------------------

/// Significant-digit precision for the dashu computation: cover the result's
/// integer digits + the requested fractional rscale + a guard margin. Saturating
/// (so a degenerate caller can't panic) and capped — callers bound the magnitude
/// up front (`MAX_WEIGHT`), so this cap is only defense-in-depth.
fn transc_prec(result_dweight: i64, rscale: i64) -> usize {
    result_dweight
        .max(0)
        .saturating_add(rscale.max(0))
        .saturating_add(16)
        .clamp(24, MAX_WEIGHT + 64) as usize
}

/// Round a dashu result (as text) to `rscale` fractional digits, half-away. The
/// caller guarantees (via an up-front `MAX_WEIGHT` bound) that the rounded value
/// is within the numeric format, so `parse` always succeeds here.
fn finish_transc(value_text: &str, rscale: i64) -> BigDecimal {
    let bd = parse(value_text).expect("bounded transcendental result is within numeric format");
    canonical(bd.with_scale_round(rscale, RoundingMode::HalfUp))
}

/// 2201F — square root of a negative number.
fn err_sqrt_negative() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "cannot take square root of a negative number",
    }
}
/// 2201E — logarithm of a non-positive number.
fn err_log_nonpositive() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201E",
        message: "cannot take logarithm of a non-positive number",
    }
}
/// 2201F — zero raised to a negative power.
fn err_zero_neg_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "zero raised to a negative power is undefined",
    }
}
/// 2201F — a negative base raised to a non-integer power (complex result).
fn err_neg_noninteger_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "a negative number raised to a non-integer power yields a complex result",
    }
}

/// numeric sqrt; `Err(2201F)` for a negative argument. (sqrt shrinks magnitude,
/// so it never overflows the numeric format.)
pub fn num_sqrt(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    let rscale = sqrt_rscale(arg);
    if is_zero(arg) {
        return Ok(canonical(
            BigDecimal::from(0).with_scale_round(rscale, RoundingMode::HalfUp),
        ));
    }
    if arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_sqrt_negative());
    }
    let prec = transc_prec(decimal_weight(arg) / 2, rscale);
    let v = bf_sqrt(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_sqrt_negative)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric ln; `Err(2201E)` for arg <= 0. (ln of an in-format value never
/// overflows — its magnitude is at most ~`ln(10)·weight`.)
pub fn num_ln(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(arg) || arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_log_nonpositive());
    }
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale);
    let v = bf_ln(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_log_nonpositive)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric log base 10; `Err(2201E)` for arg <= 0.
pub fn num_log10(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(arg) || arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_log_nonpositive());
    }
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale) + 8;
    // log10(x) = ln(x) / ln(10), both at high precision, then round to rscale.
    let lnx = bf_ln(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_log_nonpositive)?;
    let ln10 = bf_ln(&num_to_bf("10", prec), prec).expect("ln(10) defined");
    let lnx_bd = parse(&bf_to_text(&lnx)).expect("ln result is a valid numeric");
    let ln10_bd = parse(&bf_to_text(&ln10)).expect("ln10 is a valid numeric");
    let quotient = (lnx_bd / ln10_bd).with_scale_round(rscale + 4, RoundingMode::HalfUp);
    Ok(canonical(
        quotient.with_scale_round(rscale, RoundingMode::HalfUp),
    ))
}

/// numeric exp; `Err(22003)` when the result overflows the numeric format.
/// PostgreSQL `exp_var` overflows for `arg >= NUMERIC_MAX_RESULT_SCALE*3 = 6000`
/// (a one-sided bound: a large NEGATIVE argument underflows toward 0, not an error).
pub fn num_exp(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    // A magnitude beyond f64 range maps to ±∞ by sign, so the >= 6000 test still
    // fires for an enormous positive argument.
    let val = arg
        .to_f64()
        .unwrap_or(if arg.sign() == bigdecimal::num_bigint::Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    if val >= 6000.0 {
        return Err(TypeError::Overflow);
    }
    let rscale = exp_rscale(arg);
    let result_dweight = (val * std::f64::consts::LOG10_E) as i64;
    let prec = transc_prec(result_dweight, rscale);
    let v = bf_exp(&num_to_bf(&to_text(arg), prec), prec);
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric power; `Err(2201F)` on a domain error (0^neg, negative^non-integer),
/// `Err(22003)` when the result overflows the numeric format.
pub fn num_power(base: &BigDecimal, exp: &BigDecimal) -> Result<BigDecimal, TypeError> {
    use bigdecimal::num_bigint::Sign;
    if is_zero(base) {
        if exp.sign() == Sign::Minus {
            return Err(err_zero_neg_power());
        }
        if is_zero(exp) {
            return Ok(power_finish(BigDecimal::from(1)));
        }
        return Ok(power_finish(BigDecimal::from(0)));
    }
    // A negative base with a non-integer exponent is a complex result (check this
    // domain error before the overflow bound).
    if base.sign() == Sign::Minus && !exp.is_integer() {
        return Err(err_neg_noninteger_power());
    }
    // Overflow bound: the result's decimal weight is ≈ exp · log10(|base|). Reject
    // (22003) when it exceeds the numeric format BEFORE materializing it — this
    // bounds both `powi` (exact integer power) and the dashu `powf` path, and also
    // covers an integer exponent too large for i64 (`exp.to_f64()` → ±∞).
    let exp_f64 = exp.to_f64().unwrap_or(if exp.sign() == Sign::Minus {
        f64::NEG_INFINITY
    } else {
        f64::INFINITY
    });
    let base_log10 = base.to_f64().map_or(f64::INFINITY, |b| b.abs().log10());
    let est_weight = exp_f64 * base_log10;
    if est_weight > MAX_WEIGHT as f64 {
        return Err(TypeError::Overflow);
    }
    // exact integer exponent -> powi (handles negative base + negative exponent)
    if exp.is_integer()
        && let Ok(e) = to_i64(exp)
    {
        return Ok(power_finish(base.powi(e)));
    }
    // non-integer exponent: base must be > 0 (the negative case returned above).
    let rweight = (exp_f64 * decimal_weight(base) as f64) as i64;
    let rscale = (MIN_SIG_DIGITS - rweight).clamp(0, TRANSC_MAX_SCALE);
    let prec = transc_prec(rweight, rscale);
    let v = bf_powf(
        &num_to_bf(&to_text(base), prec),
        &num_to_bf(&to_text(exp), prec),
        prec,
    )
    .ok_or_else(err_neg_noninteger_power)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// Is `value` an exact power of ten (its significant digits are just "1")?
/// e.g. 0.001, 0.1, 1000 — but not 0.04 or 0.125. PostgreSQL's integer-power
/// display scale gives these one MORE fractional digit than other sub-1 results.
fn is_power_of_ten(value: &BigDecimal) -> bool {
    if is_zero(value) {
        return false;
    }
    let (mant, _) = value.as_bigint_and_exponent();
    mant.to_string()
        .trim_start_matches('-')
        .trim_end_matches('0')
        == "1"
}

/// Round an exact (integer-exponent) power result to PostgreSQL's `power_var_int`
/// display scale (validated against PostgreSQL 17.10 across a battery). The rule:
/// weight ≥ 0 → `16 - weight` (1024 → 13, 8 → 16); a sub-1 result → `15 - weight`
/// (0.04 → 17, 0.125 → 16), EXCEPT an exact power of ten keeps one more digit,
/// `16 - weight` (0.001 → 19, 0.1 → 17). The leading digit is the first
/// significant digit at position `weight`, so a sub-1 result needs one fewer
/// rscale digit to reach 16 significant digits — except a power of ten, whose
/// single significant digit keeps the extra one.
fn power_finish(value: BigDecimal) -> BigDecimal {
    let rweight = decimal_weight(&value);
    let rscale = if rweight < 0 && !is_power_of_ten(&value) {
        (MIN_SIG_DIGITS - rweight - 1).clamp(0, TRANSC_MAX_SCALE)
    } else {
        (MIN_SIG_DIGITS - rweight).clamp(0, TRANSC_MAX_SCALE)
    };
    canonical(value.with_scale_round(rscale, RoundingMode::HalfUp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn n(s: &str) -> BigDecimal {
        parse(s).expect("parse")
    }

    #[test]
    fn parse_canonicalizes_scale_and_rejects_garbage() {
        assert_eq!(to_text(&n("1.50")), "1.50"); // trailing zeros preserved
        assert_eq!(to_text(&n("1e3")), "1000"); // exponent → scale 0
        assert_eq!(to_text(&n("1.5e-3")), "0.0015");
        assert_eq!(to_text(&n("2.")), "2");
        assert_eq!(to_text(&n(".5")), "0.5");
        assert_eq!(to_text(&n("  -7.25 ")), "-7.25");
        assert!(parse("abc").is_none());
        assert!(parse("").is_none());
        assert!(parse("NaN").is_none()); // specials deferred
    }

    #[test]
    fn parse_rejects_values_that_overflow_the_numeric_format() {
        // PostgreSQL's boundary: weight ≤ 131071 (integer side), dscale ≤ 16383.
        // Beyond it PG raises "value overflows numeric format"; we reject (None) —
        // which ALSO prevents the OOM the `decode_row` fuzzer found (an adversarial
        // exponent like `8e88888888` would otherwise materialize ~88M digits).
        assert!(parse("8e88888888").is_none());
        assert!(parse("8e-88888888").is_none());
        assert!(parse("1e131072").is_none()); // just over the weight limit
        assert!(parse("1e-16384").is_none()); // just over the dscale limit
        // The in-range boundary values still parse (PG accepts these).
        assert!(parse("1e131071").is_some());
        assert!(parse("1e-16383").is_some());
    }

    #[test]
    fn text_output_is_plain_decimal_never_scientific() {
        assert_eq!(to_text(&n("1.5e-10")), "0.00000000015");
        assert_eq!(to_text(&n("1e30")), "1000000000000000000000000000000");
        assert_eq!(to_text(&n("0.0")), "0.0");
        assert_eq!(to_text(&n("0")), "0");
        assert_eq!(to_text(&n("-0.0")), "0.0"); // negative zero prints unsigned
        assert_eq!(to_text(&n("100.00")), "100.00");
    }

    #[test]
    fn arithmetic_scale_rules_match_postgres() {
        assert_eq!(to_text(&add(&n("1.50"), &n("1.5"))), "3.00"); // max scale
        assert_eq!(to_text(&sub(&n("2.5"), &n("1.25"))), "1.25");
        assert_eq!(to_text(&mul(&n("1.5"), &n("1.5"))), "2.25"); // scales add
        assert_eq!(to_text(&mul(&n("1.50"), &n("2"))), "3.00");
        assert_eq!(to_text(&add(&n("1e3"), &n("0.0"))), "1000.0");
    }

    #[test]
    fn division_display_scale_matches_select_div_scale() {
        // Cases captured from PostgreSQL 16 (identical to 18).
        for (a, b, want) in [
            ("1.0", "3", "0.33333333333333333333"),
            ("10", "3.0", "3.3333333333333333"),
            ("6.0", "2.0", "3.0000000000000000"),
            ("22.0", "7", "3.1428571428571429"),
            ("100.0", "8", "12.5000000000000000"),
            ("1000000.0", "7", "142857.142857142857"),
            ("0.0001", "7", "0.000014285714285714285714"),
            ("0.3", "3", "0.10000000000000000000"),
            ("1.0", "30000", "0.000033333333333333333333"),
            ("0.0", "3", "0.00000000000000000000"),
        ] {
            assert_eq!(to_text(&div(&n(a), &n(b)).expect("div")), want, "{a}/{b}");
        }
        assert!(matches!(
            div(&n("1.5"), &n("0")),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn numeric_to_int_rounds_half_away_from_zero() {
        // Distinct from float8→int (half-to-even): 2.5 → 3 here.
        assert_eq!(to_i32(&n("2.5")).expect("i"), 3);
        assert_eq!(to_i32(&n("3.5")).expect("i"), 4);
        assert_eq!(to_i32(&n("-2.5")).expect("i"), -3);
        assert_eq!(to_i32(&n("2.4")).expect("i"), 2);
        assert_eq!(to_i64(&n("9999999999")).expect("i"), 9_999_999_999);
        assert!(matches!(
            to_i32(&n("99999999999")),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn float_numeric_conversions_use_shortest_text() {
        assert_eq!(to_text(&from_f64(0.1).expect("f")), "0.1");
        assert_eq!(to_text(&from_f64(2.5).expect("f")), "2.5");
        assert_eq!(to_f64(&n("1.5")), 1.5);
        assert!(matches!(from_f64(f64::INFINITY), Err(TypeError::Overflow)));
        assert!(matches!(from_f64(f64::NAN), Err(TypeError::Overflow)));
    }

    #[test]
    fn typmod_rounds_to_scale_and_overflows_on_precision() {
        let tm = Typmod {
            precision: 4,
            scale: 1,
        };
        assert_eq!(
            to_text(&apply_typmod(&n("123.45"), tm).expect("ok")),
            "123.5"
        );
        assert!(matches!(
            apply_typmod(&n("1234.5"), tm),
            Err(TypeError::Overflow)
        ));
        let tm2 = Typmod {
            precision: 3,
            scale: 2,
        };
        assert_eq!(to_text(&apply_typmod(&n("9.99"), tm2).expect("ok")), "9.99");
        // rounds to 10.00 → 2 integer digits > precision-scale=1 → overflow.
        assert!(matches!(
            apply_typmod(&n("9.999"), tm2),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn binary_nbase_encoding_matches_numeric_send() {
        // 1.5 → ndigits 2, weight 0, sign +, dscale 1, digits [1, 5000].
        assert_eq!(
            binary(&n("1.5")),
            vec![0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0x13, 0x88]
        );
        // 0 → ndigits 0, weight 0, sign +, dscale 0.
        assert_eq!(binary(&n("0")), vec![0, 0, 0, 0, 0, 0, 0, 0]);
        // 10000 → ndigits 1, weight 1, dscale 0, digits [1].
        assert_eq!(binary(&n("10000")), vec![0, 1, 0, 1, 0, 0, 0, 0, 0, 1]);
        // -2.5 → ndigits 2, weight 0, sign 0x4000, dscale 1, digits [2, 5000].
        assert_eq!(
            binary(&n("-2.5")),
            vec![0, 2, 0, 0, 0x40, 0, 0, 1, 0, 2, 0x13, 0x88]
        );
    }

    #[test]
    fn abs_and_rem_match_postgres() {
        assert_eq!(to_text(&abs(&n("-2.5"))), "2.5");
        assert_eq!(to_text(&abs(&n("2.5"))), "2.5");
        // mod takes the dividend's sign; a zero divisor is 22012.
        assert_eq!(to_text(&rem(&n("7.5"), &n("2")).expect("rem")), "1.5");
        assert_eq!(to_text(&rem(&n("-7.5"), &n("2")).expect("rem")), "-1.5");
        assert!(matches!(
            rem(&n("1.5"), &n("0")),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn grouping_equality_ignores_scale() {
        // 1.50 and 1.5 are the same value (PG grouping equality).
        assert_eq!(n("1.50"), n("1.5"));
        assert_eq!(BigDecimal::from_str("1.50").expect("x"), n("1.5"));
        assert_ne!(n("1.5"), n("1.6"));
    }

    #[test]
    fn rounding_primitives_match_postgres() {
        let n = |s: &str| parse(s).expect("parse");
        // floor toward −∞, ceil toward +∞ (scale 0)
        assert_eq!(to_text(&floor(&n("2.9"))), "2");
        assert_eq!(to_text(&floor(&n("-2.1"))), "-3");
        assert_eq!(to_text(&ceil(&n("2.1"))), "3");
        assert_eq!(to_text(&ceil(&n("-2.9"))), "-2");
        // round half-away-from-zero; preserves requested scale
        assert_eq!(to_text(&round(&n("2.5"), 0)), "3");
        assert_eq!(to_text(&round(&n("-2.5"), 0)), "-3");
        assert_eq!(to_text(&round(&n("2.567"), 2)), "2.57");
        assert_eq!(to_text(&round(&n("1234"), -2)), "1200");
        // trunc toward zero
        assert_eq!(to_text(&trunc(&n("2.99"), 0)), "2");
        assert_eq!(to_text(&trunc(&n("-2.99"), 0)), "-2");
        assert_eq!(to_text(&trunc(&n("2.567"), 1)), "2.5");
        // sign
        assert_eq!(to_text(&sign(&n("-5.5"))), "-1");
        assert_eq!(to_text(&sign(&n("0"))), "0");
        assert_eq!(to_text(&sign(&n("0.3"))), "1");
    }

    #[test]
    fn dashu_wrappers_compute_known_values() {
        let p = 40; // 40 significant digits — plenty for these checks.
        // exp(0) = 1
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
        // domain guards: ln of non-positive, sqrt of negative -> None; but sqrt(0)
        // is DEFINED (the guard is `< 0`, not `<= 0`).
        assert!(bf_ln(&num_to_bf("0", p), p).is_none());
        assert!(bf_sqrt(&num_to_bf("-1", p), p).is_none());
        assert_eq!(
            bf_to_text(&bf_sqrt(&num_to_bf("0", p), p).expect("sqrt0")),
            "0"
        );
    }

    #[test]
    fn rscale_rules_match_postgres() {
        let n = |s: &str| parse(s).expect("parse");
        // sqrt: rscale = clamp(16 - (w*2 + 1), max(dscale,0), 1000), w = base-10000 weight
        assert_eq!(sqrt_rscale(&n("2")), 15);
        assert_eq!(sqrt_rscale(&n("1000000")), 13);
        assert_eq!(sqrt_rscale(&n("0.04")), 17);
        // exp: rscale = clamp(16 - trunc(val * log10(e)), 0, 1000)
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
        // decimal_weight: position of the leading significant digit
        assert_eq!(decimal_weight(&n("1234")), 3);
        assert_eq!(decimal_weight(&n("0.0067")), -3);
        assert_eq!(decimal_weight(&n("0")), 0);
    }

    #[test]
    fn numeric_transcendentals_match_postgres() {
        let t = |bd: &BigDecimal| to_text(bd);
        let n = |s: &str| parse(s).expect("parse");
        assert_eq!(t(&num_sqrt(&n("2")).expect("sqrt")), "1.414213562373095");
        assert_eq!(t(&num_sqrt(&n("4")).expect("sqrt")), "2.000000000000000");
        assert_eq!(
            t(&num_sqrt(&n("0.04")).expect("sqrt")),
            "0.20000000000000000"
        );
        assert!(num_sqrt(&n("-1")).is_err());
        assert_eq!(t(&num_ln(&n("2")).expect("ln")), "0.6931471805599453");
        assert_eq!(t(&num_ln(&n("1000000")).expect("ln")), "13.815510557964274");
        assert!(num_ln(&n("0")).is_err());
        assert_eq!(t(&num_log10(&n("100")).expect("log")), "2.0000000000000000");
        // a NON-exact log/ln (every digit matters) pins the `ln(x)/ln(10)` division
        // precision + intermediate scale — exact powers of ten alone can't.
        assert_eq!(t(&num_log10(&n("2")).expect("log")), "0.3010299956639812");
        assert_eq!(t(&num_log10(&n("5")).expect("log")), "0.6989700043360188");
        assert_eq!(
            t(&num_log10(&n("1000000")).expect("log")),
            "6.000000000000000"
        );
        assert_eq!(t(&num_exp(&n("0")).expect("exp")), "1.0000000000000000");
        assert_eq!(t(&num_exp(&n("1")).expect("exp")), "2.7182818284590452");
        assert_eq!(t(&num_exp(&n("10")).expect("exp")), "22026.465794806717");
        // power: exact integer exponent (incl. negative + large), and non-integer via powf
        assert_eq!(
            t(&num_power(&n("2"), &n("10")).expect("pow")),
            "1024.0000000000000"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("3")).expect("pow")),
            "8.0000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("3"), &n("4")).expect("pow")),
            "81.000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("-2"), &n("3")).expect("pow")),
            "-8.0000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("5"), &n("-2")).expect("pow")),
            "0.04000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("100")).expect("pow")),
            "1267650600228229401496703205376"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("0.5")).expect("pow")),
            "1.4142135623730950"
        );
        assert!(num_power(&n("0"), &n("-1")).is_err()); // 0^negative -> domain error
        assert!(num_power(&n("-2"), &n("0.5")).is_err()); // negative^non-integer -> domain error
        // overflow guards (22003): exp(>=6000), an over-format power, and an
        // integer exponent too large for i64 — none must panic or hang.
        assert!(matches!(num_exp(&n("6000")), Err(TypeError::Overflow)));
        assert!(num_exp(&n("5999")).is_ok());
        assert!(matches!(
            num_power(&n("10"), &n("200000")),
            Err(TypeError::Overflow)
        ));
        assert!(num_power(&n("10"), &n("5000")).is_ok()); // 5001 digits, comfortably in-format
        // huge integer exponent: error, not panic
        assert!(matches!(
            num_power(&n("10"), &n("1e30")),
            Err(TypeError::Overflow)
        ));
        // --- rscale/overflow-estimate edges (pin the exact arithmetic) ---
        let t = |bd: &BigDecimal| to_text(bd);
        // is_power_of_ten: an exact power-of-ten integer-power result keeps one
        // EXTRA fractional digit (19), vs 18 for a non-power-of-ten sub-1 result.
        assert_eq!(
            t(&num_power(&n("10"), &n("-3")).expect("p")),
            "0.0010000000000000000"
        );
        // non-integer-power rscale = 16 - (exp · decimal_weight(base)): for
        // power(1000, 0.5) that is 16 - (0.5·3 → 1) = 15 fractional digits. A `+`/`/`
        // mutation of the `exp·weight` product, or a `+` for the `16 - rweight`,
        // changes the digit count.
        assert_eq!(
            t(&num_power(&n("1000"), &n("0.5")).expect("p")),
            "31.622776601683793"
        );
        // The overflow estimate is `exp · log10(base)`: power(2, 200000) has weight
        // ≈ 60206 (in-format), so it must NOT be rejected — a `+`/`/` mutation of the
        // product would wrongly compute ≈200000 / ≈664000 and overflow it.
        assert!(num_power(&n("2"), &n("200000")).is_ok());
    }

    #[test]
    fn round_trunc_clamp_scale_to_avoid_oom() {
        let n = |s: &str| parse(s).expect("parse");
        // An adversarially huge scale must not materialize billions of digits:
        // it is clamped to MAX_DSCALE, so the result stays bounded.
        assert!(round(&n("2.5"), 2_000_000_000).fractional_digit_count() <= MAX_DSCALE);
        assert!(trunc(&n("2.5"), 2_000_000_000).fractional_digit_count() <= MAX_DSCALE);
        // Ordinary scales are unaffected.
        assert_eq!(to_text(&round(&n("2.567"), 2)), "2.57");
    }
}
