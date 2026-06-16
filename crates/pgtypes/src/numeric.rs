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
}
