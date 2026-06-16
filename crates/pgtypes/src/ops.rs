//! Operator semantics matching PostgreSQL: integer type promotion, checked
//! overflow (22003), division by zero (22012), NULL propagation, and
//! three-valued boolean logic.

use std::cmp::Ordering;

use crate::{Datum, TypeError};

/// Type an integer literal: narrowest of int4, then int8; overflow -> 22003.
pub fn int_literal(s: &str) -> Result<Datum, TypeError> {
    if let Ok(n) = s.parse::<i32>() {
        return Ok(Datum::Int4(n));
    }
    match s.parse::<i64>() {
        Ok(n) => Ok(Datum::Int8(n)),
        Err(_) => Err(TypeError::Overflow),
    }
}

/// Promote an integer Datum to i64 for mixed-width arithmetic.
fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn arith(
    a: &Datum,
    b: &Datum,
    op_i4: fn(i32, i32) -> Option<i32>,
    op_i8: fn(i64, i64) -> Option<i64>,
) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    match (a, b) {
        (Datum::Int4(x), Datum::Int4(y)) => {
            op_i4(*x, *y).map(Datum::Int4).ok_or(TypeError::Overflow)
        }
        _ => match (as_i64(a), as_i64(b)) {
            (Some(x), Some(y)) => op_i8(x, y).map(Datum::Int8).ok_or(TypeError::Overflow),
            _ => Err(TypeError::TypeMismatch {
                message: "operator requires integer operands".into(),
            }),
        },
    }
}

pub fn add(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_add, i64::checked_add)
}
pub fn sub(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_sub, i64::checked_sub)
}
pub fn mul(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_mul, i64::checked_mul)
}
pub fn div(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    let zero = matches!(b, Datum::Int4(0) | Datum::Int8(0));
    if zero {
        return Err(TypeError::DivisionByZero);
    }
    arith(a, b, i32::checked_div, i64::checked_div)
}

/// SQL comparison. Returns Ok(None) if either operand is NULL (so the caller
/// yields NULL / excludes the row). Cross-type integer comparison is allowed;
/// text compares lexicographically; bool compares false < true.
pub fn compare(a: &Datum, b: &Datum) -> Result<Option<Ordering>, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(None);
    }
    let ord = match (a, b) {
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        _ => match (as_i64(a), as_i64(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => {
                return Err(TypeError::TypeMismatch {
                    message: format!(
                        "cannot compare {} and {}",
                        a.column_type().map(|t| t.name()).unwrap_or("unknown"),
                        b.column_type().map(|t| t.name()).unwrap_or("unknown"),
                    ),
                });
            }
        },
    };
    Ok(Some(ord))
}

fn as_bool(d: &Datum) -> Result<Option<bool>, TypeError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Bool(b) => Ok(Some(*b)),
        _ => Err(TypeError::TypeMismatch {
            message: "argument of boolean operator must be boolean".into(),
        }),
    }
}

/// Three-valued AND: NULL AND false = false, else NULL propagates.
pub fn and(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(false), _) | (_, Some(false)) => Datum::Bool(false),
        (Some(true), Some(true)) => Datum::Bool(true),
        _ => Datum::Null,
    })
}

/// Three-valued OR: NULL OR true = true, else NULL propagates.
pub fn or(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(true), _) | (_, Some(true)) => Datum::Bool(true),
        (Some(false), Some(false)) => Datum::Bool(false),
        _ => Datum::Null,
    })
}

pub fn not(a: &Datum) -> Result<Datum, TypeError> {
    Ok(match as_bool(a)? {
        Some(b) => Datum::Bool(!b),
        None => Datum::Null,
    })
}

/// Build a Bool Datum from a comparison result and the operator.
pub fn cmp_to_bool(op_holds: bool, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(_) => Datum::Bool(op_holds),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Datum, TypeError};
    use std::cmp::Ordering;

    #[test]
    fn integer_literal_picks_narrowest_type() {
        assert_eq!(int_literal("5").expect("5"), Datum::Int4(5));
        assert_eq!(
            int_literal("2147483648").expect("big"),
            Datum::Int8(2_147_483_648)
        );
        assert!(matches!(
            int_literal("99999999999999999999"),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn arithmetic_type_promotion_and_overflow() {
        assert_eq!(
            add(&Datum::Int4(1), &Datum::Int4(2)).expect("ok"),
            Datum::Int4(3)
        );
        assert_eq!(
            add(&Datum::Int4(1), &Datum::Int8(2)).expect("ok"),
            Datum::Int8(3)
        );
        assert!(matches!(
            add(&Datum::Int4(i32::MAX), &Datum::Int4(1)),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            div(&Datum::Int4(1), &Datum::Int4(0)),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        assert_eq!(add(&Datum::Null, &Datum::Int4(1)).expect("ok"), Datum::Null);
        // NULL propagates BEFORE division-by-zero is evaluated: NULL / 0 is NULL,
        // not a 22012 error (the null check must short-circuit on EITHER operand).
        assert_eq!(div(&Datum::Null, &Datum::Int4(0)).expect("ok"), Datum::Null);
    }

    #[test]
    fn comparison_returns_none_for_null() {
        assert_eq!(
            compare(&Datum::Int4(1), &Datum::Int4(2)).expect("ok"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Datum::Int4(1), &Datum::Int8(1)).expect("ok"),
            Some(Ordering::Equal)
        );
        assert_eq!(compare(&Datum::Null, &Datum::Int4(1)).expect("ok"), None);
        assert_eq!(
            compare(&Datum::Text("a".into()), &Datum::Text("b".into())).expect("ok"),
            Some(Ordering::Less)
        );
        // bool compares false < true (its own arm, not the integer fallback).
        assert_eq!(
            compare(&Datum::Bool(false), &Datum::Bool(true)).expect("ok"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Datum::Bool(true), &Datum::Bool(true)).expect("ok"),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn three_valued_boolean_logic() {
        // Fully-defined operands: true AND true = true, false OR false = false.
        assert_eq!(
            and(&Datum::Bool(true), &Datum::Bool(true)).expect("ok"),
            Datum::Bool(true)
        );
        assert_eq!(
            or(&Datum::Bool(false), &Datum::Bool(false)).expect("ok"),
            Datum::Bool(false)
        );
        assert_eq!(
            and(&Datum::Null, &Datum::Bool(false)).expect("ok"),
            Datum::Bool(false)
        );
        assert_eq!(
            and(&Datum::Null, &Datum::Bool(true)).expect("ok"),
            Datum::Null
        );
        assert_eq!(
            or(&Datum::Null, &Datum::Bool(true)).expect("ok"),
            Datum::Bool(true)
        );
        assert_eq!(
            or(&Datum::Null, &Datum::Bool(false)).expect("ok"),
            Datum::Null
        );
        assert_eq!(not(&Datum::Null).expect("ok"), Datum::Null);
        assert_eq!(not(&Datum::Bool(true)).expect("ok"), Datum::Bool(false));
    }
}
