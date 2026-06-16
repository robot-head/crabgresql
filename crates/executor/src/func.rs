//! SP29: scalar (row) functions + the `||` concatenation operator.
//!
//! Like SP27's aggregates and SP28's predicates, every function here is a pure,
//! deterministic transform over a *single row's* already-MVCC-resolved Datums.
//! A whole table lives on one range (`RangeMap::range_for_table`), so a scalar
//! function executes entirely inside one `execute_read`/`eval` on one engine —
//! no cross-range scatter, no new lock/visibility rule, no new interleaving. This
//! is exactly CLAUDE.md's "pure-data / single-node refactor" carve-out, so SP29
//! ships NO Stateright model (a model of a scalar fold would have an
//! interleaving-free state space and merely restate these unit tests).
//!
//! The dispatch mirrors SP28: the pure combinators are shared between scalar
//! `eval` and the grouped evaluator (`agg::eval_grouped`); only the child-eval
//! closure differs (`eval_scalar` takes it as `FnMut(&Expr) -> Result<Datum>`).
//!
//! Supported: string `length`/`char_length`/`character_length`, `upper`,
//! `lower`, `btrim`/`ltrim`/`rtrim`, `substr`/`substring` (the comma form),
//! `replace`, `concat`; math `abs`, `mod`; null/conditional `coalesce`,
//! `nullif`, `greatest`, `least`. `||` is a binary operator handled in `eval`.

use std::cmp::Ordering;

use catalog::Table;
use pgparser::ast::{Expr, FuncArgs, FuncCall};
use pgtypes::{ColumnType, Datum, ops};

use crate::error::ExecError;

/// The scalar functions SP29 supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarFunc {
    Length,
    Upper,
    Lower,
    Btrim,
    Ltrim,
    Rtrim,
    Substr,
    Replace,
    Concat,
    Abs,
    Mod,
    Coalesce,
    NullIf,
    Greatest,
    Least,
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not a known scalar function"; the caller then tries the
/// aggregate path / reports an undefined function.
fn scalar_func(name: &str) -> Option<ScalarFunc> {
    Some(match name {
        "length" | "char_length" | "character_length" => ScalarFunc::Length,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "btrim" => ScalarFunc::Btrim,
        "ltrim" => ScalarFunc::Ltrim,
        "rtrim" => ScalarFunc::Rtrim,
        "substr" | "substring" => ScalarFunc::Substr,
        "replace" => ScalarFunc::Replace,
        "concat" => ScalarFunc::Concat,
        "abs" => ScalarFunc::Abs,
        "mod" => ScalarFunc::Mod,
        "coalesce" => ScalarFunc::Coalesce,
        "nullif" => ScalarFunc::NullIf,
        "greatest" => ScalarFunc::Greatest,
        "least" => ScalarFunc::Least,
        _ => return None,
    })
}

/// Is `name` a known scalar function? (The dispatch point in `eval`/`infer_type`.)
pub(crate) fn is_scalar(name: &str) -> bool {
    scalar_func(name).is_some()
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// `DISTINCT`/`ALL` is only meaningful for aggregates (PostgreSQL 42809). Our
/// parser discards `ALL`, so only an explicit `DISTINCT` reaches here.
fn distinct_not_aggregate(name: &str) -> ExecError {
    ExecError::WrongObjectType(format!(
        "DISTINCT specified, but {name} is not an aggregate function"
    ))
}

/// The positional argument list of a scalar call. `f(*)` is never valid for a
/// scalar function (only `count(*)`), so it is an undefined-function error.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

/// Reject the `DISTINCT` modifier (42809) and return the call's argument list.
/// Shared front-door check for both `scalar_result_type` and `eval_scalar`.
fn checked_args(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    if fc.distinct {
        return Err(distinct_not_aggregate(&fc.name));
    }
    exprs_of(fc)
}

/// Statically infer a scalar call's result type (for RowDescription), validating
/// name, arity, and — where the result type depends on them or the function is
/// strictly typed — argument types. A bad name/arity/argument type is 42883.
///
/// NB: this runs for PROJECTED expressions (via `resolve_projection`). A scalar
/// function appearing only in `WHERE`/`HAVING`/`ORDER BY` is evaluated without a
/// separate type-resolution pass (a pre-existing trait of the engine — the same
/// is true of arithmetic), so an argument-type misuse THERE surfaces at runtime
/// as 42804 rather than here as 42883. This per-clause difference is documented.
pub(crate) fn scalar_result_type(
    fc: &FuncCall,
    table: Option<&Table>,
) -> Result<ColumnType, ExecError> {
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        ScalarFunc::Length => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], table)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Upper | ScalarFunc::Lower => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], table)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, n == 1 || n == 2)?;
            for a in args {
                require_text(a, table)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Substr => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], table)?;
            for a in &args[1..] {
                require_int(a, table)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Replace => {
            require_arity(fc, n == 3)?;
            for a in args {
                require_text(a, table)?;
            }
            Ok(ColumnType::Text)
        }
        // concat takes any number of arguments of any (non-array) type.
        ScalarFunc::Concat => Ok(ColumnType::Text),
        ScalarFunc::Abs => {
            require_arity(fc, n == 1)?;
            // abs preserves the integer width.
            require_int(&args[0], table)
        }
        ScalarFunc::Mod => {
            require_arity(fc, n == 2)?;
            let lt = require_int(&args[0], table)?;
            let rt = require_int(&args[1], table)?;
            Ok(promote(lt, rt))
        }
        ScalarFunc::Coalesce | ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, n >= 1)?;
            unify_args(args, table)
        }
        ScalarFunc::NullIf => {
            require_arity(fc, n == 2)?;
            // NULLIF's result is the first argument's type (a bare NULL → text).
            if matches!(args[0], Expr::NullLiteral) {
                Ok(ColumnType::Text)
            } else {
                crate::eval::infer_type(&args[0], table)
            }
        }
    }
}

/// Evaluate a scalar call. `eval_child` evaluates each argument expression
/// against the current row — the SAME `eval` for scalar context and
/// `agg::eval_grouped` for a grouped context, so the combinators are shared and
/// only the closure differs. Short-circuiting functions (`coalesce`) and the
/// lazy ones evaluate arguments only as far as needed.
pub(crate) fn eval_scalar(
    fc: &FuncCall,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    match f {
        // coalesce returns the first non-NULL argument, NOT evaluating the rest
        // (so `coalesce(x, 1/0)` with x non-null never divides by zero).
        ScalarFunc::Coalesce => {
            require_arity(fc, !args.is_empty())?;
            for a in args {
                let v = eval_child(a)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Datum::Null)
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, !args.is_empty())?;
            let want_greater = matches!(f, ScalarFunc::Greatest);
            let mut best: Option<Datum> = None;
            for a in args {
                let v = eval_child(a)?;
                if v.is_null() {
                    continue; // greatest/least ignore NULL arguments
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let replace = match ops::compare(&v, &cur)? {
                            Some(Ordering::Greater) => want_greater,
                            Some(Ordering::Less) => !want_greater,
                            _ => false, // Equal (both non-null, so never None)
                        };
                        if replace { v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        ScalarFunc::NullIf => {
            require_arity(fc, args.len() == 2)?;
            let a = eval_child(&args[0])?;
            let b = eval_child(&args[1])?;
            // NULLIF(a, b) = NULL when a = b, else a. `compare` is None if either
            // is NULL (so a NULL `a` falls through to `Ok(a)` = NULL).
            match ops::compare(&a, &b)? {
                Some(Ordering::Equal) => Ok(Datum::Null),
                _ => Ok(a),
            }
        }
        // Eager, strict-or-concat functions: evaluate every argument first.
        _ => {
            let vals = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            eval_eager(f, fc, &vals)
        }
    }
}

/// Apply an eager scalar function to its already-evaluated arguments. Every
/// function here except `concat` is strict (any NULL argument → NULL); `concat`
/// skips NULLs and never returns NULL.
fn eval_eager(f: ScalarFunc, fc: &FuncCall, vals: &[Datum]) -> Result<Datum, ExecError> {
    if let ScalarFunc::Concat = f {
        let mut s = String::new();
        for v in vals {
            if !v.is_null() {
                s.push_str(&text_render(v));
            }
        }
        return Ok(Datum::Text(s));
    }
    // Strict: a NULL argument short-circuits to NULL.
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    match f {
        ScalarFunc::Length => {
            require_arity(fc, vals.len() == 1)?;
            let n = text_arg(&vals[0])?.chars().count();
            i32::try_from(n)
                .map(Datum::Int4)
                .map_err(|_| ExecError::Type(pgtypes::TypeError::Overflow))
        }
        ScalarFunc::Upper => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.to_uppercase()))
        }
        ScalarFunc::Lower => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.to_lowercase()))
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            // The optional second argument is the set of characters to strip
            // (default: ASCII/Unicode whitespace).
            let trimmed = match vals.get(1) {
                None => trim_ws(f, s),
                Some(chars) => {
                    let set: Vec<char> = text_arg(chars)?.chars().collect();
                    trim_set(f, s, &set)
                }
            };
            Ok(Datum::Text(trimmed))
        }
        ScalarFunc::Substr => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let start = int_arg(&vals[1])?;
            let count = match vals.get(2) {
                None => None,
                Some(c) => Some(int_arg(c)?),
            };
            substr(s, start, count)
        }
        ScalarFunc::Replace => {
            require_arity(fc, vals.len() == 3)?;
            let (s, from, to) = (
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
                text_arg(&vals[2])?,
            );
            // PostgreSQL `replace` leaves the string unchanged when `from` is empty.
            let out = if from.is_empty() {
                s.to_string()
            } else {
                s.replace(from, to)
            };
            Ok(Datum::Text(out))
        }
        ScalarFunc::Abs => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Int4(n) => n
                    .checked_abs()
                    .map(Datum::Int4)
                    .ok_or(ExecError::Type(pgtypes::TypeError::Overflow)),
                Datum::Int8(n) => n
                    .checked_abs()
                    .map(Datum::Int8)
                    .ok_or(ExecError::Type(pgtypes::TypeError::Overflow)),
                other => Err(type_error("abs", other)),
            }
        }
        ScalarFunc::Mod => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::rem(&vals[0], &vals[1])?)
        }
        // concat / coalesce / nullif / greatest / least are handled before here.
        _ => unreachable!("non-eager scalar function reached eval_eager"),
    }
}

// ---- argument-type helpers ----

/// 42883 for an argument whose static type a function does not accept (PG's
/// "no function matches the given name and argument types").
fn no_matching_function() -> ExecError {
    ExecError::UndefinedFunction("no function matches the given name and argument types".into())
}

/// Require the argument to statically type as `text` (a bare `NULL` qualifies,
/// since it types as text); otherwise the function does not exist for it (42883).
fn require_text(arg: &Expr, table: Option<&Table>) -> Result<(), ExecError> {
    match crate::eval::infer_type(arg, table)? {
        ColumnType::Text => Ok(()),
        _ => Err(no_matching_function()),
    }
}

/// Require the argument to statically type as an integer; returns that width.
fn require_int(arg: &Expr, table: Option<&Table>) -> Result<ColumnType, ExecError> {
    match crate::eval::infer_type(arg, table)? {
        t @ (ColumnType::Int4 | ColumnType::Int8) => Ok(t),
        _ => Err(no_matching_function()),
    }
}

/// Unify every argument's type into one (for `coalesce`/`greatest`/`least`); an
/// all-NULL argument list types as text. Incompatible types are 42804.
fn unify_args(args: &[Expr], table: Option<&Table>) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for a in args {
        acc = crate::eval::unify_branch(acc, a, table)?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

fn promote(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Int4 && b == ColumnType::Int4 {
        ColumnType::Int4
    } else {
        ColumnType::Int8
    }
}

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// A text argument at runtime. A non-text Datum here means the function was used
/// in a non-projected position (so `scalar_result_type` never type-checked it);
/// PostgreSQL rejects it at plan time (42883), we surface it at runtime (42804).
fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// An integer argument at runtime, promoted to i64.
fn int_arg(d: &Datum) -> Result<i64, ExecError> {
    match d {
        Datum::Int4(n) => Ok(i64::from(*n)),
        Datum::Int8(n) => Ok(*n),
        other => Err(type_error("function", other)),
    }
}

fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// The canonical text rendering of a non-NULL Datum (the wire text encoding), so
/// `concat` agrees with the DataRow output and with the `||` operator.
fn text_render(d: &Datum) -> String {
    String::from_utf8(pgtypes::encoding::encode_text(d))
        .expect("a Datum's text encoding is always valid UTF-8")
}

// ---- string helpers ----

fn trim_ws(f: ScalarFunc, s: &str) -> String {
    match f {
        ScalarFunc::Ltrim => s.trim_start().to_string(),
        ScalarFunc::Rtrim => s.trim_end().to_string(),
        _ => s.trim().to_string(), // btrim
    }
}

fn trim_set(f: ScalarFunc, s: &str, set: &[char]) -> String {
    let in_set = |c: char| set.contains(&c);
    match f {
        ScalarFunc::Ltrim => s.trim_start_matches(in_set).to_string(),
        ScalarFunc::Rtrim => s.trim_end_matches(in_set).to_string(),
        _ => s.trim_matches(in_set).to_string(), // btrim
    }
}

/// PostgreSQL `substr(string, start [, count])`: 1-based `start`; characters
/// before position 1 count against `count`; a negative `count` is an error
/// (22011); a NULL argument already short-circuited to NULL in `eval_eager`.
fn substr(s: &str, start: i64, count: Option<i64>) -> Result<Datum, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    // The window is [start, end) in 1-based positions, clamped to [1, len+1).
    let end = match count {
        None => len + 1,
        Some(c) => {
            if c < 0 {
                return Err(ExecError::Type(pgtypes::TypeError::TypeMismatch {
                    message: "negative substring length not allowed".into(),
                }));
            }
            start.saturating_add(c)
        }
    };
    let lo = start.max(1);
    let hi = end.min(len + 1);
    if lo >= hi {
        return Ok(Datum::Text(String::new()));
    }
    let out: String = chars[(lo - 1) as usize..(hi - 1) as usize].iter().collect();
    Ok(Datum::Text(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{Column, Table};
    use pgparser::parser::parse_expr_for_test as pexpr;

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column {
                    name: "s".into(),
                    ty: ColumnType::Text,
                },
                Column {
                    name: "n".into(),
                    ty: ColumnType::Int4,
                },
            ],
        }
    }

    /// Evaluate a scalar-function expression with no row context.
    fn ev(sql: &str) -> Datum {
        crate::eval::eval(&pexpr(sql).expect("parse"), None, &[]).expect("eval")
    }

    fn err_code(sql: &str, t: Option<&Table>) -> String {
        // Drive both the static (projection) and runtime path: infer first (this
        // is what a projected expression hits), falling back to eval.
        let e = pexpr(sql).expect("parse");
        crate::eval::infer_type(&e, t)
            .err()
            .or_else(|| crate::eval::eval(&e, t, &[Datum::Null, Datum::Null]).err())
            .expect("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn string_length_upper_lower() {
        assert_eq!(ev("length('hello')"), Datum::Int4(5));
        assert_eq!(ev("char_length('abc')"), Datum::Int4(3));
        assert_eq!(ev("character_length('')"), Datum::Int4(0));
        assert_eq!(ev("upper('aBc')"), Datum::Text("ABC".into()));
        assert_eq!(ev("lower('aBc')"), Datum::Text("abc".into()));
        // strict: NULL argument → NULL.
        assert_eq!(ev("length(null)"), Datum::Null);
        assert_eq!(ev("upper(null)"), Datum::Null);
    }

    #[test]
    fn trims_default_and_with_set() {
        assert_eq!(ev("btrim('  hi  ')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('  hi  ')"), Datum::Text("hi  ".into()));
        assert_eq!(ev("rtrim('  hi  ')"), Datum::Text("  hi".into()));
        assert_eq!(ev("btrim('xxhixx', 'x')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('xyhi', 'xy')"), Datum::Text("hi".into()));
        // a NULL character-set argument → NULL (strict).
        assert_eq!(ev("btrim('hi', null)"), Datum::Null);
    }

    #[test]
    fn substr_and_replace() {
        assert_eq!(ev("substr('abcdef', 2, 3)"), Datum::Text("bcd".into()));
        assert_eq!(ev("substring('abcdef', 4)"), Datum::Text("def".into()));
        // start before position 1: the count is consumed from position 1.
        assert_eq!(ev("substr('abcdef', 0, 2)"), Datum::Text("a".into()));
        assert_eq!(ev("substr('abc', 5)"), Datum::Text("".into()));
        assert_eq!(
            ev("replace('a.b.c', '.', '-')"),
            Datum::Text("a-b-c".into())
        );
        // negative substring length is 22011-class (mapped to 42804 here).
        let err = crate::eval::eval(&pexpr("substr('abc', 1, -1)").expect("p"), None, &[])
            .expect_err("neg len");
        assert_eq!(err.into_pg().code, "42804");
    }

    #[test]
    fn concat_skips_nulls_and_renders_each() {
        assert_eq!(ev("concat('a', 'b', 'c')"), Datum::Text("abc".into()));
        assert_eq!(ev("concat('x', null, 'y')"), Datum::Text("xy".into()));
        assert_eq!(ev("concat(1, '+', 2)"), Datum::Text("1+2".into()));
        // all-NULL (and zero-arg) concat is the empty string, never NULL.
        assert_eq!(ev("concat(null, null)"), Datum::Text("".into()));
        assert_eq!(ev("concat()"), Datum::Text("".into()));
    }

    #[test]
    fn abs_and_mod() {
        assert_eq!(ev("abs(-5)"), Datum::Int4(5));
        assert_eq!(ev("abs(7)"), Datum::Int4(7));
        assert_eq!(ev("mod(11, 3)"), Datum::Int4(2));
        assert_eq!(ev("mod(-11, 3)"), Datum::Int4(-2));
        assert_eq!(ev("abs(null)"), Datum::Null);
        // abs overflow at i32::MIN is 22003. A bare `-2147483648` literal is a
        // negated int8, so the overflow only arises on an actual int4 column
        // value; evaluate `abs(n)` against a row holding Int4(i32::MIN).
        let t = table();
        let err = crate::eval::eval(
            &pexpr("abs(n)").expect("p"),
            Some(&t),
            &[Datum::Null, Datum::Int4(i32::MIN)],
        )
        .expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
        // mod by zero is 22012.
        let err =
            crate::eval::eval(&pexpr("mod(1, 0)").expect("p"), None, &[]).expect_err("div0");
        assert_eq!(err.into_pg().code, "22012");
    }

    #[test]
    fn coalesce_short_circuits_and_nullif() {
        assert_eq!(ev("coalesce(null, null, 3)"), Datum::Int4(3));
        assert_eq!(ev("coalesce(null, null)"), Datum::Null);
        // short-circuit: the un-taken `1/0` branch is never evaluated.
        assert_eq!(ev("coalesce(7, 1/0)"), Datum::Int4(7));
        assert_eq!(ev("nullif(5, 5)"), Datum::Null);
        assert_eq!(ev("nullif(5, 6)"), Datum::Int4(5));
        assert_eq!(ev("nullif(null, 1)"), Datum::Null);
    }

    #[test]
    fn greatest_least_ignore_nulls() {
        assert_eq!(ev("greatest(3, 7, 2)"), Datum::Int4(7));
        assert_eq!(ev("least(3, 7, 2)"), Datum::Int4(2));
        assert_eq!(ev("greatest(null, 4, null)"), Datum::Int4(4));
        assert_eq!(ev("least('b', 'a', 'c')"), Datum::Text("a".into()));
        assert_eq!(ev("greatest(null, null)"), Datum::Null);
    }

    #[test]
    fn result_types_for_row_description() {
        let t = table();
        let ty = |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), Some(&t)).expect("ty");
        assert_eq!(ty("length(s)"), ColumnType::Int4);
        assert_eq!(ty("upper(s)"), ColumnType::Text);
        assert_eq!(ty("substr(s, 1, 2)"), ColumnType::Text);
        assert_eq!(ty("concat(s, n)"), ColumnType::Text);
        assert_eq!(ty("abs(n)"), ColumnType::Int4);
        assert_eq!(ty("mod(n, 2)"), ColumnType::Int4);
        // coalesce(int4, int8) unifies to int8.
        assert_eq!(ty("coalesce(n, 3000000000)"), ColumnType::Int8);
        assert_eq!(ty("nullif(s, 'x')"), ColumnType::Text);
        // `||` is text; one operand text is enough.
        assert_eq!(ty("'id=' || n"), ColumnType::Text);
    }

    #[test]
    fn error_surface() {
        let t = table();
        // unknown function → 42883.
        assert_eq!(err_code("frobnicate(s)", Some(&t)), "42883");
        // wrong arity → 42883.
        assert_eq!(err_code("length(s, s)", Some(&t)), "42883");
        // bad argument type in a projected position → 42883.
        assert_eq!(err_code("upper(n)", Some(&t)), "42883");
        assert_eq!(err_code("abs(s)", Some(&t)), "42883");
        // `int || int` (neither operand text) → 42883.
        assert_eq!(err_code("n || n", Some(&t)), "42883");
        // incompatible coalesce/greatest types → 42804.
        assert_eq!(err_code("coalesce(n, s)", Some(&t)), "42804");
        // DISTINCT on a scalar function → 42809.
        assert_eq!(err_code("upper(distinct s)", Some(&t)), "42809");
    }

    #[test]
    fn concat_operator_evaluates_and_propagates_null() {
        assert_eq!(ev("'a' || 'b' || 'c'"), Datum::Text("abc".into()));
        assert_eq!(ev("'id=' || 5"), Datum::Text("id=5".into()));
        assert_eq!(ev("'x' || null"), Datum::Null);
    }
}
