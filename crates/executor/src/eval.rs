//! Expression evaluation over Datums, plus static result-type inference (used
//! to build a stable RowDescription before any row is produced).

use std::cmp::Ordering;

use pgparser::ast::{BinaryOp, Expr, UnaryOp};
use pgtypes::{ColumnType, Datum, TypeError, ops};

use crate::error::ExecError;
use crate::scope::Scope;

/// Evaluate `expr` against a row (`values`, aligned to `scope.columns`).
pub(crate) fn eval(expr: &Expr, scope: &Scope, values: &[Datum]) -> Result<Datum, ExecError> {
    match expr {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        // SP32: a bare decimal/exponent literal is `numeric` (arbitrary precision —
        // no overflow; the lexer already guaranteed a well-formed decimal lexeme).
        Expr::NumericLiteral(s) => {
            pgtypes::numeric::parse(s)
                .map(Datum::Numeric)
                .ok_or_else(|| {
                    ExecError::Type(TypeError::InvalidText {
                        type_name: "numeric",
                        value: s.clone(),
                    })
                })
        }
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Column { table, name } => {
            let idx = scope.resolve(table.as_deref(), name)?;
            Ok(values[idx].clone())
        }
        Expr::Unary { op, expr } => {
            let v = eval(expr, scope, values)?;
            apply_unary(*op, &v)
        }
        Expr::Binary { op, left, right } => {
            let l = eval(left, scope, values)?;
            let r = eval(right, scope, values)?;
            apply_binary(*op, &l, &r)
        }
        // A function call reached scalar `eval`: a SP29 scalar function evaluates
        // here (its arguments recurse through this same `eval`). Otherwise it is
        // NOT in a valid aggregate position (the aggregate path resolves
        // aggregates from accumulators) — a known aggregate here is misplaced /
        // nested (42803); any other name is undefined (42883).
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::eval_scalar(fc, |e| eval(e, scope, values))
        }
        Expr::Func(fc) => Err(crate::agg::func_in_scalar_context_error(fc)),
        // SP28: predicate + conditional expressions. The pure-Datum combinators
        // (`eval_in_list`/`eval_between`/`eval_like`/`eval_case`) are shared with
        // the grouped evaluator (`agg::eval_grouped`); only the child-evaluation
        // closure differs.
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, scope, values)?;
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval(expr, scope, values)?;
            eval_in_list(&x, list, *negated, |e| eval(e, scope, values))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval(expr, scope, values)?;
            let lo = eval(low, scope, values)?;
            let hi = eval(high, scope, values)?;
            eval_between(&x, &lo, &hi, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let s = eval(expr, scope, values)?;
            let p = eval(pattern, scope, values)?;
            eval_like(&s, &p, *negated, *case_insensitive)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => eval_case(operand.as_deref(), whens, else_result.as_deref(), |e| {
            eval(e, scope, values)
        }),
        // SP31: explicit cast — evaluate the operand, then convert. A text-parse
        // failure (22P02), numeric overflow (22003), or undefined cast (42846)
        // surfaces here; NULL casts to NULL.
        Expr::Cast { expr, ty } => {
            let v = eval(expr, scope, values)?;
            Ok(pgtypes::cast::cast(&v, *ty)?)
        }
    }
}

/// `x IN (list)` / `x NOT IN (list)` with three-valued NULL logic. `eval_child`
/// evaluates each list element. Truth table for `IN`: NULL lhs → NULL; an
/// element comparing Equal → true; otherwise NULL if any element was NULL, else
/// false. `NOT IN` is the boolean negation (NULL stays NULL).
pub(crate) fn eval_in_list(
    x: &Datum,
    list: &[Expr],
    negated: bool,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    if x.is_null() {
        return Ok(Datum::Null);
    }
    let mut saw_null = false;
    for item in list {
        let v = eval_child(item)?;
        match ops::compare(x, &v)? {
            Some(Ordering::Equal) => return Ok(Datum::Bool(!negated)),
            Some(_) => {}
            None => saw_null = true,
        }
    }
    if saw_null {
        Ok(Datum::Null)
    } else {
        Ok(Datum::Bool(negated))
    }
}

/// `x BETWEEN lo AND hi` ≡ `x >= lo AND x <= hi`; `NOT BETWEEN` negates it. NULL
/// propagates exactly as three-valued AND/NOT define.
pub(crate) fn eval_between(
    x: &Datum,
    lo: &Datum,
    hi: &Datum,
    negated: bool,
) -> Result<Datum, ExecError> {
    let ge = apply_binary(BinaryOp::Ge, x, lo)?;
    let le = apply_binary(BinaryOp::Le, x, hi)?;
    let res = ops::and(&ge, &le)?;
    Ok(if negated { ops::not(&res)? } else { res })
}

/// `s LIKE pat` / `ILIKE` (and their negations). NULL operand → NULL; a non-text
/// operand → 42804.
pub(crate) fn eval_like(
    s: &Datum,
    pat: &Datum,
    negated: bool,
    case_insensitive: bool,
) -> Result<Datum, ExecError> {
    if s.is_null() || pat.is_null() {
        return Ok(Datum::Null);
    }
    let m = like_match(as_text(s)?, as_text(pat)?, case_insensitive)?;
    Ok(Datum::Bool(m ^ negated))
}

fn as_text(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        _ => Err(ExecError::TypeMismatch(
            "LIKE/ILIKE operands must be type text".into(),
        )),
    }
}

/// SQL `LIKE` matcher over Unicode scalar values: `%` matches zero-or-more
/// characters, `_` exactly one, and `\` escapes the next pattern character.
/// `ci` folds ASCII case (the `ILIKE` form). A pattern ending in a lone `\` is
/// an invalid escape sequence (22025). Iterative backtracking to the last `%`,
/// O(n·m) worst case.
pub(crate) fn like_match(s: &str, p: &str, ci: bool) -> Result<bool, ExecError> {
    let fold = |c: char| if ci { c.to_ascii_lowercase() } else { c };
    let sb: Vec<char> = s.chars().map(fold).collect();
    let pb: Vec<char> = p.chars().collect();
    let (mut si, mut pi) = (0usize, 0usize);
    // The last `%` seen: pattern index just past it, and the `s` index to resume
    // from (advanced by one on each backtrack).
    let mut star: Option<usize> = None;
    let mut star_si = 0usize;
    while si < sb.len() {
        if pi < pb.len() {
            match pb[pi] {
                '\\' => {
                    let lit = *pb
                        .get(pi + 1)
                        .ok_or(ExecError::Type(TypeError::InvalidEscape))?;
                    if sb[si] == fold(lit) {
                        si += 1;
                        pi += 2;
                        continue;
                    }
                }
                '%' => {
                    star = Some(pi);
                    star_si = si;
                    pi += 1;
                    continue;
                }
                '_' => {
                    si += 1;
                    pi += 1;
                    continue;
                }
                c => {
                    if sb[si] == fold(c) {
                        si += 1;
                        pi += 1;
                        continue;
                    }
                }
            }
        }
        // Mismatch (or pattern exhausted while `s` remains): backtrack to the
        // last `%`, consuming one more subject character under it.
        if let Some(sp) = star {
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return Ok(false);
        }
    }
    // `s` is consumed; the remaining pattern must be only `%` to match (and a
    // trailing lone `\` is still an invalid escape).
    while pi < pb.len() {
        match pb[pi] {
            '%' => pi += 1,
            '\\' => {
                pb.get(pi + 1)
                    .ok_or(ExecError::Type(TypeError::InvalidEscape))?;
                return Ok(false);
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// A `CASE` expression. Searched form (`operand` None): the first WHEN whose
/// condition is TRUE wins (false/NULL skip; non-boolean → 42804). Simple form:
/// the first WHEN value comparing Equal to the operand wins (NULL never
/// matches). Falls through to ELSE, or NULL. Branches are evaluated lazily and
/// in order, so a later branch's error/side-effect is never reached early.
pub(crate) fn eval_case(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    match operand {
        None => {
            for (cond, result) in whens {
                match eval_child(cond)? {
                    Datum::Bool(true) => return eval_child(result),
                    Datum::Bool(false) | Datum::Null => {}
                    _ => {
                        return Err(ExecError::TypeMismatch(
                            "argument of CASE/WHEN must be type boolean".into(),
                        ));
                    }
                }
            }
        }
        Some(op) => {
            let ov = eval_child(op)?;
            for (val, result) in whens {
                let vv = eval_child(val)?;
                if matches!(ops::compare(&ov, &vv)?, Some(Ordering::Equal)) {
                    return eval_child(result);
                }
            }
        }
    }
    match else_result {
        Some(e) => eval_child(e),
        None => Ok(Datum::Null),
    }
}

/// Apply a unary operator to an already-evaluated operand. Shared by scalar
/// `eval` and the SP27 grouped evaluator (`agg::eval_grouped`).
pub(crate) fn apply_unary(op: UnaryOp, v: &Datum) -> Result<Datum, ExecError> {
    match op {
        UnaryOp::Not => Ok(ops::not(v)?),
        UnaryOp::Neg => Ok(ops::sub(&Datum::Int4(0), v)?),
    }
}

/// Apply a binary operator to two already-evaluated operands. Shared by scalar
/// `eval` and the SP27 grouped evaluator (`agg::eval_grouped`).
pub(crate) fn apply_binary(op: BinaryOp, l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    match op {
        BinaryOp::Add => Ok(ops::add(l, r)?),
        BinaryOp::Sub => Ok(ops::sub(l, r)?),
        BinaryOp::Mul => Ok(ops::mul(l, r)?),
        BinaryOp::Div => Ok(ops::div(l, r)?),
        BinaryOp::And => Ok(ops::and(l, r)?),
        BinaryOp::Or => Ok(ops::or(l, r)?),
        BinaryOp::Concat => Ok(ops::concat(l, r)?),
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let ord = ops::compare(l, r)?;
            Ok(cmp_result(op, ord))
        }
    }
}

fn cmp_result(op: BinaryOp, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(o) => {
            let holds = match op {
                BinaryOp::Eq => o == Ordering::Equal,
                BinaryOp::Ne => o != Ordering::Equal,
                BinaryOp::Lt => o == Ordering::Less,
                BinaryOp::Le => o != Ordering::Greater,
                BinaryOp::Gt => o == Ordering::Greater,
                BinaryOp::Ge => o != Ordering::Less,
                _ => unreachable!("cmp_result called with non-comparison op"),
            };
            Datum::Bool(holds)
        }
    }
}

/// Statically infer the result column type of an expression, for RowDescription.
pub(crate) fn infer_type(expr: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    match expr {
        Expr::IntLiteral(s) => match ops::int_literal(s)? {
            Datum::Int4(_) => Ok(ColumnType::Int4),
            Datum::Int8(_) => Ok(ColumnType::Int8),
            _ => unreachable!(),
        },
        // SP32: a decimal/exponent literal types as unconstrained `numeric`.
        Expr::NumericLiteral(_) => Ok(ColumnType::Numeric(None)),
        Expr::StringLiteral(_) => Ok(ColumnType::Text),
        Expr::BoolLiteral(_) => Ok(ColumnType::Bool),
        // PostgreSQL types a bare NULL as "unknown"; the slice uses text as a
        // concrete stand-in so RowDescription has a real OID.
        Expr::NullLiteral => Ok(ColumnType::Text),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Column { table, name } => {
            let idx = scope.resolve(table.as_deref(), name)?;
            Ok(scope.ty_at(idx))
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => Ok(ColumnType::Bool),
            UnaryOp::Neg => infer_type(expr, scope),
        },
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
                Ok(numeric_result_type(lt, rt))
            }
            // SP29: `||` yields text. PostgreSQL resolves the operator at plan
            // time and requires at least one operand to be text (`text || anynonarray`
            // / `anynonarray || text`); neither-text (e.g. `int || int`) is 42883.
            BinaryOp::Concat => {
                let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
                if lt != ColumnType::Text && rt != ColumnType::Text {
                    return Err(ExecError::UndefinedFunction(format!(
                        "operator does not exist: {} || {}",
                        lt.name(),
                        rt.name()
                    )));
                }
                Ok(ColumnType::Text)
            }
            _ => Ok(ColumnType::Bool),
        },
        // SP29: a scalar function's result type; otherwise an aggregate result
        // type for RowDescription (count/sum -> int8, min/max -> the argument's
        // type); unknown names / bad arity / bad argument type -> 42883.
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::scalar_result_type(fc, scope)
        }
        Expr::Func(fc) => crate::agg::func_result_type(fc, scope),
        // SP28: predicates are boolean; CASE unifies its branch result types.
        Expr::IsNull { .. } | Expr::InList { .. } | Expr::Between { .. } | Expr::Like { .. } => {
            Ok(ColumnType::Bool)
        }
        Expr::Case {
            whens, else_result, ..
        } => infer_case_type(whens, else_result.as_deref(), scope),
        // SP31: a cast's static result type is the target type — but only if the
        // cast is defined; an undefined `(from, to)` pair is 42846 at plan time
        // (so it is rejected before any row is produced). A bare `NULL` infers as
        // text, and text → anything is defined, so `NULL::<any>` is accepted.
        Expr::Cast { expr, ty } => {
            let from = infer_type(expr, scope)?;
            if pgtypes::cast::cast_allowed(from, *ty) {
                Ok(*ty)
            } else {
                Err(ExecError::Type(TypeError::CannotCast {
                    from: from.name(),
                    to: ty.name(),
                }))
            }
        }
    }
}

/// Infer a `CASE`'s result type by unifying every THEN result and the ELSE. A
/// bare `NULL` branch imposes no constraint; an all-NULL CASE is `text` (PG's
/// "unknown" → text); incompatible branch types are 42804.
fn infer_case_type(
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for (_, result) in whens {
        acc = unify_branch(acc, result, scope)?;
    }
    if let Some(e) = else_result {
        acc = unify_branch(acc, e, scope)?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

/// Fold one branch/argument into a running unified type. A bare `NULL` is
/// type-neutral (imposes no constraint). Shared by `CASE` type inference and
/// SP29's `coalesce`/`greatest`/`least`.
pub(crate) fn unify_branch(
    acc: Option<ColumnType>,
    expr: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if matches!(expr, Expr::NullLiteral) {
        return Ok(acc); // a bare NULL branch is type-neutral
    }
    let t = infer_type(expr, scope)?;
    match acc {
        None => Ok(Some(t)),
        Some(a) => Ok(Some(unify_types(a, t)?)),
    }
}

pub(crate) fn unify_types(a: ColumnType, b: ColumnType) -> Result<ColumnType, ExecError> {
    use ColumnType::{Float8, Int4, Int8, Numeric};
    // The numeric tower: int4/int8 < numeric < float8.
    let num_family = |t: ColumnType| matches!(t, Int4 | Int8 | Float8) || t.is_numeric();
    Ok(match (a, b) {
        (x, y) if x == y => x,
        // Mirror the arithmetic int4->int8 promotion rule.
        (Int4, Int8) | (Int8, Int4) => Int8,
        // SP30/SP32: any float8 wins; else (a numeric in the mix) → numeric.
        _ if a == Float8 || b == Float8 => Float8,
        _ if num_family(a) && num_family(b) => Numeric(None),
        _ => {
            return Err(ExecError::TypeMismatch(format!(
                "types {} and {} cannot be matched",
                a.name(),
                b.name()
            )));
        }
    })
}

/// The result type of `+ - * /` on two operand types. The numeric tower is
/// int < numeric < float8: any float8 makes the result float8; else any numeric
/// makes it numeric; else int4 only if both are int4, else int8. Permissive about
/// non-numeric operands (a real type error surfaces at evaluation).
fn numeric_result_type(lt: ColumnType, rt: ColumnType) -> ColumnType {
    use ColumnType::{Float8, Int4};
    if lt == Float8 || rt == Float8 {
        Float8
    } else if lt.is_numeric() || rt.is_numeric() {
        ColumnType::Numeric(None)
    } else if lt == Int4 && rt == Int4 {
        Int4
    } else {
        ColumnType::Int8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{Column, Table};
    use pgparser::parser::parse_expr_for_test as pexpr;
    use pgtypes::{ColumnType, Datum};

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column {
                    name: "a".into(),
                    ty: ColumnType::Int4,
                },
                Column {
                    name: "b".into(),
                    ty: ColumnType::Int4,
                },
            ],
        }
    }

    /// Build the `Scope` the tests evaluate against: the table's single-relation
    /// scope, or the empty scope (FROM-less expressions).
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name),
            None => Scope::empty(),
        }
    }

    fn ev(sql: &str, t: Option<&Table>, vals: &[Datum]) -> Datum {
        eval(&pexpr(sql).expect("parse"), &scope_of(t), vals).expect("eval")
    }

    #[test]
    fn arithmetic_and_columns() {
        let t = table();
        assert_eq!(
            ev("a + b * 2", Some(&t), &[Datum::Int4(3), Datum::Int4(4)]),
            Datum::Int4(11)
        );
    }

    #[test]
    fn comparison_yields_bool_and_null() {
        let t = table();
        assert_eq!(
            ev("a > b", Some(&t), &[Datum::Int4(2), Datum::Int4(1)]),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("a > b", Some(&t), &[Datum::Null, Datum::Int4(1)]),
            Datum::Null
        );
    }

    #[test]
    fn literals_no_table() {
        assert_eq!(ev("1 + 1", None, &[]), Datum::Int4(2));
        assert_eq!(ev("'x'", None, &[]), Datum::Text("x".into()));
        assert_eq!(ev("not true", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn numeric_literals_arithmetic_and_inference() {
        let num = |s: &str| Datum::Numeric(pgtypes::numeric::parse(s).expect("n"));
        // SP32: a bare decimal literal evaluates and types as `numeric`.
        assert_eq!(ev("1.5", None, &[]), num("1.5"));
        assert_eq!(
            infer_type(&pexpr("1.5").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Numeric(None)
        );
        // int ⊕ numeric promotes to numeric (exact). `3 / 2.0` uses PG's div scale.
        assert_eq!(ev("1 + 0.5", None, &[]), num("1.5"));
        assert_eq!(ev("3 / 2.0", None, &[]), num("1.5000000000000000"));
        assert_eq!(ev("- 2.5", None, &[]), num("-2.5"));
        assert_eq!(
            infer_type(&pexpr("a + 1.0").expect("parse"), &scope_of(Some(&table())))
                .expect("infer"),
            ColumnType::Numeric(None)
        );
        // CASE/coalesce unify int and numeric to numeric.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else 2.5 end").expect("parse"),
                &scope_of(Some(&table()))
            )
            .expect("infer"),
            ColumnType::Numeric(None)
        );
        // float8 is still reachable via an explicit cast (and wins over numeric).
        assert_eq!(ev("1.5::float8", None, &[]), Datum::Float8(1.5));
        assert_eq!(ev("3 / 2.0::float8", None, &[]), Datum::Float8(1.5));
    }

    #[test]
    fn undefined_column_is_42703() {
        let t = table();
        let err = eval(
            &pexpr("zzz").expect("parse"),
            &scope_of(Some(&t)),
            &[Datum::Int4(1), Datum::Int4(1)],
        )
        .expect_err("eval zzz should fail");
        assert_eq!(err.into_pg().code, "42703");
    }

    #[test]
    fn parameter_is_0a000() {
        let err = eval(&pexpr("$1").expect("parse"), &scope_of(None), &[])
            .expect_err("eval $1 should fail");
        assert_eq!(err.into_pg().code, "0A000");
    }

    #[test]
    fn type_inference_is_static() {
        let t = table();
        assert_eq!(
            infer_type(&pexpr("a + b").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Int4
        );
        assert_eq!(
            infer_type(&pexpr("a > b").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Bool
        );
        assert_eq!(
            infer_type(&pexpr("'x'").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Text
        );
        assert_eq!(
            infer_type(&pexpr("2147483648").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Int8
        );
    }

    // ---- SP28: predicate + conditional expression breadth ----

    fn err_code(sql: &str, t: Option<&Table>, vals: &[Datum]) -> String {
        eval(&pexpr(sql).expect("parse"), &scope_of(t), vals)
            .expect_err("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn is_null_is_never_null() {
        assert_eq!(ev("null is null", None, &[]), Datum::Bool(true));
        assert_eq!(ev("1 is null", None, &[]), Datum::Bool(false));
        assert_eq!(ev("1 is not null", None, &[]), Datum::Bool(true));
        assert_eq!(ev("null is not null", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn in_list_three_valued_null_logic() {
        assert_eq!(ev("1 in (1, 2)", None, &[]), Datum::Bool(true));
        assert_eq!(ev("3 in (1, 2)", None, &[]), Datum::Bool(false));
        assert_eq!(ev("null in (1, 2)", None, &[]), Datum::Null);
        // no equal match but a NULL element present -> unknown (NULL).
        assert_eq!(ev("3 in (1, null)", None, &[]), Datum::Null);
        // an equal match short-circuits past the NULL element -> true.
        assert_eq!(ev("1 in (1, null)", None, &[]), Datum::Bool(true));
        // NOT IN is the negation; NULL stays NULL.
        assert_eq!(ev("3 not in (1, null)", None, &[]), Datum::Null);
        assert_eq!(ev("3 not in (1, 2)", None, &[]), Datum::Bool(true));
        assert_eq!(ev("1 not in (1, 2)", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn between_null_propagates() {
        assert_eq!(ev("5 between 1 and 10", None, &[]), Datum::Bool(true));
        assert_eq!(ev("5 not between 1 and 10", None, &[]), Datum::Bool(false));
        assert_eq!(ev("5 between 1 and null", None, &[]), Datum::Null);
        assert_eq!(ev("null between 1 and 2", None, &[]), Datum::Null);
    }

    #[test]
    fn like_matcher_wildcards_escape_and_ilike() {
        assert!(like_match("abc", "a%", false).expect("m"));
        assert!(like_match("abc", "a_c", false).expect("m"));
        assert!(!like_match("ac", "a_c", false).expect("m"));
        assert!(like_match("anything", "%", false).expect("m"));
        assert!(like_match("", "%", false).expect("m"));
        assert!(like_match("axyzc", "a%c", false).expect("m"));
        assert!(!like_match("abd", "a%c", false).expect("m"));
        // `\` escapes the next pattern char: `a\%b` matches a literal `%`.
        assert!(like_match("a%b", "a\\%b", false).expect("m"));
        assert!(!like_match("axb", "a\\%b", false).expect("m"));
        // ILIKE folds ASCII case.
        assert!(like_match("ABC", "a%", true).expect("m"));
        assert!(!like_match("ABC", "a%", false).expect("m"));
        // a pattern ending in a lone `\` is an invalid escape (22025).
        assert_eq!(
            like_match("a", "a\\", false)
                .expect_err("invalid escape")
                .into_pg()
                .code,
            "22025"
        );
    }

    #[test]
    fn like_eval_null_and_type_errors() {
        assert_eq!(ev("null like 'a'", None, &[]), Datum::Null);
        assert_eq!(ev("'a' like null", None, &[]), Datum::Null);
        // a non-text operand is a 42804.
        assert_eq!(err_code("1 like 'a'", None, &[]), "42804");
    }

    #[test]
    fn case_searched_simple_and_lazy() {
        // searched: first TRUE wins; false/NULL skip.
        assert_eq!(
            ev(
                "case when false then 'a' when true then 'b' else 'c' end",
                None,
                &[]
            ),
            Datum::Text("b".into())
        );
        assert_eq!(
            ev("case when null then 'a' else 'z' end", None, &[]),
            Datum::Text("z".into())
        );
        // no match, no ELSE -> NULL.
        assert_eq!(ev("case when false then 'a' end", None, &[]), Datum::Null);
        // simple form: equality; NULL never matches.
        assert_eq!(
            ev("case 1 when 1 then 'one' else 'other' end", None, &[]),
            Datum::Text("one".into())
        );
        assert_eq!(
            ev("case null when null then 'x' else 'y' end", None, &[]),
            Datum::Text("y".into())
        );
        // lazy: the unreached `1/0` branch must not raise division-by-zero.
        assert_eq!(
            ev("case when false then 1/0 else 0 end", None, &[]),
            Datum::Int4(0)
        );
    }

    #[test]
    fn case_when_non_boolean_condition_is_42804() {
        assert_eq!(err_code("case when 1 then 'x' end", None, &[]), "42804");
    }

    // ---- SP31: explicit casts ----

    #[test]
    fn cast_evaluates_each_supported_conversion() {
        // text → numeric/bool.
        assert_eq!(ev("'42'::int4", None, &[]), Datum::Int4(42));
        assert_eq!(
            ev("'9000000000'::int8", None, &[]),
            Datum::Int8(9_000_000_000)
        );
        assert_eq!(ev("'1.5'::float8", None, &[]), Datum::Float8(1.5));
        assert_eq!(ev("'true'::bool", None, &[]), Datum::Bool(true));
        // numeric → numeric (float8 → int rounds half-to-even).
        assert_eq!(ev("1.5::int4", None, &[]), Datum::Int4(2));
        assert_eq!(ev("(5::int8)::int4", None, &[]), Datum::Int4(5));
        // bool ↔ int4, and → text (`true`/`false`, not `t`/`f`).
        assert_eq!(ev("true::int4", None, &[]), Datum::Int4(1));
        assert_eq!(ev("5::bool", None, &[]), Datum::Bool(true));
        assert_eq!(ev("0::bool", None, &[]), Datum::Bool(false));
        assert_eq!(ev("42::text", None, &[]), Datum::Text("42".into()));
        assert_eq!(ev("true::text", None, &[]), Datum::Text("true".into()));
        // NULL casts to NULL; the CAST() spelling is identical to `::`.
        assert_eq!(ev("null::int4", None, &[]), Datum::Null);
        assert_eq!(ev("CAST('7' AS int4)", None, &[]), Datum::Int4(7));
    }

    #[test]
    fn cast_infers_target_type_and_rejects_undefined_at_plan_time() {
        let t = table();
        // The static result type is the target type; a column operand resolves too.
        assert_eq!(
            infer_type(&pexpr("'42'::int8").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Int8
        );
        assert_eq!(
            infer_type(&pexpr("a::text").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Text
        );
        // A bare NULL infers as text, and text → anything is defined.
        assert_eq!(
            infer_type(&pexpr("null::bool").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Bool
        );
        // An undefined cast is rejected at plan time (42846), before evaluation:
        // a float8 column → bool has no defined cast.
        let ft = Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column {
                name: "a".into(),
                ty: ColumnType::Float8,
            }],
        };
        let err = infer_type(&pexpr("a::bool").expect("parse"), &scope_of(Some(&ft)))
            .expect_err("float8->bool is undefined");
        assert_eq!(err.into_pg().code, "42846");
    }

    #[test]
    fn cast_runtime_error_surface() {
        // Undefined cast at eval (42846), bad text syntax (22P02), overflow (22003).
        assert_eq!(err_code("1.5::bool", None, &[]), "42846");
        assert_eq!(err_code("'abc'::int4", None, &[]), "22P02");
        assert_eq!(err_code("'99999999999'::int4", None, &[]), "22003");
    }

    #[test]
    fn infer_predicate_and_case_result_types() {
        let t = table();
        let scope = scope_of(Some(&t));
        for sql in ["a is null", "a in (1,2)", "a between 1 and 2", "a like 'x'"] {
            // `a` is int4; `a like 'x'` infers Bool statically regardless.
            let got = infer_type(&pexpr(sql).expect("parse"), &scope).expect("infer");
            assert_eq!(got, ColumnType::Bool, "for {sql}");
        }
        // CASE unifies int4 + int8 -> int8.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else 2147483648 end").expect("parse"),
                &scope
            )
            .expect("infer"),
            ColumnType::Int8
        );
        // a bare NULL branch is type-neutral -> int4 from the other branch.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else null end").expect("parse"),
                &scope
            )
            .expect("infer"),
            ColumnType::Int4
        );
        // incompatible branch types -> 42804.
        let err = infer_type(
            &pexpr("case when a > 0 then 1 else 'x' end").expect("parse"),
            &scope,
        )
        .expect_err("incompatible");
        assert_eq!(err.into_pg().code, "42804");
    }
}
