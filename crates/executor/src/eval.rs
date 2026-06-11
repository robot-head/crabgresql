//! Expression evaluation over Datums, plus static result-type inference (used
//! to build a stable RowDescription before any row is produced).

use std::cmp::Ordering;

use catalog::Table;
use pgparser::ast::{BinaryOp, Expr, UnaryOp};
use pgtypes::{ColumnType, Datum, ops};

use crate::error::ExecError;

/// Evaluate `expr` against a row (`values`, aligned to `table.columns`).
pub(crate) fn eval(
    expr: &Expr,
    table: Option<&Table>,
    values: &[Datum],
) -> Result<Datum, ExecError> {
    match expr {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Column(name) => {
            let t = table.ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            let idx = t
                .column_index(name)
                .ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            Ok(values[idx].clone())
        }
        Expr::Unary { op, expr } => {
            let v = eval(expr, table, values)?;
            match op {
                UnaryOp::Not => Ok(ops::not(&v)?),
                UnaryOp::Neg => Ok(ops::sub(&Datum::Int4(0), &v)?),
            }
        }
        Expr::Binary { op, left, right } => {
            let l = eval(left, table, values)?;
            let r = eval(right, table, values)?;
            match op {
                BinaryOp::Add => Ok(ops::add(&l, &r)?),
                BinaryOp::Sub => Ok(ops::sub(&l, &r)?),
                BinaryOp::Mul => Ok(ops::mul(&l, &r)?),
                BinaryOp::Div => Ok(ops::div(&l, &r)?),
                BinaryOp::And => Ok(ops::and(&l, &r)?),
                BinaryOp::Or => Ok(ops::or(&l, &r)?),
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge => {
                    let ord = ops::compare(&l, &r)?;
                    Ok(cmp_result(*op, ord))
                }
            }
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
pub(crate) fn infer_type(expr: &Expr, table: Option<&Table>) -> Result<ColumnType, ExecError> {
    match expr {
        Expr::IntLiteral(s) => match ops::int_literal(s)? {
            Datum::Int4(_) => Ok(ColumnType::Int4),
            Datum::Int8(_) => Ok(ColumnType::Int8),
            _ => unreachable!(),
        },
        Expr::StringLiteral(_) => Ok(ColumnType::Text),
        Expr::BoolLiteral(_) => Ok(ColumnType::Bool),
        // PostgreSQL types a bare NULL as "unknown"; the slice uses text as a
        // concrete stand-in so RowDescription has a real OID.
        Expr::NullLiteral => Ok(ColumnType::Text),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Column(name) => {
            let t = table.ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            let idx = t
                .column_index(name)
                .ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            Ok(t.columns[idx].ty)
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => Ok(ColumnType::Bool),
            UnaryOp::Neg => infer_type(expr, table),
        },
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                let (lt, rt) = (infer_type(left, table)?, infer_type(right, table)?);
                Ok(if lt == ColumnType::Int4 && rt == ColumnType::Int4 {
                    ColumnType::Int4
                } else {
                    ColumnType::Int8
                })
            }
            _ => Ok(ColumnType::Bool),
        },
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

    fn ev(sql: &str, t: Option<&Table>, vals: &[Datum]) -> Datum {
        eval(&pexpr(sql).expect("parse"), t, vals).expect("eval")
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
    fn undefined_column_is_42703() {
        let t = table();
        let err = eval(
            &pexpr("zzz").expect("parse"),
            Some(&t),
            &[Datum::Int4(1), Datum::Int4(1)],
        )
        .expect_err("eval zzz should fail");
        assert_eq!(err.into_pg().code, "42703");
    }

    #[test]
    fn parameter_is_0a000() {
        let err = eval(&pexpr("$1").expect("parse"), None, &[]).expect_err("eval $1 should fail");
        assert_eq!(err.into_pg().code, "0A000");
    }

    #[test]
    fn type_inference_is_static() {
        let t = table();
        assert_eq!(
            infer_type(&pexpr("a + b").expect("parse"), Some(&t)).expect("infer"),
            ColumnType::Int4
        );
        assert_eq!(
            infer_type(&pexpr("a > b").expect("parse"), Some(&t)).expect("infer"),
            ColumnType::Bool
        );
        assert_eq!(
            infer_type(&pexpr("'x'").expect("parse"), None).expect("infer"),
            ColumnType::Text
        );
        assert_eq!(
            infer_type(&pexpr("2147483648").expect("parse"), None).expect("infer"),
            ColumnType::Int8
        );
    }
}
