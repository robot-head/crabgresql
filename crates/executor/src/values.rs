use pgparser::ast::{Expr, ValuesQuery, ValuesStmt};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::FieldDescription;

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::scope::{ColumnBinding, Scope};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValuesSchema {
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<ColumnType>,
}

pub(crate) fn describe_values(v: &ValuesStmt) -> Result<ValuesSchema, ExecError> {
    analyze_values(v)
}

pub(crate) fn describe_values_query(q: &ValuesQuery) -> Result<Vec<FieldDescription>, ExecError> {
    let schema = describe_values(&q.body)?;
    Ok(schema
        .names
        .iter()
        .zip(&schema.types)
        .map(|(name, ty)| crate::exec::field(name, *ty))
        .collect())
}

pub(crate) fn execute_values_query(
    q: &ValuesQuery,
    ctx: &EvalCtx,
) -> Result<pgwire::engine::QueryResult, ExecError> {
    let mut rel = values_to_relation(&q.body, ctx)?;
    apply_query_order(&mut rel, &q.order_by, q.offset, q.limit, ctx)?;
    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    Ok(crate::exec::rows_result(fields, &rel.rows, &ctx.time_zone))
}

pub(crate) fn values_to_relation(
    v: &ValuesStmt,
    ctx: &EvalCtx,
) -> Result<crate::join::Relation, ExecError> {
    let schema = analyze_values(v)?;
    let mut rows = Vec::with_capacity(v.rows.len());
    for row in &v.rows {
        let mut out = Vec::with_capacity(row.len());
        for (expr, ty) in row.iter().zip(&schema.types) {
            let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
            out.push(pgtypes::cast::cast(&value, *ty, &ctx.time_zone)?);
        }
        rows.push(out);
    }
    Ok(crate::join::Relation {
        scope: scope_from_schema(&schema, None),
        rows,
    })
}

pub(crate) fn apply_query_order(
    rel: &mut crate::join::Relation,
    order_by: &[pgparser::ast::OrderItem],
    offset: Option<i64>,
    limit: Option<i64>,
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    if !order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rel.rows.len());
        for row in rel.rows.drain(..) {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(order_key(&item.expr, &rel.scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, order_by));
        rel.rows = keyed.into_iter().map(|(_, row)| row).collect();
    }
    crate::exec::apply_offset_limit(&mut rel.rows, offset, limit);
    Ok(())
}

pub(crate) fn requalify_derived(
    mut rel: crate::join::Relation,
    alias: &str,
    columns: &Option<Vec<String>>,
) -> Result<crate::join::Relation, ExecError> {
    if let Some(names) = columns {
        if names.len() != rel.scope.width() {
            return Err(ExecError::DerivedColumnAliasCount {
                expected: rel.scope.width(),
                got: names.len(),
            });
        }
        for (col, name) in rel.scope.columns.iter_mut().zip(names) {
            col.name = name.clone();
        }
    }
    for col in &mut rel.scope.columns {
        col.qualifier = Some(alias.to_string());
    }
    Ok(rel)
}

fn scope_from_schema(schema: &ValuesSchema, qualifier: Option<&str>) -> Scope {
    Scope {
        columns: schema
            .names
            .iter()
            .zip(&schema.types)
            .map(|(name, ty)| ColumnBinding {
                qualifier: qualifier.map(str::to_string),
                name: name.clone(),
                ty: *ty,
            })
            .collect(),
    }
}

fn order_key(expr: &Expr, scope: &Scope, row: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let Expr::IntLiteral(s) = expr {
        let pos: usize = s.parse().map_err(|_| {
            ExecError::InvalidColumnReference(format!(
                "ORDER BY position {s} is not in select list"
            ))
        })?;
        if pos == 0 || pos > scope.width() {
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {pos} is not in select list"
            )));
        }
        return Ok(row[pos - 1].clone());
    }
    crate::eval::eval(expr, scope, row, ctx)
}

fn analyze_values(v: &ValuesStmt) -> Result<ValuesSchema, ExecError> {
    let width = v.rows.first().map(Vec::len).unwrap_or(0);
    let mut cols: Vec<(ColumnType, bool)> = vec![(ColumnType::Text, true); width];
    for row in &v.rows {
        if row.len() != width {
            return Err(ExecError::ValuesColumnCount);
        }
        for (idx, expr) in row.iter().enumerate() {
            let ty = infer_values_expr_type(expr)?;
            let unknown = is_unknown_literal(expr);
            cols[idx] = unify_values_col(cols[idx].0, cols[idx].1, ty, unknown)?;
        }
    }
    let types = cols
        .into_iter()
        .map(|(ty, unknown)| if unknown { ColumnType::Text } else { ty })
        .collect::<Vec<_>>();
    let names = (1..=width).map(|n| format!("column{n}")).collect();
    Ok(ValuesSchema { names, types })
}

fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::NullLiteral | Expr::StringLiteral(_))
}

fn infer_values_expr_type(e: &Expr) -> Result<ColumnType, ExecError> {
    crate::eval::infer_type(e, &Scope::empty())
}

fn unify_values_col(
    lt: ColumnType,
    lunk: bool,
    rt: ColumnType,
    runk: bool,
) -> Result<(ColumnType, bool), ExecError> {
    Ok(match (lunk, runk) {
        (true, true) => (lt, true),
        (true, false) => (rt, false),
        (false, true) => (lt, false),
        (false, false) => (crate::eval::unify_types(lt, rt)?, false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;

    fn int(s: &str) -> Expr {
        Expr::IntLiteral(s.to_string())
    }

    fn str_lit(s: &str) -> Expr {
        Expr::StringLiteral(s.to_string())
    }

    #[test]
    fn default_names_and_types_are_resolved() {
        let v = ValuesStmt {
            rows: vec![vec![int("1"), str_lit("a")], vec![int("2"), str_lit("b")]],
        };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.names, vec!["column1", "column2"]);
        assert_eq!(schema.types, vec![ColumnType::Int4, ColumnType::Text]);
    }

    #[test]
    fn row_arity_mismatch_is_42601() {
        let v = ValuesStmt {
            rows: vec![vec![int("1")], vec![int("2"), int("3")]],
        };
        assert_eq!(describe_values(&v), Err(ExecError::ValuesColumnCount));
    }

    #[test]
    fn null_unknown_resolves_to_peer_type() {
        let v = ValuesStmt {
            rows: vec![vec![Expr::NullLiteral], vec![int("2")]],
        };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.types, vec![ColumnType::Int4]);
    }

    #[test]
    fn all_unknown_resolves_to_text() {
        let v = ValuesStmt {
            rows: vec![vec![Expr::NullLiteral], vec![str_lit("x")]],
        };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.types, vec![ColumnType::Text]);
    }

    #[test]
    fn evaluates_and_coerces_rows() {
        let v = ValuesStmt {
            rows: vec![vec![Expr::NullLiteral], vec![int("2")]],
        };
        let rel = values_to_relation(&v, &EvalCtx::test_default()).expect("relation");
        assert_eq!(rel.rows, vec![vec![Datum::Null], vec![Datum::Int4(2)]]);
        assert_eq!(rel.scope.columns[0].name, "column1");
    }
}
