use kv::Kv;
use mvcc::visibility::Snapshot;
use pgparser::ast::{QueryBody, QueryExpr, SetExpr};
use pgwire::engine::FieldDescription;

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;
use crate::scope::Scope;

#[allow(clippy::too_many_arguments)]
pub(crate) fn query_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &QueryExpr,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    let sub_ctx = crate::subquery::SubCtx {
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        eval_ctx: ctx,
    };
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut s = (**s).clone();
            s.order_by = q.order_by.clone();
            s.limit = q.limit;
            s.offset = q.offset;
            s.locking = q.locking;
            crate::exec::select_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, &s, ctx)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let mut rel = crate::values::values_to_relation(v, ctx)?;
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::values::apply_query_order(&mut rel, &order_by, q.offset, q.limit, ctx)?;
            Ok(rel)
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut rel =
                query_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, nested, ctx)?;
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::values::apply_query_order(&mut rel, &order_by, q.offset, q.limit, ctx)?;
            Ok(rel)
        }
        SetExpr::SetOp { .. } => {
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::setops::set_expr_to_relation(
                catalog_kv, kv, global, gsnap, snapshot, own, &q.body, &order_by, q.offset,
                q.limit, ctx,
            )
        }
    }
}

pub(crate) fn describe_query_expr(
    catalog_kv: &dyn Kv,
    q: &QueryExpr,
) -> Result<Vec<FieldDescription>, ExecError> {
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            let scope = if s.from.is_empty() {
                Scope::empty()
            } else {
                crate::exec::build_from_schema(catalog_kv, &s.from)?.scope
            };
            let projection =
                crate::subquery::resolve_types_in_projection(catalog_kv, &s.projection)?;
            let (fields, _exprs, _tys) = crate::exec::resolve_projection(&projection, &scope)?;
            Ok(fields)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let schema = crate::values::describe_values(v)?;
            Ok(schema
                .names
                .iter()
                .zip(&schema.types)
                .map(|(name, ty)| crate::exec::field(name, *ty))
                .collect())
        }
        SetExpr::Query(QueryBody::Nested(nested)) => describe_query_expr(catalog_kv, nested),
        SetExpr::SetOp { .. } => crate::setops::describe_set_expr(catalog_kv, &q.body),
    }
}

pub(crate) fn relation_to_rows_result(rel: Relation, ctx: &EvalCtx) -> pgwire::engine::QueryResult {
    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    crate::exec::rows_result(fields, &rel.rows, &ctx.time_zone)
}

#[cfg(test)]
mod tests {
    use crate::SqlEngine;
    use pgwire::engine::{Engine, QueryResult, Session};

    async fn run(sql: &str) -> QueryResult {
        SqlEngine::new()
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn cells(result: QueryResult) -> Vec<Vec<Option<String>>> {
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8")))
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn top_level_select_values_and_setops_use_query_pipeline() {
        assert_eq!(cells(run("SELECT 1").await), vec![vec![Some("1".into())]]);
        assert_eq!(
            cells(run("VALUES (2), (1) ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            cells(run("SELECT 1 UNION SELECT 2 ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            cells(run("(VALUES (2), (1) ORDER BY 1) LIMIT 1").await),
            vec![vec![Some("1".into())]]
        );
    }
}
