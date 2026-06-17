//! SP34: uncorrelated subquery resolution (scalar / IN / EXISTS / ANY-ALL).
//!
//! An uncorrelated subquery's result is identical for every outer row, so it is
//! evaluated ONCE here — before the outer row loop — and the node is rewritten into
//! already-supported nodes (`Expr::Const`; an `InList` of consts; an `OR`/`AND`
//! fold of comparisons). The pure `eval`/`agg` evaluators then run unchanged over
//! the rewritten tree. Subqueries run through the SP33 join read path
//! (`exec::select_to_relation`) under the SAME snapshot handles as the outer query,
//! so the read is consistent and read-your-writes holds. Correlation (a reference
//! to an outer column) is out of scope: a subquery's scope is built solely from its
//! own FROM, so an outer-column reference fails name resolution (42703).

use pgparser::ast::{BinaryOp, Expr, FuncArgs, FuncCall, SelectItem, SelectStmt};
use pgtypes::{ColumnType, Datum};

use crate::error::ExecError;
use crate::exec::{build_from_schema, resolve_projection, select_to_relation};
use crate::scope::Scope;

/// The read-side handles a subquery needs to execute (mirrors `execute_read`'s
/// parameters). Threaded through the resolution recursion so each nested subquery
/// reads under the outer query's snapshot.
pub(crate) struct SubCtx<'a> {
    pub catalog_kv: &'a dyn kv::Kv,
    pub kv: &'a dyn kv::Kv,
    pub global: &'a dyn kv::Kv,
    pub gsnap: &'a mvcc::visibility::Snapshot,
    pub snapshot: &'a mvcc::visibility::Snapshot,
    pub own: Option<u64>,
}

/// Rewrite every uncorrelated subquery in `s`'s expr-bearing clauses to a resolved
/// constant form. The FROM clause (base tables, joins, derived tables) is left to
/// the SP33 join read path; only expression positions are rewritten here.
pub(crate) fn resolve_in_select(ctx: &SubCtx, s: &SelectStmt) -> Result<SelectStmt, ExecError> {
    let mut out = s.clone();
    for item in &mut out.projection {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = resolve_expr(ctx, expr)?;
        }
    }
    if let Some(f) = &mut out.filter {
        *f = resolve_expr(ctx, f)?;
    }
    if let Some(h) = &mut out.having {
        *h = resolve_expr(ctx, h)?;
    }
    for g in &mut out.group_by {
        *g = resolve_expr(ctx, g)?;
    }
    for o in &mut out.order_by {
        o.expr = resolve_expr(ctx, &o.expr)?;
    }
    Ok(out)
}

/// Recursively rewrite subquery nodes in `e`, bottom-up.
fn resolve_expr(ctx: &SubCtx, e: &Expr) -> Result<Expr, ExecError> {
    Ok(match e {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Const { .. } => e.clone(),
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_expr(ctx, expr)?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(resolve_expr(ctx, left)?),
            right: Box::new(resolve_expr(ctx, right)?),
        },
        Expr::Func(fc) => Expr::Func(FuncCall {
            name: fc.name.clone(),
            distinct: fc.distinct,
            args: match &fc.args {
                FuncArgs::Star => FuncArgs::Star,
                FuncArgs::Exprs(args) => FuncArgs::Exprs(
                    args.iter()
                        .map(|a| resolve_expr(ctx, a))
                        .collect::<Result<_, _>>()?,
                ),
            },
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(resolve_expr(ctx, expr)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(resolve_expr(ctx, expr)?),
            list: list
                .iter()
                .map(|x| resolve_expr(ctx, x))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(resolve_expr(ctx, expr)?),
            low: Box::new(resolve_expr(ctx, low)?),
            high: Box::new(resolve_expr(ctx, high)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(resolve_expr(ctx, expr)?),
            pattern: Box::new(resolve_expr(ctx, pattern)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: match operand {
                Some(o) => Some(Box::new(resolve_expr(ctx, o)?)),
                None => None,
            },
            whens: whens
                .iter()
                .map(|(c, r)| Ok((resolve_expr(ctx, c)?, resolve_expr(ctx, r)?)))
                .collect::<Result<_, ExecError>>()?,
            else_result: match else_result {
                Some(o) => Some(Box::new(resolve_expr(ctx, o)?)),
                None => None,
            },
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(resolve_expr(ctx, expr)?),
            ty: *ty,
        },
        // ---- the subquery nodes: run once, fold to constants ----
        Expr::ScalarSubquery(s) => {
            let (value, ty) = run_scalar(ctx, s)?;
            Expr::Const { value, ty }
        }
        Expr::Exists(s) => {
            let rows = run_rows(ctx, s)?;
            Expr::Const {
                value: Datum::Bool(!rows.is_empty()),
                ty: ColumnType::Bool,
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let (ty, values) = run_single_column(ctx, subquery)?;
            Expr::InList {
                expr: Box::new(resolve_expr(ctx, expr)?),
                list: values
                    .into_iter()
                    .map(|value| Expr::Const { value, ty })
                    .collect(),
                negated: *negated,
            }
        }
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => {
            let (ty, values) = run_single_column(ctx, subquery)?;
            let lhs = resolve_expr(ctx, expr)?;
            lower_quantified(&lhs, *op, *all, ty, values)
        }
    })
}

/// Reject a `FOR UPDATE/SHARE` inside a subquery (meaningless for a folded read).
fn no_locking(s: &SelectStmt) -> Result<(), ExecError> {
    if s.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed inside a subquery".into(),
        ));
    }
    Ok(())
}

/// Run a subquery through the join read path to its materialized rows.
fn run_relation(ctx: &SubCtx, s: &SelectStmt) -> Result<crate::join::Relation, ExecError> {
    no_locking(s)?;
    select_to_relation(
        ctx.catalog_kv,
        ctx.kv,
        ctx.global,
        ctx.gsnap,
        ctx.snapshot,
        ctx.own,
        s,
    )
}

/// Run a subquery to its raw rows (any shape) — used by `EXISTS`.
fn run_rows(ctx: &SubCtx, s: &SelectStmt) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(run_relation(ctx, s)?.rows)
}

/// Run a scalar subquery: exactly one column, at most one row → `(value, type)`.
fn run_scalar(ctx: &SubCtx, s: &SelectStmt) -> Result<(Datum, ColumnType), ExecError> {
    let rel = run_relation(ctx, s)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    if rel.rows.len() > 1 {
        return Err(ExecError::CardinalityViolation);
    }
    let value = rel
        .rows
        .into_iter()
        .next()
        .map(|mut r| r.remove(0))
        .unwrap_or(Datum::Null);
    Ok((value, ty))
}

/// Run a single-column subquery → its column type + every value (in row order).
fn run_single_column(ctx: &SubCtx, s: &SelectStmt) -> Result<(ColumnType, Vec<Datum>), ExecError> {
    let rel = run_relation(ctx, s)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    let col = rel.rows.into_iter().map(|mut r| r.remove(0)).collect();
    Ok((ty, col))
}

/// Lower `lhs op ANY|SOME|ALL (values)` to an `OR`/`AND` fold of comparisons, with
/// PostgreSQL's empty-set semantics (ANY → false, ALL → true). NULL three-valued
/// logic falls out of the existing `ops::or`/`ops::and`/`ops::compare`.
fn lower_quantified(
    lhs: &Expr,
    op: BinaryOp,
    all: bool,
    ty: ColumnType,
    values: Vec<Datum>,
) -> Expr {
    if values.is_empty() {
        return Expr::Const {
            value: Datum::Bool(all),
            ty: ColumnType::Bool,
        };
    }
    let join = if all { BinaryOp::And } else { BinaryOp::Or };
    let mut acc: Option<Expr> = None;
    for v in values {
        let cmp = Expr::Binary {
            op,
            left: Box::new(lhs.clone()),
            right: Box::new(Expr::Const { value: v, ty }),
        };
        acc = Some(match acc {
            None => cmp,
            Some(prev) => Expr::Binary {
                op: join,
                left: Box::new(prev),
                right: Box::new(cmp),
            },
        });
    }
    acc.expect("non-empty values")
}

// ---- describe (extended-protocol, no execution): catalog-only type pass ----

/// Rewrite scalar subqueries in a projection to a typed-NULL `Const`, so
/// `infer_type` yields the right OID without executing. EXISTS / IN / quantified
/// nodes are left for `infer_type` to type as boolean. Used by `describe`.
pub(crate) fn resolve_types_in_projection(
    catalog_kv: &dyn kv::Kv,
    items: &[SelectItem],
) -> Result<Vec<SelectItem>, ExecError> {
    items
        .iter()
        .map(|it| match it {
            SelectItem::Expr { expr, alias } => Ok(SelectItem::Expr {
                expr: resolve_types_in_expr(catalog_kv, expr)?,
                alias: alias.clone(),
            }),
            other => Ok(other.clone()),
        })
        .collect()
}

/// Recursively replace scalar subqueries with `Const { Null, <type> }` (type-only).
fn resolve_types_in_expr(catalog_kv: &dyn kv::Kv, e: &Expr) -> Result<Expr, ExecError> {
    Ok(match e {
        Expr::ScalarSubquery(s) => Expr::Const {
            value: Datum::Null,
            ty: scalar_subquery_type(catalog_kv, s)?,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_types_in_expr(catalog_kv, expr)?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(resolve_types_in_expr(catalog_kv, left)?),
            right: Box::new(resolve_types_in_expr(catalog_kv, right)?),
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(resolve_types_in_expr(catalog_kv, expr)?),
            ty: *ty,
        },
        // Everything else (incl. EXISTS / IN / quantified, which infer as bool) is
        // typed directly by `infer_type` without substitution.
        other => other.clone(),
    })
}

/// The static type of a scalar subquery's single projection column (catalog only).
fn scalar_subquery_type(catalog_kv: &dyn kv::Kv, s: &SelectStmt) -> Result<ColumnType, ExecError> {
    let scope = if s.from.is_empty() {
        Scope::empty()
    } else {
        build_from_schema(catalog_kv, &s.from)?.scope
    };
    // Type-resolve nested scalar subqueries inside this subquery's projection first.
    let projection = resolve_types_in_projection(catalog_kv, &s.projection)?;
    let (fields, _exprs, tys) = resolve_projection(&projection, &scope)?;
    if fields.len() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    Ok(tys[0])
}

#[cfg(test)]
mod tests {
    use crate::SqlEngine;
    use pgwire::engine::{Cell, Engine, QueryResult, Session};

    async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
        engine
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn cell0(r: &QueryResult) -> Option<String> {
        match r {
            QueryResult::Rows { rows, .. } => rows
                .first()
                .and_then(|row| row[0].as_ref())
                .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8")),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn rowcount(r: &QueryResult) -> usize {
        match r {
            QueryResult::Rows { rows, .. } => rows.len(),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    async fn seed() -> SqlEngine {
        let e = SqlEngine::new();
        run(&e, "CREATE TABLE t (id int4, v int4)").await;
        run(&e, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").await;
        e
    }

    #[tokio::test]
    async fn scalar_subquery_in_projection_and_where() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT (SELECT max(v) FROM t)").await),
            Some("30".into())
        );
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1); // only id=3 (v=30 > avg 20)
        assert_eq!(cell0(&r), Some("3".into()));
    }

    #[tokio::test]
    async fn scalar_subquery_zero_rows_is_null() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT (SELECT v FROM t WHERE id = 999)").await),
            None,
        );
    }

    #[tokio::test]
    async fn scalar_subquery_more_than_one_row_is_21000() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT (SELECT v FROM t)")
            .await
            .expect_err("cardinality");
        assert_eq!(err.code, "21000");
    }

    #[tokio::test]
    async fn scalar_subquery_more_than_one_column_is_42601() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT (SELECT id, v FROM t WHERE id = 1)")
            .await
            .expect_err("columns");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn exists_and_not_exists() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 1)").await),
            Some("t".into())
        );
        assert_eq!(
            cell0(&run(&e, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 999)").await),
            Some("f".into())
        );
        let r = run(
            &e,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM t WHERE id = 999) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 3);
    }

    #[tokio::test]
    async fn in_subquery_and_not_in() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (1), (3)").await;
        let r = run(
            &e,
            "SELECT id FROM t WHERE id IN (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 2);
        assert_eq!(cell0(&r), Some("1".into()));
        let r = run(
            &e,
            "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1);
        assert_eq!(cell0(&r), Some("2".into()));
    }

    #[tokio::test]
    async fn not_in_with_a_null_element_is_unknown_for_all() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (1), (null)").await;
        let r = run(&e, "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u)").await;
        assert_eq!(rowcount(&r), 0);
    }

    #[tokio::test]
    async fn quantified_any_all_and_empty_set() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (15), (25)").await;
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > ALL (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1);
        assert_eq!(cell0(&r), Some("3".into()));
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > ANY (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 2);
        run(&e, "CREATE TABLE empt (k int4)").await;
        let r_any = run(&e, "SELECT id FROM t WHERE v > ANY (SELECT k FROM empt)").await;
        assert_eq!(rowcount(&r_any), 0);
        let r_all = run(&e, "SELECT id FROM t WHERE v > ALL (SELECT k FROM empt)").await;
        assert_eq!(rowcount(&r_all), 3);
    }

    #[tokio::test]
    async fn in_subquery_more_than_one_column_is_42601() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT id FROM t WHERE id IN (SELECT id, v FROM t)")
            .await
            .expect_err("columns");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn describe_types_a_scalar_subquery_projection_column() {
        let e = seed().await; // t (id int4, v int4)
        // A scalar subquery in the projection types as its single column's type (int4),
        // without executing — the catalog-only describe type pass.
        let fields = crate::describe_fields(&*e.kv, "SELECT (SELECT max(v) FROM t) FROM t")
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].type_oid, pgtypes::oids::INT4); // max(int4) → int4
    }
}
