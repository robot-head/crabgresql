//! SP38: set operations — UNION / INTERSECT / EXCEPT [ALL].
//!
//! A set operation folds the outputs of two or more SELECT branches. Each leaf is
//! evaluated to a `Relation` via the existing `exec::select_to_relation` (Task 6);
//! this module supplies the pure combine: column-count check, cross-branch type
//! unification + value coercion, and the duplicate semantics. Duplicate matching
//! reuses `Datum`'s grouping `Eq`/`Hash` (NULL = NULL), which is exactly PG's
//! "not distinct" rule for set operations.

use std::collections::{HashMap, HashSet};

use kv::Kv;
use mvcc::visibility::Snapshot;
use pgparser::ast::{Expr, SetExpr, SetOp, SetQuery};
use pgtypes::{ColumnType, Datum};

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;
use crate::scope::{ColumnBinding, Scope};

/// Evaluate a complete set-operation query to a wire result. Each leaf runs through
/// the existing single-SELECT read path (`exec::select_to_relation`) under the
/// statement's snapshot handles; the tree folds via `combine`; the query-level
/// ORDER BY / OFFSET / LIMIT then apply to the combined output.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_set_operation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &SetQuery,
    ctx: &EvalCtx,
) -> Result<pgwire::engine::QueryResult, ExecError> {
    let rel = fold(catalog_kv, kv, global, gsnap, snapshot, own, &q.body, ctx)?;
    let mut rows = rel.rows;

    // Query-level ORDER BY over the OUTPUT columns: a bare integer is a 1-based
    // position; anything else is evaluated against the output scope.
    if !q.order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(q.order_by.len());
            for item in &q.order_by {
                keys.push(order_key(&item.expr, &rel.scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, &q.order_by));
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    crate::exec::apply_offset_limit(&mut rows, q.offset, q.limit);

    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    Ok(crate::exec::rows_result(fields, &rows, &ctx.time_zone))
}

/// One ORDER BY key for the set-op output: integer literal → 1-based position;
/// otherwise evaluate against the output scope (output column name / expression).
fn order_key(expr: &Expr, scope: &Scope, row: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let Expr::IntLiteral(s) = expr {
        let pos: usize = s
            .parse()
            .map_err(|_| ExecError::Unsupported(format!("invalid ORDER BY position {s}")))?;
        if pos == 0 || pos > scope.width() {
            return Err(ExecError::Unsupported(format!(
                "ORDER BY position {pos} is out of range (1..{})",
                scope.width()
            )));
        }
        return Ok(row[pos - 1].clone());
    }
    crate::eval::eval(expr, scope, row, ctx)
}

#[allow(clippy::too_many_arguments)]
fn fold(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    e: &SetExpr,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    match e {
        SetExpr::Select(s) => {
            crate::exec::select_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, s, ctx)
        }
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            let l = fold(catalog_kv, kv, global, gsnap, snapshot, own, left, ctx)?;
            let r = fold(catalog_kv, kv, global, gsnap, snapshot, own, right, ctx)?;
            combine(*op, *all, l, r, ctx)
        }
    }
}

/// Combine two child relations under one set operator into a single relation.
/// Output column NAMES come from the left child; TYPES are the per-column
/// unification of both children (numeric tower + identical types; incompatible →
/// 42804). Rows of both sides are coerced to the unified types before combining.
pub(crate) fn combine(
    op: SetOp,
    all: bool,
    left: Relation,
    right: Relation,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    let (lw, rw) = (left.scope.width(), right.scope.width());
    if lw != rw {
        return Err(ExecError::SetOpColumnCount {
            op,
            left: lw,
            right: rw,
        });
    }
    let mut out_cols = Vec::with_capacity(lw);
    let mut tys = Vec::with_capacity(lw);
    for i in 0..lw {
        let ty = crate::eval::unify_types(left.scope.ty_at(i), right.scope.ty_at(i))?;
        tys.push(ty);
        out_cols.push(ColumnBinding {
            qualifier: None,
            name: left.scope.columns[i].name.clone(),
            ty,
        });
    }
    let lrows = coerce_rows(left.rows, &left.scope, &tys, ctx)?;
    let rrows = coerce_rows(right.rows, &right.scope, &tys, ctx)?;

    let rows = match op {
        SetOp::Union if all => {
            let mut v = lrows;
            v.extend(rrows);
            v
        }
        SetOp::Union => dedup_keep_order(lrows.into_iter().chain(rrows)),
        SetOp::Intersect => intersect(lrows, rrows, all),
        SetOp::Except => except(lrows, rrows, all),
    };
    Ok(Relation {
        scope: Scope { columns: out_cols },
        rows,
    })
}

/// Coerce each row's cells from the child's column types to the unified `tys`.
fn coerce_rows(
    rows: Vec<Vec<Datum>>,
    scope: &Scope,
    tys: &[ColumnType],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(row.len());
        for (i, cell) in row.into_iter().enumerate() {
            if scope.ty_at(i) == tys[i] || cell.is_null() {
                cells.push(cell);
            } else {
                cells.push(pgtypes::cast::cast(&cell, tys[i], &ctx.time_zone)?);
            }
        }
        out.push(cells);
    }
    Ok(out)
}

/// Distinct, preserving first-seen order (UNION).
fn dedup_keep_order<I: Iterator<Item = Vec<Datum>>>(it: I) -> Vec<Vec<Datum>> {
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in it {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

/// Multiset count of each distinct row.
fn counts(rows: &[Vec<Datum>]) -> HashMap<Vec<Datum>, usize> {
    let mut m: HashMap<Vec<Datum>, usize> = HashMap::new();
    for r in rows {
        *m.entry(r.clone()).or_insert(0) += 1;
    }
    m
}

/// INTERSECT: rows in both. distinct → once per distinct row present in both;
/// ALL → min(Lₙ, Rₙ). Distinct left rows are processed in first-seen order.
fn intersect(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows); // read only on the ALL path (min multiplicity)
    let rc = counts(&rrows);
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue; // each distinct left row handled once, in order
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        if rcount == 0 {
            continue; // not present in right
        }
        let mult = if all { lc[row].min(rcount) } else { 1 };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

/// EXCEPT: distinct → distinct left rows ABSENT from right (count_R == 0), once;
/// ALL → max(0, Lₙ − Rₙ). Distinct left rows are processed in first-seen order.
fn except(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows);
    let rc = counts(&rrows);
    let mut seen: HashSet<Vec<Datum>> = HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue;
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        let mult = if all {
            lc[row].saturating_sub(rcount)
        } else if rcount == 0 {
            1
        } else {
            0
        };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::ColumnBinding;

    fn rel(name: &str, ty: ColumnType, rows: Vec<Vec<Datum>>) -> Relation {
        Relation {
            scope: Scope {
                columns: vec![ColumnBinding {
                    qualifier: None,
                    name: name.into(),
                    ty,
                }],
            },
            rows,
        }
    }
    fn i4(n: i32) -> Vec<Datum> {
        vec![Datum::Int4(n)]
    }

    #[test]
    fn union_dedups_union_all_keeps() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1), i4(2)]);
        let r = rel("a", ColumnType::Int4, vec![i4(2), i4(3)]);
        let u = combine(SetOp::Union, false, l.clone(), r.clone(), &ctx).expect("union");
        assert_eq!(u.rows, vec![i4(1), i4(2), i4(3)]);
        let ua = combine(SetOp::Union, true, l, r, &ctx).expect("union all");
        assert_eq!(ua.rows, vec![i4(1), i4(2), i4(2), i4(3)]);
    }

    #[test]
    fn intersect_and_except_multiplicity() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1), i4(1), i4(2)]);
        let r = rel("a", ColumnType::Int4, vec![i4(1), i4(3)]);
        assert_eq!(
            combine(SetOp::Intersect, false, l.clone(), r.clone(), &ctx)
                .expect("i")
                .rows,
            vec![i4(1)]
        );
        assert_eq!(
            combine(SetOp::Intersect, true, l.clone(), r.clone(), &ctx)
                .expect("ia")
                .rows,
            vec![i4(1)]
        );
        // EXCEPT distinct: {2}; EXCEPT ALL: two 1s minus one 1 = one 1, plus 2 => [1,2]
        assert_eq!(
            combine(SetOp::Except, false, l.clone(), r.clone(), &ctx)
                .expect("e")
                .rows,
            vec![i4(2)]
        );
        assert_eq!(
            combine(SetOp::Except, true, l, r, &ctx).expect("ea").rows,
            vec![i4(1), i4(2)]
        );
    }

    #[test]
    fn except_all_underflows_to_empty() {
        // When the right side has MORE copies than the left, EXCEPT ALL clamps the
        // multiplicity at 0 (max(0, Lₙ − Rₙ)) — it never wraps. Pins `saturating_sub`.
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = rel("a", ColumnType::Int4, vec![i4(1), i4(1)]);
        assert_eq!(
            combine(SetOp::Except, true, l, r, &ctx).expect("ea").rows,
            Vec::<Vec<Datum>>::new()
        );
    }

    #[test]
    fn null_equals_null_in_dedup() {
        let ctx = EvalCtx::test_default();
        let n = || vec![Datum::Null];
        let l = rel("a", ColumnType::Int4, vec![n(), n()]);
        let r = rel("a", ColumnType::Int4, vec![n()]);
        assert_eq!(
            combine(SetOp::Union, false, l, r, &ctx).expect("u").rows,
            vec![n()]
        );
    }

    #[test]
    fn unifies_int4_and_int8_to_int8() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = rel("a", ColumnType::Int8, vec![vec![Datum::Int8(2)]]);
        let u = combine(SetOp::Union, true, l, r, &ctx).expect("u");
        assert_eq!(u.scope.ty_at(0), ColumnType::Int8);
        assert_eq!(u.rows, vec![vec![Datum::Int8(1)], vec![Datum::Int8(2)]]);
    }

    #[test]
    fn column_count_mismatch_errors() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = Relation {
            scope: Scope {
                columns: vec![
                    ColumnBinding {
                        qualifier: None,
                        name: "a".into(),
                        ty: ColumnType::Int4,
                    },
                    ColumnBinding {
                        qualifier: None,
                        name: "b".into(),
                        ty: ColumnType::Int4,
                    },
                ],
            },
            rows: vec![vec![Datum::Int4(1), Datum::Int4(2)]],
        };
        assert_eq!(
            combine(SetOp::Union, false, l, r, &ctx).expect_err("count mismatch"),
            ExecError::SetOpColumnCount {
                op: SetOp::Union,
                left: 1,
                right: 2
            }
        );
    }

    #[test]
    fn incompatible_types_error_42804() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = rel("a", ColumnType::Text, vec![vec![Datum::Text("x".into())]]);
        assert!(matches!(
            combine(SetOp::Union, false, l, r, &ctx).expect_err("incompatible"),
            ExecError::TypeMismatch(_)
        ));
    }

    /// End-to-end: UNION deduplicates across two tables and ORDER BY positions
    /// the combined output — exercises `execute_set_operation` + session dispatch.
    #[tokio::test]
    async fn union_runs_end_to_end() {
        use pgwire::engine::{Engine, QueryResult, Session};

        use crate::SqlEngine;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            "CREATE TABLE t (a int4)",
            "INSERT INTO t VALUES (1),(2),(2)",
            "CREATE TABLE u (a int4)",
            "INSERT INTO u VALUES (2),(3)",
        ] {
            s.simple_query(sql).await.expect("setup");
        }
        let r = s
            .simple_query("SELECT a FROM t UNION SELECT a FROM u ORDER BY a")
            .await
            .expect("union");
        let QueryResult::Rows { rows, .. } = &r[0] else {
            panic!("expected rows")
        };
        let got: Vec<_> = rows
            .iter()
            .map(|row| row[0].as_ref().expect("non-null").text.to_vec())
            .collect();
        assert_eq!(
            got,
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
            "UNION should dedup and order: [1, 2, 3]"
        );
    }
}
