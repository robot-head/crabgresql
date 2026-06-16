//! SP33: nested-loop joins over `Relation`s. A `Relation` is a `Scope` (ordered
//! schema) plus its materialized rows; base tables, joins, and derived subqueries
//! all produce one. This module is pure relational algebra over already-fetched
//! rows — no kv/catalog access — so it is unit-testable with hand-built relations.
//! (See the SP33 design doc for why this single-range pure fold warrants no model.)

use pgparser::ast::{Expr, JoinConstraint, JoinKind};
use pgtypes::Datum;

use crate::error::ExecError;
use crate::scope::Scope;

/// A materialized relation: an ordered `Scope` (the schema) plus its rows, each
/// row positionally aligned to `scope.columns`. Base tables, joins, and (later)
/// derived subqueries all produce one.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub scope: Scope,
    pub rows: Vec<Vec<Datum>>,
}

/// Join two relations under `kind` + `constraint`, returning the combined
/// relation. INNER/CROSS only in this step; outer kinds land in the next task.
pub(crate) fn join_relations(
    left: Relation,
    right: Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
) -> Result<Relation, ExecError> {
    // Self-join / duplicate alias: a qualifier may not appear on both sides.
    for c in &right.scope.columns {
        if let Some(q) = &c.qualifier
            && left
                .scope
                .columns
                .iter()
                .any(|lc| lc.qualifier.as_ref() == Some(q))
        {
            return Err(ExecError::DuplicateAlias(q.clone()));
        }
    }

    // Combined schema for predicate evaluation (left ++ right).
    let mut combined_scope = left.scope.clone();
    combined_scope
        .columns
        .extend(right.scope.columns.iter().cloned());

    // The effective ON predicate. USING/NATURAL synthesis lands in a later task;
    // here On(expr) or always-true (Cross / None).
    let pred: Option<&Expr> = match constraint {
        JoinConstraint::On(e) => Some(e),
        JoinConstraint::None => None,
        JoinConstraint::Using(_) | JoinConstraint::Natural => {
            return Err(ExecError::Unsupported(
                "USING/NATURAL land in a later task".into(),
            ));
        }
    };

    let matches = |lrow: &[Datum], rrow: &[Datum]| -> Result<bool, ExecError> {
        let Some(e) = pred else { return Ok(true) };
        let mut combined = lrow.to_vec();
        combined.extend_from_slice(rrow);
        match crate::eval::eval(e, &combined_scope, &combined)? {
            Datum::Bool(b) => Ok(b),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "JOIN/ON condition must be boolean".into(),
            )),
        }
    };

    let mut rows = Vec::new();
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            for l in &left.rows {
                for r in &right.rows {
                    if matches(l, r)? {
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        rows.push(row);
                    }
                }
            }
        }
        JoinKind::Left | JoinKind::Right | JoinKind::Full => {
            let lw = left.scope.width();
            let rw = right.scope.width();
            let want_left = matches!(kind, JoinKind::Left | JoinKind::Full);
            let want_right = matches!(kind, JoinKind::Right | JoinKind::Full);
            let mut right_matched = vec![false; right.rows.len()];
            for l in &left.rows {
                let mut any = false;
                for (ri, r) in right.rows.iter().enumerate() {
                    if matches(l, r)? {
                        any = true;
                        right_matched[ri] = true;
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        rows.push(row);
                    }
                }
                if !any && want_left {
                    let mut row = l.clone();
                    row.extend(vec![Datum::Null; rw]);
                    rows.push(row);
                }
            }
            if want_right {
                for (ri, r) in right.rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let mut row = vec![Datum::Null; lw];
                        row.extend(r.iter().cloned());
                        rows.push(row);
                    }
                }
            }
        }
    }
    Ok(Relation {
        scope: combined_scope,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::ColumnType;

    fn rel(qual: &str, cols: &[&str], rows: Vec<Vec<i32>>) -> Relation {
        let scope = Scope {
            columns: cols
                .iter()
                .map(|n| crate::scope::ColumnBinding {
                    qualifier: Some(qual.into()),
                    name: (*n).into(),
                    ty: ColumnType::Int4,
                })
                .collect(),
        };
        Relation {
            scope,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(Datum::Int4).collect())
                .collect(),
        }
    }

    fn on_eq(lq: &str, lc: &str, rq: &str, rc: &str) -> JoinConstraint {
        JoinConstraint::On(Expr::Binary {
            op: pgparser::ast::BinaryOp::Eq,
            left: Box::new(Expr::Column {
                table: Some(lq.into()),
                name: lc.into(),
            }),
            right: Box::new(Expr::Column {
                table: Some(rq.into()),
                name: rc.into(),
            }),
        })
    }

    #[test]
    fn inner_join_keeps_only_matches() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3], vec![4]]);
        let j = join_relations(a, b, JoinKind::Inner, &on_eq("a", "id", "b", "id")).expect("join");
        assert_eq!(
            j.rows,
            vec![
                vec![Datum::Int4(2), Datum::Int4(2)],
                vec![Datum::Int4(3), Datum::Int4(3)]
            ]
        );
    }

    #[test]
    fn cross_join_is_the_product() {
        let a = rel("a", &["x"], vec![vec![1], vec![2]]);
        let b = rel("b", &["y"], vec![vec![9]]);
        let j = join_relations(a, b, JoinKind::Cross, &JoinConstraint::None).expect("cross join");
        assert_eq!(j.rows.len(), 2);
        assert_eq!(j.scope.width(), 2);
    }

    #[test]
    fn left_join_null_extends_unmatched_left_rows() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j =
            join_relations(a, b, JoinKind::Left, &on_eq("a", "id", "b", "id")).expect("left join");
        // id=1 has no match -> (1, NULL); 2,3 match.
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null]));
        assert_eq!(j.rows.len(), 3);
    }

    #[test]
    fn right_join_null_extends_unmatched_right_rows() {
        let a = rel("a", &["id"], vec![vec![2]]);
        let b = rel("b", &["id"], vec![vec![1], vec![2]]);
        let j = join_relations(a, b, JoinKind::Right, &on_eq("a", "id", "b", "id"))
            .expect("right join");
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(1)]));
        assert_eq!(j.rows.len(), 2);
    }

    #[test]
    fn full_join_keeps_unmatched_from_both_sides() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j =
            join_relations(a, b, JoinKind::Full, &on_eq("a", "id", "b", "id")).expect("full join");
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null])); // unmatched left
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(3)])); // unmatched right
        assert!(j.rows.contains(&vec![Datum::Int4(2), Datum::Int4(2)])); // matched
        assert_eq!(j.rows.len(), 3);
    }
}
