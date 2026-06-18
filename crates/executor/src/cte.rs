//! Materialized common table expression scope for SELECT execution.

use std::collections::HashMap;

use pgparser::ast::WithClause;

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;

#[derive(Debug, Clone, Default)]
pub(crate) struct CteContext {
    relations: HashMap<String, Relation>,
}

impl CteContext {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn child(&self) -> Self {
        self.clone()
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<&Relation> {
        self.relations.get(name)
    }

    pub(crate) fn insert(&mut self, name: String, rel: Relation) {
        self.relations.insert(name, rel);
    }
}

pub(crate) fn reject_recursive(with: &WithClause) -> Result<(), ExecError> {
    if with.recursive {
        return Err(ExecError::Unsupported(
            "recursive CTEs are not supported yet".into(),
        ));
    }
    Ok(())
}

pub(crate) fn requalify_cte(rel: &Relation, alias: &str) -> Relation {
    let mut out = rel.clone();
    for col in &mut out.scope.columns {
        col.qualifier = Some(alias.to_string());
    }
    out
}

pub(crate) fn apply_cte_column_aliases(
    rel: Relation,
    name: &str,
    columns: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    crate::values::requalify_derived(rel, name, columns)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_with_clause(
    catalog_kv: &dyn kv::Kv,
    kv: &dyn kv::Kv,
    global: &dyn kv::Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    with: Option<&WithClause>,
    parent: &CteContext,
    ctx: &EvalCtx,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(parent.child());
    };
    reject_recursive(with)?;

    let mut out = parent.child();
    for cte in &with.ctes {
        let rel = crate::query::query_to_relation_with_ctes(
            catalog_kv, kv, global, gsnap, snapshot, own, &cte.query, &out, ctx,
        )?;
        let rel = apply_cte_column_aliases(rel, &cte.name, &cte.columns)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}

pub(crate) fn describe_with_clause(
    catalog_kv: &dyn kv::Kv,
    with: Option<&WithClause>,
    parent: &CteContext,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(parent.child());
    };
    reject_recursive(with)?;

    let mut out = parent.child();
    for cte in &with.ctes {
        let fields = crate::query::describe_query_expr_with_ctes(catalog_kv, &cte.query, &out)?;
        let columns = fields
            .iter()
            .map(|f| {
                Ok(crate::scope::ColumnBinding {
                    qualifier: None,
                    name: f.name.clone(),
                    ty: crate::exec::column_type_from_oid(f.type_oid)?,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let rel = Relation {
            scope: crate::scope::Scope { columns },
            rows: Vec::new(),
        };
        let rel = apply_cte_column_aliases(rel, &cte.name, &cte.columns)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}
