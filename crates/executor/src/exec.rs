//! Per-statement execution.

use catalog::Column;
use pgparser::ast::Statement;
use pgwire::engine::QueryResult;

use crate::SqlEngine;
use crate::error::ExecError;

pub(crate) fn execute(engine: &SqlEngine, stmt: &Statement) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect();
            engine.catalog.create_table(name, cols)?;
            Ok(QueryResult::Command {
                tag: "CREATE TABLE".into(),
            })
        }
        Statement::DropTable { name } => {
            engine.catalog.drop_table(name)?;
            Ok(QueryResult::Command {
                tag: "DROP TABLE".into(),
            })
        }
        Statement::Insert { .. } => Err(ExecError::Unsupported("INSERT lands in Task 16".into())),
        Statement::Select(_) => Err(ExecError::Unsupported("SELECT lands in Task 17".into())),
    }
}

pub(crate) fn describe(
    engine: &SqlEngine,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let _ = (engine, sql);
    Ok(Vec::new()) // real describe lands in Task 18
}

#[cfg(test)]
mod tests {
    use crate::SqlEngine;
    use pgwire::engine::{Engine, QueryResult};

    async fn run(engine: &SqlEngine, sql: &str) -> Vec<QueryResult> {
        engine.simple_query(sql).await.expect("ok")
    }

    #[tokio::test]
    async fn create_then_drop_table() {
        let engine = SqlEngine::new();
        let r = run(&engine, "CREATE TABLE t (id int4, name text)").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "CREATE TABLE".into()
            }]
        );
        // Re-creating is a duplicate error (42P07), session survives.
        let err = engine
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect_err("dup");
        assert_eq!(err.code, "42P07");
        let r = run(&engine, "DROP TABLE t").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "DROP TABLE".into()
            }]
        );
        let err = engine.simple_query("DROP TABLE t").await.expect_err("gone");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn empty_query_yields_empty_result() {
        let engine = SqlEngine::new();
        assert_eq!(run(&engine, "   ").await, vec![QueryResult::Empty]);
    }

    #[tokio::test]
    async fn syntax_error_is_42601() {
        let engine = SqlEngine::new();
        let err = engine.simple_query("SELCT 1").await.expect_err("syntax");
        assert_eq!(err.code, "42601");
    }
}
