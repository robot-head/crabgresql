//! Canned-response engine: enough surface for psql, driver integration
//! tests, and the conformance harness to exercise the wire protocol.

use bytes::Bytes;

use crate::engine::{Cell, Engine, FieldDescription, QueryResult, oids};
use crate::error::{PgError, sqlstate};

pub const STUB_VERSION: &str =
    "PostgreSQL 18.0 (crabgresql 0.1.0) on aarch64, compiled by rustc, 64-bit";

#[derive(Debug, Default, Clone)]
pub struct StubEngine {}

impl StubEngine {
    pub fn new() -> Self {
        Self {}
    }

    fn canned(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" => Ok(vec![QueryResult::Empty]),
            "select 1" => Ok(vec![QueryResult::Rows {
                fields: vec![int4_field("?column?")],
                rows: vec![vec![Some(int4_cell(1))]],
                tag: "SELECT 1".into(),
            }]),
            "select version()" => Ok(vec![QueryResult::Rows {
                fields: vec![text_field("version")],
                rows: vec![vec![Some(text_cell(STUB_VERSION))]],
                tag: "SELECT 1".into(),
            }]),
            other => Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("stub engine does not implement: {other}"),
            )),
        }
    }
}

impl Engine for StubEngine {
    async fn simple_query(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // `pg_sleep` exists so cancellation has something to cancel.
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        if let Some(secs) = normalized
            .strip_prefix("select pg_sleep(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|n| n.parse::<u64>().ok())
        {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            return Ok(vec![QueryResult::Rows {
                fields: vec![text_field("pg_sleep")],
                rows: vec![vec![Some(text_cell(""))]],
                tag: "SELECT 1".into(),
            }]);
        }
        self.canned(sql)
    }

    async fn describe(&self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        match self.canned(sql)?.first() {
            Some(QueryResult::Rows { fields, .. }) => Ok(fields.clone()),
            _ => Ok(Vec::new()),
        }
    }
}

fn int4_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::INT4,
        type_size: 4,
        type_modifier: -1,
        format: 0,
    }
}

fn text_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::TEXT,
        type_size: -1,
        type_modifier: -1,
        format: 0,
    }
}

fn int4_cell(v: i32) -> Cell {
    Cell {
        text: Bytes::from(v.to_string()),
        binary: Bytes::copy_from_slice(&v.to_be_bytes()),
    }
}

fn text_cell(v: &str) -> Cell {
    let b = Bytes::copy_from_slice(v.as_bytes());
    Cell {
        text: b.clone(),
        binary: b,
    }
}
