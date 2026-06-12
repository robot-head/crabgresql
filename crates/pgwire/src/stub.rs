//! Canned-response engine: enough surface for psql, driver integration
//! tests, and the conformance harness to exercise the wire protocol.

use bytes::Bytes;

use crate::engine::{Cell, Engine, FieldDescription, QueryResult, Session, TxStatus, oids};
use crate::error::{PgError, sqlstate};

pub const STUB_VERSION: &str =
    "PostgreSQL 18.0 (crabgresql 0.1.0) on aarch64, compiled by rustc, 64-bit";

#[derive(Debug, Default, Clone)]
pub struct StubEngine {}

impl StubEngine {
    pub fn new() -> Self {
        Self {}
    }
}

impl Engine for StubEngine {
    type Session = StubSession;

    fn connect(&self) -> StubSession {
        StubSession
    }
}

/// Per-connection session for the canned stub engine. Holds no state; the
/// transaction status is always `Idle`.
pub struct StubSession;

impl StubSession {
    fn canned(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        match normalize(sql).as_str() {
            "" => Ok(vec![QueryResult::Empty]),
            "select 1" => {
                let rows = vec![vec![Some(int4_cell(1))]];
                let tag = select_tag(&rows);
                Ok(vec![QueryResult::Rows {
                    fields: vec![int4_field("?column?")],
                    rows,
                    tag,
                }])
            }
            "select version()" => {
                let rows = vec![vec![Some(text_cell(STUB_VERSION))]];
                let tag = select_tag(&rows);
                Ok(vec![QueryResult::Rows {
                    fields: vec![text_field("version")],
                    rows,
                    tag,
                }])
            }
            other => Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("stub engine does not implement: {other}"),
            )),
        }
    }
}

impl Session for StubSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // `pg_sleep` exists so cancellation has something to cancel.
        if let Some(secs) = normalize(sql)
            .strip_prefix("select pg_sleep(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|n| n.parse::<u64>().ok())
        {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            let rows = vec![vec![Some(text_cell(""))]];
            let tag = select_tag(&rows);
            return Ok(vec![QueryResult::Rows {
                fields: vec![text_field("pg_sleep")],
                rows,
                tag,
            }]);
        }
        self.canned(sql)
    }

    // Returns 0A000 for any unrecognized SQL — acceptable for the stub; a real engine reports proper codes (e.g. 26000) per statement state.
    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        match self.canned(sql)?.first() {
            Some(QueryResult::Rows { fields, .. }) => Ok(fields.clone()),
            _ => Ok(Vec::new()),
        }
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}

fn normalize(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_ascii_lowercase()
}

fn select_tag(rows: &[Vec<Option<Cell>>]) -> String {
    format!("SELECT {}", rows.len())
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
