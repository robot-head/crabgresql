//! Differential conformance harness: run the same SQL against real PostgreSQL
//! (the oracle) and crabgresql (the subject), diff the outcomes.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryOutcome {
    /// Row values in text format; None = NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// SQLSTATE if the statement errored.
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffResult {
    pub matched: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct CaseResult {
    pub file: String,
    pub sql: String,
    pub matched: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub total: usize,
    pub matched: usize,
    pub parity_percent: f64,
    pub cases: Vec<CaseResult>,
}

impl Report {
    pub fn new(cases: Vec<CaseResult>) -> Self {
        let total = cases.len();
        let matched = cases.iter().filter(|c| c.matched).count();
        let parity_percent = if total == 0 {
            0.0
        } else {
            matched as f64 * 100.0 / total as f64
        };
        Self {
            total,
            matched,
            parity_percent,
            cases,
        }
    }

    pub fn markdown_summary(&self) -> String {
        let mut md = format!(
            "# crabgresql conformance report\n\n**Parity: {:.1}%** ({} / {} statements match the oracle)\n\n",
            self.parity_percent, self.matched, self.total
        );
        md.push_str("| file | statement | result |\n|---|---|---|\n");
        for c in &self.cases {
            let sql = c.sql.replace('|', "\\|");
            let result = if c.matched {
                "match".to_string()
            } else {
                format!("MISMATCH: {}", c.detail)
            };
            md.push_str(&format!("| {} | `{}` | {} |\n", c.file, sql, result));
        }
        md
    }
}

pub fn diff(oracle: &QueryOutcome, subject: &QueryOutcome) -> DiffResult {
    if oracle.error_code != subject.error_code {
        return DiffResult {
            matched: false,
            detail: format!(
                "error code: oracle={:?} subject={:?}",
                oracle.error_code, subject.error_code
            ),
        };
    }
    if oracle.rows != subject.rows {
        return DiffResult {
            matched: false,
            detail: format!("rows: oracle={:?} subject={:?}", oracle.rows, subject.rows),
        };
    }
    DiffResult {
        matched: true,
        detail: String::new(),
    }
}

/// Executes one statement via the simple query protocol, normalizing the
/// outcome. Errors with no SQLSTATE (I/O, disconnect) map to "XXIO" so they
/// are visible as harness-level failures rather than silently matching.
pub async fn run_one(client: &tokio_postgres::Client, sql: &str) -> QueryOutcome {
    use tokio_postgres::SimpleQueryMessage;
    match client.simple_query(sql).await {
        Ok(messages) => {
            let mut rows = Vec::new();
            for m in messages {
                if let SimpleQueryMessage::Row(row) = m {
                    let mut values = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        values.push(row.get(i).map(|s| s.to_string()));
                    }
                    rows.push(values);
                }
            }
            QueryOutcome {
                rows,
                error_code: None,
            }
        }
        Err(e) => QueryOutcome {
            rows: vec![],
            error_code: Some(
                e.as_db_error()
                    .map(|db| db.code().code().to_string())
                    .unwrap_or_else(|| "XXIO".to_string()),
            ),
        },
    }
}

/// Minimal statement splitter: semicolons outside single/double quotes and
/// line comments. Dollar-quoting is NOT handled yet — tracked for the
/// pg_regress import in SP2, which needs it.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(c) = chars.next() {
        if !in_single && !in_double && c == '-' && chars.peek() == Some(&'-') {
            for c2 in chars.by_ref() {
                if c2 == '\n' {
                    break;
                }
            }
            continue;
        }
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ';' if !in_single && !in_double => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }
    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_statements_on_semicolons_respecting_quotes_and_comments() {
        let sql = "SELECT 1;\n-- a comment; with a semicolon\nSELECT 'a;b';\nSELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 1", "SELECT 'a;b'", "SELECT 2"]
        );
    }

    #[test]
    fn identical_outcomes_match() {
        let a = QueryOutcome {
            rows: vec![vec![Some("1".into())]],
            error_code: None,
        };
        assert!(diff(&a, &a.clone()).matched);
    }

    #[test]
    fn differing_rows_mismatch_with_detail() {
        let oracle = QueryOutcome {
            rows: vec![vec![Some("1".into())]],
            error_code: None,
        };
        let subject = QueryOutcome {
            rows: vec![vec![Some("2".into())]],
            error_code: None,
        };
        let d = diff(&oracle, &subject);
        assert!(!d.matched);
        assert!(d.detail.contains("rows"));
    }

    #[test]
    fn matching_error_codes_match() {
        // Same SQLSTATE on both sides counts as parity (e.g. both reject).
        let a = QueryOutcome {
            rows: vec![],
            error_code: Some("42601".into()),
        };
        assert!(diff(&a, &a.clone()).matched);
        let b = QueryOutcome {
            rows: vec![],
            error_code: Some("0A000".into()),
        };
        assert!(!diff(&a, &b).matched);
    }
}
