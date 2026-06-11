//! Differential parser oracle: our parser must agree with libpg_query on
//! accept/reject for slice-grammar statements and clear syntax errors.
//! Gated behind --features oracle (libpg_query is a C build-time dep).
#![cfg(feature = "oracle")]

/// Statements inside the SP2 slice — BOTH parsers must accept.
const ACCEPTED: &[&str] = &[
    "CREATE TABLE t (id int4, name text)",
    "CREATE TABLE t (a integer, b bigint, c boolean, d text)",
    "DROP TABLE t",
    "INSERT INTO t VALUES (1, 'a')",
    "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
    "SELECT 1",
    "SELECT 1 + 2 * 3",
    "SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10",
    "SELECT * FROM t",
    "SELECT NOT a OR b AND c FROM t",
    "SELECT 'it''s' FROM t",
];

/// Clear syntax errors — BOTH parsers must reject.
const REJECTED: &[&str] = &[
    "SELECT FROM",
    "CREATE TABLE",
    "INSERT INTO t VALUES",
    "SELECT 1 +",
    "SELECT * FROM",
    "SELECT 1 ORDER BY",
    "(",
    "SELECT 'unterminated",
];

fn pg_accepts(sql: &str) -> bool {
    pg_query::parse(sql).is_ok()
}

fn we_accept(sql: &str) -> bool {
    pgparser::parse(sql).is_ok()
}

#[test]
fn agreement_on_accepted() {
    for &sql in ACCEPTED {
        assert!(pg_accepts(sql), "libpg_query should accept: {sql}");
        assert!(we_accept(sql), "pgparser should accept (PG does): {sql}");
    }
}

#[test]
fn agreement_on_rejected() {
    for &sql in REJECTED {
        assert!(!pg_accepts(sql), "libpg_query should reject: {sql}");
        assert!(!we_accept(sql), "pgparser should reject (PG does): {sql}");
    }
}
