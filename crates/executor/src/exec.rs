//! Per-statement execution.

use bytes::Bytes;
use catalog::{Column, Table, TableId};
use kv::Kv;
use pgparser::ast::{Expr, SelectItem, SelectStmt, Statement};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::{Cell, FieldDescription, QueryResult};

use crate::SqlSession;
use crate::error::ExecError;
use crate::session::{Pending, TxnCtx};

/// Read a table's durable next-rowid (1 if unset). Single source of truth for
/// the sequence read.
pub(crate) fn read_seq_kv(kv: &dyn Kv, table: TableId) -> Result<u64, ExecError> {
    match kv.get(&kv::key::seq_key(table))? {
        Some(b) => {
            let arr: [u8; 8] = b
                .as_slice()
                .try_into()
                .map_err(|_| kv::KvError::CorruptRow("sequence is not u64".into()))?;
            Ok(u64::from_be_bytes(arr))
        }
        None => Ok(1),
    }
}

/// DDL (CREATE/DROP TABLE) is its own immediate critical section: it takes the
/// write_lock itself and writes through to the store. Non-DDL is unreachable
/// here (routed via `run_one`) but handled defensively to keep the match total.
pub(crate) fn execute_ddl(
    session: &SqlSession,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect();
            let _guard = session.write_lock.lock().expect("write lock");
            catalog::create_table(&*session.kv, name, cols)?;
            Ok(QueryResult::Command {
                tag: "CREATE TABLE".into(),
            })
        }
        Statement::DropTable { name } => {
            let _guard = session.write_lock.lock().expect("write lock");
            catalog::drop_table(&*session.kv, name)?;
            Ok(QueryResult::Command {
                tag: "DROP TABLE".into(),
            })
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}

/// DML (INSERT/SELECT, plus UPDATE/DELETE in Task 6). Reads and writes go
/// through the transaction's `ctx`: writes buffer into `ctx.writes`/`ctx.seq`
/// (flushed at COMMIT by the caller) and reads overlay the write-set on top of
/// the visible store snapshot. This function NEVER takes the write_lock — the
/// caller decides locking (autocommit holds it; in-txn DML does not).
pub(crate) fn execute_dml(
    kv: &dyn Kv,
    ctx: &mut TxnCtx,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            if rows.is_empty() {
                return Ok(QueryResult::Command {
                    tag: "INSERT 0 0".into(),
                });
            }
            let t = catalog::get_table(kv, table)?;
            let target_idx: Vec<usize> = match columns {
                Some(cols) => cols
                    .iter()
                    .map(|c| {
                        t.column_index(c)
                            .ok_or_else(|| ExecError::UndefinedColumn(c.clone()))
                    })
                    .collect::<Result<_, _>>()?,
                None => (0..t.columns.len()).collect(),
            };
            // Allocate rowids from the write-set's per-table counter, seeded
            // from the durable next-rowid the first time this txn touches it.
            let start = *ctx.seq.entry(t.id).or_insert(read_seq_kv(kv, t.id)?);
            let mut rowid = start;
            for row_exprs in rows {
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
                for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
                    // VALUES expressions are literal (no FROM/columns in scope).
                    let v = crate::eval::eval(expr, None, &[])?;
                    full[*slot] = coerce(v, t.columns[*slot].ty)?;
                }
                ctx.writes.insert(
                    (t.id, rowid),
                    Pending::Row(mvcc::encode_version(false, &full)),
                );
                rowid += 1;
            }
            let n = rowid - start;
            // Advance the txn-local sequence to the new next-rowid.
            ctx.seq.insert(t.id, rowid);
            Ok(QueryResult::Command {
                tag: format!("INSERT 0 {n}"),
            })
        }
        Statement::Select(s) => exec_select(kv, ctx, s),
        Statement::Update {
            table,
            assignments,
            filter,
        } => {
            let t = catalog::get_table(kv, table)?;
            // Resolve each assignment's target column index up front (42703 on miss).
            let targets: Vec<(usize, &Expr)> = assignments
                .iter()
                .map(|(col, expr)| {
                    t.column_index(col)
                        .map(|idx| (idx, expr))
                        .ok_or_else(|| ExecError::UndefinedColumn(col.clone()))
                })
                .collect::<Result<_, _>>()?;
            let mut n: u64 = 0;
            for (rowid, row) in scan_live_rows(kv, ctx, &t)? {
                if !row_matches(filter.as_ref(), &t, &row)? {
                    continue;
                }
                let mut next = row.clone();
                for (idx, expr) in &targets {
                    let v = crate::eval::eval(expr, Some(&t), &row)?;
                    next[*idx] = coerce(v, t.columns[*idx].ty)?;
                }
                ctx.writes.insert(
                    (t.id, rowid),
                    Pending::Row(mvcc::encode_version(false, &next)),
                );
                n += 1;
            }
            Ok(QueryResult::Command {
                tag: format!("UPDATE {n}"),
            })
        }
        Statement::Delete { table, filter } => {
            let t = catalog::get_table(kv, table)?;
            let mut n: u64 = 0;
            for (rowid, row) in scan_live_rows(kv, ctx, &t)? {
                if !row_matches(filter.as_ref(), &t, &row)? {
                    continue;
                }
                ctx.writes.insert((t.id, rowid), Pending::Tombstone);
                n += 1;
            }
            Ok(QueryResult::Command {
                tag: format!("DELETE {n}"),
            })
        }
        _ => Err(ExecError::Unsupported("not a DML statement".into())),
    }
}

/// Coerce an evaluated value into a target column type (assignment context).
fn coerce(value: pgtypes::Datum, target: pgtypes::ColumnType) -> Result<pgtypes::Datum, ExecError> {
    use pgtypes::{ColumnType, Datum, TypeError};
    Ok(match (value, target) {
        (Datum::Null, _) => Datum::Null,
        (Datum::Bool(b), ColumnType::Bool) => Datum::Bool(b),
        (Datum::Int4(n), ColumnType::Int4) => Datum::Int4(n),
        (Datum::Int4(n), ColumnType::Int8) => Datum::Int8(i64::from(n)),
        (Datum::Int8(n), ColumnType::Int8) => Datum::Int8(n),
        (Datum::Int8(n), ColumnType::Int4) => i32::try_from(n)
            .map(Datum::Int4)
            .map_err(|_| TypeError::Overflow)?,
        (Datum::Text(s), ColumnType::Text) => Datum::Text(s),
        (v, target) => {
            return Err(ExecError::TypeMismatch(format!(
                "column is of type {} but expression is of type {}",
                target.name(),
                v.column_type().map(|t| t.name()).unwrap_or("unknown"),
            )));
        }
    })
}

/// Scan a table's live rows under the txn snapshot, overlaying the write-set
/// (read-your-writes). Returns `(rowid, row)` pairs sorted by rowid so the
/// buffered writes interleave deterministically with the store rows.
pub(crate) fn scan_live_rows(
    kv: &dyn Kv,
    ctx: &TxnCtx,
    table: &catalog::Table,
) -> Result<Vec<(u64, Vec<pgtypes::Datum>)>, ExecError> {
    let snapshot = mvcc::Snapshot(ctx.snapshot);
    let scanned = kv.scan_prefix(&kv::key::table_prefix(table.id))?;
    let mut out: Vec<(u64, Vec<pgtypes::Datum>)> = Vec::new();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut i = 0;
    while i < scanned.len() {
        let prefix = mvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = kv::key::rowid_of(table.id, &prefix)?;
        let mut versions: Vec<(u64, &[u8])> = Vec::new();
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            versions.push((
                mvcc::version::ts_of_key(&scanned[i].0)?,
                scanned[i].1.as_slice(),
            ));
            i += 1;
        }
        seen.insert(rowid);
        match ctx.writes.get(&(table.id, rowid)) {
            Some(Pending::Tombstone) => {}
            Some(Pending::Row(v)) => out.push((rowid, mvcc::decode_version(v)?.1)),
            None => {
                if let Some(row) = mvcc::visible_version(snapshot, versions)? {
                    out.push((rowid, row));
                }
            }
        }
    }
    // Rows created in this txn that have no store version yet. A Tombstone for
    // a not-yet-stored rowid (created then deleted in this txn) surfaces nothing.
    for ((t, rowid), pending) in &ctx.writes {
        if *t != table.id || seen.contains(rowid) {
            continue;
        }
        if let Pending::Row(v) = pending {
            out.push((*rowid, mvcc::decode_version(v)?.1));
        }
    }
    out.sort_by_key(|(rowid, _)| *rowid);
    Ok(out)
}

/// Evaluate an optional WHERE predicate against a row (NULL => false, like SELECT).
fn row_matches(
    filter: Option<&Expr>,
    table: &catalog::Table,
    row: &[pgtypes::Datum],
) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, Some(table), row)? {
            pgtypes::Datum::Bool(b) => Ok(b),
            pgtypes::Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}

fn exec_select(kv: &dyn Kv, ctx: &TxnCtx, s: &SelectStmt) -> Result<QueryResult, ExecError> {
    let table: Option<Table> = match &s.from {
        Some(name) => Some(catalog::get_table(kv, name)?),
        None => None,
    };

    // Source rows: scan the table (dropping the rowid for projection), or a
    // single empty row for FROM-less SELECT.
    let source: Vec<Vec<Datum>> = match &table {
        Some(t) => scan_live_rows(kv, ctx, t)?
            .into_iter()
            .map(|(_, row)| row)
            .collect(),
        None => vec![vec![]],
    };

    // Resolve the projection into (field, expr) pairs.
    let (fields, out_exprs) = resolve_projection(&s.projection, table.as_ref())?;

    // Filter, keeping each surviving source row for ORDER BY evaluation.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for row in &source {
        let keep = match table.as_ref() {
            Some(t) => row_matches(s.filter.as_ref(), t, row)?,
            // FROM-less SELECT: no table context; WHERE is not supported here
            // (the parser doesn't produce it), so treat as always-true.
            None => true,
        };
        if keep {
            kept.push(row.clone());
        }
    }

    // ORDER BY: sort by evaluated order keys (over the source row).
    if !s.order_by.is_empty() {
        // Precompute keys to keep comparisons total and error-free during sort.
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        for row in kept {
            let mut keys = Vec::with_capacity(s.order_by.len());
            for item in &s.order_by {
                keys.push(crate::eval::eval(&item.expr, table.as_ref(), &row)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, s));
        kept = keyed.into_iter().map(|(_, row)| row).collect();
    }

    // LIMIT.
    if let Some(limit) = s.limit {
        let n = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        kept.truncate(n);
    }

    // Project + encode to cells.
    let mut out_rows: Vec<Vec<Option<Cell>>> = Vec::with_capacity(kept.len());
    for row in &kept {
        let mut cells = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            let d = crate::eval::eval(e, table.as_ref(), row)?;
            cells.push(datum_to_cell(&d));
        }
        out_rows.push(cells);
    }

    let tag = format!("SELECT {}", out_rows.len());
    Ok(QueryResult::Rows {
        fields,
        rows: out_rows,
        tag,
    })
}

/// Expand the projection list into output FieldDescriptions and the expressions
/// that produce each column.
fn resolve_projection(
    items: &[SelectItem],
    table: Option<&Table>,
) -> Result<(Vec<FieldDescription>, Vec<Expr>), ExecError> {
    // SELECT * requires a FROM.
    if items == [SelectItem::Wildcard] {
        let t = table.ok_or_else(|| {
            ExecError::Unsupported("SELECT * with no FROM clause is not supported".into())
        })?;
        let fields = t.columns.iter().map(|c| field(&c.name, c.ty)).collect();
        let exprs = t
            .columns
            .iter()
            .map(|c| Expr::Column(c.name.clone()))
            .collect();
        return Ok((fields, exprs));
    }
    let mut fields = Vec::with_capacity(items.len());
    let mut exprs = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard => {
                return Err(ExecError::Unsupported(
                    "* mixed with other items is not supported".into(),
                ));
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                let ty = crate::eval::infer_type(expr, table)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
            }
        }
    }
    Ok((fields, exprs))
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(c) => c.clone(),
        _ => "?column?".to_string(),
    }
}

fn field(name: &str, ty: ColumnType) -> FieldDescription {
    FieldDescription {
        name: name.to_string(),
        table_oid: 0,
        column_id: 0,
        type_oid: ty.oid(),
        type_size: ty.type_size(),
        type_modifier: -1,
        format: 0,
    }
}

fn datum_to_cell(d: &Datum) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    Some(Cell {
        text: Bytes::from(pgtypes::encoding::encode_text(d)),
        binary: Bytes::from(pgtypes::encoding::encode_binary(d)),
    })
}

/// Compare two order-key vectors per the SELECT's ASC/DESC flags, with PG's
/// default null placement (NULLS LAST for ASC, NULLS FIRST for DESC).
fn order_cmp(a: &[Datum], b: &[Datum], s: &SelectStmt) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in s.order_by.iter().enumerate() {
        let (x, y) = (&a[i], &b[i]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            // NULLS LAST for ASC: null is "greater"; NULLS FIRST for DESC.
            (true, false) => {
                if item.asc {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                if item.asc {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, false) => {
                // SLICE INVARIANT: each ORDER BY key position is type-homogeneous
                // (one column = one declared type; one expression = one static
                // type), so ops::compare never errors here. The Equal fallback is
                // defensive — when CAST / heterogeneous keys arrive in a later SP,
                // this must become a real error path or the sort loses total order.
                let base = pgtypes::ops::compare(x, y)
                    .ok()
                    .flatten()
                    .unwrap_or(Ordering::Equal);
                if item.asc { base } else { base.reverse() }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(crate) fn describe(
    session: &SqlSession,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let statements = pgparser::parse(sql)?;
    // Extended-protocol Describe targets a single statement.
    let Some(Statement::Select(s)) = statements.first() else {
        return Ok(Vec::new()); // non-SELECT (or empty) returns no row description
    };
    let table = match &s.from {
        Some(name) => Some(catalog::get_table(&*session.kv, name)?),
        None => None,
    };
    let (fields, _exprs) = resolve_projection(&s.projection, table.as_ref())?;
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use crate::SqlEngine;
    use pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

    fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
        match r {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn fields_of(r: &QueryResult) -> &Vec<FieldDescription> {
        match r {
            QueryResult::Rows { fields, .. } => fields,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn text(cell: &Option<Cell>) -> Option<String> {
        cell.as_ref()
            .map(|c| String::from_utf8(c.text.to_vec()).expect("cell text is valid UTF-8"))
    }

    #[tokio::test]
    async fn select_literal_no_from() {
        let engine = SqlEngine::new();
        let r = &run(&engine, "SELECT 1 + 1 AS two").await[0];
        assert_eq!(fields_of(r)[0].name, "two");
        assert_eq!(fields_of(r)[0].type_oid, pgtypes::oids::INT4);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn select_where_order_limit() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
        let r = &run(
            &engine,
            "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5",
        )
        .await[0];
        let rows = rows_of(r);
        assert_eq!(rows.len(), 2);
        assert_eq!(text(&rows[0][0]), Some("c".into())); // id=3 first (DESC)
        assert_eq!(text(&rows[1][0]), Some("b".into()));
    }

    #[tokio::test]
    async fn select_star_projects_all_columns() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (7,'x')").await;
        let r = &run(&engine, "SELECT * FROM t").await[0];
        assert_eq!(
            fields_of(r)
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );
        assert_eq!(text(&rows_of(r)[0][0]), Some("7".into()));
        assert_eq!(text(&rows_of(r)[0][1]), Some("x".into()));
    }

    #[tokio::test]
    async fn select_command_tag_counts_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(2)").await;
        match &run(&engine, "SELECT id FROM t").await[0] {
            QueryResult::Rows { tag, .. } => assert_eq!(tag, "SELECT 2"),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_boolean_where_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let err = engine
            .connect()
            .simple_query("SELECT id FROM t WHERE id")
            .await
            .expect_err("non-bool");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    async fn null_orders_last_ascending() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (2),(null),(1)").await;
        let r = &run(&engine, "SELECT id FROM t ORDER BY id ASC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("1".into()), Some("2".into()), None]); // NULLS LAST
    }

    #[tokio::test]
    async fn order_by_mixed_width_expression_key() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(3),(2)").await;
        // a + 3000000000 promotes each key to int8; sort must still be 1,2,3.
        let r = &run(&engine, "SELECT a FROM t ORDER BY a + 3000000000 ASC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );
    }

    async fn run(engine: &SqlEngine, sql: &str) -> Vec<QueryResult> {
        // Autocommit per statement: a fresh session per call preserves the same
        // semantics the old direct `engine.simple_query` had.
        engine.connect().simple_query(sql).await.expect("ok")
    }

    #[tokio::test]
    async fn insert_then_count_via_kv() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let r = run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "INSERT 0 2".into()
            }]
        );
        // A third single-row insert with explicit columns.
        let r = run(&engine, "INSERT INTO t (name, id) VALUES ('c', 3)").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "INSERT 0 1".into()
            }]
        );
    }

    #[tokio::test]
    async fn insert_writes_a_versioned_row_visible_to_select() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let r = &run(&engine, "SELECT id FROM t").await[0];
        assert_eq!(rows_of(r).len(), 1);
    }

    #[tokio::test]
    async fn insert_widens_int4_to_int8_column() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (big int8)").await;
        run(&engine, "INSERT INTO t VALUES (5)").await;
        // Round-trips through SELECT in Task 17; here just assert no error.
    }

    #[tokio::test]
    async fn insert_type_mismatch_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (flag bool)").await;
        let err = engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect_err("mismatch");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn insert_into_missing_table_is_42P01() {
        let engine = SqlEngine::new();
        let err = engine
            .connect()
            .simple_query("INSERT INTO nope VALUES (1)")
            .await
            .expect_err("no table");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn insert_wrong_arity_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        let err = engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect_err("arity");
        assert_eq!(err.code, "42804");
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
            .connect()
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
        let err = engine
            .connect()
            .simple_query("DROP TABLE t")
            .await
            .expect_err("gone");
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
        let err = engine
            .connect()
            .simple_query("SELCT 1")
            .await
            .expect_err("syntax");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn describe_select_returns_field_types_without_executing() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let fields = engine
            .connect()
            .describe("SELECT id, name FROM t")
            .await
            .expect("describe");
        assert_eq!(
            fields.iter().map(|f| f.type_oid).collect::<Vec<_>>(),
            vec![pgtypes::oids::INT4, pgtypes::oids::TEXT]
        );
    }

    #[tokio::test]
    async fn describe_non_select_has_no_fields() {
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .describe("CREATE TABLE t (id int4)")
            .await
            .expect("describe");
        assert!(fields.is_empty());
    }

    #[tokio::test]
    async fn two_inserts_are_both_visible() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        run(&engine, "INSERT INTO t VALUES (2)").await;
        let r = &run(&engine, "SELECT id FROM t ORDER BY id").await[0];
        assert_eq!(rows_of(r).len(), 2);
    }

    #[tokio::test]
    async fn select_on_empty_table_sees_no_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        let r = &run(&engine, "SELECT id FROM t").await[0];
        assert_eq!(rows_of(r).len(), 0);
    }
}
