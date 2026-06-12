//! Per-statement execution.

use bytes::Bytes;
use catalog::{Column, Table, TableId};
use kv::Kv;
use pgparser::ast::{Expr, SelectItem, SelectStmt, Statement};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::{Cell, FieldDescription, QueryResult};

use crate::error::ExecError;

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

/// DDL (CREATE/DROP TABLE) writes through to the store. The session holds the
/// catalog lock around this call (serializing DDL among DDLs). Non-DDL is
/// unreachable here (routed via `run_one`) but handled defensively to keep the
/// match total.
pub(crate) fn execute_ddl(kv: &dyn Kv, stmt: &Statement) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect();
            catalog::create_table(kv, name, cols)?;
            Ok(QueryResult::Command {
                tag: "CREATE TABLE".into(),
            })
        }
        Statement::DropTable { name } => {
            catalog::drop_table(kv, name)?;
            Ok(QueryResult::Command {
                tag: "DROP TABLE".into(),
            })
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}

/// Resolve INSERT target column indices: explicit `(cols...)` mapped to their
/// catalog positions (42703 on miss), or all columns in declared order.
fn resolve_targets(t: &Table, columns: &Option<Vec<String>>) -> Result<Vec<usize>, ExecError> {
    match columns {
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.column_index(c)
                    .ok_or_else(|| ExecError::UndefinedColumn(c.clone()))
            })
            .collect::<Result<_, _>>(),
        None => Ok((0..t.columns.len()).collect()),
    }
}

/// The write path (INSERT/UPDATE/DELETE) with concurrent writers (SP6). Builds
/// the version write ops tagged with the transaction's `xid` and returns them
/// WITHOUT writing — the session assembles the final batch (clog for autocommit)
/// and writes once. INSERT allocates rowids via the `SequenceManager` (which
/// persists the sequence durably itself). UPDATE/DELETE lock each candidate row
/// exclusively via the `RowLockManager` (blocking until granted, or 40P01 on a
/// deadlock), then re-check the row's current state under EvalPlanQual: a
/// concurrent committed change is a 40001 under REPEATABLE READ, or a re-find
/// under READ COMMITTED. Reads resolve via `satisfies_mvcc` with the txn's own
/// xid (read-your-writes).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_write(
    kv: &dyn Kv,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    seq: &crate::seq::SequenceManager,
    snapshot: &mvcc::visibility::Snapshot,
    xid: u64,
    repeatable_read: bool,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<kv::WriteOp>), ExecError> {
    let mut ops: Vec<kv::WriteOp> = Vec::new();
    match stmt {
        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            if rows.is_empty() {
                return Ok((
                    QueryResult::Command {
                        tag: "INSERT 0 0".into(),
                    },
                    ops,
                ));
            }
            let t = catalog::get_table(kv, table)?;
            let target_idx = resolve_targets(&t, columns)?;
            // Reserve a contiguous block of rowids atomically (the SequenceManager
            // persists the new next-rowid durably itself — no seq Put in ops).
            let n_rows = rows.len() as u64;
            let start = seq.alloc(kv, t.id, n_rows)?;
            for (rowid, row_exprs) in (start..).zip(rows.iter()) {
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
                ops.push(kv::WriteOp::Put {
                    key: mvcc::version::version_key_xid(t.id, rowid, xid),
                    value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &full),
                });
            }
            Ok((
                QueryResult::Command {
                    tag: format!("INSERT 0 {n_rows}"),
                },
                ops,
            ))
        }
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
            for (rowid, _xmin, scanned_row) in scan_live(kv, snapshot, Some(xid), &t)? {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause (avoids over-locking and
                //    restores row-level write concurrency for different rows).
                if !row_matches(filter.as_ref(), Some(&t), &scanned_row)? {
                    continue;
                }
                // 2. Lock only matching candidates.
                match lockmgr
                    .acquire(t.id, rowid, crate::lockmgr::LockMode::Exclusive, xid)
                    .await
                {
                    Ok(()) => {}
                    Err(()) => return Err(ExecError::Deadlock),
                }
                // 3. EvalPlanQual: re-read this row under the lock and decide what to
                //    operate on (40001 under RR if changed since our snapshot).
                let Some((cur_xmin, cur_row)) =
                    eval_plan_qual(kv, procarray, snapshot, &t, rowid, xid, repeatable_read)?
                else {
                    continue; // deleted by a concurrent committed txn — skip
                };
                // 4. Re-check the filter on the (possibly re-found) current row —
                //    under READ COMMITTED the row may have changed since the scan.
                if !row_matches(filter.as_ref(), Some(&t), &cur_row)? {
                    continue; // no longer matches the WHERE clause
                }
                let mut next = cur_row.clone();
                for (idx, expr) in &targets {
                    let v = crate::eval::eval(expr, Some(&t), &cur_row)?;
                    next[*idx] = coerce(v, t.columns[*idx].ty)?;
                }
                if cur_xmin == xid {
                    // Updating my own uncommitted version: overwrite in place
                    // (last-write-wins within the txn; no new tuple, xmax stays
                    // invalid). PostgreSQL uses cmin/cmax here; we have no command
                    // ids, so in-place replacement is the faithful observable result.
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xid),
                        value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &next),
                    });
                } else {
                    // Supersede a committed version: stamp its xmax, write a new tuple.
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, cur_xmin),
                        value: mvcc::version::encode_tuple(cur_xmin, xid, &cur_row),
                    });
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xid),
                        value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &next),
                    });
                }
                n += 1;
            }
            Ok((
                QueryResult::Command {
                    tag: format!("UPDATE {n}"),
                },
                ops,
            ))
        }
        Statement::Delete { table, filter } => {
            let t = catalog::get_table(kv, table)?;
            let mut n: u64 = 0;
            for (rowid, _xmin, scanned_row) in scan_live(kv, snapshot, Some(xid), &t)? {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause.
                if !row_matches(filter.as_ref(), Some(&t), &scanned_row)? {
                    continue;
                }
                // 2. Lock only matching candidates.
                match lockmgr
                    .acquire(t.id, rowid, crate::lockmgr::LockMode::Exclusive, xid)
                    .await
                {
                    Ok(()) => {}
                    Err(()) => return Err(ExecError::Deadlock),
                }
                // 3. EvalPlanQual: re-read this row under the lock.
                let Some((cur_xmin, cur_row)) =
                    eval_plan_qual(kv, procarray, snapshot, &t, rowid, xid, repeatable_read)?
                else {
                    continue; // already deleted by a concurrent committed txn
                };
                // 4. Re-check filter on the (possibly re-found) current row.
                if !row_matches(filter.as_ref(), Some(&t), &cur_row)? {
                    continue; // no longer matches the WHERE clause
                }
                if cur_xmin == xid {
                    // Deleting my own uncommitted version: PostgreSQL stamps
                    // xmax=xid so it is invisible to me. version_key is the same
                    // key; overwrite it with xmax set.
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xid),
                        value: mvcc::version::encode_tuple(xid, xid, &cur_row),
                    });
                } else {
                    // Set xmax = my xid on the matched version (keep its row bytes).
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, cur_xmin),
                        value: mvcc::version::encode_tuple(cur_xmin, xid, &cur_row),
                    });
                }
                n += 1;
            }
            Ok((
                QueryResult::Command {
                    tag: format!("DELETE {n}"),
                },
                ops,
            ))
        }
        _ => Err(ExecError::Unsupported("not a write statement".into())),
    }
}

/// Was `xid` settled (committed or aborted) before `snapshot` was taken? True
/// iff `xid` was neither still running at, nor started after, the snapshot —
/// mirroring the negation of `Snapshot::is_running`.
fn snapshot_can_see(snapshot: &mvcc::visibility::Snapshot, xid: u64) -> bool {
    xid < snapshot.xmax && !snapshot.xip.contains(&xid)
}

/// Find the single version of `rowid` visible to `snap` (with own-xid
/// read-your-writes) among already-decoded `versions`. Mirrors `scan_live`'s
/// per-version `satisfies_mvcc` check, but over one rowid's versions.
fn find_visible_one(
    kv: &dyn Kv,
    snap: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    versions: &[(u64, u64, Vec<pgtypes::Datum>)],
) -> Result<Option<(u64, Vec<pgtypes::Datum>)>, ExecError> {
    let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
    for (xmin, xmax, row) in versions {
        if mvcc::visibility::satisfies_mvcc(*xmin, *xmax, snap, own, |x| mvcc::clog::get(kv, x))? {
            visible = Some((*xmin, row.clone())); // MVCC invariant: at most one
        }
    }
    Ok(visible)
}

/// After locking the row, re-read its current versions. Returns the version to
/// operate on, or None to skip. Under REPEATABLE READ, a row changed by a txn
/// that committed after our snapshot is a serialization failure (40001). Under
/// READ COMMITTED, re-find the latest live version (a fresh snapshot).
fn eval_plan_qual(
    kv: &dyn Kv,
    procarray: &crate::procarray::ProcArray,
    snapshot: &mvcc::visibility::Snapshot, // the txn snapshot (RR) used to detect "changed since"
    table: &catalog::Table,
    rowid: u64,
    xid: u64,
    repeatable_read: bool,
) -> Result<Option<(u64, Vec<pgtypes::Datum>)>, ExecError> {
    // Re-scan just this rowid's versions from disk.
    let prefix = kv::key::row_key(table.id, rowid);
    let scanned = kv.scan_prefix(&prefix)?;
    let mut versions: Vec<(u64, u64, Vec<pgtypes::Datum>)> = Vec::with_capacity(scanned.len());
    for (_k, v) in &scanned {
        let (xmn, xmx, row) = mvcc::version::decode_tuple(v)?;
        versions.push((xmn, xmx, row));
    }
    // Is the row's latest committed version deleted/superseded by a transaction
    // NOT visible to our txn snapshot (committed AFTER it), other than ourselves?
    let changed_since_snapshot = versions.iter().any(|&(_xmn, xmx, _)| {
        xmx != mvcc::xid::INVALID_XID
            && xmx != xid
            && matches!(
                mvcc::clog::get(kv, xmx),
                Ok(mvcc::clog::XidStatus::Committed)
            )
            && !snapshot_can_see(snapshot, xmx)
    });
    if changed_since_snapshot {
        if repeatable_read {
            return Err(ExecError::SerializationFailure);
        }
        // READ COMMITTED: re-find the latest live version under a FRESH snapshot.
        let fresh = procarray.snapshot();
        return find_visible_one(kv, &fresh, Some(xid), &versions);
    }
    // No concurrent committed change: find the version visible to our snapshot.
    find_visible_one(kv, snapshot, Some(xid), &versions)
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

/// Scan a table's visible rows under `snapshot` (and the caller's own xid for
/// read-your-writes). Returns `(rowid, xmin, row)` for the one visible version
/// of each live row, sorted by rowid.
pub(crate) fn scan_live(
    kv: &dyn Kv,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &catalog::Table,
) -> Result<Vec<(u64, u64, Vec<pgtypes::Datum>)>, ExecError> {
    let scanned = kv.scan_prefix(&kv::key::table_prefix(table.id))?;
    let mut out: Vec<(u64, u64, Vec<pgtypes::Datum>)> = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        let prefix = mvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = kv::key::rowid_of(table.id, &prefix)?;
        let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, row) = mvcc::version::decode_tuple(&scanned[i].1)?;
            if mvcc::visibility::satisfies_mvcc(xmin, xmax, snapshot, own, |x| {
                mvcc::clog::get(kv, x)
            })? {
                visible = Some((xmin, row)); // the MVCC invariant: at most one
            }
            i += 1;
        }
        if let Some((xmin, row)) = visible {
            out.push((rowid, xmin, row));
        }
    }
    out.sort_by_key(|(rowid, _, _)| *rowid);
    Ok(out)
}

/// Evaluate an optional WHERE predicate against a row (NULL => false, like SELECT).
fn row_matches(
    filter: Option<&Expr>,
    table: Option<&catalog::Table>,
    row: &[pgtypes::Datum],
) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, table, row)? {
            pgtypes::Datum::Bool(b) => Ok(b),
            pgtypes::Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}

pub(crate) fn execute_read(
    kv: &dyn Kv,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let Statement::Select(s) = stmt else {
        return Err(ExecError::Unsupported("not a SELECT".into()));
    };
    let table: Option<Table> = match &s.from {
        Some(name) => Some(catalog::get_table(kv, name)?),
        None => None,
    };

    // Source rows: scan the table (dropping the rowid/xmin for projection), or a
    // single empty row for FROM-less SELECT.
    let source: Vec<Vec<Datum>> = match &table {
        Some(t) => scan_live(kv, snapshot, own, t)?
            .into_iter()
            .map(|(_, _, row)| row)
            .collect(),
        None => vec![vec![]],
    };

    // Filter.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for row in &source {
        if row_matches(s.filter.as_ref(), table.as_ref(), row)? {
            kept.push(row.clone());
        }
    }

    project_order_limit(s, table.as_ref(), kept)
}

/// Locking SELECT (FOR UPDATE / FOR SHARE). Takes a row lock on each visible
/// row before rechecking it via EvalPlanQual (same semantics as UPDATE/DELETE).
/// The snapshot and xid must already be established by the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_read_locking(
    kv: &dyn Kv,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    snapshot: &mvcc::visibility::Snapshot,
    xid: u64,
    repeatable_read: bool,
    mode: crate::lockmgr::LockMode,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    // FOR UPDATE/SHARE requires a FROM clause — there are no rows to lock
    // in a FROM-less SELECT.
    let table_name = s
        .from
        .as_ref()
        .ok_or_else(|| ExecError::Unsupported("FOR UPDATE/SHARE requires a FROM clause".into()))?;
    let t = catalog::get_table(kv, table_name)?;

    // Scan visible rows, then lock and EvalPlanQual-recheck each one.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for (rowid, _xmin, scanned_row) in scan_live(kv, snapshot, Some(xid), &t)? {
        // 1. Filter on the snapshot-visible row FIRST — only lock rows that
        //    match the WHERE clause (a FOR UPDATE/SHARE with no WHERE still
        //    locks all rows because row_matches(None, ..) returns true).
        if !row_matches(s.filter.as_ref(), Some(&t), &scanned_row)? {
            continue;
        }

        // 2. Lock only matching candidates (40P01 on deadlock).
        lockmgr
            .acquire(t.id, rowid, mode, xid)
            .await
            .map_err(|()| ExecError::Deadlock)?;

        // 3. EvalPlanQual: re-read the row under the lock (40001 under RR if
        //    changed since our snapshot; RC re-finds the latest live version).
        let Some((_cur_xmin, cur_row)) =
            eval_plan_qual(kv, procarray, snapshot, &t, rowid, xid, repeatable_read)?
        else {
            continue; // deleted by a concurrent committed txn — skip
        };

        // 4. Re-apply the WHERE filter against the (possibly newer) row.
        if !row_matches(s.filter.as_ref(), Some(&t), &cur_row)? {
            continue; // no longer matches
        }
        kept.push(cur_row);
    }

    project_order_limit(s, Some(&t), kept)
}

/// Apply ORDER BY, LIMIT, and projection to a set of already-filtered source
/// rows, producing the final `QueryResult::Rows`. Used by both `execute_read`
/// and `execute_read_locking` to avoid duplication.
fn project_order_limit(
    s: &SelectStmt,
    table: Option<&Table>,
    mut kept: Vec<Vec<Datum>>,
) -> Result<QueryResult, ExecError> {
    // Resolve the projection into (field, expr) pairs.
    let (fields, out_exprs) = resolve_projection(&s.projection, table)?;

    // ORDER BY: sort by evaluated order keys (over the source row).
    if !s.order_by.is_empty() {
        // Precompute keys to keep comparisons total and error-free during sort.
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        for row in kept {
            let mut keys = Vec::with_capacity(s.order_by.len());
            for item in &s.order_by {
                keys.push(crate::eval::eval(&item.expr, table, &row)?);
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
            let d = crate::eval::eval(e, table, row)?;
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
    kv: &dyn Kv,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let statements = pgparser::parse(sql)?;
    // Extended-protocol Describe targets a single statement.
    let Some(Statement::Select(s)) = statements.first() else {
        return Ok(Vec::new()); // non-SELECT (or empty) returns no row description
    };
    let table = match &s.from {
        Some(name) => Some(catalog::get_table(kv, name)?),
        None => None,
    };
    let (fields, _exprs) = resolve_projection(&s.projection, table.as_ref())?;
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use crate::{SqlEngine, SqlSession};
    use pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

    async fn run_s(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
        s.simple_query(sql).await.expect("ok")
    }

    #[tokio::test]
    async fn read_your_writes_via_own_xid_in_txn() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        // Own uncommitted insert is visible to this txn (no write-set; via xid).
        assert_eq!(
            rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
            1
        );
        s.simple_query("ROLLBACK").await.expect("rollback");
        assert_eq!(
            rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
            0
        );
    }

    #[tokio::test]
    async fn another_session_cannot_see_uncommitted_rows() {
        let engine = SqlEngine::new();
        let mut writer = engine.connect();
        writer
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        writer.simple_query("BEGIN").await.expect("begin");
        writer
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        // A concurrent session must not see the in-progress row.
        let mut reader = engine.connect();
        assert_eq!(
            rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
            0
        );
        writer.simple_query("COMMIT").await.expect("commit");
        // After commit a fresh snapshot sees it.
        assert_eq!(
            rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
            1
        );
    }

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

    fn tag_of(r: &QueryResult) -> String {
        match r {
            QueryResult::Command { tag } => tag.clone(),
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn select_for_update_returns_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(2),(3)").await;
        let r = &run(
            &engine,
            "SELECT id FROM t WHERE id > 1 ORDER BY id FOR UPDATE",
        )
        .await[0];
        assert_eq!(rows_of(r).len(), 2);
    }

    #[tokio::test]
    async fn for_update_in_txn_then_commit_releases() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let mut s = engine.connect();
        run_s(&mut s, "BEGIN").await;
        run_s(&mut s, "SELECT id FROM t FOR UPDATE").await; // takes a lock
        run_s(&mut s, "COMMIT").await; // must release; no hang
        // a fresh autocommit update of the same row must not block
        let r = run(&engine, "UPDATE t SET id = 9 WHERE id = 1").await;
        assert_eq!(tag_of(&r[0]), "UPDATE 1");
    }
}
