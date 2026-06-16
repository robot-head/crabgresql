//! Per-statement execution.

use bytes::Bytes;
use catalog::{Column, Table, TableId};
use kv::Kv;
use pgparser::ast::{Expr, SelectItem, SelectStmt, Statement};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::{Cell, FieldDescription, QueryResult};
use zerocopy::FromBytes;
use zerocopy::byteorder::big_endian::U64;

use crate::error::ExecError;

/// Read a table's durable next-rowid (1 if unset). Single source of truth for
/// the sequence read.
pub(crate) fn read_seq_kv(kv: &dyn Kv, table: TableId) -> Result<u64, ExecError> {
    match kv.get(&kv::key::seq_key(table))? {
        Some(b) => {
            let (v, _) = U64::read_from_prefix(b.as_slice())
                .map_err(|_| kv::KvError::CorruptRow("sequence is not u64".into()))?;
            Ok(v.get())
        }
        None => Ok(1),
    }
}

/// DDL (CREATE/DROP TABLE) reads the catalog and builds its write batch WITHOUT
/// persisting it — the session routes the returned ops through the durable-write
/// seam (so DDL replicates too). The session holds the catalog lock across the
/// read+commit (serializing DDL globally). Non-DDL is unreachable here (routed
/// via `run_one`) but handled defensively to keep the match total. Validation
/// (42P07 on duplicate, 42P01 on a missing drop) is unchanged — only the write
/// destination moved.
pub(crate) fn execute_ddl(
    kv: &dyn Kv,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<kv::WriteOp>), ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect();
            let (_id, ops) = catalog::create_table_ops(kv, name, cols)?;
            Ok((
                QueryResult::Command {
                    tag: "CREATE TABLE".into(),
                },
                ops,
            ))
        }
        Statement::DropTable { name } => {
            let ops = catalog::drop_table_ops(kv, name)?;
            Ok((
                QueryResult::Command {
                    tag: "DROP TABLE".into(),
                },
                ops,
            ))
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
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
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
            let t = catalog::get_table(catalog_kv, table)?;
            let target_idx = resolve_targets(&t, columns)?;
            // Reserve a contiguous block of rowids atomically. In Durable mode the
            // SequenceManager persists the new next-rowid itself (seq_op is None).
            // In Replicated mode it returns the seq Put for us to fold into this
            // same commit batch (max-merged by the replicated state machine).
            let n_rows = rows.len() as u64;
            let (start, seq_op) = seq.alloc(kv, t.id, n_rows)?;
            if let Some(op) = seq_op {
                ops.push(op);
            }
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
            let t = catalog::get_table(catalog_kv, table)?;
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
            for (rowid, _xmin, scanned_row) in
                scan_live(kv, global, gsnap, snapshot, Some(xid), &t)?
            {
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
                let Some((cur_xmin, cur_row)) = eval_plan_qual(
                    kv,
                    global,
                    procarray,
                    snapshot,
                    &t,
                    rowid,
                    xid,
                    repeatable_read,
                )?
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
            let t = catalog::get_table(catalog_kv, table)?;
            let mut n: u64 = 0;
            for (rowid, _xmin, scanned_row) in
                scan_live(kv, global, gsnap, snapshot, Some(xid), &t)?
            {
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
                let Some((cur_xmin, cur_row)) = eval_plan_qual(
                    kv,
                    global,
                    procarray,
                    snapshot,
                    &t,
                    rowid,
                    xid,
                    repeatable_read,
                )?
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

/// The global-aware clog resolver handed to `satisfies_mvcc`. Given this range's
/// local xid `Li`, reads this range's clog (`local`); a terminal status is
/// returned unchanged (today's single-range behavior). A `Prepared(Li -> g)`
/// marker is deref'd to range 0's global clog (`global`): if `g` is still
/// in-doubt as of the reader's global snapshot (`gsnap`) it reports `InProgress`
/// (the cross-range row is invisible until the global commit decision); once `g`
/// is settled relative to `gsnap`, range 0's global-clog status for `g` is the
/// answer — so both ranges' Prepared rows flip visible together at the single
/// `Committed(g)` instant.
///
/// For a single-range (non-GTM) engine the caller passes `global = local` and
/// `gsnap = NO_GLOBAL_SNAPSHOT()`; no `Prepared` tuple ever exists there, so the
/// `Prepared` arm is unreachable and behavior is byte-for-byte unchanged.
pub(crate) fn global_status<'a>(
    local: &'a dyn kv::Kv,
    global: &'a dyn kv::Kv,
    gsnap: &'a mvcc::visibility::Snapshot,
) -> impl Fn(u64) -> Result<mvcc::clog::XidStatus, kv::KvError> + 'a {
    use mvcc::clog::XidStatus;
    move |xid| match mvcc::clog::get(local, xid)? {
        XidStatus::Prepared(g) => {
            if g >= gsnap.xmax || gsnap.xip.binary_search(&g).is_ok() {
                Ok(XidStatus::InProgress) // global txn in-doubt as of my global snapshot
            } else {
                Ok(mvcc::clog::get(global, g)?) // settled: range 0's global decision
            }
        }
        other => Ok(other),
    }
}

/// Find the single version of `rowid` visible to `snap` (with own-xid
/// read-your-writes) among already-decoded `versions`. Mirrors `scan_live`'s
/// per-version `satisfies_mvcc` check, but over one rowid's versions.
///
/// Returns the greatest-xmin live version. The MVCC at-most-one-live invariant
/// means at most one version of a rowid is live under any one snapshot, so the
/// selection is unambiguous; choosing the max explicitly (rather than relying on
/// ascending scan order) makes it order-independent and is debug-asserted to see
/// at most one live version.
fn find_visible_one(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snap: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    versions: &[(u64, u64, Vec<pgtypes::Datum>)],
) -> Result<Option<(u64, Vec<pgtypes::Datum>)>, ExecError> {
    let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
    let mut live_count: usize = 0;
    for (xmin, xmax, row) in versions {
        if mvcc::visibility::satisfies_mvcc(
            *xmin,
            *xmax,
            snap,
            own,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            // Keep the greatest-xmin live version EXPLICITLY. The MVCC at-most-one-live
            // invariant means there is normally exactly one; selecting the max removes the
            // hidden dependence on ascending scan order, so a future scan-order change can
            // never silently return a stale shadow (e.g. an aborted re-attempt's
            // `Prepared(Li_old -> g)` tuple that resolves invisible anyway).
            // NB: `is_none_or`, NOT `map_or(true, …)` — the latter trips
            // `clippy::unnecessary_map_or` under the workspace's `-D warnings` gate.
            if visible.as_ref().is_none_or(|(cur, _)| *xmin > *cur) {
                visible = Some((*xmin, row.clone()));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one: {live_count} live versions for one rowid under one snapshot \
         — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
}

/// After locking the row, re-read its current versions. Returns the version to
/// operate on, or None to skip. Under REPEATABLE READ, a row changed by a txn
/// that committed after our snapshot is a serialization failure (40001). Under
/// READ COMMITTED, re-find the latest live version (a fresh snapshot).
#[allow(clippy::too_many_arguments)]
fn eval_plan_qual(
    kv: &dyn Kv,
    global: &dyn Kv,
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
    // Resolve this row's `Prepared(Li -> g)` markers against a SETTLED global view
    // — range 0's global clog read directly — NOT the statement's pre-lock global
    // snapshot (`gsnap`). We hold this row's lock, and a cross-range participant
    // releases a row's lock only AFTER its global decision is durable
    // (commit_release/abort_release run post-decision). So every global txn `g`
    // with a `Prepared` marker on THIS row's versions has already settled in
    // range 0's global clog; a still-in-doubt `g` could not have left a marker
    // here (it would still hold this lock, so we could not have acquired it).
    // Reading the global clog directly under the lock is therefore exact — and is
    // the read-committed-under-lock analogue of how the LOCAL clog is read
    // directly. Using `gsnap` would be stale: a `g` that committed while we were
    // blocked on the lock still appears in-doubt in `gsnap.xip`, hiding its just-
    // committed supersede and losing the update across the 2PC boundary. A settled
    // Snapshot (xmin 0, xmax MAX, empty xip) drives `global_status`'s in-doubt gate
    // (`g >= xmax || xip.contains(g)`) always false, so it reads `clog::get` for g.
    // The LOCAL `snapshot`/`fresh` handling below is unchanged — it is about local
    // creation ordering and is already correct.
    let settled_global = mvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    // Is the row's latest committed version deleted/superseded by a transaction
    // NOT visible to our txn snapshot (committed AFTER it), other than ourselves?
    // The resolver derefs a Prepared(xmx -> g) deleter to range 0's global
    // decision so a cross-range supersede is detected exactly when it commits.
    let resolve = global_status(kv, global, &settled_global);
    let changed_since_snapshot = versions.iter().any(|&(_xmn, xmx, _)| {
        xmx != mvcc::xid::INVALID_XID
            && xmx != xid
            && matches!(resolve(xmx), Ok(mvcc::clog::XidStatus::Committed))
            && !snapshot_can_see(snapshot, xmx)
    });
    if changed_since_snapshot {
        if repeatable_read {
            return Err(ExecError::SerializationFailure);
        }
        // READ COMMITTED: re-find the latest live version under a FRESH snapshot.
        let fresh = procarray.snapshot();
        return find_visible_one(kv, global, &settled_global, &fresh, Some(xid), &versions);
    }
    // No concurrent committed change: find the version visible to our snapshot.
    find_visible_one(kv, global, &settled_global, snapshot, Some(xid), &versions)
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
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
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
        let mut live_count: usize = 0;
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, row) = mvcc::version::decode_tuple(&scanned[i].1)?;
            if mvcc::visibility::satisfies_mvcc(
                xmin,
                xmax,
                snapshot,
                own,
                global_status(kv, global, gsnap),
            )? {
                live_count += 1;
                // `is_none_or`, NOT `map_or(true, …)` — see find_visible_one above.
                if visible.as_ref().is_none_or(|(cur, _)| xmin > *cur) {
                    visible = Some((xmin, row));
                }
            }
            i += 1;
        }
        debug_assert!(
            live_count <= 1,
            "scan_live: {live_count} live versions for rowid {rowid} under one snapshot \
             — MVCC at-most-one-live invariant violated"
        );
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_read(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let Statement::Select(s) = stmt else {
        return Err(ExecError::Unsupported("not a SELECT".into()));
    };
    let table: Option<Table> = match &s.from {
        Some(name) => Some(catalog::get_table(catalog_kv, name)?),
        None => None,
    };

    // Source rows: scan the table (dropping the rowid/xmin for projection), or a
    // single empty row for FROM-less SELECT.
    let source: Vec<Vec<Datum>> = match &table {
        Some(t) => scan_live(kv, global, gsnap, snapshot, own, t)?
            .into_iter()
            .map(|(_, _, row)| row)
            .collect(),
        None => vec![vec![]],
    };

    // Filter (WHERE runs before grouping).
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for row in &source {
        if row_matches(s.filter.as_ref(), table.as_ref(), row)? {
            kept.push(row.clone());
        }
    }

    // SP27: GROUP BY / HAVING / aggregate queries fold the filtered rows into
    // groups; everything else projects rows one-for-one.
    if crate::agg::is_aggregate_query(s) {
        return crate::agg::execute_aggregate(s, table.as_ref(), kept);
    }
    project_order_limit(s, table.as_ref(), kept)
}

/// Locking SELECT (FOR UPDATE / FOR SHARE). Takes a row lock on each visible
/// row before rechecking it via EvalPlanQual (same semantics as UPDATE/DELETE).
/// The snapshot and xid must already be established by the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_read_locking(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    snapshot: &mvcc::visibility::Snapshot,
    xid: u64,
    repeatable_read: bool,
    mode: crate::lockmgr::LockMode,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    // FOR UPDATE/SHARE is not allowed with aggregation (PostgreSQL 0A000).
    if crate::agg::is_aggregate_query(s) {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed with aggregate functions or GROUP BY".into(),
        ));
    }
    // FOR UPDATE/SHARE requires a FROM clause — there are no rows to lock
    // in a FROM-less SELECT.
    let table_name = s
        .from
        .as_ref()
        .ok_or_else(|| ExecError::Unsupported("FOR UPDATE/SHARE requires a FROM clause".into()))?;
    let t = catalog::get_table(catalog_kv, table_name)?;

    // Scan visible rows, then lock and EvalPlanQual-recheck each one.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for (rowid, _xmin, scanned_row) in scan_live(kv, global, gsnap, snapshot, Some(xid), &t)? {
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
        let Some((_cur_xmin, cur_row)) = eval_plan_qual(
            kv,
            global,
            procarray,
            snapshot,
            &t,
            rowid,
            xid,
            repeatable_read,
        )?
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
pub(crate) fn resolve_projection(
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
        // PostgreSQL names an aggregate output column after the function.
        Expr::Func(fc) => fc.name.clone(),
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

pub(crate) fn datum_to_cell(d: &Datum) -> Option<Cell> {
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
pub(crate) fn order_cmp(a: &[Datum], b: &[Datum], s: &SelectStmt) -> std::cmp::Ordering {
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

// `describe` only resolves the SELECT's row description from the catalog (no
// rows are scanned), so the data store `_kv` is unused here. It is kept in the
// signature for uniformity with the other three executor entry points (all take
// `catalog_kv, kv, …`) so the session's call sites stay consistent.
pub(crate) fn describe(
    catalog_kv: &dyn Kv,
    _kv: &dyn Kv,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let statements = pgparser::parse(sql)?;
    // Extended-protocol Describe targets a single statement.
    let Some(Statement::Select(s)) = statements.first() else {
        return Ok(Vec::new()); // non-SELECT (or empty) returns no row description
    };
    let table = match &s.from {
        Some(name) => Some(catalog::get_table(catalog_kv, name)?),
        None => None,
    };
    let (fields, _exprs) = resolve_projection(&s.projection, table.as_ref())?;
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use crate::{SqlEngine, SqlSession};
    use pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

    #[test]
    fn global_status_derefs_prepared_to_range0_global_clog() {
        use super::global_status;
        use kv::{Kv, MemKv};
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::xid::GLOBAL_XID_BASE;
        let (local, global) = (MemKv::new(), MemKv::new());
        let li = 5u64;
        let g = GLOBAL_XID_BASE + 1;
        local
            .write_batch(&[put_op(li, XidStatus::Prepared(g))])
            .expect("put prepared marker");
        // G in-doubt (not in global clog, gsnap says running) => InProgress (invisible)
        let running = mvcc::visibility::Snapshot {
            xmin: g,
            xmax: g + 1,
            xip: vec![g],
        };
        assert_eq!(
            global_status(&local, &global, &running)(li).expect("resolve in-doubt"),
            XidStatus::InProgress
        );
        // G committed + settled (gsnap moved past it) => Committed (visible)
        global
            .write_batch(&[put_op(g, XidStatus::Committed)])
            .expect("put global commit");
        let settled = mvcc::visibility::Snapshot {
            xmin: g + 2,
            xmax: g + 2,
            xip: vec![],
        };
        assert_eq!(
            global_status(&local, &global, &settled)(li).expect("resolve settled"),
            XidStatus::Committed
        );
        // A plain local xid is unaffected.
        local
            .write_batch(&[put_op(3, XidStatus::Committed)])
            .expect("put local commit");
        assert_eq!(
            global_status(&local, &global, &settled)(3).expect("resolve local"),
            XidStatus::Committed
        );
    }

    #[test]
    fn durable_global_snapshot_resolves_committed_against_range0() {
        use kv::{Kv, MemKv};
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::xid::GLOBAL_XID_BASE;
        let local = MemKv::new(); // this range's clog
        let global = MemKv::new(); // range 0's global clog + meta
        let g = GLOBAL_XID_BASE + 5;

        local
            .write_batch(&[put_op(3, XidStatus::Prepared(g))])
            .expect("local prepared");
        // Range 0: g committed, next_global persisted past g — BIG-ENDIAN, the exact
        // on-disk layout the GTM allocator writes (correction C1).
        global
            .write_batch(&[put_op(g, XidStatus::Committed)])
            .expect("global committed");
        global
            .write_batch(&[kv::WriteOp::Put {
                key: kv::key::meta_next_global_xid_key(),
                value: (g + 1).to_be_bytes().to_vec(),
            }])
            .expect("persist next_global");

        let gsnap = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap");
        let resolve = crate::exec::global_status(&local, &global, &gsnap);
        assert_eq!(
            resolve(3).expect("resolve"),
            XidStatus::Committed,
            "committed cross-range deleter resolves Committed via range 0's durable clog"
        );

        let g2 = GLOBAL_XID_BASE + 6;
        local
            .write_batch(&[put_op(4, XidStatus::Prepared(g2))])
            .expect("local prepared 2");
        global
            .write_batch(&[kv::WriteOp::Put {
                key: kv::key::meta_next_global_xid_key(),
                value: (g2 + 1).to_be_bytes().to_vec(),
            }])
            .expect("advance next_global past g2");
        let gsnap2 = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap2");
        let resolve2 = crate::exec::global_status(&local, &global, &gsnap2);
        assert_eq!(
            resolve2(4).expect("resolve g2"),
            XidStatus::InProgress,
            "allocated-but-undecided cross-range deleter is invisible"
        );
    }

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

    /// Regression test: `eval_plan_qual` must resolve a `Prepared(LA → g)` deleter
    /// against the CURRENT global clog (via `settled_global`), NOT the writer's
    /// pre-lock global snapshot (`gsnap`), which may still list `g` as in-flight.
    ///
    /// Scenario (reconstructed without concurrency):
    ///   - Cross-range txn `LA` (local xid on this range) UPDATE-committed row R
    ///     from value 100 (v1) to value 70 (v2), leaving local clog entry
    ///     `LA → Prepared(g1)` and global clog entry `g1 → Committed`.
    ///   - Writer W took its global snapshot BEFORE `g1` was committed, so that
    ///     snapshot still lists `g1` as in-flight (stale gsnap).
    ///   - W now holds the row lock and calls `eval_plan_qual`.
    ///
    /// With the fix (`settled_global`):
    ///   `resolve(LA) == Committed`, `changed_since_snapshot == true`, READ COMMITTED
    ///   re-finds under a fresh snapshot → returns v2 (value 70). Correct.
    ///
    /// Without the fix (using `gsnap` for resolve):
    ///   `resolve(LA) == InProgress` (g1 still in-doubt in stale gsnap) →
    ///   `changed_since_snapshot == false` → `find_visible_one` with stale snapshot
    ///   sees v1 as live (xmax=LA appears uncommitted) → returns v1 (value 100).
    ///   Lost update across the 2PC boundary.
    #[test]
    fn eval_plan_qual_settled_global_sees_committed_cross_range_version() {
        use std::sync::Arc;

        use super::eval_plan_qual;
        use catalog::{Column, Table};
        use kv::{Kv, MemKv};
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::version::{encode_tuple, version_key_xid};
        use mvcc::visibility::Snapshot;
        use mvcc::xid::{GLOBAL_XID_BASE, INVALID_XID};
        use pgtypes::{ColumnType, Datum};

        // ── xid assignments ─────────────────────────────────────────────────────
        let x0: u64 = 1; // original inserter — settled, committed
        let la: u64 = 2; // cross-range txn's local xid (Prepared state in local clog)
        let g1: u64 = GLOBAL_XID_BASE + 1; // global txn id
        let writer: u64 = 3; // the writer calling eval_plan_qual (current txn)

        // ── stores ──────────────────────────────────────────────────────────────
        // `kv` holds both the data range's row versions AND the local clog.
        // `global` holds only range-0's global clog.
        let kv = Arc::new(MemKv::new());
        let global = MemKv::new();

        // ── catalog table ────────────────────────────────────────────────────────
        // Table id 1, single int4 column "val".
        let table = Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column {
                name: "val".into(),
                ty: ColumnType::Int4,
            }],
        };
        let rowid: u64 = 1;

        // ── write two versions of row R ──────────────────────────────────────────
        // v1: created by x0, deleted (xmax) by la — value 100 (the old row)
        kv.write_batch(&[kv::WriteOp::Put {
            key: version_key_xid(table.id, rowid, x0),
            value: encode_tuple(x0, la, &[Datum::Int4(100)]),
        }])
        .expect("write v1");
        // v2: created by la, live (xmax=INVALID_XID) — value 70 (the updated row)
        kv.write_batch(&[kv::WriteOp::Put {
            key: version_key_xid(table.id, rowid, la),
            value: encode_tuple(la, INVALID_XID, &[Datum::Int4(70)]),
        }])
        .expect("write v2");

        // ── local clog in `kv` ───────────────────────────────────────────────────
        // x0 is settled-committed.  la is in Prepared state → g1.
        kv.write_batch(&[
            put_op(x0, XidStatus::Committed),
            put_op(la, XidStatus::Prepared(g1)),
        ])
        .expect("write local clog");

        // ── global clog in `global` ──────────────────────────────────────────────
        // g1 has committed — but writer's global snapshot is stale (lists g1 as
        // in-flight), so eval_plan_qual MUST use settled_global, not stale_gsnap.
        global
            .write_batch(&[put_op(g1, XidStatus::Committed)])
            .expect("write global clog");

        // ── stale global snapshot (what the writer held pre-lock) ────────────────
        // g1 is listed as in-flight — this is the bug trigger.
        // NOTE: eval_plan_qual no longer accepts gsnap as a parameter (the fix
        // bakes settled_global internally), so this snapshot is used as the
        // *local* snapshot below, which represents the writer's view of local xids.
        // The global staleness is expressed via the local clog's Prepared marker.

        // ── procarray: writer (xid=3) is running; x0 and la are not ────────────
        // The fresh snapshot produced by procarray.snapshot() inside eval_plan_qual
        // will have xmax=4, xip=[3] — so la (xid=2) < xmax=4 and not in xip,
        // meaning satisfies_mvcc will ask the clog for la → Prepared(g1) →
        // settled_global → Committed → v2 visible. Correct.
        let procarray = crate::procarray::ProcArray::open(
            Arc::clone(&kv) as Arc<dyn kv::Kv>,
            crate::PersistMode::Durable,
        )
        .expect("procarray open");
        // Advance next_xid past x0, la, and writer by allocating writer's slot.
        // (begin_write allocates sequentially starting at 1.)
        let _xid_x0 = procarray.begin_write().expect("alloc x0 slot"); // xid=1
        let _xid_la = procarray.begin_write().expect("alloc la slot"); // xid=2
        let _xid_w = procarray.begin_write().expect("alloc writer slot"); // xid=3
        // Mark x0 and la as finished (committed) so they are not in the running set.
        procarray.finish(_xid_x0);
        procarray.finish(_xid_la);
        // writer (xid=3) remains running.

        // ── local (txn) snapshot for the writer ─────────────────────────────────
        // Taken when the writer began. At that time la (xid=2) was still running
        // in the local sense because the Prepared marker hadn't been removed yet.
        // NOTE: in the real 2PC path la is deregistered from procarray at prepare,
        // so in practice it would not appear in xip here; but eval_plan_qual's
        // staleness bug is about the GLOBAL snapshot, not the local one. We make
        // la visible in the local snapshot to keep the test simple and focused:
        // x0 is settled (xid < xmax, not in xip) and la is settled too (same).
        // The critical stale element is the global clog Prepared → g1-in-doubt path,
        // which is exercised via the kv local-clog entry `la → Prepared(g1)`.
        //
        // Writer's local snapshot: xmax = writer (3), only writer in xip.
        // x0=1 < 3 and not in xip → settled; la=2 < 3 and not in xip → settled.
        // This is the snapshot held when the writer started, BEFORE it blocked on
        // the row lock. la's Prepared(g1) status makes g1 the relevant global txn.
        let writer_snapshot = Snapshot {
            xmin: writer,
            xmax: writer,      // writer itself started after x0 and la settled locally
            xip: vec![writer], // writer is the only running local txn
        };

        // ── call eval_plan_qual ──────────────────────────────────────────────────
        // With the fix: eval_plan_qual uses settled_global internally, so:
        //   resolve(la) → Prepared(g1) → g1 not in-doubt in settled_global → Committed
        //   changed_since_snapshot: xmax=la, la != INVALID_XID, la != writer,
        //     resolve(la)==Committed, !snapshot_can_see(writer_snapshot, la).
        //   snapshot_can_see(writer_snapshot, la): la=2 < xmax=3, la not in xip=[3]
        //     → la IS visible → snapshot_can_see = true → !true = false → NOT changed.
        //
        // Wait — if la is visible in writer_snapshot, changed_since_snapshot is false,
        // so we go to find_visible_one with writer_snapshot and settled_global.
        // With settled_global: resolve(la) = Committed.
        // v1: xmin=x0 (committed, visible), xmax=la (committed-visible) → NOT visible.
        // v2: xmin=la (committed-visible), xmax=INVALID_XID → visible. Returns v2. Correct.
        //
        // Without the fix (using stale gsnap where g1 is in-doubt):
        //   resolve(la) → Prepared(g1) → g1 in-doubt → InProgress
        //   changed_since_snapshot: resolve(la)==InProgress, not Committed → false
        //   find_visible_one with writer_snapshot and stale resolver:
        //     v1: xmin=x0 visible, xmax=la → resolve(la)=InProgress → not committed
        //         → xmax not committed-visible → v1 appears live → visible!
        //     v2: xmin=la → committed_visible(la): la not own, la < xmax=3, not in xip
        //         → NOT running → asks status: InProgress → NOT committed → v2 invisible
        //   Returns v1 (value 100). Bug.
        let result = eval_plan_qual(
            kv.as_ref(),
            &global,
            &procarray,
            &writer_snapshot,
            &table,
            rowid,
            writer,
            false, // READ COMMITTED
        )
        .expect("eval_plan_qual must not error");

        // The fix: must see v2 (xmin=la, value=70), NOT v1 (value=100).
        let (ret_xmin, ret_row) = result.expect("must find a version (not None)");
        assert_eq!(
            ret_xmin, la,
            "eval_plan_qual must return the cross-range committed version (xmin=la={la}), \
             not the stale pre-commit version (xmin=x0={x0})"
        );
        assert_eq!(
            ret_row,
            vec![Datum::Int4(70)],
            "eval_plan_qual must return value 70 (cross-range committed UPDATE result), \
             not value 100 (the stale pre-2PC-commit row) — lost-update bug"
        );
    }

    /// SP21: after a fresh-`g'` re-attempt, a row has TWO physical versions — the
    /// abandoned attempt's `Prepared(Li_old -> g)` with `g` Aborted, and the re-attempt's
    /// `Prepared(Li_new -> g')` with `g'` Committed. `find_visible_one` must return the
    /// committed-`g'` version (highest xmin) and never the aborted shadow; exactly one
    /// version is live (the assert holds).
    #[test]
    fn find_visible_one_returns_committed_reattempt_over_aborted_shadow() {
        use std::sync::Arc;

        use super::{find_visible_one, global_status};
        use kv::{Kv, MemKv};
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::visibility::Snapshot;
        use mvcc::xid::{GLOBAL_XID_BASE, INVALID_XID};
        use pgtypes::Datum;

        let li_old: u64 = 5; // abandoned attempt's local xid
        let li_new: u64 = 9; // re-attempt's local xid (reseed -> strictly greater)
        let g: u64 = GLOBAL_XID_BASE + 1; // abandoned global xid (Aborted)
        let g2: u64 = GLOBAL_XID_BASE + 2; // fresh global xid (Committed)

        let kv = Arc::new(MemKv::new()); // holds the local clog
        let global = MemKv::new(); // range-0 global clog

        // `find_visible_one` reads ONLY the passed `versions` slice + the local/global clogs
        // (it never touches the kv row-version store), so seed just the two clogs here.
        // Local clog: both local xids are Prepared, deref to the global clog.
        kv.write_batch(&[
            put_op(li_old, XidStatus::Prepared(g)),
            put_op(li_new, XidStatus::Prepared(g2)),
        ])
        .expect("local clog");
        // Global clog: g Aborted (abandoned), g2 Committed (re-attempt).
        global
            .write_batch(&[
                put_op(g, XidStatus::Aborted),
                put_op(g2, XidStatus::Committed),
            ])
            .expect("global clog");

        // A settled snapshot: every xid is settled, so global_status reads the global clog.
        let settled = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };
        // The two physical versions, both live (xmax = INVALID): old value 100, new value 70.
        let versions = vec![
            (li_old, INVALID_XID, vec![Datum::Int4(100)]),
            (li_new, INVALID_XID, vec![Datum::Int4(70)]),
        ];
        let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
            .expect("find_visible_one ok")
            .expect("a version is visible");
        assert_eq!(
            got.0, li_new,
            "the committed re-attempt version (highest xmin) wins"
        );
        assert_eq!(
            got.1,
            vec![Datum::Int4(70)],
            "value is the re-attempt's, not the aborted shadow's"
        );
        // Sanity: the aborted shadow really is invisible under this resolver.
        let resolve = global_status(kv.as_ref(), &global, &settled);
        assert!(matches!(resolve(li_old), Ok(XidStatus::Aborted)));
    }

    /// The explicit highest-xmin selection is order-independent, and the at-most-one-live
    /// invariant is debug-asserted. Two committed, non-deleted versions of one row are an
    /// artificial invariant violation: in DEBUG the assert fires (`should_panic`); in
    /// RELEASE the assert is compiled out and the greater xmin is returned regardless of
    /// the order the versions are presented.
    ///
    /// Debug-profile-dependent BY DESIGN: this repo's CI runs `cargo nextest` and
    /// `cargo llvm-cov nextest` in the debug profile, so the `debug_assert!` fires and the
    /// `should_panic` arm is exercised. Introducing a release/opt test profile would flip
    /// the expectation and require revisiting this `cfg_attr`.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "at-most-one-live"))]
    fn find_visible_one_orders_by_xmin_and_flags_multiple_live() {
        use std::sync::Arc;

        use super::find_visible_one;
        use kv::{Kv, MemKv};
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::visibility::Snapshot;
        use mvcc::xid::INVALID_XID;
        use pgtypes::Datum;

        let kv = Arc::new(MemKv::new());
        let global = MemKv::new();
        kv.write_batch(&[
            put_op(5, XidStatus::Committed),
            put_op(9, XidStatus::Committed),
        ])
        .expect("clog");
        let settled = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };

        // Present them in DESCENDING order so last-wins would pick the LOWER xmin; the
        // explicit max must still pick 9.
        let versions = vec![
            (9u64, INVALID_XID, vec![Datum::Int4(70)]),
            (5u64, INVALID_XID, vec![Datum::Int4(100)]),
        ];
        let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
            .expect("ok"); // only reached in release builds
        assert_eq!(
            got.expect("visible").0,
            9,
            "highest xmin regardless of presentation order"
        );
    }
}
