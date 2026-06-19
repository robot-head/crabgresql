//! Per-statement execution.

use std::sync::Arc;

use bytes::Bytes;
use catalog::{Column, Table, TableId};
use kv::Kv;
use pgparser::ast::{Expr, OrderItem, SelectItem, SelectStmt, Statement};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::{Cell, FieldDescription, QueryResult};
use zerocopy::FromBytes;
use zerocopy::byteorder::big_endian::U64;

use crate::error::ExecError;
use crate::foreign::{ForeignScanner, ScanBounds};
use crate::join::{Relation, join_relations};
use crate::scope::{ColumnBinding, Scope};

/// SP40: the foreign-table read context threaded through the SELECT pipeline. It
/// carries the registered scanner (the `kafka_fdw` seam) and the current user (for
/// resolving the per-user `UserMapping`). Bundled into one borrowed struct so the
/// already-wide read signatures gain a single argument rather than two. Paths that
/// never reach a registered scanner (`describe`, the schema-only build) use
/// `ForeignCtx::none()`; a foreign `SELECT` with no scanner registered returns
/// `0A000` ("foreign tables require the `kafka` feature").
#[derive(Clone, Copy)]
pub(crate) struct ForeignCtx<'a> {
    pub scanner: Option<&'a Arc<dyn ForeignScanner>>,
    pub current_user: &'a str,
}

impl ForeignCtx<'_> {
    /// A context with no scanner and the conventional `"public"` user — for paths
    /// that never reach a registered scanner (schema-only describe).
    pub(crate) fn none() -> Self {
        Self {
            scanner: None,
            current_user: "public",
        }
    }
}

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
    fctx: ForeignCtx,
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
        // SP40 FDW DDL. The catalog's foreign-object CRUD writes its single small
        // batch directly (it is not on the row-write hot path and has no ops-only
        // variant), so these arms persist via the catalog and return an EMPTY ops
        // vec — `run_ddl` then commits an empty batch (a no-op). The catalog
        // validates existence/duplication and surfaces 42710 / 42704 / 42P07 / 42P01.
        Statement::CreateFdw { name, options } => {
            catalog::create_fdw(kv, name, options.clone())?;
            Ok((command("CREATE FOREIGN DATA WRAPPER"), Vec::new()))
        }
        Statement::DropFdw { name, if_exists } => {
            ignore_missing(catalog::drop_fdw(kv, name), *if_exists)?;
            Ok((command("DROP FOREIGN DATA WRAPPER"), Vec::new()))
        }
        Statement::CreateServer {
            name,
            wrapper,
            options,
        } => {
            catalog::create_server(kv, name, wrapper, options.clone())?;
            Ok((command("CREATE SERVER"), Vec::new()))
        }
        Statement::DropServer { name, if_exists } => {
            ignore_missing(catalog::drop_server(kv, name), *if_exists)?;
            Ok((command("DROP SERVER"), Vec::new()))
        }
        Statement::CreateUserMapping {
            user,
            server,
            options,
        } => {
            catalog::create_user_mapping(kv, user, server, options.clone())?;
            Ok((command("CREATE USER MAPPING"), Vec::new()))
        }
        Statement::DropUserMapping {
            user,
            server,
            if_exists,
        } => {
            ignore_missing(catalog::drop_user_mapping(kv, user, server), *if_exists)?;
            Ok((command("DROP USER MAPPING"), Vec::new()))
        }
        Statement::CreateForeignTable {
            name,
            columns,
            server,
            options,
        } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect();
            catalog::create_foreign_table(kv, name, cols, server, options.clone())?;
            Ok((command("CREATE FOREIGN TABLE"), Vec::new()))
        }
        Statement::DropForeignTable { name, if_exists } => {
            // A foreign table shares the ordinary table catalog key, so `drop_table`
            // removes it (catalog entry + sequence + any rows).
            match catalog::drop_table(kv, name) {
                Ok(()) => {}
                Err(catalog::CatalogError::UndefinedTable(_)) if *if_exists => {}
                Err(e) => return Err(e.into()),
            }
            Ok((command("DROP FOREIGN TABLE"), Vec::new()))
        }
        // The catalog has no ALTER for foreign objects, and phase-1 querying does
        // not need one — surface a clear 0A000 rather than silently no-op'ing.
        Statement::AlterServer { .. } => {
            Err(ExecError::Unsupported("ALTER SERVER not supported".into()))
        }
        Statement::AlterUserMapping { .. } => Err(ExecError::Unsupported(
            "ALTER USER MAPPING not supported".into(),
        )),
        // SP40: IMPORT FOREIGN SCHEMA discovers the server's tables through the
        // registered scanner (the `kafka_fdw` seam enumerates Kafka topics and
        // derives each topic's value columns from its Schema Registry subject),
        // then materializes a foreign table per discovered table into the catalog.
        // Like CreateForeignTable, the catalog writes each small batch directly, so
        // this returns an EMPTY ops vec (the caller commits a no-op batch).
        //
        // `remote_schema` is accepted but unused in phase 1 (Kafka has no nested
        // schemas); `into_schema` is a flat namespace today — the discovered table
        // name is used verbatim as the catalog name.
        Statement::ImportForeignSchema {
            remote_schema: _,
            selector,
            server,
            into_schema: _,
        } => {
            // Resolve the server (42704 if undefined) and the current user's
            // optional mapping (no mapping → no credentials).
            let srv = catalog::get_server(kv, server)?;
            let mapping = catalog::get_user_mapping(kv, fctx.current_user, server).ok();
            // A scanner must be registered (the `kafka` feature is built in).
            let scanner = fctx.scanner.ok_or_else(|| {
                ExecError::Unsupported("foreign tables require the `kafka` feature".into())
            })?;
            let filter = crate::foreign::ImportFilter::from_selector(selector);
            let tables = scanner.import_schema(&srv, mapping.as_ref(), &filter)?;
            for table in tables {
                catalog::create_foreign_table(
                    kv,
                    &table.name,
                    table.columns,
                    &srv.name,
                    table.options,
                )?;
            }
            Ok((command("IMPORT FOREIGN SCHEMA"), Vec::new()))
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}

/// A `QueryResult::Command` with the given PostgreSQL completion tag.
fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

/// Swallow a `42704` (undefined object) when `IF EXISTS` was given; propagate
/// every other error. Used by the foreign-object `DROP … IF EXISTS` arms.
fn ignore_missing(r: Result<(), catalog::CatalogError>, if_exists: bool) -> Result<(), ExecError> {
    match r {
        Ok(()) => Ok(()),
        Err(catalog::CatalogError::UndefinedObject(_)) if if_exists => Ok(()),
        Err(e) => Err(e.into()),
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
    ctx: &crate::clock::EvalCtx,
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
                    let v = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                    full[*slot] = coerce(v, t.columns[*slot].ty, ctx)?;
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
            let scope = Scope::single(&t, &t.name);
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
                if !row_matches(filter.as_ref(), &scope, &scanned_row, ctx)? {
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
                if !row_matches(filter.as_ref(), &scope, &cur_row, ctx)? {
                    continue; // no longer matches the WHERE clause
                }
                let mut next = cur_row.clone();
                for (idx, expr) in &targets {
                    let v = crate::eval::eval(expr, &scope, &cur_row, ctx)?;
                    next[*idx] = coerce(v, t.columns[*idx].ty, ctx)?;
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
            let scope = Scope::single(&t, &t.name);
            let mut n: u64 = 0;
            for (rowid, _xmin, scanned_row) in
                scan_live(kv, global, gsnap, snapshot, Some(xid), &t)?
            {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause.
                if !row_matches(filter.as_ref(), &scope, &scanned_row, ctx)? {
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
                if !row_matches(filter.as_ref(), &scope, &cur_row, ctx)? {
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

/// Coerce an evaluated value into a target column type (assignment context). `ctx`
/// supplies the session zone for any temporal numeric conversion.
fn coerce(
    value: pgtypes::Datum,
    target: pgtypes::ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<pgtypes::Datum, ExecError> {
    use pgtypes::{ColumnType, Datum, TypeError};
    // SP32: assignment to a `numeric` column — any numeric-family value (int4/
    // int8/float8/numeric) converts, applying the column's `(p,s)` modifier (round
    // + overflow). A `text` value still needs an explicit cast (handled by the
    // catch-all below); NULL falls through to the `(Null, _)` arm.
    if target.is_numeric()
        && matches!(
            value,
            Datum::Int4(_) | Datum::Int8(_) | Datum::Float8(_) | Datum::Numeric(_)
        )
    {
        return Ok(pgtypes::cast::cast(&value, target, &ctx.time_zone)?);
    }
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
        // SP30: float8 assignment casts. int → float8 is the standard widening;
        // float8 → int rounds half-to-even (PG's float→int assignment cast) and
        // range-checks (out of range / non-finite → 22003).
        (Datum::Float8(f), ColumnType::Float8) => Datum::Float8(f),
        (Datum::Int4(n), ColumnType::Float8) => Datum::Float8(f64::from(n)),
        (Datum::Int8(n), ColumnType::Float8) => Datum::Float8(n as f64),
        (Datum::Float8(f), ColumnType::Int4) => {
            let r = f.round_ties_even();
            if r.is_finite() && (i32::MIN as f64..=i32::MAX as f64).contains(&r) {
                Datum::Int4(r as i32)
            } else {
                return Err(TypeError::Overflow.into());
            }
        }
        (Datum::Float8(f), ColumnType::Int8) => {
            let r = f.round_ties_even();
            if r.is_finite() && (i64::MIN as f64..=i64::MAX as f64).contains(&r) {
                Datum::Int8(r as i64)
            } else {
                return Err(TypeError::Overflow.into());
            }
        }
        // SP32: assignment of a numeric value into a non-numeric numeric-family
        // column (→ numeric column is handled by the pre-check above). numeric→int
        // rounds half-away-from-zero with a range check (22003); numeric→float8 may
        // become ±Infinity for an out-of-range magnitude.
        (Datum::Numeric(d), ColumnType::Float8) => Datum::Float8(pgtypes::numeric::to_f64(&d)),
        (Datum::Numeric(d), ColumnType::Int4) => pgtypes::numeric::to_i32(&d).map(Datum::Int4)?,
        (Datum::Numeric(d), ColumnType::Int8) => pgtypes::numeric::to_i64(&d).map(Datum::Int8)?,
        // SP37: date/time assignment — same-type pass-through (no implicit
        // cross-type coercion between temporal types; mismatches hit the catch-all).
        (Datum::Date(d), ColumnType::Date) => Datum::Date(d),
        (Datum::Time(t), ColumnType::Time) => Datum::Time(t),
        (Datum::Timestamp(ts), ColumnType::Timestamp) => Datum::Timestamp(ts),
        (Datum::Timestamptz(ts), ColumnType::Timestamptz) => Datum::Timestamptz(ts),
        (Datum::Interval(iv), ColumnType::Interval) => Datum::Interval(iv),
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
    scope: &Scope,
    row: &[pgtypes::Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, scope, row, ctx)? {
            pgtypes::Datum::Bool(b) => Ok(b),
            pgtypes::Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}

/// SP40 Task 14: extract per-partition offset bounds from a single-foreign-table
/// query's top-level `WHERE` for pushdown into the Kafka foreign scan.
///
/// Walks the top-level `AND` chain of the filter and, for every `_partition = N`
/// constraint, collects the `_offset` range comparisons scoped to that partition
/// into [`ScanBounds`]. This is a PURE OPTIMIZATION: anything not representable in
/// `ScanBounds` (a bare `_offset` with no `_partition =`, a `_timestamp`/`LIMIT`
/// constraint, an `OR`, a non-envelope predicate) is simply omitted here and
/// remains a residual `WHERE` filter applied locally after the scan. Callers MUST
/// keep evaluating the full `WHERE`; pushed bounds must never change results.
///
/// Conversions (the scan reads `[start, end)` per partition):
/// - `_offset >= a` → start `a`; `_offset > a` → start `a + 1` (inclusive lower).
/// - `_offset <= b` → end `b + 1`; `_offset < b` → end `b` (exclusive upper).
/// - `_offset BETWEEN a AND b` → start `a`, end `b + 1` (PG bounds are inclusive).
///
/// Only offset bounds anchored to a concrete `_partition = N` are emitted: under
/// this `ScanBounds` shape (`Vec<(partition, offset)>`) a partition-less offset
/// cannot target a partition, so it stays residual.
#[must_use]
pub(crate) fn extract_scan_bounds(filter: Option<&Expr>) -> ScanBounds {
    let mut bounds = ScanBounds::default();
    let Some(filter) = filter else {
        return bounds;
    };

    // Flatten the top-level AND chain into its conjuncts. An OR or any other
    // shape is left intact (and thus never matches a comparison below), so it
    // contributes nothing — it remains a residual filter.
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);

    // Resolve the single `_partition = N` anchor, if exactly one is present. With
    // zero (or conflicting/multiple) partition equalities we cannot scope offsets
    // to a partition, so we push nothing and let WHERE do all the work.
    let mut partition: Option<i32> = None;
    for c in &conjuncts {
        if let Some(p) = match_partition_eq(c) {
            match partition {
                None => partition = Some(p),
                Some(prev) if prev == p => {}
                // Two different `_partition =` values → unsatisfiable as written;
                // don't try to push, let the residual WHERE return zero rows.
                Some(_) => return ScanBounds::default(),
            }
        }
    }
    let Some(partition) = partition else {
        return bounds;
    };

    // Tightest inclusive-start / exclusive-end across all offset conjuncts.
    let mut start: Option<i64> = None;
    let mut end: Option<i64> = None;
    let mut tighten_start = |v: i64| {
        start = Some(start.map_or(v, |cur: i64| cur.max(v)));
    };
    let mut tighten_end = |v: i64| {
        end = Some(end.map_or(v, |cur: i64| cur.min(v)));
    };

    for c in &conjuncts {
        match match_offset_bound(c) {
            Some(OffsetBound::StartIncl(v)) => tighten_start(v),
            Some(OffsetBound::EndExcl(v)) => tighten_end(v),
            Some(OffsetBound::Between { start: s, end: e }) => {
                tighten_start(s);
                tighten_end(e);
            }
            None => {}
        }
    }

    if let Some(s) = start {
        bounds.start_offsets.push((partition, s));
    }
    if let Some(e) = end {
        bounds.end_offsets.push((partition, e));
    }
    bounds
}

/// Flatten a top-level `AND` chain into its leaf conjuncts (depth-first). A node
/// that is not an `AND` is itself one conjunct.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        op: pgparser::ast::BinaryOp::And,
        left,
        right,
    } = expr
    {
        collect_conjuncts(left, out);
        collect_conjuncts(right, out);
    } else {
        out.push(expr);
    }
}

/// An envelope-column reference by bare name (`_partition`/`_offset`/…). Envelope
/// columns are unqualified in practice; a table-qualified `t._offset` also matches
/// on the bare name (the qualifier is the single foreign table in scope).
fn envelope_col(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// Parse an integer literal expression to `i64`. Only bare/negated integer
/// literals are recognized (offsets/partitions are integers); anything else
/// (params, casts, non-integers) is not pushable and returns `None`.
fn int_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::IntLiteral(s) => s.parse::<i64>().ok(),
        Expr::Unary {
            op: pgparser::ast::UnaryOp::Neg,
            expr,
        } => int_literal(expr).map(|v| -v),
        _ => None,
    }
}

/// Match `_partition = N` (either operand order) and return `N`.
fn match_partition_eq(expr: &Expr) -> Option<i32> {
    let Expr::Binary {
        op: pgparser::ast::BinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    let v = if envelope_col(left) == Some("_partition") {
        int_literal(right)?
    } else if envelope_col(right) == Some("_partition") {
        int_literal(left)?
    } else {
        return None;
    };
    i32::try_from(v).ok()
}

/// An offset constraint normalized to the scan's `[start, end)` convention.
enum OffsetBound {
    /// Inclusive lower offset.
    StartIncl(i64),
    /// Exclusive upper offset.
    EndExcl(i64),
    /// `BETWEEN a AND b` → inclusive `start`, exclusive `end`.
    Between { start: i64, end: i64 },
}

/// Match an `_offset` comparison / BETWEEN and normalize it to an [`OffsetBound`].
/// Returns `None` for anything that is not an `_offset` range constraint. The
/// comparison is recognized with the column on either side (the operator is
/// mirrored when the column is on the right).
fn match_offset_bound(expr: &Expr) -> Option<OffsetBound> {
    use pgparser::ast::BinaryOp;
    match expr {
        Expr::Binary { op, left, right } => {
            // Normalize to `_offset <op> literal` by mirroring when reversed.
            let (op, lit) = if envelope_col(left) == Some("_offset") {
                (*op, int_literal(right)?)
            } else if envelope_col(right) == Some("_offset") {
                (mirror_op(*op)?, int_literal(left)?)
            } else {
                return None;
            };
            match op {
                BinaryOp::Ge => Some(OffsetBound::StartIncl(lit)),
                BinaryOp::Gt => Some(OffsetBound::StartIncl(lit + 1)),
                BinaryOp::Le => Some(OffsetBound::EndExcl(lit + 1)),
                BinaryOp::Lt => Some(OffsetBound::EndExcl(lit)),
                _ => None,
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } if envelope_col(expr) == Some("_offset") => {
            let lo = int_literal(low)?;
            let hi = int_literal(high)?;
            Some(OffsetBound::Between {
                start: lo,
                end: hi + 1,
            })
        }
        _ => None,
    }
}

/// Mirror a comparison operator for the reversed-operand form (`5 < _offset`
/// means `_offset > 5`). Only the inequalities used for offset bounds are mapped.
fn mirror_op(op: pgparser::ast::BinaryOp) -> Option<pgparser::ast::BinaryOp> {
    use pgparser::ast::BinaryOp;
    match op {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Le => Some(BinaryOp::Ge),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Ge => Some(BinaryOp::Le),
        _ => None,
    }
}

/// SP40 Task 14: is the FROM clause exactly one foreign base table? Only then is
/// offset pushdown applicable — a join, a comma-FROM (cross join), or a derived
/// table all keep the full-scan path. A scanner must be registered (otherwise the
/// foreign read errors anyway) and the single table's catalog entry must have
/// `foreign` metadata. Non-foreign ordinary tables return `false` (unchanged).
fn is_single_foreign_table(
    catalog_kv: &dyn Kv,
    from: &[pgparser::ast::TableExpr],
    fctx: ForeignCtx,
) -> bool {
    if fctx.scanner.is_none() {
        return false;
    }
    let [pgparser::ast::TableExpr::Table { name, .. }] = from else {
        return false;
    };
    catalog::get_table(catalog_kv, name).is_ok_and(|t| t.foreign.is_some())
}

/// Build the relation for one FROM list (comma items folded as cross joins).
#[allow(clippy::too_many_arguments)]
fn build_from(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    from: &[pgparser::ast::TableExpr],
    ctx: &crate::clock::EvalCtx,
    fctx: ForeignCtx,
    // SP40 Task 14: pushed-down offset bounds for the single-foreign-table case.
    // `Some` only when `from` is exactly one entry (set by `select_to_relation`);
    // joins/comma-FROM never see it and keep the full-scan + local-filter path.
    bounds: Option<&ScanBounds>,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from on empty FROM".into()))?;
    let mut acc = build_table_expr(
        catalog_kv, kv, global, gsnap, snapshot, own, first, ctx, fctx, bounds,
    )?;
    for te in iter {
        // A comma-FROM (multiple tables) is a cross join — no single-table
        // pushdown applies, so subsequent items always scan in full.
        let next = build_table_expr(
            catalog_kv, kv, global, gsnap, snapshot, own, te, ctx, fctx, None,
        )?;
        acc = join_relations(
            acc,
            next,
            pgparser::ast::JoinKind::Cross,
            &pgparser::ast::JoinConstraint::None,
            ctx,
        )?;
    }
    Ok(acc)
}

#[allow(clippy::too_many_arguments)]
fn build_table_expr(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    te: &pgparser::ast::TableExpr,
    ctx: &crate::clock::EvalCtx,
    fctx: ForeignCtx,
    // SP40 Task 14: pushed-down offset bounds, `Some` only for a single foreign
    // base table. Applied verbatim to the foreign scan; `None` ⇒ full scan.
    bounds: Option<&ScanBounds>,
) -> Result<Relation, ExecError> {
    use pgparser::ast::TableExpr;
    match te {
        TableExpr::Table { name, alias } => {
            let t = catalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name);
            // SP40: a foreign table reads through the registered scanner, not the
            // local MVCC version store. `build_from` materializes BEFORE WHERE, so
            // this scan runs even for `WHERE false` — there is no skip path.
            if let Some(meta) = &t.foreign {
                let scanner = fctx.scanner.ok_or_else(|| {
                    ExecError::Unsupported("foreign tables require the `kafka` feature".into())
                })?;
                let server = catalog::get_server(catalog_kv, &meta.server)?;
                // A per-user mapping is optional: fall back to no credentials when
                // the current user has none registered for this server.
                let mapping =
                    catalog::get_user_mapping(catalog_kv, fctx.current_user, &meta.server).ok();
                // SP40 Task 14: pass the pushed-down slice when present (single
                // foreign table). The residual WHERE still re-filters locally, so
                // results are identical whether or not the scan honors `bounds`.
                let default_bounds = ScanBounds::default();
                let scan_bounds = bounds.unwrap_or(&default_bounds);
                let rows = scanner.scan(&t, &server, mapping.as_ref(), scan_bounds, ctx)?;
                let scope = Scope::single(&t, qualifier);
                return Ok(Relation { scope, rows });
            }
            let scope = Scope::single(&t, qualifier);
            let rows = scan_live(kv, global, gsnap, snapshot, own, &t)?
                .into_iter()
                .map(|(_, _, row)| row)
                .collect();
            Ok(Relation { scope, rows })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            // A join is never a single foreign table: each side scans in full and
            // the join predicate / residual WHERE filters locally.
            let l = build_table_expr(
                catalog_kv, kv, global, gsnap, snapshot, own, left, ctx, fctx, None,
            )?;
            let r = build_table_expr(
                catalog_kv, kv, global, gsnap, snapshot, own, right, ctx, fctx, None,
            )?;
            join_relations(l, r, *kind, constraint, ctx)
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let inner = crate::query::query_to_relation(
                catalog_kv, kv, global, gsnap, snapshot, own, subquery, ctx, fctx,
            )?;
            crate::values::requalify_derived(inner, alias, columns)
        }
    }
}

/// Run a SELECT to a `Relation` (output columns + rows). The top-level
/// `execute_read` renders this to a `QueryResult`; a derived table re-qualifies
/// its columns under the derived alias. Non-correlated only (the subquery's scope
/// is built solely from its own FROM clause).
#[allow(clippy::too_many_arguments)]
pub(crate) fn select_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    s: &SelectStmt,
    ctx: &crate::clock::EvalCtx,
    fctx: ForeignCtx,
) -> Result<Relation, ExecError> {
    // SP34: resolve this (sub)query's uncorrelated subquery expressions to constants
    // first, under the same snapshot handles. Nested subqueries recurse here.
    let sub_ctx = crate::subquery::SubCtx {
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        eval_ctx: ctx,
        fctx,
    };
    let resolved = crate::subquery::resolve_in_select(&sub_ctx, s)?;
    let s = &resolved;
    let relation = if s.from.is_empty() {
        Relation {
            scope: Scope::empty(),
            rows: vec![vec![]],
        }
    } else {
        // SP40 Task 14: when the FROM is EXACTLY one foreign base table, extract
        // `_partition`/`_offset` bounds from the WHERE and push them into the
        // scan. The WHERE is still applied below, so this only ever reads less —
        // it never changes the result set.
        let pushed = if is_single_foreign_table(catalog_kv, &s.from, fctx) {
            Some(extract_scan_bounds(s.filter.as_ref()))
        } else {
            None
        };
        build_from(
            catalog_kv,
            kv,
            global,
            gsnap,
            snapshot,
            own,
            &s.from,
            ctx,
            fctx,
            pushed.as_ref(),
        )?
    };
    let mut kept = Vec::new();
    for row in &relation.rows {
        if row_matches(s.filter.as_ref(), &relation.scope, row, ctx)? {
            kept.push(row.clone());
        }
    }
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, &relation.scope)?;
    let out_scope = Scope {
        columns: fields
            .iter()
            .zip(&tys)
            .map(|(f, ty)| ColumnBinding {
                qualifier: None, // a projected result has no base-table qualifier
                name: f.name.clone(),
                ty: *ty,
            })
            .collect(),
    };
    let rows = if crate::agg::is_aggregate_query(s) {
        crate::agg::aggregate_rows(s, &relation.scope, kept, ctx)?
    } else {
        project_rows_ordered(s, &relation.scope, &fields, &out_exprs, kept, ctx)?
    };
    Ok(Relation {
        scope: out_scope,
        rows,
    })
}

/// Schema-only relation builder for `describe` — parallels `build_from` but base
/// tables produce no rows (no `scan_live`). Joining empty relations yields the
/// correct combined scope with no rows, so it reuses `join_relations`.
pub(crate) fn build_from_schema(
    catalog_kv: &dyn Kv,
    from: &[pgparser::ast::TableExpr],
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from_schema on empty FROM".into()))?;
    let mut acc = build_table_expr_schema(catalog_kv, first)?;
    for te in iter {
        let next = build_table_expr_schema(catalog_kv, te)?;
        // Schema-only: no rows, so no ON predicate is ever evaluated — a default
        // (UTC/epoch) eval context is correct here.
        acc = join_relations(
            acc,
            next,
            pgparser::ast::JoinKind::Cross,
            &pgparser::ast::JoinConstraint::None,
            &crate::clock::EvalCtx::test_default(),
        )?;
    }
    Ok(acc)
}

fn build_table_expr_schema(
    catalog_kv: &dyn Kv,
    te: &pgparser::ast::TableExpr,
) -> Result<Relation, ExecError> {
    use pgparser::ast::TableExpr;
    match te {
        TableExpr::Table { name, alias } => {
            let t = catalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name);
            Ok(Relation {
                scope: Scope::single(&t, qualifier),
                rows: Vec::new(),
            })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let l = build_table_expr_schema(catalog_kv, left)?;
            let r = build_table_expr_schema(catalog_kv, right)?;
            // Schema-only: no rows, so no ON predicate is ever evaluated.
            join_relations(
                l,
                r,
                *kind,
                constraint,
                &crate::clock::EvalCtx::test_default(),
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let fields = crate::query::describe_query_expr(catalog_kv, subquery)?;
            let bindings = fields
                .iter()
                .map(|f| {
                    Ok(ColumnBinding {
                        qualifier: None,
                        name: f.name.clone(),
                        ty: column_type_from_oid(f.type_oid)?,
                    })
                })
                .collect::<Result<_, ExecError>>()?;
            let inner = Relation {
                scope: Scope { columns: bindings },
                rows: Vec::new(),
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
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
    ctx: &crate::clock::EvalCtx,
    fctx: ForeignCtx,
) -> Result<QueryResult, ExecError> {
    let Statement::Query(q) = stmt else {
        return Err(ExecError::Unsupported("not a query statement".into()));
    };
    let rel = crate::query::query_to_relation(
        catalog_kv, kv, global, gsnap, snapshot, own, q, ctx, fctx,
    )?;
    Ok(crate::query::relation_to_rows_result(rel, ctx))
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
    ctx: &crate::clock::EvalCtx,
) -> Result<QueryResult, ExecError> {
    // SP34: resolve uncorrelated subqueries (e.g. in the WHERE of a FOR UPDATE) to
    // constants first, under this statement's snapshot handles.
    let sub_ctx = crate::subquery::SubCtx {
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own: Some(xid),
        eval_ctx: ctx,
        // A locking SELECT only operates over local tables; a subquery referencing a
        // foreign table here would surface the no-scanner error, which is acceptable
        // (FOR UPDATE over Kafka is not a phase-1 path).
        fctx: ForeignCtx::none(),
    };
    let resolved = crate::subquery::resolve_in_select(&sub_ctx, s)?;
    let s = &resolved;
    // FOR UPDATE/SHARE is not allowed with aggregation (PostgreSQL 0A000).
    if crate::agg::is_aggregate_query(s) {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed with aggregate functions or GROUP BY".into(),
        ));
    }
    // SP28: nor with SELECT DISTINCT (PostgreSQL 0A000).
    if s.distinct {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed with DISTINCT clause".into(),
        ));
    }
    // FOR UPDATE/SHARE requires exactly one base table — there are no rows to lock
    // in a FROM-less SELECT, and a join is not supported (0A000).
    let t = match s.from.as_slice() {
        [pgparser::ast::TableExpr::Table { name, .. }] => catalog::get_table(catalog_kv, name)?,
        [] => {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE requires a FROM clause".into(),
            ));
        }
        _ => {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE with a join is not supported".into(),
            ));
        }
    };
    let scope = Scope::single(&t, &t.name);

    // Scan visible rows, then lock and EvalPlanQual-recheck each one.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for (rowid, _xmin, scanned_row) in scan_live(kv, global, gsnap, snapshot, Some(xid), &t)? {
        // 1. Filter on the snapshot-visible row FIRST — only lock rows that
        //    match the WHERE clause (a FOR UPDATE/SHARE with no WHERE still
        //    locks all rows because row_matches(None, ..) returns true).
        if !row_matches(s.filter.as_ref(), &scope, &scanned_row, ctx)? {
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
        if !row_matches(s.filter.as_ref(), &scope, &cur_row, ctx)? {
            continue; // no longer matches
        }
        kept.push(cur_row);
    }

    project_order_limit(s, &scope, kept, ctx)
}

/// Apply DISTINCT / ORDER BY / OFFSET / LIMIT and projection, returning the
/// projected output Datum rows. Shared by the top-level row path and derived
/// tables. `ctx` carries the session zone + transaction/statement clock used by
/// temporal eval.
fn project_rows_ordered(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    mut kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let order_keys = resolve_select_order_keys(&s.order_by, scope, fields, out_exprs, s.distinct)?;

    // SP39: SELECT DISTINCT projects FIRST, dedups output rows, then ORDER BY
    // sorts the deduped output. PostgreSQL requires every sort key to refer to
    // the select-list output (ordinal, alias/name, or the exact select expression).
    if s.distinct {
        let mut projected = project_rows(out_exprs, scope, &kept, ctx)?;
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|r| seen.insert(r.clone()));
        if !s.order_by.is_empty() {
            let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = projected
                .into_iter()
                .map(|r| {
                    let keys = order_keys
                        .iter()
                        .map(|k| match k {
                            SelectOrderKey::Output(i) => r[*i].clone(),
                            SelectOrderKey::SourceExpr(_) => {
                                unreachable!("DISTINCT order keys are output-only")
                            }
                        })
                        .collect();
                    (keys, r)
                })
                .collect();
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
            projected = keyed.into_iter().map(|(_, r)| r).collect();
        }
        apply_offset_limit(&mut projected, s.offset, s.limit);
        return Ok(projected);
    }

    // Non-DISTINCT keeps the existing source-row ordering shape so non-projected
    // source expressions still work, but output ordinals/labels evaluate the
    // corresponding projection expression for each source row.
    if !s.order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        for row in kept {
            let mut keys = Vec::with_capacity(order_keys.len());
            for key in &order_keys {
                keys.push(match key {
                    SelectOrderKey::Output(i) => {
                        crate::eval::eval(&out_exprs[*i], scope, &row, ctx)?
                    }
                    SelectOrderKey::SourceExpr(expr) => crate::eval::eval(expr, scope, &row, ctx)?,
                });
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        kept = keyed.into_iter().map(|(_, row)| row).collect();
    }
    apply_offset_limit(&mut kept, s.offset, s.limit);
    project_rows(out_exprs, scope, &kept, ctx)
}

/// Apply ORDER BY, LIMIT, and projection to a set of already-filtered source
/// rows, producing the final `QueryResult::Rows`. Used by both `execute_read`
/// and `execute_read_locking` to avoid duplication.
///
/// `ctx` carries the session zone (forwarded to `rows_result` for `Timestamptz`
/// text rendering) and the transaction/statement clock used by temporal eval.
fn project_order_limit(
    s: &SelectStmt,
    scope: &Scope,
    kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<QueryResult, ExecError> {
    let (fields, out_exprs, _tys) = resolve_projection(&s.projection, scope)?;
    let rows = project_rows_ordered(s, scope, &fields, &out_exprs, kept, ctx)?;
    Ok(rows_result(fields, &rows, &ctx.time_zone))
}

/// Evaluate the projection expressions for each source row, yielding output
/// Datum rows (one `Datum` per output column).
fn project_rows(
    out_exprs: &[Expr],
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(out_exprs.len());
        for e in out_exprs {
            cells.push(crate::eval::eval(e, scope, row, ctx)?);
        }
        out.push(cells);
    }
    Ok(out)
}

/// Encode projected Datum rows into a `QueryResult::Rows` (text + binary cells).
///
/// `tz` is the session time zone (`EvalCtx::time_zone`) used for `Timestamptz`
/// text rendering. Task 9 threads it from the per-statement `EvalCtx`; a
/// UTC/epoch context reproduces prior behavior until the session builds it.
pub(crate) fn rows_result(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    tz: &jiff::tz::TimeZone,
) -> QueryResult {
    let rows: Vec<Vec<Option<Cell>>> = projected
        .iter()
        .map(|r| r.iter().map(|d| datum_to_cell(d, tz)).collect())
        .collect();
    let tag = format!("SELECT {}", rows.len());
    QueryResult::Rows { fields, rows, tag }
}

/// One resolved ORDER BY key for a plain SELECT. SQL92-style output references
/// (`ORDER BY 1`, `ORDER BY alias`) are represented as output indices; all other
/// expressions are evaluated against the source/group scope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectOrderKey {
    Output(usize),
    SourceExpr(Expr),
}

/// Resolve SELECT ORDER BY items using PostgreSQL's SQL92 rules:
/// integer constant -> output ordinal, bare output label -> output column, and
/// everything else -> source expression unless `require_output` is true.
pub(crate) fn resolve_select_order_keys(
    order_by: &[OrderItem],
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<Vec<SelectOrderKey>, ExecError> {
    order_by
        .iter()
        .map(|item| resolve_select_order_key(item, scope, fields, out_exprs, require_output))
        .collect()
}

fn resolve_select_order_key(
    item: &OrderItem,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<SelectOrderKey, ExecError> {
    if let Expr::IntLiteral(s) = &item.expr {
        let pos: i32 = s
            .parse()
            .map_err(|_| ExecError::Syntax("non-integer constant in ORDER BY".into()))?;
        if pos <= 0 || pos as usize > fields.len() {
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {pos} is not in select list"
            )));
        }
        return Ok(SelectOrderKey::Output(pos as usize - 1));
    }

    if let Expr::Column { table: None, name } = &item.expr
        && let Some(i) = output_label_index(scope, fields, out_exprs, name)?
    {
        return Ok(SelectOrderKey::Output(i));
    }

    if require_output {
        if let Some(i) = out_exprs
            .iter()
            .position(|e| order_output_exprs_equivalent(scope, e, &item.expr))
        {
            return Ok(SelectOrderKey::Output(i));
        }
        if let Expr::Column {
            table: Some(table),
            name,
        } = &item.expr
        {
            scope.resolve(Some(table), name)?;
        }
        return Err(ExecError::InvalidColumnReference(
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
        ));
    }

    Ok(SelectOrderKey::SourceExpr(item.expr.clone()))
}

fn output_label_index(
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    name: &str,
) -> Result<Option<usize>, ExecError> {
    let mut found = None;
    for (i, f) in fields.iter().enumerate() {
        if f.name == name {
            if let Some(prev) = found {
                if !order_output_exprs_equivalent(scope, &out_exprs[prev], &out_exprs[i]) {
                    return Err(ExecError::AmbiguousOrderBy(name.to_string()));
                }
            } else {
                found = Some(i);
            }
        }
    }
    Ok(found)
}

fn order_output_exprs_equivalent(scope: &Scope, a: &Expr, b: &Expr) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (
            Expr::Column {
                table: table_a,
                name: name_a,
            },
            Expr::Column {
                table: table_b,
                name: name_b,
            },
        ) => {
            let left = scope.resolve(table_a.as_deref(), name_a);
            let right = scope.resolve(table_b.as_deref(), name_b);
            matches!((left, right), (Ok(left), Ok(right)) if left == right)
        }
        _ => false,
    }
}

/// SP28: drop the first `offset` rows then keep at most `limit` (negative values
/// clamp to 0). Shared by the row and aggregate output paths.
pub(crate) fn apply_offset_limit<T>(rows: &mut Vec<T>, offset: Option<i64>, limit: Option<i64>) {
    if let Some(off) = offset {
        let n = usize::try_from(off.max(0))
            .unwrap_or(usize::MAX)
            .min(rows.len());
        rows.drain(0..n);
    }
    if let Some(limit) = limit {
        let n = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        rows.truncate(n);
    }
}

/// Expand the projection list into output FieldDescriptions, the expressions
/// that produce each column, and each column's `ColumnType` (the third element
/// lets `select_to_relation` build a derived table's output scope without
/// re-inferring types).
#[allow(clippy::type_complexity)]
pub(crate) fn resolve_projection(
    items: &[SelectItem],
    scope: &Scope,
) -> Result<(Vec<FieldDescription>, Vec<Expr>, Vec<ColumnType>), ExecError> {
    // SP33: expand each item in turn so `*` spans every FROM table and `a.*`
    // expands one qualifier. Each `*`-expanded column carries its qualifier so a
    // multi-table `*` re-resolves unambiguously via `scope.resolve`.
    let mut fields = Vec::new();
    let mut exprs = Vec::new();
    let mut tys = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                if scope.columns.is_empty() {
                    return Err(ExecError::Unsupported(
                        "SELECT * with no FROM clause is not supported".into(),
                    ));
                }
                for c in &scope.columns {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(Expr::Column {
                        table: c.qualifier.clone(),
                        name: c.name.clone(),
                    });
                    tys.push(c.ty);
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let cols: Vec<_> = scope
                    .columns
                    .iter()
                    .filter(|c| c.qualifier.as_deref() == Some(q))
                    .collect();
                if cols.is_empty() {
                    return Err(ExecError::MissingFromEntry(q.clone()));
                }
                for c in cols {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(Expr::Column {
                        table: c.qualifier.clone(),
                        name: c.name.clone(),
                    });
                    tys.push(c.ty);
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                let ty = crate::eval::infer_type(expr, scope)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
                tys.push(ty);
            }
        }
    }
    Ok((fields, exprs, tys))
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        // PostgreSQL names an aggregate output column after the function.
        Expr::Func(fc) => fc.name.clone(),
        _ => "?column?".to_string(),
    }
}

pub(crate) fn field(name: &str, ty: ColumnType) -> FieldDescription {
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

pub(crate) fn column_type_from_oid(oid: u32) -> Result<ColumnType, ExecError> {
    Ok(match oid {
        pgtypes::oids::BOOL => ColumnType::Bool,
        pgtypes::oids::INT4 => ColumnType::Int4,
        pgtypes::oids::INT8 => ColumnType::Int8,
        pgtypes::oids::TEXT => ColumnType::Text,
        pgtypes::oids::FLOAT8 => ColumnType::Float8,
        pgtypes::oids::NUMERIC => ColumnType::Numeric(None),
        pgtypes::oids::DATE => ColumnType::Date,
        pgtypes::oids::TIME => ColumnType::Time,
        pgtypes::oids::TIMESTAMP => ColumnType::Timestamp,
        pgtypes::oids::TIMESTAMPTZ => ColumnType::Timestamptz,
        pgtypes::oids::INTERVAL => ColumnType::Interval,
        _ => {
            return Err(ExecError::Unsupported(format!(
                "unknown query field type oid {oid}"
            )));
        }
    })
}

pub(crate) fn datum_to_cell(d: &Datum, tz: &jiff::tz::TimeZone) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    Some(Cell {
        text: Bytes::from(pgtypes::encoding::encode_text(d, tz)),
        binary: Bytes::from(pgtypes::encoding::encode_binary(d)),
    })
}

/// Compare two order-key vectors per the SELECT's ASC/DESC flags, with PG's
/// default null placement (NULLS LAST for ASC, NULLS FIRST for DESC).
pub(crate) fn order_cmp(
    a: &[Datum],
    b: &[Datum],
    order_by: &[pgparser::ast::OrderItem],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in order_by.iter().enumerate() {
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
    let Some(Statement::Query(q)) = statements.first() else {
        return Ok(Vec::new()); // non-SELECT (or empty) returns no row description
    };
    crate::query::describe_query_expr(catalog_kv, q)
}

#[cfg(test)]
mod tests {
    use crate::scope::{ColumnBinding, Scope};
    use crate::{SqlEngine, SqlSession};
    use pgparser::ast::{QueryBody, SelectStmt, SetExpr, Statement};
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
    async fn derived_table_in_from() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, v int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,10),(2,20),(3,30)").await;
        let r = &run(
            &engine,
            "SELECT d.s FROM (SELECT v + 1 AS s FROM t WHERE id > 1) d ORDER BY d.s",
        )
        .await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("21".into()), Some("31".into())]);
    }

    #[tokio::test]
    async fn join_against_a_derived_table() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, v int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,10),(2,20)").await;
        let r = &run(
            &engine,
            "SELECT t.id, d.mx FROM t JOIN (SELECT max(v) AS mx FROM t) d ON t.v = d.mx",
        )
        .await[0];
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn inner_join_on_equi_key() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4, av text)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(3,'a3')").await;
        run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3'),(4,'b4')").await;
        let r = &run(
            &engine,
            "SELECT a.av, b.bv FROM a JOIN b ON a.id = b.id ORDER BY a.id",
        )
        .await[0];
        let got: Vec<_> = rows_of(r)
            .iter()
            .map(|row| (text(&row[0]), text(&row[1])))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("a2".into()), Some("b2".into())),
                (Some("a3".into()), Some("b3".into()))
            ]
        );
    }

    #[tokio::test]
    async fn comma_form_is_a_cross_join_filtered_by_where() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4)").await;
        run(&engine, "INSERT INTO a VALUES (1),(2)").await;
        run(&engine, "INSERT INTO b VALUES (2),(3)").await;
        let r = &run(&engine, "SELECT a.id FROM a, b WHERE a.id = b.id").await[0];
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn self_join_requires_distinct_aliases() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, mgr int4)").await;
        run(&engine, "INSERT INTO t VALUES (1, NULL),(2, 1)").await;
        let r = &run(
            &engine,
            "SELECT e.id, m.id FROM t e JOIN t m ON e.mgr = m.id",
        )
        .await[0];
        // Only (employee 2 -> manager 1) matches: e.id=2, m.id=1.
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
        assert_eq!(text(&rows_of(r)[0][1]), Some("1".into()));
    }

    #[tokio::test]
    async fn unaliased_self_join_is_duplicate_alias_42712() {
        // The same qualifier on both sides of a join is rejected (PG 42712).
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let err = engine
            .connect()
            .simple_query("SELECT * FROM t JOIN t ON t.id = t.id")
            .await
            .expect_err("duplicate table name");
        assert_eq!(err.code, "42712");
    }

    #[tokio::test]
    async fn ambiguous_bare_column_is_42702() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4)").await;
        let err = engine
            .connect()
            .simple_query("SELECT id FROM a JOIN b ON a.id = b.id")
            .await
            .expect_err("ambiguous");
        assert_eq!(err.code, "42702");
    }

    #[tokio::test]
    async fn left_join_emits_nulls_for_unmatched() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1),(2)").await;
        run(&engine, "INSERT INTO b VALUES (2,'two')").await;
        let r = &run(
            &engine,
            "SELECT a.id, b.bv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
        )
        .await[0];
        let got: Vec<_> = rows_of(r)
            .iter()
            .map(|row| (text(&row[0]), text(&row[1])))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("1".into()), None),
                (Some("2".into()), Some("two".into())),
            ]
        );
    }

    #[tokio::test]
    async fn using_join_merges_the_key_column() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4, av text)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2')").await;
        run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3')").await;
        // SELECT * -> merged id first, then av, then bv.
        let r = &run(&engine, "SELECT * FROM a JOIN b USING (id)").await[0];
        assert_eq!(
            fields_of(r)
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "av", "bv"]
        );
        assert_eq!(rows_of(r).len(), 1);
        // Bare `id` is unambiguous after USING/NATURAL.
        let r2 = &run(&engine, "SELECT id FROM a NATURAL JOIN b").await[0];
        assert_eq!(rows_of(r2).len(), 1);
        assert_eq!(text(&rows_of(r2)[0][0]), Some("2".into()));
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

    #[tokio::test]
    async fn plain_select_order_by_position_and_alias_use_output() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4, name text)").await;
        run(
            &engine,
            "INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c')",
        )
        .await;

        let r = &run(&engine, "SELECT name FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("c".into()), Some("b".into()), Some("a".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY t.b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("2".into()), Some("1".into()), Some("3".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b + 0").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("2".into()), Some("1".into()), Some("3".into())]
        );
    }

    #[tokio::test]
    async fn plain_select_order_by_pg_error_surface() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(2,10)").await;

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 0")
            .await
            .expect_err("position zero");
        assert_eq!(err.code, "42P10");

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 999999999999999999999999999")
            .await
            .expect_err("overflow position");
        assert_eq!(err.code, "42601");

        let err = engine
            .connect()
            .simple_query("SELECT a AS x, b AS x FROM t ORDER BY x")
            .await
            .expect_err("ambiguous output label");
        assert_eq!(err.code, "42702");
    }

    #[tokio::test]
    async fn distinct_select_order_by_uses_output_only() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(1,10),(2,30)").await;

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY x DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let err = engine
            .connect()
            .simple_query("SELECT DISTINCT a FROM t ORDER BY b")
            .await
            .expect_err("source-only distinct key");
        assert_eq!(err.code, "42P10");
    }

    fn order_scope() -> Scope {
        Scope {
            columns: vec![
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "a".into(),
                    ty: pgtypes::ColumnType::Int4,
                },
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "b".into(),
                    ty: pgtypes::ColumnType::Int4,
                },
            ],
        }
    }

    fn parsed_select(sql: &str) -> SelectStmt {
        match pgparser::parse(sql).expect("parse").pop().expect("one") {
            Statement::Query(q) => match q.body {
                SetExpr::Query(QueryBody::Select(s)) => {
                    let mut s = *s;
                    s.order_by = q.order_by;
                    s.limit = q.limit;
                    s.offset = q.offset;
                    s.locking = q.locking;
                    s
                }
                other => panic!("expected select body, got {other:?}"),
            },
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn select_order_keys_resolve_positions_aliases_and_source_fallback() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let s = parsed_select("SELECT a AS x, b FROM t ORDER BY 1, x DESC, t.b, b + 0");
        let scope = order_scope();
        let (fields, out_exprs, _) =
            super::resolve_projection(&s.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&s.order_by, &scope, &fields, &out_exprs, false)
            .expect("order keys");

        assert!(matches!(keys[0], SelectOrderKey::Output(0)));
        assert!(matches!(keys[1], SelectOrderKey::Output(0)));
        assert!(matches!(keys[2], SelectOrderKey::SourceExpr(_)));
        assert!(matches!(keys[3], SelectOrderKey::SourceExpr(_)));
    }

    #[test]
    fn select_order_keys_report_pg_errors() {
        use super::resolve_select_order_keys;

        let scope = order_scope();

        let bad_pos = parsed_select("SELECT a FROM t ORDER BY 0");
        let (fields, out_exprs, _) =
            super::resolve_projection(&bad_pos.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&bad_pos.order_by, &scope, &fields, &out_exprs, false)
            .expect_err("bad position");
        assert_eq!(err.into_pg().code, "42P10");

        let overflow = parsed_select("SELECT a FROM t ORDER BY 999999999999999999999999999");
        let (fields, out_exprs, _) =
            super::resolve_projection(&overflow.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&overflow.order_by, &scope, &fields, &out_exprs, false)
            .expect_err("overflow");
        assert_eq!(err.into_pg().code, "42601");

        let i32_overflow = parsed_select("SELECT a FROM t ORDER BY 2147483648");
        let (fields, out_exprs, _) =
            super::resolve_projection(&i32_overflow.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&i32_overflow.order_by, &scope, &fields, &out_exprs, false)
                .expect_err("i32 overflow");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42601");
        assert_eq!(pg.message, "non-integer constant in ORDER BY");

        let duplicate = parsed_select("SELECT a AS x, b AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&duplicate.order_by, &scope, &fields, &out_exprs, false)
                .expect_err("ambiguous output label");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42702");
        assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
    }

    #[test]
    fn select_order_keys_allow_identical_duplicate_output_labels() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let scope = order_scope();

        let duplicate_same_expr = parsed_select("SELECT a, a FROM t ORDER BY a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate_same_expr.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(
            &duplicate_same_expr.order_by,
            &scope,
            &fields,
            &out_exprs,
            false,
        )
        .expect("identical duplicate output expressions are not ambiguous");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let duplicate_same_alias = parsed_select("SELECT a AS x, a AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate_same_alias.projection, &scope)
                .expect("projection");
        let keys = resolve_select_order_keys(
            &duplicate_same_alias.order_by,
            &scope,
            &fields,
            &out_exprs,
            false,
        )
        .expect("identical duplicate output aliases are not ambiguous");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);
    }

    #[test]
    fn select_distinct_order_keys_require_output_columns() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let scope = order_scope();

        let by_alias = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_alias.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&by_alias.order_by, &scope, &fields, &out_exprs, true)
            .expect("alias is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let by_select_expr = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_select_expr.projection, &scope).expect("projection");
        let keys =
            resolve_select_order_keys(&by_select_expr.order_by, &scope, &fields, &out_exprs, true)
                .expect("select-list expression is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let by_qualified_select_expr = parsed_select("SELECT DISTINCT a FROM t ORDER BY t.a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_qualified_select_expr.projection, &scope)
                .expect("projection");
        let keys = resolve_select_order_keys(
            &by_qualified_select_expr.order_by,
            &scope,
            &fields,
            &out_exprs,
            true,
        )
        .expect("qualified select-list expression is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let missing_qualifier = parsed_select("SELECT DISTINCT a FROM t ORDER BY nope.a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&missing_qualifier.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(
            &missing_qualifier.order_by,
            &scope,
            &fields,
            &out_exprs,
            true,
        )
        .expect_err("missing qualified table");
        assert_eq!(err.into_pg().code, "42P01");

        let source_only = parsed_select("SELECT DISTINCT a FROM t ORDER BY b");
        let (fields, out_exprs, _) =
            super::resolve_projection(&source_only.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&source_only.order_by, &scope, &fields, &out_exprs, true)
                .expect_err("source-only key");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42P10");
        assert_eq!(
            pg.message,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
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
    async fn describe_set_op_returns_first_branch_fields() {
        // Schema-only: a set-op query reports the first branch's column name(s) and
        // the unified type, without executing.
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .describe("SELECT 1 AS x UNION SELECT 2")
            .await
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "x"); // name from the FIRST branch
    }

    #[tokio::test]
    async fn describe_set_op_unifies_branch_types() {
        // The Describe path must run cross-branch type unification: int4 ∪ int8 → int8.
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .describe("SELECT 1 AS x UNION SELECT 2::int8")
            .await
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "x");
        assert_eq!(fields[0].type_oid, pgtypes::ColumnType::Int8.oid());
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
            foreign: None,
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

    // ───────────────────────── SP40 Task 14: pushdown ─────────────────────────

    mod pushdown {
        use std::sync::{Arc, Mutex};

        use catalog::{ForeignServer, Table, UserMapping};
        use pgtypes::Datum;
        use pgwire::engine::{Engine, QueryResult, Session};

        use crate::SqlEngine;
        use crate::clock::EvalCtx;
        use crate::error::ExecError;
        use crate::exec::extract_scan_bounds;
        use crate::foreign::{ForeignScanner, ImportFilter, ScanBounds};

        /// Parse `where_sql` into a WHERE [`Expr`] and run it through
        /// `extract_scan_bounds`. The argument is the predicate text only.
        fn bounds_of(where_sql: &str) -> ScanBounds {
            let expr = pgparser::parser::parse_expr_for_test(where_sql)
                .expect("the WHERE predicate parses");
            extract_scan_bounds(Some(&expr))
        }

        #[test]
        fn partition_and_lower_bound_pushes_inclusive_start() {
            let b = bounds_of("_partition = 0 AND _offset >= 10");
            assert_eq!(b.start_offsets, vec![(0, 10)]);
            assert!(b.end_offsets.is_empty());
        }

        #[test]
        fn partition_and_upper_strict_pushes_exclusive_end() {
            // `_offset < 50` → exclusive end 50 (unchanged).
            let b = bounds_of("_partition = 1 AND _offset < 50");
            assert!(b.start_offsets.is_empty());
            assert_eq!(b.end_offsets, vec![(1, 50)]);
        }

        #[test]
        fn between_pushes_inclusive_start_and_exclusive_end_plus_one() {
            // BETWEEN bounds are inclusive: [5, 9] → start 5, exclusive end 10.
            let b = bounds_of("_partition = 2 AND _offset BETWEEN 5 AND 9");
            assert_eq!(b.start_offsets, vec![(2, 5)]);
            assert_eq!(b.end_offsets, vec![(2, 10)]);
        }

        #[test]
        fn strict_lower_and_inclusive_upper_apply_exclusivity_correctly() {
            // `_offset > 7` → start 8; `_offset <= 20` → exclusive end 21.
            let b = bounds_of("_partition = 3 AND _offset > 7 AND _offset <= 20");
            assert_eq!(b.start_offsets, vec![(3, 8)]);
            assert_eq!(b.end_offsets, vec![(3, 21)]);
        }

        #[test]
        fn reversed_operand_order_is_normalized() {
            // `10 <= _offset` ≡ `_offset >= 10`; `50 > _offset` ≡ `_offset < 50`.
            let b = bounds_of("_partition = 0 AND 10 <= _offset AND 50 > _offset");
            assert_eq!(b.start_offsets, vec![(0, 10)]);
            assert_eq!(b.end_offsets, vec![(0, 50)]);
        }

        #[test]
        fn timestamp_predicate_is_not_pushed() {
            // `_timestamp` cannot be represented in ScanBounds — stays residual.
            let b = bounds_of("_partition = 0 AND _timestamp > '2020-01-01'");
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn non_envelope_predicate_is_not_pushed() {
            let b = bounds_of("_partition = 0 AND id = 42");
            // The partition anchor exists but no offset bound → empty bounds.
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn bare_offset_without_partition_is_not_pushed() {
            // No `_partition =` to scope the offset to → cannot push.
            let b = bounds_of("_offset >= 10");
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn no_filter_yields_default_bounds() {
            assert_eq!(extract_scan_bounds(None), ScanBounds::default());
        }

        /// A scanner that RECORDS every `ScanBounds` it is handed and returns a
        /// fixed corpus of rows IGNORING the bounds — so a result-equivalence test
        /// proves the residual WHERE still filters, and a recording test proves the
        /// pushed bounds reached the scan.
        struct RecordingScanner {
            seen: Arc<Mutex<Vec<ScanBounds>>>,
            /// Fixed (partition, offset, value) corpus, returned verbatim.
            corpus: Vec<(i32, i64, i64)>,
        }

        impl ForeignScanner for RecordingScanner {
            fn scan(
                &self,
                table: &Table,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                bounds: &ScanBounds,
                _ctx: &EvalCtx,
            ) -> Result<Vec<Vec<Datum>>, ExecError> {
                self.seen.lock().expect("lock").push(bounds.clone());
                // Envelope columns then one value column `v`; deliberately ignore
                // `bounds` to prove the residual WHERE re-filters.
                assert_eq!(table.columns.len(), 6, "5 envelope cols + value `v`");
                Ok(self
                    .corpus
                    .iter()
                    .map(|&(p, off, v)| {
                        vec![
                            Datum::Int4(p),
                            Datum::Int8(off),
                            Datum::Null, // _timestamp
                            Datum::Null, // _key
                            Datum::Null, // _headers
                            Datum::Int8(v),
                        ]
                    })
                    .collect())
            }

            fn import_schema(
                &self,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                _filter: &ImportFilter,
            ) -> Result<Vec<crate::foreign::ImportedTable>, ExecError> {
                Ok(Vec::new())
            }
        }

        async fn seed_engine(
            corpus: Vec<(i32, i64, i64)>,
        ) -> (SqlEngine, Arc<Mutex<Vec<ScanBounds>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut engine = SqlEngine::new();
            engine.set_foreign_scanner(Arc::new(RecordingScanner {
                seen: Arc::clone(&seen),
                corpus,
            }));
            {
                let mut s = engine.connect();
                s.simple_query(
                    "CREATE SERVER k FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'b:9092')",
                )
                .await
                .expect("create server");
                s.simple_query("CREATE FOREIGN TABLE f (v int8) SERVER k OPTIONS (topic 'topic')")
                    .await
                    .expect("create foreign table");
            }
            (engine, seen)
        }

        fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<pgwire::engine::Cell>>> {
            match r {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected Rows, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn single_foreign_table_pushes_recorded_bounds() {
            let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
            let mut s = engine.connect();
            s.simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10")
                .await
                .expect("scan ok");
            let recorded = seen.lock().expect("lock");
            assert_eq!(recorded.len(), 1, "exactly one scan");
            assert_eq!(
                recorded[0].start_offsets,
                vec![(0, 10)],
                "the `_partition = 0 AND _offset >= 10` slice was pushed into the scan"
            );
        }

        #[tokio::test]
        async fn full_scan_when_no_pushable_predicate() {
            let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
            let mut s = engine.connect();
            // A bare-offset predicate is NOT pushable → default (full) bounds.
            s.simple_query("SELECT v FROM f WHERE _offset >= 10")
                .await
                .expect("scan ok");
            let recorded = seen.lock().expect("lock");
            assert_eq!(recorded.len(), 1);
            assert_eq!(
                recorded[0],
                ScanBounds::default(),
                "an unanchored offset stays residual → full scan"
            );
        }

        #[tokio::test]
        async fn pushdown_does_not_change_results() {
            // The scanner returns rows OUTSIDE the pushed slice (offsets 5 and 10,
            // partitions 0 and 1) and ignores bounds; the residual WHERE must still
            // yield exactly the rows passing the full predicate.
            let corpus = vec![
                (0, 5, 50),   // _offset 5 < 10 → excluded by WHERE
                (0, 10, 100), // partition 0, offset 10, v=100 → kept
                (0, 12, 7),   // v=7, fails `v > 50` → excluded by WHERE
                (1, 10, 200), // partition 1 → excluded by `_partition = 0`
            ];
            let (engine, seen) = seed_engine(corpus).await;
            let mut s = engine.connect();
            let res = s
                .simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10 AND v > 50")
                .await
                .expect("scan ok");
            // Only the (0,10,100) row passes the full predicate.
            let rows = rows_of(&res[0]);
            let got: Vec<_> = rows
                .iter()
                .map(|row| {
                    String::from_utf8(row[0].as_ref().expect("v not null").text.to_vec())
                        .expect("utf8")
                })
                .collect();
            assert_eq!(got, vec!["100".to_string()], "residual WHERE still applied");
            // And the bounds were pushed (proves it is a real pushdown, not a no-op).
            assert_eq!(seen.lock().expect("lock")[0].start_offsets, vec![(0, 10)]);
        }
    }
}
