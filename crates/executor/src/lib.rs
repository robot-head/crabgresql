//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. SP5 swaps SP4's commit_ts MVCC for PostgreSQL's
//! xid/clog/snapshot model with uncommitted versions on disk. SP6 removes the
//! global writer lock: writers run concurrently, serialized only at the row
//! level via the `RowLockManager`, with rowid allocation via the
//! `SequenceManager` and DDL serialized behind a small catalog lock.

mod commit;
mod error;
mod eval;
mod exec;
mod gtm;
mod lockmgr;
mod procarray;
mod read_gate;
mod seq;
mod session;

use std::path::Path;
use std::sync::Arc;

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;

pub use commit::{Committer, LocalCommitter};
pub use error::ExecError;
pub use read_gate::{Linearizer, LocalLinearizer};
pub use session::SqlSession;

use crate::lockmgr::RowLockManager;
use crate::procarray::ProcArray;
use crate::seq::SequenceManager;

/// Whether the counter managers (`ProcArray`, `SequenceManager`) persist their
/// counters themselves (`Durable` — the local/single-node path) or fold the
/// counter advance into the commit batch for the replicated state machine to
/// max-merge (`Replicated` — the Raft path, reseeded on leadership change).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistMode {
    Durable,
    Replicated,
}

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. Writers run concurrently
/// (SP6): row-level conflicts serialize through the `RowLockManager`, rowid
/// allocation goes through the `SequenceManager`, and DDL serializes among DDLs
/// behind `catalog_lock`. The `ProcArray` is shared so every connection's
/// snapshots see the same running-transaction set.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    /// The store catalog lookups (table name→id→schema) resolve through. For the
    /// single-range engine this is the same store as `kv`; under multi-range
    /// sharding the catalog lives only on range 0, so a data range's engine
    /// points this at range 0's store while `kv` holds its own rows.
    pub(crate) catalog_kv: Arc<dyn Kv>,
    pub(crate) procarray: Arc<ProcArray>,
    pub(crate) seq: Arc<SequenceManager>,
    pub(crate) lockmgr: Arc<RowLockManager>,
    pub(crate) catalog_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) committer: Arc<dyn crate::commit::Committer>,
    pub(crate) linearizer: Arc<dyn crate::read_gate::Linearizer>,
    pub(crate) persist_mode: PersistMode,
    /// Range 0's Global Transaction Manager. `Some` on every range engine of a
    /// multi-range cluster (injected by the cluster after construction); `None`
    /// on a single-range engine. Single-range behavior is byte-for-byte unchanged
    /// when `gtm` is `None`.
    pub(crate) gtm: Option<Arc<gtm::Gtm>>,
    /// A range-0 read barrier, injected by the cluster on every DATA-range engine
    /// (range != 0) of a multi-range node. Before a cross-range resolver reads
    /// range 0's global clog, this catches the node's LOCAL range-0 replica up to
    /// range 0's linearizable applied index. `None` on range 0's own engine (it
    /// reads its own current store) and on single-range engines.
    pub(crate) range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new())).expect("in-memory engine never fails to open")
    }

    /// Durable engine backed by a fjall store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Self::with_kv(Arc::new(FjallKv::open(path)?))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(Arc::clone(&kv), PersistMode::Durable)?);
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            });
        Ok(Self {
            catalog_kv: Arc::clone(&kv),
            kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Durable)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
            linearizer: Arc::new(crate::read_gate::LocalLinearizer),
            persist_mode: PersistMode::Durable,
            gtm: None,
            range0_barrier: None,
        })
    }

    /// Build an engine whose reads come from `sm_kv` (the applied state machine)
    /// and whose writes are proposed through `committer` (a RaftCommitter). Uses
    /// the Replicated persist mode so counters fold into the proposed batch.
    ///
    /// `catalog_kv` is the store catalog (schema) lookups resolve through. For a
    /// single-range node it is the same `Arc` as `sm_kv`; a multi-range data
    /// node passes range 0's applied store here while `sm_kv` holds its own rows.
    pub fn replicated(
        catalog_kv: Arc<dyn Kv>,
        sm_kv: Arc<dyn Kv>,
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
    ) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(
            Arc::clone(&sm_kv),
            PersistMode::Replicated,
        )?);
        Ok(Self {
            catalog_kv,
            kv: sm_kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Replicated)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
            linearizer,
            persist_mode: PersistMode::Replicated,
            gtm: None,
            range0_barrier: None,
        })
    }

    /// Reseed counters from the applied store (call when this node becomes leader).
    pub fn reseed_counters(&self) -> Result<(), ExecError> {
        self.procarray.reseed_from_applied()?;
        self.seq.reseed_from_applied();
        Ok(())
    }

    /// A second handle to the SAME engine (all fields are `Arc`/`Copy`): every
    /// clone shares the applied store, committer, linearizer, and counters.
    /// Used by the gateway to give each connection its own router without
    /// re-opening the engine.
    pub fn clone_handle(&self) -> SqlEngine {
        SqlEngine {
            kv: Arc::clone(&self.kv),
            catalog_kv: Arc::clone(&self.catalog_kv),
            procarray: Arc::clone(&self.procarray),
            seq: Arc::clone(&self.seq),
            lockmgr: Arc::clone(&self.lockmgr),
            catalog_lock: Arc::clone(&self.catalog_lock),
            committer: Arc::clone(&self.committer),
            linearizer: Arc::clone(&self.linearizer),
            persist_mode: self.persist_mode,
            gtm: self.gtm.as_ref().map(Arc::clone),
            range0_barrier: self.range0_barrier.as_ref().map(Arc::clone),
        }
    }

    /// Open a GTM over this engine's `kv` (range 0's store) and make this engine
    /// the GTM coordinator. Called once on range 0's engine by the cluster during
    /// construction, before `share_gtm_to` distributes the same `Arc` to every
    /// other range engine.
    pub fn init_gtm_coordinator(&mut self) -> Result<(), ExecError> {
        let g = Arc::new(gtm::Gtm::open(Arc::clone(&self.kv))?);
        self.gtm = Some(g);
        Ok(())
    }

    /// Copy this engine's `Arc<Gtm>` into `other`. Both engines then share the same
    /// GTM — any range can resolve a `Prepared` row and the coordinator can drive
    /// range 0. `self` must have been initialized via `init_gtm_coordinator` first;
    /// `other` can be any range's engine.
    pub fn share_gtm_to(&self, other: &mut SqlEngine) {
        other.gtm = self.gtm.as_ref().map(Arc::clone);
    }

    /// Inject a range-0 read barrier on this (data-range) engine. Called by the
    /// cluster on every range != 0 engine so its cross-range resolver reads a
    /// caught-up range-0 replica. Range 0's own engine needs no barrier.
    pub fn set_range0_barrier(&mut self, b: Arc<dyn crate::read_gate::Linearizer>) {
        self.range0_barrier = Some(b);
    }

    /// Whether this engine carries the shared GTM (so `begin_global_durable` and
    /// global-decision methods are available). `true` on range 0's engine in any
    /// multi-range configuration; `false` on a single-range engine.
    pub fn has_gtm(&self) -> bool {
        self.gtm.is_some()
    }

    /// Allocate a global (cross-range) txn id. Coordinator-only (range 0's engine).
    pub fn begin_global(&self) -> u64 {
        self.gtm
            .as_ref()
            .expect("begin_global on a non-GTM engine")
            .begin_global()
    }

    /// Durably allocate a global xid: bump the in-memory counter, then persist
    /// `next_global` through range 0's committer BEFORE returning, so any later
    /// range-0 leader reseeds past `g` and a global xid is never reused across a
    /// range-0 leader change. Only succeeds on range 0's leader (the committer
    /// rejects non-leaders -> ExecError::NotLeader).
    pub async fn begin_global_durable(&self) -> Result<u64, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("begin_global_durable on a non-GTM engine");
        let g = gtm.begin_global();
        self.committer
            .commit(vec![gtm.next_global_xid_op()])
            .await?;
        Ok(g)
    }

    /// Lift the GTM's in-memory `next_global` to the durable value (never
    /// regresses). Called on the range-0 leadership rising edge.
    pub fn reseed_gtm(&self) -> Result<(), ExecError> {
        if let Some(gtm) = self.gtm.as_ref() {
            gtm.reseed_from_applied()?;
        }
        Ok(())
    }

    /// Durably record the global decision (Committed/Aborted) for `g` in range 0's
    /// group, folding the global next-id advance. The atomic commit instant.
    pub async fn commit_global_decision(
        &self,
        g: u64,
        status: mvcc::clog::XidStatus,
    ) -> Result<mvcc::clog::XidStatus, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("commit_global_decision on a non-GTM engine");
        self.committer
            .commit(vec![
                mvcc::clog::put_op(g, status),
                gtm.next_global_xid_op(),
            ])
            .await?;
        // Write-once: apply keeps any prior terminal decision, so the EFFECTIVE
        // decision (what is actually recorded) may differ from `status` if a
        // participant won an abort-race. `commit` guarantees applied-on-leader, and
        // `self.kv` is range 0's applied store, so this read-back is authoritative.
        Ok(mvcc::clog::get(self.kv.as_ref(), g)?)
    }

    /// Scan THIS range's clog from `scan_lo` for in-doubt `Prepared(Li -> g)` markers.
    /// Returns `(in_doubt_gs, new_scan_lo)` where `new_scan_lo` is the smallest scanned
    /// `Li` whose `g` is NOT durably terminal (so it must keep being swept), or one past
    /// the largest scanned `Li` if every scanned marker is terminal (or `scan_lo` if the
    /// range is empty). `new_scan_lo` NEVER passes a non-terminal `g` — the recovery
    /// (zombie-commit) safety invariant. Markers are never deleted.
    ///
    /// The decidedness check reads `self.catalog_kv` directly (NOT through the range-0
    /// read barrier), so on a lagging local range-0 replica an already-decided `g` may
    /// be reported in-doubt. That is harmless: the recovery sweep merely abort-races
    /// `g` to range 0, and the decision is WRITE-ONCE — racing an already-terminal `g`
    /// is a no-op against the real decision. Do not "fix" this by routing through the
    /// barrier; the staleness is intentional and adds no latency to the hot path.
    pub async fn in_doubt_globals_from(&self, scan_lo: u64) -> Result<(Vec<u64>, u64), ExecError> {
        use std::collections::BTreeSet;
        let mut gs: BTreeSet<u64> = BTreeSet::new();
        let mut first_undecided: Option<u64> = None;
        let mut max_li: Option<u64> = None;
        for (k, v) in self
            .kv
            .scan_range(&kv::key::clog_key(scan_lo), &kv::key::clog_scan_end())?
        {
            let Some(li) = kv::key::clog_xid_of(&k) else {
                continue;
            };
            max_li = Some(li);
            if let mvcc::clog::XidStatus::Prepared(g) = mvcc::clog::decode(&v)? {
                let terminal = matches!(
                    mvcc::clog::get(self.catalog_kv.as_ref(), g)?,
                    mvcc::clog::XidStatus::Committed | mvcc::clog::XidStatus::Aborted
                );
                if !terminal {
                    gs.insert(g);
                    first_undecided.get_or_insert(li);
                }
            }
        }
        let new_scan_lo = first_undecided
            // `max_li` is a local `Li < GLOBAL_XID_BASE` on a real data range, so this
            // never saturates; `saturating_add` is belt-and-suspenders.
            .or_else(|| max_li.map(|m| m.saturating_add(1)))
            .unwrap_or(scan_lo)
            .max(scan_lo); // monotone
        Ok((gs.into_iter().collect(), new_scan_lo))
    }

    /// Back-compat: the full-scan in-doubt set (callers that don't track a watermark).
    pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError> {
        Ok(self.in_doubt_globals_from(0).await?.0)
    }

    /// Scan THIS range's clog (from the recovery watermark) for an existing durable
    /// `Prepared(Li -> g)` marker for the given in-doubt global xid `g`; return the local
    /// xid `Li` of the first such marker, or `None`.
    ///
    /// Makes participant `Stage` IDEMPOTENT per `(g, range)`. A `Stage(g)` RPC retried across
    /// a participant-leader failover (the original leader durably staged then died; the retry
    /// lands on the new leader, whose in-memory held-session map is empty) must NOT write a
    /// SECOND `Prepared(-> g)` version of the row. The first attempt's marker was
    /// Raft-committed before the old leader died, so the new leader — which won election with
    /// that entry in its log — finds it here and the retry becomes a no-op. Bounded by the
    /// watermark: an in-doubt `g`'s marker is never below `clog_scan_lo` (the watermark never
    /// advances past a non-terminal `g`).
    pub async fn staged_local_for(&self, g: u64) -> Result<Option<u64>, ExecError> {
        let scan_lo = self.clog_scan_lo()?;
        for (k, v) in self
            .kv
            .scan_range(&kv::key::clog_key(scan_lo), &kv::key::clog_scan_end())?
        {
            let Some(li) = kv::key::clog_xid_of(&k) else {
                continue;
            };
            if let mvcc::clog::XidStatus::Prepared(pg) = mvcc::clog::decode(&v)?
                && pg == g
            {
                return Ok(Some(li));
            }
        }
        Ok(None)
    }

    /// Read this range's durable recovery-scan watermark (`0` if absent/unset).
    pub fn clog_scan_lo(&self) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::clog_scan_lo_key())? {
            Some(b) if b.len() == 8 => Ok(u64::from_be_bytes(b[..8].try_into().expect("8 bytes"))),
            _ => Ok(0),
        }
    }

    /// Durably advance this range's recovery-scan watermark (monotone; a no-op if `lo`
    /// is not greater than the current value). Proposed through the range committer.
    ///
    /// The read-then-write is NOT a CAS: monotonicity relies on the single-writer
    /// discipline of the edge-triggered per-range leadership-rise sweep (one advance at a
    /// time). Even a hypothetical interleaving that regressed the value low is
    /// correctness-preserving — a lower watermark only enlarges the next scan, never skips
    /// an in-doubt marker.
    pub async fn advance_clog_scan_lo(&self, lo: u64) -> Result<(), ExecError> {
        if lo <= self.clog_scan_lo()? {
            return Ok(());
        }
        self.committer
            .commit(vec![kv::store::WriteOp::Put {
                key: kv::key::clog_scan_lo_key(),
                value: lo.to_be_bytes().to_vec(),
            }])
            .await
    }

    /// Deregister a decided global txn from the in-memory running-set.
    pub fn finish_global(&self, g: u64) {
        self.gtm
            .as_ref()
            .expect("finish_global on a non-GTM engine")
            .finish_global(g);
    }
}

/// A sentinel global snapshot for single-range (non-GTM) engines. Any global xid
/// `g >= xmax` is treated as InProgress by the resolver, but no `Prepared` tuples
/// ever exist on a single-range engine so the Prepared branch is unreachable.
#[allow(non_snake_case)]
pub(crate) fn NO_GLOBAL_SNAPSHOT() -> mvcc::visibility::Snapshot {
    use mvcc::xid::GLOBAL_XID_BASE;
    mvcc::visibility::Snapshot {
        xmin: GLOBAL_XID_BASE,
        xmax: GLOBAL_XID_BASE,
        xip: vec![],
    }
}

/// Field descriptions for `sql` resolving schema from `catalog_kv`, without a
/// data store or execution (the gateway's Describe only needs the catalog).
pub fn describe_fields(
    catalog_kv: &dyn Kv,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    crate::exec::describe(catalog_kv, catalog_kv, sql)
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.procarray),
            Arc::clone(&self.seq),
            Arc::clone(&self.lockmgr),
            Arc::clone(&self.catalog_lock),
            Arc::clone(&self.committer),
            Arc::clone(&self.linearizer),
            self.persist_mode,
            self.gtm.as_ref().map(Arc::clone),
            self.range0_barrier.as_ref().map(Arc::clone),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_lists_undecided_prepared_markers() {
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::xid::GLOBAL_XID_BASE;
        // Single-store in-memory engine: `self.kv == self.catalog_kv` (both are the
        // same `Arc` per `with_kv`), so `MemKv` here is range 0's global clog too.
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let g_undecided = GLOBAL_XID_BASE + 1;
        let g_committed = GLOBAL_XID_BASE + 2;
        // Two local participants prepared into two global xids.
        kv.write_batch(&[put_op(11, XidStatus::Prepared(g_undecided))])
            .expect("p1");
        kv.write_batch(&[put_op(12, XidStatus::Prepared(g_committed))])
            .expect("p2");
        // g_committed is decided; g_undecided is not.
        kv.write_batch(&[put_op(g_committed, XidStatus::Committed)])
            .expect("decide");
        let mut got = engine.in_doubt_globals().await.expect("scan");
        got.sort();
        assert_eq!(
            got,
            vec![g_undecided],
            "only undecided Prepared markers are returned"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn staged_local_for_finds_an_existing_prepared_marker() {
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::xid::GLOBAL_XID_BASE;
        // Single-store in-memory engine: `self.kv == self.catalog_kv` (both the same `Arc`
        // per `with_kv`), so `MemKv` here is this range's local clog.
        let kv = Arc::new(MemKv::new());
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let g = GLOBAL_XID_BASE + 1;
        // A durable Prepared(Li=11 -> g) marker exists on this range.
        kv.write_batch(&[put_op(11, XidStatus::Prepared(g))])
            .expect("stage marker");
        assert_eq!(
            engine.staged_local_for(g).await.expect("scan"),
            Some(11),
            "finds the existing Prepared(-> g) marker's local xid"
        );
        assert_eq!(
            engine
                .staged_local_for(GLOBAL_XID_BASE + 2)
                .await
                .expect("scan"),
            None,
            "no marker for a different global xid"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_from_bounds_the_scan_and_advances_past_terminal() {
        use mvcc::clog::{XidStatus, put_op};
        use mvcc::xid::GLOBAL_XID_BASE;
        // Two stores: sm_kv = this data range's local clog; catalog_kv = range 0's global-G clog.
        let sm_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let catalog_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&sm_kv),
        });
        let linearizer = std::sync::Arc::new(crate::read_gate::LocalLinearizer);
        let engine = SqlEngine::replicated(
            std::sync::Arc::clone(&catalog_kv),
            std::sync::Arc::clone(&sm_kv),
            committer,
            linearizer,
        )
        .expect("engine");

        let (g_term, g_doubt) = (GLOBAL_XID_BASE + 1, GLOBAL_XID_BASE + 2);
        // Local markers at Li = 10 (terminal G), 11 (in-doubt G), 12 (terminal G) — sm_kv ONLY.
        sm_kv
            .write_batch(&[put_op(10, XidStatus::Prepared(g_term))])
            .expect("p10");
        sm_kv
            .write_batch(&[put_op(11, XidStatus::Prepared(g_doubt))])
            .expect("p11");
        sm_kv
            .write_batch(&[put_op(12, XidStatus::Prepared(g_term))])
            .expect("p12");
        // Global decisions — catalog_kv ONLY.
        catalog_kv
            .write_batch(&[put_op(g_term, XidStatus::Committed)])
            .expect("decide g_term");
        // from(0): only g_doubt is in-doubt; watermark stops at the in-doubt Li (11).
        let (gs, lo) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(gs, vec![g_doubt]);
        assert_eq!(lo, 11, "watermark = smallest in-doubt Li");
        // Decide g_doubt; from(11) finds nothing in-doubt -> watermark = one past the largest local Li (12).
        catalog_kv
            .write_batch(&[put_op(g_doubt, XidStatus::Aborted)])
            .expect("decide g_doubt");
        let (gs2, lo2) = engine.in_doubt_globals_from(11).await.expect("scan");
        assert!(gs2.is_empty());
        assert_eq!(
            lo2, 13,
            "all terminal -> watermark = one past the largest local Li (12)"
        );
        // Edge: scan_lo above all markers -> empty scan -> watermark unchanged.
        assert_eq!(engine.in_doubt_globals_from(99).await.expect("scan").1, 99);
        // Edge: an in-doubt marker at the HIGHEST Li (terminals below) holds the
        // watermark exactly there (the ascending scan stops at the first undecided).
        sm_kv
            .write_batch(&[put_op(20, XidStatus::Prepared(GLOBAL_XID_BASE + 3))])
            .expect("high in-doubt marker");
        let (gs3, lo3) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(gs3, vec![GLOBAL_XID_BASE + 3]);
        assert_eq!(
            lo3, 20,
            "in-doubt at the highest Li holds the watermark there"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clog_scan_lo_persists_and_is_monotone() {
        let sm_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let catalog_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&sm_kv),
        });
        let linearizer = std::sync::Arc::new(crate::read_gate::LocalLinearizer);
        let engine = SqlEngine::replicated(
            catalog_kv,
            std::sync::Arc::clone(&sm_kv),
            committer,
            linearizer,
        )
        .expect("engine");
        assert_eq!(engine.clog_scan_lo().expect("lo"), 0); // absent -> 0
        engine.advance_clog_scan_lo(5).await.expect("advance");
        assert_eq!(engine.clog_scan_lo().expect("lo"), 5);
        engine.advance_clog_scan_lo(3).await.expect("no-op"); // lower -> no-op
        assert_eq!(engine.clog_scan_lo().expect("lo"), 5, "monotone");
    }
}
