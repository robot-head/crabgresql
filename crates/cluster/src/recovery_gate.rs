//! Per-range, term-based recovery gate for settle-before-serve (SP22). A newly-risen
//! range leader must not serve WRITES for that range until its leadership-rise in-doubt
//! sweep has settled every inherited `Prepared(-> g)` marker. A write to range R is
//! admitted only when this node leads R AND R's last-settled term equals R's CURRENT Raft
//! term — derived atomically from the term, so there is no rise-edge race. `served_term`
//! starts at the sentinel 0; a node that won an election is at term >= 1, so a range is
//! gated-by-default on every fresh rise until its sweep calls `mark_served`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};

/// Sentinel below any real Raft leadership term (a won election is term >= 1).
const UNSETTLED: u64 = 0;

/// Per-range state: this range's Raft handle plus the last term whose rise sweep
/// completed (the `served_term`, shared so `mark_served` can publish it lock-free).
type RangeState = (openraft::Raft<TypeConfig>, Arc<AtomicU64>);

pub struct RecoveryGate {
    /// Per range: (raft handle, last term whose rise sweep completed). Growable via
    /// `register_range` (copy-on-write) so the replicated bring-up — which constructs the
    /// gate before its data ranges exist — can add ranges as their Raft groups come up,
    /// exactly like `TxnService`'s engines map.
    ranges: ArcSwap<HashMap<RangeId, RangeState>>,
    id: NodeId,
}

impl RecoveryGate {
    pub fn new(id: NodeId) -> Arc<Self> {
        Arc::new(Self {
            ranges: ArcSwap::from_pointee(HashMap::new()),
            id,
        })
    }

    /// Register a range's Raft handle (gated-by-default). Idempotent: re-registering keeps
    /// the existing `served_term` Arc. Copy-on-write so lock-free `is_serving` readers never
    /// block.
    pub fn register_range(&self, range: RangeId, raft: openraft::Raft<TypeConfig>) {
        self.ranges.rcu(|cur| {
            let mut m = (**cur).clone();
            m.entry(range)
                .or_insert_with(|| (raft.clone(), Arc::new(AtomicU64::new(UNSETTLED))));
            m
        });
    }

    /// True iff this node currently leads `range` AND its rise sweep has settled the current
    /// term. A range not registered here is "not this node's concern" → `true` (such a write
    /// rejects via the normal not-local-leader path instead). Re-reads the LIVE term every
    /// call so a leadership flap re-closes the gate until the new term is settled.
    pub fn is_serving(&self, range: RangeId) -> bool {
        let ranges = self.ranges.load();
        let Some((raft, served)) = ranges.get(&range) else {
            return true;
        };
        let (leader, term) = {
            let m = raft.metrics();
            let m = m.borrow();
            (m.current_leader, m.current_term)
        };
        leader == Some(self.id) && served.load(Ordering::Acquire) == term
    }

    /// Open the gate for `range` at `term` — called by the rise sweep AFTER it has settled
    /// every inherited in-doubt marker for `term`. A no-op for an unregistered range.
    pub fn mark_served(&self, range: RangeId, term: u64) {
        if let Some((_, served)) = self.ranges.load().get(&range) {
            served.store(term, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gate_is_closed_at_a_fresh_term_and_opens_on_mark_served() {
        // A single-node 2-range ServerNode: this node leads every range at a stable term >= 1.
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let term = node.rafts[&1].metrics().borrow().current_term;
        assert!(term >= 1, "a leader is at term >= 1");

        let gate = RecoveryGate::new(node.id());
        gate.register_range(1, node.rafts[&1].clone());

        // Gated-by-default: the sentinel served_term (0) != the live term, even though we lead.
        assert!(
            !gate.is_serving(1),
            "a freshly-registered range is gated until its rise sweep settles the term"
        );
        // The rise sweep opens it for this term.
        gate.mark_served(1, term);
        assert!(
            gate.is_serving(1),
            "writes are admitted once the term is settled"
        );

        // An unregistered range is not this gate's concern.
        assert!(gate.is_serving(999), "a non-hosted range is not gated here");
    }
}
