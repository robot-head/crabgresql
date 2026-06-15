//! Exhaustive Stateright model of the **linearizable-read / ReadIndex** safety invariant
//! (SP24): a deposed or partitioned former leader must not serve a read from its stale
//! local applied state. A served read never regresses below a value a client has already
//! observed.
//!
//! ## The real mechanism this guards
//!
//! Reads in crabgresql are served from a node's *locally-applied* state machine
//! (`sm_kv`), never from the Raft log directly. Before a read takes its MVCC snapshot,
//! the [`Linearizer`] seam (`crates/executor/src/read_gate.rs`) must confirm it is safe to
//! observe local state. The replicated impl `cluster::RaftLinearizer`
//! (`crates/cluster/src/linearizer.rs`) — and the cross-range `Range0Barrier`
//! (`crates/cluster/src/twopc.rs`) — perform an openraft **ReadIndex** check: heartbeat a
//! quorum to confirm this node is STILL the leader, then block until the local state
//! machine has applied through the confirmed read index. On a leader that has been deposed
//! or network-partitioned, the quorum heartbeat fails (`QuorumNotEnough` / `ForwardToLeader`)
//! and the read is **rejected** (`ExecError::Unavailable` / `NotLeader`, both retryable)
//! rather than served from state that the rest of the cluster has moved past.
//!
//! ## The bug the gate prevents
//!
//! Without the ReadIndex check, a former leader that was partitioned away from the quorum
//! keeps serving reads from its frozen local applied state. Meanwhile the surviving
//! majority elects a new leader and commits further writes. A client that read the new
//! value from the fresh leader, then read the OLD value from the stale leader, observes
//! time going backwards — a linearizability violation (specifically a monotonic-read
//! violation: a value already acknowledged is later contradicted by an older one).
//!
//! ## The abstract model
//!
//! A monotone committed high-water mark (`committed`, the value a quorum has agreed on), a
//! possibly-partitioned former leader whose locally-applied value (`stale_applied`) freezes
//! the instant it is isolated while the quorum keeps committing, and the highest value any
//! acknowledged read has returned (`max_observed`). A `ReadLeader` is a quorum-confirmed
//! read and always returns `committed`. A `ReadStale` is the former leader serving a read:
//! GATED with the fix on (rejected while partitioned, because it cannot confirm quorum),
//! ungated with the fix off (serves its frozen `stale_applied`, which can be strictly below
//! `max_observed` — the stale read).
//!
//! `read_index_check = true` is the real system (a partitioned node's read is rejected);
//! `false` is the pre-gate bug (it serves stale local state). The teeth test proves the
//! checker CATCHES the stale read with the gate off; the positive test proves the invariant
//! HOLDS with it on. Mirrors the positive + teeth structure of the SP7 counter model in
//! `model.rs`.

use stateright::{Checker, Model, Property};

/// Cap the committed high-water mark so the BFS state space stays finite (combined with the
/// step budget). Two commits past a partition is enough to expose the stale read.
const MAX_COMMITTED: u64 = 2;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The value a quorum has committed (monotone — the real cluster only moves forward).
    committed: u64,
    /// The former leader's locally-applied value. While it is partitioned this is FROZEN
    /// (the quorum commits without it); while healthy it tracks `committed`.
    stale_applied: u64,
    /// Is the former leader currently partitioned from the quorum (cannot confirm leadership)?
    partitioned: bool,
    /// The highest value any acknowledged read has returned to a client. Monotone in the
    /// correct system; a read returning less than this is the stale read.
    max_observed: u64,
    /// Set once a read ever returns a value STRICTLY BELOW `max_observed` — a monotonic-read
    /// (linearizability) violation. The load-bearing safety flag.
    stale_read: bool,
    /// Set once a read is served by a partitioned former leader that never confirmed quorum
    /// leadership — the prevention-side invariant the ReadIndex gate maintains.
    partitioned_served: bool,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

/// `read_index_check` toggles the fix: `true` = a `ReadStale` by a partitioned former leader
/// is rejected (it cannot confirm quorum leadership); `false` = the pre-gate bug where it
/// serves its frozen local applied state.
struct LinearizableReadModel {
    max_steps: usize,
    read_index_check: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// A write commits on the surviving quorum: `committed += 1`. A partitioned former
    /// leader does NOT see it (its `stale_applied` stays frozen).
    Commit,
    /// The former leader is partitioned away from the quorum: its applied value freezes.
    Partition,
    /// The partition heals: the former leader catches up to `committed`.
    Heal,
    /// A quorum-confirmed read (from the current leader). Returns `committed`.
    ReadLeader,
    /// The former leader serves a read of its local applied state. GATED by the ReadIndex
    /// check: rejected while partitioned with the fix on.
    ReadStale,
}

impl Model for LinearizableReadModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            committed: 0,
            stale_applied: 0,
            partitioned: false,
            max_observed: 0,
            stale_read: false,
            partitioned_served: false,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        out.push(Action::Commit);
        out.push(Action::Partition);
        out.push(Action::Heal);
        out.push(Action::ReadLeader);
        out.push(Action::ReadStale);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Commit => {
                if n.committed >= MAX_COMMITTED {
                    return None; // bounded — no further commits
                }
                n.committed += 1;
                // A healthy (un-partitioned) node applies the new entry; a partitioned one
                // does not (its applied state is frozen at the value it held when isolated).
                if !n.partitioned {
                    n.stale_applied = n.committed;
                }
            }
            Action::Partition => {
                if n.partitioned {
                    return None; // already partitioned — no new state
                }
                // The former leader freezes at whatever it had applied (it was caught up).
                n.partitioned = true;
            }
            Action::Heal => {
                if !n.partitioned {
                    return None; // nothing partitioned — no new state
                }
                n.partitioned = false;
                n.stale_applied = n.committed; // catches up to the quorum
            }
            Action::ReadLeader => {
                // A quorum-confirmed read observes the committed high-water mark.
                if n.committed == n.max_observed {
                    return None; // observes nothing new — no state change
                }
                n.max_observed = n.committed;
            }
            Action::ReadStale => {
                // The ReadIndex gate: with the fix on, a partitioned former leader cannot
                // confirm quorum leadership, so its read is REJECTED — no value served.
                if self.read_index_check && n.partitioned {
                    return None;
                }
                if n.partitioned {
                    // The fix is OFF: it serves its frozen local applied state.
                    n.partitioned_served = true;
                    if n.stale_applied < n.max_observed {
                        // A value already acknowledged is contradicted by an older one.
                        n.stale_read = true;
                    }
                    n.max_observed = n.max_observed.max(n.stale_applied);
                } else {
                    // Healthy: caught up to the quorum, equivalent to a leader read.
                    if n.stale_applied == n.max_observed {
                        return None; // nothing new
                    }
                    n.max_observed = n.max_observed.max(n.stale_applied);
                }
            }
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (linearizability / monotonic reads): no acknowledged read ever returns a
            // value strictly older than one a client already observed. A stale read off a
            // deposed leader is exactly this regression. THIS is the load-bearing, teeth-
            // bearing invariant.
            Property::<Self>::always("no stale read (monotonic reads)", |_, s| !s.stale_read),
            // The prevention-side invariant the ReadIndex check maintains: a former leader
            // that cannot confirm quorum leadership (it is partitioned) never serves a read at
            // all. This is precisely what `ensure_linearizable`'s quorum heartbeat enforces.
            Property::<Self>::always("a partitioned leader never serves a read", |_, s| {
                !s.partitioned_served
            }),
        ]
    }
}

/// The real system: a partitioned former leader's read is gated by the ReadIndex check.
/// Exhaustively explore every interleaving of commit / partition / heal / leader-read /
/// stale-read and assert NO property is ever violated. A counterexample here would mean the
/// ReadIndex gate is itself unsound — a genuine finding; do not weaken the properties.
#[test]
fn read_index_check_prevents_stale_reads() {
    let checker = LinearizableReadModel {
        max_steps: 8,
        read_index_check: true,
    }
    .checker()
    .spawn_bfs()
    .join();

    checker.assert_properties();
    assert!(
        checker.unique_state_count() > 1,
        "model checking must have explored a non-trivial state space"
    );
}

/// Teeth: the SAME model with the ReadIndex check REMOVED (the pre-gate bug — a partitioned
/// former leader keeps serving reads from frozen local state). After the quorum commits a
/// new value and a client observes it, the stale leader serves the OLD value — a read going
/// backwards. This asserts the checker actually CATCHES the regression — `discoveries()` is
/// non-empty and names BOTH safety properties — proving the passing test above is meaningful
/// and not vacuously accepting everything.
#[test]
fn no_read_index_check_serves_stale_reads_is_caught() {
    let checker = LinearizableReadModel {
        max_steps: 8,
        read_index_check: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the ReadIndex check must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    for name in [
        "no stale read (monotonic reads)",
        "a partitioned leader never serves a read",
    ] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the no-ReadIndex-check variant, got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}
