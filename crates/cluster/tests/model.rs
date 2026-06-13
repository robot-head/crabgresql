//! Exhaustive Stateright model of the two highest-risk integration invariants
//! (SP7 Task 9): **counter monotonicity across failover** and **commit
//! durability**.
//!
//! The empirical tests (Tasks 4/7/8) only *sample* fault interleavings. This is
//! a small, self-contained, abstract [`Model`] of the counter/durability logic
//! that Stateright explores *exhaustively* (every interleaving up to a bounded
//! depth). It does NOT use the cluster runtime, openraft, or the SQL engine — it
//! is a pure abstract model of the logic implemented for real in
//! `SequenceManager`/`ProcArray` (Task 2/6: max-merge in the state machine plus
//! `reseed_from_applied` on a leadership change).
//!
//! The abstract system, mirroring the real one:
//!
//! - A leader hands out ids from an in-memory counter (`leader_inmem`); each
//!   allocation also *proposes* a new next-counter value (`id + 1`) into the
//!   replicated log (`in_flight`). In `Replicated` mode those increments are
//!   *not* persisted locally — they become durable only when the folded op
//!   applies via Raft (see `SequenceManager::alloc`).
//! - Proposals apply out of order and are **max-merged** into the applied
//!   high-water mark (`applied_counter`) — exactly the state machine's fold.
//! - An id is **acked** the instant its allocation's proposal applies (the
//!   transaction that used the id is now durable).
//! - On failover (`ElectAndReseed`), un-applied proposals are **discarded**
//!   (their increments were never durable) and a *different* replica becomes
//!   leader. A fresh leader's in-memory counter is **not** the old leader's
//!   volatile counter — it seeds lazily from the durable store it can see
//!   (`read_seq_kv`). `reseed_from_applied` clears any stale cache so that seed
//!   comes from the current applied high-water mark; *without* it the new leader
//!   can keep a stale cached counter that lags the applied mark and hand out an
//!   already-acked id again. The model captures this with `seed_floor` (the
//!   value the leader's cache was last seeded from).
//!
//! The invariant under test: **an acked id is never handed out again** (no id
//! reuse), and equivalently the leader's next counter always strictly dominates
//! every acked id. The max-merge + reseed discipline is what makes this hold;
//! the negative `model_without_reseed_is_caught` test removes the reseed and
//! proves the checker actually *finds* the resulting reuse — i.e. the model has
//! teeth (mirrors the rejection test added in Task 8).

use stateright::{Checker, Model, Property};

/// Abstract system state. All `Vec`s are kept sorted/canonical enough that
/// equal logical states fingerprint equally (the checker dedups on `Hash`).
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// Max-merged applied next-counter across replicas (the durable high-water
    /// mark the state machine folds proposals into).
    applied_counter: u64,
    /// Proposed new next-counter values that have not yet applied. Each entry is
    /// the `id + 1` a still-in-flight allocation proposed.
    in_flight: Vec<u64>,
    /// The leader's in-memory next counter (what the next `Allocate` hands out).
    leader_inmem: u64,
    /// The value the current leader's in-memory cache was last seeded from. A
    /// fresh leader without `reseed_from_applied` keeps a *stale* cache anchored
    /// here, which can lag `applied_counter`; the reseed re-anchors it to the
    /// applied high-water mark. (Models `read_seq_kv` seeding + the cache the
    /// reseed clears.)
    seed_floor: u64,
    /// Every id ever handed to a transaction (to detect reuse).
    handed_out: Vec<u64>,
    /// Ids whose allocation proposal has applied — i.e. durably acked.
    acked: Vec<u64>,
    /// Step counter, used only to bound the exhaustive search.
    steps: usize,
}

/// The model. `reseed` toggles whether `ElectAndReseed` lifts the leader's
/// counter to the applied high-water mark: `true` is the real, correct system;
/// `false` is the deliberately-broken variant the teeth test uses to prove the
/// checker detects id reuse. `fold_on_commit` toggles whether a commit-only
/// allocation (a locking SELECT that wrote no rows) folds `id + 1` into the log
/// at COMMIT time: `true` is the fixed system; `false` is the pre-fix bug where
/// such an allocation acks (durably commits its clog entry) without ever
/// advancing the applied counter — so even a correct reseed re-hands-out the id.
struct CounterModel {
    max_steps: usize,
    reseed: bool,
    fold_on_commit: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// A data-writing allocation: hand out the current in-memory id and propose
    /// `id + 1` into the log (the write entry folds next_xid).
    Allocate,
    /// A locking-SELECT-then-COMMIT allocation that writes no rows: hand out the
    /// current id and *immediately ack it* (its clog[id]=Committed replicates and
    /// applies durably). It advances the applied counter (proposes `id + 1`) ONLY
    /// if `fold_on_commit` — that is the fix. Without the fold the durable ack
    /// lands but the applied high-water mark never moves, so a reseeded leader can
    /// re-hand-out the committed id.
    AllocateCommitOnly,
    /// Apply the `i`th in-flight proposal (out of order), max-merged; the id it
    /// allocated becomes acked.
    ApplyAny(usize),
    /// Failover: discard all un-applied proposals and (if `reseed`) reseed the
    /// leader's in-memory counter to the applied high-water mark.
    ElectAndReseed,
}

impl Model for CounterModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            applied_counter: 0,
            in_flight: vec![],
            leader_inmem: 1,
            seed_floor: 1,
            handed_out: vec![],
            acked: vec![],
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        // Bound the search: once we hit the step budget, no further actions, so
        // the state space is finite and the BFS terminates.
        if s.steps >= self.max_steps {
            return;
        }
        out.push(Action::Allocate);
        out.push(Action::AllocateCommitOnly);
        for i in 0..s.in_flight.len() {
            out.push(Action::ApplyAny(i));
        }
        out.push(Action::ElectAndReseed);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Allocate => {
                // Hand out the current id; propose id + 1 into the log.
                let id = n.leader_inmem;
                n.handed_out.push(id);
                n.in_flight.push(id + 1);
                n.leader_inmem = id + 1;
            }
            Action::AllocateCommitOnly => {
                // A locking-SELECT-then-COMMIT that wrote no rows. The commit
                // batch is one Raft entry applied atomically on return, so the id
                // is durably acked immediately. The fix folds `id + 1` into that
                // same batch, advancing the applied high-water mark together with
                // the ack; without it the ack lands but the counter never moves.
                let id = n.leader_inmem;
                n.handed_out.push(id);
                n.leader_inmem = id + 1;
                // The clog[id]=Committed entry applied => the using txn is durable.
                n.acked.push(id);
                if self.fold_on_commit {
                    // Folded next_xid applies in the same atomic batch (max-merge).
                    n.applied_counter = n.applied_counter.max(id + 1);
                }
            }
            Action::ApplyAny(i) => {
                // Apply out of order, max-merged; the allocated id is now acked.
                if i >= n.in_flight.len() {
                    return None;
                }
                let proposed = n.in_flight.remove(i);
                n.applied_counter = n.applied_counter.max(proposed);
                // The id that allocation handed out was `proposed - 1`.
                n.acked.push(proposed - 1);
                // Keep `in_flight` canonical so logically-equal states dedup.
                n.in_flight.sort_unstable();
            }
            Action::ElectAndReseed => {
                // Failover. Two real facts the model must honor:
                //   1. Every un-applied proposal is lost — its increment was
                //      never durable (Replicated mode does not self-persist).
                //   2. The new leader is a *different* replica. Its in-memory
                //      counter is NOT the old leader's volatile counter; it seeds
                //      from the durable store it can see. A stale cache anchors
                //      that seed at `seed_floor`.
                n.in_flight.clear();
                if self.reseed {
                    // `reseed_from_applied`: clear the stale cache so the next
                    // alloc re-seeds from the current applied high-water mark.
                    n.leader_inmem = n.applied_counter.max(1);
                    n.seed_floor = n.leader_inmem;
                } else {
                    // BROKEN variant: no reseed, so the new leader keeps a stale
                    // cached counter anchored at `seed_floor`, which can lag the
                    // applied high-water mark and re-hand-out an already-acked id.
                    n.leader_inmem = n.seed_floor;
                }
            }
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (commit durability + counter monotonicity): no *acked*
            // (durable) id is ever produced twice. A handed-out id whose proposal
            // never applies is legitimately discarded on failover and may be
            // re-handed-out — that transaction was never durable. But once an
            // id's allocation has applied (the using transaction is durable), it
            // must never be allocated again. A duplicate in `acked` is exactly an
            // acked id reused, so this directly states "an acked id is never
            // handed out again."
            Property::<Self>::always("no acked id reuse", |_, s| {
                let mut v = s.acked.clone();
                v.sort_unstable();
                v.windows(2).all(|w| w[0] != w[1])
            }),
            // The sharper, prevention-side invariant that *guarantees* the above:
            // the leader's next counter always strictly dominates every acked id,
            // so the next `Allocate` can never collide with a committed one. This
            // is precisely what max-merge + `reseed_from_applied` maintains.
            Property::<Self>::always("leader counter dominates acked ids", |_, s| {
                s.acked.iter().all(|&id| s.leader_inmem > id)
            }),
        ]
    }
}

/// The real system: max-merge + reseed. Exhaustively explore every interleaving
/// of allocate / out-of-order-apply / failover-and-reseed up to the step bound
/// and assert **no** property is ever violated. If Stateright finds a
/// counterexample here, the abstract reasoning (max-merge + reseed ⇒ no reuse)
/// is wrong and that is a genuine finding — do not weaken the properties.
#[test]
fn counter_invariants_hold_under_all_interleavings() {
    let checker = CounterModel {
        max_steps: 8,
        reseed: true,
        fold_on_commit: true,
    }
    .checker()
    .spawn_bfs()
    .join();

    // No `always` property has a counterexample across the explored space.
    checker.assert_properties();

    // Sanity: the search was non-trivial (it actually explored interleavings),
    // so a clean result is not vacuous.
    assert!(
        checker.unique_state_count() > 1,
        "model checking must have explored a non-trivial state space"
    );
}

/// Teeth test: the SAME model with the reseed removed. Dropping
/// `reseed_from_applied` means a fresh leader can hand out an id at or below an
/// already-acked one, reusing it. This asserts the checker actually *catches*
/// that — `discoveries()` is non-empty — proving the passing test above is
/// meaningful and not vacuously accepting everything.
#[test]
fn model_without_reseed_is_caught() {
    let checker = CounterModel {
        max_steps: 8,
        reseed: false,
        fold_on_commit: true,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the reseed must produce at least one property counterexample; \
         if this is empty the model has no teeth and the passing test is meaningless"
    );

    // Be specific. The broken variant violates BOTH safety invariants, and we
    // require both to prove the checker detects the real fault and not just its
    // precondition:
    //   * "no acked id reuse" — the load-bearing one: a *durable* id is genuinely
    //     handed out twice (a real commit-durability / counter-monotonicity bug).
    //   * "leader counter dominates acked ids" — the prevention-side invariant
    //     the reseed exists to maintain.
    for name in ["no acked id reuse", "leader counter dominates acked ids"] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the reseed-less variant, \
             got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}

/// Teeth test for *this* fix: a commit-only allocation (a locking SELECT that
/// wrote no rows) that does NOT fold `next_xid` at COMMIT time. The clog entry
/// is durably committed (the id acks) but the applied high-water mark never
/// advances, so even with a *correct* reseed the new leader re-hands-out the
/// committed id. This asserts the checker catches that reuse — proving the
/// commit-time fold is load-bearing and the passing model is not vacuous.
#[test]
fn commit_only_without_fold_is_caught() {
    let checker = CounterModel {
        max_steps: 8,
        // Reseed is present and correct — the bug is purely the missing fold, so
        // this isolates *this* fix's failure mode from the reseed one.
        reseed: true,
        fold_on_commit: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "omitting the commit-time next_xid fold must produce a property \
         counterexample; if empty, the fix is not load-bearing in the model"
    );
    // Both safety invariants must break: a durably-committed id is genuinely
    // re-handed-out (the real defect), and the leader counter fails to dominate
    // the acked id it reuses.
    for name in ["no acked id reuse", "leader counter dominates acked ids"] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the no-commit-fold variant, \
             got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}

/// With BOTH the reseed and the commit-time fold present (the fully-fixed
/// system), the commit-only allocation path introduces no new reuse: every
/// interleaving of data-writes, commit-only allocations, out-of-order applies,
/// and failovers still upholds the invariants. This guards against the new
/// action silently weakening the model.
#[test]
fn invariants_hold_with_commit_only_path_and_fix() {
    let checker = CounterModel {
        max_steps: 8,
        reseed: true,
        fold_on_commit: true,
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
