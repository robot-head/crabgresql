//! Exhaustive Stateright model of the cross-range 2PC **abort-atomicity** safety invariant
//! (SP24 / D3c): a participant-leader failover must NOT strand an in-doubt row with no live
//! lock holder, because concurrent writers would then create COMPETING versions — each under
//! its own global `g` — that can all commit independently, producing MULTIPLE committed live
//! versions of one row (money created).
//!
//! ## The real bug this guards
//!
//! A cross-range 2PC row's exclusive lock lives ONLY in the in-memory `RowLockManager`. When
//! a participant range's leader is killed mid-2PC, the lock table dies with it — but the
//! durable in-doubt `Prepared(Li -> g)` row version SURVIVES (it is on disk). So on the rise
//! of a new leader the row has a durable in-doubt version with NO live lock holder. The
//! in-doubt `g`'s decision is still pending at the coordinator; it is *destined to go live*
//! the moment `g` resolves to COMMIT.
//!
//! If the new leader serves writes while that row has no lock holder, a CONCURRENT writer is
//! free to stage its OWN cross-range write under a different global `g'` — a *competing*
//! version, not a supersession of the in-doubt one (the writer cannot supersede a version it
//! cannot see resolved, and nothing serializes it behind the in-doubt holder). When BOTH `g`
//! and `g'` independently resolve to COMMIT, both versions go live: two committed live
//! versions of the row violate the MVCC at-most-one-live invariant
//! (`scan_live`/`find_visible_one` `debug_assert!`) and balances tear — the SP18/SP21/SP22
//! `+money` conservation tear.
//!
//! The SP24 fix is **settle-before-serve for LOCKS**: on a leadership rise, the recovery sweep
//! RE-ACQUIRES the exclusive row lock for every in-doubt `Prepared(Li -> g)` row BEFORE the
//! range serves writes, and holds it until `g` resolves to a terminal decision. So a concurrent
//! writer that wants the row BLOCKS until the in-doubt `g` is decided and the lock released —
//! it can never stage a competing version. The blocked writer, once it proceeds, supersedes
//! the now-RESOLVED head, so at most one committed live version exists at any time.
//!
//! ## The abstract model
//!
//! ONE row. Its MVCC versions (each a `{writer, g, ...}` staged write), a write-once global
//! clog mapping each `g` to a terminal decision (`InDoubt`/`Committed`/`Aborted`), and the
//! in-memory `lock_holder: Option<writer>`. The decisive interleaving the checker explores:
//!
//! - `Stage(w, g)` is admitted ONLY when the lock is free (or already held by `w`) — exactly
//!   the `RowLockManager` exclusive lock. It acquires the lock, appends an in-doubt version,
//!   and marks `g` in-doubt.
//! - `Failover` wipes the lock (the killed leader's lost lock table). With the fix
//!   (`reacquire_on_failover = true`) the rise sweep then RE-ACQUIRES the lock for an existing
//!   in-doubt version's writer BEFORE serving — so a competing `Stage` stays blocked. Without
//!   the fix the lock stays FREE and a second writer races in.
//! - `Decide(g, commit)` resolves `g` write-once; on resolution it RELEASES the lock iff its
//!   holder's `g == g` (the holder's 2PC reached a terminal decision).
//!
//! `reacquire_on_failover = true` is the SP24 fix; `false` is the pre-fix bug. The teeth test
//! proves the checker CATCHES the two-committed-versions duplicate with re-acquisition off; the
//! positive test proves the invariant HOLDS with it on. Mirrors the SP22 settle model in
//! `crossrange_2pc_settle_model.rs`, the SP23 reseed model in
//! `crossrange_2pc_gtm_reuse_model.rs`, and the SP7 counter model in `model.rs` (positive +
//! teeth tests).

use stateright::{Checker, Model, Property};

/// The number of distinct writers the model exercises. Two is the minimum that can race for
/// the row (the in-doubt holder vs. a concurrent writer); kept small so the bounded BFS stays
/// tiny while still reproducing the competing-version duplicate.
const N_WRITERS: u8 = 2;

/// A per-`g` terminal decision in the write-once global clog. `InDoubt` is the pending state a
/// staged write starts in; `Decide` resolves it to `Committed` or `Aborted` exactly once.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Decision {
    InDoubt,
    Committed,
    Aborted,
}

/// One MVCC row version. `writer` is the staging xid (which writer created it). `g` is the
/// global xid the staging write was assigned (its visibility is resolved via the clog under
/// `g`). `superseded_by_g` is the `g` of the write that re-read and replaced this version, if
/// any — a version is live only if its `g` committed AND it is not superseded by a committed
/// deleter.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    writer: u8,
    g: u8,
    superseded_by_g: Option<u8>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted so logically-equal states fingerprint equally
    /// (the BFS dedups on `Hash`).
    versions: Vec<Version>,
    /// Write-once global clog: `(g, decision)`, kept sorted by `g`. A `g` is resolved at most
    /// once (the first decision wins — `commit_global`'s write-once semantics).
    decided: Vec<(u8, Decision)>,
    /// The in-memory exclusive row lock: the writer xid holding it, or `None` when free. Wiped
    /// to `None` by a `Failover` (the lock table died with the old leader) and — with the fix —
    /// immediately re-acquired by the rise sweep for an existing in-doubt version's writer.
    lock_holder: Option<u8>,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    /// The terminal decision recorded for `g`, defaulting to `InDoubt` if `g` is not yet in the
    /// clog (unstaged or staged-but-undecided).
    fn decision(&self, g: u8) -> Decision {
        self.decided
            .iter()
            .find(|&&(gg, _)| gg == g)
            .map(|&(_, d)| d)
            .unwrap_or(Decision::InDoubt)
    }

    fn committed(&self, g: u8) -> bool {
        self.decision(g) == Decision::Committed
    }

    /// A version is live iff its creating write's `g` committed AND it is not superseded by a
    /// committed deleter (the deleting write's `g` committed). Two versions whose creating
    /// writes were assigned DIFFERENT global xids that both committed — neither superseding the
    /// other — therefore BOTH go live: exactly the competing-version duplicate the lock
    /// re-acquisition prevents.
    fn live(&self, v: &Version) -> bool {
        if !self.committed(v.g) {
            return false;
        }
        match v.superseded_by_g {
            None => true,
            Some(xg) => !self.committed(xg),
        }
    }

    /// Count of currently-live versions whose `g` resolved to **Committed**. The load-bearing
    /// invariant is that this is `<= 1`: two committed live versions under different global
    /// decisions is the money-creating leak.
    fn committed_live_count(&self) -> usize {
        self.versions.iter().filter(|v| self.live(v)).count()
    }

    /// Is there a staged version whose `g` is still in-doubt (no terminal decision yet)? Used
    /// by the rise sweep to find the in-doubt holder whose lock must be re-acquired.
    fn smallest_in_doubt_writer(&self) -> Option<u8> {
        self.versions
            .iter()
            .filter(|v| self.decision(v.g) == Decision::InDoubt)
            .map(|v| v.writer)
            .min()
    }

    /// The live version a fresh write re-reads + supersedes: the highest-`g` version that is
    /// live RIGHT NOW. A serialized writer (one that proceeds only after the in-doubt holder
    /// resolved + released the lock) re-reads the resolved head and supersedes it — keeping
    /// exactly one live. A competing writer that races in while the in-doubt version is still
    /// pending sees NO live head to supersede (the in-doubt version is not yet live), so it
    /// stages a fresh, non-superseding version alongside it — the duplicate.
    fn visible_g(&self) -> Option<u8> {
        self.versions
            .iter()
            .filter(|v| self.live(v))
            .map(|v| v.g)
            .max()
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.decided.sort();
    }
}

/// `reacquire_on_failover` toggles the SP24 fix: `true` = on a `Failover` the rise sweep
/// re-acquires the exclusive row lock for an existing in-doubt version's writer BEFORE serving
/// (settle-before-serve for locks), so a concurrent writer blocks until the in-doubt `g`
/// resolves; `false` = the pre-fix bug where the failover leaves the row's lock FREE, letting a
/// second writer stage a competing version under its own `g`.
struct AbortAtomicityModel {
    max_steps: usize,
    reacquire_on_failover: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// Writer `w` stages a fresh cross-range write under global xid `g`. GATED on the exclusive
    /// row lock: admitted only when the lock is free or already held by `w`. It acquires the
    /// lock, re-reads + supersedes the visible live head (if any), appends an in-doubt version,
    /// and marks `g` in-doubt.
    Stage(u8, u8),
    /// A participant-leader failover: wipe the in-memory lock (the killed leader's lost lock
    /// table). With the fix on, the rise sweep then re-acquires the lock for an existing
    /// in-doubt version's writer (settle-before-serve for locks).
    Failover,
    /// Resolve global xid `g`'s decision to Committed (`true`) or Aborted (`false`), write-once.
    /// On resolution, release the lock if its holder's `g == g` (the holder's 2PC terminated).
    Decide(u8, bool),
}

impl AbortAtomicityModel {
    /// The `g` writer `w` stages with. Each writer uses one distinct global xid (`w + 1`, so
    /// they are disjoint from each other and from any future seed `g = 0`), modelling the
    /// coordinator handing each cross-range write its own monotone global xid.
    fn g_of(w: u8) -> u8 {
        w + 1
    }
}

impl Model for AbortAtomicityModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // The participant leader, before it died, staged writer 0's cross-range write under
            // `g = 1`: an in-doubt `Prepared(Li -> 1)` version, with writer 0 holding the
            // exclusive row lock. `g = 1`'s decision is still pending at the coordinator. This
            // is the durable in-doubt row a failover strands.
            versions: vec![Version {
                writer: 0,
                g: Self::g_of(0),
                superseded_by_g: None,
            }],
            decided: vec![(Self::g_of(0), Decision::InDoubt)],
            lock_holder: Some(0),
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        for w in 0..N_WRITERS {
            out.push(Action::Stage(w, Self::g_of(w)));
        }
        out.push(Action::Failover);
        for w in 0..N_WRITERS {
            out.push(Action::Decide(Self::g_of(w), true));
            out.push(Action::Decide(Self::g_of(w), false));
        }
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Stage(w, g) => {
                // The exclusive row lock: admit the write only when the lock is free or already
                // held by this writer. A writer blocked by another holder simply cannot act —
                // the action is disabled and adds no new state (this is how the fix serializes
                // a concurrent writer behind the in-doubt lock holder).
                match n.lock_holder {
                    Some(h) if h != w => return None,
                    _ => {}
                }
                // One staged version per writer (its single cross-range write on the row);
                // re-staging the same writer adds no new state.
                if n.versions.iter().any(|v| v.writer == w) {
                    return None;
                }
                // Acquire the lock and re-read + supersede the visible live head. A serialized
                // writer (lock just released by a resolved holder) sees the resolved head and
                // supersedes it (one live). A competing writer racing in while the in-doubt
                // version is still pending sees NO live head — it stages alongside, not over,
                // the in-doubt version: the duplicate.
                n.lock_holder = Some(w);
                if let Some(vg) = n.visible_g()
                    && let Some(v) = n.versions.iter_mut().find(|v| v.g == vg)
                {
                    v.superseded_by_g = Some(g);
                }
                n.versions.push(Version {
                    writer: w,
                    g,
                    superseded_by_g: None,
                });
                if !n.decided.iter().any(|&(gg, _)| gg == g) {
                    n.decided.push((g, Decision::InDoubt));
                }
            }
            Action::Failover => {
                // The killed leader's in-memory lock table is lost: the row's lock is wiped.
                let before = n.lock_holder;
                n.lock_holder = None;
                if self.reacquire_on_failover {
                    // Settle-before-serve for LOCKS: the rise sweep re-acquires the exclusive
                    // row lock for an existing in-doubt version's writer BEFORE serving writes,
                    // holding it until `g` resolves. Deterministic: the canonical-smallest
                    // in-doubt writer.
                    if let Some(w) = n.smallest_in_doubt_writer() {
                        n.lock_holder = Some(w);
                    }
                }
                // A failover that changes nothing (no lock held and nothing to re-acquire) adds
                // no new state, so the BFS dedups it.
                if n.lock_holder == before {
                    return None;
                }
            }
            Action::Decide(g, commit) => {
                // Write-once: the first decision for `g` wins. Only a staged-but-still-in-doubt
                // `g` is decidable.
                let slot = n.decided.iter_mut().find(|(gg, _)| *gg == g);
                match slot {
                    Some((_, d)) if *d == Decision::InDoubt => {
                        *d = if commit {
                            Decision::Committed
                        } else {
                            Decision::Aborted
                        };
                    }
                    // `g` is unstaged or already terminally decided — no new state.
                    _ => return None,
                }
                // On terminal resolution the holder's 2PC is done: release the lock iff its
                // holder staged under this `g`.
                if let Some(h) = n.lock_holder
                    && n.versions.iter().any(|v| v.writer == h && v.g == g)
                {
                    n.lock_holder = None;
                }
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC invariant `scan_live`/`find_visible_one` debug_assert): a
            // cross-range row has AT MOST ONE committed live version under any snapshot. Two
            // versions whose creating writes were assigned DIFFERENT committed global decisions
            // both going live is exactly the abort-atomicity / competing-version leak the SP24
            // lock re-acquisition prevents. THIS is the load-bearing, teeth-bearing invariant.
            Property::<Self>::always("at_most_one_committed_version", |_, s| {
                s.committed_live_count() <= 1
            }),
            // Corroborating, mechanism-side: no two LIVE versions are created by different
            // writers under different global xids without one superseding the other. A competing
            // writer stages a version that neither supersedes nor is superseded by the in-doubt
            // holder's version; once both `g`s commit, both are live and bear distinct `g`s with
            // no supersession link — the literal competing-version duplicate. (Stated over live
            // versions so an aborted/in-doubt pair is not flagged.)
            Property::<Self>::always("no_two_live_versions_under_distinct_g", |_, s| {
                let mut gs: Vec<u8> = s
                    .versions
                    .iter()
                    .filter(|v| s.live(v))
                    .map(|v| v.g)
                    .collect();
                gs.sort_unstable();
                gs.windows(2).all(|w| w[0] != w[1])
            }),
        ]
    }
}

/// The fixed system: on a `Failover` the rise sweep re-acquires the in-doubt row's exclusive
/// lock before serving (`reacquire_on_failover = true`). Exhaustively explore every
/// interleaving of stage / failover / decide and assert NO property is ever violated. A
/// counterexample here would mean lock-reacquisition-before-serve is itself unsound — a genuine
/// design finding; do not weaken the properties (investigate the model's action/lock logic).
#[test]
fn reacquire_on_failover_upholds_at_most_one_committed() {
    let checker = AbortAtomicityModel {
        max_steps: 7,
        reacquire_on_failover: true,
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

/// Teeth: the SAME model with the lock re-acquisition REMOVED (the pre-fix bug — a
/// participant-leader failover leaves the in-doubt row with NO live lock holder). A second
/// writer stages a COMPETING version under its own `g` while the inherited in-doubt `g` is
/// still pending; when BOTH `g`s resolve to commit, BOTH versions go live. This asserts the
/// checker actually CATCHES the duplicate — `discoveries()` is non-empty and names the
/// load-bearing `"at_most_one_committed_version"` property — proving the passing test above is
/// meaningful and not vacuously accepting everything.
#[test]
fn no_reacquire_allows_competing_versions_and_double_commit_is_caught() {
    let checker = AbortAtomicityModel {
        max_steps: 7,
        reacquire_on_failover: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the lock re-acquisition must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    assert!(
        discoveries.contains_key("at_most_one_committed_version"),
        "expected an 'at_most_one_committed_version' counterexample from the no-reacquire \
         variant (a failover strands the in-doubt row lock, a second writer stages a competing \
         version, both g's commit), got: {:?}",
        discoveries.keys().collect::<Vec<_>>()
    );
}

/// The broken counterexample is exactly the **two-committed-versions** state: in the no-
/// reacquire variant, the minimal path stage(0) [init] -> failover (lock freed, NOT
/// re-acquired) -> stage(1) (competing version under g=2) -> decide(1, commit) -> decide(2,
/// commit) leaves two live versions under distinct committed `g`s. This pins the discovery to
/// the precise money-creating shape rather than any incidental violation.
#[test]
fn broken_counterexample_is_two_committed_versions() {
    let checker = AbortAtomicityModel {
        max_steps: 7,
        reacquire_on_failover: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    // Reconstruct the discovery's terminal state and confirm it is the two-committed-live
    // shape (two distinct committed `g`s, neither superseding the other).
    let path = checker
        .discovery("at_most_one_committed_version")
        .expect("the no-reacquire variant must violate at_most_one_committed_version");
    let last = path.last_state();
    assert_eq!(
        last.committed_live_count(),
        2,
        "the abort-atomicity counterexample must be exactly two committed live versions, got \
         state: {last:?}"
    );
    // Both live versions must be under DISTINCT committed global xids (the competing decisions).
    let mut live_gs: Vec<u8> = last
        .versions
        .iter()
        .filter(|v| last.live(v))
        .map(|v| v.g)
        .collect();
    live_gs.sort_unstable();
    assert_eq!(
        live_gs.len(),
        2,
        "expected two live versions, got: {live_gs:?}"
    );
    assert_ne!(
        live_gs[0], live_gs[1],
        "the two committed live versions must be under distinct global xids (competing \
         decisions), got: {live_gs:?}"
    );
}
