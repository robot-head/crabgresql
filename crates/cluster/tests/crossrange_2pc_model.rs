//! Exhaustive Stateright model of cross-range 2PC **participant-`Stage` idempotency**
//! (SP21 / D3c).
//!
//! ## The real bug this guards
//!
//! `TwoPcClient::call` (crates/cluster/src/twopc.rs) retries a participant `Stage(g)` RPC
//! once on a transport failure. `TxnService::stage` allocated a FRESH local xid `Li` per
//! call with no dedup. So a `Stage(g)` retried across a participant-leader failover — the
//! original leader durably Raft-committed `Prepared(Li_old -> g)` then died, and the retry
//! landed on the NEW leader (whose in-memory held-session map is empty) — wrote a SECOND
//! `Prepared(Li_new -> g)` version of the same row. When `g` committed, BOTH versions
//! resolved live: the MVCC at-most-one-live invariant was violated (the exact
//! `scan_live`/`find_visible_one` `debug_assert!`) and the cross-range bank balances tore
//! (conservation dropped, e.g. 788 vs 800). The fix makes `Stage` idempotent per
//! `(g, range)`: a `Stage(g)` that finds an existing durable `Prepared(-> g)` marker is a
//! no-op (`SqlEngine::staged_local_for` + the held-session-aware check in `stage`).
//!
//! ## The abstract model
//!
//! ONE row's MVCC version chain under a `Stage` that may be issued TWICE for the same `g`
//! (the failover retry), the WRITE-ONCE global clog, and the visibility resolution. A
//! version `(Li, g)` is *live* iff its global xid `g` committed AND it has not been deleted
//! by a committed deleter (`xmax`). The decisive interleaving the checker explores: the
//! first `Stage(g)` is still in-doubt (its `g` undecided) when the retry runs, so the retry
//! sees the SEED as the live version and supersedes THAT — leaving the first stage's
//! version an un-superseded orphan (`xmax = None`, exactly the `Li=11 xmax=0` phantom seen
//! in the multi-process trace). When `g` later commits, both the orphan and the retry's
//! version go live.
//!
//! `idempotent_stage = true` is the fix (a `Stage(g)` whose `g` already has a version is a
//! no-op); `false` is the pre-fix bug. The teeth test proves the checker CATCHES the
//! double-apply with the fix off; the positive test proves the invariant HOLDS with it on.
//! Mirrors the structure of the SP7 counter model in `model.rs` (positive + teeth tests).

use stateright::{Checker, Model, Property};

/// A committed base version every row starts with (the seed row). Its `g` is always
/// decided-committed, so the seed is live until a committed `Stage` supersedes it.
const G_SEED: u64 = 0;

/// One MVCC row version: created by local xid `li` under global xid `g`, optionally
/// deleted by the local xid `xmax_li` (the `Stage` that superseded it).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    li: u64,
    g: u64,
    xmax_li: Option<u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted by `li` so logically-equal states fingerprint
    /// equally (the BFS dedups on `Hash`).
    versions: Vec<Version>,
    /// Write-once global clog: `(g, committed)`, kept sorted by `g`. A `g` appears at most
    /// once (the first decision wins — `commit_global`'s write-once semantics).
    decided: Vec<(u64, bool)>,
    /// Next local xid to hand out.
    next_li: u64,
    /// The in-flight transfer's global xid once begun (`None` before `Begin`).
    staged_g: Option<u64>,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    fn committed(&self, g: u64) -> bool {
        self.decided.iter().any(|&(gg, c)| gg == g && c)
    }

    /// A version is live iff its creator's `g` committed AND it is not deleted by a
    /// committed deleter (the `xmax` version's `g` committed).
    fn live(&self, v: &Version) -> bool {
        if !self.committed(v.g) {
            return false;
        }
        match v.xmax_li {
            None => true,
            Some(x) => !self
                .versions
                .iter()
                .any(|w| w.li == x && self.committed(w.g)),
        }
    }

    fn live_count(&self) -> usize {
        self.versions.iter().filter(|v| self.live(v)).count()
    }

    /// The currently-live version a fresh `Stage` would read + supersede. The MVCC
    /// invariant is at-most-one-live, so under a correct trace there is exactly one; the
    /// `.max()` picks the highest-`li` (the chain head) when present.
    fn current_live_li(&self) -> Option<u64> {
        self.versions
            .iter()
            .filter(|v| self.live(v))
            .map(|v| v.li)
            .max()
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.decided.sort();
    }
}

/// `idempotent_stage` toggles the fix: `true` = a `Stage(g)` that already has a version for
/// `g` is a no-op (the durable `Prepared(-> g)` marker is detected); `false` = the pre-fix
/// bug where every `Stage(g)` allocates a fresh version.
struct StageModel {
    max_steps: usize,
    idempotent_stage: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// The coordinator mints the transfer's global xid and pins it (once).
    Begin,
    /// Stage the transfer's `g` on the row. Issued possibly TWICE — the second is the
    /// failover retry that landed on a new leader (the bug's trigger).
    Stage,
    /// Write the single global decision for the staged `g` (write-once).
    Decide(bool),
}

impl Model for StageModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // The seed row: created by li 0 under the committed G_SEED, never yet deleted.
            versions: vec![Version {
                li: 0,
                g: G_SEED,
                xmax_li: None,
            }],
            decided: vec![(G_SEED, true)],
            next_li: 1,
            staged_g: None,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        if s.staged_g.is_none() {
            out.push(Action::Begin);
            return;
        }
        // After Begin: the participant may Stage (including a retry), and the coordinator
        // may Decide the staged g either way.
        out.push(Action::Stage);
        out.push(Action::Decide(true));
        out.push(Action::Decide(false));
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Begin => {
                if n.staged_g.is_some() {
                    return None;
                }
                // One global xid above G_SEED for the single in-flight transfer.
                n.staged_g = Some(G_SEED + 1);
            }
            Action::Stage => {
                let g = n.staged_g?;
                // IDEMPOTENT fix: a Stage(g) whose g already has a durable version is a
                // no-op (the marker is detected; no second Prepared(-> g) version).
                if self.idempotent_stage && n.versions.iter().any(|v| v.g == g) {
                    return None; // no new state — this transition adds nothing
                }
                // Supersede the current live version (what the UPDATE re-reads) and create
                // a new version under g. On the failover RETRY, the first stage's g is
                // still in-doubt, so the live version is the SEED — superseding it leaves
                // the first stage's version an un-superseded orphan (xmax = None), which is
                // the double-apply when g later commits.
                if let Some(ll) = n.current_live_li()
                    && let Some(v) = n.versions.iter_mut().find(|v| v.li == ll)
                {
                    v.xmax_li = Some(n.next_li);
                }
                n.versions.push(Version {
                    li: n.next_li,
                    g,
                    xmax_li: None,
                });
                n.next_li += 1;
            }
            Action::Decide(commit) => {
                let g = n.staged_g?;
                // Write-once: the first decision for g wins.
                if !n.decided.iter().any(|&(gg, _)| gg == g) {
                    n.decided.push((g, commit));
                }
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC invariant `scan_live`/`find_visible_one` debug_assert): a
            // row has AT MOST ONE live version under any snapshot. A second live version
            // for the same committed `g` is exactly the non-idempotent double-stage.
            Property::<Self>::always("at most one live version", |_, s| s.live_count() <= 1),
            // The sharper, mechanism-side invariant: no two row versions share the same
            // committed global xid. Two `Prepared(-> g)` versions both committed under one
            // `g` is the literal double-stage the idempotency fix prevents.
            Property::<Self>::always("no two versions share a committed g", |_, s| {
                let mut committed_gs: Vec<u64> = s
                    .versions
                    .iter()
                    .filter(|v| s.committed(v.g) && v.g != G_SEED)
                    .map(|v| v.g)
                    .collect();
                committed_gs.sort_unstable();
                committed_gs.windows(2).all(|w| w[0] != w[1])
            }),
        ]
    }
}

/// The fixed system: `Stage` is idempotent per `g`. Exhaustively explore every interleaving
/// of begin / stage / retry-stage / decide and assert NO property is ever violated. A
/// counterexample here would mean idempotent staging is itself unsound — a genuine finding;
/// do not weaken the properties.
#[test]
fn idempotent_stage_upholds_at_most_one_live() {
    let checker = StageModel {
        max_steps: 7,
        idempotent_stage: true,
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

/// Teeth: the SAME model with idempotency REMOVED (the pre-fix bug). A `Stage(g)` retried
/// across a failover writes a second `Prepared(-> g)` version; when `g` commits both go
/// live. This asserts the checker actually CATCHES the double-apply — `discoveries()` is
/// non-empty and names BOTH safety properties — proving the passing test above is
/// meaningful and not vacuously accepting everything.
#[test]
fn non_idempotent_stage_double_apply_is_caught() {
    let checker = StageModel {
        max_steps: 7,
        idempotent_stage: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing Stage idempotency must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    for name in [
        "at most one live version",
        "no two versions share a committed g",
    ] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the non-idempotent variant, got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}
