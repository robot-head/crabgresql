//! Exhaustive Stateright model of single-range **MVCC snapshot isolation + row-lock
//! first-committer-wins** (SP24): two concurrent transactions writing the SAME row must not
//! both commit a superseding version (a lost update), or the row would have two live
//! versions under one snapshot — the MVCC at-most-one-live invariant
//! (`scan_live`/`find_visible_one` `debug_assert!`).
//!
//! ## The real mechanism this guards
//!
//! crabgresql is PostgreSQL-style MVCC: a write creates a NEW row version stamped with the
//! writer's xid and supersedes the version it read by setting that version's `xmax`
//! (`crates/mvcc/src/visibility.rs`, `crates/executor/src/procarray.rs`). A reader's
//! `Snapshot` (xmin/xmax/xip) fixes which versions it can see. The danger is two
//! transactions that BOTH took their snapshot before either committed: each reads the same
//! base version as the live head. The **row-lock manager** (`crates/executor/src/lockmgr.rs`)
//! is what serializes them: the second writer blocks on the row's write lock until the first
//! commits/aborts, then re-reads the CURRENT head under the lock and supersedes *that* — so
//! the version chain stays linear (first-committer-wins / no lost update).
//!
//! ## The bug the row lock prevents
//!
//! Without write-write conflict detection (no row lock / no re-read under lock), both
//! concurrent writers supersede the STALE base they each saw in their own snapshot. The
//! second writer's new version does not supersede the first writer's version — it leaves it
//! un-superseded. When both transactions commit, the base is dead (superseded once) but BOTH
//! writers' versions are live: two live versions of one row. This is a lost update, and it
//! trips the executor's at-most-one-live `debug_assert!` on the next read. (Structurally the
//! same stale-base supersession the cross-range SP21/SP22 models expose — but here the cause
//! is a missing *single-range* row lock between concurrent writers, a distinct and otherwise
//! unmodeled concurrency-control mechanism.)
//!
//! ## The abstract model
//!
//! ONE row, a committed seed version (creator xid 0, always committed), and TWO concurrent
//! writer transactions (xids 1 and 2) that both began against the seed snapshot. `Stage(x)`
//! has writer `x` create its version; `Commit(x)`/`Abort(x)` settle it. A version is *live*
//! iff its creator committed AND it is not superseded by a committed deleter (exactly
//! `satisfies_mvcc`). The decisive difference is what a `Stage` supersedes:
//!
//! - `lock_writes = true` (the fix): a writer takes the row lock — modelled as (a) only one
//!   writer may be staged-undecided at a time, and (b) it supersedes the CURRENT live head
//!   (re-read under the lock). So the second writer supersedes the first's committed version
//!   → linear chain → one live.
//! - `lock_writes = false` (the bug): writers stage concurrently and each supersedes the
//!   STALE seed it saw in its own snapshot. The second leaves the first's version
//!   un-superseded → both go live when both commit.
//!
//! The teeth test proves the checker CATCHES the double-live with the lock off; the positive
//! test proves the invariant HOLDS with it on. Mirrors the SP21 Stage-idempotency model in
//! `crossrange_2pc_model.rs` (positive + teeth tests).

use stateright::{Checker, Model, Property};

/// The seed version's creator xid — always committed, so the seed is live until a committed
/// writer supersedes it.
const SEED_XID: u64 = 0;

/// One MVCC row version: created by transaction `creator`, optionally superseded (deleted)
/// by transaction `xmax` (the writer that re-read and replaced it).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    creator: u64,
    xmax: Option<u64>,
}

/// A writer transaction's lifecycle. `Staged` means it has written its version and holds the
/// row's write lock (in the locking model); it is then `Committed` or `Aborted`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum WStatus {
    Unstarted,
    Staged,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted so logically-equal states fingerprint equally (the BFS
    /// dedups on `Hash`).
    versions: Vec<Version>,
    /// Per-writer lifecycle, kept sorted by xid so states canonicalize. Writers are xids 1
    /// and 2 (two concurrent transactions racing on the one row).
    writers: Vec<(u64, WStatus)>,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    fn committed(&self, xid: u64) -> bool {
        xid == SEED_XID
            || self
                .writers
                .iter()
                .any(|(x, st)| *x == xid && *st == WStatus::Committed)
    }

    /// A version is live iff its creator committed AND it is not superseded by a committed
    /// deleter — exactly `satisfies_mvcc` over the committed set.
    fn live(&self, v: &Version) -> bool {
        if !self.committed(v.creator) {
            return false;
        }
        match v.xmax {
            None => true,
            Some(x) => !self.committed(x),
        }
    }

    fn live_count(&self) -> usize {
        self.versions.iter().filter(|v| self.live(v)).count()
    }

    /// The version a writer supersedes under the row lock (the fix): the current live head,
    /// the highest-creator live version right now. After the first writer commits, this is
    /// the first writer's version — so the second writer extends the chain.
    fn current_live_head(&self) -> Option<u64> {
        self.versions
            .iter()
            .filter(|v| self.live(v))
            .map(|v| v.creator)
            .max()
    }

    fn status(&self, xid: u64) -> WStatus {
        self.writers
            .iter()
            .find(|(x, _)| *x == xid)
            .map(|(_, st)| st.clone())
            .unwrap_or(WStatus::Unstarted)
    }

    fn any_staged_other(&self, xid: u64) -> bool {
        self.writers
            .iter()
            .any(|(x, st)| *x != xid && *st == WStatus::Staged)
    }

    fn set_status(&mut self, xid: u64, st: WStatus) {
        if let Some(e) = self.writers.iter_mut().find(|(x, _)| *x == xid) {
            e.1 = st;
        } else {
            self.writers.push((xid, st));
        }
        self.writers.sort();
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.writers.sort();
    }
}

/// `lock_writes` toggles the fix: `true` = the row write lock serializes the two writers (one
/// staged-undecided at a time) and a writer supersedes the current live head re-read under
/// the lock; `false` = the pre-lock bug where writers stage concurrently and each supersedes
/// the stale seed it saw in its own snapshot.
struct WriteConflictModel {
    max_steps: usize,
    lock_writes: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// Writer `xid` stages its version (creates a new row version superseding what it reads).
    Stage(u64),
    /// Writer `xid` commits (its version's creator becomes committed/live).
    Commit(u64),
    /// Writer `xid` aborts (its version stays invisible; its supersession does not take effect).
    Abort(u64),
}

/// The two concurrent writer transactions racing on the one row.
const WRITERS: [u64; 2] = [1, 2];

impl Model for WriteConflictModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // The committed seed: created by SEED_XID, never yet deleted, the live head.
            versions: vec![Version {
                creator: SEED_XID,
                xmax: None,
            }],
            writers: WRITERS.iter().map(|&x| (x, WStatus::Unstarted)).collect(),
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        for &x in &WRITERS {
            match s.status(x) {
                WStatus::Unstarted => out.push(Action::Stage(x)),
                WStatus::Staged => {
                    out.push(Action::Commit(x));
                    out.push(Action::Abort(x));
                }
                WStatus::Committed | WStatus::Aborted => {}
            }
        }
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Stage(xid) => {
                if n.status(xid) != WStatus::Unstarted {
                    return None;
                }
                // The row write lock: with the fix on, a second writer cannot stage while
                // another holds the row staged-undecided — it blocks until that one settles.
                if self.lock_writes && n.any_staged_other(xid) {
                    return None;
                }
                // What this write supersedes:
                //  - fix: the CURRENT live head, re-read under the lock — and OVERWRITE its
                //    `xmax` (a live head may carry a stale `xmax` from an aborted deleter,
                //    which the new updater replaces, exactly as Postgres overwrites a tuple's
                //    aborted xmax). So the second writer always supersedes the first's
                //    committed version → linear chain.
                //  - bug: the STALE seed this writer saw in its own snapshot (both writers
                //    began against the seed). The first writer to stage claims the seed's
                //    `xmax` slot; a later concurrent writer leaves it and just adds its sibling
                //    version — both go live when both commit (the lost update).
                if self.lock_writes {
                    if let Some(b) = n.current_live_head()
                        && let Some(v) = n.versions.iter_mut().find(|v| v.creator == b)
                    {
                        v.xmax = Some(xid);
                    }
                } else if let Some(v) = n
                    .versions
                    .iter_mut()
                    .find(|v| v.creator == SEED_XID && v.xmax.is_none())
                {
                    v.xmax = Some(xid);
                }
                n.versions.push(Version {
                    creator: xid,
                    xmax: None,
                });
                n.set_status(xid, WStatus::Staged);
            }
            Action::Commit(xid) => {
                if n.status(xid) != WStatus::Staged {
                    return None;
                }
                n.set_status(xid, WStatus::Committed);
            }
            Action::Abort(xid) => {
                if n.status(xid) != WStatus::Staged {
                    return None;
                }
                n.set_status(xid, WStatus::Aborted);
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC invariant `scan_live`/`find_visible_one` debug_assert): a row
            // has AT MOST ONE live version under any snapshot. Two live versions — both
            // concurrent writers' — is exactly the lost update the row lock prevents. THIS is
            // the load-bearing, teeth-bearing invariant.
            Property::<Self>::always("at most one live version", |_, s| s.live_count() <= 1),
            // Corroborating, mechanism-side (first-committer-wins): the two racing writers
            // never BOTH end up with a live version. A committed writer's version is live iff
            // un-superseded; if both writers committed and both versions are live, the second
            // failed to supersede the first — the lost update.
            Property::<Self>::always("no lost update (first-committer-wins)", |_, s| {
                let both_live = WRITERS.iter().filter(|&&x| {
                    s.versions
                        .iter()
                        .any(|v| v.creator == x && s.live(v))
                }).count();
                both_live <= 1
            }),
        ]
    }
}

/// The real system: the row write lock serializes the two concurrent writers and each
/// supersedes the current live head. Exhaustively explore every interleaving of stage /
/// commit / abort and assert NO property is ever violated. A counterexample here would mean
/// row-lock first-committer-wins is itself unsound — a genuine finding; do not weaken the
/// properties.
#[test]
fn row_lock_upholds_at_most_one_live() {
    let checker = WriteConflictModel {
        max_steps: 8,
        lock_writes: true,
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

/// Teeth: the SAME model with write-write conflict detection REMOVED (no row lock — writers
/// stage concurrently and each supersedes the stale seed it saw). Both writers' versions go
/// live when both commit — a lost update. This asserts the checker actually CATCHES the
/// double-live — `discoveries()` is non-empty and names BOTH safety properties — proving the
/// passing test above is meaningful and not vacuously accepting everything.
#[test]
fn no_row_lock_loses_an_update_is_caught() {
    let checker = WriteConflictModel {
        max_steps: 8,
        lock_writes: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the row lock must produce a property counterexample; if empty the model has \
         no teeth and the passing test is meaningless"
    );
    for name in [
        "at most one live version",
        "no lost update (first-committer-wins)",
    ] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the no-row-lock variant, got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}
