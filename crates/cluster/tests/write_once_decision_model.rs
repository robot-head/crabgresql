//! Exhaustive Stateright model of the **write-once global-decision / abort-race** agreement
//! invariant (SP24): once a cross-range global xid `g` has a terminal clog decision, that
//! decision never changes, so every party — the coordinator, a self-resolving participant,
//! the silence sweeper, and any fresh reader — acts on the SAME outcome. No participant
//! commits a row while another aborts it.
//!
//! ## The real mechanism this guards
//!
//! A cross-range 2PC decision is recorded ONCE in range 0's global clog. The Raft state
//! machine's apply is **write-once**: `apply_op` keeps an existing TERMINAL decision
//! (Committed/Aborted) and ignores any later write to the same clog key
//! (`crates/cluster/src/store.rs`, gated by `mvcc::clog::is_terminal`). Several actors may
//! race to decide one `g`: the coordinator's COMMIT (`commit_global_decision(g, Committed)`),
//! a participant that timed out and self-resolves via the abort-race
//! (`TxnService::resolve_in_doubt` sends `CommitGlobal{commit:false}`), and the silence
//! sweeper. Crucially, `commit_global_decision` writes its proposal and then **reads back the
//! EFFECTIVE decision** (`crates/executor/src/lib.rs`): the loser of the race learns the
//! winner's outcome and acts on THAT, so a coordinator that lost an abort-race honestly
//! reports ROLLBACK and releases participants with abort semantics (the SP18 honesty test in
//! `range/router.rs`).
//!
//! ## The bug write-once prevents
//!
//! If a later decision could OVERWRITE an earlier terminal one, the parties diverge: the
//! coordinator writes Committed, reads back Committed, and releases a participant with COMMIT
//! (the row goes live); then a sweeper overwrites the clog with Aborted; a fresh reader
//! resolves the row against the now-Aborted decision and treats it as invisible. The same
//! `g` is simultaneously committed (by the released participant) and aborted (by the reader)
//! — a torn cross-range transaction. Write-once + read-back-effective is exactly what makes
//! every actor converge on the first decision.
//!
//! ## The abstract model
//!
//! The single clog slot for `g` (`decision: Option<bool>`, None = in-doubt), the FIRST
//! terminal value ever written (`first_decision`, for the stability property), and two flags
//! recording whether ANY actor acted on a commit / an abort outcome. `DecideCommit` and
//! `DecideAbort` are racing deciders; each writes its proposal and acts on the EFFECTIVE
//! read-back. `Resolve` is a fresh reader acting on whatever the slot currently holds.
//!
//! `write_once = true` is the real system (a terminal decision is immutable; the read-back is
//! the first decision); `false` is the broken variant (a later decision overwrites). The
//! teeth test proves the checker CATCHES the divergence with write-once off; the positive
//! test proves agreement HOLDS with it on. Mirrors the positive + teeth structure of the SP7
//! counter model in `model.rs`.

use stateright::{Checker, Model, Property};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The single clog slot for `g`: `None` = in-doubt, `Some(true)` = Committed,
    /// `Some(false)` = Aborted. Write-once keeps the first terminal value.
    decision: Option<bool>,
    /// The FIRST terminal decision ever written (set once, never changed). The stability
    /// property asserts `decision` always equals this once it is set.
    first_decision: Option<bool>,
    /// Some actor released/acted with COMMIT semantics (it read back a committed decision).
    acted_commit: bool,
    /// Some actor released/acted with ABORT semantics (it read back an aborted decision).
    acted_abort: bool,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    /// Write a proposed terminal decision and return the EFFECTIVE outcome the writer reads
    /// back. Write-once: the first terminal decision wins, so the read-back may differ from
    /// the proposal (the abort-race loser learns the winner's outcome). Without write-once a
    /// later write overwrites and the writer reads back its own proposal.
    fn decide(&mut self, proposal: bool, write_once: bool) -> bool {
        match self.decision {
            Some(existing) if write_once => existing, // immutable — read back the winner
            _ => {
                self.decision = Some(proposal);
                self.first_decision.get_or_insert(proposal);
                proposal
            }
        }
    }
}

/// `write_once` toggles the fix: `true` = a terminal clog decision is immutable and the
/// writer reads back the first decision; `false` = a later decision overwrites the slot.
struct WriteOnceModel {
    max_steps: usize,
    write_once: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// The coordinator's COMMIT: propose Committed, act on the effective read-back.
    DecideCommit,
    /// A participant/sweeper abort-race: propose Aborted, act on the effective read-back.
    DecideAbort,
    /// A fresh reader resolves the row against the current decision and acts on it.
    Resolve,
}

impl Model for WriteOnceModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            decision: None,
            first_decision: None,
            acted_commit: false,
            acted_abort: false,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        out.push(Action::DecideCommit);
        out.push(Action::DecideAbort);
        out.push(Action::Resolve);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        let acted = match a {
            Action::DecideCommit => n.decide(true, self.write_once),
            Action::DecideAbort => n.decide(false, self.write_once),
            // A reader acts on the current terminal decision; still in-doubt (`None`) is
            // nothing to act on, so `?` short-circuits to no new state.
            Action::Resolve => n.decision?,
        };
        if acted {
            n.acted_commit = true;
        } else {
            n.acted_abort = true;
        }
        // Dedup: if this transition produced no observable change, drop it so the BFS does not
        // loop on self-edges.
        if n == *s {
            return None;
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (cross-range atomicity / agreement): never does one party act on COMMIT
            // while another acts on ABORT for the same `g`. A torn decision is exactly the
            // simultaneous commit-and-abort write-once prevents. THIS is the load-bearing,
            // teeth-bearing invariant.
            Property::<Self>::always("global decision agreement", |_, s| {
                !(s.acted_commit && s.acted_abort)
            }),
            // The mechanism-side invariant that guarantees the above: once a terminal decision
            // is recorded it is STABLE — the clog slot always equals the first decision ever
            // written. This is precisely the write-once apply (`is_terminal` keep).
            Property::<Self>::always("terminal decision is stable (write-once)", |_, s| {
                match s.first_decision {
                    None => true,
                    Some(f) => s.decision == Some(f),
                }
            }),
        ]
    }
}

/// The real system: a terminal clog decision is immutable and every actor reads back the
/// first decision. Exhaustively explore every interleaving of commit-decide / abort-decide /
/// resolve and assert NO property is ever violated. A counterexample here would mean write-
/// once agreement is itself unsound — a genuine finding; do not weaken the properties.
#[test]
fn write_once_decision_keeps_every_party_in_agreement() {
    let checker = WriteOnceModel {
        max_steps: 6,
        write_once: true,
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

/// Teeth: the SAME model with write-once REMOVED (a later decision overwrites the clog slot).
/// The coordinator commits and releases a participant, then a sweeper overwrites with Aborted
/// and a reader resolves the row as invisible — the same `g` is committed by one party and
/// aborted by another. This asserts the checker actually CATCHES the divergence —
/// `discoveries()` is non-empty and names BOTH safety properties — proving the passing test
/// above is meaningful and not vacuously accepting everything.
#[test]
fn overwritable_decision_tears_the_transaction_is_caught() {
    let checker = WriteOnceModel {
        max_steps: 6,
        write_once: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing write-once must produce a property counterexample; if empty the model has \
         no teeth and the passing test is meaningless"
    );
    for name in [
        "global decision agreement",
        "terminal decision is stable (write-once)",
    ] {
        assert!(
            discoveries.contains_key(name),
            "expected a '{name}' counterexample from the overwritable-decision variant, got: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}
