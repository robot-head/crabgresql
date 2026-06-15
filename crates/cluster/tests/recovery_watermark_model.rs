//! Exhaustive Stateright model of the **recovery clog-scan watermark** safety invariant
//! (SP24): the per-range recovery watermark (`clog_scan_lo`) must never advance past a
//! still-in-doubt `Prepared(Li -> g)` marker, so a future leadership-rise sweep starting at
//! the watermark always re-finds every unresolved in-doubt marker. An in-doubt marker is
//! never orphaned (its locks left held / its row left invisible) forever.
//!
//! ## The real mechanism this guards
//!
//! On a participant range, a staged cross-range write durably records a `Prepared(Li -> g)`
//! marker in the range's clog. The leadership-rise sweep scans the clog for in-doubt markers
//! and resolves each against range 0's global decision. To keep that scan O(new markers)
//! rather than O(all history), it advances a durable watermark `clog_scan_lo`:
//! `in_doubt_globals_from(scan_lo)` returns `(in_doubt_gs, new_scan_lo)` where
//! `new_scan_lo` is the FIRST undecided marker's `Li` if any, else one past the largest
//! scanned `Li` (`crates/executor/src/lib.rs`). `advance_clog_scan_lo` then persists it
//! monotonically. The key clause is the `first_undecided` floor: **the watermark never passes
//! a non-terminal `g`** — the zombie-commit safety invariant. (`staged_local_for`, the SP21
//! Stage-idempotency check, relies on this: an in-doubt `g`'s marker is always at or above
//! the watermark, so the idempotency scan from the watermark cannot miss it.)
//!
//! ## The bug the floor prevents
//!
//! Drop the `first_undecided` floor and advance the watermark to one-past-the-largest scanned
//! marker regardless of decidedness. An in-doubt marker that sits BELOW a later terminal
//! marker is then skipped: the watermark jumps past it, the next sweep scans only above the
//! watermark, and that marker's `g` is never resolved — its row stays in-doubt (invisible)
//! and its participant lock is never released. The SP21 idempotency scan would also miss it
//! and re-stage a duplicate. Bounding the watermark by the smallest undecided `Li` is what
//! keeps every in-doubt marker discoverable.
//!
//! ## The abstract model
//!
//! A growing set of `Prepared` markers (each a `(Li, g)` at a monotone local xid), the set of
//! `g`s that have a terminal global decision, and the watermark. `AddMarker` stages a new
//! marker; `Decide` makes a `g` terminal; `Scan` runs `in_doubt_globals_from(watermark)` and
//! advances the watermark — with the fix it is floored at the first undecided `Li`, without
//! the fix it jumps past every scanned marker.
//!
//! `bound_by_undecided = true` is the real system (the `first_undecided` floor); `false` is
//! the broken variant (advance past everything scanned). The teeth test proves the checker
//! CATCHES the skipped marker with the floor off; the positive test proves the invariant
//! HOLDS with it on. Mirrors the positive + teeth structure of the SP7 counter model in
//! `model.rs`.

use stateright::{Checker, Model, Property};

/// The two distinct global xids a marker may carry. Keeping the alphabet at two lets the BFS
/// build the decisive interleaving — a low in-doubt marker beneath a higher terminal one —
/// while staying tiny. (Concrete values are immaterial; these stand in for two `g`s.)
const GS: [u64; 2] = [10, 11];

/// Cap the number of `Prepared` markers so the BFS state space stays finite (with the step
/// budget). Three markers is enough to expose a skipped in-doubt marker beneath a terminal one.
const MAX_MARKERS: usize = 3;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The range's `Prepared(Li -> g)` markers, kept sorted by `Li` so logically-equal states
    /// fingerprint equally (the BFS dedups on `Hash`). Markers are never deleted.
    markers: Vec<(u64, u64)>, // (Li, g)
    /// The `g`s with a terminal (Committed/Aborted) global decision, kept sorted.
    terminal: Vec<u64>,
    /// The durable recovery-scan watermark (`clog_scan_lo`). Monotone.
    watermark: u64,
    /// Next local xid to stamp a new marker (monotone).
    next_li: u64,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    fn is_terminal(&self, g: u64) -> bool {
        self.terminal.binary_search(&g).is_ok()
    }

    /// Run `in_doubt_globals_from(watermark)`: over markers at or above the watermark, find
    /// the first undecided `Li` and the largest scanned `Li`, and compute the new watermark.
    /// `bound` is the `first_undecided` floor — present in the real system, dropped in the bug.
    fn scan_new_watermark(&self, bound: bool) -> u64 {
        let mut first_undecided: Option<u64> = None;
        let mut max_li: Option<u64> = None;
        for &(li, g) in self.markers.iter().filter(|&&(li, _)| li >= self.watermark) {
            max_li = Some(max_li.map_or(li, |m: u64| m.max(li)));
            if !self.is_terminal(g) {
                first_undecided = Some(first_undecided.map_or(li, |m: u64| m.min(li)));
            }
        }
        // The fix floors at the first undecided Li (never pass a non-terminal g); the bug
        // drops that term and jumps to one past the largest scanned marker.
        let candidate = if bound {
            first_undecided
                .or_else(|| max_li.map(|m| m + 1))
                .unwrap_or(self.watermark)
        } else {
            max_li.map(|m| m + 1).unwrap_or(self.watermark)
        };
        candidate.max(self.watermark) // monotone
    }

    /// Every `g` that is still in-doubt (has a non-terminal marker), as a sorted set.
    fn in_doubt_gs(&self) -> Vec<u64> {
        let mut gs: Vec<u64> = self
            .markers
            .iter()
            .filter(|&&(_, g)| !self.is_terminal(g))
            .map(|&(_, g)| g)
            .collect();
        gs.sort_unstable();
        gs.dedup();
        gs
    }

    /// The in-doubt `g`s a sweep STARTING AT THE WATERMARK would discover (markers at or above
    /// the watermark). If the watermark skipped a marker, that `g` is missing here.
    fn discoverable_in_doubt_gs(&self) -> Vec<u64> {
        let mut gs: Vec<u64> = self
            .markers
            .iter()
            .filter(|&&(li, g)| li >= self.watermark && !self.is_terminal(g))
            .map(|&(_, g)| g)
            .collect();
        gs.sort_unstable();
        gs.dedup();
        gs
    }

    fn canonicalize(&mut self) {
        self.markers.sort();
        self.terminal.sort_unstable();
        self.terminal.dedup();
    }
}

/// `bound_by_undecided` toggles the fix: `true` = the watermark is floored at the first
/// undecided `Li` (the `first_undecided` clause, never passing a non-terminal `g`); `false` =
/// the broken variant that advances past every scanned marker regardless of decidedness.
struct WatermarkModel {
    max_steps: usize,
    bound_by_undecided: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// Stage a new `Prepared(Li -> g)` marker at the next local xid.
    AddMarker(u64),
    /// Record a terminal global decision for `g` (Committed or Aborted — only terminality
    /// matters to the watermark).
    Decide(u64),
    /// Run the leadership-rise sweep's scan from the watermark and advance the watermark.
    Scan,
}

impl Model for WatermarkModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            markers: vec![],
            terminal: vec![],
            watermark: 0,
            next_li: 1,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        if s.markers.len() < MAX_MARKERS {
            for &g in &GS {
                out.push(Action::AddMarker(g));
            }
        }
        for &g in &GS {
            if !s.is_terminal(g) {
                out.push(Action::Decide(g));
            }
        }
        out.push(Action::Scan);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::AddMarker(g) => {
                if n.markers.len() >= MAX_MARKERS {
                    return None;
                }
                n.markers.push((n.next_li, g));
                n.next_li += 1;
            }
            Action::Decide(g) => {
                if n.is_terminal(g) {
                    return None; // already terminal — no new state
                }
                n.terminal.push(g);
            }
            Action::Scan => {
                let new_wm = n.scan_new_watermark(self.bound_by_undecided);
                if new_wm == n.watermark {
                    return None; // watermark unchanged — no new state
                }
                n.watermark = new_wm;
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (zombie-commit / recovery completeness): every still-in-doubt marker sits
            // AT OR ABOVE the watermark, so the next sweep from the watermark re-finds it. A
            // marker the watermark has passed while its `g` is non-terminal is exactly the
            // orphaned in-doubt marker. THIS is the load-bearing, teeth-bearing invariant.
            Property::<Self>::always("no in-doubt marker below the watermark", |_, s| {
                s.markers
                    .iter()
                    .all(|&(li, g)| s.is_terminal(g) || li >= s.watermark)
            }),
            // The recovery-completeness restatement: a sweep starting at the watermark
            // discovers EVERY in-doubt `g` (none is skipped). With the floor this equals the
            // full in-doubt set; with the bug a skipped marker's `g` goes missing.
            Property::<Self>::always("every in-doubt g stays discoverable", |_, s| {
                s.discoverable_in_doubt_gs() == s.in_doubt_gs()
            }),
        ]
    }
}

/// The real system: the watermark is floored at the first undecided `Li`. Exhaustively
/// explore every interleaving of add-marker / decide / scan and assert NO property is ever
/// violated. A counterexample here would mean the watermark floor is itself unsound — a
/// genuine finding; do not weaken the properties.
#[test]
fn watermark_floor_keeps_in_doubt_markers_discoverable() {
    let checker = WatermarkModel {
        max_steps: 8,
        bound_by_undecided: true,
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

/// Teeth: the SAME model with the `first_undecided` floor REMOVED (advance past every scanned
/// marker). An in-doubt marker beneath a later terminal one is skipped — the watermark jumps
/// past it and a future sweep never re-finds its `g`. This asserts the checker actually
/// CATCHES the skipped marker — `discoveries()` is non-empty and names the load-bearing
/// `"no in-doubt marker below the watermark"` property — proving the passing test above is
/// meaningful and not vacuously accepting everything.
#[test]
fn unbounded_watermark_skips_an_in_doubt_marker_is_caught() {
    let checker = WatermarkModel {
        max_steps: 8,
        bound_by_undecided: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the watermark floor must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    assert!(
        discoveries.contains_key("no in-doubt marker below the watermark"),
        "expected a 'no in-doubt marker below the watermark' counterexample from the unbounded \
         variant (an in-doubt marker beneath a terminal one is skipped), got: {:?}",
        discoveries.keys().collect::<Vec<_>>()
    );
}
