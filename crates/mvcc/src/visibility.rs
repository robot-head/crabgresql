//! Snapshot-based visibility — PostgreSQL's `HeapTupleSatisfiesMVCC`. A snapshot
//! is `(xmin, xmax, xip[])`: `xmax` is one past the highest assigned xid, `xip`
//! is the set of xids that were running when the snapshot was taken, and `xmin`
//! is the lowest of those (a fast "everything below is settled" bound). The clog
//! answers "did this xid commit?"; the snapshot answers "before I started?".

use kv::KvError;

use crate::clog::XidStatus;
use crate::xid::INVALID_XID;

/// A read snapshot: the running-transaction set as of a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>, // sorted ascending
}

impl Snapshot {
    /// Was `xid` running at (or started after) the moment this snapshot was taken?
    fn is_running(&self, xid: u64) -> bool {
        // NOTE: PostgreSQL also treats `xid < self.xmin` as a fast "settled" case.
        // We omit that fast path: such xids fall through to the clog, which gives
        // the identical committed/aborted answer (xmin is the lowest running xid,
        // so everything below it is already settled). Pure optimization, not
        // correctness — safe to add later if clog lookups become hot.
        xid >= self.xmax || self.xip.binary_search(&xid).is_ok()
    }
}

/// Is a transaction's effect visible to this snapshot? True iff it is the
/// caller's own transaction, or it had committed before the snapshot was taken.
fn committed_visible(
    xid: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: &impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    if Some(xid) == own {
        return Ok(true); // my own write (read-your-writes)
    }
    if snapshot.is_running(xid) {
        return Ok(false); // running at, or started after, my snapshot
    }
    Ok(matches!(status(xid)?, XidStatus::Committed)) // settled: ask the clog
}

/// PostgreSQL `HeapTupleSatisfiesMVCC` for a tuple with header `(xmin, xmax)`:
/// visible iff its creator is visible to the snapshot AND it has not been
/// deleted/superseded by a transaction also visible to the snapshot.
pub fn satisfies_mvcc(
    xmin: u64,
    xmax: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    debug_assert!(
        snapshot.xip.windows(2).all(|w| w[0] <= w[1]),
        "Snapshot.xip must be sorted ascending for binary_search visibility"
    );
    if !committed_visible(xmin, snapshot, own, &status)? {
        return Ok(false);
    }
    if xmax == INVALID_XID {
        return Ok(true);
    }
    Ok(!committed_visible(xmax, snapshot, own, &status)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::XidStatus;

    // A clog stub: maps xid -> status via a small closure.
    fn status_map<'a>(
        committed: &'a [u64],
        aborted: &'a [u64],
    ) -> impl Fn(u64) -> Result<XidStatus, kv::KvError> + 'a {
        move |x| {
            if committed.contains(&x) {
                Ok(XidStatus::Committed)
            } else if aborted.contains(&x) {
                Ok(XidStatus::Aborted)
            } else {
                Ok(XidStatus::InProgress)
            }
        }
    }

    fn snap(xmax: u64, xip: &[u64]) -> Snapshot {
        let mut xip = xip.to_vec();
        xip.sort_unstable();
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    #[test]
    fn committed_before_snapshot_is_visible() {
        let s = snap(10, &[]);
        assert!(satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn running_at_snapshot_is_invisible() {
        let s = snap(10, &[5]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn started_after_snapshot_is_invisible() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(12, 0, &s, None, status_map(&[12], &[])).expect("ok"));
    }

    #[test]
    fn aborted_xmin_is_invisible() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[], &[5])).expect("ok"));
    }

    #[test]
    fn own_xid_is_visible_read_your_writes() {
        let s = snap(7, &[]);
        assert!(satisfies_mvcc(7, 0, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }

    #[test]
    fn committed_visible_delete_hides_row() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 6, &s, None, status_map(&[5, 6], &[])).expect("ok"));
    }

    #[test]
    fn aborted_or_running_delete_does_not_hide_row() {
        let s = snap(10, &[6]); // xmax=6 still running at my snapshot
        assert!(satisfies_mvcc(5, 6, &s, None, status_map(&[5], &[])).expect("ok"));
        let s2 = snap(10, &[]);
        assert!(satisfies_mvcc(5, 6, &s2, None, status_map(&[5], &[6])).expect("ok")); // xmax aborted
    }

    #[test]
    fn own_delete_hides_row_from_me() {
        let s = snap(7, &[]);
        assert!(!satisfies_mvcc(7, 7, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }

    #[test]
    fn sorted_multi_element_xip_resolves_correctly() {
        // Snapshot xmax=20, running={5,9,14}: committed row xmin=3 (below xmin=5,
        // settled) should be visible; xmin=9 (in xip, still running) should not.
        let s = snap(20, &[5, 9, 14]);
        assert_eq!(s.xip, vec![5, 9, 14]); // verify snap() sorted them
        assert!(satisfies_mvcc(3, 0, &s, None, status_map(&[3], &[])).expect("ok"));
        assert!(!satisfies_mvcc(9, 0, &s, None, status_map(&[9], &[])).expect("ok"));
    }
}
