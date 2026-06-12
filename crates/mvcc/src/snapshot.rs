//! Snapshots and version visibility. A snapshot is a commit timestamp; a
//! version is visible iff its commit_ts is <= the snapshot.

use pgtypes::Datum;

use kv::KvError;

use crate::version::decode_version;

/// A read snapshot: the commit timestamp as of which the reader sees the world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot(pub u64);

/// Given a rowid's versions in DESCENDING commit_ts order (as a forward scan
/// yields them), return the visible row, or None if the newest visible version
/// is a tombstone or no version is visible.
pub fn visible_version<'a>(
    snapshot: Snapshot,
    versions: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<Option<Vec<Datum>>, KvError> {
    for (ts, bytes) in versions {
        if ts <= snapshot.0 {
            let (deleted, row) = decode_version(bytes)?;
            return Ok(if deleted { None } else { Some(row) });
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;

    fn ver(ts: u64, deleted: bool, row: Vec<Datum>) -> (u64, Vec<u8>) {
        (ts, crate::version::encode_version(deleted, &row))
    }

    #[test]
    fn picks_newest_version_at_or_below_snapshot() {
        let versions = [
            ver(300, false, vec![Datum::Int4(3)]),
            ver(200, false, vec![Datum::Int4(2)]),
            ver(100, false, vec![Datum::Int4(1)]),
        ];
        let v = visible_version(
            Snapshot(250),
            versions.iter().map(|(t, b)| (*t, b.as_slice())),
        );
        assert_eq!(v.expect("no decode error"), Some(vec![Datum::Int4(2)]));
    }

    #[test]
    fn tombstone_hides_the_row() {
        let versions = [
            ver(300, true, vec![]),
            ver(100, false, vec![Datum::Int4(1)]),
        ];
        let v = visible_version(
            Snapshot(400),
            versions.iter().map(|(t, b)| (*t, b.as_slice())),
        );
        assert_eq!(v.expect("no decode error"), None);
    }

    #[test]
    fn nothing_visible_below_oldest() {
        let versions = [ver(100, false, vec![Datum::Int4(1)])];
        let v = visible_version(
            Snapshot(50),
            versions.iter().map(|(t, b)| (*t, b.as_slice())),
        );
        assert_eq!(v.expect("no decode error"), None);
    }
}
