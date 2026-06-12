//! Versioned-key and version-value encoding for commit-timestamp MVCC.
//!
//! A rowid's versions live under `kv::key::row_key(table, rowid)` with a
//! descending-commit_ts suffix, so a forward scan hits the newest version
//! first. The value is the row (via the row format) plus a tombstone flag.

use pgtypes::Datum;

use kv::KvError;

/// Build the key for one version of a row. The commit_ts is encoded
/// DESCENDING (`u64::MAX - ts`, big-endian) so higher timestamps sort first.
pub fn version_key(table_id: u32, rowid: u64, commit_ts: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    k
}

/// Recover the commit_ts from a version key known to belong to (table, rowid).
pub fn commit_ts_of(table_id: u32, rowid: u64, key: &[u8]) -> Result<u64, KvError> {
    let prefix = kv::key::row_key(table_id, rowid);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 8 {
        return Err(KvError::CorruptRow("version key shape mismatch".into()));
    }
    let suffix: [u8; 8] = key[prefix.len()..].try_into().expect("8 bytes");
    Ok(u64::MAX - u64::from_be_bytes(suffix))
}

const V_ROW: u8 = 1;
const V_TOMBSTONE: u8 = 2;

/// Encode a version value: a live row, or a tombstone (DELETE).
pub fn encode_version(deleted: bool, row: &[Datum]) -> Vec<u8> {
    if deleted {
        return vec![V_TOMBSTONE];
    }
    let mut out = vec![V_ROW];
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a version value into (deleted, columns).
pub fn decode_version(bytes: &[u8]) -> Result<(bool, Vec<Datum>), KvError> {
    match bytes.first() {
        Some(&V_TOMBSTONE) => Ok((true, Vec::new())),
        Some(&V_ROW) => {
            let cols = kv::rowenc::decode_row(&bytes[1..])?;
            Ok((false, cols))
        }
        _ => Err(KvError::CorruptRow("bad version value tag".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;
    use proptest::prelude::*;

    #[test]
    fn version_key_prefix_is_the_rowid_row_key() {
        let prefix = kv::key::row_key(7, 42);
        let k = version_key(7, 42, 100);
        assert!(k.starts_with(&prefix));
        assert!(k.len() > prefix.len());
    }

    #[test]
    fn newer_commit_ts_sorts_first_descending() {
        let older = version_key(7, 42, 100);
        let newer = version_key(7, 42, 200);
        assert!(
            newer < older,
            "newer version must sort before older for newest-first scan"
        );
    }

    #[test]
    fn commit_ts_roundtrips_from_key() {
        let k = version_key(7, 42, 12345);
        assert_eq!(commit_ts_of(7, 42, &k).expect("valid key"), 12345);
    }

    #[test]
    fn version_value_roundtrip_row_and_tombstone() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let bytes = encode_version(false, &row);
        assert_eq!(decode_version(&bytes).expect("live row roundtrip"), (false, row));
        let tomb = encode_version(true, &[]);
        let (deleted, cols) = decode_version(&tomb).expect("tombstone roundtrip");
        assert!(deleted);
        assert!(cols.is_empty());
    }

    #[test]
    fn decode_version_rejects_corrupt() {
        assert!(decode_version(&[]).is_err());
        assert!(decode_version(&[99]).is_err()); // bad version byte
    }

    proptest! {
        #[test]
        fn descending_order_matches_reverse_ts(a: u64, b: u64) {
            let ka = version_key(1, 1, a);
            let kb = version_key(1, 1, b);
            prop_assert_eq!(a.cmp(&b), kb.cmp(&ka));
        }
    }
}
