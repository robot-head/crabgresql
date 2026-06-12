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

/// The row-key prefix of a version key (everything but the 8-byte ts suffix).
pub fn row_prefix_of(key: &[u8]) -> Result<&[u8], KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    Ok(&key[..key.len() - 8])
}

/// The commit_ts encoded in a version key's 8-byte suffix.
pub fn ts_of_key(key: &[u8]) -> Result<u64, KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    let suffix: [u8; 8] = key[key.len() - 8..].try_into().expect("8 bytes");
    Ok(u64::MAX - u64::from_be_bytes(suffix))
}

const V_ROW: u8 = 1;
const V_TOMBSTONE: u8 = 2;

// ── SP5 xid-keyed tuple format ────────────────────────────────────────────────

/// SP5 version key: the row key followed by the creating xid (big-endian,
/// ascending). A rowid's versions all share `kv::key::row_key(table, rowid)`.
pub fn version_key_xid(table_id: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(&xid.to_be_bytes());
    k
}

/// The creating xid encoded in a version key's 8-byte suffix.
pub fn xid_of_key(key: &[u8]) -> Result<u64, KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    let suffix: [u8; 8] = key[key.len() - 8..].try_into().expect("8 bytes");
    Ok(u64::from_be_bytes(suffix))
}

const T_TUPLE: u8 = 1;

/// Encode a tuple version: a 1-byte tag, the `xmin`/`xmax` header, then the row.
/// `xmax == INVALID_XID` (0) marks a live version. A delete keeps the row bytes
/// and sets `xmax` (PostgreSQL retains the tuple until vacuum).
pub fn encode_tuple(xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + row.len() * 8);
    out.push(T_TUPLE);
    out.extend_from_slice(&xmin.to_be_bytes());
    out.extend_from_slice(&xmax.to_be_bytes());
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a tuple version into `(xmin, xmax, row)`.
pub fn decode_tuple(bytes: &[u8]) -> Result<(u64, u64, Vec<Datum>), KvError> {
    if bytes.len() < 17 || bytes[0] != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }
    let xmin = u64::from_be_bytes(bytes[1..9].try_into().expect("8 bytes"));
    let xmax = u64::from_be_bytes(bytes[9..17].try_into().expect("8 bytes"));
    let row = kv::rowenc::decode_row(&bytes[17..])?;
    Ok((xmin, xmax, row))
}

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
    fn row_prefix_of_strips_ts_suffix() {
        let k = version_key(7, 42, 5);
        let expected = kv::key::row_key(7, 42);
        assert_eq!(row_prefix_of(&k).expect("valid key"), expected.as_slice());
    }

    #[test]
    fn ts_of_key_roundtrips() {
        let k = version_key(7, 42, 5);
        assert_eq!(ts_of_key(&k).expect("valid key"), 5);
    }

    #[test]
    fn row_prefix_of_rejects_too_short() {
        assert!(row_prefix_of(&[0u8; 4]).is_err());
    }

    #[test]
    fn ts_of_key_rejects_too_short() {
        assert!(ts_of_key(&[0u8; 4]).is_err());
    }

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
        assert_eq!(
            decode_version(&bytes).expect("live row roundtrip"),
            (false, row)
        );
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

    #[test]
    fn commit_ts_of_rejects_malformed_keys() {
        // Missing 8-byte suffix: passing the bare row key (no ts suffix) must fail.
        assert!(
            commit_ts_of(7, 42, &kv::key::row_key(7, 42)).is_err(),
            "row key without ts suffix must be rejected"
        );
        // Wrong rowid: prefix is for rowid 42 but we claim rowid 99.
        assert!(
            commit_ts_of(7, 99, &version_key(7, 42, 100)).is_err(),
            "version key for rowid 42 must be rejected when rowid 99 is expected"
        );
    }

    #[test]
    fn tombstone_discards_row_payload() {
        // Encoding a tombstone with a non-empty row must produce a value that
        // decodes back as deleted=true with no columns, proving the payload is
        // intentionally dropped on DELETE.
        let bytes = encode_version(true, &[Datum::Int4(1), Datum::Text("x".into())]);
        let (deleted, cols) = decode_version(&bytes).expect("tombstone decode");
        assert!(deleted, "tombstone flag must be set");
        assert!(cols.is_empty(), "tombstone must carry no column data");
    }

    proptest! {
        #[test]
        fn descending_order_matches_reverse_ts(a: u64, b: u64) {
            let ka = version_key(1, 1, a);
            let kb = version_key(1, 1, b);
            prop_assert_eq!(a.cmp(&b), kb.cmp(&ka));
        }
    }

    // ── SP5 xid-keyed tuple tests ─────────────────────────────────────────────

    #[test]
    fn version_key_xid_is_rowid_prefix_plus_ascending_xid() {
        let prefix = kv::key::row_key(7, 42);
        let k = version_key_xid(7, 42, 100);
        assert!(k.starts_with(&prefix));
        assert_eq!(xid_of_key(&k).expect("xid"), 100);
        // ascending: a higher xid sorts after a lower one for the same row.
        assert!(version_key_xid(7, 42, 100) < version_key_xid(7, 42, 200));
        // row_prefix_of strips the 8-byte xid suffix back to the row key.
        assert_eq!(row_prefix_of(&k).expect("prefix"), prefix.as_slice());
    }

    #[test]
    fn tuple_roundtrips_header_and_row() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let bytes = encode_tuple(5, crate::xid::INVALID_XID, &row);
        assert_eq!(decode_tuple(&bytes).expect("decode"), (5, 0, row));
        // a deleted/superseded version keeps its row bytes and carries xmax.
        let bytes = encode_tuple(5, 9, &[Datum::Int4(1)]);
        assert_eq!(decode_tuple(&bytes).expect("decode"), (5, 9, vec![Datum::Int4(1)]));
    }

    #[test]
    fn decode_tuple_rejects_corrupt() {
        assert!(decode_tuple(&[]).is_err());
        assert!(decode_tuple(&[99, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // bad tag
        assert!(decode_tuple(&[1, 0, 0]).is_err()); // too short for header
    }
}
