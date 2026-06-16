//! Xid-keyed tuple encoding for crabgresql SP5+ MVCC.
//!
//! A rowid's versions live under `kv::key::row_key(table, rowid)` with an
//! ascending xid suffix, so versions sort chronologically. The value carries
//! the xmin/xmax header and the row payload.

use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use pgtypes::Datum;

use kv::KvError;

// ── SP5 xid-keyed tuple format ────────────────────────────────────────────────

/// SP5 version key: the row key followed by the creating xid (big-endian,
/// ascending). A rowid's versions all share `kv::key::row_key(table, rowid)`.
pub fn version_key_xid(table_id: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(U64::new(xid).as_bytes());
    k
}

/// The row-key prefix of a version key (everything but the 8-byte xid suffix).
pub fn row_prefix_of(key: &[u8]) -> Result<&[u8], KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    Ok(&key[..key.len() - 8])
}

/// The creating xid encoded in a version key's 8-byte suffix.
pub fn xid_of_key(key: &[u8]) -> Result<u64, KvError> {
    let (_, xid) = U64::read_from_suffix(key)
        .map_err(|_| KvError::CorruptRow("version key too short".into()))?;
    Ok(xid.get())
}

/// Fixed 17-byte tuple header: tag + big-endian xmin/xmax. `#[repr(C)]` with
/// alignment-1 fields packs with no padding, matching the on-disk layout.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleHeader {
    tag: u8,
    xmin: U64,
    xmax: U64,
}

const T_TUPLE: u8 = 1;

/// Encode a tuple version: a 1-byte tag, the `xmin`/`xmax` header, then the row.
/// `xmax == INVALID_XID` (0) marks a live version. A delete keeps the row bytes
/// and sets `xmax` (PostgreSQL retains the tuple until vacuum).
pub fn encode_tuple(xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    let header = TupleHeader {
        tag: T_TUPLE,
        xmin: U64::new(xmin),
        xmax: U64::new(xmax),
    };
    let mut out = Vec::with_capacity(17 + row.len() * 8);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a tuple version into `(xmin, xmax, row)`.
pub fn decode_tuple(bytes: &[u8]) -> Result<(u64, u64, Vec<Datum>), KvError> {
    let (header, rest) = TupleHeader::ref_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("bad tuple header".into()))?;
    if header.tag != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }
    let row = kv::rowenc::decode_row(rest)?;
    Ok((header.xmin.get(), header.xmax.get(), row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;

    #[test]
    fn tuple_header_is_packed_17_bytes() {
        assert_eq!(core::mem::size_of::<TupleHeader>(), 17);
    }

    #[test]
    fn tuple_header_layout_matches_manual_be() {
        use zerocopy::IntoBytes;
        use zerocopy::byteorder::big_endian::U64;
        let h = TupleHeader {
            tag: T_TUPLE,
            xmin: U64::new(5),
            xmax: U64::new(9),
        };
        let mut manual = vec![T_TUPLE];
        manual.extend_from_slice(&5u64.to_be_bytes());
        manual.extend_from_slice(&9u64.to_be_bytes());
        assert_eq!(h.as_bytes(), manual.as_slice());
    }

    #[test]
    fn row_prefix_of_strips_xid_suffix() {
        let k = version_key_xid(7, 42, 5);
        let expected = kv::key::row_key(7, 42);
        assert_eq!(row_prefix_of(&k).expect("valid key"), expected.as_slice());
    }

    #[test]
    fn row_prefix_of_rejects_too_short() {
        assert!(row_prefix_of(&[0u8; 4]).is_err());
    }

    #[test]
    fn row_prefix_of_at_exactly_the_suffix_length_is_an_empty_prefix() {
        // A key of exactly the 8-byte xid suffix has an EMPTY row prefix — it is
        // the boundary, not an error (only strictly-shorter keys are rejected).
        assert_eq!(row_prefix_of(&[0u8; 8]).expect("8 bytes is valid"), b"");
    }

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
        assert_eq!(
            decode_tuple(&bytes).expect("decode"),
            (5, 9, vec![Datum::Int4(1)])
        );
    }

    #[test]
    fn decode_tuple_rejects_corrupt() {
        assert!(decode_tuple(&[]).is_err());
        assert!(decode_tuple(&[99, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // bad tag
        assert!(decode_tuple(&[1, 0, 0]).is_err()); // too short for header
    }
}
