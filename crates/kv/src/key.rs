//! Key construction: `/<table_id>/<index_id>/<rowid>`. The primary "index" is
//! id 1; secondary indexes (later) get higher ids under the same table prefix.

use crate::KvError;
use crate::keyenc::{put_u32, put_u64, take_u32, take_u64};

/// The primary storage index for a table's rows.
pub const INDEX_PRIMARY: u32 = 1;

/// Bytes shared by every row of a table's primary index.
pub fn table_prefix(table_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(8);
    put_u32(&mut k, table_id);
    put_u32(&mut k, INDEX_PRIMARY);
    k
}

/// Full key for one row: table prefix followed by the order-preserving rowid.
pub fn row_key(table_id: u32, rowid: u64) -> Vec<u8> {
    let mut k = table_prefix(table_id);
    put_u64(&mut k, rowid);
    k
}

/// Reserved table id for system metadata (catalog, sequences, global meta).
pub const SYSTEM_TABLE_ID: u32 = 0;

fn system_prefix(tag: &str) -> Vec<u8> {
    let mut k = Vec::new();
    put_u32(&mut k, SYSTEM_TABLE_ID);
    k.extend_from_slice(tag.as_bytes());
    k.push(b'/');
    k
}

/// Key for a table's stored schema: `/0/catalog/<name>`.
pub fn catalog_key(table_name: &str) -> Vec<u8> {
    let mut k = system_prefix("catalog");
    k.extend_from_slice(table_name.as_bytes());
    k
}

/// Key for a table's next-rowid sequence: `/0/seq/<table_id>`.
pub fn seq_key(table_id: u32) -> Vec<u8> {
    let mut k = system_prefix("seq");
    put_u32(&mut k, table_id);
    k
}

/// Key for the global next-table-id counter: `/0/meta/next_table_id`.
pub fn meta_next_table_id_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_table_id");
    k
}

/// Key for the replicated range-descriptor blob: `/0/meta/range_map`.
pub fn meta_range_map_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"range_map");
    k
}

/// Key for the global next-transaction-id counter: `/0/meta/next_xid`.
pub fn next_xid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_xid");
    k
}

/// Key for the GTM's monotonic global-xid counter: `/0/meta/next_global_xid`.
/// Lives in range 0's store, disjoint from the per-range `next_xid` key.
pub fn meta_next_global_xid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_global_xid");
    k
}

/// Key for a DATA range's recovery-scan watermark: the smallest local xid `Li`
/// at/after which the leadership-rise recovery scan must still look. Lives in the
/// `meta` namespace (disjoint from the `/0/clog/` prefix, so a clog scan never
/// returns it). Stored per-range in that range's own store. Value = `Li` big-endian.
pub fn clog_scan_lo_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"clog_scan_lo");
    k
}

/// Exclusive upper bound for a scan over the whole `/0/clog/` keyspace: the clog
/// prefix with its trailing byte incremented (the prefix's successor). `clog_prefix`
/// is `system_prefix("clog")`, i.e. `…clog` followed by the `/` separator, so the
/// last byte `0x2f` increments to `0x30` with no carry. This is strictly greater than
/// `clog_key(u64::MAX)`, so it covers every clog entry. (Relies on the prefix never
/// ending in `0xFF` — true for the `/`-separated system prefixes.)
pub fn clog_scan_end() -> Vec<u8> {
    let mut p = clog_prefix();
    let last = p.last_mut().expect("clog prefix is non-empty");
    *last += 1; // 0x2f ('/') -> 0x30; no carry
    p
}

/// Key for a transaction's commit-status-log entry: `/0/clog/<xid>`.
pub fn clog_key(xid: u64) -> Vec<u8> {
    let mut k = system_prefix("clog");
    crate::keyenc::put_u64(&mut k, xid);
    k
}

/// The shared prefix of every `/0/clog/<xid>` entry (for the write-once apply
/// check + prefix scans). `clog_key(x)` is `clog_prefix() ++ put_u64(x)`.
pub fn clog_prefix() -> Vec<u8> {
    system_prefix("clog")
}

/// Decode the xid from a `/0/clog/<xid>` key, or `None` if `key` is not a clog key.
pub fn clog_xid_of(key: &[u8]) -> Option<u64> {
    let prefix = clog_prefix();
    if key.len() != prefix.len() + 8 || key[..prefix.len()] != prefix[..] {
        return None;
    }
    let mut rest = &key[prefix.len()..];
    crate::keyenc::take_u64(&mut rest).ok()
}

/// Recover the rowid from a key known to belong to `table_id`.
pub fn rowid_of(table_id: u32, key: &[u8]) -> Result<u64, KvError> {
    let mut cur = key;
    let t = take_u32(&mut cur)?;
    let idx = take_u32(&mut cur)?;
    if t != table_id || idx != INDEX_PRIMARY {
        return Err(KvError::CorruptRow(
            "key does not belong to this table index".into(),
        ));
    }
    take_u64(&mut cur)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_keys_sort_by_rowid_within_a_table() {
        let k1 = row_key(7, 1);
        let k2 = row_key(7, 2);
        let k10 = row_key(7, 10);
        assert!(k1 < k2 && k2 < k10, "rowid order must be byte order");
        assert!(k1.starts_with(&table_prefix(7)));
    }

    #[test]
    fn different_tables_do_not_share_a_prefix() {
        assert!(!row_key(8, 1).starts_with(&table_prefix(7)));
    }

    #[test]
    fn rowid_roundtrips_from_a_key() {
        let k = row_key(7, 42);
        assert_eq!(rowid_of(7, &k).expect("rowid"), 42);
    }

    #[test]
    fn rowid_of_rejects_wrong_table() {
        let k = row_key(7, 42);
        assert!(rowid_of(8, &k).is_err(), "wrong table id must be rejected");
    }

    #[test]
    fn system_keys_are_distinct_and_under_table_zero() {
        let cat = catalog_key("users");
        let seq = seq_key(7);
        let meta = meta_next_table_id_key();
        // All start with the reserved table-id 0 prefix.
        let zero = {
            let mut k = Vec::new();
            crate::keyenc::put_u32(&mut k, 0);
            k
        };
        assert!(cat.starts_with(&zero));
        assert!(seq.starts_with(&zero));
        assert!(meta.starts_with(&zero));
        // Distinct namespaces.
        assert_ne!(cat, seq);
        assert_ne!(seq, meta);
        assert_ne!(catalog_key("a"), catalog_key("b"));
        assert_ne!(seq_key(7), seq_key(8));
    }

    #[test]
    fn meta_next_global_xid_key_is_distinct_from_all_other_meta_keys() {
        let gxid = meta_next_global_xid_key();
        assert_ne!(gxid, next_xid_key(), "distinct from next_xid");
        assert_ne!(
            gxid,
            meta_next_table_id_key(),
            "distinct from next_table_id"
        );
        assert_ne!(gxid, meta_range_map_key(), "distinct from range_map");
        // Must be under the table-zero system prefix.
        let zero = {
            let mut k = Vec::new();
            crate::keyenc::put_u32(&mut k, 0);
            k
        };
        assert!(gxid.starts_with(&zero), "under table-zero prefix");
    }

    #[test]
    fn system_keys_do_not_collide_with_user_rows() {
        // User rows use table_id >= 1; system keys use table_id 0.
        assert!(!catalog_key("t").starts_with(&table_prefix(1)));
        assert!(!seq_key(1).starts_with(&table_prefix(1)));
    }

    #[test]
    fn meta_range_map_key_is_under_table_zero_meta() {
        let k = meta_range_map_key();
        let zero = {
            let mut z = Vec::new();
            crate::keyenc::put_u32(&mut z, 0);
            z
        };
        assert!(k.starts_with(&zero), "range map key is under table 0");
        assert_ne!(k, meta_next_table_id_key(), "distinct from next_table_id");
        assert_ne!(k, next_xid_key(), "distinct from next_xid");
    }

    #[test]
    fn xid_and_clog_keys_are_under_table_zero_and_distinct() {
        let zero = {
            let mut k = Vec::new();
            crate::keyenc::put_u32(&mut k, 0);
            k
        };
        assert!(next_xid_key().starts_with(&zero));
        assert!(clog_key(5).starts_with(&zero));
        assert_ne!(clog_key(5), clog_key(6));
        assert_ne!(next_xid_key(), meta_next_table_id_key());
        // clog keys sort by xid (order-preserving big-endian suffix).
        assert!(clog_key(5) < clog_key(6));
    }

    #[test]
    fn clog_scan_end_is_above_every_clog_key() {
        assert!(clog_scan_end() > clog_key(u64::MAX));
        // The watermark key is NOT inside the clog prefix (a clog scan won't return it).
        assert!(!clog_scan_lo_key().starts_with(&clog_prefix()));
    }
}
