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

/// Key for a foreign-data wrapper stored in the catalog: `/0/fdw/<name>`.
pub fn fdw_key(name: &str) -> Vec<u8> {
    let mut k = system_prefix("fdw");
    k.extend_from_slice(name.as_bytes());
    k
}

/// Key for a foreign server stored in the catalog: `/0/fsrv/<name>`.
pub fn server_key(name: &str) -> Vec<u8> {
    let mut k = system_prefix("fsrv");
    k.extend_from_slice(name.as_bytes());
    k
}

/// Key for a user mapping stored in the catalog: `/0/umap/<user>\0<server>`.
pub fn user_mapping_key(user: &str, server: &str) -> Vec<u8> {
    let mut k = system_prefix("umap");
    k.extend_from_slice(user.as_bytes());
    k.push(0);
    k.extend_from_slice(server.as_bytes());
    k
}

/// Shared prefix for all foreign-server entries (for listing / IMPORT scans).
pub fn server_prefix() -> Vec<u8> {
    system_prefix("fsrv")
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

/// Recover `(table_id, rowid)` from a primary-index row/version key
/// (`put_u32(table) ++ put_u32(INDEX_PRIMARY) ++ put_u64(rowid) ++ ...`). Returns
/// `None` for any key that is not a primary-index row key (a system key, a
/// non-primary index, or a too-short key) — so a caller scanning a heterogeneous
/// op batch can filter row versions without knowing each op's table up front.
pub fn table_rowid_of(key: &[u8]) -> Option<(u32, u64)> {
    let mut cur = key;
    let t = take_u32(&mut cur).ok()?;
    let idx = take_u32(&mut cur).ok()?;
    if t == SYSTEM_TABLE_ID || idx != INDEX_PRIMARY {
        return None;
    }
    let rowid = take_u64(&mut cur).ok()?;
    Some((t, rowid))
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
        // Pin the recovery-scan window `[clog_prefix(), clog_scan_end())`: every clog
        // key falls inside it, and the watermark key falls OUTSIDE it (so a range scan
        // of the clog can never sweep the watermark, regardless of meta-key ordering).
        assert!(clog_key(0) >= clog_prefix() && clog_key(0) < clog_scan_end());
        let lo = clog_scan_lo_key();
        assert!(lo < clog_prefix() || lo >= clog_scan_end());
    }

    #[test]
    fn clog_scan_lo_key_is_a_distinct_table_zero_meta_key() {
        let lo = clog_scan_lo_key();
        let zero = {
            let mut z = Vec::new();
            crate::keyenc::put_u32(&mut z, 0);
            z
        };
        assert!(lo.starts_with(&zero), "under the table-zero system prefix");
        // Lives in the meta namespace, NOT the clog prefix — a clog scan must
        // never sweep the watermark.
        assert!(!lo.starts_with(&clog_prefix()));
        // Distinct from every other system/meta key.
        assert_ne!(lo, next_xid_key());
        assert_ne!(lo, meta_next_table_id_key());
        assert_ne!(lo, meta_next_global_xid_key());
        assert_ne!(lo, meta_range_map_key());
        assert_ne!(lo, clog_key(0));
    }

    #[test]
    fn clog_xid_of_roundtrips_only_clog_keys() {
        // A real clog key decodes back to its xid (across the value range).
        assert_eq!(clog_xid_of(&clog_key(42)), Some(42));
        assert_eq!(clog_xid_of(&clog_key(0)), Some(0));
        assert_eq!(clog_xid_of(&clog_key(u64::MAX)), Some(u64::MAX));
        // A key in a different namespace is not a clog key.
        assert_eq!(clog_xid_of(&next_xid_key()), None);
        // Right LENGTH but wrong prefix is rejected (guards the prefix check, not
        // merely the length check).
        let mut wrong = clog_key(42);
        wrong[0] ^= 0xFF;
        assert_eq!(clog_xid_of(&wrong), None);
        // Prefix only, no 8-byte xid suffix → too short.
        assert_eq!(clog_xid_of(&clog_prefix()), None);
    }

    // ── SP40: FDW / server / user-mapping key tests ─────────────────────────

    /// fdw_key, server_key, user_mapping_key must be non-empty and distinct
    /// from each other and from catalog_key — kills the vec![] / vec![0] /
    /// vec![1] replacement mutants on all three functions.
    #[test]
    fn fdw_server_umap_keys_are_non_empty_and_distinct() {
        let fdw_a = fdw_key("a");
        let fdw_b = fdw_key("b");
        let srv_a = server_key("a");
        let umap = user_mapping_key("alice", "s");
        let cat = catalog_key("a");

        // Non-empty (kills vec![]).
        assert!(!fdw_a.is_empty());
        assert!(!srv_a.is_empty());
        assert!(!umap.is_empty());

        // Name-differentiated (kills vec![0] and vec![1] which are the same for every call).
        assert_ne!(fdw_a, fdw_b, "fdw_key includes the name");
        assert_ne!(srv_a, server_key("b"), "server_key includes the name");
        assert_ne!(
            user_mapping_key("alice", "s"),
            user_mapping_key("bob", "s"),
            "user_mapping_key includes the user"
        );
        assert_ne!(
            user_mapping_key("alice", "s1"),
            user_mapping_key("alice", "s2"),
            "user_mapping_key includes the server"
        );

        // Namespaces are disjoint (kills any mutant that returns a sibling key).
        assert_ne!(fdw_a, srv_a, "fdw and server keys are distinct");
        assert_ne!(fdw_a, cat, "fdw and catalog keys are distinct");
        assert_ne!(srv_a, cat, "server and catalog keys are distinct");
        assert_ne!(umap, srv_a, "umap and server keys are distinct");
    }

    /// server_prefix must be a non-empty proper prefix of every server_key — kills
    /// the vec![] / vec![0] / vec![1] replacement mutants on server_prefix.
    #[test]
    fn server_prefix_is_a_prefix_of_server_key() {
        let prefix = server_prefix();
        assert!(!prefix.is_empty(), "server_prefix must be non-empty");
        assert!(
            server_key("kafka").starts_with(&prefix),
            "server_key starts with server_prefix"
        );
        assert!(
            server_key("pg").starts_with(&prefix),
            "server_key starts with server_prefix"
        );
        // Must NOT be a prefix of fdw_key or catalog_key (namespaces are disjoint).
        assert!(
            !fdw_key("x").starts_with(&prefix),
            "server_prefix does not cover fdw keys"
        );
        assert!(
            !catalog_key("t").starts_with(&prefix),
            "server_prefix does not cover catalog keys"
        );
    }
}
