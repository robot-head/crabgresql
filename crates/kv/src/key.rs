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

/// Key for the global commit-timestamp clock: `/0/meta/commit_ts`.
pub fn commit_ts_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"commit_ts");
    k
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
    fn system_keys_do_not_collide_with_user_rows() {
        // User rows use table_id >= 1; system keys use table_id 0.
        assert!(!catalog_key("t").starts_with(&table_prefix(1)));
        assert!(!seq_key(1).starts_with(&table_prefix(1)));
    }
}
