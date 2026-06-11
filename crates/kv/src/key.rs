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
}
