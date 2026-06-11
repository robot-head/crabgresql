//! The key-value storage seam. SP2 ships an in-memory `MemKv`; SP3 swaps in a
//! durable LSM behind the same `Kv` trait, SP4 shards it into Raft ranges.

use std::collections::BTreeMap;
use std::sync::RwLock;

/// An ordered byte-key/byte-value store. Synchronous for SP2; the distributed
/// layer will introduce an async, transactional variant behind this boundary.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&self, key: &[u8]);
    /// All (key, value) pairs whose key starts with `prefix`, in key order.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
}

/// In-memory ordered store backed by a BTreeMap.
#[derive(Default)]
pub struct MemKv {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Kv for MemKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.read().expect("kv lock").get(key).cloned()
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.map.write().expect("kv lock").insert(key, value);
    }

    fn delete(&self, key: &[u8]) {
        self.map.write().expect("kv lock").remove(key);
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .read()
            .expect("kv lock")
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let kv = MemKv::new();
        assert_eq!(kv.get(b"a"), None);
        kv.put(b"a".to_vec(), b"1".to_vec());
        assert_eq!(kv.get(b"a"), Some(b"1".to_vec()));
        kv.delete(b"a");
        assert_eq!(kv.get(b"a"), None);
    }

    #[test]
    fn scan_prefix_returns_sorted_matches_only() {
        let kv = MemKv::new();
        kv.put(b"t/1/b".to_vec(), b"B".to_vec());
        kv.put(b"t/1/a".to_vec(), b"A".to_vec());
        kv.put(b"t/2/a".to_vec(), b"X".to_vec());
        let rows = kv.scan_prefix(b"t/1/");
        assert_eq!(
            rows,
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }
}
