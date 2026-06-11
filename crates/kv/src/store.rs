//! The key-value storage seam. SP2 ships an in-memory `MemKv`; SP3 swaps in a
//! durable LSM behind the same `Kv` trait, SP4 shards it into Raft ranges.

use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::KvError;

/// One mutation in an atomic batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// An ordered byte-key/byte-value store. Synchronous for SP3; the distributed
/// layer will introduce an async, transactional variant behind this boundary.
/// All methods are fallible because a durable backend can hit I/O errors.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    fn delete(&self, key: &[u8]) -> Result<(), KvError>;
    /// All (key, value) pairs whose key starts with `prefix`, in key order.
    #[allow(clippy::type_complexity)]
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError>;
    /// Apply all ops atomically and durably (fsync on a durable backend).
    /// All-or-nothing across a crash.
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError>;
}

/// In-memory ordered store backed by a BTreeMap. Infallible internally; returns
/// `Ok` to satisfy the fallible trait. Used for tests and the ephemeral default.
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
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.map.read().expect("kv lock").get(key).cloned())
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.map.write().expect("kv lock").insert(key, value);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.map.write().expect("kv lock").remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        Ok(self
            .map
            .read()
            .expect("kv lock")
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        // One lock acquisition = atomic against concurrent readers.
        let mut map = self.map.write().expect("kv lock");
        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    map.insert(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    map.remove(key);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let kv = MemKv::new();
        assert_eq!(kv.get(b"a").expect("get"), None);
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        kv.delete(b"a").expect("delete");
        assert_eq!(kv.get(b"a").expect("get"), None);
    }

    #[test]
    fn scan_prefix_returns_sorted_matches_only() {
        let kv = MemKv::new();
        kv.put(b"t/1/b".to_vec(), b"B".to_vec()).expect("put");
        kv.put(b"t/1/a".to_vec(), b"A".to_vec()).expect("put");
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()).expect("put");
        let rows = kv.scan_prefix(b"t/1/").expect("scan");
        assert_eq!(
            rows,
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }

    #[test]
    fn write_batch_applies_all_ops() {
        let kv = MemKv::new();
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            WriteOp::Put {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            WriteOp::Delete {
                key: b"keep".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"b").expect("get"), Some(b"2".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }
}
