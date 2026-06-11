//! Durable Kv over a fjall LSM partition. Crash recovery is fjall's journal
//! replay on open; durability is fsync on each commit.

use std::path::Path;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

use crate::{Kv, KvError, WriteOp};

/// Durable key-value store backed by a single fjall keyspace (partition).
///
/// Opening an existing directory recovers via fjall's journal replay —
/// no bespoke recovery code required. Every write is fsynced before returning.
pub struct FjallKv {
    db: Database,
    ks: fjall::Keyspace,
}

impl FjallKv {
    /// Opens (or creates) a `FjallKv` at the given path.
    ///
    /// If the directory already contains a database, it is recovered via
    /// fjall's journal replay.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let db = Database::builder(path).open().map_err(io)?;
        let ks = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .map_err(io)?;
        Ok(Self { db, ks })
    }

    /// Flush the journal to disk (full fsync). Called as the TAIL of every
    /// mutating op (put/delete/write_batch) so the method returns `Ok` only
    /// after the data is power-loss durable. DO NOT refactor those calls to
    /// early-return before sync() — that would make a returned-Ok write
    /// survivable only across a clean process exit, not a power loss.
    fn sync(&self) -> Result<(), KvError> {
        self.db.persist(PersistMode::SyncAll).map_err(io)
    }
}

fn io(e: impl std::fmt::Display) -> KvError {
    KvError::Io(e.to_string())
}

impl Kv for FjallKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.ks.get(key).map_err(io)?.map(|v| v.to_vec()))
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.ks.insert(key, value).map_err(io)?;
        self.sync()
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.ks.remove(key).map_err(io)?;
        self.sync()
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        let mut out = Vec::new();
        for guard in self.ks.prefix(prefix) {
            let (k, v) = guard.into_inner().map_err(io)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut batch = self.db.batch();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.insert(&self.ks, key, value),
                WriteOp::Delete { key } => batch.remove(&self.ks, key),
            }
        }
        batch.commit().map_err(io)?;
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteOp;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn put_get_delete_durable() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        assert_eq!(kv.get(b"a").expect("get"), None);
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        kv.delete(b"a").expect("delete");
        assert_eq!(kv.get(b"a").expect("get"), None);
    }

    #[test]
    fn scan_prefix_ordered_matches_only() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"t/1/b".to_vec(), b"B".to_vec()).expect("put");
        kv.put(b"t/1/a".to_vec(), b"A".to_vec()).expect("put");
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()).expect("put");
        assert_eq!(
            kv.scan_prefix(b"t/1/").expect("scan"),
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }

    #[test]
    fn write_batch_is_atomic() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            WriteOp::Delete {
                key: b"keep".to_vec(),
            },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }

    #[test]
    fn data_survives_reopen() {
        let dir = temp();
        {
            let kv = FjallKv::open(dir.path()).expect("open");
            kv.put(b"persist".to_vec(), b"yes".to_vec()).expect("put");
            // kv dropped here — must have fsynced.
        }
        let kv = FjallKv::open(dir.path()).expect("reopen");
        assert_eq!(kv.get(b"persist").expect("get"), Some(b"yes".to_vec()));
    }
}
