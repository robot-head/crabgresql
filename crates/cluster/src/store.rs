//! In-memory openraft log + state-machine storage for the single range (T2).
//!
//! Two crabgresql-specific deltas from openraft's `raft-kv-memstore` example:
//!   1. The state-machine data is an [`Arc<MemKv>`] (shared with the SQL engine),
//!      not a `BTreeMap<String, String>`.
//!   2. [`apply`] routes each op through [`apply_op`], which **max-merges** the
//!      two monotonic counter keys so out-of-order Raft application can never
//!      regress them. Snapshots are authoritative and overwrite (no merge).
//!
//! Both stores are validated by openraft's own conformance suite
//! (`openraft::testing::Suite::test_all`) — see the tests module.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use kv::{Kv, MemKv, WriteOp};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftTypeConfig,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig, WriteBatch};

// ---------------------------------------------------------------------------
// Counter max-merge
// ---------------------------------------------------------------------------

/// Apply one op to the state-machine store. The two monotonic counter keys
/// (`next_xid`, any table's `seq`) take the MAX of the existing and incoming
/// big-endian u64 so out-of-order Raft application never regresses them; every
/// other key is a plain put/delete.
fn apply_op(kv: &MemKv, op: &WriteOp) {
    match op {
        WriteOp::Put { key, value } if is_counter_key(key) => {
            let incoming = u64_be(value);
            let existing = kv
                .get(key)
                .expect("memkv get")
                .map(|b| u64_be(&b))
                .unwrap_or(0);
            let merged = existing.max(incoming);
            kv.put(key.clone(), merged.to_be_bytes().to_vec())
                .expect("memkv put");
        }
        WriteOp::Put { key, value } => {
            kv.put(key.clone(), value.clone()).expect("memkv put");
        }
        WriteOp::Delete { key } => {
            kv.delete(key).expect("memkv delete");
        }
    }
}

/// True for `/0/meta/next_xid` and any `/0/seq/<table>` key.
fn is_counter_key(key: &[u8]) -> bool {
    key == kv::key::next_xid_key().as_slice() || is_seq_key(key)
}

fn is_seq_key(key: &[u8]) -> bool {
    // `/0/seq/<u32>` — table id varies, so compare the constant prefix and length.
    let prefix = kv::key::seq_key(0);
    let plen = prefix.len() - 4; // drop the 4-byte table-id suffix of seq_key(0)
    key.len() == prefix.len() && key[..plen] == prefix[..plen]
}

fn u64_be(b: &[u8]) -> u64 {
    let a: [u8; 8] = b.try_into().expect("counter value is u64");
    u64::from_be_bytes(a)
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// The serialized form of a state-machine snapshot: the full KV contents plus
/// the metadata openraft needs to resume.
#[derive(Serialize, Deserialize, Debug, Default)]
struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// All `(key, value)` pairs of the state-machine KV at snapshot time.
    kv: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A stored snapshot: openraft metadata plus the serialized [`SnapshotPayload`].
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Metadata tracked alongside the shared KV (which holds the application data).
#[derive(Debug, Default)]
struct StateMachineMeta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// The Raft state machine for the single range. The application data lives in a
/// shared [`Arc<MemKv>`] so the SQL engine can read it directly; Raft-specific
/// metadata and the last snapshot live alongside it.
#[derive(Default)]
pub struct StateMachineStore {
    /// Shared application data. Reads from the SQL engine go straight here.
    kv: Arc<MemKv>,
    /// `last_applied` / `last_membership`, guarded for async access.
    meta: RwLock<StateMachineMeta>,
    /// Monotonic snapshot index for unique snapshot ids.
    snapshot_idx: RwLock<u64>,
    /// The last snapshot built or installed.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

// `MemKv` is not `Debug`; openraft only needs a `Debug` bound for diagnostics.
impl Debug for StateMachineStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachineStore").finish_non_exhaustive()
    }
}

impl StateMachineStore {
    /// The shared application-data store. Cloning the `Arc` lets the SQL engine
    /// read committed state without going through Raft.
    pub fn sm_kv(&self) -> Arc<MemKv> {
        self.kv.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let meta = self.meta.read().await;
            (meta.last_applied, meta.last_membership.clone())
        };

        // Snapshot the full KV contents.
        let kv = self.kv.scan_prefix(&[]).expect("memkv scan");

        let payload = SnapshotPayload {
            last_applied,
            last_membership: last_membership.clone(),
            kv,
        };
        let data = serde_json::to_vec(&payload).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshot_idx = {
            let mut idx = self.snapshot_idx.write().await;
            *idx += 1;
            *idx
        };
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        *self.current_snapshot.write().await = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let meta = self.meta.read().await;
        Ok((meta.last_applied, meta.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        let mut meta = self.meta.write().await;

        for entry in entries {
            meta.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(WriteBatch(ref ops)) => {
                    for op in ops {
                        apply_op(&self.kv, op);
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    meta.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                }
            }
            res.push(());
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let payload: SnapshotPayload = serde_json::from_slice(&stored.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(stored.meta.signature()), &e))?;

        // A snapshot is authoritative: clear the KV and overwrite (NOT max-merge).
        let existing = self.kv.scan_prefix(&[]).expect("memkv scan");
        for (k, _) in existing {
            self.kv.delete(&k).expect("memkv delete");
        }
        for (k, v) in payload.kv {
            self.kv.put(k, v).expect("memkv put");
        }

        {
            let mut sm_meta = self.meta.write().await;
            sm_meta.last_applied = meta.last_log_id;
            sm_meta.last_membership = meta.last_membership.clone();
        }

        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Log store
// ---------------------------------------------------------------------------

/// In-memory Raft log: a `BTreeMap<index, Entry>` plus vote / committed / purge
/// bookkeeping. A standard openraft in-memory log store.
#[derive(Debug, Default)]
pub struct LogStore {
    inner: RwLock<LogStoreInner>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl RaftLogReader<TypeConfig> for Arc<LogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner
            .log
            .range(range)
            .map(|(_, ent)| ent.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for Arc<LogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        let last = inner.log.iter().next_back().map(|(_, ent)| ent.log_id);
        let last_purged = inner.last_purged_log_id;
        let last = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.committed)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.vote)
    }

    #[tracing::instrument(level = "trace", skip(self, entries, callback))]
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        {
            let mut inner = self.inner.write().await;
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory store flushes synchronously; signal completion immediately.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let keys = inner
            .log
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        assert!(inner.last_purged_log_id <= Some(log_id));
        inner.last_purged_log_id = Some(log_id);
        let keys = inner
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use openraft::testing::StoreBuilder;

    use super::*;

    /// Builds a fresh `(LogStore, StateMachineStore)` pair for the conformance
    /// suite. The blanket closure `StoreBuilder` impl only covers the combined
    /// `RaftStorage` (via `Adaptor`), so the split storage needs an explicit impl.
    #[derive(Default)]
    struct MemStoreBuilder;

    impl StoreBuilder<TypeConfig, Arc<LogStore>, Arc<StateMachineStore>, ()> for MemStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), Arc<LogStore>, Arc<StateMachineStore>), StorageError<NodeId>> {
            Ok((
                (),
                Arc::new(LogStore::default()),
                Arc::new(StateMachineStore::default()),
            ))
        }
    }

    /// openraft's own storage conformance suite over our log store + state machine.
    #[test]
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    fn openraft_storage_suite() -> Result<(), StorageError<NodeId>> {
        openraft::testing::Suite::test_all(MemStoreBuilder)
    }

    #[test]
    fn counter_keys_max_merge_never_regress() {
        let kv = MemKv::new();
        let k = kv::key::next_xid_key();
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: 12u64.to_be_bytes().to_vec(),
            },
        );
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: 11u64.to_be_bytes().to_vec(),
            },
        ); // out of order
        assert_eq!(
            u64_be(&kv.get(&k).expect("get").expect("present")),
            12,
            "max-merge must not regress"
        );
    }

    #[test]
    fn seq_keys_also_max_merge() {
        let kv = MemKv::new();
        let k = kv::key::seq_key(7);
        assert!(is_counter_key(&k), "seq keys must be treated as counters");
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: 100u64.to_be_bytes().to_vec(),
            },
        );
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: 50u64.to_be_bytes().to_vec(),
            },
        );
        assert_eq!(u64_be(&kv.get(&k).expect("get").expect("present")), 100);
    }

    #[test]
    fn non_counter_keys_are_last_writer_wins() {
        let kv = MemKv::new();
        let k = kv::key::row_key(1, 1);
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: b"a".to_vec(),
            },
        );
        apply_op(
            &kv,
            &WriteOp::Put {
                key: k.clone(),
                value: b"b".to_vec(),
            },
        );
        assert_eq!(kv.get(&k).expect("get"), Some(b"b".to_vec()));
    }

    /// Build a snapshot from one store, install it into a fresh store, and
    /// assert the KV contents round-trip exactly.
    #[tokio::test]
    async fn snapshot_round_trip_overwrites() {
        let mut src = Arc::new(StateMachineStore::default());
        let kv = src.sm_kv();
        // Some application data plus a counter key.
        apply_op(
            &kv,
            &WriteOp::Put {
                key: kv::key::row_key(1, 1),
                value: b"hello".to_vec(),
            },
        );
        apply_op(
            &kv,
            &WriteOp::Put {
                key: kv::key::next_xid_key(),
                value: 42u64.to_be_bytes().to_vec(),
            },
        );
        let expected = kv.scan_prefix(&[]).expect("scan");

        let snapshot = src.build_snapshot().await.expect("build snapshot");

        // Fresh store with pre-existing junk that must be overwritten.
        let mut dst = Arc::new(StateMachineStore::default());
        dst.sm_kv()
            .put(kv::key::row_key(9, 9), b"junk".to_vec())
            .expect("put junk");
        dst.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let got = dst.sm_kv().scan_prefix(&[]).expect("scan");
        assert_eq!(got, expected, "snapshot must reproduce KV exactly");
        // Junk key must be gone (overwrite, not merge).
        assert_eq!(
            dst.sm_kv().get(&kv::key::row_key(9, 9)).expect("get"),
            None,
            "install_snapshot must clear prior state"
        );
    }
}
