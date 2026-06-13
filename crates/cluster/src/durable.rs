//! Durable per-node storage (D2a): one fjall `Database` per node with a `data`
//! keyspace (the state-machine DB content) and a `raft` keyspace (log entries,
//! vote, committed, last_applied, membership). A durable `LogStore` and
//! `StateMachineStore` share the Database (Task 3), so an apply commits data +
//! metadata in one cross-keyspace batch + one fsync.

use std::path::Path;
use std::sync::Arc;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use kv::KeyspaceKv;

/// One node's on-disk store: a shared `Database` plus its two keyspaces.
pub struct NodeStore {
    pub(crate) db: Arc<Database>,
    pub(crate) data: fjall::Keyspace,
    pub(crate) raft: fjall::Keyspace,
}

impl NodeStore {
    /// Open (or recover) a node store at `dir`. fjall journal-replays on open.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, kv::KvError> {
        let db = Arc::new(
            Database::builder(dir)
                .open()
                .map_err(|e| kv::KvError::Io(e.to_string()))?,
        );
        let data = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .map_err(|e| kv::KvError::Io(e.to_string()))?;
        let raft = db
            .keyspace("raft", KeyspaceCreateOptions::default)
            .map_err(|e| kv::KvError::Io(e.to_string()))?;
        Ok(Self { db, data, raft })
    }

    /// A `Kv` view over the `data` keyspace for the SQL engine + SM reads.
    #[allow(dead_code)] // consumed by Task 3 (durable state machine).
    pub fn data_kv(&self) -> Arc<KeyspaceKv> {
        Arc::new(KeyspaceKv::new(self.db.clone(), self.data.clone()))
    }
}

/// `log/<index-be>` — the 8-byte big-endian index sorts lexicographically by
/// index, so a prefix scan over `b"log/"` yields entries in index order.
fn log_key(index: u64) -> Vec<u8> {
    let mut k = b"log/".to_vec();
    k.extend_from_slice(&index.to_be_bytes());
    k
}

/// Decode the index from a `log/<index-be>` key, or `None` if malformed.
fn log_index(key: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(LOG_PREFIX)?;
    let bytes: [u8; 8] = suffix.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

const LOG_PREFIX: &[u8] = b"log/";
const VOTE_KEY: &[u8] = b"vote";
const COMMITTED_KEY: &[u8] = b"committed";
const PURGED_KEY: &[u8] = b"last_purged";
#[allow(dead_code)] // consumed by Task 3 (durable state machine).
pub(crate) const SM_APPLIED_KEY: &[u8] = b"sm/last_applied";
#[allow(dead_code)] // consumed by Task 3 (durable state machine).
pub(crate) const SM_MEMBERSHIP_KEY: &[u8] = b"sm/last_membership";

// ---------------------------------------------------------------------------
// Durable log store
// ---------------------------------------------------------------------------

use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, RaftLogReader, StorageError, StorageIOError, Vote};
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig};

/// Durable Raft log over the `raft` keyspace of a shared `Database`. Mirrors the
/// in-memory [`crate::store::LogStore`] method-for-method, but each mutation is
/// fsynced (`persist`) before it acks — openraft's durability contract.
pub struct DurableLogStore {
    db: Arc<Database>,
    ks: fjall::Keyspace, // the `raft` keyspace
    cache: RwLock<LogCache>,
}

/// O(1) bookkeeping reconstructed from disk on open and maintained on mutation.
#[derive(Default)]
struct LogCache {
    last_log_id: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
}

impl DurableLogStore {
    /// Open the durable log over `store`'s `raft` keyspace, reconstructing the
    /// `last_log_id` / `last_purged` cache from disk (fjall already replayed).
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    pub fn open(store: &NodeStore) -> Result<Arc<Self>, StorageError<NodeId>> {
        let db = store.db.clone();
        let ks = store.raft.clone();
        let last_purged: Option<LogId<NodeId>> = read_json(&ks, PURGED_KEY)?;
        let last_log_id = highest_log_id(&ks)?.or(last_purged);
        Ok(Arc::new(Self {
            db,
            ks,
            cache: RwLock::new(LogCache {
                last_log_id,
                last_purged,
            }),
        }))
    }

    /// fsync the whole Database. Called as the tail of every mutation so a
    /// returned `Ok` (and any subsequent callback) is power-loss durable.
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    fn persist(&self) -> Result<(), StorageError<NodeId>> {
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }
}

// --- JSON helpers over the raft keyspace -----------------------------------

#[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
fn read_json<T: serde::de::DeserializeOwned>(
    ks: &fjall::Keyspace,
    key: &[u8],
) -> Result<Option<T>, StorageError<NodeId>> {
    match ks.get(key).map_err(|e| StorageIOError::read_logs(&e))? {
        Some(b) => Ok(Some(
            serde_json::from_slice(&b).map_err(|e| StorageIOError::read_logs(&e))?,
        )),
        None => Ok(None),
    }
}

#[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
fn write_json<T: serde::Serialize>(
    ks: &fjall::Keyspace,
    key: &[u8],
    v: &T,
) -> Result<(), StorageError<NodeId>> {
    let bytes = serde_json::to_vec(v).map_err(|e| StorageIOError::write_logs(&e))?;
    ks.insert(key, bytes)
        .map_err(|e| StorageIOError::write_logs(&e))?;
    Ok(())
}

/// Highest stored log entry's `log_id`. Keys are big-endian sorted, so the
/// last key in the `log/` prefix is the highest index — read only it (O(1)).
#[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
fn highest_log_id(ks: &fjall::Keyspace) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
    // `fjall::Iter` implements `DoubleEndedIterator`, so `next_back` seeks
    // directly to the last entry without scanning earlier keys.
    if let Some(guard) = ks.prefix(LOG_PREFIX).next_back() {
        let (_k, v) = guard
            .into_inner()
            .map_err(|e| StorageIOError::read_logs(&e))?;
        let entry: Entry<TypeConfig> =
            serde_json::from_slice(&v).map_err(|e| StorageIOError::read_logs(&e))?;
        return Ok(Some(entry.log_id));
    }
    Ok(None)
}

impl RaftLogReader<TypeConfig> for Arc<DurableLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let mut out = Vec::new();
        for guard in self.ks.prefix(LOG_PREFIX) {
            let (k, v) = guard
                .into_inner()
                .map_err(|e| StorageIOError::read_logs(&e))?;
            // Keys sort by index; decode it to range-check without deserializing
            // entries outside the requested range.
            let Some(index) = log_index(&k) else { continue };
            if !range.contains(&index) {
                continue;
            }
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(&v).map_err(|e| StorageIOError::read_logs(&e))?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for Arc<DurableLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let cache = self.cache.read().await;
        Ok(LogState {
            last_purged_log_id: cache.last_purged,
            last_log_id: cache.last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        write_json(&self.ks, COMMITTED_KEY, &committed)?;
        self.persist()
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        read_json(&self.ks, COMMITTED_KEY)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        write_json(&self.ks, VOTE_KEY, vote)?;
        self.persist()
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        read_json(&self.ks, VOTE_KEY)
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
        let mut batch = self.db.batch();
        let mut max_log_id: Option<LogId<NodeId>> = None;
        for entry in entries {
            let bytes = serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
            batch.insert(&self.ks, log_key(entry.log_id.index), bytes);
            if max_log_id.is_none_or(|m| entry.log_id.index > m.index) {
                max_log_id = Some(entry.log_id);
            }
        }
        batch.commit().map_err(|e| StorageIOError::write_logs(&e))?;
        if let Some(id) = max_log_id {
            let mut cache = self.cache.write().await;
            if cache.last_log_id.is_none_or(|l| id.index > l.index) {
                cache.last_log_id = Some(id);
            }
        }
        // Durability BEFORE the callback — openraft's contract.
        self.persist()?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove every entry with index >= log_id.index.
        let mut keys = Vec::new();
        for guard in self.ks.prefix(LOG_PREFIX) {
            let (k, _v) = guard
                .into_inner()
                .map_err(|e| StorageIOError::read_logs(&e))?;
            if let Some(index) = log_index(&k)
                && index >= log_id.index
            {
                keys.push(k.to_vec());
            }
        }
        let mut batch = self.db.batch();
        for k in &keys {
            batch.remove(&self.ks, k.as_slice());
        }
        batch.commit().map_err(|e| StorageIOError::write_logs(&e))?;
        // Recompute last_log_id = highest remaining entry, or last_purged.
        {
            let mut cache = self.cache.write().await;
            cache.last_log_id = highest_log_id(&self.ks)?.or(cache.last_purged);
        }
        self.persist()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let cache = self.cache.read().await;
            assert!(cache.last_purged <= Some(log_id));
        }
        // Remove every entry with index <= log_id.index.
        let mut keys = Vec::new();
        for guard in self.ks.prefix(LOG_PREFIX) {
            let (k, _v) = guard
                .into_inner()
                .map_err(|e| StorageIOError::read_logs(&e))?;
            if let Some(index) = log_index(&k)
                && index <= log_id.index
            {
                keys.push(k.to_vec());
            }
        }
        let purged_bytes =
            serde_json::to_vec(&Some(log_id)).map_err(|e| StorageIOError::write_logs(&e))?;
        let mut batch = self.db.batch();
        for k in &keys {
            batch.remove(&self.ks, k.as_slice());
        }
        batch.insert(&self.ks, PURGED_KEY, purged_bytes);
        batch.commit().map_err(|e| StorageIOError::write_logs(&e))?;
        {
            let mut cache = self.cache.write().await;
            cache.last_purged = Some(log_id);
            // last_log_id never goes below last_purged.
            if cache.last_log_id.is_none_or(|l| l.index < log_id.index) {
                cache.last_log_id = Some(log_id);
            }
        }
        self.persist()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use openraft::storage::RaftLogStorageExt;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    use super::*;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn blank_entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    /// Append one blank entry at `index`, blocking until the flush callback runs
    /// (this is the public, durability-respecting append path in tests).
    async fn append_blank(log: &mut Arc<DurableLogStore>, index: u64) {
        log.blocking_append([blank_entry(index)])
            .await
            .expect("append");
    }

    fn a_vote() -> Vote<NodeId> {
        Vote::new(3, 1)
    }

    #[tokio::test]
    async fn append_then_reopen_recovers_entries() {
        let dir = temp();
        {
            let store = NodeStore::open(dir.path()).expect("open");
            let mut log = DurableLogStore::open(&store).expect("log open");
            for i in 1..=3 {
                append_blank(&mut log, i).await;
            }
            log.save_vote(&a_vote()).await.expect("save vote");
            // Everything dropped here — must have fsynced before each ack.
        }
        let store = NodeStore::open(dir.path()).expect("reopen");
        let mut log = DurableLogStore::open(&store).expect("log reopen");
        let state = log.get_log_state().await.expect("state");
        assert_eq!(
            state.last_log_id.map(|l| l.index),
            Some(3),
            "last_log_id must survive reopen"
        );
        assert_eq!(log.read_vote().await.expect("vote"), Some(a_vote()));
        let entries = log.try_get_log_entries(1..=3).await.expect("entries");
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn truncate_removes_tail() {
        let dir = temp();
        let store = NodeStore::open(dir.path()).expect("open");
        let mut log = DurableLogStore::open(&store).expect("log open");
        for i in 1..=5 {
            append_blank(&mut log, i).await;
        }
        // Truncate from index 3 onward: keep 1, 2.
        log.truncate(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .expect("truncate");
        let state = log.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id.map(|l| l.index), Some(2));
        let entries = log.try_get_log_entries(..).await.expect("entries");
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2]
        );

        // Survives reopen.
        drop(log);
        drop(store);
        let store = NodeStore::open(dir.path()).expect("reopen");
        let mut log = DurableLogStore::open(&store).expect("log reopen");
        assert_eq!(
            log.get_log_state()
                .await
                .expect("state")
                .last_log_id
                .map(|l| l.index),
            Some(2)
        );
    }

    #[tokio::test]
    async fn purge_removes_head_and_sets_purged() {
        let dir = temp();
        let store = NodeStore::open(dir.path()).expect("open");
        let mut log = DurableLogStore::open(&store).expect("log open");
        for i in 1..=5 {
            append_blank(&mut log, i).await;
        }
        // Purge up to index 2: remove 1, 2; last_purged = 2.
        let purged = LogId::new(CommittedLeaderId::new(1, 0), 2);
        log.purge(purged).await.expect("purge");
        let state = log.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(2));
        assert_eq!(state.last_log_id.map(|l| l.index), Some(5));
        let entries = log.try_get_log_entries(..).await.expect("entries");
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        // Survives reopen: purged head stays gone, last_purged reconstructed.
        drop(log);
        drop(store);
        let store = NodeStore::open(dir.path()).expect("reopen");
        let mut log = DurableLogStore::open(&store).expect("log reopen");
        let state = log.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(2));
        assert_eq!(state.last_log_id.map(|l| l.index), Some(5));
    }
}
