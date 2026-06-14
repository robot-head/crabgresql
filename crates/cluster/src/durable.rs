//! Durable per-node storage (D2a): one fjall `Database` per node with a `data`
//! keyspace (the state-machine DB content) and a `raft` keyspace (log entries,
//! vote, committed, last_applied, membership). A durable `LogStore` and
//! `StateMachineStore` share the Database (Task 3), so an apply commits data +
//! metadata in one cross-keyspace batch + one fsync.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use kv::KeyspaceKv;

use crate::range::map::{RangeId, RangeMap};

/// One range's on-disk keyspace pair within a node's shared `Database`:
/// `data-r{r}` (state-machine application KV) and `raft-r{r}` (log entries, vote,
/// committed, last_applied, membership). fjall keyspaces are isolated by
/// construction, so two ranges can never alias each other's state.
#[derive(Clone)]
pub struct RangeKeyspaces {
    pub data: fjall::Keyspace,
    pub raft: fjall::Keyspace,
}

/// One node's on-disk store: a shared `Database` plus, for each range it hosts,
/// a `data-r{r}` / `raft-r{r}` keyspace pair. A single-range node has exactly the
/// `data-r0` / `raft-r0` pair.
pub struct NodeStore {
    pub(crate) db: Arc<Database>,
    ranges: BTreeMap<RangeId, RangeKeyspaces>,
}

impl NodeStore {
    /// Open (or recover) a node store at `dir` hosting every range in `map`.
    /// fjall journal-replays on open. For each range `r` this opens the suffixed
    /// keyspaces `data-r{r}` and `raft-r{r}`.
    pub fn open(dir: impl AsRef<Path>, map: &RangeMap) -> Result<Self, kv::KvError> {
        let db = Arc::new(open_database_with_retry(dir.as_ref())?);
        let mut ranges = BTreeMap::new();
        for r in map.range_ids() {
            let data = db
                .keyspace(&format!("data-r{r}"), KeyspaceCreateOptions::default)
                .map_err(|e| kv::KvError::Io(e.to_string()))?;
            let raft = db
                .keyspace(&format!("raft-r{r}"), KeyspaceCreateOptions::default)
                .map_err(|e| kv::KvError::Io(e.to_string()))?;
            ranges.insert(r, RangeKeyspaces { data, raft });
        }
        Ok(Self { db, ranges })
    }

    /// The keyspace pair for `range` (panics if `range` was not in the `RangeMap`
    /// this store was opened with — a construction bug, never user input).
    pub(crate) fn keyspaces(&self, range: RangeId) -> &RangeKeyspaces {
        self.ranges
            .get(&range)
            .unwrap_or_else(|| panic!("range {range} not opened on this NodeStore"))
    }

    /// A `Kv` view over `range`'s `data-r{range}` keyspace for SQL/SM reads.
    pub fn data_kv(&self, range: RangeId) -> Arc<KeyspaceKv> {
        let ks = self.keyspaces(range);
        Arc::new(KeyspaceKv::new(self.db.clone(), ks.data.clone()))
    }
}

/// Open the fjall database at `dir`, retrying briefly on `Error::Locked`.
///
/// Reopening a node directory immediately after dropping the previous instance
/// (a restart) can transiently fail to acquire fjall's exclusive directory lock:
/// openraft's state-machine worker task is NOT joined by `Raft::shutdown()`, so
/// it releases its `Database` handle — and thus the on-disk lock — a moment later,
/// asynchronously. fjall's own open retries the lock only ~300ms; under a heavily
/// loaded runner (e.g. coverage instrumentation on a 2-core CI box) that window
/// can be too short and the reopen fails with `Locked`. Retry for a bounded
/// window so a clean restart is deterministic, while a genuinely stuck lock still
/// fails fast rather than hanging.
fn open_database_with_retry(dir: &Path) -> Result<Database, kv::KvError> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match Database::builder(dir).open() {
            Ok(db) => return Ok(db),
            Err(fjall::Error::Locked) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(kv::KvError::Io(e.to_string())),
        }
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
pub(crate) const SM_APPLIED_KEY: &[u8] = b"sm/last_applied";
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
    /// Open the durable log over `range`'s `raft-r{range}` keyspace, reconstructing
    /// the `last_log_id` / `last_purged` cache from disk (fjall already replayed).
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    pub fn open(store: &NodeStore, range: RangeId) -> Result<Arc<Self>, StorageError<NodeId>> {
        let db = store.db.clone();
        let ks = store.keyspaces(range).raft.clone();
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

/// Map an arbitrary error into a state-machine `StorageError` (write path).
#[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
fn sm_write_err<E: std::error::Error + 'static>(e: &E) -> StorageError<NodeId> {
    StorageError::from(StorageIOError::write_state_machine(e))
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
        // `save_committed` persists the `Option<LogId>` directly, so a stored
        // `None` is `null` on disk. Read it back as `Option<Option<LogId>>` and
        // flatten: an absent key and a stored `null` both yield `None` — symmetric
        // with the write path and with how `open` reloads `sm/last_applied`.
        Ok(read_json::<Option<LogId<NodeId>>>(&self.ks, COMMITTED_KEY)?.flatten())
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

// ---------------------------------------------------------------------------
// Durable state machine
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::io::Cursor;

use kv::{Kv, WriteOp};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, EntryPayload, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StoredMembership,
};
use zerocopy::IntoBytes;
use zerocopy::byteorder::big_endian::U64;

use crate::store::{SnapshotPayload, StateMachineMeta, StoredSnapshot, is_counter_key, u64_be};
use crate::types::WriteBatch;

/// Durable Raft state machine over the `data` keyspace (application KV) and the
/// `raft` keyspace (`sm/last_applied`, `sm/last_membership`) of a shared
/// `Database`. Mirrors the in-memory [`crate::store::StateMachineStore`], but
/// every `apply` commits its data ops **and** the advanced `last_applied` /
/// membership in ONE cross-keyspace fjall batch followed by ONE fsync, so data
/// and metadata can never diverge across a crash. Replay is safe because every
/// op is idempotent: puts/deletes are last-writer-wins and counter keys
/// max-merge.
pub struct DurableStateMachineStore {
    db: Arc<Database>,
    /// The `data` keyspace — application KV content (written via raw batch on
    /// apply, read via `data_kv` for max-merge lookups and snapshots).
    data: fjall::Keyspace,
    /// The `raft` keyspace — holds `SM_APPLIED_KEY` / `SM_MEMBERSHIP_KEY`.
    raft: fjall::Keyspace,
    /// `Kv` view over `data`, shared with the SQL engine for committed reads.
    data_kv: Arc<KeyspaceKv>,
    /// Cached `last_applied` / `last_membership`, the durable truth mirrored.
    meta: RwLock<StateMachineMeta>,
    /// Monotonic snapshot index for unique snapshot ids.
    snapshot_idx: RwLock<u64>,
    /// The last snapshot built or installed (in-memory cache; the authoritative
    /// data lives in the `data` keyspace).
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl std::fmt::Debug for DurableStateMachineStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableStateMachineStore")
            .finish_non_exhaustive()
    }
}

impl DurableStateMachineStore {
    /// Open the durable state machine over `range`'s `data-r{range}` /
    /// `raft-r{range}` keyspaces, reconstructing the `last_applied` /
    /// `last_membership` cache from the `raft` keyspace (fjall already replayed).
    /// An absent `SM_APPLIED_KEY` means a never-applied state machine.
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    pub fn open(store: &NodeStore, range: RangeId) -> Result<Arc<Self>, StorageError<NodeId>> {
        let ks = store.keyspaces(range);
        // `SM_APPLIED_KEY` stores `Option<LogId>`; `read_json::<Option<LogId>>`
        // therefore yields `Option<Option<LogId>>` — an absent key flattens to
        // `None` last_applied.
        let last_applied: Option<LogId<NodeId>> =
            read_json::<Option<LogId<NodeId>>>(&ks.raft, SM_APPLIED_KEY)?.unwrap_or(None);
        let last_membership: StoredMembership<NodeId, BasicNode> =
            read_json(&ks.raft, SM_MEMBERSHIP_KEY)?.unwrap_or_default();
        let meta = StateMachineMeta {
            last_applied,
            last_membership,
        };
        Ok(Arc::new(Self {
            db: store.db.clone(),
            data: ks.data.clone(),
            raft: ks.raft.clone(),
            data_kv: store.data_kv(range),
            meta: RwLock::new(meta),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
        }))
    }

    /// The shared application-data store (the `data` keyspace as a `Kv`).
    /// Cloning the `Arc` lets the SQL engine read committed state directly.
    pub fn sm_kv(&self) -> Arc<dyn Kv> {
        self.data_kv.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<DurableStateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let meta = self.meta.read().await;
            (meta.last_applied, meta.last_membership.clone())
        };

        // Snapshot the full `data` keyspace contents.
        let kv = self
            .data_kv
            .scan_prefix(&[])
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let payload = SnapshotPayload {
            last_applied,
            last_membership: last_membership.clone(),
            kv,
        };
        let data =
            serde_json::to_vec(&payload).map_err(|e| StorageIOError::read_state_machine(&e))?;

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

impl RaftStateMachine<TypeConfig> for Arc<DurableStateMachineStore> {
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
        let mut meta = self.meta.write().await;
        let mut res = Vec::new();
        let mut batch = self.db.batch();
        // Running max for every counter key touched in THIS apply. A bare
        // keyspace `get` would miss a value still pending in `batch`, so the
        // same counter key appearing twice (or once already in the batch) must
        // fold against this map, not just the durable value.
        // On first encounter the durable on-disk value seeds the entry; every later
        // encounter of the same key in this batch folds against the accumulated
        // in-memory max, never against the now-stale disk value.
        let mut counters: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut new_membership = meta.last_membership.clone();
        let mut last_id = meta.last_applied;

        for entry in entries {
            last_id = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(WriteBatch(ref ops)) => {
                    for op in ops {
                        match op {
                            WriteOp::Put { key, value } if is_counter_key(key) => {
                                let incoming = u64_be(value);
                                // Max across this apply() AND the durable value.
                                let cur = match counters.get(key) {
                                    Some(&c) => c,
                                    None => self
                                        .data
                                        .get(key)
                                        .map_err(|e| StorageIOError::write_state_machine(&e))?
                                        .map(|b| u64_be(&b))
                                        .unwrap_or(0),
                                };
                                let merged = cur.max(incoming);
                                counters.insert(key.clone(), merged);
                                batch.insert(&self.data, key, U64::new(merged).as_bytes());
                            }
                            WriteOp::Put { key, value } => batch.insert(&self.data, key, value),
                            WriteOp::Delete { key } => batch.remove(&self.data, key),
                        }
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    new_membership = StoredMembership::new(last_id, mem.clone());
                }
            }
            res.push(());
        }

        batch.insert(
            &self.raft,
            SM_APPLIED_KEY,
            serde_json::to_vec(&last_id).map_err(|e| sm_write_err(&e))?,
        );
        batch.insert(
            &self.raft,
            SM_MEMBERSHIP_KEY,
            serde_json::to_vec(&new_membership).map_err(|e| sm_write_err(&e))?,
        );
        // ONE batch (data ops + counters + last_applied + membership) and ONE
        // fsync: data and metadata advance atomically and durably.
        batch.commit().map_err(|e| sm_write_err(&e))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|e| sm_write_err(&e))?;

        meta.last_applied = last_id;
        meta.last_membership = new_membership;
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

        // A snapshot is authoritative: clear the whole `data` keyspace and
        // overwrite (NOT max-merge), then set the sm/* metadata — all in ONE
        // batch + ONE fsync so the install is atomic and durable.
        let existing = self
            .data_kv
            .scan_prefix(&[])
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut batch = self.db.batch();
        for (k, _) in &existing {
            batch.remove(&self.data, k.as_slice());
        }
        for (k, v) in &payload.kv {
            batch.insert(&self.data, k.as_slice(), v.as_slice());
        }
        batch.insert(
            &self.raft,
            SM_APPLIED_KEY,
            serde_json::to_vec(&meta.last_log_id).map_err(|e| sm_write_err(&e))?,
        );
        batch.insert(
            &self.raft,
            SM_MEMBERSHIP_KEY,
            serde_json::to_vec(&meta.last_membership).map_err(|e| sm_write_err(&e))?,
        );
        batch.commit().map_err(|e| sm_write_err(&e))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|e| sm_write_err(&e))?;

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
            let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
            let mut log = DurableLogStore::open(&store, 0).expect("log open");
            for i in 1..=3 {
                append_blank(&mut log, i).await;
            }
            log.save_vote(&a_vote()).await.expect("save vote");
            // Everything dropped here — must have fsynced before each ack.
        }
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("reopen");
        let mut log = DurableLogStore::open(&store, 0).expect("log reopen");
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
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
        let mut log = DurableLogStore::open(&store, 0).expect("log open");
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
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("reopen");
        let mut log = DurableLogStore::open(&store, 0).expect("log reopen");
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
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
        let mut log = DurableLogStore::open(&store, 0).expect("log open");
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
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("reopen");
        let mut log = DurableLogStore::open(&store, 0).expect("log reopen");
        let state = log.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(2));
        assert_eq!(state.last_log_id.map(|l| l.index), Some(5));
    }

    // -----------------------------------------------------------------------
    // Durable state machine
    // -----------------------------------------------------------------------

    use openraft::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;

    /// Build an `EntryPayload::Normal(WriteBatch(ops))` at `index` and apply it
    /// to `sm`. The SM's `apply` takes `&mut self` on `Arc<…>`, so we clone the
    /// `Arc` and call `apply` through a mutable binding of that clone.
    async fn apply_normal(sm: &Arc<DurableStateMachineStore>, index: u64, ops: Vec<WriteOp>) {
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(WriteBatch(ops)),
        };
        let mut sm = sm.clone();
        sm.apply([entry]).await.expect("apply");
    }

    /// openraft's storage conformance suite over the durable log + state machine.
    /// Each `build()` gets a fresh tempdir; the builder keeps the dirs alive for
    /// the suite's lifetime so the on-disk databases are not reaped mid-test.
    #[derive(Default)]
    struct DurableStoreBuilder {
        tmp: std::sync::Mutex<Vec<tempfile::TempDir>>,
    }

    impl
        openraft::testing::StoreBuilder<
            TypeConfig,
            Arc<DurableLogStore>,
            Arc<DurableStateMachineStore>,
            (),
        > for DurableStoreBuilder
    {
        async fn build(
            &self,
        ) -> Result<((), Arc<DurableLogStore>, Arc<DurableStateMachineStore>), StorageError<NodeId>>
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = NodeStore::open(dir.path(), &RangeMap::single())
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let log = DurableLogStore::open(&store, 0)?;
            let sm = DurableStateMachineStore::open(&store, 0)?;
            self.tmp.lock().expect("tmp").push(dir);
            Ok(((), log, sm))
        }
    }

    /// openraft's own storage conformance suite — the authoritative gate for the
    /// durable log + state machine.
    #[test]
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    fn durable_storage_suite() -> Result<(), StorageError<NodeId>> {
        openraft::testing::Suite::test_all(DurableStoreBuilder::default())
    }

    /// Apply a plain row plus a counter key, drop everything, reopen, and assert
    /// the data AND `last_applied` survived together (atomic apply + fsync).
    #[tokio::test]
    async fn apply_is_atomic_and_survives_reopen() {
        let dir = temp();
        let row = kv::key::row_key(1, 1);
        let xid = kv::key::next_xid_key();
        {
            let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
            let sm = DurableStateMachineStore::open(&store, 0).expect("sm open");
            apply_normal(
                &sm,
                7,
                vec![
                    WriteOp::Put {
                        key: row.clone(),
                        value: b"hello".to_vec(),
                    },
                    WriteOp::Put {
                        key: xid.clone(),
                        value: 42u64.to_be_bytes().to_vec(),
                    },
                ],
            )
            .await;
            // Dropped here — must have fsynced before apply returned.
        }
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("reopen");
        let mut sm = DurableStateMachineStore::open(&store, 0).expect("sm reopen");

        assert_eq!(
            sm.sm_kv().get(&row).expect("get row"),
            Some(b"hello".to_vec()),
            "row data must survive reopen"
        );
        assert_eq!(
            sm.sm_kv().get(&xid).expect("get xid").map(|b| u64_be(&b)),
            Some(42),
            "counter value must survive reopen"
        );
        let (last_applied, _) = sm.applied_state().await.expect("applied_state");
        assert_eq!(
            last_applied.map(|l| l.index),
            Some(7),
            "last_applied must advance with the data it committed"
        );
    }

    /// A counter key appearing twice in one `apply` must max-merge against both
    /// the pending batch value and the durable value (never regress).
    #[tokio::test]
    async fn apply_counter_max_merges_same_key_twice_in_one_batch() {
        let dir = temp();
        let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
        let sm = DurableStateMachineStore::open(&store, 0).expect("sm open");
        let k = kv::key::next_xid_key();
        // Two puts to the same counter key in ONE apply, higher then lower.
        apply_normal(
            &sm,
            1,
            vec![
                WriteOp::Put {
                    key: k.clone(),
                    value: 9u64.to_be_bytes().to_vec(),
                },
                WriteOp::Put {
                    key: k.clone(),
                    value: 4u64.to_be_bytes().to_vec(),
                },
            ],
        )
        .await;
        assert_eq!(
            sm.sm_kv().get(&k).expect("get").map(|b| u64_be(&b)),
            Some(9),
            "same-key-twice must fold to the max, not the last write"
        );
        // A later apply with a still-lower value also must not regress.
        apply_normal(
            &sm,
            2,
            vec![WriteOp::Put {
                key: k.clone(),
                value: 3u64.to_be_bytes().to_vec(),
            }],
        )
        .await;
        assert_eq!(
            sm.sm_kv().get(&k).expect("get").map(|b| u64_be(&b)),
            Some(9),
            "max-merge against the durable value must not regress"
        );
    }

    /// Build a snapshot from one durable SM, install it into a fresh one with
    /// pre-existing junk, and assert the data round-trips exactly (overwrite).
    #[tokio::test]
    async fn snapshot_round_trip_overwrites() {
        let src_dir = temp();
        let src_store = NodeStore::open(src_dir.path(), &RangeMap::single()).expect("src open");
        let mut src = DurableStateMachineStore::open(&src_store, 0).expect("src sm");
        apply_normal(
            &src,
            1,
            vec![
                WriteOp::Put {
                    key: kv::key::row_key(1, 1),
                    value: b"hello".to_vec(),
                },
                WriteOp::Put {
                    key: kv::key::next_xid_key(),
                    value: 42u64.to_be_bytes().to_vec(),
                },
            ],
        )
        .await;
        let expected = src.sm_kv().scan_prefix(&[]).expect("scan");

        let snapshot = src.build_snapshot().await.expect("build snapshot");

        // Fresh store with pre-existing junk that must be overwritten.
        let dst_dir = temp();
        let dst_store = NodeStore::open(dst_dir.path(), &RangeMap::single()).expect("dst open");
        let mut dst = DurableStateMachineStore::open(&dst_store, 0).expect("dst sm");
        // Seed junk directly into the keyspace (bypassing the Raft apply path) to
        // simulate pre-existing state that install_snapshot must authoritatively clear.
        dst.sm_kv()
            .put(kv::key::row_key(9, 9), b"junk".to_vec())
            .expect("put junk");
        dst.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let got = dst.sm_kv().scan_prefix(&[]).expect("scan");
        assert_eq!(got, expected, "snapshot must reproduce KV exactly");
        assert_eq!(
            dst.sm_kv().get(&kv::key::row_key(9, 9)).expect("get"),
            None,
            "install_snapshot must clear prior state"
        );
        // Metadata advanced to the snapshot's last_log_id.
        let (last_applied, _) = dst.applied_state().await.expect("applied_state");
        assert_eq!(last_applied, snapshot.meta.last_log_id);

        // Install survives reopen (data + metadata both durable).
        drop(dst);
        drop(dst_store);
        let dst_store = NodeStore::open(dst_dir.path(), &RangeMap::single()).expect("dst reopen");
        let mut dst = DurableStateMachineStore::open(&dst_store, 0).expect("dst sm reopen");
        assert_eq!(
            dst.sm_kv().scan_prefix(&[]).expect("scan"),
            expected,
            "installed snapshot must survive reopen"
        );
        let (last_applied, _) = dst.applied_state().await.expect("applied_state");
        assert_eq!(last_applied, snapshot.meta.last_log_id);
    }
}
