//! The meta-range descriptor store: the replicated `RangeMap` lives under
//! `/0/meta/range_map` in range 0, committed through range 0's Raft. Reads are
//! served from a local applied store (correct because the blob is immutable this
//! slice — SP15 relocate-only); a future mutable-descriptor slice (D4) must
//! upgrade reads to leader-confirmed.

use std::time::Duration;

use crate::range::map::RangeMap;
use crate::types::{TypeConfig, WriteBatch};

/// Read the committed range-descriptor blob from a range-0 applied store.
/// `Ok(None)` if no blob has been committed yet.
pub fn read_range_map(store: &dyn kv::Kv) -> Result<Option<RangeMap>, kv::KvError> {
    match store.get(&kv::key::meta_range_map_key())? {
        Some(bytes) => Ok(Some(RangeMap::from_descriptor_bytes(&bytes)?)),
        None => Ok(None),
    }
}

/// Commit the descriptor blob to range 0 through its Raft `client_write`. The
/// caller writes only when the blob is absent, so this is a one-time seed.
pub async fn write_range_map(
    raft: &openraft::Raft<TypeConfig>,
    map: &RangeMap,
) -> std::io::Result<()> {
    raft.client_write(WriteBatch(vec![kv::WriteOp::Put {
        key: kv::key::meta_range_map_key(),
        value: map.to_descriptor_bytes(),
    }]))
    .await
    .map(|_| ())
    .map_err(|e| std::io::Error::other(format!("seed range map: {e}")))
}

/// Seed the descriptor blob only if absent (create-if-absent). The relocate-only
/// write-once invariant: the blob is written exactly once, at cluster create — a
/// second bring-up finds it present and does NOT rewrite it, so every node's local
/// immutable read stays correct. Returns `Ok(())` whether or not it wrote.
pub async fn seed_if_absent(
    raft: &openraft::Raft<TypeConfig>,
    store: &dyn kv::Kv,
    map: &RangeMap,
) -> std::io::Result<()> {
    if read_range_map(store)
        .map_err(|e| std::io::Error::other(format!("read seed: {e:?}")))?
        .is_none()
    {
        write_range_map(raft, map).await?;
    }
    Ok(())
}

/// Wait (bounded, event-driven on range 0's metrics) until the committed
/// descriptor blob is present in `store`, then decode + return it. Wakes on each
/// openraft metrics change (≈ each apply) rather than sleeping a fixed interval;
/// fails with `TimedOut` if the blob never arrives within `timeout`.
pub async fn wait_for_range_map(
    raft: &openraft::Raft<TypeConfig>,
    store: &dyn kv::Kv,
    timeout: Duration,
) -> std::io::Result<RangeMap> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(map) = read_range_map(store)
            .map_err(|e| std::io::Error::other(format!("decode range map: {e:?}")))?
        {
            return Ok(map);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "range map not committed within bound",
            ));
        }
        // Wake on the next metrics change (a new apply may be the blob), bounded
        // by the deadline. The loop re-checks the store on both wake and timeout.
        let mut rx = raft.metrics();
        let _ = tokio::time::timeout(remaining, rx.changed()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::{Kv, MemKv};

    #[test]
    fn read_range_map_absent_then_present() {
        let store = MemKv::new();
        assert_eq!(read_range_map(&store).expect("read"), None);

        let map = RangeMap::with_boundaries(vec![2]);
        store
            .put(kv::key::meta_range_map_key(), map.to_descriptor_bytes())
            .expect("put");
        assert_eq!(
            read_range_map(&store).expect("read"),
            Some(map),
            "a committed blob reads back as the same RangeMap"
        );
    }

    #[test]
    fn read_range_map_rejects_corrupt_blob() {
        let store = MemKv::new();
        store
            .put(kv::key::meta_range_map_key(), vec![99, 0, 0, 0, 1])
            .expect("put");
        assert!(read_range_map(&store).is_err());
    }
}
