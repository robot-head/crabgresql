//! The running-transaction registry (PostgreSQL's ProcArray). Shared across all
//! connections behind an `Arc`. Owns the next-xid counter (seeded from the
//! durable `/0/meta/next_xid` at open) and the set of currently-running xids,
//! and builds `mvcc::visibility::Snapshot`s. After a restart it starts empty, so
//! any clog `in-progress` xid is in no snapshot and resolves as aborted.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use kv::Kv;
use mvcc::visibility::Snapshot;
use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

use crate::PersistMode;
use crate::error::ExecError;

struct Inner {
    next_xid: u64,
    running: BTreeSet<u64>,
}

/// The running-transaction registry.
pub(crate) struct ProcArray {
    inner: Mutex<Inner>,
    kv: Arc<dyn Kv>,
    mode: PersistMode,
}

impl ProcArray {
    /// Seed the next-xid counter from the durable key (default 1 — real xids
    /// start at 1; 0 is the invalid sentinel).
    pub fn open(kv: Arc<dyn Kv>, mode: PersistMode) -> Result<Self, ExecError> {
        let next_xid = match kv.get(&kv::key::next_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| kv::KvError::CorruptRow("next_xid is not u64".into()))?;
                v.get()
            }
            None => 1,
        };
        Ok(Self {
            inner: Mutex::new(Inner {
                next_xid: next_xid.max(1),
                running: BTreeSet::new(),
            }),
            kv,
            mode,
        })
    }

    /// Allocate the next xid and register it as running. In `Durable` mode,
    /// persists the bumped counter durably under the lock so it advances
    /// monotonically — a restart never reuses an xid even when concurrent commit
    /// batches land out of order. In `Replicated` mode, the counter is NOT
    /// persisted here: the session folds `next_xid_op()` into the same commit
    /// batch as the write that triggered it (max-merged by the state machine),
    /// and `reseed_from_applied` lifts the counter on leadership change.
    pub fn begin_write(&self) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("procarray");
        let xid = g.next_xid;
        let new_next = xid + 1;
        if self.mode == PersistMode::Durable {
            self.kv.write_batch(&[kv::WriteOp::Put {
                key: kv::key::next_xid_key(),
                value: U64::new(new_next).as_bytes().to_vec(),
            }])?;
        }
        g.next_xid = new_next;
        g.running.insert(xid);
        Ok(xid)
    }

    /// Reseed the in-memory counter from the applied store (called when this node
    /// becomes leader, so it never hands out an xid the old leader already used).
    pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
        let durable = match self.kv.get(&kv::key::next_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| kv::KvError::CorruptRow("next_xid not u64".into()))?;
                v.get()
            }
            None => 1,
        };
        let mut g = self.inner.lock().expect("procarray");
        g.next_xid = g.next_xid.max(durable.max(1));
        Ok(())
    }

    /// The WriteOp recording the current next_xid (folded into the commit batch in
    /// Replicated mode).
    pub fn next_xid_op(&self) -> kv::WriteOp {
        let next = self.inner.lock().expect("procarray").next_xid;
        kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: U64::new(next).as_bytes().to_vec(),
        }
    }

    /// The durable next-xid value (one past the highest allocated). `begin_write`
    /// persists this eagerly, so callers no longer batch it with their writes;
    /// retained as a test accessor that proves the counter advanced.
    #[cfg(test)]
    pub(crate) fn next_xid(&self) -> u64 {
        self.inner.lock().expect("procarray").next_xid
    }

    /// A snapshot of the currently-running transactions.
    pub fn snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("procarray");
        let xip: Vec<u64> = g.running.iter().copied().collect(); // BTreeSet => sorted ascending
        let xmax = g.next_xid;
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    /// Deregister a finished (committed or aborted) transaction. Call only after
    /// its clog entry is durable.
    pub fn finish(&self, xid: u64) {
        self.inner.lock().expect("procarray").running.remove(&xid);
    }

    /// Number of currently-registered running transactions (test helper).
    #[cfg(test)]
    pub(crate) fn running_len(&self) -> usize {
        self.inner.lock().expect("procarray").running.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use kv::MemKv;

    #[test]
    fn fresh_store_starts_at_xid_one() {
        let pa = ProcArray::open(Arc::new(MemKv::new()), PersistMode::Durable).expect("open");
        let s = pa.snapshot();
        assert_eq!(s.xmax, 1);
        assert!(s.xip.is_empty());
    }

    #[test]
    fn allocate_registers_running_and_snapshot_excludes_committed() {
        let pa = ProcArray::open(Arc::new(MemKv::new()), PersistMode::Durable).expect("open");
        let x1 = pa.begin_write().expect("begin_write");
        let x2 = pa.begin_write().expect("begin_write");
        assert_eq!((x1, x2), (1, 2));
        let s = pa.snapshot();
        assert_eq!(s.xmax, 3);
        assert_eq!(s.xip, vec![1, 2]);
        pa.finish(x1);
        let s2 = pa.snapshot();
        assert_eq!(s2.xip, vec![2]);
        assert_eq!(s2.xmax, 3);
    }

    #[test]
    fn open_seeds_next_xid_from_durable_counter() {
        let kv = Arc::new(MemKv::new());
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open");
        assert_eq!(pa.begin_write().expect("begin_write"), 42);
        assert_eq!(pa.next_xid(), 43);

        // Prove monotonic persist: a fresh ProcArray on the same kv should pick
        // up the durable counter (43) and return 43 as its first xid.
        let pa2 =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Durable).expect("open2");
        assert_eq!(
            pa2.begin_write().expect("begin_write2"),
            43,
            "durable counter must have advanced to 43"
        );
    }

    #[test]
    fn replicated_begin_write_does_not_persist_but_reseed_lifts_counter() {
        let kv = Arc::new(MemKv::new());
        let pa =
            ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Replicated).expect("open");
        assert_eq!(pa.begin_write().expect("bw"), 1);
        // Nothing persisted (replicated mode folds via the batch, not here).
        assert!(kv.get(&kv::key::next_xid_key()).expect("get").is_none());
        // Simulate the applied store advancing to 50 (via Raft), then becoming leader.
        kv.put(kv::key::next_xid_key(), 50u64.to_be_bytes().to_vec())
            .expect("put");
        pa.reseed_from_applied().expect("reseed");
        assert_eq!(
            pa.begin_write().expect("bw"),
            50,
            "reseed lifts the counter above applied"
        );
    }
}
