//! The running-transaction registry (PostgreSQL's ProcArray). Shared across all
//! connections behind an `Arc`. Owns the next-xid counter (seeded from the
//! durable `/0/meta/next_xid` at open) and the set of currently-running xids,
//! and builds `mvcc::visibility::Snapshot`s. After a restart it starts empty, so
//! any clog `in-progress` xid is in no snapshot and resolves as aborted.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use kv::Kv;
use mvcc::visibility::Snapshot;

use crate::error::ExecError;

struct Inner {
    next_xid: u64,
    running: BTreeSet<u64>,
}

/// The running-transaction registry.
pub(crate) struct ProcArray {
    inner: Mutex<Inner>,
    kv: Arc<dyn Kv>,
}

impl ProcArray {
    /// Seed the next-xid counter from the durable key (default 1 — real xids
    /// start at 1; 0 is the invalid sentinel).
    pub fn open(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let next_xid = match kv.get(&kv::key::next_xid_key())? {
            Some(b) => {
                let a: [u8; 8] = b
                    .as_slice()
                    .try_into()
                    .map_err(|_| kv::KvError::CorruptRow("next_xid is not u64".into()))?;
                u64::from_be_bytes(a)
            }
            None => 1,
        };
        Ok(Self {
            inner: Mutex::new(Inner {
                next_xid: next_xid.max(1),
                running: BTreeSet::new(),
            }),
            kv,
        })
    }

    /// Allocate the next xid and register it as running. Persists the bumped
    /// counter durably under the lock so it advances monotonically — a restart
    /// never reuses an xid even when concurrent commit batches land out of order.
    pub fn begin_write(&self) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("procarray");
        let xid = g.next_xid;
        let new_next = xid + 1;
        // Persist the bumped counter durably under the lock so it advances
        // monotonically — a restart never reuses an xid even when concurrent
        // commit batches land out of order.
        self.kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: new_next.to_be_bytes().to_vec(),
        }])?;
        g.next_xid = new_next;
        g.running.insert(xid);
        Ok(xid)
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
        let pa = ProcArray::open(Arc::new(MemKv::new())).expect("open");
        let s = pa.snapshot();
        assert_eq!(s.xmax, 1);
        assert!(s.xip.is_empty());
    }

    #[test]
    fn allocate_registers_running_and_snapshot_excludes_committed() {
        let pa = ProcArray::open(Arc::new(MemKv::new())).expect("open");
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
        let pa = ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>).expect("open");
        assert_eq!(pa.begin_write().expect("begin_write"), 42);
        assert_eq!(pa.next_xid(), 43);

        // Prove monotonic persist: a fresh ProcArray on the same kv should pick
        // up the durable counter (43) and return 43 as its first xid.
        let pa2 = ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>).expect("open2");
        assert_eq!(
            pa2.begin_write().expect("begin_write2"),
            43,
            "durable counter must have advanced to 43"
        );
    }
}
