//! Range 0's Global Transaction Manager: allocates monotonic GLOBAL xids
//! (>= GLOBAL_XID_BASE, disjoint from every range's local xids), tracks the
//! in-flight global set, and builds the global snapshot a cross-range reader
//! resolves Prepared(->G) tuples against. Backed by range 0's store; the counter
//! is max-merged by the state machine exactly like ProcArray's next_xid.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use kv::Kv;
use mvcc::visibility::Snapshot;
use mvcc::xid::GLOBAL_XID_BASE;
use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::ExecError;

struct Inner {
    next_global: u64,
    running: BTreeSet<u64>,
}

pub(crate) struct Gtm {
    inner: Mutex<Inner>,
    kv: Arc<dyn Kv>,
}

impl Gtm {
    pub fn open(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let next = match kv.get(&kv::key::meta_next_global_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?;
                v.get()
            }
            None => GLOBAL_XID_BASE,
        };
        Ok(Self {
            inner: Mutex::new(Inner {
                next_global: next.max(GLOBAL_XID_BASE),
                running: BTreeSet::new(),
            }),
            kv,
        })
    }

    pub fn begin_global(&self) -> u64 {
        let mut g = self.inner.lock().expect("gtm");
        let xid = g.next_global;
        g.next_global = xid + 1;
        g.running.insert(xid);
        xid
    }

    pub fn next_global_xid_op(&self) -> kv::WriteOp {
        let next = self.inner.lock().expect("gtm").next_global;
        kv::WriteOp::Put {
            key: kv::key::meta_next_global_xid_key(),
            value: U64::new(next).as_bytes().to_vec(),
        }
    }

    #[allow(dead_code)] // used on leader transition in Tasks 3/4
    pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
        let durable = match self.kv.get(&kv::key::meta_next_global_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice())
                    .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?;
                v.get()
            }
            None => GLOBAL_XID_BASE,
        };
        let mut g = self.inner.lock().expect("gtm");
        g.next_global = g.next_global.max(durable.max(GLOBAL_XID_BASE));
        Ok(())
    }

    /// Consumed ONLY by `global_status` (never handed to satisfies_mvcc): xip is
    /// BTreeSet-sorted for the resolver's binary_search; xmin is unused.
    pub fn global_snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("gtm");
        let xip: Vec<u64> = g.running.iter().copied().collect();
        let xmax = g.next_global;
        Snapshot {
            xmin: xip.first().copied().unwrap_or(xmax),
            xmax,
            xip,
        }
    }

    pub fn finish_global(&self, g: u64) {
        self.inner.lock().expect("gtm").running.remove(&g);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;

    #[test]
    fn allocates_disjoint_monotonic_global_ids() {
        let gtm = Gtm::open(Arc::new(MemKv::new())).expect("open");
        let (a, b) = (gtm.begin_global(), gtm.begin_global());
        assert!(a >= GLOBAL_XID_BASE && b == a + 1);
        assert_eq!(gtm.global_snapshot().xip, vec![a, b]);
        gtm.finish_global(a);
        assert_eq!(gtm.global_snapshot().xip, vec![b]);
    }

    #[test]
    fn reseed_lifts_counter_and_never_regresses() {
        let kv = Arc::new(MemKv::new());
        let gtm = Gtm::open(kv.clone() as Arc<dyn Kv>).expect("open");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE);
        kv.put(
            kv::key::meta_next_global_xid_key(),
            (GLOBAL_XID_BASE + 50).to_be_bytes().to_vec(),
        )
        .expect("put");
        gtm.reseed_from_applied().expect("reseed");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE + 50);
    }
}
