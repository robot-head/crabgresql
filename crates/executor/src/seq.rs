//! Atomic per-table rowid allocation for concurrent INSERTs. An in-memory
//! counter per table, seeded once from the durable `/0/seq/<table>` key, bumped
//! under a mutex, with the new value persisted durably *under the mutex* before
//! the rowid is returned — so the durable counter is monotonic and a restart
//! never reuses a rowid (a crash only leaks a gap, like a PostgreSQL sequence).

use std::collections::HashMap;
use std::sync::Mutex;

use kv::Kv;

use crate::error::ExecError;

pub(crate) struct SequenceManager {
    inner: Mutex<HashMap<catalog::TableId, u64>>,
}

impl SequenceManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Reserve `count` consecutive rowids for `table` and return the first.
    /// Persists the new next-rowid durably before returning so it cannot regress.
    pub fn alloc(
        &self,
        kv: &dyn Kv,
        table: catalog::TableId,
        count: u64,
    ) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("seqmgr");
        let next = match g.get(&table) {
            Some(&n) => n,
            None => crate::exec::read_seq_kv(kv, table)?, // seed once from disk
        };
        let new_next = next + count;
        // Persist BEFORE releasing the lock and BEFORE handing out the rowid, so
        // the durable counter is monotonic even under concurrent allocators.
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::seq_key(table),
            value: new_next.to_be_bytes().to_vec(),
        }])?;
        g.insert(table, new_next);
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;
    use std::sync::Arc;

    #[test]
    fn allocates_distinct_increasing_rowids() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new();
        assert_eq!(seq.alloc(&*kv, 7, 3).expect("alloc"), 1); // rows 1,2,3
        assert_eq!(seq.alloc(&*kv, 7, 2).expect("alloc"), 4); // rows 4,5
        assert_eq!(seq.alloc(&*kv, 8, 1).expect("alloc"), 1); // a different table is independent
    }

    #[test]
    fn durable_seq_is_monotonic_and_seeds_a_fresh_manager() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new();
        seq.alloc(&*kv, 7, 5).expect("alloc"); // consumes 1..=5, persists next=6
        let seq2 = SequenceManager::new(); // simulate restart
        assert_eq!(
            seq2.alloc(&*kv, 7, 1).expect("alloc"),
            6,
            "must not reuse 1..=5"
        );
    }

    #[test]
    fn seeds_from_existing_durable_seq_key() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::seq_key(7),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let seq = SequenceManager::new();
        assert_eq!(seq.alloc(&*kv, 7, 1).expect("alloc"), 42);
    }
}
