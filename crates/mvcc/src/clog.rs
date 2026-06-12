//! Commit-status log — PostgreSQL's `pg_xact`. Maps each transaction id to its
//! final outcome; the authority on whether a writer committed. An ABSENT entry
//! means the xid recorded no outcome: it is in-progress while the transaction
//! runs, and aborted-equivalent after a crash (it is then in no live snapshot).

use kv::{Kv, KvError, WriteOp};

/// A transaction's recorded outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus {
    InProgress,
    Committed,
    Aborted,
}

const S_IN_PROGRESS: u8 = 0;
const S_COMMITTED: u8 = 1;
const S_ABORTED: u8 = 2;

/// Read an xid's status. An absent entry is treated as `InProgress`
/// (aborted-equivalent once the xid is in no live snapshot — see recovery).
pub fn get(kv: &dyn Kv, xid: u64) -> Result<XidStatus, KvError> {
    match kv.get(&kv::key::clog_key(xid))? {
        None => Ok(XidStatus::InProgress),
        Some(b) => match b.first() {
            Some(&S_COMMITTED) => Ok(XidStatus::Committed),
            Some(&S_ABORTED) => Ok(XidStatus::Aborted),
            Some(&S_IN_PROGRESS) => Ok(XidStatus::InProgress),
            _ => Err(KvError::CorruptRow("bad clog status byte".into())),
        },
    }
}

/// A write-batch op recording an xid's final status.
pub fn put_op(xid: u64, status: XidStatus) -> WriteOp {
    let byte = match status {
        XidStatus::InProgress => S_IN_PROGRESS,
        XidStatus::Committed => S_COMMITTED,
        XidStatus::Aborted => S_ABORTED,
    };
    WriteOp::Put {
        key: kv::key::clog_key(xid),
        value: vec![byte],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;

    #[test]
    fn absent_entry_is_in_progress() {
        let kv = MemKv::new();
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::InProgress);
    }

    #[test]
    fn committed_and_aborted_roundtrip() {
        let kv = MemKv::new();
        kv.write_batch(&[put_op(7, XidStatus::Committed)])
            .expect("put");
        kv.write_batch(&[put_op(8, XidStatus::Aborted)])
            .expect("put");
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::Committed);
        assert_eq!(get(&kv, 8).expect("get"), XidStatus::Aborted);
    }

    #[test]
    fn corrupt_status_byte_errors() {
        let kv = MemKv::new();
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::clog_key(9),
            value: vec![99],
        }])
        .expect("put");
        assert!(get(&kv, 9).is_err());
    }
}
