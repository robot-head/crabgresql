//! Over-the-wire serializability checking (SP11 / D2d): a single-key list-append
//! workload run against the real multi-process cluster, recorded as a
//! linearizability history and checked for strict serializability with stateright.
// NOTE: `mod harness;` is deferred to Task 3 (when harness is first used).
// Adding it here (unused) triggers dead-code errors under `-D warnings`.

use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};

// Reference object: a per-key append-only list. Each transaction is ONE atomic
// op — `AppendRead(v)` (append v, return the new list) for a writing txn, or
// `Read` (return the list) for a read-only txn. Linearizability of these atomic
// ops over one key == strict serializability of that key.

#[derive(Clone, Default)]
struct AppendList(Vec<i64>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ListOp {
    AppendRead(i64),
    Read,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ListRet(Vec<i64>);

impl SequentialSpec for AppendList {
    type Op = ListOp;
    type Ret = ListRet;
    fn invoke(&mut self, op: &ListOp) -> ListRet {
        if let ListOp::AppendRead(v) = op {
            self.0.push(*v);
        }
        ListRet(self.0.clone())
    }
}

#[test]
fn checker_accepts_a_serial_list_history() {
    let mut t: LinearizabilityTester<usize, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    t.on_invoke(0, ListOp::AppendRead(1))
        .expect("inv")
        .on_return(0, ListRet(vec![1]))
        .expect("ret");
    t.on_invoke(1, ListOp::AppendRead(2))
        .expect("inv")
        .on_return(1, ListRet(vec![1, 2]))
        .expect("ret");
    t.on_invoke(2, ListOp::Read)
        .expect("inv")
        .on_return(2, ListRet(vec![1, 2]))
        .expect("ret");
    assert!(
        t.is_consistent(),
        "a serial append/read history must be accepted"
    );
}

#[test]
fn checker_rejects_a_stale_read_history() {
    let mut t: LinearizabilityTester<usize, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    t.on_invoke(0, ListOp::AppendRead(1))
        .expect("inv")
        .on_return(0, ListRet(vec![1]))
        .expect("ret");
    t.on_invoke(1, ListOp::AppendRead(2))
        .expect("inv")
        .on_return(1, ListRet(vec![1, 2]))
        .expect("ret");
    t.on_invoke(2, ListOp::Read)
        .expect("inv")
        .on_return(2, ListRet(vec![1]))
        .expect("ret");
    assert!(
        !t.is_consistent(),
        "a read missing an already-acked append must be rejected"
    );
}
