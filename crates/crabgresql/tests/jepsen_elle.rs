//! Over-the-wire serializability checking (SP11 / D2d): a single-key list-append
//! workload run against the real multi-process cluster, recorded as a
//! linearizability history and checked for strict serializability with stateright.

mod harness;

use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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

// ---------------------------------------------------------------------------
// Task 2 — history recorder, per-key checker, Elle EDN emitter
// ---------------------------------------------------------------------------

/// One recorded linearizability event. `seq` is a global real-time order (an
/// invoke is stamped when the txn BEGINs; a return when it COMMITs), so replaying
/// events in `seq` order reconstructs the real-time interleaving the tester needs.
///
/// `txn` is a unique id per transaction (= the invoke's seq).  An Invoke and its
/// matching Return share the same `txn`.  Using `txn` (rather than `process`) as
/// the linearizability-tester thread id prevents a stranded in-flight invoke (from
/// an indeterminate txn that never committed) from colliding with the same worker's
/// next transaction.
///
/// `Return::process` and `Return::key` are metadata fields retained for debug
/// output; they are not pattern-matched by the checker (which uses `txn` for
/// pairing) but are set by the workload functions so they appear in {:?} traces.
#[allow(dead_code)]
#[derive(Clone, Debug)]
enum Event {
    Invoke {
        process: usize,
        key: i64,
        seq: u64,
        txn: u64,
        op: ListOp,
    },
    Return {
        process: usize,
        key: i64,
        seq: u64,
        txn: u64,
        ret: ListRet,
    },
}

fn ev_seq(e: &Event) -> u64 {
    match e {
        Event::Invoke { seq, .. } | Event::Return { seq, .. } => *seq,
    }
}

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
    seq: Arc<AtomicU64>,
}

impl Recorder {
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    fn push(&self, e: Event) {
        self.events.lock().expect("recorder lock").push(e);
    }

    fn take_sorted(&self) -> Vec<Event> {
        let mut v = self.events.lock().expect("recorder lock").clone();
        v.sort_by_key(ev_seq);
        v
    }
}

/// Strict serializability PER KEY: feed each key's events (in global seq order)
/// into a fresh LinearizabilityTester; require every key consistent.
fn all_keys_consistent(events: &[Event]) -> bool {
    let mut keys: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for e in events {
        match e {
            Event::Invoke { key, .. } | Event::Return { key, .. } => {
                keys.insert(*key);
            }
        }
    }
    keys.into_iter().all(|k| key_consistent(events, k))
}

fn key_consistent(events: &[Event], key: i64) -> bool {
    // Key by `txn` (unique per transaction) rather than `process` (worker id).
    // A stranded Invoke from an indeterminate txn occupies its own unique thread
    // id and can never collide with the same worker's subsequent transaction.
    let mut t: LinearizabilityTester<u64, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    for e in events {
        match e {
            Event::Invoke {
                txn, key: ek, op, ..
            } if *ek == key => {
                t.on_invoke(*txn, op.clone()).expect("on_invoke");
            }
            Event::Return {
                txn, key: ek, ret, ..
            } if *ek == key => {
                t.on_return(*txn, ret.clone()).expect("on_return");
            }
            _ => {}
        }
    }
    t.is_consistent()
}

/// Emit a jepsen/elle `:list-append` history in EDN format.
///
/// For each `Invoke` event, finds the `Return` that shares the same `txn` id.
/// Committed operations get `:type :ok`; invokes with no matching return (i.e.
/// indeterminate/stranded transactions) get `:type :info`.
fn history_to_elle_edn(events: &[Event]) -> String {
    use std::collections::HashMap;

    // Build a lookup: txn id → Return event (there is at most one per txn).
    let returns: HashMap<u64, &Event> = events
        .iter()
        .filter_map(|e| {
            if let Event::Return { txn, .. } = e {
                Some((*txn, e))
            } else {
                None
            }
        })
        .collect();

    let mut out = String::new();

    for e in events {
        let (inv_process, inv_key, inv_txn, inv_op) = match e {
            Event::Invoke {
                process,
                key,
                txn,
                op,
                ..
            } => (*process, *key, *txn, op),
            Event::Return { .. } => continue,
        };

        match returns.get(&inv_txn) {
            Some(ret_ev) => {
                let ret = match ret_ev {
                    Event::Return { ret, .. } => ret,
                    _ => unreachable!(),
                };
                let list_str = ret
                    .0
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                let value = match inv_op {
                    ListOp::AppendRead(v) => {
                        format!("[[:append {inv_key} {v}] [:r {inv_key} [{list_str}]]]")
                    }
                    ListOp::Read => format!("[[:r {inv_key} [{list_str}]]]"),
                };
                writeln!(
                    out,
                    "{{:process {inv_process}, :type :ok, :f :txn, :value {value}}}"
                )
                .expect("write");
            }
            None => {
                // Indeterminate — stranded invoke with no matching return.
                let value = match inv_op {
                    ListOp::AppendRead(v) => format!("[[:append {inv_key} {v}]]"),
                    ListOp::Read => "[]".to_owned(),
                };
                writeln!(
                    out,
                    "{{:process {inv_process}, :type :info, :f :txn, :value {value}}}"
                )
                .expect("write");
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Task 2 tests
// ---------------------------------------------------------------------------

#[test]
fn edn_format_round_trips_a_small_history() {
    let r = Recorder::default();
    let s0 = r.next_seq();
    r.push(Event::Invoke {
        process: 0,
        key: 1,
        seq: s0,
        txn: s0,
        op: ListOp::AppendRead(5),
    });
    let s1 = r.next_seq();
    r.push(Event::Return {
        process: 0,
        key: 1,
        seq: s1,
        txn: s0,
        ret: ListRet(vec![5]),
    });
    let edn = history_to_elle_edn(&r.take_sorted());
    assert!(
        edn.contains("[:append 1 5]"),
        "append clause present: {edn}"
    );
    assert!(edn.contains("[:r 1 [5]]"), "read clause present: {edn}");
    assert!(edn.contains(":type :ok"), "ok type present: {edn}");
}

#[test]
fn all_keys_consistent_accepts_a_valid_event_history() {
    let r = Recorder::default();
    for (p, v, list) in [(0usize, 1i64, vec![1i64]), (1, 2, vec![1, 2])] {
        let inv = r.next_seq();
        r.push(Event::Invoke {
            process: p,
            key: 9,
            seq: inv,
            txn: inv,
            op: ListOp::AppendRead(v),
        });
        let ret = r.next_seq();
        r.push(Event::Return {
            process: p,
            key: 9,
            seq: ret,
            txn: inv,
            ret: ListRet(list),
        });
    }
    assert!(
        all_keys_consistent(&r.take_sorted()),
        "valid per-key history accepted"
    );
}

// ---------------------------------------------------------------------------
// Task 3 — list-append workload + Scenario A (leader-fixed strict-serializable)
// ---------------------------------------------------------------------------

use std::time::Duration;
use tokio_postgres::SimpleQueryMessage;

/// Read the `val` column of a `simple_query` SELECT result as an ordered Vec.
fn list_from(msgs: &[SimpleQueryMessage]) -> Vec<i64> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some(r.get(0).expect("val col").parse::<i64>().expect("i64"))
            }
            _ => None,
        })
        .collect()
}

/// Run ONE list-append transaction. Records an Invoke at BEGIN and (on clean
/// COMMIT) a Return with the observed list; an indeterminate COMMIT leaves the
/// Invoke in-flight. Returns whether it committed.
async fn append_txn(
    client: &tokio_postgres::Client,
    rec: &Recorder,
    process: usize,
    key: i64,
    val: i64,
) -> bool {
    let inv = rec.next_seq();
    rec.push(Event::Invoke {
        process,
        key,
        seq: inv,
        txn: inv,
        op: ListOp::AppendRead(val),
    });
    async fn stmt(client: &tokio_postgres::Client, sql: &str) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(10), client.simple_query(sql)).await,
            Ok(Ok(_))
        )
    }
    if !stmt(client, "BEGIN").await {
        let _ = client.simple_query("ROLLBACK").await;
        return false;
    }
    // Serialize concurrent same-key appends so the workload is strict-serializable
    // under snapshot isolation (without this, two concurrent appends can each
    // snapshot-miss the other — a real SI anomaly the checker correctly flags).
    if !stmt(
        client,
        &format!("SELECT key FROM anchor WHERE key = {key} FOR UPDATE"),
    )
    .await
    {
        let _ = client.simple_query("ROLLBACK").await;
        return false;
    }
    if !stmt(
        client,
        &format!("INSERT INTO appends(key, val) VALUES ({key}, {val})"),
    )
    .await
    {
        let _ = client.simple_query("ROLLBACK").await;
        return false;
    }
    let list = match tokio::time::timeout(
        Duration::from_secs(10),
        client.simple_query(&format!("SELECT val FROM appends WHERE key = {key}")),
    )
    .await
    {
        Ok(Ok(msgs)) => list_from(&msgs),
        _ => {
            let _ = client.simple_query("ROLLBACK").await;
            return false;
        }
    };
    if stmt(client, "COMMIT").await {
        let ret = rec.next_seq();
        rec.push(Event::Return {
            process,
            key,
            seq: ret,
            txn: inv,
            ret: ListRet(list),
        });
        true
    } else {
        let _ = client.simple_query("ROLLBACK").await;
        false
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_append_is_strict_serializable_under_follower_faults() {
    use cluster::transport::protocol::ControlRequest;
    let mut c = harness::Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    const KEYS: i64 = 2;
    const PROCS: usize = 2;
    const OPS: usize = 6;
    {
        let mut idx = 0;
        let setup = loop {
            if let Some(cl) = c.pg_try(idx).await {
                break cl;
            }
            idx += 1;
            assert!(idx < 30, "no node accepted the setup connection");
        };
        setup
            .simple_query("CREATE TABLE appends (key int8, val int8)")
            .await
            .expect("create appends");
        setup
            .simple_query("CREATE TABLE anchor (key int8)")
            .await
            .expect("create anchor");
        for k in 0..KEYS {
            setup
                .simple_query(&format!("INSERT INTO anchor VALUES ({k})"))
                .await
                .expect("seed anchor");
        }
    }
    let rec = Recorder::default();
    let n_nodes = c.len();
    let mut workers = Vec::new();
    for p in 0..PROCS {
        let rec = rec.clone();
        let addrs: Vec<String> = (0..n_nodes)
            .map(|i| c.sql_addr(i as u64).to_string())
            .collect();
        workers.push(tokio::spawn(async move {
            for i in 0..OPS {
                let key = ((p + i) as i64) % KEYS;
                let val = (p as i64) * 1000 + i as i64 + 1;
                let mut connected = None;
                for a in 0..addrs.len() {
                    let node = (p + i + a) % addrs.len();
                    let port = addrs[node].rsplit(':').next().expect("port");
                    let cs = format!("host=127.0.0.1 port={port} user=postgres");
                    if let Ok(Ok((cl, conn))) = tokio::time::timeout(
                        Duration::from_secs(8),
                        tokio_postgres::connect(&cs, tokio_postgres::NoTls),
                    )
                    .await
                    {
                        tokio::spawn(conn);
                        connected = Some(cl);
                        break;
                    }
                }
                if let Some(cl) = connected {
                    let _ = append_txn(&cl, &rec, p, key, val).await;
                }
            }
        }));
    }
    let followers: Vec<u64> = (0..3u64).filter(|&i| i != leader).collect();
    let mut round = 0usize;
    const MIN_ROUNDS: usize = 3;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let victim = followers[round % followers.len()];
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..3u64).filter(|&i| i != victim).collect();
            c.control(victim, ControlRequest::SetPartition(others))
                .await;
            for o in (0..3u64).filter(|&i| i != victim) {
                c.control(o, ControlRequest::SetPartition(vec![victim]))
                    .await;
            }
            for id in 0..3u64 {
                c.control(id, ControlRequest::Heal).await;
            }
        }
        round += 1;
    }
    for w in workers {
        let _ = w.await;
    }
    for id in 0..3u64 {
        c.control(id, ControlRequest::Heal).await;
    }
    let events = rec.take_sorted();
    let returns = events
        .iter()
        .filter(|e| matches!(e, Event::Return { .. }))
        .count();
    assert!(
        returns >= 3,
        "workload must commit several transactions (got {returns})"
    );
    assert!(
        all_keys_consistent(&events),
        "list-append history must be strict-serializable"
    );
}

// ---------------------------------------------------------------------------
// Task 4 — Scenario B (leader-failover gap-finder) + EDN artifact
// ---------------------------------------------------------------------------

/// A single-key read transaction (BEGIN; SELECT; COMMIT). Records Invoke(Read) +
/// Return(list). Used for the stale read in the gap-finder.
async fn read_txn(
    client: &tokio_postgres::Client,
    rec: &Recorder,
    process: usize,
    key: i64,
) -> Vec<i64> {
    let inv = rec.next_seq();
    rec.push(Event::Invoke {
        process,
        key,
        seq: inv,
        txn: inv,
        op: ListOp::Read,
    });
    let msgs = tokio::time::timeout(
        Duration::from_secs(10),
        client.simple_query(&format!("SELECT val FROM appends WHERE key = {key}")),
    )
    .await
    .expect("read not timed out")
    .expect("read ok");
    let list = list_from(&msgs);
    let ret = rec.next_seq();
    rec.push(Event::Return {
        process,
        key,
        seq: ret,
        txn: inv,
        ret: ListRet(list.clone()),
    });
    list
}

/// A read that tolerates a not-leader / connection error (returns `None`) instead
/// of panicking. Post-D5 the gate makes a read on a deposed leader either error
/// (`None`) or proxy to the fresh leader — never the stale value — so this is how
/// Scenario B asserts "no stale read".
async fn try_read(client: &tokio_postgres::Client, key: i64) -> Option<Vec<i64>> {
    match tokio::time::timeout(
        Duration::from_secs(10),
        client.simple_query(&format!("SELECT val FROM appends WHERE key = {key}")),
    )
    .await
    {
        Ok(Ok(msgs)) => Some(list_from(&msgs)),
        _ => None, // timeout, NotLeader (40001), or dropped connection
    }
}

/// Scenario B (post-D5) — across a leader failover, reads are LINEARIZABLE. The
/// deposed-but-not-yet-stepped-down leader L no longer serves a stale local read:
/// the ReadIndex gate (`RaftLinearizer::ensure_readable` → `Raft::ensure_linearizable`)
/// can't confirm L's leadership against the isolated majority, so L rejects the
/// read (retryable 40001) — or, if L has stepped down, proxies to L' — instead of
/// returning the stale `[1]`. A routed read reaches the new leader L' and returns
/// the fresh `[1, 2]`. The recorded history is strict-serializable.
///
/// This is the TDD flip of SP11's gap-finder: the same orchestration that used to
/// surface the stale read now proves it can't happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_read_is_linearizable() {
    use cluster::transport::protocol::ControlRequest;
    const KEY: i64 = 7;
    // Establishing a failover (electing L', committing v2) is timing-bound; retry
    // a bounded number of times. The D5 assertions below are deterministic once
    // the failover is established.
    const ATTEMPTS: usize = 15;
    for attempt in 0..ATTEMPTS {
        let c = harness::Cluster::spawn(3).await;
        let l = c.wait_for_leader().await;
        let rec = Recorder::default();
        // Seed: table + anchor, append 1 to KEY via the leader (process 0).
        {
            let setup = c.pg(l).await;
            setup
                .simple_query("CREATE TABLE appends (key int8, val int8)")
                .await
                .expect("create");
            setup
                .simple_query("CREATE TABLE anchor (key int8)")
                .await
                .expect("create anchor");
            setup
                .simple_query(&format!("INSERT INTO anchor VALUES ({KEY})"))
                .await
                .expect("seed anchor");
        }
        let ok1 = append_txn(&c.pg(l).await, &rec, 0, KEY, 1).await;
        assert!(ok1, "seed append must commit");
        // Isolate L; the majority elects L'.
        let others: Vec<u64> = (0..3u64).filter(|&i| i != l).collect();
        c.control(l, ControlRequest::SetPartition(others.clone())).await;
        for &o in &others {
            c.control(o, ControlRequest::SetPartition(vec![l])).await;
        }
        // Bounded wait for a NEW leader among the survivors.
        let neu = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let mut found = None;
                for &o in &others {
                    if let Some(st) = c.status(o).await
                        && st.current_leader.is_some_and(|x| x != l)
                    {
                        found = st.current_leader;
                    }
                }
                if let Some(x) = found {
                    break x;
                }
                if tokio::time::Instant::now() >= deadline {
                    break l; // no failover; retry attempt
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        if neu == l {
            eprintln!("attempt {attempt}: no new leader elected within the window; retrying");
            for id in 0..3u64 {
                c.control(id, ControlRequest::Heal).await;
            }
            continue;
        }
        // Commit append 2 via the new leader L' (process 1).
        let ok2 = append_txn(&c.pg(neu).await, &rec, 1, KEY, 2).await;
        if !ok2 {
            eprintln!("attempt {attempt}: append 2 did not commit on L'={neu}; retrying");
            for id in 0..3u64 {
                c.control(id, ControlRequest::Heal).await;
            }
            continue;
        }
        // (1) The deposed leader must NOT serve a stale read. Read DIRECTLY from L
        // (the harness→L SQL connection isn't subject to the Raft partition): the
        // gate either rejects the read (None) or proxies it to L' (fresh [1,2]) —
        // it must never serve a stale or partial value from L's local state.
        let direct = try_read(&c.pg(l).await, KEY).await;
        match &direct {
            None => {} // gate rejected the read — no stale data served
            Some(v) => assert_eq!(
                *v,
                vec![1, 2],
                "deposed-leader read must be rejected or fresh [1,2], never stale/partial: {direct:?}"
            ),
        }
        // (2) A routed read observes the fresh, linearizable value (recorded).
        let fresh = read_txn(&c.pg(neu).await, &rec, 2, KEY).await;
        assert_eq!(fresh, vec![1, 2], "routed read after failover must be fresh");
        // Heal.
        for id in 0..3u64 {
            c.control(id, ControlRequest::Heal).await;
        }
        // History {append 1→[1]; append 2→[1,2]; read→[1,2]} is strict-serializable.
        let events = rec.take_sorted();
        assert!(
            all_keys_consistent(&events),
            "post-D5 failover read history must be strict-serializable"
        );
        let edn = history_to_elle_edn(&events);
        let path = std::env::temp_dir().join("crabgresql-sp12-linearizable-read.edn");
        std::fs::write(&path, edn).expect("write edn");
        eprintln!("wrote linearizable Elle EDN history to {}", path.display());
        return; // success: failover read proven linearizable
    }
    panic!("could not establish a leader failover within the {ATTEMPTS}-attempt budget");
}
