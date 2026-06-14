//! In-process N-node cluster for tests: build, initialize, find the leader,
//! and inject faults via the Switchboard.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use openraft::{BasicNode, ServerState, SnapshotPolicy};

use crate::network::Switchboard;
use crate::node::Node;
use crate::types::{NodeId, WriteBatch};

/// An in-process cluster of [`Node`]s wired together by one [`Switchboard`].
/// Owns the nodes; tests drive the group and inject faults through it.
pub struct Cluster {
    /// The replicas, indexed by id (`nodes[id]`).
    pub nodes: Vec<Node>,
    /// The shared transport / fault registry.
    pub sb: Switchboard,
}

impl Cluster {
    /// Build `n` nodes and initialize a single voting group `{0..n}`.
    pub async fn new(n: u64) -> Self {
        Self::build(n, Node::default_config()).await
    }

    /// Like [`Cluster::new`], but every node runs an aggressive snapshot policy:
    /// snapshot after one log since the last, keep zero in-snapshot logs, and
    /// purge one entry at a time. A follower paused past a snapshot boundary then
    /// finds its needed log entries purged, so the leader must repair it with an
    /// installed snapshot rather than log replay.
    pub async fn new_with_snapshotting(n: u64) -> Self {
        let config = openraft::Config {
            // Snapshot almost every entry...
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1),
            // ...and purge logs already captured by a snapshot immediately, one
            // at a time, so a far-behind follower's entries are truly gone.
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Node::default_config()
        };
        Self::build(n, config).await
    }

    /// Like [`Cluster::new`], but with long election timers so a CPU-starved run
    /// (e.g. coverage instrumentation on a small CI runner) cannot miss heartbeats
    /// and trigger a spurious election that moves the leader. Leader-stability-
    /// sensitive tests (the register linearizability check, whose fixed-leader
    /// premise a leader change would void) use this instead of [`Cluster::new`].
    pub async fn new_stable_leader(n: u64) -> Self {
        let config = openraft::Config {
            heartbeat_interval: 300,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        Self::build(n, config).await
    }

    /// Build `n` nodes with the given Raft `config` and initialize the group.
    async fn build(n: u64, config: openraft::Config) -> Self {
        let sb = Switchboard::new();
        let mut nodes = Vec::new();
        for id in 0..n {
            nodes.push(Node::start_with_config(0, id, sb.clone(), config.clone()).await);
        }
        let members: BTreeMap<NodeId, BasicNode> =
            (0..n).map(|id| (id, BasicNode::default())).collect();
        nodes[0].raft.initialize(members).await.expect("initialize");
        Self { nodes, sb }
    }

    /// Build `n` durable nodes under `base_dir/node-<id>` and initialize the group.
    pub async fn durable(n: u64, base_dir: &Path) -> Self {
        let sb = Switchboard::new();
        let mut nodes = Vec::new();
        for id in 0..n {
            let dir = base_dir.join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("mkdir node");
            nodes.push(Node::start_durable(0, id, sb.clone(), dir, Node::default_config()).await);
        }
        let members: BTreeMap<NodeId, BasicNode> =
            (0..n).map(|id| (id, BasicNode::default())).collect();
        nodes[0].raft.initialize(members).await.expect("initialize");
        Self { nodes, sb }
    }

    /// Borrow the node with the given id.
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    /// Restart node `id`: fully shut down and DROP the old replica (closing its
    /// fjall `Database` so the on-disk lock is released), then reopen from its dir
    /// (journal replay + openraft resume) and re-register. Models a clean bounce.
    ///
    /// The drop-before-reopen ordering is mandatory: fjall locks the data directory,
    /// so two `Database` handles on the same path cannot coexist. We `shutdown()` the
    /// core (joining the core task, which synchronously drops the log-store `Database`
    /// Arc), `deregister` the switchboard's handle, and drop the old `Node` (dropping
    /// `sm_kv`'s `Database` Arc). The state-machine `Database` Arc is released slightly
    /// later: openraft's SM-worker task is not joined by `shutdown()`, so it drops its
    /// Arc asynchronously once its command channel closes (a microsecond-scale,
    /// I/O-free path). What actually guarantees the reopen succeeds is fjall's lock
    /// acquisition, which retries (3×100ms) when `<dir>/.lock` is still held — a window
    /// that dwarfs the worker-drop latency, so by reopen time the lock is free.
    ///
    /// Panics if `id` is an in-memory node (no `dir`).
    pub async fn restart(&mut self, id: NodeId) {
        let i = id as usize;
        let dir = self.nodes[i].dir.clone().expect("durable node has a dir");
        // The single-range cluster lives at range 0; deregister/reopen the same
        // (range, id) the node registered under.
        let range = self.nodes[i].range;
        // Take ownership of the old node out of the vec (remove+insert at the same
        // index preserves order and keeps `nodes[id]` stable).
        let old = self.nodes.remove(i);
        old.raft.shutdown().await.ok(); // join the core; releases the log-store Arc
        self.sb.deregister(range, id); // drop the switchboard's (now-dead) handle
        drop(old); // drop sm_kv's Database Arc (SM-worker Arc drops just after)
        let new =
            Node::start_durable(range, id, self.sb.clone(), dir, Node::default_config()).await;
        self.nodes.insert(i, new);
    }

    /// Crash + restart node `id`. In-process we cannot truly kill mid-fsync; fjall's
    /// fsync-before-ack plus journal replay give the same guarantee as power loss for
    /// already-acked writes, so the nemesis form reduces to a hard reopen.
    pub async fn crash_restart(&mut self, id: NodeId) {
        self.restart(id).await;
    }

    /// Await a stable leader and return its id. Bounded so a stuck group fails
    /// the test instead of hanging.
    pub async fn wait_for_leader(&self) -> NodeId {
        self.nodes[0]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("leader elected")
            .current_leader
            .expect("leader id")
    }

    /// The current leader, scanning every node's own view. A node is the leader
    /// only if it both reports `state == Leader` and names itself as
    /// `current_leader`; this avoids trusting a stale `current_leader` from an
    /// isolated node (e.g. an old leader that hasn't yet noticed it lost quorum).
    pub fn leader(&self) -> Option<&Node> {
        self.nodes.iter().find(|n| {
            let m = n.raft.metrics().borrow().clone();
            m.state == ServerState::Leader && m.current_leader == Some(n.id)
        })
    }

    /// Await a leader that is *not* `old`, returning its id. Used after isolating
    /// the previous leader to confirm the surviving majority elected a fresh one.
    ///
    /// Probes from a node other than `old` (whose view of `old` is stale once it
    /// is isolated): it waits until that node names some leader `l != old`. To be
    /// robust against probing a node that is itself in the minority, it probes
    /// every `id != old` concurrently-ish and returns the first established
    /// non-`old` leader. Bounded so a stuck group fails the test, never hangs.
    pub async fn wait_for_leader_excluding(&self, old: NodeId) -> NodeId {
        let n = self.nodes.len() as u64;
        for probe in 0..n {
            if probe == old {
                continue;
            }
            // A probe in the majority will observe the new leader; one stranded
            // with `old` would time out, so try the next probe instead of failing.
            let observed = self.nodes[probe as usize]
                .raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(
                    |m| m.current_leader.is_some_and(|l| l != old),
                    "new leader excluding old",
                )
                .await;
            if let Ok(m) = observed
                && let Some(l) = m.current_leader.filter(|&l| l != old)
            {
                return l;
            }
        }
        panic!("no new leader (excluding {old}) was elected within the bound");
    }

    /// Pause (crash) a node: it drops every RPC it would send or receive.
    pub fn pause(&self, id: NodeId) {
        self.sb.pause(id);
    }

    /// Resume a previously paused node.
    pub fn resume(&self, id: NodeId) {
        self.sb.resume(id);
    }

    /// Partition a node away from every other node (cut all its links).
    pub fn isolate(&self, id: NodeId) {
        for other in 0..self.nodes.len() as u64 {
            if other != id {
                self.sb.cut(id, other);
            }
        }
    }

    /// Clear every fault: all cuts healed and all paused nodes resumed.
    pub fn heal(&self) {
        self.sb.heal();
    }

    /// Propose a raw write batch on the current leader.
    pub async fn write(&self, ops: Vec<kv::WriteOp>) -> Result<(), String> {
        let leader = self.leader().ok_or("no leader")?;
        leader
            .raft
            .client_write(WriteBatch(ops))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
