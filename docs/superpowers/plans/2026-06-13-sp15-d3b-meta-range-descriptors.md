# SP15 / D3b-meta — Replicated range descriptors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the table→range layout from static `--range-boundaries` config into a Raft-replicated descriptor blob in range 0, read+cached at a two-phase node bootstrap, so every node routes by replicated truth instead of trusting identical operator config.

**Architecture:** A versioned descriptor blob (`RangeMap::to/from_descriptor_bytes`) is committed under `/0/meta/range_map` through range 0's existing Raft group. A node in the new `RangeLayout::Replicated` mode brings up range 0 first from a static seed, (if bootstrap) writes the seed boundaries, waits for the committed blob to apply locally, decodes the authoritative `RangeMap`, then brings up the data ranges it names. Default `RangeLayout::Static` preserves today's single-pass bring-up verbatim.

**Tech Stack:** Rust 2024, openraft 0.9.24, `cluster`/`executor`/`catalog`/`kv` crates, fjall. Tests under cargo-nextest. No new shipped dependency.

**Spec:** `docs/superpowers/specs/2026-06-13-crabgresql-sp15-d3b-meta-range-descriptors-design.md`

**Branch:** `sp15-d3b-meta-range-descriptors` (already created, stacked on merged SP14).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `crates/cluster/src/range/map.rs` | Modify | Add `RangeDescriptor`, `RANGE_MAP_VERSION`, `descriptors()`, `to_descriptor_bytes()`, `from_descriptor_bytes()` + decode helpers. `RangeMap`'s routing API is unchanged. |
| `crates/kv/src/key.rs` | Modify | Add `meta_range_map_key()` (`/0/meta/range_map`). |
| `crates/cluster/src/range/meta.rs` | Create | `read_range_map`, `write_range_map`, `wait_for_range_map` — the meta-range descriptor store + bootstrap wait. |
| `crates/cluster/src/range/mod.rs` | Modify | `pub mod meta;` + re-exports. |
| `crates/cluster/src/durable.rs` | Modify | `NodeStore::open_range(&mut self, range)` — on-demand keyspace open. |
| `crates/cluster/src/server_node.rs` | Modify | `RangeLayout` enum; `NodeConfig.layout`; extract `build_range_group`; two-phase Replicated `start()`; `ServerNode.range_map` field. Update the 3 in-file test NodeConfig sites. |
| `crates/crabgresql/src/main.rs` | Modify | `--replicated-ranges` flag; build `RangeLayout` in `run_node`. Update the NodeConfig site. |
| `crates/cluster/tests/remote_forward.rs`, `crates/cluster/tests/gateway_local.rs` | Modify | Update NodeConfig sites to `layout: RangeLayout::Static(map)`. |
| `crates/cluster/tests/meta_range_replicated.rs` | Create | Deterministic in-crate 2-node tests: no-seed derivation, wrong-seed override, replicated routing. |
| `crates/crabgresql/tests/meta_range_gateway.rs` | Create | Multi-process e2e (UAC-safe binary name). |
| `crates/crabgresql/tests/harness/mod.rs` | Modify | `replicated` param on `spawn_node`; `spawn_multirange_replicated`. |
| `CLAUDE.md` | Modify | Add `meta_range_gateway` + `meta_range_replicated` to the SP14 audit list (now SP15). |

**Verify-each-task convention.** Every task's final step runs, from the repo root:
```
cargo fmt --all
cargo clippy -p <crate touched> --all-targets -- -D warnings
cargo nextest run -p <crate touched> <filter>
```
`cargo fmt` is part of every task (implementers run clippy+test but historically skip fmt). For tasks touching `cluster`, also run the cluster integration suites named in the step.

---

## Task 1: Descriptor data model + versioned blob codec

**Files:**
- Modify: `crates/cluster/src/range/map.rs`

A `RangeMap` is fully described by its boundaries; a descriptor adds an explicit `range_id` and the `[start, end)` span so the on-disk format is forward-compatible. Decode validates a contiguous `0..N` partition and returns `KvError::CorruptRow` (never panics) — mirroring `catalog/src/serde.rs`, which uses the same error for corrupt schema bytes (so no new error type is introduced; this is a deliberate, consistent reuse).

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block in `crates/cluster/src/range/map.rs`:

```rust
    #[test]
    fn descriptor_bytes_round_trip() {
        for m in [
            RangeMap::single(),
            RangeMap::with_boundaries(vec![2]),
            RangeMap::with_boundaries(vec![10, 20]),
            RangeMap::with_boundaries(vec![1, 5, 100, 4096]),
        ] {
            let bytes = m.to_descriptor_bytes();
            let back = RangeMap::from_descriptor_bytes(&bytes).expect("decode");
            assert_eq!(back, m, "range map must round-trip through its blob");
        }
    }

    #[test]
    fn descriptors_describe_each_range_span() {
        let m = RangeMap::with_boundaries(vec![2]);
        let d = m.descriptors();
        assert_eq!(d.len(), 2);
        assert_eq!(d[0], RangeDescriptor { range_id: 0, start_table_id: 0, end: Some(2) });
        assert_eq!(d[1], RangeDescriptor { range_id: 1, start_table_id: 2, end: None });
    }

    #[test]
    fn corrupt_blobs_error_not_panic() {
        // Truncated.
        assert!(RangeMap::from_descriptor_bytes(&[RANGE_MAP_VERSION, 0, 0]).is_err());
        // Unknown version.
        assert!(RangeMap::from_descriptor_bytes(&[99, 0, 0, 0, 1]).is_err());
        // Empty range set.
        let mut empty = vec![RANGE_MAP_VERSION];
        empty.extend_from_slice(&0u32.to_be_bytes());
        assert!(RangeMap::from_descriptor_bytes(&empty).is_err());
    }

    #[test]
    fn non_contiguous_descriptor_set_is_rejected() {
        // A hand-built blob whose range 1 starts at 5 but range 0 ends at 2
        // (a gap) must be rejected, not silently accepted.
        let mut b = vec![RANGE_MAP_VERSION];
        b.extend_from_slice(&2u32.to_be_bytes()); // count = 2
        // range 0: id 0, start 0, end Some(2)
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.push(1);
        b.extend_from_slice(&2u32.to_be_bytes());
        // range 1: id 1, start 5 (gap! should be 2), end None
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&5u32.to_be_bytes());
        b.push(0);
        b.extend_from_slice(&0u32.to_be_bytes());
        assert!(RangeMap::from_descriptor_bytes(&b).is_err());
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo nextest run -p cluster range::map::tests`
Expected: FAIL — `RangeDescriptor`, `RANGE_MAP_VERSION`, `descriptors`, `to_descriptor_bytes`, `from_descriptor_bytes` are not defined (compile error).

- [ ] **Step 3: Implement the model + codec**

In `crates/cluster/src/range/map.rs`, add `use kv::KvError;` to the imports (top of file, after `use catalog::TableId;`). Then add, after the `impl RangeMap { … }` block:

```rust
/// Current range-descriptor blob format version.
pub const RANGE_MAP_VERSION: u8 = 1;

/// One range's descriptor: its id and the half-open `[start_table_id, end)` span
/// of table ids it owns. `end == None` is the unbounded last range. This is the
/// unit of the replicated meta-range layout (SP15); today it is derived from a
/// `RangeMap`'s boundaries, but storing `range_id` explicitly keeps the on-disk
/// format forward-compatible with non-positional ids (range splits, D4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub range_id: RangeId,
    pub start_table_id: TableId,
    pub end: Option<TableId>,
}

impl RangeMap {
    /// This map as an ordered list of range descriptors (range 0 first).
    pub fn descriptors(&self) -> Vec<RangeDescriptor> {
        (0..self.range_count() as RangeId)
            .map(|i| RangeDescriptor {
                range_id: i,
                start_table_id: if i == 0 { 0 } else { self.boundaries[i as usize - 1] },
                end: self.boundaries.get(i as usize).copied(),
            })
            .collect()
    }

    /// Serialize to the replicated descriptor blob. Format:
    /// `[version:u8][count:u32 BE]` then per range
    /// `[range_id:u32 BE][start:u32 BE][end_present:u8][end:u32 BE]`.
    pub fn to_descriptor_bytes(&self) -> Vec<u8> {
        let descs = self.descriptors();
        let mut out = vec![RANGE_MAP_VERSION];
        out.extend_from_slice(&(descs.len() as u32).to_be_bytes());
        for d in &descs {
            out.extend_from_slice(&d.range_id.to_be_bytes());
            out.extend_from_slice(&d.start_table_id.to_be_bytes());
            match d.end {
                Some(e) => {
                    out.push(1);
                    out.extend_from_slice(&e.to_be_bytes());
                }
                None => {
                    out.push(0);
                    out.extend_from_slice(&0u32.to_be_bytes());
                }
            }
        }
        out
    }

    /// Reconstruct a `RangeMap` from a descriptor blob. Returns
    /// `KvError::CorruptRow` (never panics) for truncated bytes, an unknown
    /// version, or a descriptor set that is not a contiguous `0..N` table-id
    /// partition starting at range 0 / table 0 — e.g. a forward-version blob that
    /// uses split features this slice does not support.
    pub fn from_descriptor_bytes(bytes: &[u8]) -> Result<RangeMap, KvError> {
        let mut cur = bytes;
        let version = take_u8(&mut cur)?;
        if version != RANGE_MAP_VERSION {
            return Err(KvError::CorruptRow(format!(
                "unknown range-map version {version}"
            )));
        }
        let count = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
        if count == 0 {
            return Err(KvError::CorruptRow("range map has zero ranges".into()));
        }
        let mut boundaries = Vec::with_capacity(count.saturating_sub(1));
        let mut expected_start: TableId = 0;
        for i in 0..count {
            let range_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            let start = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            let end_present = take_u8(&mut cur)?;
            let end_raw = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            if range_id as usize != i {
                return Err(KvError::CorruptRow(format!(
                    "range ids must be contiguous 0..N; got {range_id} at position {i}"
                )));
            }
            if start != expected_start {
                return Err(KvError::CorruptRow(format!(
                    "range {i} starts at {start}, expected {expected_start} (ranges must be contiguous)"
                )));
            }
            let is_last = i + 1 == count;
            match (is_last, end_present) {
                (true, 0) => {} // last range is unbounded
                (true, _) => {
                    return Err(KvError::CorruptRow("last range must be unbounded".into()));
                }
                (false, 1) => {
                    if end_raw <= start {
                        return Err(KvError::CorruptRow("range end must exceed start".into()));
                    }
                    boundaries.push(end_raw);
                    expected_start = end_raw;
                }
                (false, _) => {
                    return Err(KvError::CorruptRow("non-last range must be bounded".into()));
                }
            }
        }
        Ok(RangeMap { boundaries })
    }
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated range map".into()))?;
    *cur = rest;
    Ok(*h)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated range-map field".into()));
    }
    let (h, rest) = cur.split_at(n);
    *cur = rest;
    Ok(h)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo nextest run -p cluster range::map::tests`
Expected: PASS (all `range::map::tests::*` green).

- [ ] **Step 5: Lint + format + commit**

```bash
cargo fmt --all
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/range/map.rs
git commit -m "feat(sp15): RangeMap descriptor model + versioned blob codec"
```

---

## Task 2: Meta-range descriptor store (key + read/write/wait)

**Files:**
- Modify: `crates/kv/src/key.rs`
- Create: `crates/cluster/src/range/meta.rs`
- Modify: `crates/cluster/src/range/mod.rs`

`read_range_map` is the pure read (testable with `MemKv`); `write_range_map` commits the blob through range 0's Raft; `wait_for_range_map` is the bootstrap wait — event-driven on range 0's openraft metrics, bounded, no fixed sleep.

- [ ] **Step 1: Write the failing key test**

Add to `#[cfg(test)] mod tests` in `crates/kv/src/key.rs`:

```rust
    #[test]
    fn meta_range_map_key_is_under_table_zero_meta() {
        let k = meta_range_map_key();
        let zero = {
            let mut z = Vec::new();
            crate::keyenc::put_u32(&mut z, 0);
            z
        };
        assert!(k.starts_with(&zero), "range map key is under table 0");
        assert_ne!(k, meta_next_table_id_key(), "distinct from next_table_id");
        assert_ne!(k, next_xid_key(), "distinct from next_xid");
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo nextest run -p kv key::tests::meta_range_map_key_is_under_table_zero_meta`
Expected: FAIL — `meta_range_map_key` not defined.

- [ ] **Step 3: Add the key**

In `crates/kv/src/key.rs`, after `meta_next_table_id_key`:

```rust
/// Key for the replicated range-descriptor blob: `/0/meta/range_map`.
pub fn meta_range_map_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"range_map");
    k
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cargo nextest run -p kv key::tests::meta_range_map_key_is_under_table_zero_meta`
Expected: PASS.

- [ ] **Step 5: Write the failing meta-store test**

Create `crates/cluster/src/range/meta.rs` with ONLY the test module first (the impl in Step 7 must make it compile+pass):

```rust
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
        if let Some(map) =
            read_range_map(store).map_err(|e| std::io::Error::other(format!("decode range map: {e:?}")))?
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
```

- [ ] **Step 6: Register the module**

In `crates/cluster/src/range/mod.rs`, add `pub mod meta;` alongside the other `pub mod` lines (keep `map`, `cluster`, `router` declarations intact).

- [ ] **Step 7: Run the meta tests to verify they pass**

Run: `cargo nextest run -p cluster range::meta::tests`
Expected: PASS (the impl above already satisfies the tests; this step confirms the module compiles and the pure read works).

- [ ] **Step 8: Lint + format + commit**

```bash
cargo fmt --all
cargo clippy -p kv -p cluster --all-targets -- -D warnings
git add crates/kv/src/key.rs crates/cluster/src/range/meta.rs crates/cluster/src/range/mod.rs
git commit -m "feat(sp15): meta-range descriptor store (key + read/write/wait)"
```

---

## Task 3: `NodeStore::open_range` — on-demand keyspace open

**Files:**
- Modify: `crates/cluster/src/durable.rs`

Replicated bring-up opens range 0 first, reads the blob, then opens each data range. `open_range` adds a range's `data-r{r}`/`raft-r{r}` pair to an already-open store, reusing the retained `Arc<Database>`. Static mode keeps `NodeStore::open(dir, &map)` opening everything up front — unchanged.

- [ ] **Step 1: Write the failing test**

Add to the `NodeStore` test module in `crates/cluster/src/durable.rs` (the existing `#[cfg(test)] mod tests` near the top-of-file NodeStore tests). If unsure which module, add a fresh test fn inside the existing NodeStore tests block:

```rust
    #[test]
    fn open_range_adds_a_keyspace_pair_after_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Open with the single-range map: only data-r0 / raft-r0 exist.
        let mut store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
        // Range 1 is not yet hosted — its keyspaces do not exist.
        assert!(store.ranges.get(&1).is_none(), "range 1 absent before open_range");
        // Add range 1 on demand.
        store.open_range(1).expect("open_range 1");
        assert!(store.ranges.get(&1).is_some(), "range 1 present after open_range");
        // data_kv(1) now works (would panic before).
        let kv1 = store.data_kv(1);
        kv1.put(b"k".to_vec(), b"v".to_vec()).expect("put r1");
        assert_eq!(kv1.get(b"k").expect("get r1"), Some(b"v".to_vec()));
        // Idempotent: opening an already-open range is a no-op, not an error.
        store.open_range(1).expect("open_range 1 again (idempotent)");
    }
```

(Note: `ranges` is a private field of `NodeStore`; this test lives in the same module, so it can read it. `RangeMap` and `tempfile` are already imported in the durable.rs test module per the existing NodeStore tests; if not, add `use crate::range::map::RangeMap;` to the test module.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo nextest run -p cluster durable::tests::open_range_adds_a_keyspace_pair_after_open`
Expected: FAIL — `open_range` not defined.

- [ ] **Step 3: Implement `open_range`**

In `crates/cluster/src/durable.rs`, inside `impl NodeStore`, after `keyspaces` / before `data_kv`:

```rust
    /// Open (creating if absent) `range`'s `data-r{range}` / `raft-r{range}`
    /// keyspace pair on an already-open store, reusing the shared `Database`.
    /// Idempotent: a range already hosted is left as-is. Replicated-mode bring-up
    /// (SP15) calls this for each data range after reading the descriptor blob;
    /// Static mode never calls it (every range is opened up front by `open`).
    pub fn open_range(&mut self, range: RangeId) -> Result<(), kv::KvError> {
        if self.ranges.contains_key(&range) {
            return Ok(());
        }
        let data = self
            .db
            .keyspace(&format!("data-r{range}"), KeyspaceCreateOptions::default)
            .map_err(|e| kv::KvError::Io(e.to_string()))?;
        let raft = self
            .db
            .keyspace(&format!("raft-r{range}"), KeyspaceCreateOptions::default)
            .map_err(|e| kv::KvError::Io(e.to_string()))?;
        self.ranges.insert(range, RangeKeyspaces { data, raft });
        Ok(())
    }
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cargo nextest run -p cluster durable::tests::open_range_adds_a_keyspace_pair_after_open`
Expected: PASS.

- [ ] **Step 5: Lint + format + commit**

```bash
cargo fmt --all
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/durable.rs
git commit -m "feat(sp15): NodeStore::open_range for on-demand keyspace open"
```

---

## Task 4: Two-phase `ServerNode` bootstrap + `RangeLayout` + CLI

**Files:**
- Modify: `crates/cluster/src/server_node.rs`
- Modify: `crates/crabgresql/src/main.rs`
- Modify: `crates/cluster/tests/remote_forward.rs`, `crates/cluster/tests/gateway_local.rs`

This is the slice's center of gravity. It has three internal stages, each committed: (4a) a **pure refactor** — extract the per-range build loop, introduce `RangeLayout`, add the `range_map` field, port every `NodeConfig` site to `Static` (all existing tests stay green); (4b) the **Replicated** two-phase `start()`; (4c) the **CLI** flag.

### Stage 4a — Pure refactor (Static path unchanged)

- [ ] **Step 1: Add the `RangeLayout` enum and the `range_map` field; change `NodeConfig`**

In `crates/cluster/src/server_node.rs`:

Add the enum (after the imports, before `NodeConfig`):

```rust
/// Where a node's range layout comes from.
pub enum RangeLayout {
    /// Static config: the range map is fixed at startup, identical on every node
    /// (today's behavior — the SP9/SP10/SP13/SP14 path). The single-range default
    /// is `RangeLayout::Static(RangeMap::single())`.
    Static(RangeMap),
    /// Replicated: the authoritative range map is read from the meta range (range
    /// 0) at a two-phase bootstrap. `seed` is `Some` only on the bootstrap node,
    /// which writes it as the initial descriptor blob; a joining node passes
    /// `None` and learns the layout from the meta range.
    Replicated { seed: Option<RangeMap> },
}
```

In `NodeConfig`, replace the `range_map` field:

```rust
    /// Where this node's range layout comes from. Defaults to
    /// `RangeLayout::Static(RangeMap::single())` (single range — the fast path).
    pub layout: RangeLayout,
```
(Delete the old `pub range_map: RangeMap,` field and its doc comment.)

In the `ServerNode` struct, add a field (after `id: NodeId,`):

```rust
    /// The authoritative range map this node brought up (Static config or the
    /// committed Replicated descriptor blob). Exposed so tests can assert a node
    /// derived its layout from the meta range rather than its own seed.
    pub range_map: RangeMap,
```

- [ ] **Step 2: Extract the per-range build into `build_range_group`**

Add this free async fn in `crates/cluster/src/server_node.rs` (e.g. just after `raft_config`):

```rust
/// Build one range's Raft group + applied store + replicated engine over its
/// per-range keyspaces, register it in the `(range, node)` registry, and spawn its
/// reseed-on-leadership task. Returns `(raft, sm_kv, engine)` for the caller's
/// maps. Both Static and Replicated bring-up call this — Static once per range up
/// front, Replicated once for range 0 then once per data range after the blob read.
async fn build_range_group(
    store: &NodeStore,
    range: RangeId,
    id: NodeId,
    partition: &PartitionState,
    registry: &RangeRegistry,
    catalog_kv: &Arc<dyn kv::Kv>,
) -> (openraft::Raft<TypeConfig>, Arc<dyn kv::Kv>, Arc<SqlEngine>) {
    let log = DurableLogStore::open(store, range).expect("durable log");
    let sm = DurableStateMachineStore::open(store, range).expect("durable sm");
    // Annotate as the trait object so the returned trio coerces cleanly (the
    // original inlined code relied on the HashMap-insert site for this coercion).
    let sm_kv: Arc<dyn kv::Kv> = sm.sm_kv();

    let net = TcpRaftNetwork {
        from: id,
        range,
        partition: partition.clone(),
    };
    let raft = openraft::Raft::new(id, raft_config(), net, log, sm)
        .await
        .expect("raft::new");
    registry.register(range, id, raft.clone());

    let engine = Arc::new(
        SqlEngine::replicated(
            catalog_kv.clone(),
            sm_kv.clone(),
            Arc::new(RaftCommitter { raft: raft.clone() }),
            Arc::new(RaftLinearizer { raft: raft.clone() }),
        )
        .expect("replicated engine"),
    );
    tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));

    (raft, sm_kv, engine)
}
```

- [ ] **Step 3: Rewrite `start()` to dispatch on `layout`, with the Static path using the helper**

Replace the body of `start()` (`crates/cluster/src/server_node.rs`). The Static arm reproduces today's behavior exactly via `build_range_group`; the Replicated arm is a stub that `todo!()`s for now (Stage 4b fills it):

```rust
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        match cfg.layout {
            RangeLayout::Static(_) => Self::start_static(cfg).await,
            RangeLayout::Replicated { .. } => Self::start_replicated(cfg).await,
        }
    }

    async fn start_static(cfg: NodeConfig) -> std::io::Result<Self> {
        let RangeLayout::Static(map) = cfg.layout else {
            unreachable!("start_static called with non-static layout")
        };
        let store = NodeStore::open(&cfg.data_dir, &map).expect("open node store");
        let partition = PartitionState::default();
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        for range in map.range_ids() {
            let (raft, sm_kv, engine) =
                build_range_group(&store, range, cfg.id, &partition, &registry, &catalog_kv).await;
            rafts.insert(range, raft);
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        if cfg.bootstrap {
            for raft in rafts.values() {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
        }

        let sql_listener = bind_with_retry(&cfg.sql_addr).await?;
        let sql_config = Arc::new(pgwire::session::SessionConfig::trust());
        Self::spawn_sql(
            sql_listener,
            &map,
            &rafts,
            &engines,
            &partition,
            &catalog_kv,
            cfg.id,
            sql_config,
        );

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
            range_map: map,
        })
    }
```

Extract the SQL-listener wiring (today's `if range_count > 1 { serve_range_routed } else { serve_routed }`) into a shared helper so Stage 4b reuses it:

```rust
    /// Spawn the SQL listener: the per-statement range gateway for a multi-range
    /// node, or the single-range leader-routing fast path for one range.
    fn spawn_sql(
        sql_listener: TcpListener,
        map: &RangeMap,
        rafts: &HashMap<RangeId, openraft::Raft<TypeConfig>>,
        engines: &HashMap<RangeId, Arc<SqlEngine>>,
        partition: &PartitionState,
        catalog_kv: &Arc<dyn kv::Kv>,
        id: NodeId,
        sql_config: Arc<pgwire::session::SessionConfig>,
    ) {
        if map.range_count() > 1 {
            let pool = crate::forward::ForwardPool::new(
                rafts.clone(),
                partition.clone(),
                crate::forward::RetryCounter::default(),
            );
            let forward: Arc<dyn crate::range::router::RemoteForward> =
                Arc::new(crate::forward::PgwireForward { pool });
            let leads: Arc<dyn crate::range::router::LeadsRange> = Arc::new(NodeLeadership {
                rafts: rafts.clone(),
                id,
            });
            tokio::spawn(crate::route::serve_range_routed(
                sql_listener,
                map.clone(),
                engines.clone(),
                leads,
                catalog_kv.clone(),
                forward,
                sql_config,
            ));
        } else {
            tokio::spawn(crate::route::serve_routed(
                sql_listener,
                rafts[&0].clone(),
                engines[&0].clone(),
                sql_config,
            ));
        }
    }
```

Add a placeholder for the Replicated arm so the crate compiles (Stage 4b replaces it):

```rust
    async fn start_replicated(_cfg: NodeConfig) -> std::io::Result<Self> {
        todo!("Stage 4b: two-phase replicated bootstrap")
    }
```

- [ ] **Step 4: Port every `NodeConfig` construction site to `layout`**

The 3 in-file test sites in `crates/cluster/src/server_node.rs` and the test harnesses change `range_map: X` → `layout: RangeLayout::Static(X)`:

- `server_node.rs` tests: `single_node_serves_sql_after_election` (`range_map: RangeMap::single()` → `layout: RangeLayout::Static(RangeMap::single())`), `multi_range_node_elects_a_leader_per_range` and `a_write_to_range1_is_isolated_to_data_r1` (`range_map: RangeMap::with_boundaries(vec![1])` → `layout: RangeLayout::Static(RangeMap::with_boundaries(vec![1]))`). Add `RangeLayout` to the test `use super::*;` scope (it's a sibling item, already in scope).
- `crates/cluster/tests/remote_forward.rs` (`try_two_node_cluster`, 2 sites): `range_map: map.clone()` → `layout: RangeLayout::Static(map.clone())` and `range_map: map` → `layout: RangeLayout::Static(map)`. Add `RangeLayout` to the import: `use cluster::server_node::{NodeConfig, RangeLayout, ServerNode};`.
- `crates/cluster/tests/gateway_local.rs`: same edit at its NodeConfig site(s); add `RangeLayout` to the `cluster::server_node` import.
- `crates/crabgresql/src/main.rs` (`run_node`): handled in Stage 4c — for now, to keep the workspace compiling, change `range_map,` to `layout: cluster::server_node::RangeLayout::Static(range_map),`.

- [ ] **Step 5: Verify the refactor is behavior-preserving (Static tests green)**

Run:
```
cargo nextest run -p cluster -p crabgresql
```
Expected: PASS — every existing test (server_node election/isolation, remote_forward, gateway_local, multirange, multiprocess, multirange_gateway, jepsen_elle) passes unchanged. This proves 4a is a pure refactor.

- [ ] **Step 6: Commit Stage 4a**

```bash
cargo fmt --all
cargo clippy -p cluster -p crabgresql --all-targets -- -D warnings
git add -A
git commit -m "refactor(sp15): RangeLayout + extract build_range_group/spawn_sql (Static path unchanged)"
```

### Stage 4b — Replicated two-phase bootstrap

- [ ] **Step 7: Implement `start_replicated`**

Replace the `start_replicated` placeholder in `crates/cluster/src/server_node.rs`. Add exactly ONE import at the top of the file: `use crate::range::meta::{seed_if_absent, wait_for_range_map};`. Do **not** add `use openraft::Raft;` — the file uses the fully-qualified `openraft::Raft<TypeConfig>` everywhere, and a bare unused import fails the `-D warnings` clippy gate. (`read_range_map`/`write_range_map` are used only transitively via `seed_if_absent`, so they are not imported here.) Use a generous bound for the bootstrap waits.

```rust
    /// How long the two-phase bootstrap waits for range 0 to elect + the
    /// descriptor blob to apply. Long enough to survive a slow CI election; a
    /// genuinely stuck cluster fails the start rather than hanging forever.
    async fn start_replicated(cfg: NodeConfig) -> std::io::Result<Self> {
        let RangeLayout::Replicated { seed } = cfg.layout else {
            unreachable!("start_replicated called with non-replicated layout")
        };
        let boot_timeout = Duration::from_secs(60);

        // Phase 1: bring up range 0 (the meta range) ONLY, from the static seed.
        let mut store = NodeStore::open(&cfg.data_dir, &RangeMap::single()).expect("open node store");
        let partition = PartitionState::default();
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        let (r0_raft, r0_sm_kv, r0_engine) =
            build_range_group(&store, 0, cfg.id, &partition, &registry, &catalog_kv).await;
        rafts.insert(0, r0_raft.clone());
        sm_kvs.insert(0, r0_sm_kv);
        engines.insert(0, r0_engine);

        // Bind the node listener BEFORE blocking on the blob, so peers can reach us
        // (range 0 needs a quorum to elect, and a joining node needs to receive the
        // seed via replication).
        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        // Bootstrap range 0's voting group (bootstrap node only) and, once we lead
        // it, seed the descriptor blob if absent. A bootstrap node defines the
        // cluster, so it must seed SOME layout; an absent seed (a direct caller
        // passing `seed: None` with `bootstrap: true`) defaults to a single range
        // rather than hanging the whole cluster on the blob wait.
        if cfg.bootstrap {
            tokio::spawn(bootstrap(r0_raft.clone(), cfg.peers.clone()));
            let seed_map = seed.unwrap_or_else(RangeMap::single);
            r0_raft
                .wait(Some(boot_timeout))
                .metrics(|m| m.current_leader == Some(cfg.id), "self range-0 leader")
                .await
                .map_err(|e| std::io::Error::other(format!("await range-0 leadership: {e}")))?;
            // create-if-absent: writes the blob once, at create; a restart finds it
            // present and does not rewrite it (the write-once invariant).
            seed_if_absent(&r0_raft, catalog_kv.as_ref(), &seed_map).await?;
        }

        // Phase 2: every node waits for the committed blob to apply locally, then
        // decodes the authoritative map.
        let map = wait_for_range_map(&r0_raft, catalog_kv.as_ref(), boot_timeout).await?;

        // Build each data range named by the descriptors.
        for range in map.range_ids().filter(|&r| r != 0) {
            store.open_range(range).expect("open data range keyspace");
            let (raft, sm_kv, engine) =
                build_range_group(&store, range, cfg.id, &partition, &registry, &catalog_kv).await;
            if cfg.bootstrap {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
            rafts.insert(range, raft);
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // A replicated node always serves through the gateway (even at one range):
        // the layout is dynamic in principle, so the byte-proxy fast path — which is
        // a static single-range optimization — does not apply here.
        let sql_listener = bind_with_retry(&cfg.sql_addr).await?;
        let sql_config = Arc::new(pgwire::session::SessionConfig::trust());
        Self::spawn_sql_gateway(
            sql_listener,
            &map,
            &rafts,
            &engines,
            &partition,
            &catalog_kv,
            cfg.id,
            sql_config,
        );

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
            range_map: map,
        })
    }
```

Add `spawn_sql_gateway` — the gateway-always variant (the `range_count > 1` branch of `spawn_sql`, factored so Replicated uses it regardless of count):

```rust
    /// Spawn the SQL listener as the per-statement range gateway, unconditionally
    /// (Replicated mode — see `start_replicated`).
    fn spawn_sql_gateway(
        sql_listener: TcpListener,
        map: &RangeMap,
        rafts: &HashMap<RangeId, openraft::Raft<TypeConfig>>,
        engines: &HashMap<RangeId, Arc<SqlEngine>>,
        partition: &PartitionState,
        catalog_kv: &Arc<dyn kv::Kv>,
        id: NodeId,
        sql_config: Arc<pgwire::session::SessionConfig>,
    ) {
        let pool = crate::forward::ForwardPool::new(
            rafts.clone(),
            partition.clone(),
            crate::forward::RetryCounter::default(),
        );
        let forward: Arc<dyn crate::range::router::RemoteForward> =
            Arc::new(crate::forward::PgwireForward { pool });
        let leads: Arc<dyn crate::range::router::LeadsRange> = Arc::new(NodeLeadership {
            rafts: rafts.clone(),
            id,
        });
        tokio::spawn(crate::route::serve_range_routed(
            sql_listener,
            map.clone(),
            engines.clone(),
            leads,
            catalog_kv.clone(),
            forward,
            sql_config,
        ));
    }
```

Then refactor `spawn_sql`'s `range_count > 1` branch to call `spawn_sql_gateway` (DRY): replace the inner body of that branch with `Self::spawn_sql_gateway(sql_listener, map, rafts, engines, partition, catalog_kv, id, sql_config);` and `return;`-style flow — or simply have `spawn_sql` call `spawn_sql_gateway` in the `> 1` arm and `serve_routed` in the `else` arm.

- [ ] **Step 8: Verify the crate still builds + Static tests still green**

Run:
```
cargo nextest run -p cluster -p crabgresql
```
Expected: PASS — no Replicated test exists yet, so this confirms 4b compiles and did not regress Static. (Replicated is exercised in Tasks 5/6.)

- [ ] **Step 9: Commit Stage 4b**

```bash
cargo fmt --all
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/server_node.rs
git commit -m "feat(sp15): two-phase replicated ServerNode bootstrap"
```

### Stage 4c — CLI `--replicated-ranges`

- [ ] **Step 10: Add the flag + build the layout**

In `crates/crabgresql/src/main.rs`, add to `NodeArgs` (after `bootstrap`):

```rust
    /// Source the range layout from the replicated meta range (range 0) instead of
    /// trusting `--range-boundaries`. The bootstrap node seeds the layout from its
    /// `--range-boundaries`; a joining node needs no boundaries — it reads them
    /// from the meta range.
    #[arg(long)]
    replicated_ranges: bool,
```

In `run_node`, replace the `range_map` construction + the `layout: ...Static(range_map)` line (from Stage 4a Step 4) with:

```rust
    let seed = if a.range_boundaries.is_empty() {
        cluster::range::RangeMap::single()
    } else {
        cluster::range::RangeMap::with_boundaries(a.range_boundaries.clone())
    };
    let layout = if a.replicated_ranges {
        // Only the bootstrap node carries a seed; a joining node learns the layout
        // from the meta range.
        let seed = if a.bootstrap { Some(seed) } else { None };
        cluster::server_node::RangeLayout::Replicated { seed }
    } else {
        cluster::server_node::RangeLayout::Static(seed)
    };
```

And in the `NodeConfig { … }` literal, use `layout,` instead of `range_map,` / the temporary `layout: ...Static(range_map)`.

- [ ] **Step 11: Verify the binary builds + workspace green**

Run:
```
cargo nextest run -p crabgresql
cargo build -p crabgresql
```
Expected: PASS / builds. (The flag is exercised e2e in Task 6.)

- [ ] **Step 12: Commit Stage 4c**

```bash
cargo fmt --all
cargo clippy -p crabgresql --all-targets -- -D warnings
git add crates/crabgresql/src/main.rs
git commit -m "feat(sp15): --replicated-ranges CLI flag"
```

---

## Task 5: Deterministic in-crate Replicated tests

**Files:**
- Create: `crates/cluster/tests/meta_range_replicated.rs`

Two `ServerNode`s in one process over loopback TCP (modeled on `crates/cluster/tests/remote_forward.rs`): node 0 bootstraps in Replicated mode with a seed; node 1 joins with `seed = None` (or a *wrong* seed). The tests assert node 1's authoritative `range_map` is the committed one, not its config. Deterministic via `ServerNode.range_map` being populated only after the blob is read (so once `start()` returns, the layout is known).

- [ ] **Step 1: Write the test file**

Create `crates/cluster/tests/meta_range_replicated.rs`:

```rust
//! SP15 in-crate proof: a node in Replicated mode sources its range layout from
//! the meta range (range 0), not from its own `--range-boundaries`. Two
//! ServerNodes over loopback TCP; node 0 seeds the layout, node 1 learns it.
//! Deterministic — `start()` returns only after the committed blob is read, so
//! `node.range_map` is the authoritative map.

use cluster::range::map::RangeMap;
use cluster::server_node::{NodeConfig, RangeLayout, ServerNode};

async fn free_port() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Bring up a 2-node Replicated cluster: node 0 bootstraps with `seed`, node 1
/// joins with `joiner_seed`. Retries on the `free_port` bind race (bounded), the
/// established pattern from `remote_forward.rs`.
async fn two_node_replicated(
    seed: RangeMap,
    joiner_seed: Option<RangeMap>,
) -> (ServerNode, ServerNode) {
    let mut last_err = None;
    for _ in 0..16 {
        match try_two_node_replicated(seed.clone(), joiner_seed.clone()).await {
            Ok(pair) => return pair,
            Err(e) => last_err = Some(e),
        }
    }
    panic!("two_node_replicated: port race did not clear in 16 attempts: {last_err:?}");
}

async fn try_two_node_replicated(
    seed: RangeMap,
    joiner_seed: Option<RangeMap>,
) -> std::io::Result<(ServerNode, ServerNode)> {
    let n0_node = free_port().await;
    let n0_sql = free_port().await;
    let n1_node = free_port().await;
    let n1_sql = free_port().await;
    let peers = vec![
        (0u64, cluster::addr::pack(&n0_node, &n0_sql)),
        (1u64, cluster::addr::pack(&n1_node, &n1_sql)),
    ];
    let d0 = tempfile::tempdir().expect("tempdir0").keep();
    let d1 = tempfile::tempdir().expect("tempdir1").keep();

    // Start node 0 (bootstrap, seeds the layout) and node 1 concurrently — node 1
    // must be up to form range 0's quorum so node 0 can become leader and seed.
    let n0_cfg = NodeConfig {
        id: 0,
        node_addr: n0_node.clone(),
        sql_addr: n0_sql.clone(),
        data_dir: d0,
        peers: peers.clone(),
        bootstrap: true,
        layout: RangeLayout::Replicated { seed: Some(seed) },
    };
    let n1_cfg = NodeConfig {
        id: 1,
        node_addr: n1_node.clone(),
        sql_addr: n1_sql.clone(),
        data_dir: d1,
        peers,
        bootstrap: false,
        layout: RangeLayout::Replicated { seed: joiner_seed },
    };
    let (n0, n1) = tokio::try_join!(ServerNode::start(n0_cfg), ServerNode::start(n1_cfg))?;
    Ok((n0, n1))
}

/// Criterion 3: a node started with NO seed derives the bootstrap node's
/// committed range map from the meta range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_seed_node_derives_committed_range_map() {
    let seed = RangeMap::with_boundaries(vec![2]);
    let (n0, n1) = two_node_replicated(seed.clone(), None).await;
    assert_eq!(n0.range_map, seed, "bootstrap node uses its seed");
    assert_eq!(
        n1.range_map, seed,
        "joiner with no boundaries learns the layout from the meta range"
    );
}

/// Criterion 4 (load-bearing): a node started with a WRONG seed still routes by
/// the committed descriptors. Committed `[2]` ⇒ table id 2 is range 1; the
/// joiner's wrong seed `[3]` alone would put id 2 in range 0. The joiner follows
/// the committed map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_seed_node_routes_by_committed_map() {
    let committed = RangeMap::with_boundaries(vec![2]);
    let wrong = RangeMap::with_boundaries(vec![3]);
    let (_n0, n1) = two_node_replicated(committed.clone(), Some(wrong)).await;
    assert_eq!(
        n1.range_map, committed,
        "the meta range overrides the joiner's wrong local seed"
    );
    assert_eq!(
        n1.range_map.range_for_table(2),
        1,
        "table id 2 routes to range 1 per the committed map, not range 0 per the wrong seed"
    );
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cargo nextest run -p cluster --test meta_range_replicated`
Expected: PASS — both `no_seed_node_derives_committed_range_map` and `wrong_seed_node_routes_by_committed_map` green.

(If they hang/timeout, the likely cause is the two-phase bootstrap deadlock guard: node 0 must become range-0 leader, which needs node 1's range-0 raft alive — `tokio::try_join!` starting both concurrently is what makes that possible. Confirm both `start()` calls are joined concurrently, not awaited sequentially.)

- [ ] **Step 3: Add a replicated SQL-routing test (criterion 5)**

Append to `crates/cluster/tests/meta_range_replicated.rs`:

```rust
use std::time::Duration;

/// Await `range`'s self-confirmed leader across the two nodes (openraft event
/// wait, no sleep).
async fn wait_leader(n0: &ServerNode, n1: &ServerNode, range: u32) -> u64 {
    let mut set = tokio::task::JoinSet::new();
    for node in [n0, n1] {
        let raft = node.rafts.get(&range).expect("range raft").clone();
        set.spawn(async move {
            raft.wait(Some(Duration::from_secs(20)))
                .metrics(
                    |m| m.state == openraft::ServerState::Leader && m.current_leader == Some(m.id),
                    "self leader",
                )
                .await
                .map(|m| m.id)
                .ok()
        });
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Some(id)) = res {
            return id;
        }
    }
    panic!("range {range} elected no leader");
}

/// Await every replica of `range` applying up to the leader's applied index — the
/// `wait_for_replication` analog from `remote_forward.rs`. Captures the leader's
/// applied index as a relative target AFTER the write, then waits each replica to
/// reach it. Event-based, no sleep, no vacuous fixed index.
async fn wait_for_replication(n0: &ServerNode, n1: &ServerNode, range: u32) {
    let leader = wait_leader(n0, n1, range).await;
    let nodes = [n0, n1];
    let target = nodes[leader as usize]
        .rafts
        .get(&range)
        .expect("range raft")
        .metrics()
        .borrow()
        .last_applied
        .map(|l| l.index)
        .unwrap_or(0);
    for node in nodes {
        node.rafts
            .get(&range)
            .expect("range raft")
            .wait(Some(Duration::from_secs(20)))
            .metrics(
                |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                "follower caught up to leader applied index",
            )
            .await
            .expect("replication within bound");
    }
}

/// Criterion 5: routing through a replicated node lands rows in the range the
/// committed boundaries dictate. Drive writes via the forward pool (as
/// remote_forward.rs does) so the test does not depend on which node leads which
/// range. Committed `[2]`: table `a` (id 1) ⇒ range 0, table `b` (id 2) ⇒ range 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_routing_lands_rows_in_the_committed_range() {
    use cluster::forward::{ForwardPool, RetryCounter};
    let (n0, n1) = two_node_replicated(RangeMap::with_boundaries(vec![2]), None).await;
    wait_leader(&n0, &n1, 0).await;
    wait_leader(&n0, &n1, 1).await;

    // Use node 0's forward pool: it resolves each range's leader and forwards.
    let pool = ForwardPool::new(n0.rafts.clone(), n0.partition.clone(), RetryCounter::default());
    pool.forward(0, "CREATE TABLE a (id int4)".into())
        .await
        .expect("create a -> range 0"); // table id 1
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0 (DDL routes to range 0)"); // table id 2
    pool.forward(1, "INSERT INTO b VALUES (42)".into())
        .await
        .expect("insert b -> range 1");

    // The forwarded INSERT is committed+applied on range 1's leader (forward()
    // returns post-apply); wait for it to replicate to EVERY range-1 replica using
    // the relative-target pattern (NOT a vacuous index >= 1 — bootstrap + election
    // already push range 1's last_applied past 1 before any INSERT). Then assert the
    // row is in range 1's store and absent from range 0's, on BOTH nodes.
    wait_for_replication(&n0, &n1, 1).await;
    let prefix = kv::key::table_prefix(2); // table id 2 ⇒ range 1
    for node in [&n0, &n1] {
        assert!(
            !node.sm_kv(1).scan_prefix(&prefix).expect("scan r1").is_empty(),
            "row for table id 2 is on range 1's store of node {}",
            node.id()
        );
        assert!(
            node.sm_kv(0).scan_prefix(&prefix).expect("scan r0").is_empty(),
            "row for table id 2 is NOT on range 0's store of node {}",
            node.id()
        );
    }
}
```

- [ ] **Step 4: Run the routing test**

Run: `cargo nextest run -p cluster --test meta_range_replicated`
Expected: PASS — all three tests green.

- [ ] **Step 5: Add the write-once guard test (the immutable-blob invariant)**

The relocate-only correctness rationale (Decision 4 / criterion 4) is that the blob never changes after create, so a node's *local* read is always correct. Guard that invariant: once committed, a second seed with a *different* map must be a no-op. This drives the `create-if-absent` skip branch directly (a regression that drops the `.is_none()` check would otherwise pass the whole suite). Append to `crates/cluster/tests/meta_range_replicated.rs`:

```rust
/// Write-once guard: once the descriptor blob is committed, a second seed with a
/// DIFFERENT map does NOT rewrite it. This is the immutable-local-read invariant —
/// a stray rewrite would make every node's local (un-leader-confirmed) read stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descriptor_blob_is_write_once() {
    use cluster::range::meta::{read_range_map, seed_if_absent};
    let committed = RangeMap::with_boundaries(vec![2]);
    let (n0, _n1) = two_node_replicated(committed.clone(), None).await;

    // The blob is already present (seeded at bring-up). A second seed attempt with
    // a DIFFERENT map must be a no-op: create-if-absent sees the existing blob and
    // skips the write. (read_range_map reads the local applied store, which already
    // holds the committed [2], so no leader round-trip is needed.)
    seed_if_absent(
        &n0.rafts[&0],
        n0.sm_kv(0).as_ref(),
        &RangeMap::with_boundaries(vec![3]),
    )
    .await
    .expect("second seed is a no-op");

    assert_eq!(
        read_range_map(n0.sm_kv(0).as_ref()).expect("read"),
        Some(committed),
        "the committed blob was NOT overwritten by the second seed"
    );
}
```

- [ ] **Step 6: Run the guard test**

Run: `cargo nextest run -p cluster --test meta_range_replicated`
Expected: PASS — all four tests green (`no_seed_*`, `wrong_seed_*`, `replicated_routing_*`, `descriptor_blob_is_write_once`).

- [ ] **Step 7: Lint + format + commit**

```bash
cargo fmt --all
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/tests/meta_range_replicated.rs
git commit -m "test(sp15): replicated routing + no-seed/wrong-seed + write-once guard"
```

---

## Task 6: Multi-process e2e

**Files:**
- Modify: `crates/crabgresql/tests/harness/mod.rs`
- Create: `crates/crabgresql/tests/meta_range_gateway.rs`

A node that joins a 3-process cluster with **no** boundary config learns the layout from the meta range and routes/reads correctly across the real process boundary. Binary name `meta_range_gateway` is UAC-safe (no `setup/install/update/patch/upgrad`).

- [ ] **Step 1: Add `replicated` support to the harness**

In `crates/crabgresql/tests/harness/mod.rs`, change `spawn_node`'s signature to take a `replicated: bool` and a per-node boundaries slice (the bootstrap node seeds; joiners pass none). Update the existing callers to pass `replicated: false`:

```rust
fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
    boundaries: &[u32],
    replicated: bool,
    bootstrap: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_crabgresql"));
    cmd.arg("node")
        .arg("--id").arg(id.to_string())
        .arg("--node-addr").arg(node_addr)
        .arg("--sql-addr").arg(sql_addr)
        .arg("--data-dir").arg(dir);
    for p in peers {
        cmd.arg("--peer").arg(p);
    }
    for b in boundaries {
        cmd.arg("--range-boundaries").arg(b.to_string());
    }
    if replicated {
        cmd.arg("--replicated-ranges");
    }
    if bootstrap {
        cmd.arg("--bootstrap");
    }
    cmd.stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.spawn().expect("spawn node")
}
```

Update every existing `spawn_node(...)` call in the file (`spawn`, `spawn_multirange`, `respawn`, `add_node`) to insert `false` for the new `replicated` arg in the correct position (before `bootstrap`). Example for `spawn` (non-replicated, no boundaries): `spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, &[], false, *id == 0)`.

Add the replicated spawn method (after `spawn_multirange`):

```rust
    /// Spawn `n` Replicated-mode node processes. Node 0 bootstraps WITH
    /// `boundaries` (it seeds the descriptor blob); nodes 1.. start with NO
    /// boundaries and learn the layout from the meta range. All pass
    /// `--replicated-ranges`.
    pub async fn spawn_multirange_replicated(n: u64, boundaries: Vec<u32>) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> = info
            .iter()
            .map(|(id, na, sa)| format!("{id}@{na}|{sa}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            // Only the bootstrap node (0) carries the boundaries seed.
            let node_boundaries: &[u32] = if *id == 0 { &boundaries } else { &[] };
            let child = spawn_node(
                *id, node_addr, sql_addr, &dir, &peers_arg, node_boundaries, true, *id == 0,
            );
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        // `boundaries` is retained for respawn parity, but a respawned replicated
        // node also reads its layout from the meta range, so its boundaries are
        // irrelevant after the first boot.
        let c = Self { nodes, _tmp: tmp, peers_arg, boundaries };
        c.wait_for_leader().await;
        c
    }
```

- [ ] **Step 2: Write the e2e test**

Create `crates/crabgresql/tests/meta_range_gateway.rs`:

```rust
//! SP15 e2e: 3 processes in Replicated mode. Node 0 seeds the range layout into
//! the meta range; nodes 1 and 2 are given NO `--range-boundaries` and learn the
//! layout from the meta range. A client connects to a node that was never told the
//! boundaries, writes a row into each range, and reads them back through a
//! different node — proving the layout came from the replicated meta range, not
//! config. One test (a 3-node × 2-range cluster = 6 Raft instances) to keep the
//! binary from running two such clusters at once on a constrained runner.
mod harness;
use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

fn first_col(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(0).map(|s| s.to_string()),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_layout_is_learned_from_the_meta_range() {
    // Boundary at table_id 2 seeded by node 0 only; nodes 1 and 2 get no
    // boundaries. table `a` (id 1) -> range 0; table `b` (id 2) -> range 1.
    let c = Cluster::spawn_multirange_replicated(3, vec![2]).await;
    c.wait_for_leader().await;

    // Connect to node 1 — which was NEVER given the boundaries. Its gateway routes
    // by the layout it read from the meta range.
    {
        let gw = c.pg(1).await;
        gw.simple_query("CREATE TABLE a (id int4)")
            .await
            .expect("create a (range 0)");
        gw.simple_query("CREATE TABLE b (id int4)")
            .await
            .expect("create b (range 1)");
        gw.simple_query("INSERT INTO a VALUES (10)")
            .await
            .expect("insert a");
        gw.simple_query("INSERT INTO b VALUES (20)")
            .await
            .expect("insert b");
    }

    // Read both back through node 2 (also never given boundaries).
    let client = c.pg(2).await;
    let ra = client.simple_query("SELECT id FROM a").await.expect("select a");
    assert_eq!(row_count(&ra), 1, "node 2 reads a (range 0)");
    assert_eq!(first_col(&ra).as_deref(), Some("10"), "a.id == 10");
    let rb = client.simple_query("SELECT id FROM b").await.expect("select b");
    assert_eq!(row_count(&rb), 1, "node 2 reads b (range 1)");
    assert_eq!(first_col(&rb).as_deref(), Some("20"), "b.id == 20");
}
```

- [ ] **Step 3: Run the e2e test**

Run: `cargo nextest run -p crabgresql --test meta_range_gateway`
Expected: PASS — `replicated_layout_is_learned_from_the_meta_range` green. (The harness builds the `crabgresql` binary first; the test exercises `--replicated-ranges` end-to-end.)

- [ ] **Step 4: Confirm the other harness consumers still compile + pass**

Run: `cargo nextest run -p crabgresql`
Expected: PASS — `multiprocess`, `jepsen_elle`, `multirange_gateway` all still green (the `spawn_node` signature change was threaded through every caller).

- [ ] **Step 5: Lint + format + commit**

```bash
cargo fmt --all
cargo clippy -p crabgresql --all-targets -- -D warnings
git add crates/crabgresql/tests/harness/mod.rs crates/crabgresql/tests/meta_range_gateway.rs
git commit -m "test(sp15): multi-process e2e — layout learned from the meta range"
```

---

## Task 7: Gauntlet + traceability + CLAUDE.md audit + finish

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/specs/2026-06-13-crabgresql-sp15-d3b-meta-range-descriptors-design.md` (append a "Traceability (implemented)" table)

- [ ] **Step 1: Update the CLAUDE.md SP14 audit to include the new SP15 binaries**

In `CLAUDE.md`, the "SP14 audit" paragraph lists every UAC-safe integration-test binary. Add the two new cluster/crabgresql test binaries and note SP15:

- Add `meta_range_replicated` to the cluster crate's list.
- Add `meta_range_gateway` to the crabgresql crate's list.
- Append a sentence: "**SP15 (2026-06-13):** two new binaries — `cluster::meta_range_replicated` and `crabgresql::meta_range_gateway` — both UAC-safe (no `setup/install/update/patch/upgrad` substring)."

- [ ] **Step 2: Run the UAC name guard**

Run (Bash tool):
```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```
Expected: empty output (no UAC-trigger filenames). Also eyeball that no new `[[test]]/[[bin]]` name in any `Cargo.toml` was added.

- [ ] **Step 3: Append the traceability table to the spec**

Add to the end of the spec file:

```markdown
## Traceability (implemented)

| # | Criterion | Verified by |
|---|---|---|
| 1 | RangeMap round-trips its blob; corrupt/forward-version → Err, not panic | `cluster::range::map::tests::{descriptor_bytes_round_trip, corrupt_blobs_error_not_panic, non_contiguous_descriptor_set_is_rejected}` (T1) |
| 2 | bootstrap node commits its seed; blob present + decodes to seed | `cluster::range::meta::tests::read_range_map_absent_then_present` + `meta_range_replicated::no_seed_node_derives_committed_range_map` (T2/T5) |
| 3 | no-seed node derives the bootstrap node's committed map | `cluster::meta_range_replicated::no_seed_node_derives_committed_range_map` (T5) |
| 4 | wrong-seed node routes by the committed descriptors | `cluster::meta_range_replicated::wrong_seed_node_routes_by_committed_map` (T5) |
| R | write-once invariant: a second seed does NOT rewrite the committed blob (Risks-section guard) | `cluster::meta_range_replicated::descriptor_blob_is_write_once` (T5) |
| 5 | replicated routing lands rows in the committed range | `cluster::meta_range_replicated::replicated_routing_lands_rows_in_the_committed_range` (T5) |
| 6 | SP9/SP10/SP13/SP14 suites pass unchanged in Static mode | full `cargo nextest run --workspace` green (T4 refactor gate) |
| 7 | multi-process: a node with no boundary config learns the layout + routes/reads | `crabgresql::meta_range_gateway::replicated_layout_is_learned_from_the_meta_range` (T6) |
| 8 | no new dependency; `#![forbid(unsafe_code)]`; full gauntlet green | `cargo deny` + fmt + clippy + nextest + doctests (T7) |
```

- [ ] **Step 4: Run the full gauntlet**

Run, from the repo root:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo deny check bans licenses
./scripts/check-no-native.sh
```
Expected: all green — 0 test failures, no clippy warnings, fmt clean, deny clean, no native deps. (Run the heavy `nextest` last; on a constrained machine it respects the `.config/nextest.toml` concurrency groups.)

- [ ] **Step 5: Commit the gauntlet artifacts**

```bash
cargo fmt --all
git add CLAUDE.md docs/superpowers/specs/2026-06-13-crabgresql-sp15-d3b-meta-range-descriptors-design.md
git commit -m "docs(sp15): traceability table + CLAUDE.md UAC audit for SP15 binaries"
```

- [ ] **Step 6: Finish the branch**

Use the **superpowers:finishing-a-development-branch** skill. Standing preference for this project: option **2** (push to a fresh branch + open a PR). The PR body summarizes the relocate-only meta-range cut and links the spec; base is `main`.

---

## Self-Review notes (for the executor)

- **Type consistency:** `RangeLayout::Replicated { seed: Option<RangeMap> }`, `ServerNode.range_map: RangeMap`, `NodeConfig.layout: RangeLayout`, `RangeMap::from_descriptor_bytes(&[u8]) -> Result<RangeMap, kv::KvError>`, `meta::read_range_map(&dyn kv::Kv) -> Result<Option<RangeMap>, kv::KvError>`, `meta::wait_for_range_map(&Raft, &dyn kv::Kv, Duration) -> std::io::Result<RangeMap>` — these names are used identically across Tasks 1–6.
- **No-sleep rule:** the only waits added are `wait_for_range_map` (event-driven on openraft metrics, bounded) and openraft `wait().metrics(...)` in tests. The multi-process harness keeps its existing bounded poll cadence. No new fixed `sleep` in any test or harness path.
- **Backward-compat gate:** Task 4 Stage 4a Step 5 is the load-bearing proof that the `RangeLayout`/`build_range_group` refactor is behavior-preserving — it must be green before Stage 4b begins.
- **Bootstrap concurrency:** Task 5's `two_node_replicated` starts both nodes via `tokio::try_join!` because node 0 cannot become range-0 leader (and thus cannot seed) until node 1's range-0 raft is alive to form a quorum. Sequential `start()` would deadlock.
