# zerocopy Fixed-Layout Encoders Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the hand-rolled big-endian fixed-width byte reads/writes in `kv::keyenc` and `mvcc::version` to `zerocopy` typed records, byte-for-byte behavior-preserving.

**Architecture:** Use `zerocopy::byteorder::big_endian::{U32, U64}` (alignment-1, big-endian wrapper types) for the leaf key primitives, and a `#[repr(C)]` `TupleHeader` record for the 17-byte tuple header. Keys stay composable concatenations of order-preserving components; only the leaf reads/writes and the one genuine multi-field record change. Variable/tagged formats (`pgwire`, `kv::rowenc`, `catalog::serde`, `pgtypes::encoding`, `mvcc::clog`) are untouched.

**Tech Stack:** Rust (edition 2024), `zerocopy` 0.8 (`derive` feature), existing `proptest` suites, `cargo test` / `cargo clippy`.

**Spec:** `docs/superpowers/specs/2026-06-12-zerocopy-fixed-layout-encoders-design.md`

**Hard contract:** Every byte emitted must be identical to today. The existing tests in each module are the regression spec and MUST pass unchanged. `unsafe_code = "forbid"` stays in force (verified zerocopy-compatible).

---

## File Structure

- **Modify** `Cargo.toml` (root) — add `zerocopy` to `[workspace.dependencies]`.
- **Modify** `crates/kv/Cargo.toml` — add `zerocopy.workspace = true`.
- **Modify** `crates/mvcc/Cargo.toml` — add `zerocopy.workspace = true`.
- **Modify** `crates/kv/src/keyenc.rs` — reimplement `put_u32/put_u64/take_u32/take_u64` on `U32`/`U64`.
- **Modify** `crates/mvcc/src/version.rs` — add private `TupleHeader` record; rewrite `encode_tuple`, `decode_tuple`, `version_key_xid`, `xid_of_key`.

No new files. No public API signature changes.

---

## Task 1: Wire up the zerocopy dependency

**Files:**
- Modify: `Cargo.toml` (root, `[workspace.dependencies]`)
- Modify: `crates/kv/Cargo.toml`
- Modify: `crates/mvcc/Cargo.toml`

- [ ] **Step 1: Add zerocopy to workspace dependencies**

In root `Cargo.toml`, inside `[workspace.dependencies]`, add this line (next to the other third-party deps such as `bytes = "1"`):

```toml
zerocopy = { version = "0.8", features = ["derive"] }
```

- [ ] **Step 2: Add zerocopy to the `kv` crate**

In `crates/kv/Cargo.toml`, under `[dependencies]`, add:

```toml
zerocopy.workspace = true
```

- [ ] **Step 3: Add zerocopy to the `mvcc` crate**

In `crates/mvcc/Cargo.toml`, under `[dependencies]`, add:

```toml
zerocopy.workspace = true
```

- [ ] **Step 4: Verify both crates still build**

Run: `cargo build -p kv -p mvcc`
Expected: builds clean, no errors. (zerocopy was already a transitive dep, so this only promotes it to direct.)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/kv/Cargo.toml crates/mvcc/Cargo.toml
git commit -m "build(kv,mvcc): add direct zerocopy dependency"
```

---

## Task 2: Convert `kv::keyenc` leaf primitives to zerocopy

**Files:**
- Modify: `crates/kv/src/keyenc.rs`

This is a refactor of already-tested code. The existing `roundtrip_u32_u64`, `order_preservation_boundaries`, and the order-preserving proptests are the regression spec. We add one characterization test pinning the exact big-endian bytes, confirm it passes on the *current* implementation, then refactor and confirm everything still passes.

- [ ] **Step 1: Add a byte-layout characterization test**

In `crates/kv/src/keyenc.rs`, inside `mod tests`, add:

```rust
    #[test]
    fn put_emits_big_endian_bytes() {
        let mut b = Vec::new();
        put_u32(&mut b, 0x0102_0304);
        assert_eq!(b, vec![0x01, 0x02, 0x03, 0x04]);

        let mut b = Vec::new();
        put_u64(&mut b, 0x0102_0304_0506_0708);
        assert_eq!(b, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }
```

- [ ] **Step 2: Run the keyenc tests on the current (unchanged) implementation**

Run: `cargo test -p kv keyenc`
Expected: PASS, including the new `put_emits_big_endian_bytes`. This locks the byte contract before refactoring.

- [ ] **Step 3: Refactor the four primitives onto zerocopy**

Replace the top of `crates/kv/src/keyenc.rs` (the `use crate::KvError;` line and the four functions `put_u32`, `put_u64`, `take_u32`, `take_u64`) with:

```rust
use zerocopy::byteorder::big_endian::{U32, U64};
use zerocopy::{FromBytes, IntoBytes};

use crate::KvError;

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(U32::new(v).as_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(U64::new(v).as_bytes());
}

pub fn take_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let (v, rest) = U32::read_from_prefix(*cur)
        .map_err(|_| KvError::CorruptRow("truncated u32 key component".into()))?;
    *cur = rest;
    Ok(v.get())
}

pub fn take_u64(cur: &mut &[u8]) -> Result<u64, KvError> {
    let (v, rest) = U64::read_from_prefix(*cur)
        .map_err(|_| KvError::CorruptRow("truncated u64 key component".into()))?;
    *cur = rest;
    Ok(v.get())
}
```

Notes for the implementer:
- `U32::new(v).as_bytes()` yields the value's 4 big-endian bytes (`IntoBytes` trait). `read_from_prefix` returns `(value, rest)` and only fails when the slice is shorter than the type, exactly the old length check. `v.get()` converts the big-endian wrapper back to a native `u32`.
- The module doc comment and the `#[cfg(test)] mod tests` block stay as-is.

- [ ] **Step 4: Run the keyenc tests on the refactored implementation**

Run: `cargo test -p kv keyenc`
Expected: PASS — identical results to Step 2.

- [ ] **Step 5: Run the full `kv` suite (key construction rides on these primitives)**

Run: `cargo test -p kv`
Expected: PASS (all `key`, `keyenc`, `rowenc` tests green).

- [ ] **Step 6: Commit**

```bash
git add crates/kv/src/keyenc.rs
git commit -m "refactor(kv): zerocopy big-endian U32/U64 for key primitives"
```

---

## Task 3: Convert the `mvcc::version` tuple header to a zerocopy record

**Files:**
- Modify: `crates/mvcc/src/version.rs`

- [ ] **Step 1: Add the imports and the `TupleHeader` record**

In `crates/mvcc/src/version.rs`, add to the top imports (currently `use pgtypes::Datum;` and `use kv::KvError;`):

```rust
use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};
```

Then, directly above the existing `const T_TUPLE: u8 = 1;`, add:

```rust
/// Fixed 17-byte tuple header: tag + big-endian xmin/xmax. `#[repr(C)]` with
/// alignment-1 fields packs with no padding, matching the on-disk layout.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleHeader {
    tag: u8,
    xmin: U64,
    xmax: U64,
}
```

- [ ] **Step 2: Add lock-in tests for the header layout**

In `crates/mvcc/src/version.rs`, inside `mod tests`, add. `TupleHeader` and `T_TUPLE` come from the test module's existing `use super::*;`; the external `U64` type and `IntoBytes` trait are imported at function scope so the test does not depend on glob re-export of the parent's private `use` aliases:

```rust
    #[test]
    fn tuple_header_is_packed_17_bytes() {
        assert_eq!(core::mem::size_of::<TupleHeader>(), 17);
    }

    #[test]
    fn tuple_header_layout_matches_manual_be() {
        use zerocopy::IntoBytes;
        use zerocopy::byteorder::big_endian::U64;
        let h = TupleHeader {
            tag: T_TUPLE,
            xmin: U64::new(5),
            xmax: U64::new(9),
        };
        let mut manual = vec![T_TUPLE];
        manual.extend_from_slice(&5u64.to_be_bytes());
        manual.extend_from_slice(&9u64.to_be_bytes());
        assert_eq!(h.as_bytes(), manual.as_slice());
    }
```

- [ ] **Step 3: Run the new tests to verify the record compiles and packs correctly**

Run: `cargo test -p mvcc tuple_header`
Expected: PASS — `tuple_header_is_packed_17_bytes` and `tuple_header_layout_matches_manual_be` both green. (If `size_of` were not 17, the no-padding assumption would be wrong — this is the guard.)

- [ ] **Step 4: Rewrite `encode_tuple` and `decode_tuple` to use the record**

Replace the existing `encode_tuple` and `decode_tuple` function bodies in `crates/mvcc/src/version.rs` with:

```rust
/// Encode a tuple version: a 1-byte tag, the `xmin`/`xmax` header, then the row.
/// `xmax == INVALID_XID` (0) marks a live version. A delete keeps the row bytes
/// and sets `xmax` (PostgreSQL retains the tuple until vacuum).
pub fn encode_tuple(xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    let header = TupleHeader {
        tag: T_TUPLE,
        xmin: U64::new(xmin),
        xmax: U64::new(xmax),
    };
    let mut out = Vec::with_capacity(17 + row.len() * 8);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a tuple version into `(xmin, xmax, row)`.
pub fn decode_tuple(bytes: &[u8]) -> Result<(u64, u64, Vec<Datum>), KvError> {
    let (header, rest) = TupleHeader::ref_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("bad tuple header".into()))?;
    if header.tag != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }
    let row = kv::rowenc::decode_row(rest)?;
    Ok((header.xmin.get(), header.xmax.get(), row))
}
```

Notes for the implementer:
- `ref_from_prefix` borrows a `&TupleHeader` directly from `bytes` (zero-copy) and returns the trailing row bytes as `rest`. It fails when `bytes` is shorter than 17, matching the old `bytes.len() < 17` guard. The bad-tag branch matches the old `bytes[0] != T_TUPLE` guard. Both map to the same `"bad tuple header"` error as before.

- [ ] **Step 5: Run the version tuple tests**

Run: `cargo test -p mvcc`
Expected: PASS — `tuple_roundtrips_header_and_row`, `decode_tuple_rejects_corrupt`, the header lock-in tests, and all key tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/mvcc/src/version.rs
git commit -m "refactor(mvcc): zerocopy TupleHeader record for tuple encode/decode"
```

---

## Task 4: Convert the `mvcc::version` xid key helpers

**Files:**
- Modify: `crates/mvcc/src/version.rs`

- [ ] **Step 1: Rewrite `version_key_xid` and `xid_of_key`**

Replace the existing `version_key_xid` and `xid_of_key` function bodies in `crates/mvcc/src/version.rs` with:

```rust
/// SP5 version key: the row key followed by the creating xid (big-endian,
/// ascending). A rowid's versions all share `kv::key::row_key(table, rowid)`.
pub fn version_key_xid(table_id: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(U64::new(xid).as_bytes());
    k
}

/// The creating xid encoded in a version key's 8-byte suffix.
pub fn xid_of_key(key: &[u8]) -> Result<u64, KvError> {
    let (_, xid) = U64::read_from_suffix(key)
        .map_err(|_| KvError::CorruptRow("version key too short".into()))?;
    Ok(xid.get())
}
```

Notes for the implementer:
- `version_key_xid` now appends `U64::new(xid).as_bytes()` (identical big-endian bytes to `xid.to_be_bytes()`).
- `read_from_suffix` reads the trailing 8 bytes and returns `(prefix, value)`; it fails when `key.len() < 8`, matching the old guard, and produces the same `"version key too short"` error.
- `row_prefix_of` is unchanged — it returns a subslice, not a fixed scalar read, so it is not a zerocopy candidate.

- [ ] **Step 2: Run the version key tests**

Run: `cargo test -p mvcc`
Expected: PASS — `version_key_xid_is_rowid_prefix_plus_ascending_xid`, `row_prefix_of_strips_xid_suffix`, `row_prefix_of_rejects_too_short`, and all others green.

- [ ] **Step 3: Commit**

```bash
git add crates/mvcc/src/version.rs
git commit -m "refactor(mvcc): zerocopy U64 for version-key xid suffix"
```

---

## Task 5 (OPTIONAL): `kv::key::clog_key` consistency cleanup

The spec marks this **optional / not required for correctness** — it only removes a stray raw `to_be_bytes()` in favor of the keyenc helper. Skip this task entirely if you prefer the minimal diff; the bytes are identical either way.

**Files:**
- Modify: `crates/kv/src/key.rs`

- [ ] **Step 1: Route `clog_key` through the `put_u64` helper**

In `crates/kv/src/key.rs`, the `clog_key` function currently reads:

```rust
pub fn clog_key(xid: u64) -> Vec<u8> {
    let mut k = system_prefix("clog");
    k.extend_from_slice(&xid.to_be_bytes());
    k
}
```

Replace the body with:

```rust
pub fn clog_key(xid: u64) -> Vec<u8> {
    let mut k = system_prefix("clog");
    crate::keyenc::put_u64(&mut k, xid);
    k
}
```

- [ ] **Step 2: Run the `kv` suite**

Run: `cargo test -p kv`
Expected: PASS — `xid_and_clog_keys_are_under_table_zero_and_distinct` and all others green (clog keys still sort by xid; bytes unchanged).

- [ ] **Step 3: Commit**

```bash
git add crates/kv/src/key.rs
git commit -m "refactor(kv): route clog_key through keyenc::put_u64"
```

---

## Task 6: Full-workspace verification

**Files:** none (verification only).

- [ ] **Step 1: Run the whole test suite**

Run: `cargo test --workspace`
Expected: PASS — in particular the cross-crate regression suites: `executor` `durability` and `recovery`, the `conformance` corpus, plus `kv`/`mvcc` units. These exercise the on-disk formats end-to-end and prove the bytes are unchanged.

- [ ] **Step 2: Run clippy with warnings-as-errors (matches CI)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean — no new warnings, no `unwrap_used` hits, no `unsafe_code` violations (the `forbid` lint stays satisfied).

- [ ] **Step 3: Confirm no stray format changes**

Run: `git log --oneline main..HEAD`
Expected: the spec commit plus the Task 1–4 (and optionally Task 5) refactor commits, each scoped to the files listed above. No format-version bumps, no API signature changes.
