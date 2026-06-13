# Design: zerocopy for fixed-layout encoders (kv + mvcc)

**Date:** 2026-06-12
**Status:** Approved
**Scope:** Fixed-layout byte records in `kv` and `mvcc` only.

## Motivation

The directive: "use google/zerocopy everywhere possible." Taken literally this
collides with two facts about the codebase, so "possible" is bounded below.

1. **The workspace forbids unsafe.** `[workspace.lints.rust] unsafe_code =
   "forbid"`. zerocopy's derives emit trait impls for `unsafe` traits, so the
   first question was whether they even compile here. **Verified yes** with a
   throwaway crate: `#[derive(FromBytes, IntoBytes, KnownLayout, Immutable,
   Unaligned)]` on a `#[repr(C)] { u8, U64, U64 }` struct compiles and its tests
   pass under `unsafe_code = "forbid"`. zerocopy (already a transitive dep at
   v0.8.52) is forbid-compatible.

2. **The codebase splits into fixed-layout vs. variable/tagged formats.**
   zerocopy models a fixed-layout record (a `#[repr(C)]` struct whose bytes are
   the wire/disk bytes). It does **not** model tag-dispatched enums or
   length-prefixed strings as a single type. The variable formats are already
   on `bytes::Buf`/`BufMut` or hand-rolled tagged streams, which is the right
   tool for them.

So the genuine surface is the fixed-layout records, all in `kv` and `mvcc`.

### Why these formats suit zerocopy

`zerocopy::byteorder::big_endian::{U32, U64}` are alignment-1 wrapper types
that store their value in big-endian order. A `#[repr(C)]` struct built from
`u8` + `U64` fields therefore packs with **no padding** and lays out
byte-for-byte identically to the existing big-endian on-disk/wire formats. The
existing code hand-rolls this with `to_be_bytes()` / `from_be_bytes()` plus
`slice.try_into().expect("N bytes")`. zerocopy replaces the manual juggling
with a declarative struct and total (non-panicking) parsing.

## Goals

- Replace hand-rolled big-endian fixed-width reads/writes in `kv` and `mvcc`
  with zerocopy typed reads/writes.
- Remove `try_into().expect(...)` panic-paths in the targeted code; parsing
  becomes structurally total.
- Keep `unsafe_code = "forbid"` in force.

## Non-Goals

- No change to any byte format. This is byte-for-byte behavior-preserving; no
  format version bumps, no public API signature changes.
- No conversion of variable/tagged formats (see Non-Targets).

## Approach (chosen: "leaves + the one real record")

Convert at the leaf primitives that every key rides on, plus the single genuine
multi-field record (the tuple header). Keys remain *composable* concatenations
of order-preserving components, because system keys interleave string tags
(`"catalog/"`, `"seq/"`, `"/"`) that no monolithic key struct could share.

Rejected alternative: also introduce full typed key structs (`RowKey`,
`VersionKey`). More churn, and it forces the order-preserving user-row keys into
a struct mold the string-tagged system keys can't share, splitting the key
module into two idioms for little payoff.

## Dependency wiring

- Root `Cargo.toml`, `[workspace.dependencies]`:
  `zerocopy = { version = "0.8", features = ["derive"] }`
- `crates/kv/Cargo.toml` and `crates/mvcc/Cargo.toml`: add
  `zerocopy.workspace = true`.

Promotes the existing transitive dependency to a direct one in the two crates
that use it.

## Change 1 — `kv::keyenc` (the substrate)

Reimplement the four leaf functions on `U32`/`U64`. Example for `u32`; `u64` is
analogous:

```rust
use zerocopy::byteorder::big_endian::{U32, U64};
use zerocopy::{FromBytes, IntoBytes};

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(U32::new(v).as_bytes());
}

pub fn take_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let (v, rest) = U32::read_from_prefix(cur)
        .map_err(|_| KvError::CorruptRow("truncated u32 key component".into()))?;
    *cur = rest;
    Ok(v.get())
}
```

- `read_from_prefix` returns `(value, rest)` and advances the cursor; it fails
  only when the slice is too short — the same condition the manual length check
  guarded. Error messages are preserved verbatim.
- Byte output is identical big-endian.
- Every caller in `kv::key` is unchanged and improves for free.

## Change 2 — `mvcc::version`

Private record struct replacing the manual 17-byte header:

```rust
use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleHeader {
    tag: u8,
    xmin: U64,
    xmax: U64,
}
```

- `encode_tuple`: push
  `TupleHeader { tag: T_TUPLE, xmin: U64::new(xmin), xmax: U64::new(xmax) }
  .as_bytes()`, then the encoded row.
- `decode_tuple`: `TupleHeader::ref_from_prefix(bytes)` yields a borrowed
  header (true zero-copy read) plus the remaining row bytes; validate
  `tag == T_TUPLE`, then `decode_row(rest)`. A too-short slice or bad tag is a
  `CorruptRow` error, matching current behavior.
- `version_key_xid`: append `U64::new(xid).as_bytes()` instead of
  `xid.to_be_bytes()`.
- `xid_of_key`: `U64::read_from_suffix(key)` reads the trailing 8 bytes and
  errors when `key.len() < 8` — same contract as the current manual suffix
  read, without `try_into().expect("8 bytes")`.

Optional consistency cleanup (low priority): `kv::key::clog_key` currently calls
`xid.to_be_bytes()` directly; it may route through `U64::new(xid).as_bytes()`
for uniformity. Not required for correctness.

## Non-Targets (explicitly left alone)

- `kv::rowenc` — tagged datum stream, length-prefixed text. Variable.
- `catalog::serde` — version byte + variable columns. Variable.
- `pgtypes::encoding` — produces `Vec<u8>` from datums. Variable.
- All `pgwire` frontend/backend messages — variable, cstrings, nested
  count-prefixed arrays; already correctly on `bytes::Buf`/`BufMut`.
- `mvcc::clog` — a single enum status byte; zerocopy adds nothing.

## Invariants & Testing

**Hard contract: byte-for-byte identical output.** These are on-disk and
key-ordering formats; changing the bytes would break durability, recovery, and
ordering. The existing tests already pin the contract and MUST pass unchanged:

- `kv::keyenc` — `roundtrip_u32_u64`, `order_preservation_boundaries`, and the
  `u32`/`u64` order-preserving proptests.
- `kv::key` — `row_keys_sort_by_rowid_within_a_table` and siblings.
- `mvcc::version` — `tuple_roundtrips_header_and_row`,
  `version_key_xid_is_rowid_prefix_plus_ascending_xid`,
  `decode_tuple_rejects_corrupt`.
- Cross-crate `executor` `durability` / `recovery` suites and the
  `conformance` corpus.

**New lock-in tests:**

- `assert_eq!(core::mem::size_of::<TupleHeader>(), 17)` — guards the no-padding
  packing guarantee.
- A byte-layout assertion that `TupleHeader { .. }.as_bytes()` equals the old
  manual layout `[tag] ++ xmin.to_be_bytes() ++ xmax.to_be_bytes()`.

**Conventions:**

- `unsafe_code = "forbid"` stays in force (verified compatible).
- Repo convention `expect("reason")` over `unwrap`; the net effect here removes
  `expect` calls rather than adding them.
- CI runs `-D warnings` with `clippy::unwrap_used = "warn"`; the conversion adds
  no new warnings.

## Risks

- **Order-preservation depends on big-endian.** `U64`/`U32` from
  `zerocopy::byteorder::big_endian` are big-endian by definition, and the
  `keyenc` order-preserving proptests guard it. Low risk.
- **Struct padding.** Mitigated by `Unaligned` + alignment-1 byteorder types
  and the explicit `size_of == 17` test.
