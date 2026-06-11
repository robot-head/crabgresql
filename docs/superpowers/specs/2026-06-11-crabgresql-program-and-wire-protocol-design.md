# crabgresql: Program Architecture & Sub-project 1 (Wire Protocol + Conformance Oracle)

**Date:** 2026-06-11
**Status:** Approved

## Vision

crabgresql is a from-scratch implementation of PostgreSQL in Rust with two
headline goals:

1. **Literal full parity** with PostgreSQL — every feature, every datatype,
   every behavior, measured against PostgreSQL's own regression suite. This is
   a multi-year program executed as a sequence of independently shippable
   sub-projects.
2. **Cloud-native horizontal scalability** — a distributed-first core
   (shared-nothing, range-sharded, Raft-replicated), not a sharding layer
   bolted onto a single-node engine. Object-storage tiering is a later phase.

**Safety policy:** `#![forbid(unsafe_code)]` in every crabgresql crate,
enforced in CI. Dependencies must be pure Rust: no C/C++ anywhere in the
shipped binary's build graph, no `*-sys` crates, no build scripts invoking a C
compiler. Enforced with `cargo-deny`. Dependencies may contain `unsafe`
internally (tokio, etc.) — the guarantee is "zero unsafe in our code, zero C
in the shipped artifact." Dev/test-only tooling (e.g. a real PostgreSQL in a
container used as a test oracle) is exempt.

**Parity baseline:** PostgreSQL **18** (current stable). The baseline moves
only by deliberate re-pinning as a tracked task, never by chasing head.
"Full parity" is parity with something fixed.

## Program architecture

### Workspace layout

One Cargo workspace; every crate carries `#![forbid(unsafe_code)]`.

| Crate | Purpose |
|---|---|
| `pgwire` | PostgreSQL v3 wire protocol: startup, TLS, auth, simple + extended query, COPY (later) |
| `pgparser` | Lexer/parser producing a Postgres-fidelity AST, ported incrementally from PostgreSQL's `gram.y`. Off-the-shelf `sqlparser-rs` lacks the fidelity full parity requires, though it may be cribbed from early. |
| `catalog` | System catalogs (`pg_catalog`, `information_schema`) |
| `pgtypes` | Datatype/operator/function system (text + binary encodings) |
| `planner` | Logical/physical planning, cost model |
| `executor` | Execution engine |
| `kv` | Distributed-first heart: range-sharded transactional KV with MVCC |
| `raft` | Consensus, via **openraft** (pure Rust) initially |
| `storage` | Per-range local persistence, via **fjall** (pure-Rust LSM) initially |
| `conformance` | Differential-testing oracle + ported regression suite + parity dashboard |
| `crabgresql` | The node binary |

### Locked-in technology choices

- **tokio** for async I/O.
- **rustls with a pure-Rust crypto provider** for TLS. The default
  `ring`/`aws-lc-rs` providers contain C and assembly — this is the easiest
  place to silently violate zero-C, so the provider choice is pinned and
  guarded by `cargo-deny`.
- **RustCrypto** crates (`sha2`, `hmac`, `pbkdf2`, etc.) for SCRAM-SHA-256.
- **openraft** for consensus and **fjall** for local storage initially.
  Owning Raft and the storage engine are candidate later sub-projects once
  profiling or correctness needs justify them — "use pure-Rust crates now,
  own them later" is the deliberate strategy, not a compromise to revisit
  accidentally.

### Known-hard parity items (planned, not discovered)

A distributed-first core makes some PostgreSQL behaviors materially harder.
These are tracked from day one rather than found late: advisory locks,
`txid_current()`/xid semantics, vacuum/freeze observable behavior, sequence
semantics under distribution, `pg_stat_*` views, and the C-ABI extension
ecosystem (out of scope by definition — zero C — and the largest permanent
parity asterisk; native-Rust extension points are a future design).

### Roadmap

Each item is one spec → plan → implementation cycle. The first four are
concrete; later items are directional and re-planned as parity data comes in.

1. **Wire protocol + conformance oracle** (this document, below)
2. **Thin vertical slice** — parser subset, minimal catalog, in-memory
   single-range engine; `CREATE TABLE`/`INSERT`/`SELECT` end-to-end via psql
3. **Durable storage + WAL + MVCC** on a single node
4. **Ranges + Raft** — multi-node replication, range splits, rebalancing
5. Distributed transactions — cross-range atomicity, serializable isolation
6. SQL breadth waves — planner cost model, joins, indexes, then
   datatypes/functions/PL/pgSQL in successive parity-dashboard-driven waves
7. Logical & physical replication, backup/restore
8. Object-storage tiering for cold data and backups

Build-order strategy: vertical slice first, with the conformance oracle built
alongside the wire protocol so parity is a measured number from the start.
After the slice, sub-projects alternate between depth (distribution) and
breadth (SQL surface).

## Sub-project 1: `pgwire` + `conformance`

### Goal

Real PostgreSQL clients — psql, tokio-postgres, sqlx — connect to crabgresql
over the v3 wire protocol with TLS and SCRAM-SHA-256, and run queries answered
by a pluggable stub engine. The conformance harness goes live in CI: the
parity scoreboard exists before the database does.

### `pgwire` design

Three layers:

**Codec.** Typed frontend/backend message definitions and a tokio framed
codec. Decoding is fully length-checked and allocation-bounded; malformed
input yields protocol errors, never panics.

**Connection state machine.**
- Startup: SSLRequest → rustls handshake (or plaintext), then StartupMessage.
  Protocol 3.0 only; protocol 3.2 (new in PG 18) is a tracked gap.
- Authentication: SCRAM-SHA-256 (RustCrypto), plus `trust` mode for tests.
- Simple query cycle (Query → RowDescription/DataRow/CommandComplete →
  ReadyForQuery), including multi-statement query strings.
- Extended query cycle: Parse/Bind/Describe/Execute/Sync, portals and
  prepared statements, including the rule that an error skips all messages
  until Sync.
- ErrorResponse/NoticeResponse with correct SQLSTATE codes and field layout.
- Cancellation: BackendKeyData secret issued at startup; CancelRequest
  handled on a separate connection.

**`Engine` trait.** The seam where the real database plugs in later: an async
trait taking protocol-level requests (simple query text, parse/bind/execute
operations) and returning row descriptions, data rows, command tags, and
errors. Sub-project 1 ships a `StubEngine` with canned responses: `SELECT 1`,
`version()`, and the small set of catalog probes psql issues on connect.

**Out of scope for SP1:** COPY subprotocol, streaming-replication protocol,
GSSAPI/SSPI auth, protocol 3.2.

### `conformance` design

Two parts:

**Differential runner.** Spins up real PostgreSQL 18 (container, test-only
dependency) and crabgresql side by side; executes a SQL corpus against both
via tokio-postgres; normalizes and diffs rows, column types, command tags,
and error SQLSTATEs.

**Regression importer.** Ports `pg_regress`'s `.sql`/`.out` files into the
corpus, tracked individually. CI emits `parity.json` plus a markdown summary:
suites passed / queries matched / known gaps. Against the stub engine parity
reads ~0% — the deliverable is the pipeline, and the number only goes up.

### Error handling

- Protocol violations → ErrorResponse with the SQLSTATE real PostgreSQL uses
  (verified via the differential runner), then connection state per PG
  semantics (recover at next Sync, or terminate, matching PG).
- I/O errors and client disconnects tear down the session cleanly; no panics
  reachable from network input (enforced by property tests on the codec).

### Testing

- **Unit/property:** codec round-trip property tests (encode → decode = id);
  decoder robustness against arbitrary bytes via proptest.
- **Golden traces:** byte traces recorded from real PG 18 sessions, replayed
  against our codec to pin exact framing.
- **Integration:** scripted psql, tokio-postgres, and sqlx clients connect
  (TLS + SCRAM and trust), run queries against the stub, assert results.
- **CI gates:** `forbid(unsafe_code)` in every crate; `cargo-deny` rejecting
  C/`-sys` crates in the shipped tree; conformance harness publishing the
  parity report on every run.

### Success criteria

1. psql 18 connects over TLS with SCRAM-SHA-256 and gets a `SELECT 1` answer
   from the stub engine.
2. tokio-postgres and sqlx integration tests pass against the stub.
3. The conformance harness runs in CI and publishes `parity.json` + summary.
4. All safety gates green: zero `unsafe` in workspace, pure-Rust shipped
   dependency tree.
