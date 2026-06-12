# SP4: Transactions + Serialized-Writer MVCC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real transactions and MVCC — `BEGIN`/`COMMIT`/`ROLLBACK`, commit-timestamp MVCC with snapshot-isolated reads (READ COMMITTED default + REPEATABLE READ), and `UPDATE`/`DELETE`. Writers stay serialized behind the SP3 `write_lock` (no write-write conflicts possible), so no lock manager is needed; concurrent writers + block-and-retry are SP5.

**Architecture:** A new `mvcc` crate holds commit-timestamp versioned keys, tombstone-aware version values, and `Snapshot`/visibility. The durable store holds **only committed versions** (transactions buffer writes in an in-memory write-set and flush one atomic `write_batch` at COMMIT), so visibility is a single `ts ≤ snapshot` compare — no clog until SP5. The `pgwire::Engine` trait becomes connection-oriented (`connect() -> Session`); the per-connection `Session` (in the executor) owns the transaction state machine, the write-set, and read-your-writes. The wire `ReadyForQuery` status (`I`/`T`/`E`) is threaded from the session.

**Tech Stack:** Rust 2024. New crate `mvcc` (deps kv, pgtypes). Existing: fjall, bytes, thiserror, tokio, tokio-postgres.

**Spec:** `docs/superpowers/specs/2026-06-11-crabgresql-sp4-mvcc-transactions-design.md`

---

## File structure

```
Cargo.toml                       # + mvcc workspace member/dep
crates/mvcc/
  Cargo.toml
  src/lib.rs                     # re-exports
  src/version.rs                 # versioned key (commit_ts descending) + version-value (tombstone) encode/decode
  src/snapshot.rs                # Snapshot (a commit_ts) + visible-version resolution over a rowid's versions
crates/pgparser/
  src/ast.rs                     # + Begin/Commit/Rollback/Update/Delete variants, IsolationLevel
  src/token.rs, src/lexer.rs     # + keywords (begin/commit/rollback/update/delete/set/from/isolation/level/read/committed/repeatable/...)
  src/parser.rs                  # + transaction-control + UPDATE + DELETE grammar
  tests/libpg_query_oracle.rs    # + accept cases
crates/pgwire/
  src/engine.rs                  # Engine::connect() -> Session trait; Session{simple_query,describe,tx_status}
  src/session.rs                 # run_session creates a Session per conn; ReadyForQuery from tx_status()
  src/stub.rs                    # StubEngine::connect() -> StubSession (always Idle)
crates/executor/
  src/lib.rs                     # SqlEngine::connect() -> SqlSession; shared kv + write_lock
  src/session.rs                 # SqlSession: txn state machine, write-set, snapshot timing, commit/rollback
  src/exec.rs                    # versioned read/write path (INSERT/SELECT/UPDATE/DELETE) over a TxnCtx
  src/error.rs                   # + 25P02 mapping
  tests/transactions.rs          # BEGIN/COMMIT/ROLLBACK, read-your-writes, snapshot isolation, 25P02
  tests/update_delete.rs         # UPDATE/DELETE semantics
  (durability.rs, end_to_end.rs, concurrency.rs updated for connect()->session)
crates/conformance/corpus/       # + update_delete.sql, transactions handled per-oracle
.github/workflows/ci.yml         # unchanged gates cover it
```

Task order (each ends workspace-green): mvcc crate → parser (UPDATE/DELETE/txn-control) + executor stub arms → Engine→Session plumbing (behavior identical) → MVCC storage on the autocommit path → explicit transactions (BEGIN/COMMIT/ROLLBACK + RC/RR + failed-block) → UPDATE/DELETE → durability + snapshot + e2e tests → conformance + gauntlet.

---

### Task 1: `mvcc` crate — versioned keys, version values, Snapshot, visibility

**Files:**
- Create: `crates/mvcc/Cargo.toml`, `crates/mvcc/src/lib.rs`, `crates/mvcc/src/version.rs`, `crates/mvcc/src/snapshot.rs`
- Modify: root `Cargo.toml` (member + dep)

- [ ] **Step 1: Scaffold.** Add `"crates/mvcc"` to the workspace `members` and
`mvcc = { path = "crates/mvcc" }` to `[workspace.dependencies]`. Create
`crates/mvcc/Cargo.toml`:

```toml
[package]
name = "mvcc"
version.workspace = true
edition.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
thiserror.workspace = true
kv.workspace = true
pgtypes.workspace = true

[dev-dependencies]
proptest.workspace = true
```

- [ ] **Step 2: Failing tests** — `crates/mvcc/src/version.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;
    use proptest::prelude::*;

    #[test]
    fn version_key_prefix_is_the_rowid_row_key() {
        // A rowid's versions all live under kv::key::row_key(table, rowid).
        let prefix = kv::key::row_key(7, 42);
        let k = version_key(7, 42, 100);
        assert!(k.starts_with(&prefix));
        assert!(k.len() > prefix.len());
    }

    #[test]
    fn newer_commit_ts_sorts_first_descending() {
        // Descending encoding: higher commit_ts → smaller key bytes → scanned first.
        let older = version_key(7, 42, 100);
        let newer = version_key(7, 42, 200);
        assert!(newer < older, "newer version must sort before older for newest-first scan");
    }

    #[test]
    fn commit_ts_roundtrips_from_key() {
        let k = version_key(7, 42, 12345);
        assert_eq!(commit_ts_of(7, 42, &k).unwrap(), 12345);
    }

    #[test]
    fn version_value_roundtrip_row_and_tombstone() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let bytes = encode_version(false, &row);
        assert_eq!(decode_version(&bytes).unwrap(), (false, row));
        let tomb = encode_version(true, &[]);
        let (deleted, cols) = decode_version(&tomb).unwrap();
        assert!(deleted);
        assert!(cols.is_empty());
    }

    #[test]
    fn decode_version_rejects_corrupt() {
        assert!(decode_version(&[]).is_err());
        assert!(decode_version(&[99]).is_err()); // bad version byte
    }

    proptest! {
        #[test]
        fn descending_order_matches_reverse_ts(a: u64, b: u64) {
            let ka = version_key(1, 1, a);
            let kb = version_key(1, 1, b);
            // key order is the REVERSE of ts order
            prop_assert_eq!(a.cmp(&b), kb.cmp(&ka));
        }
    }
}
```

- [ ] **Step 3:** `cargo test -p mvcc -- version::` → COMPILE FAIL.

- [ ] **Step 4: Implement** `crates/mvcc/src/version.rs`:

```rust
//! Versioned-key and version-value encoding for commit-timestamp MVCC.
//!
//! A rowid's versions live under `kv::key::row_key(table, rowid)` with a
//! descending-commit_ts suffix, so a forward scan hits the newest version
//! first. The value is the row (via the row format) plus a tombstone flag.

use pgtypes::Datum;

use kv::KvError;

/// Build the key for one version of a row. The commit_ts is encoded
/// DESCENDING (`u64::MAX - ts`, big-endian) so higher timestamps sort first.
pub fn version_key(table_id: u32, rowid: u64, commit_ts: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    k
}

/// Recover the commit_ts from a version key known to belong to (table, rowid).
pub fn commit_ts_of(table_id: u32, rowid: u64, key: &[u8]) -> Result<u64, KvError> {
    let prefix = kv::key::row_key(table_id, rowid);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 8 {
        return Err(KvError::CorruptRow("version key shape mismatch".into()));
    }
    let suffix: [u8; 8] = key[prefix.len()..].try_into().expect("8 bytes");
    Ok(u64::MAX - u64::from_be_bytes(suffix))
}

const V_ROW: u8 = 1;
const V_TOMBSTONE: u8 = 2;

/// Encode a version value: a live row, or a tombstone (DELETE).
pub fn encode_version(deleted: bool, row: &[Datum]) -> Vec<u8> {
    if deleted {
        return vec![V_TOMBSTONE];
    }
    let mut out = vec![V_ROW];
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a version value into (deleted, columns).
pub fn decode_version(bytes: &[u8]) -> Result<(bool, Vec<Datum>), KvError> {
    match bytes.first() {
        Some(&V_TOMBSTONE) => Ok((true, Vec::new())),
        Some(&V_ROW) => {
            let cols = kv::rowenc::decode_row(&bytes[1..])?;
            Ok((false, cols))
        }
        _ => Err(KvError::CorruptRow("bad version value tag".into())),
    }
}
```

- [ ] **Step 5: Failing tests** — `crates/mvcc/src/snapshot.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;

    fn ver(ts: u64, deleted: bool, row: Vec<Datum>) -> (u64, Vec<u8>) {
        (ts, crate::version::encode_version(deleted, &row))
    }

    #[test]
    fn picks_newest_version_at_or_below_snapshot() {
        // Versions for one rowid, in descending-ts order (as scanned).
        let versions = vec![
            ver(300, false, vec![Datum::Int4(3)]),
            ver(200, false, vec![Datum::Int4(2)]),
            ver(100, false, vec![Datum::Int4(1)]),
        ];
        // Snapshot 250 sees version 200 (the newest with ts <= 250).
        let v = visible_version(Snapshot(250), versions.iter().map(|(t, b)| (*t, b.as_slice())));
        assert_eq!(v.unwrap(), Some(vec![Datum::Int4(2)]));
    }

    #[test]
    fn tombstone_hides_the_row() {
        let versions = vec![
            ver(300, true, vec![]),
            ver(100, false, vec![Datum::Int4(1)]),
        ];
        let v = visible_version(Snapshot(400), versions.iter().map(|(t, b)| (*t, b.as_slice())));
        assert_eq!(v.unwrap(), None); // newest visible is a tombstone
    }

    #[test]
    fn nothing_visible_below_oldest() {
        let versions = vec![ver(100, false, vec![Datum::Int4(1)])];
        let v = visible_version(Snapshot(50), versions.iter().map(|(t, b)| (*t, b.as_slice())));
        assert_eq!(v.unwrap(), None);
    }
}
```

- [ ] **Step 6: Implement** `crates/mvcc/src/snapshot.rs`:

```rust
//! Snapshots and version visibility. A snapshot is a commit timestamp; a
//! version is visible iff its commit_ts is <= the snapshot.

use pgtypes::Datum;

use kv::KvError;

use crate::version::decode_version;

/// A read snapshot: the commit timestamp as of which the reader sees the world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot(pub u64);

/// Given a rowid's versions in DESCENDING commit_ts order (as a forward scan
/// yields them), return the visible row, or None if the newest visible version
/// is a tombstone or no version is visible.
pub fn visible_version<'a>(
    snapshot: Snapshot,
    versions: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<Option<Vec<Datum>>, KvError> {
    for (ts, bytes) in versions {
        if ts <= snapshot.0 {
            let (deleted, row) = decode_version(bytes)?;
            return Ok(if deleted { None } else { Some(row) });
        }
    }
    Ok(None)
}
```

- [ ] **Step 7: lib.rs:**

```rust
//! mvcc: commit-timestamp multiversion concurrency control primitives for
//! crabgresql — versioned keys, tombstone version values, snapshots, and
//! visibility. The durable store holds only committed versions (SP4); the
//! commit-status log (clog) arrives with concurrent writers in SP5.

pub mod snapshot;
pub mod version;

pub use snapshot::{Snapshot, visible_version};
pub use version::{commit_ts_of, decode_version, encode_version, version_key};
```

- [ ] **Step 8:** `cargo test -p mvcc` → all pass. `cargo fmt --all && cargo clippy -p mvcc --all-targets -- -D warnings`. Commit:

```bash
git add crates/mvcc Cargo.toml Cargo.lock
git commit -m "feat(mvcc): versioned keys, tombstone version values, snapshot visibility"
```

---

### Task 2: Parser — transaction control + UPDATE + DELETE (executor stubs)

**Files:**
- Modify: `crates/pgparser/src/ast.rs`, `crates/pgparser/src/token.rs`, `crates/pgparser/src/lexer.rs`, `crates/pgparser/src/parser.rs`, `crates/pgparser/tests/libpg_query_oracle.rs`
- Modify: `crates/executor/src/exec.rs` (temporary stub arms to keep the exhaustive match compiling)

Additive grammar. New statements parse; the executor stub-rejects them with
`0A000` until Tasks 5–6 implement them (keeps the workspace green).

- [ ] **Step 1: AST additions.** In `crates/pgparser/src/ast.rs` add to the
`Statement` enum and a new `IsolationLevel`:

```rust
    Begin {
        isolation: Option<IsolationLevel>,
    },
    Commit,
    Rollback,
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
```

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}
```

- [ ] **Step 2: Keywords.** In `crates/pgparser/src/token.rs` add to the
`Keyword` enum and its `from_word` match: `Begin, Start, Transaction, Commit,
End, Rollback, Abort, Update, Set, Delete, Isolation, Level, Read, Committed,
Repeatable` (lowercase words `begin/start/transaction/commit/end/rollback/abort/
update/set/delete/isolation/level/read/committed/repeatable`). `From` and `Where`
already exist.

- [ ] **Step 3: Failing parser tests.** Add to the `parser.rs` test module:

```rust
    #[test]
    fn parses_begin_variants() {
        assert_eq!(one("BEGIN"), Statement::Begin { isolation: None });
        assert_eq!(one("START TRANSACTION"), Statement::Begin { isolation: None });
        assert_eq!(
            one("BEGIN ISOLATION LEVEL REPEATABLE READ"),
            Statement::Begin { isolation: Some(IsolationLevel::RepeatableRead) }
        );
        assert_eq!(
            one("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Statement::Begin { isolation: Some(IsolationLevel::ReadCommitted) }
        );
    }

    #[test]
    fn parses_commit_rollback_aliases() {
        assert_eq!(one("COMMIT"), Statement::Commit);
        assert_eq!(one("END"), Statement::Commit);
        assert_eq!(one("ROLLBACK"), Statement::Rollback);
        assert_eq!(one("ABORT"), Statement::Rollback);
    }

    #[test]
    fn parses_update() {
        match one("UPDATE t SET a = 1, b = a + 2 WHERE id = 5") {
            Statement::Update { table, assignments, filter } => {
                assert_eq!(table, "t");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, "a");
                assert_eq!(assignments[1].0, "b");
                assert!(filter.is_some());
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_delete() {
        match one("DELETE FROM t WHERE id > 3") {
            Statement::Delete { table, filter } => {
                assert_eq!(table, "t");
                assert!(filter.is_some());
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        assert_eq!(one("DELETE FROM t"), Statement::Delete { table: "t".into(), filter: None });
    }
```

(`one()` and `IsolationLevel` need importing in the test module — add
`use crate::ast::IsolationLevel;`.)

- [ ] **Step 4:** `cargo test -p pgparser -- parser::` → FAIL.

- [ ] **Step 5: Implement the grammar.** In `parser.rs`, extend `statement()`
dispatch and add the methods:

```rust
            Token::Keyword(Keyword::Begin) | Token::Keyword(Keyword::Start) => self.begin(),
            Token::Keyword(Keyword::Commit) | Token::Keyword(Keyword::End) => {
                self.bump();
                Ok(crate::ast::Statement::Commit)
            }
            Token::Keyword(Keyword::Rollback) | Token::Keyword(Keyword::Abort) => {
                self.bump();
                Ok(crate::ast::Statement::Rollback)
            }
            Token::Keyword(Keyword::Update) => self.update(),
            Token::Keyword(Keyword::Delete) => self.delete(),
```

```rust
    fn begin(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{IsolationLevel, Statement};
        self.bump(); // BEGIN or START
        // optional TRANSACTION
        self.eat_keyword(Keyword::Transaction);
        // optional ISOLATION LEVEL ...
        let isolation = if self.eat_keyword(Keyword::Isolation) {
            self.expect(&Token::Keyword(Keyword::Level))?;
            if self.eat_keyword(Keyword::Repeatable) {
                self.expect(&Token::Keyword(Keyword::Read))?;
                Some(IsolationLevel::RepeatableRead)
            } else if self.eat_keyword(Keyword::Read) {
                self.expect(&Token::Keyword(Keyword::Committed))?;
                Some(IsolationLevel::ReadCommitted)
            } else {
                return Err(ParseError::new("expected REPEATABLE READ or READ COMMITTED", self.peek_pos()));
            }
        } else {
            None
        };
        Ok(Statement::Begin { isolation })
    }

    fn update(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Update))?;
        let table = self.expect_ident()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.expr(0)?;
            assignments.push((col, value));
            if self.eat_comma() {
                continue;
            }
            break;
        }
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Statement::Update { table, assignments, filter })
    }

    fn delete(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Delete))?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let table = self.expect_ident()?;
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Statement::Delete { table, filter })
    }
```

- [ ] **Step 6: Executor stub arms.** In `crates/executor/src/exec.rs`, add arms
to the `execute()` match so it stays exhaustive (real impls in Tasks 5–6):

```rust
        Statement::Begin { .. } | Statement::Commit | Statement::Rollback => {
            Err(ExecError::Unsupported("transaction control lands in Task 5".into()))
        }
        Statement::Update { .. } => Err(ExecError::Unsupported("UPDATE lands in Task 6".into())),
        Statement::Delete { .. } => Err(ExecError::Unsupported("DELETE lands in Task 6".into())),
```

- [ ] **Step 7:** `cargo test -p pgparser` → all pass. `cargo test --workspace`
→ green (new statements parse; executor returns 0A000 for them; existing tests
unaffected).

- [ ] **Step 8: Oracle corpus.** In `crates/pgparser/tests/libpg_query_oracle.rs`,
add to the `ACCEPTED` array:

```rust
    "BEGIN",
    "START TRANSACTION",
    "BEGIN ISOLATION LEVEL REPEATABLE READ",
    "COMMIT",
    "ROLLBACK",
    "UPDATE t SET a = 1 WHERE id = 5",
    "UPDATE t SET a = 1, b = 2",
    "DELETE FROM t WHERE id > 3",
    "DELETE FROM t",
```

Run `cargo test -p pgparser --features oracle` → both agreement tests pass
(libpg_query accepts all these; ours must too). If one diverges, fix the parser
(report what changed).

- [ ] **Step 9:** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`, then commit:

```bash
git add crates/pgparser crates/executor
git commit -m "feat(pgparser): BEGIN/COMMIT/ROLLBACK and UPDATE/DELETE grammar (executor stubs)"
```

---

### Task 3: Engine → per-connection Session refactor (behavior identical)

**Files:**
- Modify: `crates/pgwire/src/engine.rs`, `crates/pgwire/src/session.rs`, `crates/pgwire/src/stub.rs`
- Modify: `crates/executor/src/lib.rs`, `crates/executor/src/exec.rs`
- Create: `crates/executor/src/session.rs`
- Modify: executor tests that call `engine.simple_query` directly (`tests/durability.rs`, `tests/concurrency.rs`, and the `exec.rs` unit tests)

Pure plumbing: the engine becomes a factory producing per-connection sessions.
No transactions or MVCC yet — `tx_status()` is always `Idle` and the autocommit
behavior is byte-for-byte identical. This isolates the large mechanical change
from the semantic ones (Tasks 4–6). pgwire's wire integration tests (which go
through `serve`) are unaffected; only direct-call executor tests and the stub
change.

- [ ] **Step 1: New trait shape.** In `crates/pgwire/src/engine.rs`, replace the
`Engine` trait with a factory + a `Session` trait, and re-export `TxStatus`:

```rust
use std::future::Future;

pub use crate::messages::backend::TxStatus;

/// A database engine: a factory for per-connection sessions. Shared across all
/// connections (Send + Sync); each connection gets its own Session.
pub trait Engine: Send + Sync + 'static {
    type Session: Session;
    fn connect(&self) -> Self::Session;
}

/// A per-connection session. Owns transaction state; not shared between
/// connections. `simple_query`/`describe` take `&mut self` because they mutate
/// transaction state.
pub trait Session: Send {
    fn simple_query(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<QueryResult>, PgError>> + Send;

    fn describe(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<FieldDescription>, PgError>> + Send;

    /// The transaction status reported to the client in ReadyForQuery.
    fn tx_status(&self) -> TxStatus;
}
```

(Keep `QueryResult`, `Cell`, `FieldDescription`, `oids` exactly as they are.)

- [ ] **Step 2: Update `session.rs`.** In `run_session<S, E>`, create one session
and thread it through. Change the helper signatures from `engine: &E` to
`session: &mut E::Session` (or make them generic `<Sess: Session>`):
- At the top of `run_session`, after auth/startup: `let mut session = engine.connect();`
- `handle_parse`/`handle_describe`(if it calls describe)/`handle_execute`: take
  `session: &mut Sess` (generic `Sess: Session`) and call `session.describe(...)`
  / `session.simple_query(...)`.
- The simple-query `Query` arm: `r = session.simple_query(&sql) => r` (keep the
  `tokio::select!` + cancel token; `&mut session` is borrowed for the select).
- Replace ALL THREE `backend::ready_for_query(&mut out, TxStatus::Idle)` calls
  with `backend::ready_for_query(&mut out, session.tx_status())`.
- `engine` is still `Arc<E>` (the factory); only the per-statement calls move to
  `session`. The `import use crate::messages::backend::{self, TxStatus}` stays.

- [ ] **Step 3: Update `stub.rs`.** `StubEngine` implements the factory; add a
`StubSession` carrying the old canned behavior:

```rust
impl crate::engine::Engine for StubEngine {
    type Session = StubSession;
    fn connect(&self) -> StubSession {
        StubSession
    }
}

pub struct StubSession;

impl crate::engine::Session for StubSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // ... move the existing StubEngine::simple_query body here ...
    }
    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        // ... move the existing StubEngine::describe body here ...
    }
    fn tx_status(&self) -> crate::engine::TxStatus {
        crate::engine::TxStatus::Idle
    }
}
```

(Move the canned `SELECT 1`/`version()`/`pg_sleep` logic from the old
`StubEngine` impl into `StubSession`. `StubEngine` keeps `new()`/`Default`.)

- [ ] **Step 4: Build pgwire.** `cargo build -p pgwire` → compiles. (Executor
won't yet — next steps.)

- [ ] **Step 5: Executor — split engine/session.** In `crates/executor/src/lib.rs`,
make `SqlEngine` a factory holding shared state, and add a `SqlSession`:

```rust
mod error;
mod eval;
mod exec;
mod session;

use std::path::Path;
use std::sync::{Arc, Mutex};

use kv::{FjallKv, Kv, MemKv};

pub use error::ExecError;
pub use session::SqlSession;

/// Shared engine state: the KV store and the global write lock (serializes all
/// writers across every connection). A factory for per-connection sessions.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) write_lock: Arc<Mutex<()>>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new()))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Ok(Self::with_kv(Arc::new(FjallKv::open(path)?)))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Self {
        Self { kv, write_lock: Arc::new(Mutex::new(())) }
    }
}

impl pgwire::engine::Engine for SqlEngine {
    type Session = SqlSession;
    fn connect(&self) -> SqlSession {
        SqlSession::new(Arc::clone(&self.kv), Arc::clone(&self.write_lock))
    }
}
```

- [ ] **Step 6: `crates/executor/src/session.rs`** — the SqlSession (Task 3:
autocommit only, tx_status always Idle; transaction state added in Task 5):

```rust
//! Per-connection session: runs SQL against the shared KV store. SP4 Task 3
//! ships autocommit only; the transaction state machine arrives in Task 5.

use std::sync::{Arc, Mutex};

use kv::Kv;
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;

use crate::error::ExecError;

pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) write_lock: Arc<Mutex<()>>,
}

impl SqlSession {
    pub fn new(kv: Arc<dyn Kv>, write_lock: Arc<Mutex<()>>) -> Self {
        Self { kv, write_lock }
    }

    /// Read a table's durable next-rowid (1 if unset). (Moved from SqlEngine.)
    pub(crate) fn read_seq(&self, table: catalog::TableId) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::seq_key(table))? {
            Some(b) => {
                let arr: [u8; 8] = b.as_slice().try_into()
                    .map_err(|_| kv::KvError::CorruptRow("sequence is not u64".into()))?;
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(1),
        }
    }
}

impl Session for SqlSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(crate::exec::execute(self, &stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        crate::exec::describe(self, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}
```

- [ ] **Step 7: `exec.rs` — rename `engine` → `session`.** `execute(engine: &SqlEngine, …)`
becomes `execute(session: &SqlSession, …)`; every `engine.kv` → `session.kv`,
`engine.write_lock` → `session.write_lock`, `engine.read_seq` → `session.read_seq`,
`catalog::…(&*engine.kv, …)` → `catalog::…(&*session.kv, …)`. Same in `describe`,
`exec_select`. Pure rename; no behavior change. The `Begin/Commit/Rollback/
Update/Delete` stub arms from Task 2 stay.

- [ ] **Step 8: Update direct-call executor tests.** In `exec.rs` test module,
`tests/durability.rs`, and `tests/concurrency.rs`, replace direct
`engine.simple_query(...)` with a session: `let mut s = engine.connect();
s.simple_query(...).await`. Add `use pgwire::engine::{Engine, Session};` where
needed. For helpers that took `&SqlEngine`, switch to taking `&mut SqlSession`
or constructing a session inside. (`end_to_end.rs` goes over the wire via
tokio-postgres — unchanged.) Each autocommit statement still gets its own
implicit commit, so a fresh `connect()` per logical operation preserves the old
semantics; reuse one session across a test's statements where convenient.

- [ ] **Step 9:** `cargo test --workspace` → green (behavior identical; the
158→ existing tests pass through the session). `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`. `./scripts/psql-smoke.sh` (3 legs) + `./scripts/durable-restart-smoke.sh` still PASS (wire behavior unchanged).

- [ ] **Step 10: Commit.**

```bash
git add crates/pgwire crates/executor
git commit -m "refactor: Engine becomes a factory for per-connection Sessions (behavior identical)"
```

---

### Task 4: Versioned MVCC storage on the autocommit path

**Files:**
- Modify: `crates/kv/src/key.rs` (+ `commit_ts_key`), `crates/mvcc/src/version.rs` (+ key-split helpers)
- Modify: `crates/executor/src/session.rs` (commit_ts read/bump), `crates/executor/src/exec.rs` (versioned INSERT + SELECT)
- Modify: `crates/executor/Cargo.toml` (+ `mvcc`)

INSERT now writes a *versioned* row tagged with a fresh `commit_ts`; SELECT reads
the newest version visible at the current `commit_ts`. Still autocommit
(write-immediately); the write-set + explicit transactions arrive in Task 5.
Existing INSERT/SELECT tests must still pass (same observable results).

- [ ] **Step 1: `commit_ts` key + version-key helpers.** In `crates/kv/src/key.rs`
add:

```rust
/// Key for the global commit-timestamp clock: `/0/meta/commit_ts`.
pub fn commit_ts_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"commit_ts");
    k
}
```

In `crates/mvcc/src/version.rs` add (with tests in the same module):

```rust
/// The row-key prefix of a version key (everything but the 8-byte ts suffix).
pub fn row_prefix_of(key: &[u8]) -> Result<&[u8], KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    Ok(&key[..key.len() - 8])
}

/// The commit_ts encoded in a version key's 8-byte suffix.
pub fn ts_of_key(key: &[u8]) -> Result<u64, KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    let suffix: [u8; 8] = key[key.len() - 8..].try_into().expect("8 bytes");
    Ok(u64::MAX - u64::from_be_bytes(suffix))
}
```

Add tests: `row_prefix_of(version_key(7,42,5))` == `kv::key::row_key(7,42)`;
`ts_of_key(version_key(7,42,5))` == 5; both error on a too-short key.

- [ ] **Step 2: `executor/Cargo.toml`** — add `mvcc.workspace = true` to
`[dependencies]`.

- [ ] **Step 3: commit_ts read/bump in `session.rs`:**

```rust
    /// Read the global commit timestamp (0 if unset).
    pub(crate) fn read_commit_ts(&self) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::commit_ts_key())? {
            Some(b) => {
                let arr: [u8; 8] = b.as_slice().try_into()
                    .map_err(|_| kv::KvError::CorruptRow("commit_ts is not u64".into()))?;
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }
```

- [ ] **Step 4: Failing test** — confirm versioned round-trip via the existing
behavior. In `exec.rs` tests (or a quick new one), the existing
`select_where_order_limit` etc. already assert INSERT→SELECT results; they will
exercise the versioned path once implemented. Add one explicit MVCC test:

```rust
    #[tokio::test]
    async fn insert_writes_a_versioned_row_visible_to_select() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
        s.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
        let r = &run(&mut s, "SELECT id FROM t").await[0];
        assert_eq!(rows_of(r).len(), 1);
    }
```

(`run` here takes `&mut SqlSession`; adapt the existing `run` helper accordingly.)

- [ ] **Step 5: Implement versioned INSERT** in `exec.rs`. Replace the INSERT
arm's write so each row is a version at a fresh commit_ts, committed atomically
with the seq and the bumped commit_ts (autocommit: one statement = one commit):

```rust
        Statement::Insert { table, columns, rows } => {
            let t = catalog::get_table(&*session.kv, table)?;
            let target_idx: Vec<usize> = match columns {
                Some(cols) => cols.iter()
                    .map(|c| t.column_index(c).ok_or_else(|| ExecError::UndefinedColumn(c.clone())))
                    .collect::<Result<_, _>>()?,
                None => (0..t.columns.len()).collect(),
            };
            let _guard = session.write_lock.lock().expect("write lock");
            let new_ts = session.read_commit_ts()? + 1;
            let mut rowid = session.read_seq(t.id)?;
            let mut ops: Vec<kv::WriteOp> = Vec::new();
            for row_exprs in rows {
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
                for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
                    full[*slot] = coerce(crate::eval::eval(expr, None, &[])?, t.columns[*slot].ty)?;
                }
                ops.push(kv::WriteOp::Put {
                    key: mvcc::version_key(t.id, rowid, new_ts),
                    value: mvcc::encode_version(false, &full),
                });
                rowid += 1;
            }
            let n = (rowid - session.read_seq(t.id)?) ; // rows inserted
            ops.push(kv::WriteOp::Put { key: kv::key::seq_key(t.id), value: rowid.to_be_bytes().to_vec() });
            ops.push(kv::WriteOp::Put { key: kv::key::commit_ts_key(), value: new_ts.to_be_bytes().to_vec() });
            session.kv.write_batch(&ops)?;
            Ok(QueryResult::Command { tag: format!("INSERT 0 {n}") })
        }
```

(Compute `n` as the row count before mutating — capture `let start = session.read_seq(t.id)?;` once at the top and use `rowid - start`. Adjust to avoid the double read shown above; the intent: `n = number of VALUES rows`.)

- [ ] **Step 6: Implement versioned SELECT read path.** In `exec_select`, replace
the scan+decode with a version-aware scan: snapshot = current commit_ts; group the
flat scan by rowid prefix (newest-first within a group), pick the visible version:

```rust
fn scan_live_rows(session: &SqlSession, table: &catalog::Table) -> Result<Vec<Vec<pgtypes::Datum>>, ExecError> {
    let snapshot = mvcc::Snapshot(session.read_commit_ts()?);
    let scanned = session.kv.scan_prefix(&kv::key::table_prefix(table.id))?;
    let mut out = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        let prefix = mvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        // collect this rowid's versions (already in descending-ts order)
        let mut versions: Vec<(u64, &[u8])> = Vec::new();
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice() {
            let ts = mvcc::version::ts_of_key(&scanned[i].0)?;
            versions.push((ts, scanned[i].1.as_slice()));
            i += 1;
        }
        if let Some(row) = mvcc::visible_version(snapshot, versions.into_iter())? {
            out.push(row);
        }
    }
    Ok(out)
}
```

Use `scan_live_rows` where `exec_select` previously did
`scan_prefix(...).into_iter().map(decode_row)`. The rest of `exec_select` (WHERE
filter, projection, ORDER BY, LIMIT) operates on these decoded rows unchanged.
`Some(name) => Some(catalog::get_table(...))` stays.

- [ ] **Step 7:** `cargo test --workspace` → green (existing INSERT/SELECT tests
pass against versioned storage; the new MVCC test passes). `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 8: Commit.**

```bash
git add crates/kv crates/mvcc crates/executor Cargo.toml Cargo.lock
git commit -m "feat(executor): versioned MVCC storage for INSERT/SELECT (autocommit)"
```

---

### Task 5: Session transaction state machine — BEGIN/COMMIT/ROLLBACK, write-set, RC/RR

**Files:**
- Modify: `crates/executor/src/session.rs` (the state machine + write-set + snapshot timing), `crates/executor/src/exec.rs` (route DML through a `TxnCtx`), `crates/executor/src/error.rs` (+ 25P02)
- Create: `crates/executor/tests/transactions.rs`

Lift the per-statement write path into a transaction context that can span
multiple statements. The session becomes `Idle | InTransaction | Failed`;
writes buffer in an in-memory write-set and flush at COMMIT; reads overlay the
write-set on the visible versions; RC re-snapshots per statement, RR holds one
snapshot.

- [ ] **Step 1: Failing tests** — `crates/executor/tests/transactions.rs`:

```rust
use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session, TxStatus};

fn text(c: &Option<Cell>) -> Option<String> {
    c.as_ref().map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}
async fn rows(s: &mut executor::SqlSession, sql: &str) -> Vec<Vec<Option<Cell>>> {
    match s.simple_query(sql).await.expect("q").remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn rollback_discards_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    assert_eq!(s.tx_status(), TxStatus::InTransaction);
    s.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    // read-your-writes: the uncommitted row is visible inside the txn
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 1);
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 0, "rollback discarded the insert");
}

#[tokio::test]
async fn commit_persists_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1),(2)").await.expect("insert");
    s.simple_query("COMMIT").await.expect("commit");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn repeatable_read_does_not_see_concurrent_commit() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    setup.simple_query("INSERT INTO t VALUES (1)").await.expect("seed");

    let mut reader = engine.connect();
    reader.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin rr");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1); // snapshot taken here

    // Another connection commits a new row.
    let mut writer = engine.connect();
    writer.simple_query("INSERT INTO t VALUES (2)").await.expect("concurrent insert");

    // RR reader still sees only 1 (its snapshot predates the commit).
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    reader.simple_query("COMMIT").await.expect("commit");
    // After commit, a fresh read sees both.
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn read_committed_sees_concurrent_commit_next_statement() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    setup.simple_query("INSERT INTO t VALUES (1)").await.expect("seed");

    let mut reader = engine.connect();
    reader.simple_query("BEGIN").await.expect("begin rc"); // default READ COMMITTED
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);

    let mut writer = engine.connect();
    writer.simple_query("INSERT INTO t VALUES (2)").await.expect("concurrent insert");

    // RC re-snapshots per statement → sees the new row now.
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn error_in_block_fails_transaction_until_rollback() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    let err = s.simple_query("SELECT * FROM nope").await.expect_err("undefined table");
    assert_eq!(err.code, "42P01");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    // Subsequent non-ROLLBACK statement → 25P02.
    let err = s.simple_query("SELECT 1").await.expect_err("aborted block");
    assert_eq!(err.code, "25P02");
    // ROLLBACK clears it.
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    s.simple_query("SELECT 1").await.expect("works again");
}
```

- [ ] **Step 2:** `cargo test -p executor --test transactions` → FAIL (BEGIN etc.
return 0A000 from the Task-2 stub).

- [ ] **Step 3: Add `25P02` mapping** in `crates/executor/src/error.rs`:
`ExecError` gains `InFailedTransaction` → `PgError::error("25P02", "current
transaction is aborted, commands ignored until end of transaction block")`.

- [ ] **Step 4: Implement the state machine** in `crates/executor/src/session.rs`.
Add the txn types and rework `simple_query`:

```rust
use std::collections::HashMap;
use catalog::TableId;
use pgparser::ast::{IsolationLevel, Statement};

#[derive(Clone)]
enum Pending {
    Row(Vec<u8>),   // encoded version value (live row)
    Tombstone,
}

#[derive(Default)]
pub(crate) struct TxnCtx {
    pub(crate) snapshot: u64,                     // commit_ts the reads see
    pub(crate) repeatable_read: bool,             // RR fixes the snapshot; RC re-takes it
    pub(crate) writes: HashMap<(TableId, u64), Pending>, // (table, rowid) -> pending version
    pub(crate) seq: HashMap<TableId, u64>,        // pending next-rowid per table (read-your-writes)
}

enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed,
}
```

`SqlSession` gains `state: TxnState` (init `Idle`). `simple_query` becomes a
per-statement driver:

```rust
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(self.run_one(&stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }
```

`run_one(&mut self, stmt)` is the heart (sync; it may take the write_lock but
never awaits):

```rust
    fn run_one(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        // Failed block: only COMMIT/ROLLBACK allowed.
        if matches!(self.state, TxnState::Failed)
            && !matches!(stmt, Statement::Commit | Statement::Rollback)
        {
            return Err(ExecError::InFailedTransaction);
        }
        match stmt {
            Statement::Begin { isolation } => self.begin(*isolation),
            Statement::Commit => self.commit_cmd(),
            Statement::Rollback => self.rollback_cmd(),
            // DDL: non-transactional, executes immediately.
            Statement::CreateTable { .. } | Statement::DropTable { .. } => {
                crate::exec::execute_ddl(self, stmt)
            }
            // DML: run inside the current (or an implicit) transaction.
            _ => self.run_dml(stmt),
        }
    }
```

`begin`/`commit_cmd`/`rollback_cmd`:

```rust
    fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        // BEGIN inside a block: no-op (PG warns; we succeed).
        if matches!(self.state, TxnState::InTransaction(_)) {
            return Ok(QueryResult::Command { tag: "BEGIN".into() });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        let mut ctx = TxnCtx { repeatable_read: rr, ..Default::default() };
        // RR fixes the snapshot at the first statement; we set it lazily (0 means "unset").
        // For simplicity, capture now for RR; RC leaves it to be refreshed per statement.
        if rr {
            ctx.snapshot = self.read_commit_ts()?;
        }
        self.state = TxnState::InTransaction(ctx);
        Ok(QueryResult::Command { tag: "BEGIN".into() })
    }

    fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                self.flush(ctx)?;
                Ok(QueryResult::Command { tag: "COMMIT".into() })
            }
            // COMMIT of a failed block rolls back, reports ROLLBACK (matches PG).
            TxnState::Failed => Ok(QueryResult::Command { tag: "ROLLBACK".into() }),
            TxnState::Idle => Ok(QueryResult::Command { tag: "COMMIT".into() }),
        }
    }

    fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        self.state = TxnState::Idle; // discard any write-set
        Ok(QueryResult::Command { tag: "ROLLBACK".into() })
    }
```

`run_dml` runs a DML statement either inside the open txn or as an implicit
one-statement txn (autocommit), and transitions to `Failed` on error inside a
block:

```rust
    fn run_dml(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &mut self.state {
            TxnState::InTransaction(_) => {
                // RC: refresh the snapshot at statement start.
                let refresh = {
                    if let TxnState::InTransaction(ctx) = &self.state { !ctx.repeatable_read } else { false }
                };
                if refresh {
                    let ts = self.read_commit_ts()?;
                    if let TxnState::InTransaction(ctx) = &mut self.state { ctx.snapshot = ts; }
                }
                // Borrow the ctx and execute against it.
                let result = {
                    let ctx = match &mut self.state { TxnState::InTransaction(c) => c, _ => unreachable!() };
                    crate::exec::execute_dml(&self.kv, ctx, stmt)
                };
                if result.is_err() {
                    self.state = TxnState::Failed;
                }
                result
            }
            TxnState::Idle => {
                // Implicit one-statement transaction.
                let _guard = self.write_lock.lock().expect("write lock");
                let mut ctx = TxnCtx { snapshot: self.read_commit_ts()?, ..Default::default() };
                let result = crate::exec::execute_dml(&self.kv, &mut ctx, stmt)?;
                self.flush(ctx)?;
                Ok(result)
            }
            TxnState::Failed => unreachable!("guarded in run_one"),
        }
    }
```

(Note: borrowing `self.kv` and `&mut ctx` from `self.state` simultaneously needs
care — clone the `Arc<dyn Kv>` first: `let kv = Arc::clone(&self.kv);` then pass
`&*kv`. Adjust to satisfy the borrow checker; the intent is: execute_dml reads
via kv + ctx, writes into ctx.)

`flush(ctx)` commits a write-set under the write_lock (the implicit path already
holds it; for explicit COMMIT, take it here):

```rust
    fn flush(&self, ctx: TxnCtx) -> Result<(), ExecError> {
        if ctx.writes.is_empty() && ctx.seq.is_empty() {
            return Ok(()); // read-only txn: nothing to commit, no ts bump
        }
        let _guard = self.write_lock.lock().expect("write lock");
        let new_ts = self.read_commit_ts()? + 1;
        let mut ops: Vec<kv::WriteOp> = Vec::new();
        for ((table, rowid), pending) in &ctx.writes {
            let value = match pending {
                Pending::Row(v) => v.clone(),
                Pending::Tombstone => mvcc::encode_version(true, &[]),
            };
            ops.push(kv::WriteOp::Put { key: mvcc::version_key(*table, *rowid, new_ts), value });
        }
        for (table, next) in &ctx.seq {
            ops.push(kv::WriteOp::Put { key: kv::key::seq_key(*table), value: next.to_be_bytes().to_vec() });
        }
        ops.push(kv::WriteOp::Put { key: kv::key::commit_ts_key(), value: new_ts.to_be_bytes().to_vec() });
        self.kv.write_batch(&ops)?;
        Ok(())
    }
```

(Note the `Pending::Row(v)` stores the already-`encode_version(false, row)`-ed
bytes; tombstones are encoded at flush. The implicit-path already holds the
write_lock, so `flush` re-locking a `std::sync::Mutex` would deadlock — make
`flush` NOT take the lock and require callers to hold it, OR use the implicit
path without pre-locking and let flush lock. Pick one: have ONLY `flush` take the
lock, and remove the `_guard` from the implicit `run_dml` arm. Document which.)

- [ ] **Step 5: `exec.rs` — split into `execute_ddl` + `execute_dml`.** Refactor
the old `execute` into: `execute_ddl(session, stmt)` (CREATE/DROP — immediate,
non-transactional, as today) and `execute_dml(kv: &dyn Kv, ctx: &mut TxnCtx,
stmt)` (INSERT/SELECT now; UPDATE/DELETE in Task 6). INSERT writes into
`ctx.writes`/`ctx.seq` (NOT directly to kv); SELECT reads via the write-set
overlay on the visible scan. Implement the overlay in `scan_live_rows`:

```rust
pub(crate) fn scan_live_rows(
    kv: &dyn Kv,
    ctx: &TxnCtx,
    table: &catalog::Table,
) -> Result<Vec<(u64, Vec<pgtypes::Datum>)>, ExecError> {
    let snapshot = mvcc::Snapshot(ctx.snapshot);
    let scanned = kv.scan_prefix(&kv::key::table_prefix(table.id))?;
    let mut out: Vec<(u64, Vec<pgtypes::Datum>)> = Vec::new();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut i = 0;
    while i < scanned.len() {
        let prefix = mvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = kv::key::rowid_of(table.id, &prefix)?;
        let mut versions: Vec<(u64, &[u8])> = Vec::new();
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice() {
            versions.push((mvcc::version::ts_of_key(&scanned[i].0)?, scanned[i].1.as_slice()));
            i += 1;
        }
        seen.insert(rowid);
        // write-set overlay: a pending version shadows the committed one.
        match ctx.writes.get(&(table.id, rowid)) {
            Some(Pending::Tombstone) => {}                       // deleted in this txn
            Some(Pending::Row(v)) => out.push((rowid, mvcc::decode_version(v)?.1)),
            None => {
                if let Some(row) = mvcc::visible_version(snapshot, versions.into_iter())? {
                    out.push((rowid, row));
                }
            }
        }
    }
    // rows that exist ONLY in the write-set (inserted this txn, no committed version yet)
    for ((t, rowid), pending) in &ctx.writes {
        if *t == table.id && !seen.contains(rowid) {
            if let Pending::Row(v) = pending {
                out.push((*rowid, mvcc::decode_version(v)?.1));
            }
        }
    }
    out.sort_by_key(|(rowid, _)| *rowid);
    Ok(out)
}
```

`exec_select` uses `scan_live_rows(kv, ctx, &table)` and drops the rowid for
projection (it already only needs the row datums). INSERT's `execute_dml` arm:
allocate rowids from `ctx.seq.entry(t.id).or_insert(session_seq_or_disk)`,
inserting `Pending::Row(encode_version(false, full))` per row, and recording the
final next-rowid in `ctx.seq`. (For the seq base, read disk via a `kv`-based
read_seq helper that takes `&dyn Kv`.)

- [ ] **Step 6:** `cargo test -p executor --test transactions && cargo test --workspace`
→ all green (transactions + the existing 158+ tests; autocommit still identical).
Note: the `Pending`/`TxnCtx`/`SqlSession` borrow-checker dance is the fiddly part
— if `run_dml` can't borrow `self.kv` and `&mut self.state` together, clone the
`Arc<dyn Kv>` first.

- [ ] **Step 7:** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`, then commit:

```bash
git add crates/executor
git commit -m "feat(executor): transaction state machine, write-set, RC/RR snapshots, 25P02"
```

---

### Task 6: UPDATE and DELETE

**Files:**
- Modify: `crates/executor/src/exec.rs` (add `Statement::Update`/`Statement::Delete` arms to `execute_dml`, replacing the Task 2 `0A000` stubs)
- Test: `crates/executor/tests/update_delete.rs`

UPDATE and DELETE are the read-then-write statements MVCC makes honest. Both
read the visible rows via `scan_live_rows` (the same write-set-overlay reader
SELECT uses, so they see read-your-writes), then write new versions into
`ctx.writes` at the **same rowid** — UPDATE a fresh row version, DELETE a
tombstone. They never touch disk; COMMIT flushes them.

- [ ] **Step 1: Write the failing tests.** Create `crates/executor/tests/update_delete.rs`:

```rust
//! UPDATE / DELETE semantics over MVCC: autocommit and in-transaction,
//! read-your-writes, tombstone hiding, command tags.

use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut impl Session, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } => tag,
        QueryResult::Rows { tag, .. } => tag,
        other => panic!("expected a tagged result, got {other:?}"),
    }
}

fn col0(r: &QueryResult) -> Vec<Option<String>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn update_changes_value_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
    let r = run(&mut s, "UPDATE t SET name = 'z' WHERE id > 1").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 2");
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("a".into()), Some("z".into()), Some("z".into())]
    );
}

#[tokio::test]
async fn update_expression_references_current_row() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "UPDATE t SET id = id + 10").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 3");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("11".into()), Some("12".into()), Some("13".into())]
    );
}

#[tokio::test]
async fn delete_hides_rows_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "DELETE FROM t WHERE id = 2").await;
    assert_eq!(tag_of(&r[0]), "DELETE 1");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("1".into()), Some("3".into())]);
}

#[tokio::test]
async fn delete_all_then_select_is_empty() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2)").await;
    assert_eq!(tag_of(&run(&mut s, "DELETE FROM t").await[0]), "DELETE 2");
    assert!(col0(&run(&mut s, "SELECT id FROM t").await[0]).is_empty());
}

#[tokio::test]
async fn update_then_delete_read_your_writes_in_txn() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b')").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "UPDATE t SET name = 'x' WHERE id = 1").await;
    run(&mut s, "DELETE FROM t WHERE id = 2").await;
    // inside the txn we see our own update + delete
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("x".into())]);
    run(&mut s, "ROLLBACK").await;
    // after rollback the original rows are back
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("a".into()), Some("b".into())]);
}

#[tokio::test]
#[allow(non_snake_case)]
async fn update_missing_table_is_42P01() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let err = s
        .simple_query("UPDATE nope SET a = 1")
        .await
        .expect_err("no table");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
async fn update_unknown_column_is_42703() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    let err = s
        .simple_query("UPDATE t SET nope = 1")
        .await
        .expect_err("no column");
    assert_eq!(err.code, "42703");
}
```

- [ ] **Step 2: Run to verify they fail.**

Run: `cargo test -p executor --test update_delete`
Expected: FAIL — the `Statement::Update`/`Statement::Delete` arms still return the
Task 2 `0A000` stub (`UPDATE 2` etc. never produced).

- [ ] **Step 3: Implement the UPDATE/DELETE arms in `execute_dml`.** Replace the
Task 2 stub arms. Both reuse `scan_live_rows(kv, ctx, &table)` (returns
`Vec<(u64 /*rowid*/, Vec<Datum>)>`) and the existing `coerce` /
`crate::eval::eval` helpers; they accumulate into `ctx.writes`:

```rust
Statement::Update {
    table,
    assignments,
    filter,
} => {
    let t = catalog::get_table(kv, table)?;
    // Resolve each assignment's target column index up front (42703 on miss).
    let targets: Vec<(usize, &Expr)> = assignments
        .iter()
        .map(|(col, expr)| {
            t.column_index(col)
                .map(|idx| (idx, expr))
                .ok_or_else(|| ExecError::UndefinedColumn(col.clone()))
        })
        .collect::<Result<_, _>>()?;
    let mut n: u64 = 0;
    for (rowid, row) in scan_live_rows(kv, ctx, &t)? {
        if !row_matches(filter.as_ref(), &t, &row)? {
            continue;
        }
        // Evaluate SET expressions against the CURRENT row, then coerce.
        let mut next = row.clone();
        for (idx, expr) in &targets {
            let v = crate::eval::eval(expr, Some(&t), &row)?;
            next[*idx] = coerce(v, t.columns[*idx].ty)?;
        }
        ctx.writes
            .insert((t.id, rowid), Pending::Row(mvcc::encode_version(false, &next)));
        n += 1;
    }
    Ok(QueryResult::Command {
        tag: format!("UPDATE {n}"),
    })
}
Statement::Delete { table, filter } => {
    let t = catalog::get_table(kv, table)?;
    let mut n: u64 = 0;
    for (rowid, row) in scan_live_rows(kv, ctx, &t)? {
        if !row_matches(filter.as_ref(), &t, &row)? {
            continue;
        }
        ctx.writes.insert((t.id, rowid), Pending::Tombstone);
        n += 1;
    }
    Ok(QueryResult::Command {
        tag: format!("DELETE {n}"),
    })
}
```

Add the shared filter helper next to `scan_live_rows` (factor the WHERE
evaluation SELECT already does so the three statements agree on three-valued
logic — NULL ⇒ not matched):

```rust
/// Evaluate an optional WHERE predicate against a row (NULL ⇒ false, like SELECT).
fn row_matches(filter: Option<&Expr>, table: &Table, row: &[Datum]) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, Some(table), row)? {
            Datum::Bool(b) => Ok(b),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}
```

Refactor `exec_select`'s inline WHERE block to call `row_matches` too (DRY; same
behavior). UPDATE/DELETE on a non-existent table surface `42P01` from
`catalog::get_table`; an unknown SET column surfaces `42703` from
`column_index`. Both statements are DML, so in autocommit they go through the
same implicit snapshot→flush path as INSERT (Task 5's `run_dml`), and in a block
they accumulate in `ctx.writes` until COMMIT.

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `cargo test -p executor --test update_delete`
Expected: PASS (7 tests).

- [ ] **Step 5: Full workspace + lints.**

Run: `cargo test --workspace && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all green; UPDATE/DELETE no longer report `0A000`.

- [ ] **Step 6: Commit.**

```bash
git add crates/executor
git commit -m "feat(executor): UPDATE and DELETE over MVCC write-set"
```

---

### Task 7: Durability, snapshot-isolation, and end-to-end wire tests

**Files:**
- Modify: `crates/executor/tests/durability.rs` (committed txn survives reopen; rolled-back leaves nothing)
- Modify: `crates/executor/tests/end_to_end.rs` (tokio-postgres `transaction()`, UPDATE/DELETE round-trips)
- Test (snapshot isolation already covered by `transactions.rs` from Task 5; this task adds the durable + wire proofs)

This task proves the spec's success criteria 1, 4, and 6 end-to-end: only
committed versions reach disk, they survive a restart, and the wire protocol
drives a working multi-statement transaction.

- [ ] **Step 1: Write the failing durability tests.** Append to
`crates/executor/tests/durability.rs` (which already opens a `FjallKv`-backed
`SqlEngine` at a `tempfile::TempDir` and reopens it):

```rust
#[tokio::test]
async fn committed_transaction_survives_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, name text)").await;
        run(&mut s, "BEGIN").await;
        run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b')").await;
        run(&mut s, "UPDATE t SET name = 'z' WHERE id = 2").await;
        run(&mut s, "COMMIT").await;
    } // drop closes the store
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("a".into()), Some("z".into())]
    );
}

#[tokio::test]
async fn rolled_back_transaction_leaves_nothing() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4)").await;
        run(&mut s, "BEGIN").await;
        run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
        run(&mut s, "ROLLBACK").await;
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    // Table exists (DDL is non-transactional) but holds no rows.
    let r = run(&mut s, "SELECT id FROM t").await;
    assert!(col0(&r[0]).is_empty());
}
```

(Reuse the `run`/`col0` helpers already in `durability.rs`; if absent, copy the
two-helper shim from `update_delete.rs`. The `Session` import must be present.)

- [ ] **Step 2: Run to verify they fail.**

Run: `cargo test -p executor --test durability`
Expected: the two new tests are present and the suite compiles; new tests pass
once Task 5/6 are in (they exercise no new production code — this step is the
durability *proof*). If `committed_transaction_survives_reopen` fails on the
UPDATE value, the COMMIT flush isn't tagging the new version with the bumped
`commit_ts` — fix in `flush` before proceeding.

- [ ] **Step 3: Write the failing e2e wire tests.** Append to
`crates/executor/tests/end_to_end.rs` (boots a real listener via the SP1/SP2
`serve` harness and connects with tokio-postgres):

```rust
#[tokio::test]
async fn wire_transaction_commit_and_rollback() {
    let (addr, _shutdown) = spawn_server().await; // existing harness
    let (mut client, conn) = connect(addr).await;
    tokio::spawn(conn);

    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");

    // Rollback path: tokio-postgres transaction() dropped without commit.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (1,'a')")
            .await
            .expect("insert");
        // drop without commit → ROLLBACK
    }
    let rows = client.query("SELECT id FROM t", &[]).await.expect("select");
    assert_eq!(rows.len(), 0, "rolled-back insert must be gone");

    // Commit path.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (2,'b')")
            .await
            .expect("insert");
        tx.commit().await.expect("commit");
    }
    let rows = client.query("SELECT id FROM t", &[]).await.expect("select");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 2);
}

#[tokio::test]
async fn wire_update_delete_roundtrip() {
    let (addr, _shutdown) = spawn_server().await;
    let (mut client, conn) = connect(addr).await;
    tokio::spawn(conn);

    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("insert");

    let updated = client
        .execute("UPDATE t SET name = 'z' WHERE id > 1", &[])
        .await
        .expect("update");
    assert_eq!(updated, 2);

    let deleted = client
        .execute("DELETE FROM t WHERE id = 1", &[])
        .await
        .expect("delete");
    assert_eq!(deleted, 1);

    let rows = client
        .query("SELECT id, name FROM t ORDER BY id", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get::<_, &str>(1)).collect();
    assert_eq!(names, vec!["z", "z"]);
}
```

(Use the exact `spawn_server`/`connect` helper names already in
`end_to_end.rs` — match them; do not invent new ones. `client.execute` returns
the affected-row count parsed from the `UPDATE n`/`DELETE n` command tag, which
is why the tags in Task 6 must be exact.)

- [ ] **Step 4: Run to verify they fail, then pass.**

Run: `cargo test -p executor --test end_to_end`
Expected: the two new wire tests compile and pass. A `transaction()` that drops
without commit must leave nothing (proves the wire `BEGIN`/`ROLLBACK` path); the
`execute` row counts prove the command tags round-trip over the wire.

- [ ] **Step 5: Full workspace.**

Run: `cargo test --workspace && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: green.

- [ ] **Step 6: Commit.**

```bash
git add crates/executor
git commit -m "test(executor): durable commit/rollback + wire transaction & UPDATE/DELETE e2e"
```

---

### Task 8: Conformance corpus, success-criteria traceability, and final gauntlet

**Files:**
- Create: `crates/conformance/corpus/update_delete.sql`
- Create: `crates/conformance/corpus/transactions.sql`
- Modify: `crates/pgparser/tests/libpg_query_oracle.rs` (accept cases — if not already added in Task 2)
- Verify: `.github/workflows/ci.yml` gates cover the new crate (no change expected)

The conformance corpus compares crabgresql against a real `postgres:18` oracle.
SP4 adds UPDATE/DELETE result sets and transaction-controlled sequences where
the oracle agrees. This task also runs the full release gauntlet.

- [ ] **Step 1: Add the UPDATE/DELETE corpus.** Create
`crates/conformance/corpus/update_delete.sql` (the harness splits on `;`, runs
each statement against both engines, and diffs result sets + tags):

```sql
CREATE TABLE u (id int4, name text);
INSERT INTO u VALUES (1, 'a'), (2, 'b'), (3, 'c');
UPDATE u SET name = 'z' WHERE id > 1;
SELECT id, name FROM u ORDER BY id;
UPDATE u SET id = id + 10;
SELECT id, name FROM u ORDER BY id;
DELETE FROM u WHERE id = 11;
SELECT id, name FROM u ORDER BY id;
DELETE FROM u;
SELECT count(*) FROM u;
DROP TABLE u;
```

If `count(*)` is not yet supported (aggregates are out of scope), replace the
final `SELECT count(*)` with `SELECT id FROM u ORDER BY id` (empty result) so
the corpus stays within the implemented feature set. Verify by running the
oracle locally (Step 3); drop any line the oracle and crabgresql disagree on and
note it in the conformance gap log rather than forcing parity.

- [ ] **Step 2: Add the transaction corpus.** Create
`crates/conformance/corpus/transactions.sql`. Transactions are stateful, so keep
each assertion to observable end-state result sets:

```sql
CREATE TABLE tx (id int4);
BEGIN;
INSERT INTO tx VALUES (1), (2);
ROLLBACK;
SELECT id FROM tx ORDER BY id;
BEGIN;
INSERT INTO tx VALUES (3), (4);
COMMIT;
SELECT id FROM tx ORDER BY id;
DROP TABLE tx;
```

- [ ] **Step 3: Run the conformance suite against the oracle.**

Run: `cargo test -p conformance -- --include-ignored` (or the project's oracle
invocation — check `crates/conformance/README` / how SP1-SP3 ran it; it needs
Docker `postgres:18`).
Expected: the new corpus files pass parity; overall parity ≥ the SP3 baseline
(96.4%). Record the new percentage. If any statement diverges from the oracle,
remove it from the corpus and add a one-line entry to the tracked-gap list in
the spec's "Scope boundaries" rather than masking the difference.

- [ ] **Step 4: Confirm the parser oracle accept-cases exist.** Verify
`crates/pgparser/tests/libpg_query_oracle.rs` includes the SP4 grammar (added in
Task 2): `BEGIN`, `BEGIN ISOLATION LEVEL REPEATABLE READ`, `START TRANSACTION`,
`COMMIT`, `END`, `ROLLBACK`, `ABORT`, `UPDATE t SET a = 1 WHERE id = 2`,
`DELETE FROM t WHERE id = 1`. If any are missing, add them now.

Run: `cargo test -p pgparser --features oracle`
Expected: PASS (libpg_query agrees crabgresql's accepted SQL is valid PG-18).

- [ ] **Step 5: Run the full release gauntlet.** This is the same gate set SP1-SP3
shipped behind:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p pgparser --features oracle
./scripts/check-no-native.sh        # forbid(unsafe) + no C deps in the shipped tree
cargo deny check                    # license + banned-crate (ring/aws-lc/openssl/cc/...) gate
```

Expected: every gate green. `check-no-native.sh` must still pass with the new
`mvcc` crate present (it has no `build.rs`, no `-sys` deps — pure Rust).

- [ ] **Step 6: Verify the success-criteria traceability.** Confirm every spec
success criterion maps to a green test:

| # | Spec success criterion | Verifying test(s) |
|---|------------------------|-------------------|
| 1 | BEGIN/INSERT/ROLLBACK leaves table unchanged; BEGIN/INSERT/COMMIT persists across restart | `transactions::rollback_discards_writes`, `transactions::commit_persists_writes`, `durability::committed_transaction_survives_reopen`, `durability::rolled_back_transaction_leaves_nothing`, `end_to_end::wire_transaction_commit_and_rollback` |
| 2 | Read-your-writes inside a txn (INSERT/UPDATE/DELETE) | `transactions::*read_your_writes*`, `update_delete::update_then_delete_read_your_writes_in_txn` |
| 3 | Snapshot isolation: RR doesn't see a later commit; RC sees it next statement | `transactions::repeatable_read_does_not_see_concurrent_commit`, `transactions::read_committed_sees_concurrent_commit_next_statement` |
| 4 | UPDATE/DELETE correct results + tags, versioned in MVCC | `update_delete::*`, `end_to_end::wire_update_delete_roundtrip`, conformance `update_delete.sql` |
| 5 | Error in a block ⇒ failed state (25P02 until COMMIT/ROLLBACK); wire reports T/E/I | `transactions::error_in_block_fails_transaction_until_rollback`, wire `tx_status()` test in `session` |
| 6 | Only committed versions reach disk; committed data survives restart | `durability::rolled_back_transaction_leaves_nothing`, `durability::committed_transaction_survives_reopen` |
| 7 | All SP1-SP3 gates green; existing tests pass; conformance parity ≥ baseline | full gauntlet (Step 5) + conformance (Step 3) |

If any row has no green test, that is a gap — add the missing test before
finishing.

- [ ] **Step 7: Commit.**

```bash
git add crates/conformance crates/pgparser
git commit -m "test(conformance): UPDATE/DELETE + transaction corpus; SP4 gauntlet green"
```

---

## Final review (after all tasks)

Dispatch a final code-reviewer subagent over the entire SP4 diff (against the
pre-SP4 main), then run `superpowers:finishing-a-development-branch`. Review focus:

- **MVCC correctness:** descending-ts encoding really yields newest-first; the
  `ts ≤ snapshot` visibility compare is exact at the boundary (a version
  committed *at* the snapshot ts is visible; one *after* is not).
- **Buffer-until-commit invariant:** no code path writes a version to the `Kv`
  outside `flush` under the `write_lock`; ROLLBACK and a dropped session leave
  the store untouched.
- **Snapshot timing:** RC re-reads `commit_ts` per statement; RR captures once at
  the first statement and reuses it; autocommit captures at statement start.
- **Failed-block semantics:** only COMMIT/ROLLBACK escape `25P02`; COMMIT of a
  failed txn reports `ROLLBACK`; an autocommit error stays `Idle`.
- **No `unsafe`, no `unwrap` on fallible paths, no panic on I/O** (`58030` /
  `XX000` instead); `expect()` only where a poisoned mutex is truly unrecoverable.
- **Session lifecycle:** one `Session` per wire connection; no transaction state
  leaks between connections; the shared `kv`/`write_lock` are the only cross-
  session state.
