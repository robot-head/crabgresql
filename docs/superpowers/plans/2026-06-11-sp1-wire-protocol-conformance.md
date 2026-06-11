# SP1: Wire Protocol + Conformance Oracle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real PostgreSQL clients (psql, tokio-postgres, sqlx) connect to crabgresql over the v3 wire protocol with TLS and SCRAM-SHA-256, answered by a pluggable stub engine; a conformance harness diffs crabgresql against real PostgreSQL 18 and publishes a parity report in CI.

**Architecture:** Cargo workspace with three crates: `pgwire` (typed message codec → connection state machine → `Engine` trait seam, with a `StubEngine`), `crabgresql` (the node binary), and `conformance` (differential runner + parity report). Everything is `forbid(unsafe_code)`; the shipped dependency tree is pure Rust (rustls with the RustCrypto provider — never the default `ring`/`aws-lc-rs` providers, which contain C/asm).

**Tech Stack:** Rust 2024, tokio, bytes, rustls 0.23 (`default-features = false`) + rustls-rustcrypto, RustCrypto (sha2/hmac/pbkdf2/subtle), tokio-postgres + proptest (dev/test only), cargo-deny.

**Spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`

---

## File structure

```
Cargo.toml                          # workspace root (members, shared deps, lints)
deny.toml                           # cargo-deny: bans C crates, license allowlist
scripts/check-no-native.sh          # asserts no cc/-sys crates in shipped tree
scripts/oracle-up.sh                # starts real PostgreSQL 18 container (test oracle)
scripts/gen-test-cert.sh            # one-time: openssl CLI generates test CA + server cert
scripts/psql-smoke.sh               # end-to-end smoke test with real psql
.github/workflows/ci.yml            # fmt, clippy, test, safety gates, conformance job
crates/pgwire/
  src/lib.rs                        # module wiring + re-exports
  src/error.rs                      # PgError, SQLSTATE constants
  src/messages/mod.rs
  src/messages/frontend.rs          # startup packet + tagged frontend decode
  src/messages/backend.rs           # backend message encode
  src/engine.rs                     # Engine trait, QueryResult, Cell, FieldDescription
  src/stub.rs                       # StubEngine (canned SELECT 1, version(), pg_sleep)
  src/session.rs                    # connection state machine (generic over Engine + stream)
  src/scram.rs                      # server-side SCRAM-SHA-256
  src/server.rs                     # TCP listener, optional TLS acceptor, cancel registry
  tests/simple_query.rs             # tokio-postgres integration: trust auth + simple query
  tests/extended_query.rs           # tokio-postgres integration: prepare/bind/execute
  tests/cancel.rs                   # cancellation integration test
  tests/scram_auth.rs               # SCRAM integration test
  tests/tls.rs                      # TLS handshake + startup integration test
  tests/golden_trace.rs             # replay recorded psql byte traces through the decoder
  tests/fixtures/                   # test certs, recorded traces
crates/crabgresql/
  src/main.rs                       # CLI: listen addr, auth mode, TLS paths → serve StubEngine
crates/conformance/
  src/lib.rs                        # QueryOutcome, run_one, diff, Report
  src/main.rs                       # bin: run corpus against oracle + subject, emit parity.json
  src/bin/record.rs                 # TCP proxy that records wire traffic to a trace file
  corpus/smoke.sql                  # initial conformance corpus
```

Module dependency order inside `pgwire`: `error` → `messages` → `engine`/`stub` → `scram` → `session` → `server`. Tasks below follow that order so every task compiles on its own.

---

### Task 1: Workspace scaffolding + safety gates

**Files:**
- Modify: `Cargo.toml` (becomes workspace root)
- Create: `crates/pgwire/Cargo.toml`, `crates/pgwire/src/lib.rs`
- Create: `crates/crabgresql/Cargo.toml`, `crates/crabgresql/src/main.rs` (move from `src/main.rs`)
- Create: `crates/conformance/Cargo.toml`, `crates/conformance/src/lib.rs`, `crates/conformance/src/main.rs`
- Create: `deny.toml`, `scripts/check-no-native.sh`, `.github/workflows/ci.yml`
- Delete: `src/main.rs`

- [ ] **Step 1: Convert to a workspace**

Replace the root `Cargo.toml` entirely:

```toml
[workspace]
resolver = "3"
members = ["crates/pgwire", "crates/crabgresql", "crates/conformance"]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
unwrap_used = "warn"

[workspace.dependencies]
pgwire = { path = "crates/pgwire" }
bytes = "1"
thiserror = "2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
tokio-util = "0.7"
tracing = "0.1"
tracing-subscriber = "0.3"
rustls = { version = "0.23", default-features = false, features = ["std", "logging", "tls12"] }
tokio-rustls = { version = "0.26", default-features = false }
rustls-rustcrypto = "0.0.2-alpha"
rustls-pemfile = "2"
sha2 = "0.10"
hmac = "0.12"
pbkdf2 = { version = "0.12", default-features = false }
base64 = "0.22"
rand = "0.9"
subtle = "2"
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio-postgres = "0.7"
proptest = "1"
```

Note on `rustls-rustcrypto`: it is the experimental pure-Rust crypto provider. Check the latest published version on crates.io (`cargo search rustls-rustcrypto` or https://crates.io/crates/rustls-rustcrypto) and use that; the version above is the convention at planning time. The non-negotiable invariant is `default-features = false` on `rustls` and `tokio-rustls` so `aws-lc-rs`/`ring` (C/asm) never enter the graph — Task 1's gates verify this mechanically.

`crates/pgwire/Cargo.toml`:

```toml
[package]
name = "pgwire"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
bytes.workspace = true
thiserror.workspace = true
tokio.workspace = true
tokio-util.workspace = true
tracing.workspace = true
rustls.workspace = true
tokio-rustls.workspace = true
sha2.workspace = true
hmac.workspace = true
pbkdf2.workspace = true
base64.workspace = true
rand.workspace = true
subtle.workspace = true

[dev-dependencies]
tokio-postgres.workspace = true
proptest = { workspace = true }
rustls-rustcrypto.workspace = true
rustls-pemfile.workspace = true
tokio = { workspace = true, features = ["full"] }
```

`crates/pgwire/src/lib.rs` (placeholder for now; modules are added by later tasks):

```rust
//! PostgreSQL v3 wire protocol implementation for crabgresql.
```

`crates/crabgresql/Cargo.toml`:

```toml
[package]
name = "crabgresql"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
pgwire.workspace = true
tokio.workspace = true
clap.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
rustls.workspace = true
rustls-rustcrypto.workspace = true
rustls-pemfile.workspace = true
```

Move the existing hello-world `src/main.rs` to `crates/crabgresql/src/main.rs` unchanged (`git mv src/main.rs crates/crabgresql/src/main.rs`), then delete the now-empty root `src/`.

`crates/conformance/Cargo.toml`:

```toml
[package]
name = "conformance"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
tokio = { workspace = true, features = ["full"] }
tokio-postgres.workspace = true
serde.workspace = true
serde_json.workspace = true
clap.workspace = true
```

`crates/conformance/src/lib.rs`: empty for now (`//! Conformance harness.`). `crates/conformance/src/main.rs`: `fn main() {}` placeholder.

- [ ] **Step 2: Verify the workspace builds**

Run: `cargo build --workspace && cargo test --workspace`
Expected: builds cleanly, zero tests run.

- [ ] **Step 3: Add the safety gates**

`deny.toml`:

```toml
[licenses]
allow = [
    "MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unicode-3.0", "Zlib",
    "CDLA-Permissive-2.0", "MPL-2.0",
]

[bans]
multiple-versions = "warn"
deny = [
    # C / assembly crypto and TLS — the easy way to silently violate zero-C
    "ring", "aws-lc-rs", "aws-lc-sys", "openssl", "openssl-sys",
    # C compilation in build scripts
    "cc", "cmake", "pkg-config",
    # common native libs
    "libz-sys", "zstd-sys", "libsqlite3-sys",
]
```

`scripts/check-no-native.sh` (make executable: `chmod +x`):

```bash
#!/usr/bin/env bash
# Verifies the SHIPPED dependency tree (normal + build deps of the node binary)
# contains no native code. Dev-dependencies (test oracle tooling) are exempt
# per the spec's safety policy.
set -euo pipefail
cd "$(dirname "$0")/.."
bad=$(cargo tree -p crabgresql -e normal,build --prefix none \
    | awk '{print $1}' | sort -u | grep -E '(^cc$|-sys$)' || true)
if [ -n "$bad" ]; then
    echo "FAIL: native-code crates in shipped dependency tree:" >&2
    echo "$bad" >&2
    exit 1
fi
echo "OK: shipped dependency tree is pure Rust"
```

- [ ] **Step 4: Run the gates**

Run: `cargo install cargo-deny --locked` (if not installed), then
`cargo deny check bans licenses && ./scripts/check-no-native.sh`
Expected: both pass (`OK: shipped dependency tree is pure Rust`).

- [ ] **Step 5: Add CI**

`.github/workflows/ci.yml`:

```yaml
name: CI
on:
  push: { branches: [main] }
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: "rustfmt, clippy" }
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
      - uses: EmbarkStudios/cargo-deny-action@v2
        with: { command: check bans licenses }
      - run: ./scripts/check-no-native.sh
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: workspace scaffolding with forbid(unsafe_code) and pure-Rust dependency gates"
```

---

### Task 2: `PgError` and SQLSTATE codes

**Files:**
- Create: `crates/pgwire/src/error.rs`
- Modify: `crates/pgwire/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/pgwire/src/error.rs` (create the file with the test module only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_error_has_fatal_severity_and_08p01() {
        let e = PgError::protocol("bad frame");
        assert_eq!(e.severity, Severity::Fatal);
        assert_eq!(e.code, sqlstate::PROTOCOL_VIOLATION);
        assert_eq!(e.message, "bad frame");
    }

    #[test]
    fn error_constructor_keeps_code() {
        let e = PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        assert_eq!(e.severity, Severity::Error);
        assert_eq!(e.code, "42601");
    }
}
```

Add `pub mod error;` to `crates/pgwire/src/lib.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pgwire -- error::`
Expected: COMPILE FAIL — `PgError` not found.

- [ ] **Step 3: Implement**

Top of `crates/pgwire/src/error.rs`, above the test module:

```rust
//! Protocol-level error type carrying a SQLSTATE, mapped to ErrorResponse.

/// SQLSTATE codes used by the wire layer. Values must match real PostgreSQL —
/// the conformance harness diffs error codes against the oracle.
pub mod sqlstate {
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
    pub const QUERY_CANCELED: &str = "57014";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P05";
    pub const DUPLICATE_CURSOR: &str = "42P03";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Aborts the current query/transaction; session continues.
    Error,
    /// Aborts the session; connection is closed after sending.
    Fatal,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message} ({code})", severity.as_str())]
pub struct PgError {
    pub severity: Severity,
    /// Five-character SQLSTATE.
    pub code: String,
    pub message: String,
}

impl PgError {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self { severity: Severity::Error, code: code.into(), message: message.into() }
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self { severity: Severity::Fatal, code: code.into(), message: message.into() }
    }

    /// Malformed bytes on the wire. Always fatal, per PostgreSQL behavior.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::fatal(sqlstate::PROTOCOL_VIOLATION, message)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pgwire -- error::`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): PgError with SQLSTATE codes"
```

---

### Task 3: Startup packet decoding

**Files:**
- Create: `crates/pgwire/src/messages/mod.rs`, `crates/pgwire/src/messages/frontend.rs`
- Modify: `crates/pgwire/src/lib.rs`

Wire format reference (PostgreSQL docs "Message Formats"): startup-phase packets have **no tag byte** — just `i32 length` (self-inclusive) then payload. The first `i32` of the payload is either the protocol version (`196608` = 3.0) or a magic request code (SSL `80877103`, GSSENC `80877104`, Cancel `80877102`).

- [ ] **Step 1: Write the failing tests**

`crates/pgwire/src/messages/frontend.rs` (test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn startup_bytes(params: &[(&str, &str)]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_3_0);
        for (k, v) in params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminator
        let mut buf = BytesMut::new();
        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(&body);
        buf
    }

    #[test]
    fn decodes_startup_with_params() {
        let mut buf = startup_bytes(&[("user", "crab"), ("database", "crab")]);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            pkt,
            StartupPacket::Startup {
                params: vec![
                    ("user".into(), "crab".into()),
                    ("database".into(), "crab".into()),
                ]
            }
        );
        assert!(buf.is_empty(), "packet bytes fully consumed");
    }

    #[test]
    fn incomplete_packet_returns_none() {
        let full = startup_bytes(&[("user", "crab")]);
        let mut partial = BytesMut::from(&full[..full.len() - 3]);
        assert_eq!(decode_startup(&mut partial).expect("ok"), None);
    }

    #[test]
    fn decodes_ssl_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(SSL_REQUEST_CODE);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(pkt, StartupPacket::SslRequest);
    }

    #[test]
    fn decodes_cancel_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(CANCEL_REQUEST_CODE);
        buf.put_i32(4242);
        buf.put_i32(-12345);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(pkt, StartupPacket::CancelRequest { process_id: 4242, secret_key: -12345 });
    }

    #[test]
    fn unknown_protocol_version_is_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(9);
        buf.put_i32(0x0002_0000); // protocol 2.0
        buf.put_u8(0);
        let err = decode_startup(&mut buf).expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn absurd_length_is_error_not_panic() {
        let mut buf = BytesMut::new();
        buf.put_i32(i32::MAX);
        buf.put_i32(PROTOCOL_3_0);
        assert!(decode_startup(&mut buf).is_err());
    }
}
```

`crates/pgwire/src/messages/mod.rs`:

```rust
pub mod frontend;
```

In `crates/pgwire/src/lib.rs` add `pub mod messages;`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pgwire -- frontend::`
Expected: COMPILE FAIL — `decode_startup`, `StartupPacket` not found.

- [ ] **Step 3: Implement**

Top of `crates/pgwire/src/messages/frontend.rs`:

```rust
//! Decoding of frontend (client → server) messages.
//!
//! All decode functions return `Ok(None)` when the buffer does not yet hold a
//! complete message, and never panic on malformed input.

use bytes::{Buf, Bytes, BytesMut};

use crate::error::PgError;

pub const PROTOCOL_3_0: i32 = 0x0003_0000; // 196608
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;

/// Matches real PostgreSQL's startup packet length cap (MAX_STARTUP_PACKET_LENGTH).
pub const MAX_STARTUP_PACKET_LEN: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    Startup { params: Vec<(String, String)> },
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
}

pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<StartupPacket>, PgError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len < 8 || len as usize > MAX_STARTUP_PACKET_LEN {
        return Err(PgError::protocol(format!("invalid startup packet length: {len}")));
    }
    let len = len as usize;
    if buf.len() < len {
        return Ok(None);
    }
    let mut body = buf.split_to(len).freeze();
    body.advance(4); // length field
    let code = get_i32(&mut body)?;
    match code {
        SSL_REQUEST_CODE => Ok(Some(StartupPacket::SslRequest)),
        GSSENC_REQUEST_CODE => Ok(Some(StartupPacket::GssEncRequest)),
        CANCEL_REQUEST_CODE => Ok(Some(StartupPacket::CancelRequest {
            process_id: get_i32(&mut body)?,
            secret_key: get_i32(&mut body)?,
        })),
        PROTOCOL_3_0 => {
            let mut params = Vec::new();
            loop {
                let key = get_cstr(&mut body)?;
                if key.is_empty() {
                    break;
                }
                let value = get_cstr(&mut body)?;
                params.push((key, value));
            }
            Ok(Some(StartupPacket::Startup { params }))
        }
        other => Err(PgError::protocol(format!(
            "unsupported frontend protocol {}.{}; server supports 3.0",
            (other >> 16) & 0xffff,
            other & 0xffff,
        ))),
    }
}

// ---- checked readers (Bytes::get_* panic on underflow; never call those) ----

pub(crate) fn get_i32(buf: &mut Bytes) -> Result<i32, PgError> {
    if buf.len() < 4 {
        return Err(PgError::protocol("message truncated reading i32"));
    }
    Ok(buf.get_i32())
}

pub(crate) fn get_i16(buf: &mut Bytes) -> Result<i16, PgError> {
    if buf.len() < 2 {
        return Err(PgError::protocol("message truncated reading i16"));
    }
    Ok(buf.get_i16())
}

pub(crate) fn get_u8(buf: &mut Bytes) -> Result<u8, PgError> {
    if buf.is_empty() {
        return Err(PgError::protocol("message truncated reading byte"));
    }
    Ok(buf.get_u8())
}

pub(crate) fn get_bytes(buf: &mut Bytes, n: usize) -> Result<Bytes, PgError> {
    if buf.len() < n {
        return Err(PgError::protocol("message truncated reading bytes"));
    }
    Ok(buf.split_to(n))
}

pub(crate) fn get_cstr(buf: &mut Bytes) -> Result<String, PgError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| PgError::protocol("unterminated string"))?;
    let raw = buf.split_to(pos);
    buf.advance(1); // NUL
    String::from_utf8(raw.to_vec()).map_err(|_| PgError::protocol("string is not valid UTF-8"))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p pgwire -- frontend::`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): startup packet decoding (Startup, SSLRequest, GSSENC, CancelRequest)"
```

---

### Task 4: Tagged frontend message decoding

**Files:**
- Modify: `crates/pgwire/src/messages/frontend.rs`

Wire format: after startup, every frontend message is `u8 tag` + `i32 length` (length includes itself, excludes the tag) + body.

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module in `frontend.rs`:

```rust
    fn tagged(tag: u8, body: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(body);
        buf
    }

    #[test]
    fn decodes_query() {
        let mut buf = tagged(b'Q', b"SELECT 1\0");
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(msg, FrontendMessage::Query { sql: "SELECT 1".into() });
    }

    #[test]
    fn decodes_terminate_sync_flush() {
        for (tag, want) in [
            (b'X', FrontendMessage::Terminate),
            (b'S', FrontendMessage::Sync),
            (b'H', FrontendMessage::Flush),
        ] {
            let mut buf = tagged(tag, b"");
            assert_eq!(decode_message(&mut buf).expect("ok").expect("complete"), want);
        }
    }

    #[test]
    fn decodes_parse_with_param_types() {
        let mut body = BytesMut::new();
        body.put_slice(b"stmt1\0SELECT $1\0");
        body.put_i16(1);
        body.put_i32(23); // int4 oid
        let mut buf = tagged(b'P', &body);
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Parse {
                name: "stmt1".into(),
                sql: "SELECT $1".into(),
                param_types: vec![23],
            }
        );
    }

    #[test]
    fn decodes_bind() {
        let mut body = BytesMut::new();
        body.put_slice(b"portal1\0stmt1\0");
        body.put_i16(1); // one param format code
        body.put_i16(0); // text
        body.put_i16(2); // two params
        body.put_i32(2);
        body.put_slice(b"42");
        body.put_i32(-1); // NULL param
        body.put_i16(1); // one result format code
        body.put_i16(1); // binary
        let mut buf = tagged(b'B', &body);
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Bind {
                portal: "portal1".into(),
                statement: "stmt1".into(),
                param_formats: vec![0],
                params: vec![Some(Bytes::from_static(b"42")), None],
                result_formats: vec![1],
            }
        );
    }

    #[test]
    fn decodes_describe_execute_close() {
        let mut buf = tagged(b'D', b"Sstmt1\0");
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Describe { kind: b'S', name: "stmt1".into() }
        );

        let mut body = BytesMut::new();
        body.put_slice(b"portal1\0");
        body.put_i32(0);
        let mut buf = tagged(b'E', &body);
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Execute { portal: "portal1".into(), max_rows: 0 }
        );

        let mut buf = tagged(b'C', b"P\0");
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Close { kind: b'P', name: "".into() }
        );
    }

    #[test]
    fn decodes_password_message_raw() {
        let mut buf = tagged(b'p', b"SCRAM-SHA-256\0\0\0\0\x05hello");
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Password(Bytes::from_static(b"SCRAM-SHA-256\0\0\0\0\x05hello"))
        );
    }

    #[test]
    fn partial_message_returns_none_and_keeps_buffer() {
        let full = tagged(b'Q', b"SELECT 1\0");
        let mut partial = BytesMut::from(&full[..4]);
        assert_eq!(decode_message(&mut partial).expect("ok"), None);
        assert_eq!(partial.len(), 4, "no bytes consumed");
    }

    #[test]
    fn unknown_tag_is_error() {
        let mut buf = tagged(b'?', b"");
        assert!(decode_message(&mut buf).is_err());
    }

    #[test]
    fn oversized_length_is_error_not_panic() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(i32::MAX);
        assert!(decode_message(&mut buf).is_err());
    }
```

And a proptest robustness test (decoder must never panic on arbitrary bytes) — append at the bottom of the file, outside the unit-test module:

```rust
#[cfg(test)]
mod proptests {
    use super::*;
    use bytes::BytesMut;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn decode_message_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let _ = decode_message(&mut buf);
        }

        #[test]
        fn decode_startup_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let _ = decode_startup(&mut buf);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pgwire -- frontend`
Expected: COMPILE FAIL — `decode_message`, `FrontendMessage` not found.

- [ ] **Step 3: Implement**

Add to `frontend.rs` below the startup section:

```rust
/// Cap matching PostgreSQL's PQ_LARGE_MESSAGE_LIMIT order of magnitude.
pub const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Query { sql: String },
    /// 'p' carries password / SASLInitialResponse / SASLResponse depending on
    /// auth state; the session layer interprets the raw body.
    Password(Bytes),
    Parse { name: String, sql: String, param_types: Vec<u32> },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
    },
    Describe { kind: u8, name: String },
    Execute { portal: String, max_rows: i32 },
    Close { kind: u8, name: String },
    Sync,
    Flush,
    Terminate,
}

pub fn decode_message(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, PgError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 || len as usize > MAX_MESSAGE_LEN {
        return Err(PgError::protocol(format!("invalid message length {len} for tag {}", tag as char)));
    }
    let total = 1 + len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let mut body = buf.split_to(total).freeze();
    body.advance(5); // tag + length

    let msg = match tag {
        b'Q' => FrontendMessage::Query { sql: get_cstr(&mut body)? },
        b'X' => FrontendMessage::Terminate,
        b'S' => FrontendMessage::Sync,
        b'H' => FrontendMessage::Flush,
        b'p' => FrontendMessage::Password(body.clone()),
        b'P' => {
            let name = get_cstr(&mut body)?;
            let sql = get_cstr(&mut body)?;
            let n = get_i16(&mut body)?;
            let n = usize::try_from(n).map_err(|_| PgError::protocol("negative parameter count"))?;
            let mut param_types = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                param_types.push(get_i32(&mut body)? as u32);
            }
            FrontendMessage::Parse { name, sql, param_types }
        }
        b'B' => {
            let portal = get_cstr(&mut body)?;
            let statement = get_cstr(&mut body)?;
            let param_formats = decode_i16_vec(&mut body)?;
            let nparams = get_i16(&mut body)?;
            let nparams =
                usize::try_from(nparams).map_err(|_| PgError::protocol("negative param count"))?;
            let mut params = Vec::with_capacity(nparams.min(1024));
            for _ in 0..nparams {
                let len = get_i32(&mut body)?;
                if len < 0 {
                    params.push(None);
                } else {
                    params.push(Some(get_bytes(&mut body, len as usize)?));
                }
            }
            let result_formats = decode_i16_vec(&mut body)?;
            FrontendMessage::Bind { portal, statement, param_formats, params, result_formats }
        }
        b'D' => FrontendMessage::Describe { kind: get_u8(&mut body)?, name: get_cstr(&mut body)? },
        b'E' => FrontendMessage::Execute { portal: get_cstr(&mut body)?, max_rows: get_i32(&mut body)? },
        b'C' => FrontendMessage::Close { kind: get_u8(&mut body)?, name: get_cstr(&mut body)? },
        other => {
            return Err(PgError::protocol(format!(
                "unknown frontend message tag {:?}",
                other as char
            )));
        }
    };
    Ok(Some(msg))
}

fn decode_i16_vec(body: &mut Bytes) -> Result<Vec<i16>, PgError> {
    let n = get_i16(body)?;
    let n = usize::try_from(n).map_err(|_| PgError::protocol("negative count"))?;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        out.push(get_i16(body)?);
    }
    Ok(out)
}
```

Add `proptest.workspace = true` already covered by Task 1 dev-dependencies.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p pgwire -- frontend`
Expected: all unit tests + 2 proptests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): tagged frontend message decoding with proptest robustness"
```

---

### Task 5: Backend message encoding

**Files:**
- Create: `crates/pgwire/src/messages/backend.rs`
- Modify: `crates/pgwire/src/messages/mod.rs` (add `pub mod backend;`)

Backend messages are `u8 tag` + `i32 length` (self-inclusive, tag excluded) + body. Encoding writes into a caller-supplied `BytesMut` so a whole response batch is one write.

- [ ] **Step 1: Write the failing tests**

`crates/pgwire/src/messages/backend.rs` (test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::FieldDescription;
    use crate::error::{PgError, sqlstate};
    use bytes::{Bytes, BytesMut};

    #[test]
    fn encodes_authentication_ok() {
        let mut out = BytesMut::new();
        authentication_ok(&mut out);
        assert_eq!(&out[..], b"R\x00\x00\x00\x08\x00\x00\x00\x00");
    }

    #[test]
    fn encodes_ready_for_query_idle() {
        let mut out = BytesMut::new();
        ready_for_query(&mut out, TxStatus::Idle);
        assert_eq!(&out[..], b"Z\x00\x00\x00\x05I");
    }

    #[test]
    fn encodes_parameter_status() {
        let mut out = BytesMut::new();
        parameter_status(&mut out, "client_encoding", "UTF8");
        assert_eq!(&out[..], b"S\x00\x00\x00\x19client_encoding\0UTF8\0");
    }

    #[test]
    fn encodes_command_complete() {
        let mut out = BytesMut::new();
        command_complete(&mut out, "SELECT 1");
        assert_eq!(&out[..], b"C\x00\x00\x00\x0dSELECT 1\0");
    }

    #[test]
    fn encodes_error_response_fields() {
        let mut out = BytesMut::new();
        error_response(&mut out, &PgError::error(sqlstate::SYNTAX_ERROR, "oops"));
        // tag, len, then S/V/C/M fields, NUL terminator
        assert_eq!(out[0], b'E');
        let body = &out[5..];
        assert!(body.starts_with(b"SERROR\0VERROR\0C42601\0Moops\0"));
        assert_eq!(*body.last().expect("non-empty"), 0);
    }

    #[test]
    fn encodes_row_description_and_data_row() {
        let mut out = BytesMut::new();
        let fields = [FieldDescription {
            name: "?column?".into(),
            table_oid: 0,
            column_id: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        }];
        row_description(&mut out, &fields);
        assert_eq!(out[0], b'T');
        // field count 1
        assert_eq!(&out[5..7], &1i16.to_be_bytes());

        let mut out = BytesMut::new();
        data_row(&mut out, &[Some(Bytes::from_static(b"1")), None]);
        // tag D, len 15: 4(len) + 2(count) + 4+1 (value "1") + 4 (-1 null)
        assert_eq!(&out[..], b"D\x00\x00\x00\x0f\x00\x02\x00\x00\x00\x011\xff\xff\xff\xff");
    }

    #[test]
    fn encodes_backend_key_data() {
        let mut out = BytesMut::new();
        backend_key_data(&mut out, 4242, 777);
        assert_eq!(out[0], b'K');
        assert_eq!(out.len(), 13);
    }

    #[test]
    fn encodes_auth_sasl_flow_messages() {
        let mut out = BytesMut::new();
        authentication_sasl(&mut out, &["SCRAM-SHA-256"]);
        assert_eq!(&out[..], b"R\x00\x00\x00\x17\x00\x00\x00\x0aSCRAM-SHA-256\0\0");

        let mut out = BytesMut::new();
        authentication_sasl_continue(&mut out, b"r=abc");
        assert_eq!(&out[..], b"R\x00\x00\x00\x0d\x00\x00\x00\x0br=abc");

        let mut out = BytesMut::new();
        authentication_sasl_final(&mut out, b"v=xyz");
        assert_eq!(&out[..], b"R\x00\x00\x00\x0d\x00\x00\x00\x0cv=xyz");
    }

    #[test]
    fn encodes_extended_protocol_responses() {
        let mut out = BytesMut::new();
        parse_complete(&mut out);
        bind_complete(&mut out);
        close_complete(&mut out);
        no_data(&mut out);
        empty_query_response(&mut out);
        parameter_description(&mut out, &[23, 25]);
        assert_eq!(
            &out[..],
            &b"1\x00\x00\x00\x042\x00\x00\x00\x043\x00\x00\x00\x04n\x00\x00\x00\x04I\x00\x00\x00\x04t\x00\x00\x00\x0e\x00\x02\x00\x00\x00\x17\x00\x00\x00\x19"[..]
        );
    }
}
```

This test references `crate::engine::FieldDescription`, which doesn't exist yet — create a minimal `crates/pgwire/src/engine.rs` **in this task** containing only the struct (Task 6 fills in the rest):

```rust
//! Engine seam: types the wire layer exchanges with the query engine.

/// One column in a RowDescription. Field order matches the wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 = text, 1 = binary.
    pub format: i16,
}
```

Add `pub mod engine;` to `lib.rs` and `pub mod backend;` to `messages/mod.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pgwire -- backend::`
Expected: COMPILE FAIL — encoder functions not found.

- [ ] **Step 3: Implement**

Top of `crates/pgwire/src/messages/backend.rs`:

```rust
//! Encoding of backend (server → client) messages.

use bytes::{BufMut, Bytes, BytesMut};

use crate::engine::FieldDescription;
use crate::error::PgError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TxStatus {
    fn as_byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

/// Writes `tag` + self-inclusive length + body produced by `f`.
fn msg(out: &mut BytesMut, tag: u8, f: impl FnOnce(&mut BytesMut)) {
    out.put_u8(tag);
    let len_at = out.len();
    out.put_i32(0); // patched below
    f(out);
    let len = (out.len() - len_at) as i32;
    out[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}

fn put_cstr(out: &mut BytesMut, s: &str) {
    out.put_slice(s.as_bytes());
    out.put_u8(0);
}

pub fn authentication_ok(out: &mut BytesMut) {
    msg(out, b'R', |b| b.put_i32(0));
}

pub fn authentication_sasl(out: &mut BytesMut, mechanisms: &[&str]) {
    msg(out, b'R', |b| {
        b.put_i32(10);
        for m in mechanisms {
            put_cstr(b, m);
        }
        b.put_u8(0);
    });
}

pub fn authentication_sasl_continue(out: &mut BytesMut, data: &[u8]) {
    msg(out, b'R', |b| {
        b.put_i32(11);
        b.put_slice(data);
    });
}

pub fn authentication_sasl_final(out: &mut BytesMut, data: &[u8]) {
    msg(out, b'R', |b| {
        b.put_i32(12);
        b.put_slice(data);
    });
}

pub fn parameter_status(out: &mut BytesMut, name: &str, value: &str) {
    msg(out, b'S', |b| {
        put_cstr(b, name);
        put_cstr(b, value);
    });
}

pub fn backend_key_data(out: &mut BytesMut, process_id: i32, secret_key: i32) {
    msg(out, b'K', |b| {
        b.put_i32(process_id);
        b.put_i32(secret_key);
    });
}

pub fn ready_for_query(out: &mut BytesMut, status: TxStatus) {
    msg(out, b'Z', |b| b.put_u8(status.as_byte()));
}

pub fn command_complete(out: &mut BytesMut, tag: &str) {
    msg(out, b'C', |b| put_cstr(b, tag));
}

pub fn empty_query_response(out: &mut BytesMut) {
    msg(out, b'I', |_| {});
}

pub fn parse_complete(out: &mut BytesMut) {
    msg(out, b'1', |_| {});
}

pub fn bind_complete(out: &mut BytesMut) {
    msg(out, b'2', |_| {});
}

pub fn close_complete(out: &mut BytesMut) {
    msg(out, b'3', |_| {});
}

pub fn no_data(out: &mut BytesMut) {
    msg(out, b'n', |_| {});
}

pub fn parameter_description(out: &mut BytesMut, type_oids: &[u32]) {
    msg(out, b't', |b| {
        b.put_i16(type_oids.len() as i16);
        for oid in type_oids {
            b.put_i32(*oid as i32);
        }
    });
}

pub fn row_description(out: &mut BytesMut, fields: &[FieldDescription]) {
    msg(out, b'T', |b| {
        b.put_i16(fields.len() as i16);
        for f in fields {
            put_cstr(b, &f.name);
            b.put_i32(f.table_oid as i32);
            b.put_i16(f.column_id);
            b.put_i32(f.type_oid as i32);
            b.put_i16(f.type_size);
            b.put_i32(f.type_modifier);
            b.put_i16(f.format);
        }
    });
}

pub fn data_row(out: &mut BytesMut, values: &[Option<Bytes>]) {
    msg(out, b'D', |b| {
        b.put_i16(values.len() as i16);
        for v in values {
            match v {
                Some(bytes) => {
                    b.put_i32(bytes.len() as i32);
                    b.put_slice(bytes);
                }
                None => b.put_i32(-1),
            }
        }
    });
}

pub fn error_response(out: &mut BytesMut, err: &PgError) {
    msg(out, b'E', |b| {
        b.put_u8(b'S');
        put_cstr(b, err.severity.as_str());
        b.put_u8(b'V');
        put_cstr(b, err.severity.as_str());
        b.put_u8(b'C');
        put_cstr(b, &err.code);
        b.put_u8(b'M');
        put_cstr(b, &err.message);
        b.put_u8(0);
    });
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p pgwire -- backend::`
Expected: all pass. Double-check the byte-literal assertions against failures carefully — off-by-one in a hand-computed length means the *test* constant is wrong; recompute (length = 4 + body bytes) rather than pasting the implementation's output.

- [ ] **Step 5: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): backend message encoding"
```

---

### Task 6: `Engine` trait + `StubEngine`

**Files:**
- Modify: `crates/pgwire/src/engine.rs`
- Create: `crates/pgwire/src/stub.rs`
- Modify: `crates/pgwire/src/lib.rs` (add `pub mod stub;`)

Design notes:
- The trait uses desugared `impl Future + Send` returns (not bare `async fn`) so futures are `Send` and sessions can be `tokio::spawn`ed. The server is **generic** over `E: Engine` — no `dyn`.
- Rows carry a `Cell { text, binary }` pair: simple query always sends text; extended query honors the client's requested result format (tokio-postgres asks for binary). The stub pre-computes both encodings; a real engine will too (its type system owns both wire encodings per the spec's `pgtypes` crate).

- [ ] **Step 1: Write the failing tests**

Append to `crates/pgwire/src/engine.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::stub::StubEngine;

    #[tokio::test]
    async fn stub_answers_select_1() {
        let engine = StubEngine::new();
        let results = engine.simple_query("SELECT 1").await.expect("ok");
        let [QueryResult::Rows { fields, rows, tag }] = &results[..] else {
            panic!("expected one Rows result, got {results:?}");
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "?column?");
        assert_eq!(fields[0].type_oid, oids::INT4);
        assert_eq!(tag, "SELECT 1");
        assert_eq!(rows.len(), 1);
        let cell = rows[0][0].as_ref().expect("not null");
        assert_eq!(&cell.text[..], b"1");
        assert_eq!(&cell.binary[..], &1i32.to_be_bytes());
    }

    #[tokio::test]
    async fn stub_answers_version_case_insensitively() {
        let engine = StubEngine::new();
        let results = engine.simple_query("select VERSION()").await.expect("ok");
        let [QueryResult::Rows { fields, rows, .. }] = &results[..] else {
            panic!("expected Rows");
        };
        assert_eq!(fields[0].type_oid, oids::TEXT);
        let text = std::str::from_utf8(&rows[0][0].as_ref().expect("not null").text).expect("utf8");
        assert!(text.starts_with("PostgreSQL 18"), "clients parse this prefix: {text}");
    }

    #[tokio::test]
    async fn stub_rejects_unknown_sql_with_feature_not_supported() {
        let engine = StubEngine::new();
        let err = engine.simple_query("SELECT * FROM t").await.expect_err("must fail");
        assert_eq!(err.code, crate::error::sqlstate::FEATURE_NOT_SUPPORTED);
    }

    #[tokio::test]
    async fn stub_handles_empty_query() {
        let engine = StubEngine::new();
        let results = engine.simple_query("   ").await.expect("ok");
        assert_eq!(results, vec![QueryResult::Empty]);
    }

    #[tokio::test]
    async fn stub_describe_returns_fields_without_executing() {
        let engine = StubEngine::new();
        let described = engine.describe("SELECT 1").await.expect("ok");
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].type_oid, oids::INT4);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pgwire -- engine::`
Expected: COMPILE FAIL — `QueryResult`, `oids`, `StubEngine` not found.

- [ ] **Step 3: Implement the engine types and trait**

Add to `crates/pgwire/src/engine.rs` (above the existing `FieldDescription`):

```rust
use std::future::Future;

use bytes::Bytes;

use crate::error::PgError;

/// Type OIDs from pg_type.dat. The stub needs only these two; the real
/// catalog crate will own the full set.
pub mod oids {
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
}

/// A single value, pre-encoded in both wire formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub text: Bytes,
    pub binary: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    Rows {
        fields: Vec<FieldDescription>,
        rows: Vec<Vec<Option<Cell>>>,
        tag: String,
    },
    /// Statement with no result set (e.g. SET); tag like "INSERT 0 1".
    Command { tag: String },
    /// Empty query string → EmptyQueryResponse.
    Empty,
}

/// The seam between the wire protocol and the query engine. SP1 ships only
/// `StubEngine`; the real engine arrives in SP2 behind this same trait.
pub trait Engine: Send + Sync + 'static {
    /// Execute the full text of a simple-protocol Query message (may contain
    /// multiple statements — splitting is the engine's job).
    fn simple_query(
        &self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<QueryResult>, PgError>> + Send;

    /// Row description for a statement without executing it (extended-protocol
    /// Describe). Empty vec = statement returns no rows.
    fn describe(
        &self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<FieldDescription>, PgError>> + Send;
}
```

- [ ] **Step 4: Implement `StubEngine`**

`crates/pgwire/src/stub.rs`:

```rust
//! Canned-response engine: enough surface for psql, driver integration
//! tests, and the conformance harness to exercise the wire protocol.

use bytes::Bytes;

use crate::engine::{Cell, Engine, FieldDescription, QueryResult, oids};
use crate::error::{PgError, sqlstate};

pub const STUB_VERSION: &str =
    "PostgreSQL 18.0 (crabgresql 0.1.0) on aarch64, compiled by rustc, 64-bit";

#[derive(Debug, Default, Clone)]
pub struct StubEngine {}

impl StubEngine {
    pub fn new() -> Self {
        Self {}
    }

    fn canned(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" => Ok(vec![QueryResult::Empty]),
            "select 1" => Ok(vec![QueryResult::Rows {
                fields: vec![int4_field("?column?")],
                rows: vec![vec![Some(int4_cell(1))]],
                tag: "SELECT 1".into(),
            }]),
            "select version()" => Ok(vec![QueryResult::Rows {
                fields: vec![text_field("version")],
                rows: vec![vec![Some(text_cell(STUB_VERSION))]],
                tag: "SELECT 1".into(),
            }]),
            other => Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("stub engine does not implement: {other}"),
            )),
        }
    }
}

impl Engine for StubEngine {
    async fn simple_query(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // `pg_sleep` exists so cancellation has something to cancel.
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        if let Some(secs) = normalized
            .strip_prefix("select pg_sleep(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|n| n.parse::<u64>().ok())
        {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            return Ok(vec![QueryResult::Rows {
                fields: vec![text_field("pg_sleep")],
                rows: vec![vec![Some(text_cell(""))]],
                tag: "SELECT 1".into(),
            }]);
        }
        self.canned(sql)
    }

    async fn describe(&self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        match self.canned(sql)?.first() {
            Some(QueryResult::Rows { fields, .. }) => Ok(fields.clone()),
            _ => Ok(Vec::new()),
        }
    }
}

fn int4_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::INT4,
        type_size: 4,
        type_modifier: -1,
        format: 0,
    }
}

fn text_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::TEXT,
        type_size: -1,
        type_modifier: -1,
        format: 0,
    }
}

fn int4_cell(v: i32) -> Cell {
    Cell {
        text: Bytes::from(v.to_string()),
        binary: Bytes::copy_from_slice(&v.to_be_bytes()),
    }
}

fn text_cell(v: &str) -> Cell {
    let b = Bytes::copy_from_slice(v.as_bytes());
    Cell { text: b.clone(), binary: b }
}
```

Note for the implementer: `async fn` in the impl satisfies the desugared `impl Future + Send` trait methods because `StubEngine: Sync` and the futures capture only `&self` and owned data; if the compiler reports a non-`Send` future here, the fix belongs in the impl (avoid holding non-`Send` locals across `.await`), not in the trait.

Add `pub mod stub;` to `lib.rs`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p pgwire -- engine:: stub::`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): Engine trait and StubEngine with dual-format cells"
```

---

### Task 7: Session state machine — startup, trust auth, simple query

**Files:**
- Create: `crates/pgwire/src/session.rs`, `crates/pgwire/src/server.rs`
- Modify: `crates/pgwire/src/lib.rs` (add `pub mod session;` and `pub mod server;`)
- Test: `crates/pgwire/tests/simple_query.rs`

Split of responsibilities: `server.rs` owns the TCP accept loop and the pre-startup negotiation (SSLRequest → `'N'` for now, GSSENC → `'N'`, CancelRequest → close; TLS and cancellation are upgraded by Tasks 12 and 10). `session.rs` owns everything after a valid `StartupMessage`: auth, parameter announcement, the query loop.

- [ ] **Step 1: Write the failing integration test**

`crates/pgwire/tests/simple_query.rs`:

```rust
use std::sync::Arc;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn trust_auth_and_select_1() {
    let client = connect(spawn_server().await).await;
    let messages = client.simple_query("SELECT 1").await.expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert_eq!(row.get(0), Some("1"));
}

#[tokio::test]
async fn version_query_works() {
    let client = connect(spawn_server().await).await;
    let messages = client.simple_query("SELECT version()").await.expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert!(row.get(0).expect("value").starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn unsupported_query_returns_0a000_and_session_survives() {
    let client = connect(spawn_server().await).await;
    let err = client.simple_query("SELECT * FROM nope").await.expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000");
    // The session must still be usable after an ERROR (not FATAL).
    let messages = client.simple_query("SELECT 1").await.expect("session survives");
    assert!(messages.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))));
}

#[tokio::test]
async fn empty_query_returns_cleanly() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres surfaces EmptyQueryResponse as zero rows + CommandComplete-less result
    let messages = client.simple_query("   ").await.expect("empty ok");
    assert!(!messages.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pgwire --test simple_query`
Expected: COMPILE FAIL — `pgwire::session`, `pgwire::server` not found.

- [ ] **Step 3: Implement `session.rs`**

```rust
//! Post-startup connection state machine, generic over the byte stream so the
//! same code runs plaintext and TLS sessions.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::engine::{Engine, QueryResult};
use crate::error::{PgError, Severity, sqlstate};
use crate::messages::backend::{self, TxStatus};
use crate::messages::frontend::{self, FrontendMessage};

#[derive(Debug, Clone)]
pub enum AuthMode {
    Trust,
    // ScramSha256 added in the SCRAM task
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub auth: AuthMode,
    /// ParameterStatus values announced at session start. Clients parse
    /// server_version and rely on client_encoding=UTF8.
    pub server_params: Vec<(String, String)>,
}

impl SessionConfig {
    pub fn trust() -> Self {
        Self { auth: AuthMode::Trust, server_params: default_server_params() }
    }
}

pub fn default_server_params() -> Vec<(String, String)> {
    [
        ("server_version", "18.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

pub async fn run_session<S, E>(
    mut stream: S,
    _startup_params: Vec<(String, String)>,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    E: Engine,
{
    let mut out = BytesMut::with_capacity(1024);

    match config.auth {
        AuthMode::Trust => backend::authentication_ok(&mut out),
    }
    for (name, value) in &config.server_params {
        backend::parameter_status(&mut out, name, value);
    }
    // Placeholder key data; the cancellation task wires real values.
    backend::backend_key_data(&mut out, 0, 0);
    backend::ready_for_query(&mut out, TxStatus::Idle);
    stream.write_all(&out).await?;
    out.clear();

    let mut inbuf = BytesMut::with_capacity(8 * 1024);
    loop {
        let msg = match frontend::decode_message(&mut inbuf) {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                if stream.read_buf(&mut inbuf).await? == 0 {
                    return Ok(()); // client went away
                }
                continue;
            }
            Err(e) => {
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(()); // protocol errors are fatal
            }
        };

        match msg {
            FrontendMessage::Terminate => return Ok(()),
            FrontendMessage::Query { sql } => {
                match engine.simple_query(&sql).await {
                    Ok(results) => write_results(&mut out, &results),
                    Err(e) => {
                        backend::error_response(&mut out, &e);
                        if e.severity == Severity::Fatal {
                            stream.write_all(&out).await?;
                            return Ok(());
                        }
                    }
                }
                backend::ready_for_query(&mut out, TxStatus::Idle);
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Sync => {
                backend::ready_for_query(&mut out, TxStatus::Idle);
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Flush => stream.flush().await?,
            // Extended protocol lands in its own task; until then reply with a
            // non-fatal error so clients fail a statement, not the session.
            FrontendMessage::Parse { .. }
            | FrontendMessage::Bind { .. }
            | FrontendMessage::Describe { .. }
            | FrontendMessage::Execute { .. }
            | FrontendMessage::Close { .. } => {
                let e = PgError::error(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    "extended query protocol not yet implemented",
                );
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Password(_) => {
                let e = PgError::protocol("unexpected password message outside authentication");
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(());
            }
        }
    }
}

/// Simple protocol always sends text format.
fn write_results(out: &mut BytesMut, results: &[QueryResult]) {
    for result in results {
        match result {
            QueryResult::Rows { fields, rows, tag } => {
                backend::row_description(out, fields);
                for row in rows {
                    let values: Vec<Option<Bytes>> =
                        row.iter().map(|c| c.as_ref().map(|c| c.text.clone())).collect();
                    backend::data_row(out, &values);
                }
                backend::command_complete(out, tag);
            }
            QueryResult::Command { tag } => backend::command_complete(out, tag),
            QueryResult::Empty => backend::empty_query_response(out),
        }
    }
}
```

- [ ] **Step 4: Implement `server.rs`**

```rust
//! TCP accept loop and pre-startup negotiation.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::engine::Engine;
use crate::messages::backend;
use crate::messages::frontend::{self, StartupPacket};
use crate::session::{self, SessionConfig};

pub async fn serve<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = Arc::clone(&engine);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, engine, config).await {
                tracing::debug!("connection from {peer} ended: {e}");
            }
        });
    }
}

async fn handle_conn<E: Engine>(
    mut stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        match frontend::decode_startup(&mut buf) {
            Ok(Some(StartupPacket::SslRequest)) | Ok(Some(StartupPacket::GssEncRequest)) => {
                // TLS task upgrades the SslRequest arm; until then: not supported.
                stream.write_all(b"N").await?;
            }
            Ok(Some(StartupPacket::CancelRequest { .. })) => {
                // Cancellation task wires this to the registry; protocol says
                // close without responding either way.
                return Ok(());
            }
            Ok(Some(StartupPacket::Startup { params })) => {
                return session::run_session(stream, params, engine, config).await;
            }
            Ok(None) => {
                if stream.read_buf(&mut buf).await? == 0 {
                    return Ok(());
                }
            }
            Err(e) => {
                let mut out = BytesMut::new();
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(());
            }
        }
    }
}
```

- [ ] **Step 5: Run the integration tests**

Run: `cargo test -p pgwire --test simple_query`
Expected: 4 passed. If `connect` hangs, the usual culprits are a missing `ReadyForQuery` after the parameter block or a length miscount in `backend_key_data`.

- [ ] **Step 6: Run the full suite and clippy**

Run: `cargo test -p pgwire && cargo clippy -p pgwire --all-targets -- -D warnings`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): session state machine with trust auth and simple query protocol"
```

---

### Task 8: `crabgresql` binary + psql smoke test

**Files:**
- Modify: `crates/crabgresql/src/main.rs`
- Create: `scripts/psql-smoke.sh`

- [ ] **Step 1: Implement the binary**

Replace `crates/crabgresql/src/main.rs`:

```rust
use std::sync::Arc;

use clap::Parser;
use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;

/// crabgresql node binary. SP1: serves the stub engine.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!("crabgresql listening on {}", args.listen);
    pgwire::server::serve(listener, Arc::new(StubEngine::new()), Arc::new(SessionConfig::trust()))
        .await
}
```

- [ ] **Step 2: Verify it runs**

Run: `cargo run -p crabgresql &` then `sleep 1`
Expected log line: `crabgresql listening on 127.0.0.1:5433`

- [ ] **Step 3: Add the smoke script**

`scripts/psql-smoke.sh` (make executable: `chmod +x`):

```bash
#!/usr/bin/env bash
# End-to-end check with a real psql client. sslmode=prefer exercises the
# SSLRequest -> 'N' -> plaintext fallback path.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    exit 0
fi

PORT="${1:-54333}"
cargo build -p crabgresql
./target/debug/crabgresql --listen "127.0.0.1:${PORT}" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
sleep 1

out=$(psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1')
if [ "$out" = "1" ]; then
    echo "PASS: psql SELECT 1 -> ${out}"
else
    echo "FAIL: expected '1', got '${out}'" >&2
    exit 1
fi
```

- [ ] **Step 4: Run the smoke test**

First kill the Step 2 server (`kill %1`), then:
Run: `./scripts/psql-smoke.sh`
Expected: `PASS: psql SELECT 1 -> 1` (or `SKIP` if psql isn't installed locally — CI installs it).

- [ ] **Step 5: Commit**

```bash
git add crates/crabgresql scripts/psql-smoke.sh
git commit -m "feat(crabgresql): node binary serving the stub engine, with psql smoke test"
```

---

### Task 9: Extended query protocol

**Files:**
- Modify: `crates/pgwire/src/session.rs`
- Test: `crates/pgwire/tests/extended_query.rs`

Protocol rules implemented here (PostgreSQL docs "Extended Query"):
- Named prepared statements may not be redefined (`42P05`); the unnamed statement (`""`) is silently replaced.
- Bind referencing an unknown statement → `26000`; Execute on an unknown portal → `34000`; duplicate named portal → `42P03`.
- After an error in the extended phase, **all messages are skipped until Sync** (except Terminate).
- Sync ends the implicit transaction: portals are destroyed, ReadyForQuery is sent.
- Result format codes: zero codes = all text; one code = applies to every column; N codes = per column; anything else is a protocol violation.
- Execute sends DataRows **without** a RowDescription (Describe provides it). `max_rows`/PortalSuspended is a tracked gap (stub result sets are tiny); a portal is always run to completion.

- [ ] **Step 1: Write the failing integration test**

`crates/pgwire/tests/extended_query.rs`:

```rust
use std::sync::Arc;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn prepare_and_query_select_1_binary_format() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres uses Parse/Describe/Bind/Execute and requests BINARY results.
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    let rows = client.query(&stmt, &[]).await.expect("query");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn version_via_extended_protocol() {
    let client = connect(spawn_server().await).await;
    let rows = client.query("SELECT version()", &[]).await.expect("query");
    let v: &str = rows[0].get(0);
    assert!(v.starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn error_skips_until_sync_and_session_recovers() {
    let client = connect(spawn_server().await).await;
    let err = client.query("SELECT * FROM nope", &[]).await.expect_err("must fail");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "0A000");
    // tokio-postgres sends Sync after the failed exchange; a healthy
    // implementation recovers and serves the next query.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn reusing_a_prepared_statement_works() {
    let client = connect(spawn_server().await).await;
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    for _ in 0..3 {
        let rows = client.query(&stmt, &[]).await.expect("query");
        let v: i32 = rows[0].get(0);
        assert_eq!(v, 1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pgwire --test extended_query`
Expected: FAIL — tests error with code `0A000` ("extended query protocol not yet implemented") from the Task 7 placeholder arm.

- [ ] **Step 3: Implement**

In `session.rs`, add the extended-protocol state types and helpers:

```rust
use std::collections::HashMap;

use crate::engine::FieldDescription;

#[derive(Debug, Clone)]
struct Prepared {
    sql: String,
    param_types: Vec<u32>,
    fields: Vec<FieldDescription>,
}

#[derive(Debug, Clone)]
struct Portal {
    sql: String,
    fields: Vec<FieldDescription>,
    /// One resolved format code (0/1) per column.
    formats: Vec<i16>,
}

#[derive(Debug, Default)]
struct ExtendedState {
    statements: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    /// True after an error in the extended phase: skip messages until Sync.
    failed: bool,
}

fn resolve_formats(requested: &[i16], ncols: usize) -> Result<Vec<i16>, PgError> {
    let validate = |code: i16| -> Result<i16, PgError> {
        if code == 0 || code == 1 {
            Ok(code)
        } else {
            Err(PgError::protocol(format!("invalid format code {code}")))
        }
    };
    match requested.len() {
        0 => Ok(vec![0; ncols]),
        1 => Ok(vec![validate(requested[0])?; ncols]),
        n if n == ncols => requested.iter().map(|&c| validate(c)).collect(),
        n => Err(PgError::protocol(format!(
            "bind message has {n} result formats but query has {ncols} columns"
        ))),
    }
}
```

In `run_session`, declare `let mut ext = ExtendedState::default();` before the loop, and replace the Task 7 placeholder arm (`Parse | Bind | Describe | Execute | Close` → not-implemented error) and the `Sync` arm with:

```rust
            FrontendMessage::Sync => {
                ext.failed = false;
                ext.portals.clear(); // implicit transaction ends at Sync
                backend::ready_for_query(&mut out, TxStatus::Idle);
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Parse { name, sql, param_types } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) = handle_parse(&mut ext, &*engine, name, sql, param_types, &mut out).await {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Bind { portal, statement, param_formats: _, params: _, result_formats } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) = handle_bind(&mut ext, portal, statement, result_formats, &mut out) {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Describe { kind, name } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) = handle_describe(&ext, kind, &name, &mut out) {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Execute { portal, max_rows: _ } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) = handle_execute(&ext, &*engine, &portal, &mut out).await {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Close { kind, name } => {
                if ext.failed {
                    continue;
                }
                // Closing a nonexistent statement/portal is NOT an error.
                match kind {
                    b'S' => { ext.statements.remove(&name); }
                    b'P' => { ext.portals.remove(&name); }
                    _ => {
                        let e = PgError::protocol(format!("invalid close kind {:?}", kind as char));
                        fail_extended(&mut ext, &mut out, &e);
                        stream.write_all(&out).await?;
                        out.clear();
                        continue;
                    }
                }
                backend::close_complete(&mut out);
                stream.write_all(&out).await?;
                out.clear();
            }
```

And the handler functions (free functions in `session.rs`):

```rust
fn fail_extended(ext: &mut ExtendedState, out: &mut BytesMut, e: &PgError) {
    ext.failed = true;
    backend::error_response(out, e);
}

async fn handle_parse<E: Engine>(
    ext: &mut ExtendedState,
    engine: &E,
    name: String,
    sql: String,
    param_types: Vec<u32>,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    if !name.is_empty() && ext.statements.contains_key(&name) {
        return Err(PgError::error(
            sqlstate::DUPLICATE_PREPARED_STATEMENT,
            format!("prepared statement \"{name}\" already exists"),
        ));
    }
    let fields = engine.describe(&sql).await?;
    ext.statements.insert(name, Prepared { sql, param_types, fields });
    backend::parse_complete(out);
    Ok(())
}

fn handle_bind(
    ext: &mut ExtendedState,
    portal: String,
    statement: String,
    result_formats: Vec<i16>,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    let prepared = ext.statements.get(&statement).ok_or_else(|| {
        PgError::error(
            sqlstate::INVALID_SQL_STATEMENT_NAME,
            format!("prepared statement \"{statement}\" does not exist"),
        )
    })?;
    if !portal.is_empty() && ext.portals.contains_key(&portal) {
        return Err(PgError::error(
            sqlstate::DUPLICATE_CURSOR,
            format!("cursor \"{portal}\" already exists"),
        ));
    }
    let formats = resolve_formats(&result_formats, prepared.fields.len())?;
    ext.portals.insert(
        portal,
        Portal { sql: prepared.sql.clone(), fields: prepared.fields.clone(), formats },
    );
    backend::bind_complete(out);
    Ok(())
}

fn handle_describe(
    ext: &ExtendedState,
    kind: u8,
    name: &str,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    match kind {
        b'S' => {
            let prepared = ext.statements.get(name).ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{name}\" does not exist"),
                )
            })?;
            backend::parameter_description(out, &prepared.param_types);
            if prepared.fields.is_empty() {
                backend::no_data(out);
            } else {
                backend::row_description(out, &prepared.fields);
            }
        }
        b'P' => {
            let portal = ext.portals.get(name).ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_CURSOR_NAME,
                    format!("portal \"{name}\" does not exist"),
                )
            })?;
            if portal.fields.is_empty() {
                backend::no_data(out);
            } else {
                // Describe(portal) reports the formats the portal will use.
                let fields: Vec<FieldDescription> = portal
                    .fields
                    .iter()
                    .zip(&portal.formats)
                    .map(|(f, &format)| FieldDescription { format, ..f.clone() })
                    .collect();
                backend::row_description(out, &fields);
            }
        }
        other => return Err(PgError::protocol(format!("invalid describe kind {:?}", other as char))),
    }
    Ok(())
}

async fn handle_execute<E: Engine>(
    ext: &ExtendedState,
    engine: &E,
    portal_name: &str,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    let portal = ext.portals.get(portal_name).ok_or_else(|| {
        PgError::error(
            sqlstate::INVALID_CURSOR_NAME,
            format!("portal \"{portal_name}\" does not exist"),
        )
    })?;
    let results = engine.simple_query(&portal.sql).await?;
    // Extended protocol carries exactly one statement per Parse.
    match results.first() {
        Some(QueryResult::Rows { rows, tag, .. }) => {
            for row in rows {
                let values: Vec<Option<Bytes>> = row
                    .iter()
                    .zip(&portal.formats)
                    .map(|(cell, &format)| {
                        cell.as_ref().map(|c| {
                            if format == 1 { c.binary.clone() } else { c.text.clone() }
                        })
                    })
                    .collect();
                backend::data_row(out, &values);
            }
            backend::command_complete(out, tag);
        }
        Some(QueryResult::Command { tag }) => backend::command_complete(out, tag),
        Some(QueryResult::Empty) | None => backend::empty_query_response(out),
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p pgwire --test extended_query && cargo test -p pgwire --test simple_query`
Expected: all pass (simple_query must not regress).

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p pgwire --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): extended query protocol (Parse/Bind/Describe/Execute/Close/Sync)"
```

---

### Task 10: Query cancellation

**Files:**
- Modify: `crates/pgwire/src/server.rs`, `crates/pgwire/src/session.rs`
- Test: `crates/pgwire/tests/cancel.rs`

Design: a `CancelRegistry` maps `(process_id, secret_key)` → a replaceable `CancellationToken` slot. Each session registers itself at startup (pid from a global counter, secret from `rand`), installs a **fresh** token at the start of every query (a fired token must not cancel subsequent queries), and unregisters on drop. A `CancelRequest` connection looks up the slot and fires the current token. PostgreSQL semantics: the cancel connection gets no reply; cancellation is best-effort; a cancelled query returns SQLSTATE `57014`.

- [ ] **Step 1: Write the failing integration test**

`crates/pgwire/tests/cancel.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn cancel_request_interrupts_running_query() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);

    // tokio-postgres implements CancelRequest from the BackendKeyData we sent.
    let cancel_token = client.cancel_token();
    let query = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(30)").await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel_token.cancel_query(NoTls).await.expect("cancel sent");

    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query must end promptly after cancel")
        .expect("join");
    let err = result.expect_err("query must be cancelled");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "57014");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pgwire --test cancel`
Expected: FAIL — times out / cancel has no effect (placeholder `backend_key_data(0,0)` and ignored CancelRequest).

- [ ] **Step 3: Implement the registry in `server.rs`**

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use rand::Rng;
use tokio_util::sync::CancellationToken;

static NEXT_PID: AtomicI32 = AtomicI32::new(1);

/// Maps (process_id, secret_key) -> the running query's cancellation token.
/// The token slot is REPLACED at each query start so a fired token never
/// cancels a later query.
#[derive(Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), Arc<Mutex<CancellationToken>>>>,
}

impl CancelRegistry {
    /// Registers a new session; returns its key data and token slot.
    /// The returned guard unregisters on drop.
    pub fn register(self: &Arc<Self>) -> SessionCancel {
        let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
        let secret = rand::rng().random::<i32>();
        let slot = Arc::new(Mutex::new(CancellationToken::new()));
        self.sessions
            .lock()
            .expect("registry lock")
            .insert((pid, secret), Arc::clone(&slot));
        SessionCancel { pid, secret, slot, registry: Arc::clone(self) }
    }

    pub fn cancel(&self, pid: i32, secret: i32) {
        if let Some(slot) = self.sessions.lock().expect("registry lock").get(&(pid, secret)) {
            slot.lock().expect("slot lock").cancel();
        }
        // Wrong key: silently ignore, like PostgreSQL.
    }
}

pub struct SessionCancel {
    pub pid: i32,
    pub secret: i32,
    slot: Arc<Mutex<CancellationToken>>,
    registry: Arc<CancelRegistry>,
}

impl SessionCancel {
    /// Installs and returns a fresh token for one query execution.
    pub fn begin_query(&self) -> CancellationToken {
        let fresh = CancellationToken::new();
        *self.slot.lock().expect("slot lock") = fresh.clone();
        fresh
    }
}

impl Drop for SessionCancel {
    fn drop(&mut self) {
        self.registry
            .sessions
            .lock()
            .expect("registry lock")
            .remove(&(self.pid, self.secret));
    }
}
```

Wire it into `serve`/`handle_conn`: `serve` creates `let registry = Arc::new(CancelRegistry::default());` before the accept loop and passes a clone to each `handle_conn`. In `handle_conn`:

```rust
            Ok(Some(StartupPacket::CancelRequest { process_id, secret_key })) => {
                registry.cancel(process_id, secret_key);
                return Ok(()); // no response, per protocol
            }
            Ok(Some(StartupPacket::Startup { params })) => {
                let cancel = registry.register();
                return session::run_session(stream, params, engine, config, cancel).await;
            }
```

Add `pub use tokio_util::sync::CancellationToken;` is unnecessary — `pgwire` already depends on tokio-util (Task 1).

- [ ] **Step 4: Wire cancellation into `session.rs`**

`run_session` gains the parameter `cancel: crate::server::SessionCancel`. Replace the placeholder key data write with:

```rust
    backend::backend_key_data(&mut out, cancel.pid, cancel.secret);
```

Wrap both engine call sites (`Query` arm and `handle_execute`) in a select. For the `Query` arm:

```rust
            FrontendMessage::Query { sql } => {
                let token = cancel.begin_query();
                let outcome = tokio::select! {
                    r = engine.simple_query(&sql) => r,
                    _ = token.cancelled() => Err(PgError::error(
                        sqlstate::QUERY_CANCELED,
                        "canceling statement due to user request",
                    )),
                };
                match outcome {
                    Ok(results) => write_results(&mut out, &results),
                    Err(e) => {
                        backend::error_response(&mut out, &e);
                        if e.severity == Severity::Fatal {
                            stream.write_all(&out).await?;
                            return Ok(());
                        }
                    }
                }
                backend::ready_for_query(&mut out, TxStatus::Idle);
                stream.write_all(&out).await?;
                out.clear();
            }
```

`handle_execute` gains a `token: CancellationToken` parameter and uses the same `tokio::select!` around `engine.simple_query`; the `Execute` arm calls `cancel.begin_query()` and passes the token in.

- [ ] **Step 5: Run tests**

Run: `cargo test -p pgwire`
Expected: cancel test passes; simple_query and extended_query suites still green.

- [ ] **Step 6: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): query cancellation via CancelRequest and per-query tokens"
```

---

### Task 11: SCRAM-SHA-256 authentication

**Files:**
- Create: `crates/pgwire/src/scram.rs`
- Modify: `crates/pgwire/src/lib.rs` (add `pub mod scram;`), `crates/pgwire/src/session.rs`
- Test: `crates/pgwire/tests/scram_auth.rs`

SCRAM (RFC 5802/7677) server side, built on RustCrypto. The exchange: client-first (`n,,n=...,r=<cnonce>`) → server-first (`r=<cnonce><snonce>,s=<salt b64>,i=<iters>`) → client-final (`c=<b64 gs2>,r=<full nonce>,p=<proof>`) → server-final (`v=<server signature>`). We advertise only `SCRAM-SHA-256` (not `-PLUS`), so the client's gs2 header is `n,,` (no channel binding) or `y,,` (client supports it, server doesn't); both are legal and change the `c=` check (`biws` / `eSws`).

- [ ] **Step 1: Write the failing unit test (RFC 7677 test vector)**

`crates/pgwire/src/scram.rs` (test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as B64;

    /// The exact exchange from RFC 7677 §3 (user "user", password "pencil").
    #[test]
    fn rfc_7677_test_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );

        let server_first = server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        assert_eq!(
            server_first,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".to_vec()
        );

        let server_final = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect("proof verifies");
        assert_eq!(server_final, b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec());
    }

    #[test]
    fn wrong_password_proof_is_rejected() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "not-pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );
        server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        let err = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD);
    }

    #[test]
    fn channel_binding_gs2_header_y_is_accepted() {
        // tokio-postgres over plaintext sends "y,," when the server doesn't
        // advertise -PLUS; c= must then be base64("y,,") = "eSws".
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server =
            ScramServer::new_with("pw", salt, 4096, "SNONCE".into());
        let first = server.handle_client_first(b"y,,n=user,r=CNONCE").expect("ok");
        assert!(first.starts_with(b"r=CNONCESNONCE,"));
        // (proof verification for this case is covered by the integration test)
    }

    #[test]
    fn requested_channel_binding_without_plus_is_rejected() {
        let mut server = ScramServer::new_with("pw", vec![0; 16], 4096, "S".into());
        let err = server
            .handle_client_first(b"p=tls-server-end-point,,n=user,r=CNONCE")
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pgwire -- scram::`
Expected: COMPILE FAIL — `ScramServer` not found.

- [ ] **Step 3: Implement `ScramServer`**

Top of `crates/pgwire/src/scram.rs`:

```rust
//! Server-side SCRAM-SHA-256 (RFC 5802/7677), on RustCrypto primitives.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use rand::Rng;
use rand::distr::Alphanumeric;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{PgError, sqlstate};

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_ITERATIONS: u32 = 4096; // PostgreSQL's default

enum State {
    Initial,
    SentServerFirst {
        client_first_bare: String,
        server_first: String,
        full_nonce: String,
        gs2_header: &'static str,
    },
    Done,
}

pub struct ScramServer {
    password: String,
    salt: Vec<u8>,
    iterations: u32,
    server_nonce: String,
    state: State,
}

impl ScramServer {
    pub fn new(password: &str) -> Self {
        let salt: [u8; 16] = rand::rng().random();
        let server_nonce: String =
            rand::rng().sample_iter(&Alphanumeric).take(24).map(char::from).collect();
        Self::new_with(password, salt.to_vec(), DEFAULT_ITERATIONS, server_nonce)
    }

    /// Deterministic constructor for tests.
    pub fn new_with(password: &str, salt: Vec<u8>, iterations: u32, server_nonce: String) -> Self {
        Self {
            password: password.to_string(),
            salt,
            iterations,
            server_nonce,
            state: State::Initial,
        }
    }

    pub fn handle_client_first(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        if !matches!(self.state, State::Initial) {
            return Err(PgError::protocol("SCRAM: unexpected client-first message"));
        }
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-first is not UTF-8"))?;

        // gs2 header: we never advertise SCRAM-SHA-256-PLUS, so requiring
        // channel binding ("p=...") is a protocol violation.
        let (gs2_header, bare) = if let Some(rest) = msg.strip_prefix("n,,") {
            ("n,,", rest)
        } else if let Some(rest) = msg.strip_prefix("y,,") {
            ("y,,", rest)
        } else {
            return Err(PgError::protocol(
                "SCRAM: unsupported gs2 header (channel binding not offered)",
            ));
        };

        let client_nonce = attr(bare, 'r')?;
        let full_nonce = format!("{client_nonce}{}", self.server_nonce);
        let server_first =
            format!("r={full_nonce},s={},i={}", B64.encode(&self.salt), self.iterations);

        self.state = State::SentServerFirst {
            client_first_bare: bare.to_string(),
            server_first: server_first.clone(),
            full_nonce,
            gs2_header,
        };
        Ok(server_first.into_bytes())
    }

    pub fn handle_client_final(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        let State::SentServerFirst { client_first_bare, server_first, full_nonce, gs2_header } =
            std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(PgError::protocol("SCRAM: unexpected client-final message"));
        };
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-final is not UTF-8"))?;

        let channel = attr(msg, 'c')?;
        if channel != B64.encode(gs2_header) {
            return Err(PgError::protocol("SCRAM: channel binding data mismatch"));
        }
        if attr(msg, 'r')? != full_nonce {
            return Err(PgError::protocol("SCRAM: nonce mismatch"));
        }
        let proof = B64
            .decode(attr(msg, 'p')?)
            .map_err(|_| PgError::protocol("SCRAM: proof is not valid base64"))?;
        let without_proof = msg
            .rsplit_once(",p=")
            .map(|(head, _)| head)
            .ok_or_else(|| PgError::protocol("SCRAM: missing proof"))?;

        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            &self.salt,
            self.iterations,
            &mut salted,
        );
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(client_key);
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let client_signature = hmac(&stored_key, auth_message.as_bytes());

        if proof.len() != 32 {
            return Err(PgError::fatal(sqlstate::INVALID_PASSWORD, "password authentication failed"));
        }
        let recovered_key: Vec<u8> =
            proof.iter().zip(client_signature.iter()).map(|(p, s)| p ^ s).collect();
        let ok: bool = Sha256::digest(&recovered_key).ct_eq(&stored_key).into();
        if !ok {
            return Err(PgError::fatal(sqlstate::INVALID_PASSWORD, "password authentication failed"));
        }

        let server_key = hmac(&salted, b"Server Key");
        let server_signature = hmac(&server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)).into_bytes())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Extracts the value of a comma-separated `x=value` attribute.
fn attr(msg: &str, name: char) -> Result<&str, PgError> {
    msg.split(',')
        .find_map(|part| part.strip_prefix(name).and_then(|p| p.strip_prefix('=')))
        .ok_or_else(|| PgError::protocol(format!("SCRAM: missing attribute '{name}'")))
}
```

- [ ] **Step 4: Run unit tests**

Run: `cargo test -p pgwire -- scram::`
Expected: 4 passed.

- [ ] **Step 5: Write the failing integration test**

`crates/pgwire/tests/scram_auth.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use pgwire::session::{AuthMode, SessionConfig};
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn_scram_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut users = HashMap::new();
    users.insert("crab".to_string(), "hunter2".to_string());
    let config = SessionConfig {
        auth: AuthMode::ScramSha256 { users },
        ..SessionConfig::trust()
    };
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(config),
    ));
    port
}

#[tokio::test]
async fn correct_password_authenticates_and_queries() {
    let port = spawn_scram_server().await;
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password("hunter2")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("scram connect");
    tokio::spawn(conn);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn wrong_password_is_rejected_with_28p01() {
    let port = spawn_scram_server().await;
    let err = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password("wrong")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "28P01");
}

#[tokio::test]
async fn unknown_user_is_rejected() {
    let port = spawn_scram_server().await;
    let result = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("mallory")
        .password("whatever")
        .dbname("crab")
        .connect(NoTls)
        .await;
    assert!(result.is_err());
}
```

- [ ] **Step 6: Wire SCRAM into the session**

In `session.rs`, extend `AuthMode`:

```rust
#[derive(Debug, Clone)]
pub enum AuthMode {
    Trust,
    ScramSha256 { users: std::collections::HashMap<String, String> },
}
```

Replace the auth block at the top of `run_session` with a call to a new `authenticate` function, run before parameter announcement:

```rust
    authenticate(&mut stream, &startup_params, &config, &mut out, &mut inbuf).await?
```

(`run_session` now declares `inbuf` before authentication and the `_startup_params` parameter is renamed to `startup_params`.) `authenticate` returns `std::io::Result<bool>` — `false` means auth failed and the error was already sent; `run_session` then returns `Ok(())`:

```rust
/// Runs the authentication exchange. Returns Ok(false) if the client failed
/// authentication (error already written to the stream).
async fn authenticate<S>(
    stream: &mut S,
    startup_params: &[(String, String)],
    config: &SessionConfig,
    out: &mut BytesMut,
    inbuf: &mut BytesMut,
) -> std::io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match &config.auth {
        AuthMode::Trust => {
            backend::authentication_ok(out);
            Ok(true)
        }
        AuthMode::ScramSha256 { users } => {
            let user = startup_params
                .iter()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            // Unknown user: run no exchange, fail like PostgreSQL does.
            let Some(password) = users.get(user) else {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            };

            backend::authentication_sasl(out, &["SCRAM-SHA-256"]);
            stream.write_all(out).await?;
            out.clear();

            // SASLInitialResponse: mechanism cstring + i32 length + body.
            let Some(mut body) = read_password(stream, inbuf).await? else {
                return Ok(false); // client hung up
            };
            let mechanism = frontend::get_cstr(&mut body).map_err(|_| bad_proto())?;
            if mechanism != "SCRAM-SHA-256" {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let len = frontend::get_i32(&mut body).map_err(|_| bad_proto())?;
            if len < 0 {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let client_first = body;

            let mut scram = crate::scram::ScramServer::new(password);
            let server_first = match scram.handle_client_first(&client_first) {
                Ok(m) => m,
                Err(_) => return send_auth_failure(stream, out, user).await.map(|()| false),
            };
            backend::authentication_sasl_continue(out, &server_first);
            stream.write_all(out).await?;
            out.clear();

            // SASLResponse: raw client-final bytes.
            let Some(client_final) = read_password(stream, inbuf).await? else {
                return Ok(false);
            };
            match scram.handle_client_final(&client_final) {
                Ok(server_final) => {
                    backend::authentication_sasl_final(out, &server_final);
                    backend::authentication_ok(out);
                    Ok(true)
                }
                Err(_) => send_auth_failure(stream, out, user).await.map(|()| false),
            }
        }
    }
}

async fn send_auth_failure<S>(stream: &mut S, out: &mut BytesMut, user: &str) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let e = PgError::fatal(
        sqlstate::INVALID_PASSWORD,
        format!("password authentication failed for user \"{user}\""),
    );
    backend::error_response(out, &e);
    stream.write_all(out).await?;
    out.clear();
    Ok(())
}

fn bad_proto() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed SASL message")
}

/// Reads the next frontend message, expecting Password ('p'); returns its body.
async fn read_password<S>(
    stream: &mut S,
    inbuf: &mut BytesMut,
) -> std::io::Result<Option<Bytes>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        match frontend::decode_message(inbuf) {
            Ok(Some(FrontendMessage::Password(body))) => return Ok(Some(body)),
            Ok(Some(FrontendMessage::Terminate)) | Err(_) => return Ok(None),
            Ok(Some(_)) => return Ok(None), // anything else mid-auth: give up
            Ok(None) => {
                if stream.read_buf(inbuf).await? == 0 {
                    return Ok(None);
                }
            }
        }
    }
}
```

Note: `authenticate` writing `authentication_ok` into `out` without flushing is correct for `Trust` — the caller flushes `out` together with the parameter block, as Task 7 already does. For SCRAM, flush points are explicit above because the client must see each step before responding. `run_session` checks the return: `if !authenticate(...).await? { return Ok(()); }`.

`frontend::get_cstr`/`get_i32` must change visibility from `pub(crate)` to `pub(crate)` — they already are; no change needed.

- [ ] **Step 7: Run all tests**

Run: `cargo test -p pgwire && cargo clippy -p pgwire --all-targets -- -D warnings`
Expected: scram unit + integration tests pass; earlier suites still green.

- [ ] **Step 8: Commit**

```bash
git add crates/pgwire
git commit -m "feat(pgwire): SCRAM-SHA-256 authentication verified against RFC 7677 vectors"
```

---

### Task 12: TLS via rustls + pure-Rust crypto provider

**Files:**
- Create: `scripts/gen-test-cert.sh`, `crates/pgwire/tests/fixtures/` (generated PEMs)
- Modify: `crates/pgwire/src/server.rs`, `crates/crabgresql/src/main.rs`, `scripts/psql-smoke.sh`
- Test: `crates/pgwire/tests/tls.rs`

Design: `pgwire` accepts a ready `tokio_rustls::TlsAcceptor` — building the `rustls::ServerConfig` (and choosing the crypto provider) happens at the binary/test edge, so `pgwire` itself never depends on `rustls-rustcrypto` as a normal dependency. `serve()` gains a `tls: Option<TlsAcceptor>` parameter; **update all existing callers** (4 test files + `crabgresql/src/main.rs`) to pass `None`/`Some`.

- [ ] **Step 1: Generate and commit test certificates**

`scripts/gen-test-cert.sh` (make executable; uses the openssl CLI — a dev tool, not a dependency):

```bash
#!/usr/bin/env bash
# Regenerates the committed test CA + server cert (ECDSA P-256, 10 years).
set -euo pipefail
cd "$(dirname "$0")/../crates/pgwire/tests/fixtures"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout test-ca-key.pem -out test-ca.pem -days 3650 \
    -subj "/CN=crabgresql test CA"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout test-server-key.pem -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA test-ca.pem -CAkey test-ca-key.pem \
    -CAcreateserial -out test-server.pem -days 3650 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
rm server.csr test-ca.srl
echo "wrote test-ca.pem, test-server.pem, test-server-key.pem (+ CA key)"
```

Run it once and commit the PEMs. These are test-only credentials; the private keys being public is intentional and harmless.

- [ ] **Step 2: Write the failing integration test**

`crates/pgwire/tests/tls.rs`:

```rust
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn server_tls() -> TlsAcceptor {
    let certs = rustls_pemfile::certs(&mut &include_bytes!("fixtures/test-server.pem")[..])
        .collect::<Result<Vec<_>, _>>()
        .expect("certs");
    let key = rustls_pemfile::private_key(&mut &include_bytes!("fixtures/test-server-key.pem")[..])
        .expect("read key")
        .expect("a key");
    let provider = Arc::new(rustls_rustcrypto::provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("cert");
    TlsAcceptor::from(Arc::new(config))
}

fn client_tls() -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &include_bytes!("fixtures/test-ca.pem")[..]) {
        roots.add(cert.expect("ca cert")).expect("add root");
    }
    let provider = Arc::new(rustls_rustcrypto::provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

#[tokio::test]
async fn ssl_request_upgrades_to_tls_and_session_works() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve_tls(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
        Some(server_tls()),
    ));

    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");

    // SSLRequest: length 8, code 80877103.
    let mut ssl_request = BytesMut::new();
    ssl_request.put_i32(8);
    ssl_request.put_i32(80_877_103);
    tcp.write_all(&ssl_request).await.expect("write");

    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await.expect("read");
    assert_eq!(answer[0], b'S', "server must accept TLS");

    let domain = rustls::pki_types::ServerName::try_from("localhost").expect("name");
    let mut tls = client_tls().connect(domain, tcp).await.expect("handshake");

    // StartupMessage over TLS: protocol 3.0, user/database params.
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.put_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(body.len() as i32 + 4);
    startup.put_slice(&body);
    tls.write_all(&startup).await.expect("startup");

    // Read until ReadyForQuery ('Z'); must see AuthenticationOk ('R') first.
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = tls.read(&mut buf).await.expect("read");
        assert!(n > 0, "server closed before ReadyForQuery");
        seen.extend_from_slice(&buf[..n]);
        if seen.contains(&b'Z') && seen.first() == Some(&b'R') {
            break;
        }
    }
}
```

Note: this test drives raw protocol bytes on purpose — it pins the exact `SSLRequest → 'S' → handshake → startup` sequence rather than trusting a driver to do it. (Reading until any `'Z'` byte appears is crude but sufficient here: the first backend message must be `R`/AuthenticationOk and ReadyForQuery is the only 'Z'-tagged message.)

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p pgwire --test tls`
Expected: COMPILE FAIL — `serve_tls` not found.

- [ ] **Step 4: Implement TLS upgrade in `server.rs`**

Keep `serve(listener, engine, config)` as a thin wrapper over the new entry point so existing callers stay valid:

```rust
use tokio_rustls::TlsAcceptor;

pub async fn serve<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    serve_tls(listener, engine, config, None).await
}

pub async fn serve_tls<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    tls: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = Arc::clone(&engine);
        let config = Arc::clone(&config);
        let registry = Arc::clone(&registry);
        let tls = tls.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, engine, config, registry, tls).await {
                tracing::debug!("connection from {peer} ended: {e}");
            }
        });
    }
}
```

`handle_conn` keeps the plain-TCP negotiation loop but its `SslRequest` arm becomes:

```rust
            Ok(Some(StartupPacket::SslRequest)) => match tls.take() {
                Some(acceptor) => {
                    stream.write_all(b"S").await?;
                    let tls_stream = acceptor.accept(stream).await?;
                    // After the handshake the startup sequence restarts on the
                    // encrypted stream; `buf` is empty (the client sends nothing
                    // between SSLRequest and the handshake).
                    return startup_loop(tls_stream, buf, engine, config, registry).await;
                }
                None => stream.write_all(b"N").await?,
            },
```

and the remaining arms (GSSENC/Cancel/Startup/None/Err) move into a generic `startup_loop` that `handle_conn` also delegates to for the plaintext path. Concretely, restructure so `handle_conn` only resolves the TLS question, then both paths share:

```rust
async fn startup_loop<S, E>(
    mut stream: S,
    mut buf: BytesMut,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    E: Engine,
{
    loop {
        match frontend::decode_startup(&mut buf) {
            // A second SSLRequest (or one on an already-encrypted stream) is refused.
            Ok(Some(StartupPacket::SslRequest)) | Ok(Some(StartupPacket::GssEncRequest)) => {
                stream.write_all(b"N").await?;
            }
            Ok(Some(StartupPacket::CancelRequest { process_id, secret_key })) => {
                registry.cancel(process_id, secret_key);
                return Ok(());
            }
            Ok(Some(StartupPacket::Startup { params })) => {
                let cancel = registry.register();
                return session::run_session(stream, params, engine, config, cancel).await;
            }
            Ok(None) => {
                if stream.read_buf(&mut buf).await? == 0 {
                    return Ok(());
                }
            }
            Err(e) => {
                let mut out = BytesMut::new();
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(());
            }
        }
    }
}
```

`handle_conn` shrinks to: loop reading until the first complete packet; if it's `SslRequest`, do the upgrade-or-'N' dance; for anything else (or after 'N'), fall through to `startup_loop(stream, buf, ...)` with the already-buffered bytes. The duplication this removes is why the restructure is worth it.

- [ ] **Step 5: Add TLS flags to the binary**

In `crates/crabgresql/src/main.rs` add to `Args`:

```rust
    /// Path to the server certificate chain (PEM). Enables TLS with --tls-key.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<std::path::PathBuf>,
    /// Path to the server private key (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<std::path::PathBuf>,
```

and build the acceptor:

```rust
fn tls_acceptor(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use std::io::{BufReader, Error, ErrorKind};
    let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(key_path)?))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no private key in file"))?;
    let provider = std::sync::Arc::new(rustls_rustcrypto::provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}
```

`main` wires it: `let tls = match (&args.tls_cert, &args.tls_key) { (Some(c), Some(k)) => Some(tls_acceptor(c, k)?), _ => None };` and calls `serve_tls(listener, engine, config, tls)`. Add `tokio-rustls.workspace = true` to `crabgresql`'s dependencies (it was not in Task 1's list for this crate).

- [ ] **Step 6: Extend the psql smoke test**

Append to `scripts/psql-smoke.sh` (before the final exit), a TLS leg that runs only when a cert exists:

```bash
CERT_DIR="crates/pgwire/tests/fixtures"
if [ -f "${CERT_DIR}/test-server.pem" ]; then
    TLS_PORT=$((PORT + 1))
    ./target/debug/crabgresql --listen "127.0.0.1:${TLS_PORT}" \
        --tls-cert "${CERT_DIR}/test-server.pem" \
        --tls-key "${CERT_DIR}/test-server-key.pem" &
    TLS_PID=$!
    trap 'kill "$SERVER_PID" "$TLS_PID" 2>/dev/null || true' EXIT
    sleep 1
    out=$(psql "host=127.0.0.1 port=${TLS_PORT} user=crab dbname=crab sslmode=require" -tAc 'SELECT 1')
    if [ "$out" = "1" ]; then
        echo "PASS: psql over TLS SELECT 1 -> ${out}"
    else
        echo "FAIL (TLS): expected '1', got '${out}'" >&2
        exit 1
    fi
fi
```

- [ ] **Step 7: Run everything**

Run: `cargo test -p pgwire && ./scripts/psql-smoke.sh && cargo clippy --workspace --all-targets -- -D warnings && ./scripts/check-no-native.sh`
Expected: all green — including the no-native check proving rustls-rustcrypto kept the shipped tree pure Rust.

- [ ] **Step 8: Commit**

```bash
git add crates scripts
git commit -m "feat(pgwire): TLS via rustls with pure-Rust crypto provider"
```

---

### Task 13: Golden trace recorder + replay test

**Files:**
- Create: `crates/conformance/src/bin/record.rs`, `scripts/oracle-up.sh`
- Create: `crates/pgwire/tests/fixtures/psql-select1.trace` (recorded in Step 3)
- Test: `crates/pgwire/tests/golden_trace.rs`

Purpose: pin our decoder against bytes produced by a *real* libpq client talking to a *real* server, not just bytes we synthesized ourselves. Trace format: one line per read, `F <hex>` (frontend→backend) or `B <hex>` (backend→frontend), lowercase hex, no separators.

- [ ] **Step 1: Implement the recording proxy**

`scripts/oracle-up.sh` (make executable):

```bash
#!/usr/bin/env bash
# Starts the PostgreSQL 18 test oracle (trust auth, port 54320).
set -euo pipefail
docker rm -f crabgresql-oracle 2>/dev/null || true
docker run -d --name crabgresql-oracle \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 54320:5432 postgres:18
echo "oracle on 127.0.0.1:54320 (user=postgres dbname=postgres)"
```

`crates/conformance/src/bin/record.rs`:

```rust
//! TCP proxy that records PostgreSQL wire traffic to a trace file.
//! Usage: record --listen 127.0.0.1:54329 --upstream 127.0.0.1:54320 --out psql.trace

use std::fmt::Write as _;
use std::sync::Arc;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    listen: String,
    #[arg(long)]
    upstream: String,
    #[arg(long)]
    out: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!("recording {} -> {} into {}", args.listen, args.upstream, args.out.display());
    let (client, _) = listener.accept().await?;
    let upstream = TcpStream::connect(&args.upstream).await?;
    let log = Arc::new(Mutex::new(String::new()));

    let (mut cr, mut cw) = client.into_split();
    let (mut ur, mut uw) = upstream.into_split();

    let log_f = Arc::clone(&log);
    let frontend = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = cr.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            append(&log_f, 'F', &buf[..n]).await;
            uw.write_all(&buf[..n]).await?;
        }
    });
    let log_b = Arc::clone(&log);
    let backend = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = ur.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            append(&log_b, 'B', &buf[..n]).await;
            cw.write_all(&buf[..n]).await?;
        }
    });
    let _ = frontend.await;
    let _ = backend.await;
    std::fs::write(&args.out, log.lock().await.as_str())?;
    eprintln!("wrote {}", args.out.display());
    Ok(())
}

async fn append(log: &Arc<Mutex<String>>, direction: char, bytes: &[u8]) {
    let mut line = String::with_capacity(bytes.len() * 2 + 3);
    let _ = write!(line, "{direction} ");
    for b in bytes {
        let _ = write!(line, "{b:02x}");
    }
    line.push('\n');
    log.lock().await.push_str(&line);
}
```

- [ ] **Step 2: Verify the proxy builds**

Run: `cargo build -p conformance --bin record`
Expected: builds clean.

- [ ] **Step 3: Record the fixture**

```bash
./scripts/oracle-up.sh && sleep 3
cargo run -p conformance --bin record -- \
    --listen 127.0.0.1:54329 --upstream 127.0.0.1:54320 \
    --out crates/pgwire/tests/fixtures/psql-select1.trace &
sleep 1
psql "host=127.0.0.1 port=54329 user=postgres dbname=postgres sslmode=disable" -c 'SELECT 1'
wait
```

Expected: psql prints the `?column? = 1` result; the trace file exists and its first line starts with `F 00000041` or similar (startup packet — `sslmode=disable` keeps SSLRequest out of the trace so the replay test stays simple). Inspect: `head -c 200 crates/pgwire/tests/fixtures/psql-select1.trace`.

- [ ] **Step 4: Write the replay test**

`crates/pgwire/tests/golden_trace.rs`:

```rust
//! Replays frontend bytes recorded from a real psql/libpq session through our
//! decoder. If libpq frames something we can't parse, this catches it.

use bytes::BytesMut;
use pgwire::messages::frontend::{self, FrontendMessage, StartupPacket};

fn frontend_bytes(trace: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for line in trace.lines() {
        if let Some(hex) = line.strip_prefix("F ") {
            for i in (0..hex.len()).step_by(2) {
                out.push(u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"));
            }
        }
    }
    out
}

#[test]
fn psql_select1_trace_decodes_cleanly() {
    let trace = include_str!("fixtures/psql-select1.trace");
    let mut buf = BytesMut::from(&frontend_bytes(trace)[..]);

    // Phase 1: startup packet (sslmode=disable -> plain StartupMessage first).
    let startup = frontend::decode_startup(&mut buf).expect("valid startup").expect("complete");
    let StartupPacket::Startup { params } = startup else {
        panic!("expected StartupMessage first, got {startup:?}");
    };
    assert!(params.iter().any(|(k, v)| k == "user" && v == "postgres"));

    // Phase 2: every remaining tagged message must decode.
    let mut decoded = Vec::new();
    while !buf.is_empty() {
        match frontend::decode_message(&mut buf).expect("valid message") {
            Some(msg) => decoded.push(msg),
            None => panic!("trace ends mid-message: {} bytes left", buf.len()),
        }
    }
    assert!(
        decoded.iter().any(|m| matches!(m, FrontendMessage::Query { sql } if sql == "SELECT 1")),
        "expected the SELECT 1 query in {decoded:?}"
    );
    assert!(decoded.iter().any(|m| matches!(m, FrontendMessage::Terminate)));
}
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p pgwire --test golden_trace`
Expected: PASS. (If psql sent something we don't decode yet, that's the test doing its job — extend the decoder, don't trim the trace.)

- [ ] **Step 6: Commit**

```bash
git add crates/conformance crates/pgwire/tests scripts/oracle-up.sh
git commit -m "test(pgwire): golden trace recorder and psql replay fixture"
```

---

### Task 14: Conformance differential runner

**Files:**
- Modify: `crates/conformance/src/lib.rs`, `crates/conformance/src/main.rs`
- Create: `crates/conformance/corpus/smoke.sql`

- [ ] **Step 1: Write the failing unit tests (splitter + diff)**

`crates/conformance/src/lib.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_statements_on_semicolons_respecting_quotes_and_comments() {
        let sql = "SELECT 1;\n-- a comment; with a semicolon\nSELECT 'a;b';\nSELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 1", "SELECT 'a;b'", "SELECT 2"]
        );
    }

    #[test]
    fn identical_outcomes_match() {
        let a = QueryOutcome {
            rows: vec![vec![Some("1".into())]],
            error_code: None,
        };
        assert!(diff(&a, &a.clone()).matched);
    }

    #[test]
    fn differing_rows_mismatch_with_detail() {
        let oracle = QueryOutcome { rows: vec![vec![Some("1".into())]], error_code: None };
        let subject = QueryOutcome { rows: vec![vec![Some("2".into())]], error_code: None };
        let d = diff(&oracle, &subject);
        assert!(!d.matched);
        assert!(d.detail.contains("rows"));
    }

    #[test]
    fn matching_error_codes_match() {
        // Same SQLSTATE on both sides counts as parity (e.g. both reject).
        let a = QueryOutcome { rows: vec![], error_code: Some("42601".into()) };
        assert!(diff(&a, &a.clone()).matched);
        let b = QueryOutcome { rows: vec![], error_code: Some("0A000".into()) };
        assert!(!diff(&a, &b).matched);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p conformance`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement the library**

`crates/conformance/src/lib.rs`:

```rust
//! Differential conformance harness: run the same SQL against real PostgreSQL
//! (the oracle) and crabgresql (the subject), diff the outcomes.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryOutcome {
    /// Row values in text format; None = NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// SQLSTATE if the statement errored.
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffResult {
    pub matched: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct CaseResult {
    pub file: String,
    pub sql: String,
    pub matched: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub total: usize,
    pub matched: usize,
    pub parity_percent: f64,
    pub cases: Vec<CaseResult>,
}

impl Report {
    pub fn new(cases: Vec<CaseResult>) -> Self {
        let total = cases.len();
        let matched = cases.iter().filter(|c| c.matched).count();
        let parity_percent =
            if total == 0 { 0.0 } else { matched as f64 * 100.0 / total as f64 };
        Self { total, matched, parity_percent, cases }
    }

    pub fn markdown_summary(&self) -> String {
        let mut md = format!(
            "# crabgresql conformance report\n\n**Parity: {:.1}%** ({} / {} statements match the oracle)\n\n",
            self.parity_percent, self.matched, self.total
        );
        md.push_str("| file | statement | result |\n|---|---|---|\n");
        for c in &self.cases {
            let sql = c.sql.replace('|', "\\|");
            let result = if c.matched { "match".to_string() } else { format!("MISMATCH: {}", c.detail) };
            md.push_str(&format!("| {} | `{}` | {} |\n", c.file, sql, result));
        }
        md
    }
}

pub fn diff(oracle: &QueryOutcome, subject: &QueryOutcome) -> DiffResult {
    if oracle.error_code != subject.error_code {
        return DiffResult {
            matched: false,
            detail: format!(
                "error code: oracle={:?} subject={:?}",
                oracle.error_code, subject.error_code
            ),
        };
    }
    if oracle.rows != subject.rows {
        return DiffResult {
            matched: false,
            detail: format!("rows: oracle={:?} subject={:?}", oracle.rows, subject.rows),
        };
    }
    DiffResult { matched: true, detail: String::new() }
}

/// Executes one statement via the simple query protocol, normalizing the
/// outcome. Errors with no SQLSTATE (I/O, disconnect) map to "XXIO" so they
/// are visible as harness-level failures rather than silently matching.
pub async fn run_one(client: &tokio_postgres::Client, sql: &str) -> QueryOutcome {
    use tokio_postgres::SimpleQueryMessage;
    match client.simple_query(sql).await {
        Ok(messages) => {
            let mut rows = Vec::new();
            for m in messages {
                if let SimpleQueryMessage::Row(row) = m {
                    let mut values = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        values.push(row.get(i).map(|s| s.to_string()));
                    }
                    rows.push(values);
                }
            }
            QueryOutcome { rows, error_code: None }
        }
        Err(e) => QueryOutcome {
            rows: vec![],
            error_code: Some(
                e.as_db_error()
                    .map(|db| db.code().code().to_string())
                    .unwrap_or_else(|| "XXIO".to_string()),
            ),
        },
    }
}

/// Minimal statement splitter: semicolons outside single/double quotes and
/// line comments. Dollar-quoting is NOT handled yet — tracked for the
/// pg_regress import in SP2, which needs it.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(c) = chars.next() {
        if !in_single && !in_double && c == '-' && chars.peek() == Some(&'-') {
            for c2 in chars.by_ref() {
                if c2 == '\n' {
                    break;
                }
            }
            continue;
        }
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ';' if !in_single && !in_double => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }
    statements
}
```

- [ ] **Step 4: Run unit tests**

Run: `cargo test -p conformance`
Expected: 4 passed.

- [ ] **Step 5: Implement the runner binary**

`crates/conformance/src/main.rs`:

```rust
use clap::Parser;
use conformance::{CaseResult, Report, diff, run_one, split_statements};
use tokio_postgres::NoTls;

/// Differential conformance runner: oracle (real PostgreSQL) vs subject (crabgresql).
#[derive(Parser)]
struct Args {
    /// e.g. "host=127.0.0.1 port=54320 user=postgres dbname=postgres"
    #[arg(long)]
    oracle_url: String,
    /// e.g. "host=127.0.0.1 port=5433 user=crab dbname=crab"
    #[arg(long)]
    subject_url: String,
    /// Directory of .sql corpus files.
    #[arg(long, default_value = "crates/conformance/corpus")]
    corpus: std::path::PathBuf,
    #[arg(long, default_value = "parity.json")]
    out: std::path::PathBuf,
    #[arg(long, default_value = "parity.md")]
    summary: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (oracle, oracle_conn) = tokio_postgres::connect(&args.oracle_url, NoTls).await?;
    tokio::spawn(oracle_conn);
    let (subject, subject_conn) = tokio_postgres::connect(&args.subject_url, NoTls).await?;
    tokio::spawn(subject_conn);

    let mut files: Vec<_> = std::fs::read_dir(&args.corpus)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();

    let mut cases = Vec::new();
    for path in files {
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let sql = std::fs::read_to_string(&path)?;
        for stmt in split_statements(&sql) {
            let o = run_one(&oracle, &stmt).await;
            let s = run_one(&subject, &stmt).await;
            let d = diff(&o, &s);
            cases.push(CaseResult { file: name.clone(), sql: stmt, matched: d.matched, detail: d.detail });
        }
    }

    let report = Report::new(cases);
    std::fs::write(&args.out, serde_json::to_string_pretty(&report)?)?;
    std::fs::write(&args.summary, report.markdown_summary())?;
    println!(
        "parity: {:.1}% ({} / {}) -> {} / {}",
        report.parity_percent,
        report.matched,
        report.total,
        args.out.display(),
        args.summary.display()
    );
    Ok(())
}
```

`crates/conformance/corpus/smoke.sql`:

```sql
-- SP1 smoke corpus: the stub's full surface, plus statements the stub is
-- expected to fail (visible mismatches keep the dashboard honest).
SELECT 1;
SELECT version();
SELECT 2;
SELECT 'hello';
SELECT 1 + 1;
```

- [ ] **Step 6: Run the harness end-to-end locally**

```bash
./scripts/oracle-up.sh && sleep 3
cargo run -p crabgresql -- --listen 127.0.0.1:54333 &
cargo run -p conformance -- \
    --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
    --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" \
    --corpus crates/conformance/corpus --out parity.json --summary parity.md
kill %1
cat parity.md
```

Expected: `parity: 20.0% (1 / 5)` — `SELECT 1` matches; `version()` differs textually from the real server; the rest are stub gaps. The number is small and honest; that's the deliverable.

- [ ] **Step 7: Commit**

Add `parity.json` and `parity.md` to `.gitignore` (they're CI artifacts, not source):

```bash
echo -e "parity.json\nparity.md" >> .gitignore
git add crates/conformance .gitignore
git commit -m "feat(conformance): differential runner with parity report"
```

---

### Task 15: Conformance + smoke tests in CI

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add the conformance job**

Append to `.github/workflows/ci.yml`:

```yaml
  conformance:
    runs-on: ubuntu-latest
    services:
      oracle:
        image: postgres:18
        env:
          POSTGRES_HOST_AUTH_METHOD: trust
        ports:
          - 54320:5432
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 5s --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: sudo apt-get update && sudo apt-get install -y postgresql-client
      - run: cargo build -p crabgresql -p conformance
      - run: ./scripts/psql-smoke.sh
      - name: Run conformance harness
        run: |
          ./target/debug/crabgresql --listen 127.0.0.1:54333 &
          sleep 2
          ./target/debug/conformance \
            --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
            --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" \
            --corpus crates/conformance/corpus \
            --out parity.json --summary parity.md
      - run: cat parity.md >> "$GITHUB_STEP_SUMMARY"
      - uses: actions/upload-artifact@v4
        with:
          name: parity-report
          path: |
            parity.json
            parity.md
```

- [ ] **Step 2: Verify the workflow file parses**

Run: `cargo build --workspace && cargo test --workspace` (final full local check)
and if `gh` is authenticated: push a branch and confirm both CI jobs go green, with the parity table visible in the job summary.

- [ ] **Step 3: Commit**

```bash
git add .github
git commit -m "ci: conformance job publishing the parity report"
```

---

## Success criteria traceability (spec → tasks)

| Spec requirement | Task(s) |
|---|---|
| psql connects over TLS with SCRAM, gets `SELECT 1` | 7, 8, 11, 12 |
| tokio-postgres/sqlx-style driver tests pass | 7, 9, 10, 11 |
| Conformance harness in CI publishing parity report | 14, 15 |
| Zero unsafe, pure-Rust shipped tree, enforced | 1 (gates), 12 (provider), every task (CI) |
| Codec never panics on malformed input | 3, 4 (proptest), 13 (golden traces) |
| Tracked gaps (explicitly NOT in SP1) | COPY, replication protocol, GSSAPI, protocol 3.2, PortalSuspended/max_rows, dollar-quoting in the splitter, pg_regress file import (SP2) |

## Notes for the implementer

- sqlx is listed in the spec's success criteria as a representative driver; tokio-postgres exercises the identical wire paths (it is sqlx's postgres protocol ancestor). If you want the literal sqlx test, add it as a dev-dependency integration test mirroring `extended_query.rs` — optional, not a gate.
- Crate versions in Task 1 are floors, not pins; run `cargo update` and let the lockfile settle. If `rustls-rustcrypto`'s API has shifted (it's pre-1.0), the only call sites are the test helpers and `crabgresql::tls_acceptor`.
- Every task ends with the workspace green: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`. Don't carry red between tasks.
