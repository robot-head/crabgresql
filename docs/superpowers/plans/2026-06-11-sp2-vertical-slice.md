# SP2: Vertical Slice (parser → catalog → KV engine → executor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SP1's canned `StubEngine` with a real SQL engine — `CREATE TABLE`/`DROP TABLE`/`INSERT`/`SELECT` flow through a genuine parser → catalog → executor → order-preserving KV pipeline, served over the existing wire protocol; plus three SP1 carry-overs (SCRAM verifier storage, the libpg_query parser oracle, the first pg_regress conformance import).

**Architecture:** Five new `forbid(unsafe_code)` workspace crates with strictly downward dependencies: `pgtypes` (Datum values + wire encodings + operators), `kv` (Kv trait + MemKv + order-preserving key/value encoding — the permanent storage seam), `catalog` (table metadata), `pgparser` (hand-written lexer + recursive-descent/Pratt parser → AST), `executor` (AST execution; implements `pgwire::Engine`). The binary swaps `StubEngine` for `executor::SqlEngine`.

**Tech Stack:** Rust 2024, no new shipped deps (existing: bytes, thiserror, tokio). Dev-only: proptest (encoding properties), tokio-postgres (e2e), and `pg_query` (libpg_query Rust bindings — C dev dependency, exempt from the zero-C shipped-tree rule, gated out of `check-no-native.sh` which only inspects the `crabgresql` binary's tree).

**Spec:** `docs/superpowers/specs/2026-06-11-crabgresql-sp2-vertical-slice-design.md`

---

## File structure

```
Cargo.toml                              # add 5 members + workspace deps (proptest already present)
crates/pgtypes/
  src/lib.rs                            # module wiring + re-exports
  src/datum.rs                          # Datum enum, TypeOid, type-name parsing
  src/error.rs                          # TypeError enum
  src/encoding.rs                       # text + binary wire encodings per type
  src/ops.rs                            # arithmetic / comparison / boolean operators (PG semantics)
crates/kv/
  src/lib.rs
  src/store.rs                          # Kv trait + MemKv
  src/keyenc.rs                         # order-preserving key component encoders
  src/key.rs                            # table/index/rowid key construction
  src/rowenc.rs                         # versioned value (row) encoding of Datums
crates/catalog/
  src/lib.rs                            # Catalog, TableId, Column, ColumnType; CRUD + error codes
crates/pgparser/
  src/lib.rs                            # parse() entry, re-exports
  src/token.rs                          # Token, Keyword
  src/lexer.rs                          # Lexer
  src/ast.rs                            # Statement, Select, Expr, ... AST types
  src/parser.rs                         # recursive-descent + Pratt
  src/error.rs                          # ParseError (carries byte position)
  tests/libpg_query_oracle.rs           # differential test vs libpg_query (dev-only C dep)
crates/executor/
  src/lib.rs                            # SqlEngine (impl pgwire::Engine), module wiring
  src/eval.rs                           # expression evaluator over Datums
  src/exec.rs                           # per-statement execution (DDL/INSERT/SELECT)
  src/error.rs                          # map pgtypes/catalog/kv errors -> pgwire::PgError
  tests/end_to_end.rs                   # tokio-postgres: CREATE/INSERT/SELECT round trips
crates/crabgresql/src/main.rs           # construct executor::SqlEngine; --auth scram verifier config
crates/pgwire/src/scram.rs              # ScramVerifier; verifier-based ScramServer ctor; mock auth
crates/pgwire/src/session.rs            # AuthMode::ScramSha256 { verifiers }; mock-auth path
crates/conformance/src/lib.rs           # dollar-quote-aware split_statements
crates/conformance/corpus/              # vendored pg_regress subsets: int4.sql, boolean.sql, select_basic.sql
```

Dependency order for tasks: SCRAM carry-over (independent) → pgtypes → kv → catalog → pgparser → executor → binary wiring → conformance/pg_regress → CI. Each crate is fully tested before the next depends on it.

---

### Task 1: Workspace — add the five crates

**Files:**
- Modify: `Cargo.toml` (workspace members + deps)
- Create: `crates/{pgtypes,kv,catalog,pgparser,executor}/Cargo.toml` and a placeholder `src/lib.rs` each

- [ ] **Step 1: Add members and shared deps**

In root `Cargo.toml`, change the members line to:

```toml
members = [
    "crates/pgwire",
    "crates/crabgresql",
    "crates/conformance",
    "crates/pgtypes",
    "crates/kv",
    "crates/catalog",
    "crates/pgparser",
    "crates/executor",
]
```

In `[workspace.dependencies]` add path entries (place near the existing `pgwire = { path = ... }` line):

```toml
pgtypes = { path = "crates/pgtypes" }
kv = { path = "crates/kv" }
catalog = { path = "crates/catalog" }
pgparser = { path = "crates/pgparser" }
executor = { path = "crates/executor" }
```

- [ ] **Step 2: Scaffold each crate**

For each of `pgtypes`, `kv`, `catalog`, `pgparser`, `executor`, create `crates/<name>/Cargo.toml`:

```toml
[package]
name = "<name>"
version.workspace = true
edition.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
thiserror.workspace = true

[dev-dependencies]
proptest.workspace = true
```

And `crates/<name>/src/lib.rs`:

```rust
//! <name> crate for crabgresql. See SP2 spec.
```

`pgtypes` needs only `thiserror`. `kv` needs `thiserror` + `proptest` (dev). `catalog` adds `pgtypes.workspace = true`. `pgparser` adds `pgtypes.workspace = true` and (dev) we will add `pg_query` in Task 13 — leave it out for now. `executor` adds `pgtypes`, `kv`, `catalog`, `pgparser`, `pgwire`, and `tokio` + `tokio-postgres` (dev), `bytes` — set its deps:

```toml
[dependencies]
thiserror.workspace = true
pgtypes.workspace = true
kv.workspace = true
catalog.workspace = true
pgparser.workspace = true
pgwire.workspace = true
bytes.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["full"] }
tokio-postgres.workspace = true
```

- [ ] **Step 3: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: all crates compile (empty lib crates), no errors.

- [ ] **Step 4: Verify gates still green**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && ./scripts/check-no-native.sh`
Expected: all pass — the new crates are pure Rust and not in the `crabgresql` binary tree yet.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/pgtypes crates/kv crates/catalog crates/pgparser crates/executor
git commit -m "chore: scaffold pgtypes/kv/catalog/pgparser/executor crates"
```

---

### Task 2: SCRAM verifier storage + mock authentication (carry-over)

**Files:**
- Modify: `crates/pgwire/src/scram.rs` (add `ScramVerifier`; verifier-based `ScramServer`; keep RFC vectors green)
- Modify: `crates/pgwire/src/session.rs` (`AuthMode::ScramSha256 { verifiers, mock_secret }`; mock-auth path)
- Test: unit tests in `scram.rs`; integration `crates/pgwire/tests/scram_auth.rs` (extend)

Closes the SP1 security trio: plaintext at rest, per-connection PBKDF2 (now done once at config time, not per auth), and the username-enumeration oracle (mock auth gives unknown users an identical message sequence).

A SCRAM verifier stores what's needed to verify a proof WITHOUT the password:
`SaltedPassword = PBKDF2-HMAC-SHA256(password, salt, iters)`, then
`StoredKey = SHA256(HMAC(SaltedPassword,"Client Key"))` and
`ServerKey = HMAC(SaltedPassword,"Server Key")`. Verification computes
`ClientSignature = HMAC(StoredKey, AuthMessage)`, recovers
`ClientKey = proof XOR ClientSignature`, and checks
`SHA256(ClientKey) == StoredKey` (constant-time); the server signature is
`HMAC(ServerKey, AuthMessage)`.

- [ ] **Step 1: Write failing unit test for the verifier**

Add to `scram.rs` tests module:

```rust
    #[test]
    fn verifier_from_password_then_verify_roundtrip() {
        let salt = vec![1u8; 16];
        let v = ScramVerifier::from_password("pencil", salt.clone(), 4096);
        assert_eq!(v.salt, salt);
        assert_eq!(v.iterations, 4096);
        // A server built from the verifier must accept the right password's proof.
        // Drive a full exchange with a fixed nonce, computing the client side here.
        let mut server = ScramServer::from_verifier(v.clone(), "SNONCE".into());
        let server_first = server
            .handle_client_first(b"n,,n=user,r=CNONCE")
            .expect("client-first");
        // Build the client-final proof for password "pencil".
        let final_msg = client_final_for(&v, "pencil", "CNONCE", &server_first);
        let server_final = server.handle_client_final(final_msg.as_bytes()).expect("verify");
        assert!(server_final.starts_with(b"v="));
    }

    #[test]
    fn verifier_rejects_wrong_password() {
        let v = ScramVerifier::from_password("pencil", vec![2u8; 16], 4096);
        let mut server = ScramServer::from_verifier(v.clone(), "SNONCE".into());
        let server_first = server.handle_client_first(b"n,,n=user,r=CNONCE").expect("cf");
        let final_msg = client_final_for(&v, "WRONG", "CNONCE", &server_first);
        let err = server.handle_client_final(final_msg.as_bytes()).expect_err("reject");
        assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD);
    }

    // Test helper: compute a client-final message (c=,r=,p=) for a candidate
    // password against the server-first message, mirroring the RFC client side.
    fn client_final_for(v: &ScramVerifier, password: &str, cnonce: &str, server_first: &[u8]) -> String {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as B64;
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        let sf = std::str::from_utf8(server_first).expect("utf8");
        let full_nonce = sf.split(',').find_map(|p| p.strip_prefix("r=")).expect("r");
        let client_first_bare = format!("n=user,r={cnonce}");
        let without_proof = format!("c=biws,r={full_nonce}");
        let auth_message = format!("{client_first_bare},{sf},{without_proof}");
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &v.salt, v.iterations, &mut salted);
        let mut m = Hmac::<Sha256>::new_from_slice(&salted).expect("hmac");
        m.update(b"Client Key");
        let client_key = m.finalize().into_bytes();
        let stored_key = Sha256::digest(client_key);
        let mut ms = Hmac::<Sha256>::new_from_slice(&stored_key).expect("hmac");
        ms.update(auth_message.as_bytes());
        let client_sig = ms.finalize().into_bytes();
        let proof: Vec<u8> = client_key.iter().zip(client_sig.iter()).map(|(k, s)| k ^ s).collect();
        format!("{without_proof},p={}", B64.encode(proof))
    }
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p pgwire -- scram::`
Expected: FAIL — `ScramVerifier`, `from_verifier` not found.

- [ ] **Step 3: Implement `ScramVerifier` and refactor `ScramServer`**

In `scram.rs`, add the verifier type and PBKDF2/HMAC helpers (reuse the existing `hmac()` helper):

```rust
/// Precomputed SCRAM-SHA-256 verifier — stores no plaintext password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramVerifier {
    pub fn from_password(password: &str, salt: Vec<u8>, iterations: u32) -> Self {
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(&client_key).into();
        let server_key: [u8; 32] =
            hmac(&salted, b"Server Key").try_into().expect("hmac-sha256 is 32 bytes");
        Self { salt, iterations, stored_key, server_key }
    }
}
```

Refactor `ScramServer` to hold a `ScramVerifier` instead of `password`/`salt`/`iterations`:

```rust
pub struct ScramServer {
    verifier: ScramVerifier,
    server_nonce: String,
    state: State,
}

impl ScramServer {
    /// Build from a stored verifier (production path).
    pub fn from_verifier(verifier: ScramVerifier, server_nonce: String) -> Self {
        Self { verifier, server_nonce, state: State::Initial }
    }
}
```

Replace the old `new`/`new_with` constructors: keep a test-friendly `new_with(password, salt, iterations, server_nonce)` that derives a verifier so the existing RFC 7677 vector test still compiles and passes:

```rust
    /// Deterministic constructor for tests: derives a verifier from a password.
    pub fn new_with(password: &str, salt: Vec<u8>, iterations: u32, server_nonce: String) -> Self {
        Self::from_verifier(ScramVerifier::from_password(password, salt, iterations), server_nonce)
    }
```

In `handle_client_first` replace `B64.encode(&self.salt)` / `self.iterations` with
`B64.encode(&self.verifier.salt)` / `self.verifier.iterations`.

In `handle_client_final` replace the PBKDF2-from-password block with the stored
keys:

```rust
        let stored_key = self.verifier.stored_key;
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

        let server_signature = hmac(&self.verifier.server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)).into_bytes())
```

The RFC 7677 vector test (`rfc_7677_test_vector`) and `wrong_password_proof_is_rejected` use `new_with`, which now routes through the verifier — they must still pass byte-for-byte.

- [ ] **Step 4: Run unit tests**

Run: `cargo test -p pgwire -- scram::`
Expected: all scram unit tests pass (RFC vector + the two new verifier tests + channel-binding tests).

- [ ] **Step 5: Wire verifiers + mock auth into the session**

In `session.rs`, change `AuthMode`:

```rust
#[derive(Debug, Clone)]
pub enum AuthMode {
    Trust,
    /// SCRAM-SHA-256 against stored verifiers (no plaintext at rest). A server
    /// mock secret derives a deterministic fake verifier for unknown users so
    /// the message sequence and timing match a real user — closing the
    /// username-enumeration oracle (RFC 5802 mock authentication).
    ScramSha256 {
        verifiers: std::collections::HashMap<String, crate::scram::ScramVerifier>,
        mock_secret: [u8; 32],
    },
}
```

In the `authenticate` function's `ScramSha256` arm, replace the unknown-user
early-return with mock authentication: always run the full SASL exchange. For a
known user use their verifier; for an unknown user derive a deterministic mock
verifier and run the exchange (which always fails the proof check, yielding the
same `28P01`):

```rust
        AuthMode::ScramSha256 { verifiers, mock_secret } => {
            let user = startup_params
                .iter()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            let verifier = match verifiers.get(user) {
                Some(v) => v.clone(),
                None => crate::scram::ScramVerifier::mock(mock_secret, user),
            };

            backend::authentication_sasl(out, &["SCRAM-SHA-256"]);
            stream.write_all(out).await?;
            out.clear();

            let Some(mut body) = read_password(stream, inbuf).await? else { return Ok(false) };
            let mechanism = frontend::get_cstr(&mut body).map_err(|_| bad_proto())?;
            if mechanism != "SCRAM-SHA-256" {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let len = frontend::get_i32(&mut body).map_err(|_| bad_proto())?;
            if len < 0 {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let client_first = body;

            let mut scram = crate::scram::ScramServer::from_verifier(verifier, server_nonce());
            let server_first = match scram.handle_client_first(&client_first) {
                Ok(m) => m,
                Err(_) => return send_auth_failure(stream, out, user).await.map(|()| false),
            };
            backend::authentication_sasl_continue(out, &server_first);
            stream.write_all(out).await?;
            out.clear();

            let Some(client_final) = read_password(stream, inbuf).await? else { return Ok(false) };
            match scram.handle_client_final(&client_final) {
                Ok(server_final) => {
                    backend::authentication_sasl_final(out, &server_final);
                    backend::authentication_ok(out);
                    Ok(true)
                }
                Err(_) => send_auth_failure(stream, out, user).await.map(|()| false),
            }
        }
```

Add a `server_nonce()` free function in `session.rs` (24 alphanumeric chars via
`rand`, mirroring the old `ScramServer::new`):

```rust
fn server_nonce() -> String {
    use rand::Rng;
    use rand::distr::Alphanumeric;
    rand::rng().sample_iter(&Alphanumeric).take(24).map(char::from).collect()
}
```

Add `ScramVerifier::mock` in `scram.rs` — a deterministic fake verifier derived
from the server secret and username, so unknown users are indistinguishable
from users with a wrong password:

```rust
impl ScramVerifier {
    /// Deterministic fake verifier for an unknown user (mock authentication).
    /// Salt and keys are HMAC-derived from the server secret so the exchange
    /// is byte-shaped like a real one but no proof can ever satisfy it.
    pub fn mock(server_secret: &[u8; 32], user: &str) -> Self {
        let salt = hmac(server_secret, format!("mock-salt:{user}").as_bytes());
        let stored_key: [u8; 32] =
            hmac(server_secret, format!("mock-stored:{user}").as_bytes())
                .try_into()
                .expect("32 bytes");
        let server_key: [u8; 32] =
            hmac(server_secret, format!("mock-server:{user}").as_bytes())
                .try_into()
                .expect("32 bytes");
        Self { salt, iterations: DEFAULT_ITERATIONS, stored_key, server_key }
    }
}
```

- [ ] **Step 6: Update the integration tests**

In `crates/pgwire/tests/scram_auth.rs`, build the config from verifiers:

```rust
fn scram_config() -> SessionConfig {
    use pgwire::scram::ScramVerifier;
    let mut verifiers = std::collections::HashMap::new();
    verifiers.insert(
        "crab".to_string(),
        ScramVerifier::from_password("hunter2", vec![7u8; 16], 4096),
    );
    SessionConfig {
        auth: AuthMode::ScramSha256 { verifiers, mock_secret: [42u8; 32] },
        ..SessionConfig::trust()
    }
}
```

Replace the `spawn_scram_server` body's config construction with `scram_config()`.
The three existing tests (correct password, wrong password → 28P01, unknown user
rejected) must still pass — unknown-user now runs a full mock exchange and still
fails with 28P01.

- [ ] **Step 7: Fix the binary's SCRAM config (required — the variant changed shape).**

Changing `AuthMode::ScramSha256` from `{ users }` to `{ verifiers, mock_secret }`
breaks the binary's `build_session_config` (SP1 built the old `users` map), so
the workspace won't compile until `main.rs` is updated. Do it now. Add to
`crates/crabgresql/Cargo.toml` `[dependencies]`: `rand.workspace = true` (if not
already present). Replace the `scram` arm of `build_session_config` in
`crates/crabgresql/src/main.rs` with:

```rust
        "scram" => {
            use pgwire::scram::ScramVerifier;
            use rand::Rng;
            if args.user_creds.is_empty() {
                return Err(Error::new(ErrorKind::InvalidInput, "--auth scram requires --user-cred"));
            }
            let mut verifiers = std::collections::HashMap::new();
            for cred in &args.user_creds {
                let (user, pass) = cred
                    .split_once('=')
                    .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "--user-cred must be USER=PASSWORD"))?;
                if user.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidInput, "--user-cred user name is empty"));
                }
                let salt: [u8; 16] = rand::rng().random();
                verifiers.insert(user.to_string(), ScramVerifier::from_password(pass, salt.to_vec(), 4096));
            }
            let mock_secret: [u8; 32] = rand::rng().random();
            Ok(SessionConfig { auth: AuthMode::ScramSha256 { verifiers, mock_secret }, ..SessionConfig::trust() })
        }
```

(Match the surrounding `match args.auth.as_str()` / `Error`/`ErrorKind` imports
already in `main.rs`.)

- [ ] **Step 8: Run pgwire suite + workspace build + gauntlet**

Run: `cargo build --workspace && cargo test -p pgwire && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all green — the workspace compiles (binary included), RFC vectors and
the three scram integration tests pass.

- [ ] **Step 9: Confirm TLS+SCRAM still works end-to-end**

Run: `./scripts/psql-smoke.sh`
Expected: all three legs PASS — the TLS+SCRAM leg now authenticates against a
stored verifier rather than a plaintext password.

- [ ] **Step 10: Commit**

```bash
git add crates/pgwire crates/crabgresql Cargo.toml Cargo.lock
git commit -m "feat(pgwire): SCRAM verifier storage with mock authentication (closes SP1 security trio)"
```

Task 19 reuses this `build_session_config` unchanged — it only swaps the engine
from `StubEngine` to `SqlEngine`.

---

### Task 3: pgtypes — Datum, type OIDs, ColumnType, errors

**Files:**
- Create: `crates/pgtypes/src/datum.rs`, `crates/pgtypes/src/error.rs`
- Modify: `crates/pgtypes/src/lib.rs`

- [ ] **Step 1: Failing tests** — `crates/pgtypes/src/datum.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_from_sql_names_and_aliases() {
        assert_eq!(ColumnType::from_sql_name("int4"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("integer"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("INT"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("int8"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("bigint"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("text"), Some(ColumnType::Text));
        assert_eq!(ColumnType::from_sql_name("bool"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("boolean"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("widget"), None);
    }

    #[test]
    fn column_type_oids_match_postgres() {
        assert_eq!(ColumnType::Bool.oid(), 16);
        assert_eq!(ColumnType::Int8.oid(), 20);
        assert_eq!(ColumnType::Int4.oid(), 23);
        assert_eq!(ColumnType::Text.oid(), 25);
    }

    #[test]
    fn datum_reports_its_column_type() {
        assert_eq!(Datum::Int4(1).column_type(), Some(ColumnType::Int4));
        assert_eq!(Datum::Null.column_type(), None);
    }
}
```

- [ ] **Step 2:** `cargo test -p pgtypes` → COMPILE FAIL.

- [ ] **Step 3: Implement** `datum.rs`:

```rust
//! The runtime value type and the SQL column types of the SP2 slice.

/// PostgreSQL type OIDs (from pg_type.dat) for the slice's types.
pub mod oids {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
}

/// A SQL column type in the SP2 slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int4,
    Int8,
    Text,
}

impl ColumnType {
    pub fn from_sql_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "int4" | "integer" | "int" => Some(ColumnType::Int4),
            "int8" | "bigint" => Some(ColumnType::Int8),
            "text" => Some(ColumnType::Text),
            "bool" | "boolean" => Some(ColumnType::Bool),
            _ => None,
        }
    }

    pub fn oid(self) -> u32 {
        match self {
            ColumnType::Bool => oids::BOOL,
            ColumnType::Int8 => oids::INT8,
            ColumnType::Int4 => oids::INT4,
            ColumnType::Text => oids::TEXT,
        }
    }

    /// PostgreSQL type name (for error messages and FieldDescription debugging).
    pub fn name(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int8 => "bigint",
            ColumnType::Int4 => "integer",
            ColumnType::Text => "text",
        }
    }

    /// pg_type.typlen: fixed sizes, -1 for variable-length text.
    pub fn type_size(self) -> i16 {
        match self {
            ColumnType::Bool => 1,
            ColumnType::Int8 => 8,
            ColumnType::Int4 => 4,
            ColumnType::Text => -1,
        }
    }
}

/// A runtime value.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
}

impl Datum {
    /// The non-null column type of this value (None for NULL).
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(ColumnType::Bool),
            Datum::Int4(_) => Some(ColumnType::Int4),
            Datum::Int8(_) => Some(ColumnType::Int8),
            Datum::Text(_) => Some(ColumnType::Text),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
}
```

`error.rs`:

```rust
//! Errors from the type layer, each carrying the PostgreSQL SQLSTATE the
//! executor maps onto a wire ErrorResponse.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    #[error("integer out of range")]
    Overflow,
    #[error("division by zero")]
    DivisionByZero,
    #[error("invalid input syntax for type {type_name}: \"{value}\"")]
    InvalidText { type_name: &'static str, value: String },
    #[error("{message}")]
    TypeMismatch { message: String },
}

impl TypeError {
    /// The five-character SQLSTATE for this error.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TypeError::Overflow => "22003",
            TypeError::DivisionByZero => "22012",
            TypeError::InvalidText { .. } => "22P02",
            TypeError::TypeMismatch { .. } => "42804",
        }
    }
}
```

`lib.rs`:

```rust
//! pgtypes: the value layer for crabgresql — Datum, column types, wire
//! encodings, and operator semantics matching PostgreSQL.

pub mod datum;
pub mod encoding;
pub mod error;
pub mod ops;

pub use datum::{ColumnType, Datum, oids};
pub use error::TypeError;
```

(`encoding` and `ops` modules are added in Tasks 4 and 5; create empty
`encoding.rs`/`ops.rs` with `//!` doc lines now so `lib.rs` compiles, or add the
`pub mod` lines in those tasks. To keep this task self-contained, create
`crates/pgtypes/src/encoding.rs` and `crates/pgtypes/src/ops.rs` each containing
only `//! Filled in by a later task.` and reference them from lib.rs.)

- [ ] **Step 4:** `cargo test -p pgtypes` → 3 passed.

- [ ] **Step 5:** `cargo fmt --all && cargo clippy -p pgtypes --all-targets -- -D warnings`, then:

```bash
git add crates/pgtypes
git commit -m "feat(pgtypes): Datum, ColumnType with PG OIDs, TypeError"
```

---

### Task 4: pgtypes — text + binary wire encodings

**Files:**
- Modify: `crates/pgtypes/src/encoding.rs`

- [ ] **Step 1: Failing tests** — `encoding.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Datum;

    #[test]
    fn text_encoding_matches_postgres() {
        assert_eq!(encode_text(&Datum::Bool(true)), b"t");
        assert_eq!(encode_text(&Datum::Bool(false)), b"f");
        assert_eq!(encode_text(&Datum::Int4(-5)), b"-5");
        assert_eq!(encode_text(&Datum::Int8(9_000_000_000)), b"9000000000");
        assert_eq!(encode_text(&Datum::Text("hi".into())), b"hi");
    }

    #[test]
    fn binary_encoding_is_network_order() {
        assert_eq!(encode_binary(&Datum::Bool(true)), vec![1]);
        assert_eq!(encode_binary(&Datum::Bool(false)), vec![0]);
        assert_eq!(encode_binary(&Datum::Int4(1)), 1i32.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Int8(1)), 1i64.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Text("hi".into())), b"hi".to_vec());
    }

    #[test]
    #[should_panic]
    fn encoding_null_is_a_caller_error() {
        // NULL is signalled out-of-band (DataRow length -1); encoding it is a bug.
        let _ = encode_text(&Datum::Null);
    }
}
```

- [ ] **Step 2:** `cargo test -p pgtypes -- encoding::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `encoding.rs` (replace the placeholder):

```rust
//! Text and binary wire encodings for Datums. NULL is signalled out-of-band by
//! the wire layer (DataRow value length -1), so encoding a NULL Datum panics —
//! it indicates a caller bug, never reachable from valid execution.

use crate::Datum;

/// PostgreSQL text-format encoding of a (non-null) value.
pub fn encode_text(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_text called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => if *b { b"t".to_vec() } else { b"f".to_vec() },
        Datum::Int4(n) => n.to_string().into_bytes(),
        Datum::Int8(n) => n.to_string().into_bytes(),
        Datum::Text(s) => s.clone().into_bytes(),
    }
}

/// PostgreSQL binary-format encoding of a (non-null) value.
pub fn encode_binary(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_binary called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => vec![u8::from(*b)],
        Datum::Int4(n) => n.to_be_bytes().to_vec(),
        Datum::Int8(n) => n.to_be_bytes().to_vec(),
        Datum::Text(s) => s.clone().into_bytes(),
    }
}
```

- [ ] **Step 4:** `cargo test -p pgtypes -- encoding::` → 3 passed.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/pgtypes
git commit -m "feat(pgtypes): text and binary wire encodings"
```

---

### Task 5: pgtypes — operators, literal typing, NULL/three-valued logic

**Files:**
- Modify: `crates/pgtypes/src/ops.rs`

- [ ] **Step 1: Failing tests** — `ops.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Datum, TypeError};
    use std::cmp::Ordering;

    #[test]
    fn integer_literal_picks_narrowest_type() {
        assert_eq!(int_literal("5").unwrap(), Datum::Int4(5));
        assert_eq!(int_literal("2147483648").unwrap(), Datum::Int8(2_147_483_648));
        assert!(matches!(int_literal("99999999999999999999"), Err(TypeError::Overflow)));
    }

    #[test]
    fn arithmetic_type_promotion_and_overflow() {
        assert_eq!(add(&Datum::Int4(1), &Datum::Int4(2)).unwrap(), Datum::Int4(3));
        assert_eq!(add(&Datum::Int4(1), &Datum::Int8(2)).unwrap(), Datum::Int8(3));
        assert!(matches!(add(&Datum::Int4(i32::MAX), &Datum::Int4(1)), Err(TypeError::Overflow)));
        assert!(matches!(div(&Datum::Int4(1), &Datum::Int4(0)), Err(TypeError::DivisionByZero)));
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        assert_eq!(add(&Datum::Null, &Datum::Int4(1)).unwrap(), Datum::Null);
    }

    #[test]
    fn comparison_returns_none_for_null() {
        assert_eq!(compare(&Datum::Int4(1), &Datum::Int4(2)).unwrap(), Some(Ordering::Less));
        assert_eq!(compare(&Datum::Int4(1), &Datum::Int8(1)).unwrap(), Some(Ordering::Equal));
        assert_eq!(compare(&Datum::Null, &Datum::Int4(1)).unwrap(), None);
        assert_eq!(compare(&Datum::Text("a".into()), &Datum::Text("b".into())).unwrap(), Some(Ordering::Less));
    }

    #[test]
    fn three_valued_boolean_logic() {
        // AND: NULL AND false = false; NULL AND true = NULL.
        assert_eq!(and(&Datum::Null, &Datum::Bool(false)).unwrap(), Datum::Bool(false));
        assert_eq!(and(&Datum::Null, &Datum::Bool(true)).unwrap(), Datum::Null);
        // OR: NULL OR true = true; NULL OR false = NULL.
        assert_eq!(or(&Datum::Null, &Datum::Bool(true)).unwrap(), Datum::Bool(true));
        assert_eq!(or(&Datum::Null, &Datum::Bool(false)).unwrap(), Datum::Null);
        assert_eq!(not(&Datum::Null).unwrap(), Datum::Null);
        assert_eq!(not(&Datum::Bool(true)).unwrap(), Datum::Bool(false));
    }
}
```

- [ ] **Step 2:** `cargo test -p pgtypes -- ops::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `ops.rs` (replace placeholder):

```rust
//! Operator semantics matching PostgreSQL: integer type promotion, checked
//! overflow (22003), division by zero (22012), NULL propagation, and
//! three-valued boolean logic.

use std::cmp::Ordering;

use crate::{Datum, TypeError};

/// Type an integer literal: narrowest of int4, then int8; overflow -> 22003.
pub fn int_literal(s: &str) -> Result<Datum, TypeError> {
    if let Ok(n) = s.parse::<i32>() {
        return Ok(Datum::Int4(n));
    }
    match s.parse::<i64>() {
        Ok(n) => Ok(Datum::Int8(n)),
        Err(_) => Err(TypeError::Overflow),
    }
}

/// Promote an integer Datum to i64 for mixed-width arithmetic.
fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

/// Both operands int4? (determines result width).
fn both_int4(a: &Datum, b: &Datum) -> bool {
    matches!(a, Datum::Int4(_)) && matches!(b, Datum::Int4(_))
}

fn arith(
    a: &Datum,
    b: &Datum,
    i4: fn(i32, i32) -> Option<i32>,
    i8: fn(i64, i64) -> Option<i64>,
) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    if both_int4(a, b) {
        if let (Datum::Int4(x), Datum::Int4(y)) = (a, b) {
            return i4(*x, *y).map(Datum::Int4).ok_or(TypeError::Overflow);
        }
    }
    match (as_i64(a), as_i64(b)) {
        (Some(x), Some(y)) => i8(x, y).map(Datum::Int8).ok_or(TypeError::Overflow),
        _ => Err(TypeError::TypeMismatch {
            message: "operator requires integer operands".into(),
        }),
    }
}

pub fn add(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_add, i64::checked_add)
}
pub fn sub(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_sub, i64::checked_sub)
}
pub fn mul(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    arith(a, b, i32::checked_mul, i64::checked_mul)
}
pub fn div(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    let zero = matches!(b, Datum::Int4(0) | Datum::Int8(0));
    if zero {
        return Err(TypeError::DivisionByZero);
    }
    arith(a, b, i32::checked_div, i64::checked_div)
}

/// SQL comparison. Returns Ok(None) if either operand is NULL (so the caller
/// yields NULL / excludes the row). Cross-type integer comparison is allowed;
/// text compares lexicographically; bool compares false < true.
pub fn compare(a: &Datum, b: &Datum) -> Result<Option<Ordering>, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(None);
    }
    let ord = match (a, b) {
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        _ => match (as_i64(a), as_i64(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => {
                return Err(TypeError::TypeMismatch {
                    message: format!(
                        "cannot compare {} and {}",
                        a.column_type().map(|t| t.name()).unwrap_or("unknown"),
                        b.column_type().map(|t| t.name()).unwrap_or("unknown"),
                    ),
                });
            }
        },
    };
    Ok(Some(ord))
}

fn as_bool(d: &Datum) -> Result<Option<bool>, TypeError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Bool(b) => Ok(Some(*b)),
        _ => Err(TypeError::TypeMismatch {
            message: "argument of boolean operator must be boolean".into(),
        }),
    }
}

/// Three-valued AND: NULL AND false = false, else NULL propagates.
pub fn and(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(false), _) | (_, Some(false)) => Datum::Bool(false),
        (Some(true), Some(true)) => Datum::Bool(true),
        _ => Datum::Null,
    })
}

/// Three-valued OR: NULL OR true = true, else NULL propagates.
pub fn or(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(true), _) | (_, Some(true)) => Datum::Bool(true),
        (Some(false), Some(false)) => Datum::Bool(false),
        _ => Datum::Null,
    })
}

pub fn not(a: &Datum) -> Result<Datum, TypeError> {
    Ok(match as_bool(a)? {
        Some(b) => Datum::Bool(!b),
        None => Datum::Null,
    })
}

/// Build a Bool Datum from a comparison result and the operator.
pub fn cmp_to_bool(op_holds: bool, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(_) => Datum::Bool(op_holds),
    }
}
```

- [ ] **Step 4:** `cargo test -p pgtypes` → all pass (3 + 3 + 5).

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/pgtypes
git commit -m "feat(pgtypes): operators, literal typing, three-valued logic"
```

---

### Task 6: kv — Kv trait + MemKv store

**Files:**
- Create: `crates/kv/src/store.rs`, `crates/kv/src/error.rs`
- Modify: `crates/kv/src/lib.rs`, `crates/kv/Cargo.toml` (add `pgtypes`)

- [ ] **Step 1: Add the pgtypes dependency.** In `crates/kv/Cargo.toml` `[dependencies]` add `pgtypes.workspace = true` (the row encoder in Task 8 needs `Datum`).

- [ ] **Step 2: Failing tests** — `store.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let kv = MemKv::new();
        assert_eq!(kv.get(b"a"), None);
        kv.put(b"a".to_vec(), b"1".to_vec());
        assert_eq!(kv.get(b"a"), Some(b"1".to_vec()));
        kv.delete(b"a");
        assert_eq!(kv.get(b"a"), None);
    }

    #[test]
    fn scan_prefix_returns_sorted_matches_only() {
        let kv = MemKv::new();
        kv.put(b"t/1/b".to_vec(), b"B".to_vec());
        kv.put(b"t/1/a".to_vec(), b"A".to_vec());
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()); // different prefix
        let rows = kv.scan_prefix(b"t/1/");
        assert_eq!(
            rows,
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }
}
```

- [ ] **Step 3: Implement** `store.rs`:

```rust
//! The key-value storage seam. SP2 ships an in-memory `MemKv`; SP3 swaps in a
//! durable LSM behind the same `Kv` trait, SP4 shards it into Raft ranges.

use std::collections::BTreeMap;
use std::sync::RwLock;

/// An ordered byte-key/byte-value store. Synchronous for SP2; the distributed
/// layer will introduce an async, transactional variant behind this boundary.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&self, key: &[u8]);
    /// All (key, value) pairs whose key starts with `prefix`, in key order.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
}

/// In-memory ordered store backed by a BTreeMap.
#[derive(Default)]
pub struct MemKv {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Kv for MemKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.read().expect("kv lock").get(key).cloned()
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.map.write().expect("kv lock").insert(key, value);
    }

    fn delete(&self, key: &[u8]) {
        self.map.write().expect("kv lock").remove(key);
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .read()
            .expect("kv lock")
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
```

`error.rs`:

```rust
//! Errors from decoding stored bytes. Our own writes never produce these, but
//! decoders must fail rather than panic on corrupt or truncated input.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    #[error("corrupt row encoding: {0}")]
    CorruptRow(String),
}
```

`lib.rs`:

```rust
//! kv: ordered key-value storage with order-preserving key encoding and a
//! versioned row value encoding. The permanent storage seam for crabgresql.

pub mod error;
pub mod key;
pub mod keyenc;
pub mod rowenc;
pub mod store;

pub use error::KvError;
pub use store::{Kv, MemKv};
```

(Create empty `keyenc.rs`/`key.rs`/`rowenc.rs` with `//!` doc lines so lib.rs
compiles; Tasks 7 and 8 fill them.)

- [ ] **Step 4:** `cargo test -p kv -- store::` → 2 passed.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/kv Cargo.toml Cargo.lock
git commit -m "feat(kv): Kv trait and in-memory MemKv store"
```

---

### Task 7: kv — order-preserving key encoding (the permanent invariant)

**Files:**
- Modify: `crates/kv/src/keyenc.rs`, `crates/kv/src/key.rs`

- [ ] **Step 1: Failing tests** — `keyenc.rs` test module (unit + property):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn roundtrip_u32_u64() {
        let mut b = Vec::new();
        put_u32(&mut b, 0x0102_0304);
        put_u64(&mut b, 0x0102_0304_0506_0708);
        let mut cur = &b[..];
        assert_eq!(take_u32(&mut cur).unwrap(), 0x0102_0304);
        assert_eq!(take_u64(&mut cur).unwrap(), 0x0102_0304_0506_0708);
        assert!(cur.is_empty());
    }

    #[test]
    fn truncated_take_errors_not_panics() {
        let mut cur = &[0u8, 1][..];
        assert!(take_u32(&mut cur).is_err());
    }

    proptest! {
        // The load-bearing invariant: byte order == logical order.
        #[test]
        fn u64_encoding_is_order_preserving(a: u64, b: u64) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            put_u64(&mut ea, a);
            put_u64(&mut eb, b);
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        #[test]
        fn u32_encoding_is_order_preserving(a: u32, b: u32) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            put_u32(&mut ea, a);
            put_u32(&mut eb, b);
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }
    }
}
```

- [ ] **Step 2:** `cargo test -p kv -- keyenc::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `keyenc.rs`:

```rust
//! Order-preserving encoders for key components. Unsigned big-endian fixed
//! width is already order-preserving, which is all the SP2 slice needs (table
//! ids, index ids, and a monotonic hidden rowid). Sortable encodings for
//! arbitrary PRIMARY KEY column types are deferred; the key layout reserves the
//! slot, so adding them is additive.

use crate::KvError;

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn take_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    if cur.len() < 4 {
        return Err(KvError::CorruptRow("truncated u32 key component".into()));
    }
    let (head, rest) = cur.split_at(4);
    *cur = rest;
    Ok(u32::from_be_bytes(head.try_into().expect("4 bytes")))
}

pub fn take_u64(cur: &mut &[u8]) -> Result<u64, KvError> {
    if cur.len() < 8 {
        return Err(KvError::CorruptRow("truncated u64 key component".into()));
    }
    let (head, rest) = cur.split_at(8);
    *cur = rest;
    Ok(u64::from_be_bytes(head.try_into().expect("8 bytes")))
}
```

- [ ] **Step 4: Failing tests** — `key.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_keys_sort_by_rowid_within_a_table() {
        let k1 = row_key(7, 1);
        let k2 = row_key(7, 2);
        let k10 = row_key(7, 10);
        assert!(k1 < k2 && k2 < k10, "rowid order must be byte order");
        assert!(k1.starts_with(&table_prefix(7)));
    }

    #[test]
    fn different_tables_do_not_share_a_prefix() {
        assert!(!row_key(8, 1).starts_with(&table_prefix(7)));
    }

    #[test]
    fn rowid_roundtrips_from_a_key() {
        let k = row_key(7, 42);
        assert_eq!(rowid_of(7, &k).unwrap(), 42);
    }
}
```

- [ ] **Step 5: Implement** `key.rs`:

```rust
//! Key construction: `/<table_id>/<index_id>/<rowid>`. The primary "index" is
//! id 1; secondary indexes (later) get higher ids under the same table prefix.

use crate::KvError;
use crate::keyenc::{put_u32, put_u64, take_u32, take_u64};

/// The primary storage index for a table's rows.
pub const INDEX_PRIMARY: u32 = 1;

/// Bytes shared by every row of a table's primary index.
pub fn table_prefix(table_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(8);
    put_u32(&mut k, table_id);
    put_u32(&mut k, INDEX_PRIMARY);
    k
}

/// Full key for one row: table prefix followed by the order-preserving rowid.
pub fn row_key(table_id: u32, rowid: u64) -> Vec<u8> {
    let mut k = table_prefix(table_id);
    put_u64(&mut k, rowid);
    k
}

/// Recover the rowid from a key known to belong to `table_id`.
pub fn rowid_of(table_id: u32, key: &[u8]) -> Result<u64, KvError> {
    let mut cur = key;
    let t = take_u32(&mut cur)?;
    let idx = take_u32(&mut cur)?;
    if t != table_id || idx != INDEX_PRIMARY {
        return Err(KvError::CorruptRow("key does not belong to this table index".into()));
    }
    take_u64(&mut cur)
}
```

- [ ] **Step 6:** `cargo test -p kv` → all pass (store + keyenc unit/property + key).

- [ ] **Step 7:** fmt + clippy, then:

```bash
git add crates/kv
git commit -m "feat(kv): order-preserving key encoding with property tests"
```

---

### Task 8: kv — versioned row value encoding

**Files:**
- Modify: `crates/kv/src/rowenc.rs`

- [ ] **Step 1: Failing tests** — `rowenc.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;
    use proptest::prelude::*;

    #[test]
    fn roundtrip_all_datum_kinds_including_null() {
        let row = vec![
            Datum::Null,
            Datum::Bool(true),
            Datum::Int4(i32::MIN),
            Datum::Int8(i64::MIN),
            Datum::Text("héllo".into()),
        ];
        let bytes = encode_row(&row);
        assert_eq!(decode_row(&bytes).unwrap(), row);
    }

    #[test]
    fn version_byte_is_present() {
        assert_eq!(encode_row(&[Datum::Int4(1)])[0], ROW_VERSION);
    }

    #[test]
    fn truncated_value_errors_not_panics() {
        assert!(decode_row(&[ROW_VERSION, 2, 0, 0]).is_err()); // int4 tag, only 2 payload bytes
    }

    #[test]
    fn unknown_version_errors() {
        assert!(decode_row(&[99, 1, 1]).is_err());
    }

    fn arb_datum() -> impl Strategy<Value = Datum> {
        prop_oneof![
            Just(Datum::Null),
            any::<bool>().prop_map(Datum::Bool),
            any::<i32>().prop_map(Datum::Int4),
            any::<i64>().prop_map(Datum::Int8),
            ".*".prop_map(Datum::Text),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_rows(row in prop::collection::vec(arb_datum(), 0..8)) {
            let bytes = encode_row(&row);
            prop_assert_eq!(decode_row(&bytes).unwrap(), row);
        }
    }
}
```

- [ ] **Step 2:** `cargo test -p kv -- rowenc::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `rowenc.rs`:

```rust
//! Versioned row value encoding: a leading version byte (so SP3 can evolve the
//! format) then one tagged field per column. NOT order-preserving — values are
//! never sorted by raw bytes.

use pgtypes::Datum;

use crate::KvError;

/// Current row-value format version.
pub const ROW_VERSION: u8 = 1;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
}

pub fn encode_row(cols: &[Datum]) -> Vec<u8> {
    let mut out = vec![ROW_VERSION];
    for d in cols {
        match d {
            Datum::Null => out.push(tag::NULL),
            Datum::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Datum::Int4(n) => {
                out.push(tag::INT4);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Int8(n) => {
                out.push(tag::INT8);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Text(s) => {
                out.push(tag::TEXT);
                out.extend_from_slice(&(s.len() as u32).to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    out
}

pub fn decode_row(bytes: &[u8]) -> Result<Vec<Datum>, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != ROW_VERSION {
        return Err(KvError::CorruptRow(format!("unknown row version {version}")));
    }
    let mut cols = Vec::new();
    while !cur.is_empty() {
        let t = take_u8(&mut cur)?;
        let d = match t {
            tag::NULL => Datum::Null,
            tag::BOOL => Datum::Bool(take_u8(&mut cur)? != 0),
            tag::INT4 => Datum::Int4(i32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"))),
            tag::INT8 => Datum::Int8(i64::from_be_bytes(take_n(&mut cur, 8)?.try_into().expect("8"))),
            tag::TEXT => {
                let len = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
                let raw = take_n(&mut cur, len)?;
                Datum::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| KvError::CorruptRow("text is not valid UTF-8".into()))?,
                )
            }
            other => return Err(KvError::CorruptRow(format!("unknown field tag {other}"))),
        };
        cols.push(d);
    }
    Ok(cols)
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (head, rest) = cur.split_first().ok_or_else(|| KvError::CorruptRow("truncated".into()))?;
    *cur = rest;
    Ok(*head)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated field".into()));
    }
    let (head, rest) = cur.split_at(n);
    *cur = rest;
    Ok(head)
}
```

- [ ] **Step 4:** `cargo test -p kv` → all pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/kv
git commit -m "feat(kv): versioned row value encoding with roundtrip property tests"
```

---

### Task 9: catalog — table metadata + CRUD with PG error codes

**Files:**
- Modify: `crates/catalog/src/lib.rs`, `crates/catalog/Cargo.toml` (add `pgtypes`)

- [ ] **Step 1: Add dependency.** In `crates/catalog/Cargo.toml` `[dependencies]` add `pgtypes.workspace = true`.

- [ ] **Step 2: Failing tests** — `lib.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::ColumnType;

    fn cols() -> Vec<Column> {
        vec![
            Column { name: "id".into(), ty: ColumnType::Int4 },
            Column { name: "name".into(), ty: ColumnType::Text },
        ]
    }

    #[test]
    fn create_lookup_drop() {
        let cat = Catalog::new();
        let id = cat.create_table("t", cols()).expect("create");
        let t = cat.get_table("t").expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("name"), Some(1));
        assert_eq!(t.column_index("missing"), None);
        cat.drop_table("t").expect("drop");
        assert!(matches!(cat.get_table("t"), Err(CatalogError::UndefinedTable(_))));
    }

    #[test]
    fn duplicate_table_is_42P07() {
        let cat = Catalog::new();
        cat.create_table("t", cols()).expect("create");
        let err = cat.create_table("t", cols()).expect_err("dup");
        assert_eq!(err.sqlstate(), "42P07");
    }

    #[test]
    fn drop_missing_is_42P01() {
        let cat = Catalog::new();
        let err = cat.drop_table("nope").expect_err("missing");
        assert_eq!(err.sqlstate(), "42P01");
    }

    #[test]
    fn table_ids_are_distinct_and_nonzero() {
        let cat = Catalog::new();
        let a = cat.create_table("a", cols()).expect("a");
        let b = cat.create_table("b", cols()).expect("b");
        assert_ne!(a, b);
        assert!(a >= 1 && b >= 1);
    }
}
```

- [ ] **Step 3: Implement** `lib.rs`:

```rust
//! In-memory system catalog: tables, their columns, and CRUD with PostgreSQL
//! error codes. Persistence arrives in SP3; no pg_catalog SQL views in SP2.

use std::collections::HashMap;
use std::sync::RwLock;

use pgtypes::ColumnType;

/// OID-style table identifier (never 0; 0 is reserved/invalid).
pub type TableId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Zero-based ordinal of a column by name, or None.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
}

impl CatalogError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_) => "42P07",
            CatalogError::UndefinedTable(_) => "42P01",
            CatalogError::UndefinedColumn(_) => "42703",
        }
    }
}

struct Inner {
    next_id: TableId,
    by_name: HashMap<String, Table>,
}

/// The catalog. Cheap to share behind an `Arc`; internally `RwLock`-guarded.
pub struct Catalog {
    inner: RwLock<Inner>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self { inner: RwLock::new(Inner { next_id: 1, by_name: HashMap::new() }) }
    }

    pub fn create_table(&self, name: &str, columns: Vec<Column>) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.write().expect("catalog lock");
        if inner.by_name.contains_key(name) {
            return Err(CatalogError::DuplicateTable(name.to_string()));
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner
            .by_name
            .insert(name.to_string(), Table { id, name: name.to_string(), columns });
        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.write().expect("catalog lock");
        if inner.by_name.remove(name).is_none() {
            return Err(CatalogError::UndefinedTable(name.to_string()));
        }
        Ok(())
    }

    /// Snapshot of a table's metadata by name.
    pub fn get_table(&self, name: &str) -> Result<Table, CatalogError> {
        self.inner
            .read()
            .expect("catalog lock")
            .by_name
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))
    }
}
```

- [ ] **Step 4:** `cargo test -p catalog` → 4 passed.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/catalog Cargo.toml Cargo.lock
git commit -m "feat(catalog): in-memory table metadata with PG error codes"
```

---

### Task 10: pgparser — tokens, lexer, ParseError

**Files:**
- Create: `crates/pgparser/src/token.rs`, `crates/pgparser/src/lexer.rs`, `crates/pgparser/src/error.rs`
- Modify: `crates/pgparser/src/lib.rs`

- [ ] **Step 1: Failing tests** — `lexer.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Keyword, Token};

    fn toks(sql: &str) -> Vec<Token> {
        lex(sql).expect("lex").into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn keywords_idents_literals() {
        assert_eq!(
            toks("SELECT id FROM t WHERE x = 'a'"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::Ident("id".into()),
                Token::Keyword(Keyword::From),
                Token::Ident("t".into()),
                Token::Keyword(Keyword::Where),
                Token::Ident("x".into()),
                Token::Eq,
                Token::StringLit("a".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn keywords_are_case_insensitive_idents_lowercased() {
        assert_eq!(toks("Select FOO")[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks("Select FOO")[1], Token::Ident("foo".into()));
    }

    #[test]
    fn quoted_ident_preserves_case() {
        assert_eq!(toks("\"MixedCase\"")[0], Token::Ident("MixedCase".into()));
    }

    #[test]
    fn string_escaping_doubles_quote() {
        assert_eq!(toks("'it''s'")[0], Token::StringLit("it's".into()));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(toks("1 -- c\n+ /* x */ 2"),
            vec![Token::IntLit("1".into()), Token::Plus, Token::IntLit("2".into()), Token::Eof]);
    }

    #[test]
    fn operators_lex() {
        assert_eq!(
            toks("<= >= <> < > = + - * / ( ) , ;"),
            vec![
                Token::Le, Token::Ge, Token::Ne, Token::Lt, Token::Gt, Token::Eq,
                Token::Plus, Token::Minus, Token::Star, Token::Slash,
                Token::LParen, Token::RParen, Token::Comma, Token::Semicolon, Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_string_errors_with_position() {
        let e = lex("'abc").expect_err("unterminated");
        assert_eq!(e.position, 0);
    }
}
```

- [ ] **Step 2:** `cargo test -p pgparser -- lexer::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `token.rs`:

```rust
//! Lexical tokens for the SP2 SQL slice.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Ident(String),
    Keyword(Keyword),
    IntLit(String),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Create,
    Table,
    Drop,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    As,
}

impl Keyword {
    pub fn from_word(w: &str) -> Option<Keyword> {
        Some(match w {
            "create" => Keyword::Create,
            "table" => Keyword::Table,
            "drop" => Keyword::Drop,
            "insert" => Keyword::Insert,
            "into" => Keyword::Into,
            "values" => Keyword::Values,
            "select" => Keyword::Select,
            "from" => Keyword::From,
            "where" => Keyword::Where,
            "order" => Keyword::Order,
            "by" => Keyword::By,
            "asc" => Keyword::Asc,
            "desc" => Keyword::Desc,
            "limit" => Keyword::Limit,
            "and" => Keyword::And,
            "or" => Keyword::Or,
            "not" => Keyword::Not,
            "true" => Keyword::True,
            "false" => Keyword::False,
            "null" => Keyword::Null,
            "as" => Keyword::As,
            _ => return None,
        })
    }
}
```

`error.rs`:

```rust
//! Parse/lex errors. All map to SQLSTATE 42601 (syntax_error) and carry the
//! byte offset where the problem was detected.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("syntax error at position {position}: {message}")]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl ParseError {
    pub fn new(message: impl Into<String>, position: usize) -> Self {
        Self { message: message.into(), position }
    }

    pub fn sqlstate(&self) -> &'static str {
        "42601"
    }
}
```

`lexer.rs`:

```rust
//! Hand-written lexer. Produces (Token, byte-offset) pairs; offsets feed
//! 42601 error positions. Integer literals only (the slice has no float type).

use crate::error::ParseError;
use crate::token::{Keyword, Token};

pub fn lex(sql: &str) -> Result<Vec<(Token, usize)>, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 >= bytes.len() {
                    return Err(ParseError::new("unterminated block comment", start));
                }
                i += 2;
            }
            b'\'' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => return Err(ParseError::new("unterminated string literal", start)),
                        Some(&b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            s.push('\'');
                            i += 2;
                        }
                        Some(&b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::StringLit(s), start));
            }
            b'"' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => return Err(ParseError::new("unterminated quoted identifier", start)),
                        Some(&b'"') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::Ident(s), start));
            }
            b'<' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Le, i));
                i += 2;
            }
            b'>' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Ge, i));
                i += 2;
            }
            b'<' if bytes.get(i + 1) == Some(&b'>') => {
                out.push((Token::Ne, i));
                i += 2;
            }
            b'(' => push1(&mut out, Token::LParen, &mut i),
            b')' => push1(&mut out, Token::RParen, &mut i),
            b',' => push1(&mut out, Token::Comma, &mut i),
            b';' => push1(&mut out, Token::Semicolon, &mut i),
            b'*' => push1(&mut out, Token::Star, &mut i),
            b'+' => push1(&mut out, Token::Plus, &mut i),
            b'-' => push1(&mut out, Token::Minus, &mut i),
            b'/' => push1(&mut out, Token::Slash, &mut i),
            b'=' => push1(&mut out, Token::Eq, &mut i),
            b'<' => push1(&mut out, Token::Lt, &mut i),
            b'>' => push1(&mut out, Token::Gt, &mut i),
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                out.push((Token::IntLit(sql[start..i].to_string()), start));
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                let word = sql[start..i].to_ascii_lowercase();
                let tok = match Keyword::from_word(&word) {
                    Some(kw) => Token::Keyword(kw),
                    None => Token::Ident(word),
                };
                out.push((tok, start));
            }
            _ => return Err(ParseError::new(format!("unexpected character {:?}", c as char), i)),
        }
    }
    out.push((Token::Eof, sql.len()));
    Ok(out)
}

fn push1(out: &mut Vec<(Token, usize)>, tok: Token, i: &mut usize) {
    out.push((tok, *i));
    *i += 1;
}
```

`lib.rs` (partial — parser added in Tasks 11-12):

```rust
//! pgparser: hand-written lexer + recursive-descent/Pratt parser producing the
//! crabgresql AST for the SP2 SQL slice.

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use error::ParseError;
pub use parser::parse;
```

(Create empty `ast.rs` and `parser.rs` with `//!` doc lines so this compiles;
Tasks 11-12 fill them. `parse` won't exist until Task 11 — to keep this task
green, temporarily add to `parser.rs`:
`pub fn parse(_sql: &str) -> Result<Vec<crate::ast::Statement>, crate::ParseError> { unimplemented!() }`
and a minimal `ast.rs` with `pub enum Statement {}` — Task 11 replaces both. Do
NOT test `parse` in this task.)

- [ ] **Step 4:** `cargo test -p pgparser -- lexer::` → all lexer tests pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/pgparser
git commit -m "feat(pgparser): tokens, hand-written lexer, ParseError with positions"
```

---

### Task 11: pgparser — AST + Pratt expression parser

**Files:**
- Modify: `crates/pgparser/src/ast.rs`, `crates/pgparser/src/parser.rs`, `crates/pgparser/src/token.rs`, `crates/pgparser/src/lexer.rs`

Expressions are the shared primitive (INSERT VALUES, SELECT projection, WHERE,
ORDER BY all use them), so they come before the statement grammar.

- [ ] **Step 1: Add `$n` parameter lexing.** In `token.rs` add `Param(u32),` to
the `Token` enum. In `lexer.rs`, add this arm to the match (before the digit
arm):

```rust
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                let ds = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let n: u32 = sql[ds..i]
                    .parse()
                    .map_err(|_| ParseError::new("parameter number out of range", start))?;
                out.push((Token::Param(n), start));
            }
```

Add a lexer test:

```rust
    #[test]
    fn lexes_parameter_placeholder() {
        assert_eq!(toks("$1")[0], Token::Param(1));
    }
```

- [ ] **Step 2: Failing expression tests** — `parser.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, Expr, UnaryOp};

    fn expr(sql: &str) -> Expr {
        // Wrap in a SELECT so the public parse() entry can reach it once
        // statements exist; until then, use the crate-internal expr parser.
        parse_expr_for_test(sql).expect("parse expr")
    }

    #[test]
    fn precedence_mul_over_add() {
        // 1 + 2 * 3  ==  1 + (2 * 3)
        let e = expr("1 + 2 * 3");
        assert_eq!(
            e,
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::IntLiteral("1".into())),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::IntLiteral("2".into())),
                    right: Box::new(Expr::IntLiteral("3".into())),
                }),
            }
        );
    }

    #[test]
    fn comparison_and_boolean_precedence() {
        // a = 1 AND b < 2  ==  (a = 1) AND (b < 2)
        let e = expr("a = 1 AND b < 2");
        assert!(matches!(e, Expr::Binary { op: BinaryOp::And, .. }));
    }

    #[test]
    fn not_and_or_precedence() {
        // NOT x OR y  ==  (NOT x) OR y
        let e = expr("NOT x OR y");
        match e {
            Expr::Binary { op: BinaryOp::Or, left, .. } => {
                assert!(matches!(*left, Expr::Unary { op: UnaryOp::Not, .. }));
            }
            _ => panic!("expected OR at top, got {e:?}"),
        }
    }

    #[test]
    fn unary_minus_and_parens() {
        let e = expr("-(1 + 2)");
        assert!(matches!(e, Expr::Unary { op: UnaryOp::Neg, .. }));
    }

    #[test]
    fn literals_columns_params() {
        assert_eq!(expr("'hi'"), Expr::StringLiteral("hi".into()));
        assert_eq!(expr("true"), Expr::BoolLiteral(true));
        assert_eq!(expr("null"), Expr::NullLiteral);
        assert_eq!(expr("col"), Expr::Column("col".into()));
        assert_eq!(expr("$2"), Expr::Param(2));
    }
}
```

- [ ] **Step 3:** `cargo test -p pgparser -- parser::` → COMPILE FAIL.

- [ ] **Step 4: Implement** `ast.rs`:

```rust
//! The crabgresql AST for the SP2 slice.

use pgtypes::ColumnType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<ColumnDef> },
    DropTable { name: String },
    Insert { table: String, columns: Option<Vec<String>>, rows: Vec<Vec<Expr>> },
    Select(SelectStmt),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub projection: Vec<SelectItem>,
    pub from: Option<String>,
    pub filter: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLiteral(String),
    StringLiteral(String),
    BoolLiteral(bool),
    NullLiteral,
    Column(String),
    Param(u32),
    Unary { op: UnaryOp, expr: Box<Expr> },
    Binary { op: BinaryOp, left: Box<Expr>, right: Box<Expr> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
```

- [ ] **Step 5: Implement the parser cursor + Pratt expressions** in `parser.rs`:

```rust
//! Recursive-descent statement parser with Pratt expression parsing.

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::error::ParseError;
use crate::lexer::lex;
use crate::token::{Keyword, Token};

pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    pos: usize,
}

impl Parser {
    pub(crate) fn new(toks: Vec<(Token, usize)>) -> Self {
        Self { toks, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.toks[self.pos].0
    }

    fn peek_pos(&self) -> usize {
        self.toks[self.pos].1
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if *self.peek() == Token::Keyword(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, want: &Token) -> Result<(), ParseError> {
        if self.peek() == want {
            self.bump();
            Ok(())
        } else {
            Err(ParseError::new(
                format!("expected {want:?}, found {:?}", self.peek()),
                self.peek_pos(),
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s),
            other => Err(ParseError::new(format!("expected identifier, found {other:?}"), self.peek_pos())),
        }
    }

    /// Pratt expression parser. `min_bp` is the minimum left binding power.
    pub(crate) fn expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.prefix()?;
        loop {
            let (op, l_bp, r_bp) = match self.peek() {
                Token::Keyword(Keyword::Or) => (BinaryOp::Or, 1, 2),
                Token::Keyword(Keyword::And) => (BinaryOp::And, 3, 4),
                Token::Eq => (BinaryOp::Eq, 5, 6),
                Token::Ne => (BinaryOp::Ne, 5, 6),
                Token::Lt => (BinaryOp::Lt, 5, 6),
                Token::Le => (BinaryOp::Le, 5, 6),
                Token::Gt => (BinaryOp::Gt, 5, 6),
                Token::Ge => (BinaryOp::Ge, 5, 6),
                Token::Plus => (BinaryOp::Add, 7, 8),
                Token::Minus => (BinaryOp::Sub, 7, 8),
                Token::Star => (BinaryOp::Mul, 9, 10),
                Token::Slash => (BinaryOp::Div, 9, 10),
                _ => break,
            };
            if l_bp < min_bp {
                break;
            }
            self.bump();
            let rhs = self.expr(r_bp)?;
            lhs = Expr::Binary { op, left: Box::new(lhs), right: Box::new(rhs) };
        }
        Ok(lhs)
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Keyword(Keyword::Not) => {
                self.bump();
                Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(self.expr(3)?) })
            }
            Token::Minus => {
                self.bump();
                Ok(Expr::Unary { op: UnaryOp::Neg, expr: Box::new(self.expr(11)?) })
            }
            Token::LParen => {
                self.bump();
                let e = self.expr(0)?;
                self.expect(&Token::RParen)?;
                Ok(e)
            }
            Token::IntLit(s) => {
                self.bump();
                Ok(Expr::IntLiteral(s))
            }
            Token::StringLit(s) => {
                self.bump();
                Ok(Expr::StringLiteral(s))
            }
            Token::Keyword(Keyword::True) => {
                self.bump();
                Ok(Expr::BoolLiteral(true))
            }
            Token::Keyword(Keyword::False) => {
                self.bump();
                Ok(Expr::BoolLiteral(false))
            }
            Token::Keyword(Keyword::Null) => {
                self.bump();
                Ok(Expr::NullLiteral)
            }
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            Token::Ident(s) => {
                self.bump();
                Ok(Expr::Column(s))
            }
            other => Err(ParseError::new(format!("unexpected token {other:?}"), self.peek_pos())),
        }
    }
}

/// Test-support entry: parse a bare expression. `pub` (not cfg(test)) so the
/// executor crate's tests can reuse it; `doc(hidden)` keeps it out of the API.
#[doc(hidden)]
pub fn parse_expr_for_test(sql: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(lex(sql)?);
    let e = p.expr(0)?;
    if *p.peek() != Token::Eof {
        return Err(ParseError::new("trailing tokens after expression", p.peek_pos()));
    }
    Ok(e)
}

/// Public statement entry — implemented in Task 12.
pub fn parse(sql: &str) -> Result<Vec<crate::ast::Statement>, ParseError> {
    let mut p = Parser::new(lex(sql)?);
    p.program()
}
```

Add a temporary `program()` method so Task 11 compiles (Task 12 replaces it):

```rust
impl Parser {
    pub(crate) fn program(&mut self) -> Result<Vec<crate::ast::Statement>, ParseError> {
        unimplemented!("statement grammar lands in Task 12")
    }
}
```

- [ ] **Step 6:** `cargo test -p pgparser -- parser:: lexer::` → all expression and
lexer tests pass (do not call `parse`/`program` yet).

- [ ] **Step 7:** fmt + clippy, then:

```bash
git add crates/pgparser
git commit -m "feat(pgparser): AST and Pratt expression parser"
```

---

### Task 12: pgparser — statement grammar (CREATE/DROP/INSERT/SELECT)

**Files:**
- Modify: `crates/pgparser/src/parser.rs`

- [ ] **Step 1: Failing tests** — add to `parser.rs` test module:

```rust
    use crate::ast::{ColumnDef, SelectItem, Statement};
    use pgtypes::ColumnType;

    fn one(sql: &str) -> Statement {
        let mut v = parse(sql).expect("parse");
        assert_eq!(v.len(), 1);
        v.pop().unwrap()
    }

    #[test]
    fn parses_create_table() {
        assert_eq!(
            one("CREATE TABLE t (id int4, name text)"),
            Statement::CreateTable {
                name: "t".into(),
                columns: vec![
                    ColumnDef { name: "id".into(), ty: ColumnType::Int4 },
                    ColumnDef { name: "name".into(), ty: ColumnType::Text },
                ],
            }
        );
    }

    #[test]
    fn unknown_column_type_is_error() {
        let e = parse("CREATE TABLE t (x widget)").expect_err("bad type");
        assert_eq!(e.sqlstate(), "42601");
    }

    #[test]
    fn parses_drop_table() {
        assert_eq!(one("DROP TABLE t"), Statement::DropTable { name: "t".into() });
    }

    #[test]
    fn parses_multi_row_insert_with_columns() {
        match one("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')") {
            Statement::Insert { table, columns, rows } => {
                assert_eq!(table, "t");
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_with_all_clauses() {
        match one("SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10") {
            Statement::Select(s) => {
                assert_eq!(s.projection.len(), 2);
                assert!(matches!(s.projection[1], SelectItem::Expr { alias: Some(ref n), .. } if n == "bee"));
                assert_eq!(s.from.as_deref(), Some("t"));
                assert!(s.filter.is_some());
                assert_eq!(s.order_by.len(), 2);
                assert!(!s.order_by[0].asc); // DESC
                assert!(s.order_by[1].asc); // default ASC
                assert_eq!(s.limit, Some(10));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_star_no_from() {
        match one("SELECT *") {
            Statement::Select(s) => {
                assert_eq!(s.projection, vec![SelectItem::Wildcard]);
                assert!(s.from.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_multiple_statements() {
        let v = parse("SELECT 1; SELECT 2;").expect("parse");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn trailing_garbage_is_error() {
        assert!(parse("SELECT 1 foo bar").is_err());
    }
```

- [ ] **Step 2:** `cargo test -p pgparser -- parser::` → FAIL (`program` unimplemented).

- [ ] **Step 3: Implement** the statement grammar in `parser.rs` — replace the
temporary `program()` with the real grammar and helpers:

```rust
impl Parser {
    pub(crate) fn program(&mut self) -> Result<Vec<crate::ast::Statement>, ParseError> {
        use crate::ast::Statement;
        let mut stmts: Vec<Statement> = Vec::new();
        loop {
            while *self.peek() == Token::Semicolon {
                self.bump();
            }
            if *self.peek() == Token::Eof {
                break;
            }
            stmts.push(self.statement()?);
            match self.peek() {
                Token::Semicolon => {
                    self.bump();
                }
                Token::Eof => break,
                other => {
                    return Err(ParseError::new(
                        format!("expected ; or end of input, found {other:?}"),
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(stmts)
    }

    fn statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Create) => self.create_table(),
            Token::Keyword(Keyword::Drop) => self.drop_table(),
            Token::Keyword(Keyword::Insert) => self.insert(),
            Token::Keyword(Keyword::Select) => self.select(),
            other => Err(ParseError::new(format!("unexpected statement start {other:?}"), self.peek_pos())),
        }
    }

    fn create_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ColumnDef, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.expect_ident()?;
            let type_pos = self.peek_pos();
            let type_word = self.expect_ident()?;
            let ty = pgtypes::ColumnType::from_sql_name(&type_word)
                .ok_or_else(|| ParseError::new(format!("unknown type \"{type_word}\""), type_pos))?;
            columns.push(ColumnDef { name: col_name, ty });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(Statement::CreateTable { name, columns })
    }

    fn drop_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        Ok(Statement::DropTable { name: self.expect_ident()? })
    }

    fn insert(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Insert))?;
        self.expect(&Token::Keyword(Keyword::Into))?;
        let table = self.expect_ident()?;
        let columns = if *self.peek() == Token::LParen {
            self.bump();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(Statement::Insert { table, columns, rows })
    }

    fn select(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{OrderItem, SelectItem, SelectStmt, Statement};
        self.expect(&Token::Keyword(Keyword::Select))?;
        let mut projection = Vec::new();
        if *self.peek() == Token::Star {
            self.bump();
            projection.push(SelectItem::Wildcard);
        } else {
            loop {
                let expr = self.expr(0)?;
                let alias = if self.eat_keyword(Keyword::As) {
                    Some(self.expect_ident()?)
                } else if let Token::Ident(_) = self.peek() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                projection.push(SelectItem::Expr { expr, alias });
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let from = if self.eat_keyword(Keyword::From) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                let expr = self.expr(0)?;
                let asc = if self.eat_keyword(Keyword::Desc) {
                    false
                } else {
                    self.eat_keyword(Keyword::Asc);
                    true
                };
                order_by.push(OrderItem { expr, asc });
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let limit = if self.eat_keyword(Keyword::Limit) {
            let pos = self.peek_pos();
            match self.bump() {
                Token::IntLit(s) => Some(
                    s.parse::<i64>()
                        .map_err(|_| ParseError::new("LIMIT value out of range", pos))?,
                ),
                other => return Err(ParseError::new(format!("expected LIMIT count, found {other:?}"), pos)),
            }
        } else {
            None
        };
        Ok(Statement::Select(SelectStmt { projection, from, filter, order_by, limit }))
    }

    fn eat_comma(&mut self) -> bool {
        if *self.peek() == Token::Comma {
            self.bump();
            true
        } else {
            false
        }
    }
}
```

Note: the `else if let Token::Ident(_)` branch implements implicit aliases
(`SELECT a b`); since type-name words are lexed as `Ident`, this is unambiguous
in the slice grammar. Keep it — PostgreSQL supports implicit aliases.

- [ ] **Step 4:** `cargo test -p pgparser` → all lexer + expression + statement
tests pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/pgparser
git commit -m "feat(pgparser): statement grammar for CREATE/DROP/INSERT/SELECT"
```

---

### Task 13: pgparser — libpg_query differential oracle (carry-over, feature-gated)

**Files:**
- Modify: `crates/pgparser/Cargo.toml` (optional `pg_query` dep + `oracle` feature)
- Create: `crates/pgparser/tests/libpg_query_oracle.rs`

The oracle proves our parser **accepts exactly what PostgreSQL accepts on the
slice grammar, and rejects true syntax errors PostgreSQL rejects**. We compare
accept/reject agreement (not tree shape — the two ASTs differ structurally).
`pg_query` builds libpg_query (C); to keep it out of the shipped tree AND out of
the default dependency graph that `cargo deny check bans` inspects, it is an
**optional dependency behind the `oracle` feature**, run only via
`cargo test -p pgparser --features oracle` (a dedicated CI step, never the deny
gate). `check-no-native.sh` is unaffected — it inspects only the `crabgresql`
binary's tree.

- [ ] **Step 1: Add the optional dependency + feature.** In
`crates/pgparser/Cargo.toml`:

```toml
[features]
oracle = ["dep:pg_query"]

[dependencies]
# ... existing ...
pg_query = { version = "6", optional = true }
```

(Check the current `pg_query` version on crates.io; v6 wraps libpg_query 17. Any
recent version is fine — it only needs to parse the slice grammar.)

- [ ] **Step 2: Write the oracle test** — `crates/pgparser/tests/libpg_query_oracle.rs`:

```rust
//! Differential parser oracle: our parser must agree with libpg_query on
//! accept/reject for slice-grammar statements and clear syntax errors.
//! Gated behind --features oracle (libpg_query is a C build-time dep).
#![cfg(feature = "oracle")]

/// Statements inside the SP2 slice — BOTH parsers must accept.
const ACCEPTED: &[&str] = &[
    "CREATE TABLE t (id int4, name text)",
    "CREATE TABLE t (a integer, b bigint, c boolean, d text)",
    "DROP TABLE t",
    "INSERT INTO t VALUES (1, 'a')",
    "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
    "SELECT 1",
    "SELECT 1 + 2 * 3",
    "SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10",
    "SELECT * FROM t",
    "SELECT NOT a OR b AND c FROM t",
    "SELECT 'it''s' FROM t",
];

/// Clear syntax errors — BOTH parsers must reject.
const REJECTED: &[&str] = &[
    "SELECT FROM",
    "CREATE TABLE",
    "INSERT INTO t VALUES",
    "SELECT 1 +",
    "SELECT * FROM",
    "SELECT 1 ORDER BY",
    "(",
    "SELECT 'unterminated",
];

fn pg_accepts(sql: &str) -> bool {
    pg_query::parse(sql).is_ok()
}

fn we_accept(sql: &str) -> bool {
    pgparser::parse(sql).is_ok()
}

#[test]
fn agreement_on_accepted() {
    for &sql in ACCEPTED {
        assert!(pg_accepts(sql), "libpg_query should accept: {sql}");
        assert!(we_accept(sql), "pgparser should accept (PG does): {sql}");
    }
}

#[test]
fn agreement_on_rejected() {
    for &sql in REJECTED {
        assert!(!pg_accepts(sql), "libpg_query should reject: {sql}");
        assert!(!we_accept(sql), "pgparser should reject (PG does): {sql}");
    }
}
```

- [ ] **Step 3: Run the oracle**

Run: `cargo test -p pgparser --features oracle`
Expected: both tests pass. If a slice statement we accept is rejected by
libpg_query (or vice versa), the divergence is a real parser bug — fix the
parser, do NOT delete the corpus entry. (If a REJECTED case turns out to be
valid PostgreSQL, it doesn't belong in the slice's reject set — move it; report
which.)

- [ ] **Step 4: Confirm the default gate is unaffected**

Run: `cargo deny check bans 2>&1 | tail -2 && ./scripts/check-no-native.sh`
Expected: `bans ok` and `OK: shipped dependency tree is pure Rust` — `pg_query`/
`cc` are absent from the default graph (optional, feature-off) and from the
binary tree. If `cargo deny check bans` DOES flag `cc` via the optional dep,
add `{ crate = "cc" }` is already banned — instead scope it: in `deny.toml`
under `[bans]` the simplest robust fix is to confirm cargo resolved without the
optional dep (`cargo tree -e no-dev -i cc` should be empty). Report what you
observed; do not relax the ban for the shipped tree.

- [ ] **Step 5: Commit**

```bash
git add crates/pgparser Cargo.lock
git commit -m "test(pgparser): libpg_query differential oracle (feature-gated)"
```

---

### Task 14: executor — SqlEngine skeleton, error mapping, CREATE/DROP

**Files:**
- Modify: `crates/executor/src/lib.rs`
- Create: `crates/executor/src/error.rs`, `crates/executor/src/exec.rs`

- [ ] **Step 1: Failing test** — `crates/executor/src/exec.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use crate::SqlEngine;
    use pgwire::engine::{Engine, QueryResult};

    async fn run(engine: &SqlEngine, sql: &str) -> Vec<QueryResult> {
        engine.simple_query(sql).await.expect("ok")
    }

    #[tokio::test]
    async fn create_then_drop_table() {
        let engine = SqlEngine::new();
        let r = run(&engine, "CREATE TABLE t (id int4, name text)").await;
        assert_eq!(r, vec![QueryResult::Command { tag: "CREATE TABLE".into() }]);
        // Re-creating is a duplicate error (42P07), session survives.
        let err = engine.simple_query("CREATE TABLE t (id int4)").await.expect_err("dup");
        assert_eq!(err.code, "42P07");
        let r = run(&engine, "DROP TABLE t").await;
        assert_eq!(r, vec![QueryResult::Command { tag: "DROP TABLE".into() }]);
        let err = engine.simple_query("DROP TABLE t").await.expect_err("gone");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn empty_query_yields_empty_result() {
        let engine = SqlEngine::new();
        assert_eq!(run(&engine, "   ").await, vec![QueryResult::Empty]);
    }

    #[tokio::test]
    async fn syntax_error_is_42601() {
        let engine = SqlEngine::new();
        let err = engine.simple_query("SELCT 1").await.expect_err("syntax");
        assert_eq!(err.code, "42601");
    }
}
```

(Add `tokio = { workspace = true, features = ["macros", "rt"] }` to executor's
dev-dependencies if not already present from Task 1 — Task 1 set `features =
["full"]`, which covers `macros` + `rt`.)

- [ ] **Step 2:** `cargo test -p executor` → COMPILE FAIL.

- [ ] **Step 3: Implement** `error.rs`:

```rust
//! Map lower-crate error enums onto wire `PgError`s with the right SQLSTATE.

use catalog::CatalogError;
use kv::KvError;
use pgparser::ParseError;
use pgtypes::TypeError;
use pgwire::error::PgError;

/// Executor-level error; converts to a non-fatal `PgError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    Parse(ParseError),
    Catalog(CatalogError),
    Type(TypeError),
    Kv(KvError),
    /// Column referenced that the row/table doesn't have (42703).
    UndefinedColumn(String),
    /// In-grammar but unimplemented (0A000) — e.g. $1 parameters.
    Unsupported(String),
    /// Wrong type in a context that demands a specific one (42804) — e.g. a
    /// non-boolean WHERE.
    TypeMismatch(String),
}

impl ExecError {
    pub fn into_pg(self) -> PgError {
        match self {
            ExecError::Parse(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Catalog(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Type(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Kv(e) => PgError::error("XX000", e.to_string()),
            ExecError::UndefinedColumn(c) => {
                PgError::error("42703", format!("column \"{c}\" does not exist"))
            }
            ExecError::Unsupported(m) => PgError::error("0A000", m),
            ExecError::TypeMismatch(m) => PgError::error("42804", m),
        }
    }
}

impl From<ParseError> for ExecError {
    fn from(e: ParseError) -> Self {
        ExecError::Parse(e)
    }
}
impl From<CatalogError> for ExecError {
    fn from(e: CatalogError) -> Self {
        ExecError::Catalog(e)
    }
}
impl From<TypeError> for ExecError {
    fn from(e: TypeError) -> Self {
        ExecError::Type(e)
    }
}
impl From<KvError> for ExecError {
    fn from(e: KvError) -> Self {
        ExecError::Kv(e)
    }
}
```

`lib.rs`:

```rust
//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. The real engine behind the wire protocol for SP2.

mod error;
mod eval;
mod exec;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use catalog::{Catalog, TableId};
use kv::{Kv, MemKv};
use pgwire::engine::{Engine, FieldDescription, QueryResult};
use pgwire::error::PgError;

pub use error::ExecError;

/// The SQL engine: a catalog, a KV store, and per-table rowid counters.
pub struct SqlEngine {
    catalog: Arc<Catalog>,
    kv: Arc<dyn Kv>,
    rowids: Mutex<HashMap<TableId, u64>>,
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

    pub fn with_kv(kv: Arc<dyn Kv>) -> Self {
        Self { catalog: Arc::new(Catalog::new()), kv, rowids: Mutex::new(HashMap::new()) }
    }

    /// Allocate the next rowid for a table (monotonic per table).
    pub(crate) fn next_rowid(&self, table: TableId) -> u64 {
        let mut ids = self.rowids.lock().expect("rowid lock");
        let n = ids.entry(table).or_insert(1);
        let id = *n;
        *n += 1;
        id
    }
}

impl Engine for SqlEngine {
    async fn simple_query(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(exec::execute(self, &stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        exec::describe(self, sql).map_err(ExecError::into_pg)
    }
}
```

`exec.rs` (skeleton + DDL; INSERT/SELECT/describe land in Tasks 16-18):

```rust
//! Per-statement execution.

use catalog::Column;
use pgparser::ast::Statement;
use pgwire::engine::QueryResult;

use crate::SqlEngine;
use crate::error::ExecError;

pub(crate) fn execute(engine: &SqlEngine, stmt: &Statement) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns
                .iter()
                .map(|c| Column { name: c.name.clone(), ty: c.ty })
                .collect();
            engine.catalog.create_table(name, cols)?;
            Ok(QueryResult::Command { tag: "CREATE TABLE".into() })
        }
        Statement::DropTable { name } => {
            engine.catalog.drop_table(name)?;
            Ok(QueryResult::Command { tag: "DROP TABLE".into() })
        }
        Statement::Insert { .. } => Err(ExecError::Unsupported("INSERT lands in Task 16".into())),
        Statement::Select(_) => Err(ExecError::Unsupported("SELECT lands in Task 17".into())),
    }
}

pub(crate) fn describe(
    engine: &SqlEngine,
    sql: &str,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let _ = (engine, sql);
    Ok(Vec::new()) // real describe lands in Task 18
}
```

`engine.catalog` and `engine.kv` are private fields accessed from the sibling
`exec` module — that works because `exec` is a child module of the crate root
where `SqlEngine` is defined. To allow it, make the fields `pub(crate)`:
change `catalog`, `kv`, `rowids` in `lib.rs` to `pub(crate)`.

Create an empty `crates/executor/src/eval.rs` with `//! Filled in Task 15.` so
`mod eval;` compiles.

- [ ] **Step 4:** `cargo test -p executor` → 3 passed (create/drop, empty, syntax).

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/executor
git commit -m "feat(executor): SqlEngine skeleton, error mapping, CREATE/DROP TABLE"
```

---

### Task 15: executor — expression evaluator + static type inference

**Files:**
- Modify: `crates/executor/src/eval.rs`

- [ ] **Step 1: Failing tests** — `eval.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{Column, Table};
    use pgparser::parser::parse_expr_for_test as pexpr;
    use pgtypes::{ColumnType, Datum};

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column { name: "a".into(), ty: ColumnType::Int4 },
                Column { name: "b".into(), ty: ColumnType::Int4 },
            ],
        }
    }

    fn ev(sql: &str, t: Option<&Table>, vals: &[Datum]) -> Datum {
        eval(&pexpr(sql).unwrap(), t, vals).expect("eval")
    }

    #[test]
    fn arithmetic_and_columns() {
        let t = table();
        assert_eq!(ev("a + b * 2", Some(&t), &[Datum::Int4(3), Datum::Int4(4)]), Datum::Int4(11));
    }

    #[test]
    fn comparison_yields_bool_and_null() {
        let t = table();
        assert_eq!(ev("a > b", Some(&t), &[Datum::Int4(2), Datum::Int4(1)]), Datum::Bool(true));
        assert_eq!(ev("a > b", Some(&t), &[Datum::Null, Datum::Int4(1)]), Datum::Null);
    }

    #[test]
    fn literals_no_table() {
        assert_eq!(ev("1 + 1", None, &[]), Datum::Int4(2));
        assert_eq!(ev("'x'", None, &[]), Datum::Text("x".into()));
        assert_eq!(ev("not true", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn undefined_column_is_42703() {
        let t = table();
        let err = eval(&pexpr("zzz").unwrap(), Some(&t), &[Datum::Int4(1), Datum::Int4(1)]).unwrap_err();
        assert_eq!(err.into_pg().code, "42703");
    }

    #[test]
    fn parameter_is_0a000() {
        let err = eval(&pexpr("$1").unwrap(), None, &[]).unwrap_err();
        assert_eq!(err.into_pg().code, "0A000");
    }

    #[test]
    fn type_inference_is_static() {
        let t = table();
        assert_eq!(infer_type(&pexpr("a + b").unwrap(), Some(&t)).unwrap(), ColumnType::Int4);
        assert_eq!(infer_type(&pexpr("a > b").unwrap(), Some(&t)).unwrap(), ColumnType::Bool);
        assert_eq!(infer_type(&pexpr("'x'").unwrap(), None).unwrap(), ColumnType::Text);
        assert_eq!(infer_type(&pexpr("2147483648").unwrap(), None).unwrap(), ColumnType::Int8);
    }
}
```

This imports `pgparser::parser::parse_expr_for_test`, which Task 11 already
defines as `#[doc(hidden)] pub` precisely so it's reachable from the executor
crate's tests. No further change to pgparser is needed.

- [ ] **Step 2:** `cargo test -p executor -- eval::` → COMPILE FAIL.

- [ ] **Step 3: Implement** `eval.rs`:

```rust
//! Expression evaluation over Datums, plus static result-type inference (used
//! to build a stable RowDescription before any row is produced).

use std::cmp::Ordering;

use catalog::Table;
use pgparser::ast::{BinaryOp, Expr, UnaryOp};
use pgtypes::{ColumnType, Datum, ops};

use crate::error::ExecError;

/// Evaluate `expr` against a row (`values`, aligned to `table.columns`).
pub(crate) fn eval(expr: &Expr, table: Option<&Table>, values: &[Datum]) -> Result<Datum, ExecError> {
    match expr {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported("query parameters ($n) are not supported".into())),
        Expr::Column(name) => {
            let t = table.ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            let idx = t.column_index(name).ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            Ok(values[idx].clone())
        }
        Expr::Unary { op, expr } => {
            let v = eval(expr, table, values)?;
            match op {
                UnaryOp::Not => Ok(ops::not(&v)?),
                UnaryOp::Neg => Ok(ops::sub(&Datum::Int4(0), &v)?),
            }
        }
        Expr::Binary { op, left, right } => {
            let l = eval(left, table, values)?;
            let r = eval(right, table, values)?;
            match op {
                BinaryOp::Add => Ok(ops::add(&l, &r)?),
                BinaryOp::Sub => Ok(ops::sub(&l, &r)?),
                BinaryOp::Mul => Ok(ops::mul(&l, &r)?),
                BinaryOp::Div => Ok(ops::div(&l, &r)?),
                BinaryOp::And => Ok(ops::and(&l, &r)?),
                BinaryOp::Or => Ok(ops::or(&l, &r)?),
                BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
                    let ord = ops::compare(&l, &r)?;
                    Ok(cmp_result(*op, ord))
                }
            }
        }
    }
}

fn cmp_result(op: BinaryOp, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(o) => {
            let holds = match op {
                BinaryOp::Eq => o == Ordering::Equal,
                BinaryOp::Ne => o != Ordering::Equal,
                BinaryOp::Lt => o == Ordering::Less,
                BinaryOp::Le => o != Ordering::Greater,
                BinaryOp::Gt => o == Ordering::Greater,
                BinaryOp::Ge => o != Ordering::Less,
                _ => unreachable!("cmp_result called with non-comparison op"),
            };
            Datum::Bool(holds)
        }
    }
}

/// Statically infer the result column type of an expression, for RowDescription.
pub(crate) fn infer_type(expr: &Expr, table: Option<&Table>) -> Result<ColumnType, ExecError> {
    match expr {
        Expr::IntLiteral(s) => match ops::int_literal(s)? {
            Datum::Int4(_) => Ok(ColumnType::Int4),
            Datum::Int8(_) => Ok(ColumnType::Int8),
            _ => unreachable!(),
        },
        Expr::StringLiteral(_) => Ok(ColumnType::Text),
        Expr::BoolLiteral(_) => Ok(ColumnType::Bool),
        // PostgreSQL types a bare NULL as "unknown"; the slice uses text as a
        // concrete stand-in so RowDescription has a real OID.
        Expr::NullLiteral => Ok(ColumnType::Text),
        Expr::Param(_) => Err(ExecError::Unsupported("query parameters ($n) are not supported".into())),
        Expr::Column(name) => {
            let t = table.ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            let idx = t.column_index(name).ok_or_else(|| ExecError::UndefinedColumn(name.clone()))?;
            Ok(t.columns[idx].ty)
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => Ok(ColumnType::Bool),
            UnaryOp::Neg => infer_type(expr, table),
        },
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                let (lt, rt) = (infer_type(left, table)?, infer_type(right, table)?);
                Ok(if lt == ColumnType::Int4 && rt == ColumnType::Int4 {
                    ColumnType::Int4
                } else {
                    ColumnType::Int8
                })
            }
            _ => Ok(ColumnType::Bool),
        },
    }
}
```

- [ ] **Step 4:** `cargo test -p executor -- eval::` → 6 passed.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/executor crates/pgparser
git commit -m "feat(executor): expression evaluator and static type inference"
```

---

### Task 16: executor — INSERT

**Files:**
- Modify: `crates/executor/src/exec.rs`

- [ ] **Step 1: Failing tests** — add to `exec.rs` test module:

```rust
    use pgtypes::Datum;

    #[tokio::test]
    async fn insert_then_count_via_kv() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let r = run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
        assert_eq!(r, vec![QueryResult::Command { tag: "INSERT 0 2".into() }]);
        // A third single-row insert with explicit columns.
        let r = run(&engine, "INSERT INTO t (name, id) VALUES ('c', 3)").await;
        assert_eq!(r, vec![QueryResult::Command { tag: "INSERT 0 1".into() }]);
    }

    #[tokio::test]
    async fn insert_widens_int4_to_int8_column() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (big int8)").await;
        run(&engine, "INSERT INTO t VALUES (5)").await;
        // Round-trips through SELECT in Task 17; here just assert no error.
    }

    #[tokio::test]
    async fn insert_type_mismatch_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (flag bool)").await;
        let err = engine.simple_query("INSERT INTO t VALUES (1)").await.expect_err("mismatch");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    async fn insert_into_missing_table_is_42P01() {
        let engine = SqlEngine::new();
        let err = engine.simple_query("INSERT INTO nope VALUES (1)").await.expect_err("no table");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn insert_wrong_arity_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        let err = engine.simple_query("INSERT INTO t VALUES (1)").await.expect_err("arity");
        assert_eq!(err.code, "42804");
    }
```

- [ ] **Step 2:** `cargo test -p executor -- insert` → FAIL (Insert returns Unsupported).

- [ ] **Step 3: Implement** in `exec.rs` — replace the `Statement::Insert` arm and
add the `coerce` helper:

```rust
        Statement::Insert { table, columns, rows } => {
            let t = engine.catalog.get_table(table)?;
            let target_idx: Vec<usize> = match columns {
                Some(cols) => cols
                    .iter()
                    .map(|c| t.column_index(c).ok_or_else(|| ExecError::UndefinedColumn(c.clone())))
                    .collect::<Result<_, _>>()?,
                None => (0..t.columns.len()).collect(),
            };
            let mut n: u64 = 0;
            for row_exprs in rows {
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
                for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
                    // VALUES expressions are literal (no FROM/columns in scope).
                    let v = crate::eval::eval(expr, None, &[])?;
                    full[*slot] = coerce(v, t.columns[*slot].ty)?;
                }
                let rowid = engine.next_rowid(t.id);
                engine.kv.put(kv::key::row_key(t.id, rowid), kv::rowenc::encode_row(&full));
                n += 1;
            }
            Ok(QueryResult::Command { tag: format!("INSERT 0 {n}") })
        }
```

Add at the bottom of `exec.rs`:

```rust
/// Coerce an evaluated value into a target column type (assignment context).
fn coerce(value: pgtypes::Datum, target: pgtypes::ColumnType) -> Result<pgtypes::Datum, ExecError> {
    use pgtypes::{ColumnType, Datum, TypeError};
    Ok(match (value, target) {
        (Datum::Null, _) => Datum::Null,
        (Datum::Bool(b), ColumnType::Bool) => Datum::Bool(b),
        (Datum::Int4(n), ColumnType::Int4) => Datum::Int4(n),
        (Datum::Int4(n), ColumnType::Int8) => Datum::Int8(i64::from(n)),
        (Datum::Int8(n), ColumnType::Int8) => Datum::Int8(n),
        (Datum::Int8(n), ColumnType::Int4) => {
            i32::try_from(n).map(Datum::Int4).map_err(|_| TypeError::Overflow)?
        }
        (Datum::Text(s), ColumnType::Text) => Datum::Text(s),
        (v, target) => {
            return Err(ExecError::TypeMismatch(format!(
                "column is of type {} but expression is of type {}",
                target.name(),
                v.column_type().map(|t| t.name()).unwrap_or("unknown"),
            )));
        }
    })
}
```

- [ ] **Step 4:** `cargo test -p executor` → all pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/executor
git commit -m "feat(executor): INSERT with assignment coercion"
```

---

### Task 17: executor — SELECT (scan, WHERE, projection, ORDER BY, LIMIT)

**Files:**
- Modify: `crates/executor/src/exec.rs`

- [ ] **Step 1: Failing tests** — add to `exec.rs` test module:

```rust
    use pgwire::engine::{Cell, FieldDescription};

    fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
        match r {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn fields_of(r: &QueryResult) -> &Vec<FieldDescription> {
        match r {
            QueryResult::Rows { fields, .. } => fields,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn text(cell: &Option<Cell>) -> Option<String> {
        cell.as_ref().map(|c| String::from_utf8(c.text.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn select_literal_no_from() {
        let engine = SqlEngine::new();
        let r = &run(&engine, "SELECT 1 + 1 AS two").await[0];
        assert_eq!(fields_of(r)[0].name, "two");
        assert_eq!(fields_of(r)[0].type_oid, pgtypes::oids::INT4);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn select_where_order_limit() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
        let r = &run(&engine, "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5").await[0];
        let rows = rows_of(r);
        assert_eq!(rows.len(), 2);
        assert_eq!(text(&rows[0][0]), Some("c".into())); // id=3 first (DESC)
        assert_eq!(text(&rows[1][0]), Some("b".into()));
    }

    #[tokio::test]
    async fn select_star_projects_all_columns() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (7,'x')").await;
        let r = &run(&engine, "SELECT * FROM t").await[0];
        assert_eq!(fields_of(r).iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), vec!["id", "name"]);
        assert_eq!(text(&rows_of(r)[0][0]), Some("7".into()));
        assert_eq!(text(&rows_of(r)[0][1]), Some("x".into()));
    }

    #[tokio::test]
    async fn select_command_tag_counts_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(2)").await;
        match &run(&engine, "SELECT id FROM t").await[0] {
            QueryResult::Rows { tag, .. } => assert_eq!(tag, "SELECT 2"),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_boolean_where_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let err = engine.simple_query("SELECT id FROM t WHERE id").await.expect_err("non-bool");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    async fn null_orders_last_ascending() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (2),(null),(1)").await;
        let r = &run(&engine, "SELECT id FROM t ORDER BY id ASC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("1".into()), Some("2".into()), None]); // NULLS LAST
    }
```

- [ ] **Step 2:** `cargo test -p executor -- select` → FAIL (Select returns Unsupported).

- [ ] **Step 3: Implement** in `exec.rs` — replace the `Statement::Select` arm and
add helpers:

```rust
        Statement::Select(s) => exec_select(engine, s),
```

```rust
use bytes::Bytes;
use catalog::Table;
use pgparser::ast::{Expr, SelectItem, SelectStmt};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::{Cell, FieldDescription};

fn exec_select(engine: &SqlEngine, s: &SelectStmt) -> Result<QueryResult, ExecError> {
    let table: Option<Table> = match &s.from {
        Some(name) => Some(engine.catalog.get_table(name)?),
        None => None,
    };

    // Source rows: scan the table, or a single empty row for FROM-less SELECT.
    let source: Vec<Vec<Datum>> = match &table {
        Some(t) => engine
            .kv
            .scan_prefix(&kv::key::table_prefix(t.id))
            .into_iter()
            .map(|(_, v)| kv::rowenc::decode_row(&v))
            .collect::<Result<_, _>>()?,
        None => vec![vec![]],
    };

    // Resolve the projection into (field, expr) pairs.
    let (fields, out_exprs) = resolve_projection(&s.projection, table.as_ref())?;

    // Filter, keeping each surviving source row for ORDER BY evaluation.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for row in &source {
        let keep = match &s.filter {
            None => true,
            Some(f) => match crate::eval::eval(f, table.as_ref(), row)? {
                Datum::Bool(b) => b,
                Datum::Null => false,
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "argument of WHERE must be type boolean".into(),
                    ));
                }
            },
        };
        if keep {
            kept.push(row.clone());
        }
    }

    // ORDER BY: sort by evaluated order keys (over the source row).
    if !s.order_by.is_empty() {
        // Precompute keys to keep comparisons total and error-free during sort.
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        for row in kept {
            let mut keys = Vec::with_capacity(s.order_by.len());
            for item in &s.order_by {
                keys.push(crate::eval::eval(&item.expr, table.as_ref(), &row)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, s));
        kept = keyed.into_iter().map(|(_, row)| row).collect();
    }

    // LIMIT.
    if let Some(limit) = s.limit {
        let n = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        kept.truncate(n);
    }

    // Project + encode to cells.
    let mut out_rows: Vec<Vec<Option<Cell>>> = Vec::with_capacity(kept.len());
    for row in &kept {
        let mut cells = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            let d = crate::eval::eval(e, table.as_ref(), row)?;
            cells.push(datum_to_cell(&d));
        }
        out_rows.push(cells);
    }

    let tag = format!("SELECT {}", out_rows.len());
    Ok(QueryResult::Rows { fields, rows: out_rows, tag })
}

/// Expand the projection list into output FieldDescriptions and the expressions
/// that produce each column.
fn resolve_projection(
    items: &[SelectItem],
    table: Option<&Table>,
) -> Result<(Vec<FieldDescription>, Vec<Expr>), ExecError> {
    // SELECT * requires a FROM.
    if items == [SelectItem::Wildcard] {
        let t = table.ok_or_else(|| {
            ExecError::Unsupported("SELECT * with no FROM clause is not supported".into())
        })?;
        let fields = t.columns.iter().map(|c| field(&c.name, c.ty)).collect();
        let exprs = t.columns.iter().map(|c| Expr::Column(c.name.clone())).collect();
        return Ok((fields, exprs));
    }
    let mut fields = Vec::with_capacity(items.len());
    let mut exprs = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard => {
                return Err(ExecError::Unsupported("* mixed with other items is not supported".into()));
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                let ty = crate::eval::infer_type(expr, table)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
            }
        }
    }
    Ok((fields, exprs))
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(c) => c.clone(),
        _ => "?column?".to_string(),
    }
}

fn field(name: &str, ty: ColumnType) -> FieldDescription {
    FieldDescription {
        name: name.to_string(),
        table_oid: 0,
        column_id: 0,
        type_oid: ty.oid(),
        type_size: ty.type_size(),
        type_modifier: -1,
        format: 0,
    }
}

fn datum_to_cell(d: &Datum) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    Some(Cell {
        text: Bytes::from(pgtypes::encoding::encode_text(d)),
        binary: Bytes::from(pgtypes::encoding::encode_binary(d)),
    })
}

/// Compare two order-key vectors per the SELECT's ASC/DESC flags, with PG's
/// default null placement (NULLS LAST for ASC, NULLS FIRST for DESC).
fn order_cmp(a: &[Datum], b: &[Datum], s: &SelectStmt) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in s.order_by.iter().enumerate() {
        let (x, y) = (&a[i], &b[i]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            // NULLS LAST for ASC: null is "greater"; NULLS FIRST for DESC.
            (true, false) => if item.asc { Ordering::Greater } else { Ordering::Less },
            (false, true) => if item.asc { Ordering::Less } else { Ordering::Greater },
            (false, false) => {
                let base = pgtypes::ops::compare(x, y).ok().flatten().unwrap_or(Ordering::Equal);
                if item.asc { base } else { base.reverse() }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
```

(Place the `use` lines at the top of `exec.rs` with the existing imports; keep
one copy of each.)

- [ ] **Step 4:** `cargo test -p executor` → all pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/executor
git commit -m "feat(executor): SELECT with WHERE, projection, ORDER BY, LIMIT"
```

---

### Task 18: executor — describe() (lazy, real type inference)

**Files:**
- Modify: `crates/executor/src/exec.rs`

- [ ] **Step 1: Failing test** — add to `exec.rs` test module:

```rust
    #[tokio::test]
    async fn describe_select_returns_field_types_without_executing() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let fields = engine.describe("SELECT id, name FROM t").await.expect("describe");
        assert_eq!(fields.iter().map(|f| f.type_oid).collect::<Vec<_>>(),
            vec![pgtypes::oids::INT4, pgtypes::oids::TEXT]);
    }

    #[tokio::test]
    async fn describe_non_select_has_no_fields() {
        let engine = SqlEngine::new();
        let fields = engine.describe("CREATE TABLE t (id int4)").await.expect("describe");
        assert!(fields.is_empty());
    }
```

- [ ] **Step 2:** `cargo test -p executor -- describe` → FAIL (describe returns empty).

- [ ] **Step 3: Implement** — replace the placeholder `describe` in `exec.rs`:

```rust
pub(crate) fn describe(
    engine: &SqlEngine,
    sql: &str,
) -> Result<Vec<FieldDescription>, ExecError> {
    let statements = pgparser::parse(sql)?;
    // Extended-protocol Describe targets a single statement.
    let Some(Statement::Select(s)) = statements.first() else {
        return Ok(Vec::new()); // non-SELECT (or empty) returns no row description
    };
    let table = match &s.from {
        Some(name) => Some(engine.catalog.get_table(name)?),
        None => None,
    };
    let (fields, _exprs) = resolve_projection(&s.projection, table.as_ref())?;
    Ok(fields)
}
```

- [ ] **Step 4:** `cargo test -p executor` → all pass.

- [ ] **Step 5:** fmt + clippy, then:

```bash
git add crates/executor
git commit -m "feat(executor): lazy describe() with real type inference"
```

---

### Task 19: Wire SqlEngine into the binary + end-to-end test

**Files:**
- Modify: `crates/crabgresql/Cargo.toml` (add `executor`, `rand`), `crates/crabgresql/src/main.rs`
- Create: `crates/executor/tests/end_to_end.rs`

- [ ] **Step 1: Failing e2e test** — `crates/executor/tests/end_to_end.rs`:

```rust
use std::sync::Arc;

use executor::SqlEngine;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
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
async fn create_insert_select_roundtrip() {
    let client = connect(spawn().await).await;
    client.batch_execute("CREATE TABLE t (id int4, name text)").await.expect("create");
    client.batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await.expect("insert");
    // Extended protocol with binary results (exercises describe + binary cells).
    let rows = client
        .query("SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let first: &str = rows[0].get(0);
    let second: &str = rows[1].get(0);
    assert_eq!((first, second), ("c", "b"));
}

#[tokio::test]
async fn select_expression_typed_int4() {
    let client = connect(spawn().await).await;
    let rows = client.query("SELECT 2 + 3 AS five", &[]).await.expect("select");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 5);
}

#[tokio::test]
async fn undefined_table_errors_but_session_survives() {
    let client = connect(spawn().await).await;
    let err = client.batch_execute("SELECT * FROM nope").await.expect_err("no table");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P01");
    // Session still usable.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}
```

- [ ] **Step 2:** `cargo test -p executor --test end_to_end` → COMPILE FAIL (dev-deps
`tokio`, `tokio-postgres` are present from Task 1; this should compile and then
the tests run against the real engine — they should PASS once the engine is
complete, which it is after Task 18). If they pass immediately, good — this test
is the integration proof. If a test fails, fix the executor, not the test.

- [ ] **Step 3: Swap the engine in the binary.** In `crates/crabgresql/Cargo.toml`
add to `[dependencies]`:

```toml
executor.workspace = true
rand.workspace = true
```

In `main.rs`, replace `use pgwire::stub::StubEngine;` with
`use executor::SqlEngine;` and change the `serve`/`serve_tls` call's engine
argument from `Arc::new(StubEngine::new())` to `Arc::new(SqlEngine::new())`.
(`rand` was already added to the binary's deps in Task 2; keep the
`executor.workspace = true` line above.)

`build_session_config` already derives SCRAM verifiers — that was completed in
Task 2. No auth changes are needed here; this task only swaps the engine.

- [ ] **Step 4: Verify the binary + smoke tests**

Run: `cargo build -p crabgresql && ./scripts/psql-smoke.sh`
Expected: all three smoke legs PASS — psql now talks to the real engine; `SELECT
1` flows through pgparser → executor. (`SELECT 1` is a FROM-less literal select,
which the executor handles.)

- [ ] **Step 5: Run the e2e tests + gauntlet**

Run: `cargo test -p executor && cargo clippy --workspace --all-targets -- -D warnings && ./scripts/check-no-native.sh`
Expected: green — and `check-no-native.sh` confirms the now-larger `crabgresql`
binary tree (pgtypes/kv/catalog/pgparser/executor) is still pure Rust.

- [ ] **Step 6: Commit**

```bash
git add crates/crabgresql crates/executor Cargo.toml Cargo.lock
git commit -m "feat(crabgresql): serve the real SQL engine; end-to-end round-trip tests"
```

---

### Task 20: conformance — dollar-quote splitter + pg_regress-derived corpus (carry-over)

**Files:**
- Modify: `crates/conformance/src/lib.rs` (dollar-quote aware `split_statements`)
- Create: `crates/conformance/corpus/int4_arith.sql`, `crates/conformance/corpus/boolean_logic.sql`

- [ ] **Step 1: Failing splitter test** — add to `conformance/src/lib.rs` tests:

```rust
    #[test]
    fn dollar_quoted_body_is_not_split_on_inner_semicolons() {
        let sql = "SELECT 1;\nDO $$ BEGIN x; y; END $$;\nSELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 1", "DO $$ BEGIN x; y; END $$", "SELECT 2"]
        );
    }

    #[test]
    fn tagged_dollar_quote_is_matched_by_tag() {
        let sql = "SELECT $tag$a;b$tag$ ; SELECT 2";
        assert_eq!(split_statements(sql), vec!["SELECT $tag$a;b$tag$", "SELECT 2"]);
    }
```

- [ ] **Step 2:** `cargo test -p conformance -- dollar` → FAIL (splitter ignores `$$`).

- [ ] **Step 3: Implement** dollar-quote handling in `split_statements`. Extend the
existing scanner: when not in a single/double-quoted string or line comment and a
`$` begins a valid dollar-quote tag (`$$` or `$tag$` where tag is letters/digits/
underscore starting non-digit), consume to the matching closing tag, treating
everything between (including semicolons) as literal. Replace the function body
with:

```rust
pub fn split_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < bytes.len() {
        let c = bytes[i];
        // Line comment (outside strings).
        if !in_single && !in_double && c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Dollar-quoted string (outside other strings).
        if !in_single && !in_double && c == b'$' {
            if let Some(tag_len) = dollar_tag_len(&bytes[i..]) {
                let tag = &sql[i..i + tag_len];
                current.push_str(tag);
                i += tag_len;
                // Consume until the matching closing tag.
                loop {
                    if i >= bytes.len() {
                        break; // unterminated; emit what we have
                    }
                    if sql[i..].starts_with(tag) {
                        current.push_str(tag);
                        i += tag_len;
                        break;
                    }
                    current.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
        }
        match c {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b';' if !in_single && !in_double => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
                i += 1;
                continue;
            }
            _ => {}
        }
        current.push(c as char);
        i += 1;
    }
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }
    statements
}

/// If `s` begins with a dollar-quote opening tag (`$$` or `$tag$`), return its
/// byte length, else None. A tag body is `[A-Za-z_][A-Za-z0-9_]*`.
fn dollar_tag_len(s: &[u8]) -> Option<usize> {
    if s.first() != Some(&b'$') {
        return None;
    }
    let mut j = 1;
    if s.get(j) == Some(&b'$') {
        return Some(2); // `$$`
    }
    // First tag char must be a letter or underscore.
    match s.get(j) {
        Some(&b) if b == b'_' || b.is_ascii_alphabetic() => {}
        _ => return None,
    }
    j += 1;
    while let Some(&b) = s.get(j) {
        if b == b'_' || b.is_ascii_alphanumeric() {
            j += 1;
        } else {
            break;
        }
    }
    if s.get(j) == Some(&b'$') {
        Some(j + 1)
    } else {
        None
    }
}
```

(Keep the existing doubled-quote behavior: `''` toggles `in_single` off then on,
net-protecting inner semicolons — unchanged by this edit. Re-run the existing
`doubled_quotes_keep_semicolons_protected` test to confirm.)

- [ ] **Step 4: Vendor the corpus subsets.** These are curated subsets within the
SP2 slice, drawn from the shape of PostgreSQL's `src/test/regress/sql/int4.sql`
and `boolean.sql`; out-of-slice statements (table fixtures with `.*`, casts,
`\d`) are excluded — full pg_regress import is a later sub-project. Each
statement runs against both the oracle and the subject; matches lift the parity
number.

`crates/conformance/corpus/int4_arith.sql`:

```sql
-- Subset of pg_regress int4.sql: literal integer arithmetic and comparison
-- within the SP2 slice (no INT4_TBL fixture, no casts).
SELECT 2 + 2;
SELECT 4 - 1;
SELECT 3 * 4;
SELECT 12 / 4;
SELECT 7 / 2;
SELECT 2 + 2 * 3;
SELECT (2 + 2) * 3;
SELECT 1 < 2;
SELECT 2 <= 2;
SELECT 3 <> 4;
SELECT 5 = 5;
SELECT 10 > 9;
SELECT 9 >= 10;
```

`crates/conformance/corpus/boolean_logic.sql`:

```sql
-- Subset of pg_regress boolean.sql: three-valued logic within the SP2 slice.
SELECT true;
SELECT false;
SELECT true AND false;
SELECT true OR false;
SELECT NOT true;
SELECT NOT false;
SELECT true AND true;
SELECT false OR false;
SELECT 1 < 2 AND 3 > 2;
SELECT 1 = 1 OR 2 = 3;
```

- [ ] **Step 5: Run the harness end-to-end** (oracle must be up:
`./scripts/oracle-up.sh` then wait for readiness):

```bash
cargo build -p crabgresql -p conformance
./target/debug/crabgresql --listen 127.0.0.1:54336 &
sleep 1
./target/debug/conformance \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
  --subject-url "host=127.0.0.1 port=54336 user=crab dbname=crab" \
  --corpus crates/conformance/corpus --out /tmp/parity.json --summary /tmp/parity.md
kill %1
cat /tmp/parity.md
```

Expected: a materially higher parity than SP1's 20% — the int4/boolean statements
match the oracle now that the real engine evaluates them. Report the actual
percentage. (`SELECT 7 / 2` must equal PG's `3` — integer division; confirm the
row matches. `SELECT version()` from the old smoke corpus still mismatches; that
is expected.)

- [ ] **Step 6:** `cargo test -p conformance && cargo fmt --all && cargo clippy -p conformance --all-targets -- -D warnings`, then:

```bash
git add crates/conformance
git commit -m "feat(conformance): dollar-quote-aware splitter; pg_regress-derived int4/boolean corpus"
```

---

### Task 21: CI — parser oracle step + final gauntlet

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add the parser-oracle step** to the `check` job (after the test
step). libpg_query is built from source by the `pg_query` crate via a C compiler
already present on `ubuntu-latest`; no PostgreSQL install is needed for the
parser oracle:

```yaml
      - name: Parser differential oracle (libpg_query)
        run: cargo test --locked -p pgparser --features oracle
```

This step is separate from the `cargo-deny` step, which continues to run against
the default feature set (no `pg_query`, no `cc`) — so the ban on `cc` stays
intact for the shipped graph.

- [ ] **Step 2: Confirm gates locally one final time**

Run:
```
cargo test --workspace \
  && cargo test -p pgparser --features oracle \
  && cargo fmt --all --check \
  && cargo clippy --workspace --all-targets -- -D warnings \
  && ./scripts/check-no-native.sh \
  && cargo deny check bans licenses \
  && ./scripts/psql-smoke.sh
```
Expected: every gate green. `check-no-native.sh` proves the five new crates kept
the shipped tree pure Rust; `cargo deny` proves the optional `pg_query`/`cc` are
absent from the default graph.

- [ ] **Step 3: Validate the workflow YAML parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"`
Expected: no error.

- [ ] **Step 4: Commit**

```bash
git add .github
git commit -m "ci: run the libpg_query parser oracle (feature-gated)"
```

---

## Success criteria traceability (spec → tasks)

| Spec success criterion | Task(s) |
|---|---|
| psql/tokio-postgres run CREATE/INSERT/SELECT…WHERE…ORDER BY…LIMIT through the real pipeline | 14–19 |
| KV key encoding order-preservation holds under property tests | 7 (+ 8 value roundtrip) |
| pgparser matches libpg_query on the slice grammar | 13 |
| SCRAM authenticates against stored verifiers; unknown users get mock auth | 2 |
| Conformance corpus includes real-pg_regress-derived files; parity rises | 20 |
| All SP1 CI gates remain green (zero unsafe, pure-Rust shipped tree, fmt, clippy, conformance) | 1, 19, 21 |

## Notes for the implementer

- Crate versions are floors; run `cargo update` and let the lockfile settle. The
  only new shipped-graph deps are path crates (pure Rust); `pg_query` is optional
  and dev/feature-gated only.
- Every task ends green: `cargo test --workspace && cargo clippy --workspace
  --all-targets -- -D warnings`. Do not carry red between tasks.
- Tracked gaps stay out of scope (no UPDATE/DELETE, no MVCC, no joins/aggregates,
  no pg_catalog views, `$1` parameters → 0A000, only int4/int8/text/bool). The
  conformance dashboard will show them as mismatches — that is the honest signal,
  not a regression.
- The oracle container from SP1 (`crabgresql-oracle`) may need restarting via
  `./scripts/oracle-up.sh` for Task 20; CI uses its own service container.
