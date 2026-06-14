//! The remote-forward seam: a minimal, POOLED pgwire forwarding client built on
//! the existing `pgwire` frame primitives (no new dependency). When the gateway
//! is not the local leader of a statement's target range, it forwards the single
//! `Query` to that range's leader's pgwire SQL port and relays the one response
//! back.
//!
//! Pooling: one authenticated connection per remote leader node, kept inside the
//! sticky client connection and reused for later statements to the same leader.
//! The forwarding handshake is `Trust`-auth (the cluster's only mode), so the
//! client sends a StartupMessage and reads to ReadyForQuery — no SASL exchange.
//!
//! Leader resolution reuses the `route.rs` metrics-watch pattern PER RANGE: read
//! the range-`r` raft metrics, take `current_leader` + its packed `sql_addr`, and
//! DROP the `Ref` before any `.await`. A paused/partitioned leader (still
//! self-reporting `Leader` in frozen metrics) is excluded. On `NotLeader` or a
//! wire error, re-resolve the leader ONCE and retry; on exhaustion surface the
//! error to the client.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin as FuturePin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use executor::ExecError;
use pgwire::engine::{Cell, FieldDescription, QueryResult};
use pgwire::messages::frontend::PROTOCOL_3_0;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::addr::sql_addr_part;
use crate::range::map::RangeId;
use crate::range::router::RemoteForward;
use crate::transport::partition::PartitionState;
use crate::types::{NodeId, TypeConfig};

/// How long to wait, total, for a forward (dial + handshake + query) before
/// giving up the current attempt.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

/// A mechanically-observable counter of re-resolve+retries the gateway performed.
/// Cloneable; tests assert its value to prove retry behavior without racing an
/// election or timing a sleep.
#[derive(Clone, Default)]
pub struct RetryCounter(Arc<AtomicU64>);

impl RetryCounter {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn incr(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// One pooled, authenticated pgwire connection to a single remote leader's SQL
/// port. `Query`s are serialized over it (each forward reads to ReadyForQuery
/// before the next is sent).
struct PooledConn {
    addr: String,
    stream: TcpStream,
    inbuf: BytesMut,
}

/// The gateway's remote-forward pool. Holds the per-range raft handles (to
/// resolve each range's current leader's SQL addr), the node's partition state
/// (to exclude unreachable leaders), one pooled connection per remote leader
/// node, and a retry counter.
pub struct ForwardPool {
    rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    partition: PartitionState,
    /// leader NodeId -> its pooled connection. Reused across statements.
    conns: Mutex<HashMap<NodeId, PooledConn>>,
    retries: RetryCounter,
    /// TEST-ONLY one-shot: when armed for `inject_notleader`, the next forward to
    /// that range fakes a single `NotLeader` before any wire contact, then
    /// disarms. Lets a test force exactly one re-resolve+retry deterministically
    /// (no real election race).
    inject_notleader: AtomicU64,
    inject_armed: AtomicBool,
}

impl ForwardPool {
    /// Build a pool over the gateway's per-range raft handles + partition state.
    pub fn new(
        rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
        partition: PartitionState,
        retries: RetryCounter,
    ) -> Arc<Self> {
        Arc::new(Self {
            rafts,
            partition,
            conns: Mutex::new(HashMap::new()),
            retries,
            inject_notleader: AtomicU64::new(0),
            inject_armed: AtomicBool::new(false),
        })
    }

    /// TEST-ONLY: arm a single fake `NotLeader` for the next forward to `range`.
    pub fn arm_one_shot_notleader(&self, range: RangeId) {
        self.inject_notleader
            .store(u64::from(range), Ordering::SeqCst);
        self.inject_armed.store(true, Ordering::SeqCst);
    }

    /// Resolve `range`'s current leader `(node_id, sql_addr)` from the range-`r`
    /// metrics watch, excluding a paused/partitioned self-reporting leader. The
    /// `Ref` is dropped before returning (no `Ref` held across an `await`).
    fn resolve_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let metrics = raft.metrics();
        let (leader, sql) = {
            let m = metrics.borrow();
            let leader = m.current_leader;
            let sql = leader.and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .and_then(|n| sql_addr_part(&n.addr).map(str::to_string))
            });
            (leader, sql)
        }; // Ref dropped here, before any await.
        let leader = leader?;
        // A partitioned/cut leader still self-reports `Leader` in frozen metrics;
        // never forward to it (the SP13 `is_paused` lesson, on the TCP path it is
        // the node's PartitionState).
        if self.partition.blocked(leader) {
            return None;
        }
        Some((leader, sql?))
    }

    /// Resolve `range`'s leader, awaiting the metrics watch (bounded by
    /// `FORWARD_TIMEOUT`) if none is resolvable yet. A FOLLOWER gateway's metrics
    /// lag the leader's heartbeat, so a single instantaneous read can see
    /// `current_leader == None` right after an election; this waits for the next
    /// metrics change (event-driven, no fixed sleep) instead of failing the
    /// attempt. The `Ref` from `resolve_leader` is always dropped before the
    /// `.await` on `changed()`.
    async fn await_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let deadline = tokio::time::Instant::now() + FORWARD_TIMEOUT;
        loop {
            if let Some(found) = self.resolve_leader(range) {
                return Some(found);
            }
            let mut rx = raft.metrics();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            // No (reachable) leader yet: await the next metrics change rather than
            // polling. `changed()` resolves the instant `current_leader` updates.
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return None; // deadline elapsed with no resolvable leader.
            }
        }
    }

    /// Forward one `Query` to `range`'s remote leader and relay the single
    /// response back as a `QueryResult`. Bounded ONE re-resolve+retry on
    /// `NotLeader`/wire-error; on exhaustion the error is surfaced.
    pub async fn forward(&self, range: RangeId, sql: String) -> Result<QueryResult, ExecError> {
        // TEST-ONLY one-shot: fake a single NotLeader for the first try, disarm,
        // count the retry, and fall through to a real (re-resolved) second try.
        let armed = self.inject_armed.load(Ordering::SeqCst)
            && self.inject_notleader.load(Ordering::SeqCst) == u64::from(range);
        if armed {
            self.inject_armed.store(false, Ordering::SeqCst);
            self.retries.incr();
            // (No real wire contact happened; go straight to the real attempt.)
            return self.try_forward(range, &sql).await;
        }

        match self.try_forward(range, &sql).await {
            Ok(r) => Ok(r),
            // A genuine NotLeader/wire-error: re-resolve once and retry.
            Err(ExecError::NotLeader) | Err(ExecError::Unavailable) => {
                self.retries.incr();
                self.try_forward(range, &sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// One attempt: resolve the leader, get/open its pooled connection, send the
    /// `Query`, read to `ReadyForQuery`, map the frames to a `QueryResult`. A
    /// poisoned pooled connection is dropped so the retry redials.
    async fn try_forward(&self, range: RangeId, sql: &str) -> Result<QueryResult, ExecError> {
        let (leader, addr) = self.await_leader(range).await.ok_or(ExecError::NotLeader)?;

        let mut conns = self.conns.lock().await;
        // (Re)dial when there is no pooled conn for this leader, or the pooled
        // conn's addr no longer matches the current leader (the leader moved); the
        // existing conn (if any) is dropped by the overwrite.
        let needs_dial = conns.get(&leader).is_none_or(|c| c.addr != addr);
        if needs_dial {
            let conn = open_pooled(&addr)
                .await
                .map_err(|_| ExecError::Unavailable)?;
            conns.insert(leader, conn);
        }
        let conn = conns.get_mut(&leader).expect("pooled conn present");

        match send_query(conn, sql).await {
            Ok(result) => Ok(result),
            Err(ForwardErr::Sql(code, msg)) => {
                // `40001`/`08006` are the leader's own NotLeader/Unavailable wire
                // codes (executor::ExecError::into_pg) — retryable redirects.
                if code == "40001" {
                    Err(ExecError::NotLeader)
                } else if code == "08006" {
                    conns.remove(&leader); // upstream lost; redial on retry.
                    Err(ExecError::Unavailable)
                } else {
                    Err(ExecError::Unsupported(format!("remote {code}: {msg}")))
                }
            }
            Err(ForwardErr::Wire) => {
                conns.remove(&leader); // poisoned stream; redial on retry.
                Err(ExecError::Unavailable)
            }
        }
    }
}

/// The canonical Task-3 forward seam: a `RemoteForward` impl backed by the pooled
/// pgwire client. `RangeRouter::new` takes `Arc<dyn RemoteForward>`, so the gateway
/// wires `Arc::new(PgwireForward { pool })` rather than a closure. `forward()` is a
/// thin delegate to `ForwardPool::forward` (which owns leader resolution + the
/// bounded one re-resolve+retry).
pub struct PgwireForward {
    pub pool: Arc<ForwardPool>,
}

impl RemoteForward for PgwireForward {
    fn forward<'a>(
        &'a self,
        range: RangeId,
        sql: &'a str,
    ) -> FuturePin<Box<dyn Future<Output = Result<QueryResult, ExecError>> + Send + 'a>> {
        Box::pin(async move { self.pool.forward(range, sql.to_string()).await })
    }
}

/// A forward attempt's failure: a structured upstream ErrorResponse (with its
/// SQLSTATE) or a transport-level wire failure.
enum ForwardErr {
    Sql(String, String),
    Wire,
}

/// Dial the leader's SQL port and complete the `Trust`-auth startup handshake:
/// send StartupMessage(user=postgres), read backend frames until the first
/// `ReadyForQuery` ('Z'). AuthenticationOk/ParameterStatus/BackendKeyData are
/// consumed and discarded.
async fn open_pooled(addr: &str) -> std::io::Result<PooledConn> {
    let mut stream = tokio::time::timeout(FORWARD_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "dial timeout"))??;

    // StartupMessage: int32 len, int32 protocol, then NUL-terminated key/value
    // pairs, then a final NUL.
    let mut body = BytesMut::new();
    body.put_i32(PROTOCOL_3_0);
    for (k, v) in [("user", "postgres"), ("database", "postgres")] {
        body.put_slice(k.as_bytes());
        body.put_u8(0);
        body.put_slice(v.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0); // params terminator
    let mut startup = BytesMut::new();
    startup.put_i32(body.len() as i32 + 4);
    startup.put_slice(&body);
    stream.write_all(&startup).await?;

    let mut inbuf = BytesMut::with_capacity(1024);
    // Read backend frames until ReadyForQuery ('Z'); on auth/error close, fail.
    loop {
        match next_backend_frame(&mut inbuf)? {
            Some((b'Z', _)) => break,
            Some((b'E', body)) => {
                let _ = body;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "leader rejected startup",
                ));
            }
            Some(_) => continue, // R/S/K/etc. — consume and keep reading.
            None => {
                if stream.read_buf(&mut inbuf).await? == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "eof during startup",
                    ));
                }
            }
        }
    }
    Ok(PooledConn {
        addr: addr.to_string(),
        stream,
        inbuf,
    })
}

/// Send one simple `Query` over a pooled conn and read frames to ReadyForQuery,
/// folding RowDescription/DataRow/CommandComplete/ErrorResponse into a single
/// `QueryResult`. An ErrorResponse becomes `Err(ForwardErr::Sql(code, msg))`.
async fn send_query(conn: &mut PooledConn, sql: &str) -> Result<QueryResult, ForwardErr> {
    // Query message: 'Q', int32 len, NUL-terminated SQL.
    let mut q = BytesMut::new();
    q.put_u8(b'Q');
    q.put_i32(sql.len() as i32 + 4 + 1);
    q.put_slice(sql.as_bytes());
    q.put_u8(0);
    if tokio::time::timeout(FORWARD_TIMEOUT, conn.stream.write_all(&q))
        .await
        .map_err(|_| ForwardErr::Wire)?
        .is_err()
    {
        return Err(ForwardErr::Wire);
    }

    let mut fields: Vec<FieldDescription> = Vec::new();
    let mut rows: Vec<Vec<Option<Cell>>> = Vec::new();
    let mut tag = String::new();
    let mut sql_err: Option<(String, String)> = None;
    loop {
        let frame = match next_backend_frame(&mut conn.inbuf) {
            Ok(Some(f)) => f,
            Ok(None) => {
                let read =
                    tokio::time::timeout(FORWARD_TIMEOUT, conn.stream.read_buf(&mut conn.inbuf))
                        .await
                        .map_err(|_| ForwardErr::Wire)?
                        .map_err(|_| ForwardErr::Wire)?;
                if read == 0 {
                    return Err(ForwardErr::Wire); // upstream closed mid-response.
                }
                continue;
            }
            Err(_) => return Err(ForwardErr::Wire),
        };
        match frame {
            (b'T', body) => fields = parse_row_description(&body).ok_or(ForwardErr::Wire)?,
            (b'D', body) => rows.push(parse_data_row(&body).ok_or(ForwardErr::Wire)?),
            (b'C', body) => tag = parse_cstr(&body).ok_or(ForwardErr::Wire)?,
            (b'E', body) => sql_err = Some(parse_error(&body).ok_or(ForwardErr::Wire)?),
            (b'Z', _) => break, // ReadyForQuery: response complete.
            _ => {}             // I (EmptyQueryResponse), S, N (notice), etc. ignored.
        }
    }
    if let Some((code, msg)) = sql_err {
        return Err(ForwardErr::Sql(code, msg));
    }
    if fields.is_empty() {
        Ok(QueryResult::Command { tag })
    } else {
        Ok(QueryResult::Rows { fields, rows, tag })
    }
}

/// Pull one complete backend frame `(tag, body_without_tag_or_len)` from `buf`,
/// or `None` if the buffer doesn't yet hold a full frame. Backend framing is
/// uniform: u8 tag, i32 self-inclusive length, then `length-4` body bytes.
fn next_backend_frame(buf: &mut BytesMut) -> std::io::Result<Option<(u8, BytesMut)>> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "backend frame length < 4",
        ));
    }
    let total = 1 + len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let mut frame = buf.split_to(total);
    let _ = frame.split_to(5); // drop tag + length
    Ok(Some((tag, frame)))
}

/// Parse a RowDescription body into `FieldDescription`s.
fn parse_row_description(body: &[u8]) -> Option<Vec<FieldDescription>> {
    let mut b = body;
    let count = read_i16(&mut b)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_cstr(&mut b)?;
        let table_oid = read_i32(&mut b)? as u32;
        let column_id = read_i16(&mut b)?;
        let type_oid = read_i32(&mut b)? as u32;
        let type_size = read_i16(&mut b)?;
        let type_modifier = read_i32(&mut b)?;
        let format = read_i16(&mut b)?;
        fields.push(FieldDescription {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        });
    }
    Some(fields)
}

/// Parse a DataRow body into cells. Simple-protocol responses are text format, so
/// the binary half of each `Cell` is set equal to the text bytes (the relay only
/// re-emits text; the gateway's caller reads `Cell.text`).
fn parse_data_row(body: &[u8]) -> Option<Vec<Option<Cell>>> {
    let mut b = body;
    let count = read_i16(&mut b)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_i32(&mut b)?;
        if len < 0 {
            out.push(None);
        } else {
            let n = len as usize;
            if b.len() < n {
                return None;
            }
            let bytes = bytes::Bytes::copy_from_slice(&b[..n]);
            b = &b[n..];
            out.push(Some(Cell {
                text: bytes.clone(),
                binary: bytes,
            }));
        }
    }
    Some(out)
}

/// Parse an ErrorResponse body into `(sqlstate_code, message)`. Fields are
/// type-byte + NUL-terminated value; 'C' = code, 'M' = message; terminated by a
/// zero type byte.
fn parse_error(body: &[u8]) -> Option<(String, String)> {
    let mut b = body;
    let mut code = String::new();
    let mut msg = String::new();
    loop {
        if b.is_empty() {
            break;
        }
        let field = b[0];
        b = &b[1..];
        if field == 0 {
            break;
        }
        let value = read_cstr(&mut b)?;
        match field {
            b'C' => code = value,
            b'M' => msg = value,
            _ => {}
        }
    }
    Some((code, msg))
}

fn parse_cstr(body: &[u8]) -> Option<String> {
    let mut b = body;
    read_cstr(&mut b)
}

fn read_i16(b: &mut &[u8]) -> Option<i16> {
    if b.len() < 2 {
        return None;
    }
    let v = i16::from_be_bytes([b[0], b[1]]);
    *b = &b[2..];
    Some(v)
}

fn read_i32(b: &mut &[u8]) -> Option<i32> {
    if b.len() < 4 {
        return None;
    }
    let v = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(v)
}

fn read_cstr(b: &mut &[u8]) -> Option<String> {
    let pos = b.iter().position(|&c| c == 0)?;
    let s = String::from_utf8(b[..pos].to_vec()).ok()?;
    *b = &b[pos + 1..];
    Some(s)
}
