//! TCP accept loop and pre-startup negotiation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::engine::Engine;
use crate::messages::backend;
use crate::messages::frontend::{self, StartupPacket};
use crate::session::{self, SessionConfig};

static NEXT_PID: AtomicI32 = AtomicI32::new(1);

/// Combined cancellation target for one session.
///
/// `slot` holds the current query's [`CancellationToken`] and is replaced at
/// every `begin_query` call so a fired token cannot reach a later query.
///
/// `pending` closes the *extended-batch cancel window*: during an extended
/// message sequence (Parse → Bind → Describe → Execute) no engine future is
/// running, so a `CancelRequest` that arrives between messages would fire the
/// spent token from the previous query and then be silently lost when
/// `begin_query` replaces it.  Setting `pending = true` alongside the token
/// fire lets `begin_query` detect the race and immediately cancel the fresh
/// token — ensuring the next engine call sees a cancelled token right away.
///
/// Conformance note: this means a cancel that arrives while the session is
/// completely idle (no batch in flight) will also poison the next query.  Real
/// PostgreSQL treats such a cancel as a no-op for future queries.  Matching
/// that behaviour exactly would require tracking whether an extended batch is
/// in progress; that refinement is deferred — for now the simpler
/// "best-effort" semantics are acceptable and the test suite covers both
/// outcomes.
struct CancelTarget {
    slot: Mutex<CancellationToken>,
    /// Set by `CancelRegistry::cancel`; consumed (one-shot) by
    /// `SessionCancel::begin_query` so that one cancel fires exactly one
    /// engine call.
    pending: AtomicBool,
}

/// Maps (process_id, secret_key) -> the running query's cancellation target.
///
/// The token inside the target is REPLACED at each query start so a fired
/// token never cancels a later query.  The `pending` flag survives the
/// replacement to handle cancels that race the extended-batch window.
#[derive(Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), Arc<CancelTarget>>>,
}

impl CancelRegistry {
    /// Registers a new session; returns a guard that unregisters on drop,
    /// carrying the pid, secret, and a shared cancellation target.
    pub fn register(self: &Arc<Self>) -> SessionCancel {
        let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
        let secret = rand::rng().random::<i32>();
        let target = Arc::new(CancelTarget {
            slot: Mutex::new(CancellationToken::new()),
            pending: AtomicBool::new(false),
        });
        self.sessions
            .lock()
            .expect("registry lock")
            .insert((pid, secret), Arc::clone(&target));
        SessionCancel {
            pid,
            secret,
            target,
            registry: Arc::clone(self),
        }
    }

    /// Fire the current query token for the given (pid, secret) and set the
    /// sticky `pending` flag so a cancel that races the extended-batch window
    /// is not lost.
    ///
    /// Silently ignores unknown keys, matching PostgreSQL behaviour.
    pub fn cancel(&self, pid: i32, secret: i32) {
        if let Some(target) = self
            .sessions
            .lock()
            .expect("registry lock")
            .get(&(pid, secret))
        {
            target.pending.store(true, Ordering::SeqCst);
            target.slot.lock().expect("slot lock").cancel();
        }
    }
}

/// Per-session handle to the cancel registry.
///
/// Holds the pid/secret announced to the client and the shared cancellation
/// target.  Automatically unregisters from the registry when dropped.
pub struct SessionCancel {
    pub pid: i32,
    pub secret: i32,
    target: Arc<CancelTarget>,
    registry: Arc<CancelRegistry>,
}

impl SessionCancel {
    /// Installs and returns a fresh [`CancellationToken`] for one query
    /// execution.  A previously fired token is replaced so it cannot cancel
    /// a subsequent query.
    ///
    /// If a `CancelRequest` arrived while no engine future was running (the
    /// extended-batch window), the `pending` flag will be set; this method
    /// consumes it and immediately cancels the fresh token so the next
    /// `tokio::select!` sees `cancelled()` right away.
    pub fn begin_query(&self) -> CancellationToken {
        let fresh = CancellationToken::new();
        *self.target.slot.lock().expect("slot lock") = fresh.clone();
        if self.target.pending.swap(false, Ordering::SeqCst) {
            fresh.cancel();
        }
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

pub async fn serve<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = Arc::clone(&engine);
        let config = Arc::clone(&config);
        let registry = Arc::clone(&registry);
        // TODO(config-era): connection cap (Semaphore) and pre-auth read timeout — slowloris guard. Deliberately deferred in SP1.
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, engine, config, registry).await {
                tracing::debug!("connection from {peer} ended: {e}");
            }
        });
    }
}

async fn handle_conn<E: Engine>(
    mut stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        match frontend::decode_startup(&mut buf) {
            Ok(Some(StartupPacket::SslRequest)) | Ok(Some(StartupPacket::GssEncRequest)) => {
                // TLS task upgrades the SslRequest arm; until then: not supported.
                stream.write_all(b"N").await?;
            }
            Ok(Some(StartupPacket::CancelRequest {
                process_id,
                secret_key,
            })) => {
                registry.cancel(process_id, secret_key);
                // Protocol says close without responding.
                return Ok(());
            }
            Ok(Some(StartupPacket::Startup { params })) => {
                let cancel = registry.register();
                // Pass the residual buffer so any bytes pipelined by the client
                // immediately after the startup packet are not silently dropped.
                return session::run_session(stream, params, engine, config, cancel, buf).await;
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
