//! TCP accept loop and pre-startup negotiation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
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

/// Shared, replaceable token slot for one session.
type TokenSlot = Arc<Mutex<CancellationToken>>;

/// Maps (process_id, secret_key) -> the running query's cancellation token slot.
///
/// The token slot is REPLACED at each query start so a fired token never
/// cancels a later query.
#[derive(Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), TokenSlot>>,
}

impl CancelRegistry {
    /// Registers a new session; returns a guard that unregisters on drop,
    /// carrying the pid, secret, and a shared token slot.
    pub fn register(self: &Arc<Self>) -> SessionCancel {
        let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
        let secret = rand::rng().random::<i32>();
        let slot = Arc::new(Mutex::new(CancellationToken::new()));
        self.sessions
            .lock()
            .expect("registry lock")
            .insert((pid, secret), Arc::clone(&slot));
        SessionCancel {
            pid,
            secret,
            slot,
            registry: Arc::clone(self),
        }
    }

    /// Fire the current query token for the given (pid, secret).
    /// Silently ignores unknown keys, matching PostgreSQL behaviour.
    pub fn cancel(&self, pid: i32, secret: i32) {
        if let Some(slot) = self
            .sessions
            .lock()
            .expect("registry lock")
            .get(&(pid, secret))
        {
            slot.lock().expect("slot lock").cancel();
        }
    }
}

/// Per-session handle to the cancel registry.
///
/// Holds the pid/secret announced to the client and the shared token slot.
/// Automatically unregisters from the registry when dropped.
pub struct SessionCancel {
    pub pid: i32,
    pub secret: i32,
    slot: TokenSlot,
    registry: Arc<CancelRegistry>,
}

impl SessionCancel {
    /// Installs and returns a fresh [`CancellationToken`] for one query
    /// execution.  A previously fired token is replaced so it cannot cancel
    /// a subsequent query.
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
