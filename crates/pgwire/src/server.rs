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
                // Pass the residual buffer so any bytes pipelined by the client
                // immediately after the startup packet are not silently dropped.
                return session::run_session(stream, params, engine, config, buf).await;
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
