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
        Self {
            auth: AuthMode::Trust,
            server_params: default_server_params(),
        }
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

/// Drive a single connection from the point immediately after the StartupMessage
/// has been decoded.
///
/// `inbuf` is the residual buffer from the pre-startup negotiation phase (owned
/// by `server::handle_conn`). Any bytes the client pipelined immediately after
/// the startup packet are already in `inbuf`; passing it here avoids silently
/// dropping those bytes.
pub async fn run_session<S, E>(
    mut stream: S,
    _startup_params: Vec<(String, String)>,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    mut inbuf: BytesMut,
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
                    let values: Vec<Option<Bytes>> = row
                        .iter()
                        .map(|c| c.as_ref().map(|c| c.text.clone()))
                        .collect();
                    backend::data_row(out, &values);
                }
                backend::command_complete(out, tag);
            }
            QueryResult::Command { tag } => backend::command_complete(out, tag),
            QueryResult::Empty => backend::empty_query_response(out),
        }
    }
}
