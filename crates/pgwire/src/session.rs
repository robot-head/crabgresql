//! Post-startup connection state machine, generic over the byte stream so the
//! same code runs plaintext and TLS sessions.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use tokio_util::sync::CancellationToken;

use crate::engine::{Engine, FieldDescription, QueryResult};
use crate::error::{PgError, Severity, sqlstate};
use crate::messages::backend::{self, TxStatus};
use crate::messages::frontend::{self, FrontendMessage};
use crate::server::SessionCancel;

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

// ── Extended-query state ────────────────────────────────────────────────────

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
    /// One resolved format code (0 = text / 1 = binary) per column.
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
    ext.statements.insert(
        name,
        Prepared {
            sql,
            param_types,
            fields,
        },
    );
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
        Portal {
            sql: prepared.sql.clone(),
            fields: prepared.fields.clone(),
            formats,
        },
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
                    .map(|(f, &format)| FieldDescription {
                        format,
                        ..f.clone()
                    })
                    .collect();
                backend::row_description(out, &fields);
            }
        }
        other => {
            return Err(PgError::protocol(format!(
                "invalid describe kind {:?}",
                other as char
            )));
        }
    }
    Ok(())
}

async fn handle_execute<E: Engine>(
    ext: &ExtendedState,
    engine: &E,
    portal_name: &str,
    token: CancellationToken,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    let (sql, formats) = {
        let portal = ext.portals.get(portal_name).ok_or_else(|| {
            PgError::error(
                sqlstate::INVALID_CURSOR_NAME,
                format!("portal \"{portal_name}\" does not exist"),
            )
        })?;
        (portal.sql.clone(), portal.formats.clone())
    }; // ext borrow ends here, before the await

    let results = tokio::select! {
        r = engine.simple_query(&sql) => r?,
        _ = token.cancelled() => return Err(PgError::error(
            sqlstate::QUERY_CANCELED,
            "canceling statement due to user request",
        )),
    };
    // Extended protocol carries exactly one statement per Parse.
    match results.first() {
        Some(QueryResult::Rows { rows, tag, .. }) => {
            for row in rows {
                let values: Vec<Option<Bytes>> = row
                    .iter()
                    .zip(&formats)
                    .map(|(cell, &format)| {
                        cell.as_ref().map(|c| {
                            if format == 1 {
                                c.binary.clone()
                            } else {
                                c.text.clone()
                            }
                        })
                    })
                    .collect();
                backend::data_row(out, &values);
            }
            backend::command_complete(out, tag);
        }
        Some(QueryResult::Command { tag }) => backend::command_complete(out, tag),
        Some(QueryResult::Empty) => backend::empty_query_response(out),
        None => {
            // SP2-fragile: extended protocol must send EmptyQueryResponse ONLY for an
            // empty query string; a zero-row real query must send CommandComplete.
            // None is unreachable for single-statement extended exec against a real
            // engine — revisit in SP2.
            backend::empty_query_response(out);
        }
    }
    Ok(())
}

// ── Main session loop ───────────────────────────────────────────────────────

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
    cancel: SessionCancel,
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
    backend::backend_key_data(&mut out, cancel.pid, cancel.secret);
    backend::ready_for_query(&mut out, TxStatus::Idle);
    stream.write_all(&out).await?;
    out.clear();

    let mut ext = ExtendedState::default();

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
            FrontendMessage::Sync => {
                ext.failed = false;
                ext.portals.clear(); // implicit transaction ends at Sync
                backend::ready_for_query(&mut out, TxStatus::Idle);
                stream.write_all(&out).await?;
                out.clear();
            }
            // Every arm write_all()s eagerly, so there is never pending response data; Flush has nothing to drain and TcpStream::flush is a no-op.
            FrontendMessage::Flush => stream.flush().await?,
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) =
                    handle_parse(&mut ext, &*engine, name, sql, param_types, &mut out).await
                {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats: _,
                params: _,
                result_formats,
            } => {
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
            FrontendMessage::Execute {
                portal,
                max_rows: _,
            } => {
                if ext.failed {
                    continue;
                }
                // Cancel window: between extended messages no engine future runs; the pending flag in CancelRegistry makes a cancel received there fire on the next engine call.
                let token = cancel.begin_query();
                if let Err(e) = handle_execute(&ext, &*engine, &portal, token, &mut out).await {
                    fail_extended(&mut ext, &mut out, &e);
                }
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Close { kind, name } => {
                if ext.failed {
                    continue;
                }
                match kind {
                    b'S' => {
                        ext.statements.remove(&name);
                    }
                    b'P' => {
                        ext.portals.remove(&name);
                    }
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
