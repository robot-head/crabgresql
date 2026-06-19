//! PostgreSQL foreign-data wrapper exposing crabka (Kafka) topics as SQL tables.
//! All Kafka-touching code is gated behind the `kafka` feature.
#![cfg(feature = "kafka")]

use std::sync::Arc;

use catalog::{Column, ForeignServer, Table, UserMapping};
use crabka_client_admin::AdminClient;
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache};
use executor::ExecError;
use executor::clock::EvalCtx;
use executor::foreign::{ForeignScanner, ImportFilter, ImportedTable, ScanBounds};
use pgtypes::{ColumnType, Datum};

mod config;
pub mod decode;
mod error;
pub mod provider;
mod scan;
pub mod source;
pub mod types;

pub use config::{ConnProfile, ServerProfile, resolve, resolve_server};
pub use decode::{DecodedValue, Wire, decode_value};
pub use error::KafkaFdwError;
pub use source::{FetchPlan, RawRecord, plan_fetch, scan_topic};
pub use types::{avro_schema_to_columns, json_schema_to_columns, project};

/// The Kafka foreign-data wrapper. A unit struct: a scan carries no
/// cross-call state — connection profiles and schema caches are built
/// per-scan from the catalog metadata it is handed.
///
/// Registered with the engine via
/// [`executor::SqlEngine::set_foreign_scanner`].
#[derive(Debug, Default)]
pub struct KafkaFdw;

/// Map a [`KafkaFdwError`] onto an [`ExecError`]. Both config and runtime
/// failures surface as `0A000` (`Unsupported`) for now — the closest existing
/// variant; a dedicated foreign-table error class can follow if needed.
fn to_exec_err(err: KafkaFdwError) -> ExecError {
    ExecError::Unsupported(err.to_string())
}

/// Build a [`SchemaCache`] for one scan from the profile's registry URL.
fn build_cache(profile: &ConnProfile) -> Arc<SchemaCache> {
    SchemaCache::new(
        RegistryClient::new(profile.registry_url.clone()),
        CacheConfig::default(),
    )
}

impl ForeignScanner for KafkaFdw {
    fn scan(
        &self,
        table: &Table,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        bounds: &ScanBounds,
        _ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        // Ensure the rustcrypto TLS provider is the process default before any
        // crabka-client TLS handshake (idempotent).
        provider::install_default_provider();

        let foreign = table.foreign.as_ref().ok_or_else(|| {
            ExecError::Unsupported(format!("table \"{}\" is not a foreign table", table.name))
        })?;

        let profile = config::resolve(server, mapping, &foreign.options).map_err(to_exec_err)?;
        let cache = build_cache(&profile);

        // Drive the async fetch + decode on the current multi-thread runtime
        // without blocking its worker pool (`block_in_place` moves this task to
        // a blocking thread).
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raws = source::scan_topic(&profile, &profile.topic, bounds)
                    .await
                    .map_err(to_exec_err)?;
                scan::assemble_rows(table, &raws, &profile, &cache)
                    .await
                    .map_err(to_exec_err)
            })
        })
    }

    fn import_schema(
        &self,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        filter: &ImportFilter,
    ) -> Result<Vec<ImportedTable>, ExecError> {
        // Idempotent: ensure the rustcrypto TLS provider is installed before any
        // crabka-client TLS handshake.
        provider::install_default_provider();

        // Resolve bootstrap + registry URL from the server only — IMPORT has no
        // per-table OPTIONS (no `topic`); it discovers the topics itself.
        let profile = config::resolve_server(server, mapping).map_err(to_exec_err)?;
        let registry = RegistryClient::new(profile.registry_url.clone());

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Enumerate every topic via the admin metadata RPC (empty topic
                // list = all topics, per Kafka semantics).
                let mut admin =
                    AdminClient::connect_secured(&profile.bootstrap, profile.security.clone())
                        .await
                        .map_err(|e| {
                            ExecError::Unsupported(format!("import: admin connect: {e}"))
                        })?;
                let meta = admin.metadata(&[]).await.map_err(|e| {
                    ExecError::Unsupported(format!("import: list topics metadata: {e}"))
                })?;

                let mut out: Vec<ImportedTable> = Vec::new();
                for entry in meta.topics {
                    // Skip topics the metadata response flagged as errored, and
                    // Kafka's internal topics (e.g. __consumer_offsets) which are
                    // not user data.
                    if entry.error.is_some() || entry.name.starts_with("__") {
                        continue;
                    }
                    // Apply the LIMIT TO / EXCEPT filter on the topic name.
                    if !filter.retains(&entry.name) {
                        continue;
                    }
                    let (value_columns, wire) =
                        value_columns_for_topic(&registry, &entry.name).await;
                    // Persist the detected `value_format` alongside `topic` so a
                    // later scan decodes the value bytes the SAME way the columns
                    // were derived. Without this the scan would default to `raw`
                    // and produce a 1-column bytea row that mismatches the
                    // schema-derived column count.
                    let options = vec![
                        ("topic".to_string(), entry.name.clone()),
                        ("value_format".to_string(), wire_option(wire).to_string()),
                    ];
                    out.push(ImportedTable {
                        name: entry.name,
                        columns: value_columns,
                        options,
                    });
                }
                // Stable ordering so repeated imports / tests are deterministic.
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            })
        })
    }
}

/// Derive the value columns + wire format for one topic from its Schema
/// Registry `"<topic>-value"` subject.
///
/// Raw-fallback policy: a topic whose `"<topic>-value"` subject is NOT registered
/// (or whose schema fails to parse / yields no columns) is still importable — it
/// gets a single raw `value bytea` column and [`Wire::Raw`]. This makes EVERY
/// topic queryable (matching the scanner's `Wire::Raw` path, which projects to
/// one bytea column), rather than silently skipping un-schematized topics.
async fn value_columns_for_topic(registry: &RegistryClient, topic: &str) -> (Vec<Column>, Wire) {
    let subject = format!("{topic}-value");
    match fetch_value_columns(registry, &subject).await {
        Some((cols, wire)) if !cols.is_empty() => (cols, wire),
        _ => (vec![raw_value_column()], Wire::Raw),
    }
}

/// The `value_format` OPTIONS string for a detected [`Wire`].
fn wire_option(wire: Wire) -> &'static str {
    match wire {
        Wire::Raw => "raw",
        Wire::Avro => "avro",
        Wire::Json => "json",
        Wire::Protobuf => "protobuf",
    }
}

/// A single raw `value bytea` column — the import raw-fallback shape.
fn raw_value_column() -> Column {
    Column {
        name: "value".to_string(),
        ty: ColumnType::Bytea,
    }
}

/// Fetch the latest schema for `subject` and derive columns + the [`Wire`]
/// format the columns were derived from. Returns `None` when the subject is
/// unregistered, the fetch fails, or the schema is unparseable — the caller
/// then applies the raw-fallback. Detection: a schema that parses as an Avro
/// record yields Avro columns + [`Wire::Avro`]; otherwise it is treated as JSON
/// Schema + [`Wire::Json`].
async fn fetch_value_columns(
    registry: &RegistryClient,
    subject: &str,
) -> Option<(Vec<Column>, Wire)> {
    let id = registry.latest_id(subject).await.ok()?;
    let schema_text = registry.schema_by_id(id).await.ok()?;

    // Try Avro first: a Confluent Avro subject's schema text parses as an Avro
    // Schema; `avro_schema_to_columns` returns a non-empty list only for a
    // top-level record.
    if let Ok(avro_schema) = apache_avro::Schema::parse_str(&schema_text) {
        let cols = types::avro_schema_to_columns(&avro_schema);
        if !cols.is_empty() {
            return Some((cols, Wire::Avro));
        }
    }

    // Fall back to JSON Schema (Confluent JSON subjects store a JSON Schema
    // object with a top-level `properties` map).
    if let Ok(json_schema) = serde_json::from_str::<serde_json::Value>(&schema_text) {
        let cols = types::json_schema_to_columns(&json_schema);
        if !cols.is_empty() {
            return Some((cols, Wire::Json));
        }
    }

    None
}
