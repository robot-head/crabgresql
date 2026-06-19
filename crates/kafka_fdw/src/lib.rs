//! PostgreSQL foreign-data wrapper exposing crabka (Kafka) topics as SQL tables.
//! All Kafka-touching code is gated behind the `kafka` feature.
#![cfg(feature = "kafka")]

use std::sync::Arc;

use catalog::{Column, ForeignServer, Table, UserMapping};
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache};
use executor::ExecError;
use executor::clock::EvalCtx;
use executor::foreign::{ForeignScanner, ScanBounds};
use pgtypes::Datum;

mod config;
pub mod decode;
mod error;
pub mod provider;
mod scan;
pub mod source;
pub mod types;

pub use config::{ConnProfile, resolve};
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
}

impl KafkaFdw {
    /// `IMPORT FOREIGN SCHEMA` support arrives in a later task; until then a
    /// real (non-panicking) `0A000` error is returned.
    ///
    /// This is intentionally an inherent method rather than a trait method:
    /// the current [`ForeignScanner`] trait does not declare `import_schema`,
    /// so the stub lives here until the trait grows the method in Task 13.
    pub fn import_schema(
        &self,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        _filter: &executor::foreign::ImportFilter,
    ) -> Result<Vec<(String, Vec<Column>)>, ExecError> {
        Err(ExecError::Unsupported(
            "IMPORT FOREIGN SCHEMA: implemented in a later task".into(),
        ))
    }
}
