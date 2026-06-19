//! SP40: the dependency-inversion seam between the executor and a foreign-data
//! wrapper (the `kafka_fdw` crate). The executor knows only this trait; the FDW
//! crate implements it and is injected into the engine via
//! [`crate::SqlEngine::set_foreign_scanner`]. With no scanner registered a
//! `SELECT` from a foreign table returns `0A000` ("foreign tables require the
//! `kafka` feature").

use catalog::{ForeignServer, Table, UserMapping};
use pgtypes::Datum;

use crate::clock::EvalCtx;
use crate::error::ExecError;

/// The slice of the scan a [`ForeignScanner`] should materialize. Phase-1 always
/// passes `ScanBounds::default()` (a full snapshot to the topic's high-water
/// mark); predicate/offset pushdown lands in a later task, which will populate
/// these fields from the query's `WHERE` clause.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanBounds {
    /// Lower partition-offset bound (inclusive) per partition, if pushed down.
    /// Empty = scan from the beginning of every partition.
    pub start_offsets: Vec<(i32, i64)>,
    /// Upper partition-offset bound (exclusive) per partition, if pushed down.
    /// Empty = scan to each partition's high-water mark.
    pub end_offsets: Vec<(i32, i64)>,
}

/// A filter on the tables an `IMPORT FOREIGN SCHEMA` materializes — the executor
/// translates the parsed `ImportSelector` into this neutral shape so the FDW does
/// not depend on the parser's AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportFilter {
    /// Import every table the server exposes.
    All,
    /// Import only the named tables (`LIMIT TO (...)`).
    Only(Vec<String>),
    /// Import every table except the named ones (`EXCEPT (...)`).
    Except(Vec<String>),
}

/// The executor↔FDW seam. An implementor turns a foreign table's catalog
/// metadata (schema, server connection options, optional user mapping) into rows
/// aligned to the table's column order — the envelope columns (`_partition`,
/// `_offset`, `_timestamp`, `_key`, `_headers`) first, then the decoded value
/// columns, exactly as [`catalog::create_foreign_table`] lays them out.
pub trait ForeignScanner: Send + Sync {
    /// Materialize the foreign table's rows for one scan.
    ///
    /// - `table` carries the column schema and `table.foreign` metadata
    ///   (the server name + table OPTIONS such as `topic`/`value_format`).
    /// - `server` carries the server-level OPTIONS (e.g. `bootstrap`, `registry_url`).
    /// - `mapping` is the resolved user mapping (credentials), if one exists.
    /// - `bounds` is the requested slice (full-snapshot in phase 1).
    /// - `ctx` is the per-statement evaluation context (zone/clock) for any
    ///   temporal decoding.
    ///
    /// Each returned row MUST have exactly `table.columns.len()` datums in
    /// column order.
    fn scan(
        &self,
        table: &Table,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        bounds: &ScanBounds,
        ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError>;
}
