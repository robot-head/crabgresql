//! pgtypes: the value layer for crabgresql — Datum, column types, wire
//! encodings, and operator semantics matching PostgreSQL.

pub mod datum;
pub mod encoding;
pub mod error;
pub mod ops;

pub use datum::{ColumnType, Datum, oids};
pub use error::TypeError;
