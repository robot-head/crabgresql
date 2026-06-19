//! PostgreSQL foreign-data wrapper exposing crabka (Kafka) topics as SQL tables.
//! All Kafka-touching code is gated behind the `kafka` feature.
#![cfg(feature = "kafka")]

mod config;
pub mod decode;
mod error;
pub mod provider;
mod scan;
mod source;
pub mod types;

pub use decode::{DecodedValue, Wire, decode_value};
pub use error::KafkaFdwError;
pub use types::{avro_schema_to_columns, json_schema_to_columns, project};
