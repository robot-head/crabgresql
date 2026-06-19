//! PostgreSQL foreign-data wrapper exposing crabka (Kafka) topics as SQL tables.
//! All Kafka-touching code is gated behind the `kafka` feature.
#![cfg(feature = "kafka")]

mod config;
mod decode;
mod error;
pub mod provider;
mod scan;
mod source;
mod types;

pub use error::KafkaFdwError;
