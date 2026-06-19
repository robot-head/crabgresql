//! error

/// Top-level error type for the Kafka FDW.
#[derive(Debug, thiserror::Error)]
pub enum KafkaFdwError {
    #[error("kafka fdw error: {0}")]
    Other(String),
}
