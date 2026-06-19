//! Confluent-wire decode: strip the 5-byte envelope, fetch the schema from
//! the registry cache, and materialize an Avro Value or JSON Value.

use std::sync::Arc;

use crabka_schema_serde::SchemaCache;

use crate::error::KafkaFdwError;

/// Wire format declared in the foreign table OPTIONS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    /// Pass raw bytes through unchanged (no schema registry).
    Raw,
    /// Confluent Avro binary encoding.
    Avro,
    /// Confluent JSON encoding (framed with a 5-byte header).
    Json,
}

/// A decoded Kafka message body, ready for column projection.
pub enum DecodedValue {
    /// Schema-decoded Apache Avro value.
    Avro(apache_avro::types::Value),
    /// Schema-decoded JSON value.
    Json(serde_json::Value),
    /// Raw bytes (no schema decoding).
    Raw(Vec<u8>),
}

/// Decode a Kafka message payload according to `fmt`.
///
/// Returns the decoded value alongside the writer [`apache_avro::Schema`] that
/// was used to decode it (Avro only) — the schema is `None` for the JSON and
/// Raw paths. The scanner threads this schema into [`crate::types::project`] so
/// that decimal `scale` is applied during projection (the parse already happens
/// here, so returning it avoids fetching/parsing the schema a second time).
///
/// * `Wire::Raw`  — wraps the bytes verbatim; no registry access.
/// * `Wire::Avro` — strips the Confluent 5-byte header, fetches the writer
///   schema from `cache` by id, then decodes the body with
///   `apache_avro::from_avro_datum`.
/// * `Wire::Json` — strips the header, fetches the schema text (used only
///   for validation today), then deserialises the body as JSON.
pub async fn decode_value(
    cache: &Arc<SchemaCache>,
    fmt: Wire,
    _topic: &str,
    bytes: &[u8],
) -> Result<(DecodedValue, Option<apache_avro::Schema>), KafkaFdwError> {
    match fmt {
        Wire::Raw => Ok((DecodedValue::Raw(bytes.to_vec()), None)),

        Wire::Avro => {
            let (schema_id, body) = crabka_schema_serde::wire::decode(bytes)
                .map_err(|e| KafkaFdwError::Other(format!("avro wire decode: {e}")))?;

            // Fetch (or await) the writer schema by id.
            let schema_text = cache
                .writer_schema(schema_id)
                .map_err(|e| KafkaFdwError::Other(format!("schema registry: {e}")))?;

            let schema = apache_avro::Schema::parse_str(&schema_text)
                .map_err(|e| KafkaFdwError::Other(format!("avro schema parse: {e}")))?;

            let value = apache_avro::from_avro_datum(&schema, &mut &body[..], None)
                .map_err(|e| KafkaFdwError::Other(format!("avro datum decode: {e}")))?;

            Ok((DecodedValue::Avro(value), Some(schema)))
        }

        Wire::Json => {
            let (_schema_id, body) = crabka_schema_serde::wire::decode(bytes)
                .map_err(|e| KafkaFdwError::Other(format!("json wire decode: {e}")))?;

            let value: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| KafkaFdwError::Other(format!("json decode: {e}")))?;

            Ok((DecodedValue::Json(value), None))
        }
    }
}
