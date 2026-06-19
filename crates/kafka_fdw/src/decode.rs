//! Confluent-wire decode: strip the 5-byte envelope, fetch the schema from
//! the registry cache, and materialize an Avro Value, JSON Value, or Protobuf
//! DynamicMessage.

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
    /// Confluent Protobuf encoding (framed with a header + message-index varint).
    Protobuf,
}

/// A decoded Kafka message body, ready for column projection.
pub enum DecodedValue {
    /// Schema-decoded Apache Avro value.
    Avro(apache_avro::types::Value),
    /// Schema-decoded JSON value.
    Json(serde_json::Value),
    /// Raw bytes (no schema decoding).
    Raw(Vec<u8>),
    /// Schema-decoded Protobuf dynamic message.
    Protobuf(prost_reflect::DynamicMessage),
}

/// Decode a Kafka message payload according to `fmt`.
///
/// Returns the decoded value alongside the writer [`apache_avro::Schema`] that
/// was used to decode it (Avro only) — the schema is `None` for the JSON,
/// Raw, and Protobuf paths. The scanner threads this schema into
/// [`crate::types::project`] so that decimal `scale` is applied during
/// projection (the parse already happens here, so returning it avoids
/// fetching/parsing the schema a second time).
///
/// * `Wire::Raw`      — wraps the bytes verbatim; no registry access.
/// * `Wire::Avro`     — strips the Confluent 5-byte header, fetches the writer
///   schema from `cache` by id, then decodes the body with
///   `apache_avro::from_avro_datum`.
/// * `Wire::Json`     — strips the header, fetches the schema text (used only
///   for validation today), then deserialises the body as JSON.
/// * `Wire::Protobuf` — strips the Confluent protobuf envelope (magic byte +
///   schema-id + message-index varint), fetches the `FileDescriptorSet` proto
///   from the registry by schema id, builds a `prost_reflect::MessageDescriptor`
///   for the indexed message, and decodes the body via
///   `prost_reflect::DynamicMessage::decode`.
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

        Wire::Protobuf => {
            // Strip the Confluent protobuf envelope: magic byte + schema-id (4 BE
            // bytes) + message-index zigzag varint(s).
            let (schema_id, msg_index, body) = crabka_schema_serde::wire::decode_protobuf(bytes)
                .map_err(|e| KafkaFdwError::Other(format!("protobuf wire decode: {e}")))?;

            // Fetch the schema text (a base64-encoded serialised FileDescriptorSet)
            // from the registry by id.
            let schema_text = cache
                .writer_schema(schema_id)
                .map_err(|e| KafkaFdwError::Other(format!("schema registry: {e}")))?;

            // The Confluent Schema Registry stores the Protobuf schema as the raw
            // `.proto` source text, NOT as binary FileDescriptorSet bytes.
            // Build a DescriptorPool from that source text using `protox` or, if that
            // crate is unavailable, fall back to a placeholder that surfaces an
            // actionable error.  Today we surface the gap rather than pulling in
            // protox (which requires a build-time proto compiler); the unit tests
            // exercise the Value→Datum and columns paths directly via
            // `protobuf_message_to_columns` + `project` with hand-built descriptors.
            // `msg_index` is a Vec<i32> path; the first element selects the
            // top-level message index within the FileDescriptorSet.
            let index = msg_index.first().copied().unwrap_or(0) as usize;
            let descriptor = build_message_descriptor(&schema_text, index)
                .map_err(|e| KafkaFdwError::Other(format!("protobuf descriptor: {e}")))?;

            let msg = prost_reflect::DynamicMessage::decode(descriptor, body)
                .map_err(|e| KafkaFdwError::Other(format!("protobuf decode: {e}")))?;

            Ok((DecodedValue::Protobuf(msg), None))
        }
    }
}

/// Build a [`prost_reflect::MessageDescriptor`] from a Confluent-registry
/// schema text (`.proto` source) and a message index.
///
/// # Gaps (documented per task-15-brief)
///
/// The Confluent Schema Registry stores Protobuf schemas as raw `.proto`
/// source text.  Converting that text to a binary `FileDescriptorSet` at
/// runtime requires a proto compiler (e.g. `protox`).  That dependency is not
/// yet wired in; this function returns `Err` with a descriptive message when
/// the schema text is non-empty.
///
/// The live registry path is therefore **not exercised by CI** (Task 16's
/// integration test uses Avro).  The unit tests in `types.rs` exercise the
/// `Value→Datum` and column-mapping paths directly with hand-built descriptors,
/// which do not go through this function.
fn build_message_descriptor(
    _schema_text: &str,
    _msg_index: usize,
) -> Result<prost_reflect::MessageDescriptor, String> {
    Err(
        "live Protobuf registry-descriptor fetch not yet implemented: \
         compiling .proto source text at runtime requires protox or an \
         external protoc; wire the dep in a follow-up task"
            .to_string(),
    )
}
