//! Schema→column and Value→Datum type mapping for the Kafka FDW.
//!
//! Two entry-points:
//! - `avro_schema_to_columns` / `json_schema_to_columns` — derive the logical
//!   column list from a writer schema.
//! - `project` — map a decoded value to `Vec<Datum>` in column order.

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{DecimalSchema, RecordSchema, UnionSchema};
use apache_avro::types::Value as AvroValue;
use catalog::Column;
use pgtypes::{ColumnType, Datum};
use serde_json::Value as JsonValue;

use crate::decode::DecodedValue;

// ---------------------------------------------------------------------------
// Avro epoch helpers
// ---------------------------------------------------------------------------

/// Unix epoch (1970-01-01) as a `jiff` civil date, used to convert
/// Avro `Date` (days since Unix epoch) to `jiff::civil::Date`.
fn unix_epoch_date() -> jiff::civil::Date {
    jiff::civil::Date::constant(1970, 1, 1)
}

// ---------------------------------------------------------------------------
// Avro schema → ColumnType
// ---------------------------------------------------------------------------

/// Map an Avro schema node to a `ColumnType`, returning `None` for types we
/// cannot represent (unsupported unions, enums, fixed, etc.).
fn avro_to_column_type(schema: &AvroSchema) -> Option<ColumnType> {
    match schema {
        AvroSchema::Null => None,
        AvroSchema::Boolean => Some(ColumnType::Bool),
        AvroSchema::Int => Some(ColumnType::Int4),
        AvroSchema::Long => Some(ColumnType::Int8),
        // Both `float` and `double` map to Float8 — there is no Float4 in this
        // codebase (see mapping rules in Component D).
        AvroSchema::Float | AvroSchema::Double => Some(ColumnType::Float8),
        AvroSchema::Bytes => Some(ColumnType::Bytea),
        AvroSchema::String => Some(ColumnType::Text),
        // Logical timestamp types — both millis and micros → Timestamptz.
        AvroSchema::TimestampMillis | AvroSchema::TimestampMicros => Some(ColumnType::Timestamptz),
        AvroSchema::Date => Some(ColumnType::Date),
        // Decimal → unconstrained Numeric.
        AvroSchema::Decimal(_) => Some(ColumnType::Numeric(None)),
        // Nullable union `[null, T]` → the type of T (nullable column).
        AvroSchema::Union(u) => nullable_union_type(u),
        // Nested complex types → JSON-serialized text for this slice.
        AvroSchema::Record(_) | AvroSchema::Array(_) | AvroSchema::Map(_) => Some(ColumnType::Text),
        // Everything else (Enum, Fixed, LocalTimestamp*, BigDecimal, Ref, …) → Text
        // as a safe catch-all so the schema walk never silently drops a field.
        _ => Some(ColumnType::Text),
    }
}

/// If `u` is exactly `[null, T]` (or `[T, null]`), return the type of `T`.
/// Any other union shape falls through to `None`.
fn nullable_union_type(u: &UnionSchema) -> Option<ColumnType> {
    let variants = u.variants();
    if variants.len() != 2 {
        return None;
    }
    let non_null = match (&variants[0], &variants[1]) {
        (AvroSchema::Null, other) => other,
        (other, AvroSchema::Null) => other,
        _ => return None,
    };
    avro_to_column_type(non_null)
}

/// Derive the column list from an Avro Record schema.
///
/// Only top-level Record schemas are expected; non-Record schemas (primitive,
/// union, …) return an empty list.
pub fn avro_schema_to_columns(schema: &AvroSchema) -> Vec<Column> {
    let AvroSchema::Record(RecordSchema { fields, .. }) = schema else {
        return Vec::new();
    };
    fields
        .iter()
        .filter_map(|f| {
            avro_to_column_type(&f.schema).map(|ty| Column {
                name: f.name.clone(),
                ty,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// JSON schema → ColumnType
// ---------------------------------------------------------------------------

/// Map a JSON Schema `type` string to a `ColumnType`.
fn json_type_to_column_type(type_str: &str) -> ColumnType {
    match type_str {
        "boolean" => ColumnType::Bool,
        "integer" => ColumnType::Int8,
        "number" => ColumnType::Float8,
        _ => ColumnType::Text,
    }
}

/// Derive columns from a JSON Schema object (draft-04 / Confluent subset).
///
/// Inspects the top-level `"properties"` map; the `"type"` of each property
/// is mapped to a `ColumnType`.
pub fn json_schema_to_columns(schema: &JsonValue) -> Vec<Column> {
    let Some(props) = schema.get("properties").and_then(|p| p.as_object()) else {
        return Vec::new();
    };
    props
        .iter()
        .map(|(name, prop)| {
            let ty = prop
                .get("type")
                .and_then(|t| t.as_str())
                .map(json_type_to_column_type)
                .unwrap_or(ColumnType::Text);
            Column {
                name: name.clone(),
                ty,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Avro Value → Datum
// ---------------------------------------------------------------------------

/// Map an Avro `Value` to a `Datum` for the given column type.
///
/// `Union(_, inner)` is unwrapped; `Null` produces `Datum::Null`.
///
/// `decimal_scale` — the scale from the Avro field's `DecimalSchema` (number
/// of fractional digits). Must be provided when the value may be
/// `AvroValue::Decimal`; pass `None` for non-decimal fields.
fn avro_value_to_datum(value: &AvroValue, col_ty: ColumnType, decimal_scale: Option<u32>) -> Datum {
    match value {
        AvroValue::Null => Datum::Null,
        // Unwrap nullable union — recurse on the inner value.
        AvroValue::Union(_, inner) => avro_value_to_datum(inner, col_ty, decimal_scale),

        AvroValue::Boolean(b) => Datum::Bool(*b),
        AvroValue::Int(i) => Datum::Int4(*i),
        AvroValue::Long(l) => Datum::Int8(*l),
        AvroValue::Float(f) => Datum::Float8(f64::from(*f)),
        AvroValue::Double(d) => Datum::Float8(*d),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => Datum::Bytea(b.clone()),
        AvroValue::String(s) => Datum::Text(s.clone()),

        // Logical date: i32 days since the Unix epoch (1970-01-01).
        AvroValue::Date(days) => {
            let span = jiff::Span::new().days(*days);
            let date = unix_epoch_date()
                .checked_add(span)
                .expect("avro Date value in jiff range");
            Datum::Date(date)
        }

        // Logical timestamp-millis: i64 milliseconds since Unix epoch → Timestamptz.
        AvroValue::TimestampMillis(ms) => {
            let ts = jiff::Timestamp::from_millisecond(*ms)
                .expect("avro TimestampMillis value in jiff range");
            Datum::Timestamptz(ts)
        }

        // Logical timestamp-micros: i64 microseconds since Unix epoch → Timestamptz.
        AvroValue::TimestampMicros(us) => {
            let ts = jiff::Timestamp::from_microsecond(*us)
                .expect("avro TimestampMicros value in jiff range");
            Datum::Timestamptz(ts)
        }

        // Decimal → Numeric (bigdecimal).
        // apache-avro's `Decimal` wraps a `BigInt` (unscaled value). The real
        // scale lives in the field's schema (`DecimalSchema::scale`), threaded
        // in via `decimal_scale`. `BigDecimal::new(bigint, scale)` represents
        // `bigint * 10^-scale`, so we pass the Avro scale directly.
        AvroValue::Decimal(d) => {
            use num_bigint::BigInt;
            let big_int: BigInt = BigInt::from(d.clone());
            let scale = decimal_scale.unwrap_or(0) as i64;
            let bd = bigdecimal::BigDecimal::new(big_int, scale);
            Datum::Numeric(bd)
        }
        AvroValue::BigDecimal(bd) => Datum::Numeric(bd.clone()),

        // Nested complex types (Record, Array, Map) → JSON-serialised text.
        AvroValue::Record(fields) => {
            // Re-serialise as a JSON object for the text column.
            let map: serde_json::Map<String, JsonValue> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(map)).unwrap_or_default())
        }
        AvroValue::Array(arr) => {
            let arr: Vec<JsonValue> = arr.iter().map(avro_value_to_json).collect();
            Datum::Text(serde_json::to_string(&arr).unwrap_or_default())
        }
        AvroValue::Map(m) => {
            let map: serde_json::Map<String, JsonValue> = m
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(map)).unwrap_or_default())
        }

        // Catch-all: serialise as text.
        _ => {
            let _ = (col_ty, decimal_scale); // suppress unused warnings
            Datum::Text(format!("{value:?}"))
        }
    }
}

/// Convert an Avro `Value` to a `serde_json::Value` (best-effort; used when
/// serialising nested Avro values into a text column).
fn avro_value_to_json(value: &AvroValue) -> JsonValue {
    match value {
        AvroValue::Null => JsonValue::Null,
        AvroValue::Union(_, inner) => avro_value_to_json(inner),
        AvroValue::Boolean(b) => JsonValue::Bool(*b),
        AvroValue::Int(i) => JsonValue::from(*i),
        AvroValue::Long(l) => JsonValue::from(*l),
        AvroValue::Float(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AvroValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => {
            // Encode as hex string for JSON serialisation of nested bytea.
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            JsonValue::String(format!("\\x{hex}"))
        }
        AvroValue::String(s) => JsonValue::String(s.clone()),
        AvroValue::Date(d) => JsonValue::from(*d),
        AvroValue::TimestampMillis(ms) => JsonValue::from(*ms),
        AvroValue::TimestampMicros(us) => JsonValue::from(*us),
        AvroValue::Record(fields) => {
            let map: serde_json::Map<String, JsonValue> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        AvroValue::Array(arr) => JsonValue::Array(arr.iter().map(avro_value_to_json).collect()),
        AvroValue::Map(m) => {
            let map: serde_json::Map<String, JsonValue> = m
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        _ => JsonValue::Null,
    }
}

// ---------------------------------------------------------------------------
// JSON Value → Datum
// ---------------------------------------------------------------------------

fn json_value_to_datum(value: &JsonValue, col_ty: ColumnType) -> Datum {
    match (value, col_ty) {
        (JsonValue::Null, _) => Datum::Null,
        (JsonValue::Bool(b), ColumnType::Bool) => Datum::Bool(*b),
        // NOTE: `json_schema_to_columns` maps JSON "integer" → Int8 only, so
        // the Int4 arm below is never reached in practice. It is kept as a
        // defensive fallback in case a caller supplies a hand-crafted Int4
        // column for JSON data.
        (JsonValue::Number(n), ColumnType::Int4) => n
            .as_i64()
            .map(|i| Datum::Int4(i as i32))
            .unwrap_or(Datum::Null),
        (JsonValue::Number(n), ColumnType::Int8) => {
            n.as_i64().map(Datum::Int8).unwrap_or(Datum::Null)
        }
        (JsonValue::Number(n), ColumnType::Float8) => {
            n.as_f64().map(Datum::Float8).unwrap_or(Datum::Null)
        }
        (JsonValue::String(s), ColumnType::Text) => Datum::Text(s.clone()),
        // Nested object/array → JSON-serialised text.
        (v @ JsonValue::Object(_), _) | (v @ JsonValue::Array(_), _) => Datum::Text(v.to_string()),
        // Cross-type: serialise as text.
        (v, _) => Datum::Text(v.to_string()),
    }
}

// ---------------------------------------------------------------------------
// project
// ---------------------------------------------------------------------------

/// Project a decoded Kafka value onto `value_columns`, returning one `Datum`
/// per column (in order).
///
/// * `DecodedValue::Avro(Record(...))` — look up each column by name in the
///   Avro Record fields.
/// * `DecodedValue::Json(Object {...})` — look up each column by name in the
///   JSON object.
/// * `DecodedValue::Raw(bytes)` — return a single `Datum::Bytea` regardless
///   of `value_columns` (raw fallback is always one bytea column).
///
/// `avro_schema` — the parsed writer schema for the Avro case. When provided
/// and the schema is a Record, field-level sub-schemas are consulted to
/// recover the `scale` of `decimal` fields so that `AvroValue::Decimal` is
/// correctly scaled. Pass `None` for JSON / Raw paths.
pub fn project(
    decoded: &DecodedValue,
    value_columns: &[Column],
    avro_schema: Option<&AvroSchema>,
) -> Vec<Datum> {
    match decoded {
        DecodedValue::Raw(bytes) => vec![Datum::Bytea(bytes.clone())],

        DecodedValue::Avro(AvroValue::Record(fields)) => {
            // Build a lookup of field-name → DecimalSchema scale from the
            // writer schema so `Decimal` values can be correctly scaled.
            let decimal_scale_for = |name: &str| -> Option<u32> {
                let schema = avro_schema?;
                let AvroSchema::Record(RecordSchema {
                    fields: schema_fields,
                    ..
                }) = schema
                else {
                    return None;
                };
                let field_schema = schema_fields
                    .iter()
                    .find(|f| f.name == name)
                    .map(|f| &f.schema)?;
                // The field schema may be wrapped in a nullable union `[null, decimal]`.
                let inner = match field_schema {
                    AvroSchema::Union(u) => u
                        .variants()
                        .iter()
                        .find(|s| !matches!(s, AvroSchema::Null))?,
                    other => other,
                };
                if let AvroSchema::Decimal(DecimalSchema { scale, .. }) = inner {
                    Some(*scale as u32)
                } else {
                    None
                }
            };

            value_columns
                .iter()
                .map(|col| {
                    fields
                        .iter()
                        .find(|(name, _)| name == &col.name)
                        .map(|(name, v)| avro_value_to_datum(v, col.ty, decimal_scale_for(name)))
                        .unwrap_or(Datum::Null)
                })
                .collect()
        }

        DecodedValue::Avro(_) => value_columns.iter().map(|_| Datum::Null).collect(),

        DecodedValue::Json(JsonValue::Object(obj)) => value_columns
            .iter()
            .map(|col| {
                obj.get(&col.name)
                    .map(|v| json_value_to_datum(v, col.ty))
                    .unwrap_or(Datum::Null)
            })
            .collect(),

        DecodedValue::Json(_) => value_columns.iter().map(|_| Datum::Null).collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::{ColumnType, Datum};

    // -----------------------------------------------------------------------
    // Brief's required test (verbatim from task-9-brief.md)
    // -----------------------------------------------------------------------

    #[test]
    fn avro_record_projects_to_datums() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"long"},{"name":"name","type":["null","string"]}]}"#,
        )
        .expect("schema parses");
        let cols = crate::types::avro_schema_to_columns(&schema);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].ty, pgtypes::ColumnType::Int8);
        assert_eq!(cols[1].ty, pgtypes::ColumnType::Text);

        let mut record = apache_avro::types::Record::new(&schema).expect("record created");
        record.put("id", 7i64);
        record.put("name", Some("x".to_string()));
        let val = apache_avro::types::Value::from(record);
        let datums = crate::types::project(
            &crate::decode::DecodedValue::Avro(val),
            &cols,
            Some(&schema),
        );
        assert_eq!(datums[0], pgtypes::Datum::Int8(7));
        assert_eq!(datums[1], pgtypes::Datum::Text("x".into()));
    }

    // -----------------------------------------------------------------------
    // Nullable field → Datum::Null when absent or null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_nullable_field_null_when_missing() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"S","fields":[
            {"name":"val","type":["null","string"],"default":null}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].ty, ColumnType::Text);

        // Build a record with the field explicitly set to None (null).
        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("val", Option::<String>::None);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Null);
    }

    // -----------------------------------------------------------------------
    // Bytes field → Datum::Bytea
    // -----------------------------------------------------------------------

    #[test]
    fn avro_bytes_field_to_bytea() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"B","fields":[
            {"name":"blob","type":"bytes"}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].ty, ColumnType::Bytea);

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("blob", vec![0xCAu8, 0xFEu8]);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Bytea(vec![0xCA, 0xFE]));
    }

    // -----------------------------------------------------------------------
    // JSON object → typed columns
    // -----------------------------------------------------------------------

    #[test]
    fn json_object_projects_to_typed_datums() {
        let json_schema: JsonValue = serde_json::json!({
            "type": "object",
            "properties": {
                "count": {"type": "integer"},
                "label": {"type": "string"}
            }
        });
        let cols = json_schema_to_columns(&json_schema);
        // Order from serde_json::Map iteration — find by name.
        let count_col = cols.iter().find(|c| c.name == "count").expect("count col");
        let label_col = cols.iter().find(|c| c.name == "label").expect("label col");
        assert_eq!(count_col.ty, ColumnType::Int8);
        assert_eq!(label_col.ty, ColumnType::Text);

        let payload: JsonValue = serde_json::json!({"count": 42, "label": "hello"});
        let datums = project(&DecodedValue::Json(payload), &cols, None);
        // Project in column order.
        let count_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "count")
            .map(|(d, _)| d)
            .expect("count datum");
        let label_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "label")
            .map(|(d, _)| d)
            .expect("label datum");
        assert_eq!(*count_datum, Datum::Int8(42));
        assert_eq!(*label_datum, Datum::Text("hello".into()));
    }

    // -----------------------------------------------------------------------
    // Missing field → Datum::Null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_missing_field_produces_null() {
        // Schema has two fields; record only has `id`.
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"T","fields":[
            {"name":"id","type":"long"},
            {"name":"opt","type":["null","string"],"default":null}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("id", 99i64);
        record.put("opt", Option::<String>::None);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Int8(99));
        assert_eq!(datums[1], Datum::Null);
    }

    // -----------------------------------------------------------------------
    // Finding 1: Avro decimal with scale=2 projects correctly (RED→GREEN)
    // Unscaled value 1999 with scale 2 must produce Datum::Numeric "19.99".
    // -----------------------------------------------------------------------

    #[test]
    fn avro_decimal_scale_applied_correctly() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"D","fields":[
            {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].name, "price");
        assert_eq!(cols[0].ty, ColumnType::Numeric(None));

        // Build an Avro Decimal value with unscaled integer 1999 (represents 19.99).
        use num_bigint::BigInt;
        let unscaled = BigInt::from(1999i64);
        let decimal_val = apache_avro::Decimal::from(unscaled.to_signed_bytes_be());
        let val = AvroValue::Record(vec![("price".to_string(), AvroValue::Decimal(decimal_val))]);

        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        let Datum::Numeric(ref bd) = datums[0] else {
            panic!("expected Datum::Numeric, got {:?}", datums[0]);
        };
        assert_eq!(
            bd.to_string(),
            "19.99",
            "decimal scale must be applied: 1999 * 10^-2 = 19.99"
        );
    }

    // -----------------------------------------------------------------------
    // Finding 2: truly-absent field (not in record field list) → Datum::Null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_truly_absent_field_produces_null() {
        // value_columns requests a field "extra" that is not present in the
        // Avro Record's fields list at all (not merely present-but-null).
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"A","fields":[
            {"name":"id","type":"long"}]}"#,
        )
        .expect("schema parses");

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("id", 5i64);
        let val = apache_avro::types::Value::from(record);

        // Ask for both "id" and a field "extra" that does not exist in the record.
        let cols = vec![
            Column {
                name: "id".to_string(),
                ty: ColumnType::Int8,
            },
            Column {
                name: "extra".to_string(),
                ty: ColumnType::Text,
            },
        ];

        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Int8(5), "id field must project correctly");
        assert_eq!(
            datums[1],
            Datum::Null,
            "absent field must produce Datum::Null via unwrap_or fallback"
        );
    }
}
