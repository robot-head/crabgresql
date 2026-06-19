//! Versioned (de)serialization of a table schema — the value stored under
//! `kv::key::catalog_key(name)`. Format: version byte, table_id (u32 BE),
//! column count (u32 BE), then per column: u32 name length, name bytes, type tag.

use kv::KvError;
use pgtypes::ColumnType;
use pgtypes::numeric::Typmod;

use crate::Column;

/// Current schema-value format version.
pub const SCHEMA_VERSION: u8 = 1;

mod type_tag {
    pub const BOOL: u8 = 0;
    pub const INT4: u8 = 1;
    pub const INT8: u8 = 2;
    pub const TEXT: u8 = 3;
    /// SP30: `float8` / `double precision`. Append-only — no version bump.
    pub const FLOAT8: u8 = 4;
    /// SP32: `numeric` — followed by a typmod byte (0 = unconstrained; 1 = a
    /// `(precision: u16, scale: u16)` modifier). Append-only.
    pub const NUMERIC: u8 = 5;
    /// SP37: `date`. Append-only — no version bump.
    pub const DATE: u8 = 6;
    /// SP37: `time without time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIME: u8 = 7;
    /// SP37: `timestamp without time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIMESTAMP: u8 = 8;
    /// SP37: `timestamp with time zone` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const TIMESTAMPTZ: u8 = 9;
    /// SP37: `interval` — followed by a reserved precision byte (0).
    /// Append-only — no version bump.
    pub const INTERVAL: u8 = 10;
    /// SP40: `bytea`. Append-only — no version bump.
    pub const BYTEA: u8 = 11;
}

/// Append a column's type (tag byte, plus the numeric typmod payload).
fn write_type(out: &mut Vec<u8>, ty: ColumnType) {
    match ty {
        ColumnType::Bool => out.push(type_tag::BOOL),
        ColumnType::Int4 => out.push(type_tag::INT4),
        ColumnType::Int8 => out.push(type_tag::INT8),
        ColumnType::Text => out.push(type_tag::TEXT),
        ColumnType::Float8 => out.push(type_tag::FLOAT8),
        ColumnType::Numeric(tm) => {
            out.push(type_tag::NUMERIC);
            match tm {
                Some(t) => {
                    out.push(1);
                    out.extend_from_slice(&t.precision.to_be_bytes());
                    out.extend_from_slice(&t.scale.to_be_bytes());
                }
                None => out.push(0),
            }
        }
        ColumnType::Date => out.push(type_tag::DATE),
        ColumnType::Time => {
            out.push(type_tag::TIME);
            out.push(0); // reserved fractional-second typmod byte (deferred)
        }
        ColumnType::Timestamp => {
            out.push(type_tag::TIMESTAMP);
            out.push(0); // reserved fractional-second typmod byte (deferred)
        }
        ColumnType::Timestamptz => {
            out.push(type_tag::TIMESTAMPTZ);
            out.push(0); // reserved fractional-second typmod byte (deferred)
        }
        ColumnType::Interval => {
            out.push(type_tag::INTERVAL);
            out.push(0); // reserved fractional-second typmod byte (deferred)
        }
        // SP40: `bytea` — no payload (no typmod).
        ColumnType::Bytea => out.push(type_tag::BYTEA),
    }
}

/// Read a column's type, consuming the tag (and the numeric typmod payload).
fn read_type(cur: &mut &[u8]) -> Result<ColumnType, KvError> {
    Ok(match take_u8(cur)? {
        type_tag::BOOL => ColumnType::Bool,
        type_tag::INT4 => ColumnType::Int4,
        type_tag::INT8 => ColumnType::Int8,
        type_tag::TEXT => ColumnType::Text,
        type_tag::FLOAT8 => ColumnType::Float8,
        type_tag::NUMERIC => {
            if take_u8(cur)? == 1 {
                let precision = u16::from_be_bytes(take_n(cur, 2)?.try_into().expect("2"));
                let scale = u16::from_be_bytes(take_n(cur, 2)?.try_into().expect("2"));
                ColumnType::Numeric(Some(Typmod { precision, scale }))
            } else {
                ColumnType::Numeric(None)
            }
        }
        type_tag::DATE => ColumnType::Date,
        type_tag::TIME => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Time
        }
        type_tag::TIMESTAMP => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Timestamp
        }
        type_tag::TIMESTAMPTZ => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Timestamptz
        }
        type_tag::INTERVAL => {
            let reserved = take_u8(cur)?;
            if reserved != 0 {
                return Err(KvError::CorruptRow("unsupported datetime precision".into()));
            }
            ColumnType::Interval
        }
        // SP40: `bytea` — no payload.
        type_tag::BYTEA => ColumnType::Bytea,
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown column type tag {other}"
            )));
        }
    })
}

pub fn serialize_schema(table_id: u32, columns: &[Column]) -> Vec<u8> {
    let mut out = vec![SCHEMA_VERSION];
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for c in columns {
        out.extend_from_slice(&(c.name.len() as u32).to_be_bytes());
        out.extend_from_slice(c.name.as_bytes());
        write_type(&mut out, c.ty);
    }
    out
}

pub fn deserialize_schema(bytes: &[u8]) -> Result<(u32, Vec<Column>), KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != SCHEMA_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown schema version {version}"
        )));
    }
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let ncols = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
    let mut columns = Vec::with_capacity(ncols.min(1024));
    for _ in 0..ncols {
        let nlen = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
        let name = String::from_utf8(take_n(&mut cur, nlen)?.to_vec())
            .map_err(|_| KvError::CorruptRow("column name is not UTF-8".into()))?;
        let ty = read_type(&mut cur)?;
        columns.push(Column { name, ty });
    }
    Ok((table_id, columns))
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated schema".into()))?;
    *cur = rest;
    Ok(*h)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated schema field".into()));
    }
    let (h, rest) = cur.split_at(n);
    *cur = rest;
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Column;
    use pgtypes::ColumnType;

    #[test]
    fn roundtrip_schema() {
        let table_id = 42u32;
        let columns = vec![
            Column {
                name: "id".into(),
                ty: ColumnType::Int4,
            },
            Column {
                name: "name".into(),
                ty: ColumnType::Text,
            },
            Column {
                name: "ok".into(),
                ty: ColumnType::Bool,
            },
            Column {
                name: "big".into(),
                ty: ColumnType::Int8,
            },
            Column {
                name: "score".into(),
                ty: ColumnType::Float8,
            },
            Column {
                name: "amount".into(),
                ty: ColumnType::Numeric(Some(pgtypes::numeric::Typmod {
                    precision: 10,
                    scale: 2,
                })),
            },
            Column {
                name: "ratio".into(),
                ty: ColumnType::Numeric(None),
            },
        ];
        let bytes = serialize_schema(table_id, &columns);
        let (id, cols) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
    }

    #[test]
    fn roundtrip_schema_datetime_types() {
        let table_id = 99u32;
        let columns = vec![
            Column {
                name: "created".into(),
                ty: ColumnType::Date,
            },
            Column {
                name: "alarm".into(),
                ty: ColumnType::Time,
            },
            Column {
                name: "fired_at".into(),
                ty: ColumnType::Timestamp,
            },
            Column {
                name: "fired_utc".into(),
                ty: ColumnType::Timestamptz,
            },
            Column {
                name: "duration".into(),
                ty: ColumnType::Interval,
            },
        ];
        let bytes = serialize_schema(table_id, &columns);
        let (id, cols) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
    }

    #[test]
    fn unknown_version_errors() {
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn truncated_errors_not_panics() {
        assert!(deserialize_schema(&[SCHEMA_VERSION, 0, 0]).is_err());
    }

    #[test]
    fn take_n_consumes_exactly_all_remaining() {
        // Taking exactly the remaining length succeeds and empties the cursor;
        // only a STRICTLY shorter cursor is truncated (boundary at cur.len() == n).
        let data = [1u8, 2, 3, 4];
        let mut cur: &[u8] = &data;
        assert_eq!(take_n(&mut cur, 4).expect("exact length is valid"), &data);
        assert!(cur.is_empty());
        let mut cur: &[u8] = &data;
        assert!(take_n(&mut cur, 5).is_err());
    }
}
