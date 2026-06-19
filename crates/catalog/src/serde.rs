//! Versioned (de)serialization of a table schema — the value stored under
//! `kv::key::catalog_key(name)`. Format: version byte (`2`), table_id (u32 BE),
//! column count (u32 BE), then per column: u32 name length, name bytes, type
//! tag; followed by a `foreign` flag byte: `0` = ordinary table (no further
//! payload), `1` = foreign table (server name len u32, server name bytes,
//! option count u32, then per option: key len u32, key bytes, value len u32,
//! value bytes).
//!
//! Foreign-data-wrapper, foreign-server, and user-mapping objects use their own
//! simple binary format (not the schema format).

use kv::KvError;
use pgtypes::ColumnType;
use pgtypes::numeric::Typmod;

use crate::{Column, ForeignDataWrapper, ForeignServer, ForeignTableMeta, UserMapping};

/// The single schema-value format version. All tables (ordinary and foreign)
/// are written with this version byte; a flag byte after the column list
/// distinguishes ordinary (`0`) from foreign (`1`).
pub const SCHEMA_VERSION: u8 = 2;

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

// ── Options helpers ───────────────────────────────────────────────────────────

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn write_options(out: &mut Vec<u8>, opts: &[(String, String)]) {
    out.extend_from_slice(&(opts.len() as u32).to_be_bytes());
    for (k, v) in opts {
        write_str(out, k);
        write_str(out, v);
    }
}

fn read_str<'a>(cur: &mut &'a [u8]) -> Result<&'a [u8], KvError> {
    let len = u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")) as usize;
    take_n(cur, len)
}

fn read_string(cur: &mut &[u8]) -> Result<String, KvError> {
    let bytes = read_str(cur)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| KvError::CorruptRow("non-UTF-8 string in catalog".into()))
}

fn read_options(cur: &mut &[u8]) -> Result<Vec<(String, String)>, KvError> {
    let n = u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")) as usize;
    let mut opts = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        let k = read_string(cur)?;
        let v = read_string(cur)?;
        opts.push((k, v));
    }
    Ok(opts)
}

// ── Table schema ──────────────────────────────────────────────────────────────

/// Serialize a table schema (ordinary or foreign).
///
/// Always writes version byte `2`, then the column list, then a flag byte:
/// `0` for an ordinary table, `1` for a foreign table followed by the foreign
/// metadata payload.
pub fn serialize_schema(
    table_id: u32,
    columns: &[Column],
    meta: Option<&ForeignTableMeta>,
) -> Vec<u8> {
    let mut out = vec![SCHEMA_VERSION];
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for c in columns {
        write_str(&mut out, &c.name);
        write_type(&mut out, c.ty);
    }
    match meta {
        None => out.push(0), // ordinary table flag
        Some(m) => {
            out.push(1); // foreign table flag
            write_str(&mut out, &m.server);
            write_options(&mut out, &m.options);
        }
    }
    out
}

/// Deserialize a table schema.
///
/// Returns `(table_id, columns, Option<ForeignTableMeta>)`.
///
/// Returns `KvError::CorruptRow` if the version byte is not `2`, or if the
/// flag byte after the column list is not `0` (ordinary) or `1` (foreign).
pub fn deserialize_schema(
    bytes: &[u8],
) -> Result<(u32, Vec<Column>, Option<ForeignTableMeta>), KvError> {
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
        let name = read_string(&mut cur)?;
        let ty = read_type(&mut cur)?;
        columns.push(Column { name, ty });
    }
    let foreign = match take_u8(&mut cur)? {
        0 => None,
        1 => {
            let server = read_string(&mut cur)?;
            let options = read_options(&mut cur)?;
            Some(ForeignTableMeta { server, options })
        }
        flag => {
            return Err(KvError::CorruptRow(format!("unknown foreign flag {flag}")));
        }
    };
    Ok((table_id, columns, foreign))
}

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Format: `name len (u32) | name | options`.
pub fn serialize_fdw(name: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, name);
    write_options(&mut out, options);
    out
}

pub fn deserialize_fdw(bytes: &[u8]) -> Result<ForeignDataWrapper, KvError> {
    let mut cur = bytes;
    let name = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignDataWrapper { name, options })
}

// ── Foreign server ────────────────────────────────────────────────────────────

/// Format: `name len | name | wrapper len | wrapper | options`.
pub fn serialize_server(name: &str, wrapper: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, name);
    write_str(&mut out, wrapper);
    write_options(&mut out, options);
    out
}

pub fn deserialize_server(bytes: &[u8]) -> Result<ForeignServer, KvError> {
    let mut cur = bytes;
    let name = read_string(&mut cur)?;
    let wrapper = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(ForeignServer {
        name,
        wrapper,
        options,
    })
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Format: `user len | user | server len | server | options`.
pub fn serialize_user_mapping(user: &str, server: &str, options: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_str(&mut out, user);
    write_str(&mut out, server);
    write_options(&mut out, options);
    out
}

pub fn deserialize_user_mapping(bytes: &[u8]) -> Result<UserMapping, KvError> {
    let mut cur = bytes;
    let user = read_string(&mut cur)?;
    let server = read_string(&mut cur)?;
    let options = read_options(&mut cur)?;
    Ok(UserMapping {
        user,
        server,
        options,
    })
}

// ── Shared primitives ─────────────────────────────────────────────────────────

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
    use crate::{Column, ForeignTableMeta};
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
        let bytes = serialize_schema(table_id, &columns, None);
        let (id, cols, foreign) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(foreign.is_none());
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
        let bytes = serialize_schema(table_id, &columns, None);
        let (id, cols, foreign) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        assert!(foreign.is_none());
    }

    #[test]
    fn roundtrip_foreign_table() {
        let table_id = 7u32;
        let columns = vec![
            Column {
                name: "_partition".into(),
                ty: ColumnType::Int4,
            },
            Column {
                name: "_offset".into(),
                ty: ColumnType::Int8,
            },
            Column {
                name: "_timestamp".into(),
                ty: ColumnType::Timestamptz,
            },
            Column {
                name: "_key".into(),
                ty: ColumnType::Bytea,
            },
            Column {
                name: "_headers".into(),
                ty: ColumnType::Text,
            },
            Column {
                name: "payload".into(),
                ty: ColumnType::Text,
            },
        ];
        let meta = ForeignTableMeta {
            server: "kafka_srv".into(),
            options: vec![("topic".into(), "events".into())],
        };
        let bytes = serialize_schema(table_id, &columns, Some(&meta));
        let (id, cols, foreign) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
        let ft = foreign.expect("foreign meta round-trips");
        assert_eq!(ft.server, "kafka_srv");
        assert_eq!(ft.options, vec![("topic".into(), "events".into())]);
    }

    #[test]
    fn roundtrip_fdw() {
        let bytes = serialize_fdw(
            "kafka_fdw",
            &[("handler".into(), "kafka_fdw_handler".into())],
        );
        let fdw = deserialize_fdw(&bytes).expect("decode");
        assert_eq!(fdw.name, "kafka_fdw");
        assert_eq!(fdw.options[0].0, "handler");
    }

    #[test]
    fn roundtrip_server() {
        let bytes = serialize_server(
            "kafka_s",
            "kafka_fdw",
            &[("bootstrap".into(), "h:9092".into())],
        );
        let s = deserialize_server(&bytes).expect("decode");
        assert_eq!(s.name, "kafka_s");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(s.options[0], ("bootstrap".into(), "h:9092".into()));
    }

    #[test]
    fn roundtrip_user_mapping() {
        let bytes =
            serialize_user_mapping("alice", "kafka_s", &[("token".into(), "secret".into())]);
        let m = deserialize_user_mapping(&bytes).expect("decode");
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "kafka_s");
        assert_eq!(m.options[0], ("token".into(), "secret".into()));
    }

    /// A non-`2` version byte must produce a CorruptRow error.
    #[test]
    fn unknown_version_errors() {
        // Version byte 1 (legacy) is no longer valid.
        assert!(deserialize_schema(&[1, 0, 0, 0, 0]).is_err());
        // Arbitrary unknown version byte is also invalid.
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    /// Ordinary-table round-trip: flag byte `0` is written and read back as `None`.
    #[test]
    fn ordinary_table_flag_zero_roundtrip() {
        let columns = vec![Column {
            name: "x".into(),
            ty: ColumnType::Int4,
        }];
        let bytes = serialize_schema(1, &columns, None);
        // Flag byte position: 1 (version) + 4 (table_id) + 4 (ncols) + col payload
        // col payload = 4 (name len) + 1 (name "x") + 1 (type tag INT4) = 6
        // so flag is at index 15
        assert_eq!(bytes[15], 0, "ordinary flag byte must be 0");
        let (_, _, foreign) = deserialize_schema(&bytes).expect("ordinary table decode");
        assert!(foreign.is_none(), "ordinary table has no foreign meta");
    }

    /// A flag byte that is neither 0 nor 1 must produce a CorruptRow error.
    #[test]
    fn unknown_flag_byte_errors() {
        let columns = vec![Column {
            name: "x".into(),
            ty: ColumnType::Int4,
        }];
        let mut bytes = serialize_schema(1, &columns, None);
        // Overwrite the flag byte with an invalid value.
        let flag_pos = bytes.len() - 1;
        bytes[flag_pos] = 2;
        assert!(deserialize_schema(&bytes).is_err());
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
