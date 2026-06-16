//! Versioned (de)serialization of a table schema — the value stored under
//! `kv::key::catalog_key(name)`. Format: version byte, table_id (u32 BE),
//! column count (u32 BE), then per column: u32 name length, name bytes, type tag.

use kv::KvError;
use pgtypes::ColumnType;

use crate::Column;

/// Current schema-value format version.
pub const SCHEMA_VERSION: u8 = 1;

mod type_tag {
    pub const BOOL: u8 = 0;
    pub const INT4: u8 = 1;
    pub const INT8: u8 = 2;
    pub const TEXT: u8 = 3;
}

fn tag_of(ty: ColumnType) -> u8 {
    match ty {
        ColumnType::Bool => type_tag::BOOL,
        ColumnType::Int4 => type_tag::INT4,
        ColumnType::Int8 => type_tag::INT8,
        ColumnType::Text => type_tag::TEXT,
    }
}

fn type_of(tag: u8) -> Result<ColumnType, KvError> {
    Ok(match tag {
        type_tag::BOOL => ColumnType::Bool,
        type_tag::INT4 => ColumnType::Int4,
        type_tag::INT8 => ColumnType::Int8,
        type_tag::TEXT => ColumnType::Text,
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
        out.push(tag_of(c.ty));
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
        let ty = type_of(take_u8(&mut cur)?)?;
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
