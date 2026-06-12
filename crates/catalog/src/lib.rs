//! Catalog as a stateless view over a `Kv` store: tables, their columns, and
//! CRUD with PostgreSQL error codes. Persistence via SP3's KV layer.

pub mod serde;

use kv::{Kv, KvError, WriteOp, key};
use pgtypes::ColumnType;

use crate::serde::{deserialize_schema, serialize_schema};

/// OID-style table identifier (never 0; 0 is reserved/invalid).
pub type TableId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Zero-based ordinal of a column by name, or None.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
    #[error("catalog storage error: {0}")]
    Storage(#[from] KvError),
}

impl CatalogError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_) => "42P07",
            CatalogError::UndefinedTable(_) => "42P01",
            CatalogError::UndefinedColumn(_) => "42703",
            CatalogError::Storage(KvError::Io(_)) => "58030",
            CatalogError::Storage(KvError::CorruptRow(_)) => "XX000",
        }
    }
}

/// Build the write batch for creating a table (schema + sequence init +
/// next_table_id bump) WITHOUT writing — caller persists the ops. Returns the
/// allocated TableId alongside the batch. Used by the executor so DDL writes can
/// be routed through the durable-write seam (and replicated). Validation
/// (duplicate-table check, next_table_id read) is identical to `create_table`.
pub fn create_table_ops(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if kv.get(&key::catalog_key(name))?.is_some() {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let next = read_next_table_id(kv)?;
    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: 1u64.to_be_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: (next + 1).to_be_bytes().to_vec(),
        },
    ];
    Ok((next, batch))
}

/// Create a table: allocate a TableId, persist the schema, init the sequence —
/// all in one atomic batch. Caller serializes concurrent DDL.
pub fn create_table(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_table_ops(kv, name, columns)?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Look up a table by name.
pub fn get_table(kv: &dyn Kv, name: &str) -> Result<Table, CatalogError> {
    let bytes = kv
        .get(&key::catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns) = deserialize_schema(&bytes)?;
    Ok(Table {
        id,
        name: name.to_string(),
        columns,
    })
}

/// Build the write batch for dropping a table (catalog entry + sequence + every
/// row) WITHOUT writing — caller persists the ops. Errors (42P01 on a missing
/// table) are identical to `drop_table`. Used by the executor to route DDL
/// writes through the durable-write seam.
pub fn drop_table_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let table = get_table(kv, name)?;
    let mut ops = vec![
        WriteOp::Delete {
            key: key::catalog_key(name),
        },
        WriteOp::Delete {
            key: key::seq_key(table.id),
        },
    ];
    for (row_key, _) in kv.scan_prefix(&key::table_prefix(table.id))? {
        ops.push(WriteOp::Delete { key: row_key });
    }
    Ok(ops)
}

/// Drop a table: delete the catalog entry, the sequence, and all its rows — one
/// atomic batch.
pub fn drop_table(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_table_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Read the next TableId (defaults to 1 when the meta key is absent).
fn read_next_table_id(kv: &dyn Kv) -> Result<TableId, CatalogError> {
    match kv.get(&key::meta_next_table_id_key())? {
        Some(b) => {
            let arr: [u8; 4] = b
                .as_slice()
                .try_into()
                .map_err(|_| KvError::CorruptRow("next_table_id is not u32".into()))?;
            Ok(u32::from_be_bytes(arr))
        }
        None => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::{FjallKv, MemKv};
    use pgtypes::ColumnType;

    fn cols() -> Vec<Column> {
        vec![
            Column {
                name: "id".into(),
                ty: ColumnType::Int4,
            },
            Column {
                name: "name".into(),
                ty: ColumnType::Text,
            },
        ]
    }

    fn check_crud(kv: &dyn Kv) {
        let id = create_table(kv, "t", cols()).expect("create");
        let t = get_table(kv, "t").expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("name"), Some(1));
        // Duplicate → 42P07.
        assert_eq!(
            create_table(kv, "t", cols()).expect_err("dup").sqlstate(),
            "42P07"
        );
        // Distinct ids.
        let id2 = create_table(kv, "u", cols()).expect("create u");
        assert_ne!(id, id2);
        // Drop → gone.
        drop_table(kv, "t").expect("drop");
        assert_eq!(get_table(kv, "t").expect_err("gone").sqlstate(), "42P01");
        // Drop missing → 42P01.
        assert_eq!(
            drop_table(kv, "nope").expect_err("missing").sqlstate(),
            "42P01"
        );
    }

    #[test]
    fn crud_on_memkv() {
        check_crud(&MemKv::new());
    }

    #[test]
    fn crud_on_fjallkv() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_crud(&FjallKv::open(dir.path()).expect("open"));
    }
}
