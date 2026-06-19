//! Catalog as a stateless view over a `Kv` store: tables, their columns, and
//! CRUD with PostgreSQL error codes. Persistence via SP3's KV layer.

pub mod serde;

use kv::{Kv, KvError, WriteOp, key};
use pgtypes::ColumnType;
use zerocopy::byteorder::big_endian::{U32, U64};
use zerocopy::{FromBytes, IntoBytes};

use crate::serde::{
    deserialize_fdw, deserialize_schema, deserialize_server, deserialize_user_mapping,
    serialize_fdw, serialize_schema, serialize_server, serialize_user_mapping,
};

/// OID-style table identifier (never 0; 0 is reserved/invalid).
pub type TableId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

/// Metadata stored alongside a foreign table that links it to its server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignTableMeta {
    /// The foreign server name this table is attached to.
    pub server: String,
    /// Table-level OPTIONS (e.g. `topic = 'orders'`).
    pub options: Vec<(String, String)>,
}

/// A foreign-data wrapper registration (`CREATE FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignDataWrapper {
    pub name: String,
    /// OPTIONS (e.g. handler, validator).
    pub options: Vec<(String, String)>,
}

/// A foreign server registration (`CREATE SERVER … FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignServer {
    pub name: String,
    /// The FDW this server belongs to.
    pub wrapper: String,
    /// Server-level OPTIONS (e.g. `bootstrap_servers`).
    pub options: Vec<(String, String)>,
}

/// A user mapping (`CREATE USER MAPPING FOR … SERVER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserMapping {
    pub user: String,
    pub server: String,
    /// Mapping-level OPTIONS.
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    /// Present when the table is a foreign table; `None` for ordinary tables.
    pub foreign: Option<ForeignTableMeta>,
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
    /// Generic "object already exists" (42710) — for FDW, server, user-mapping.
    #[error("object \"{0}\" already exists")]
    DuplicateObject(String),
    /// Generic "undefined object" (42704) — for FDW, server, user-mapping.
    #[error("object \"{0}\" does not exist")]
    UndefinedObject(String),
    #[error("catalog storage error: {0}")]
    Storage(#[from] KvError),
}

impl CatalogError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_) => "42P07",
            CatalogError::UndefinedTable(_) => "42P01",
            CatalogError::UndefinedColumn(_) => "42703",
            CatalogError::DuplicateObject(_) => "42710",
            CatalogError::UndefinedObject(_) => "42704",
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
            value: serialize_schema(next, &columns, None),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: U32::new(next + 1).as_bytes().to_vec(),
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
    let (id, columns, foreign) = deserialize_schema(&bytes)?;
    Ok(Table {
        id,
        name: name.to_string(),
        columns,
        foreign,
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

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Register a foreign-data wrapper.
pub fn create_fdw(
    kv: &dyn Kv,
    name: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    if kv.get(&key::fdw_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    kv.write_batch(&[WriteOp::Put {
        key: key::fdw_key(name),
        value: serialize_fdw(name, &options),
    }])?;
    Ok(())
}

/// Look up a foreign-data wrapper by name.
pub fn get_fdw(kv: &dyn Kv, name: &str) -> Result<ForeignDataWrapper, CatalogError> {
    let bytes = kv
        .get(&key::fdw_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_fdw(&bytes)?)
}

/// Drop a foreign-data wrapper.
pub fn drop_fdw(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    // Verify it exists first.
    let _ = get_fdw(kv, name)?;
    kv.write_batch(&[WriteOp::Delete {
        key: key::fdw_key(name),
    }])?;
    Ok(())
}

// ── Foreign server ────────────────────────────────────────────────────────────

/// Register a foreign server.
pub fn create_server(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    if kv.get(&key::server_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    kv.write_batch(&[WriteOp::Put {
        key: key::server_key(name),
        value: serialize_server(name, wrapper, &options),
    }])?;
    Ok(())
}

/// Look up a foreign server by name.
pub fn get_server(kv: &dyn Kv, name: &str) -> Result<ForeignServer, CatalogError> {
    let bytes = kv
        .get(&key::server_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_server(&bytes)?)
}

/// Drop a foreign server.
pub fn drop_server(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let _ = get_server(kv, name)?;
    kv.write_batch(&[WriteOp::Delete {
        key: key::server_key(name),
    }])?;
    Ok(())
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Register a user mapping.
pub fn create_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    if kv.get(&key::user_mapping_key(user, server))?.is_some() {
        return Err(CatalogError::DuplicateObject(format!("{user}@{server}")));
    }
    kv.write_batch(&[WriteOp::Put {
        key: key::user_mapping_key(user, server),
        value: serialize_user_mapping(user, server, &options),
    }])?;
    Ok(())
}

/// Look up a user mapping.
pub fn get_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
) -> Result<UserMapping, CatalogError> {
    let bytes = kv
        .get(&key::user_mapping_key(user, server))?
        .ok_or_else(|| CatalogError::UndefinedObject(format!("{user}@{server}")))?;
    Ok(deserialize_user_mapping(&bytes)?)
}

/// Drop a user mapping.
pub fn drop_user_mapping(kv: &dyn Kv, user: &str, server: &str) -> Result<(), CatalogError> {
    let _ = get_user_mapping(kv, user, server)?;
    kv.write_batch(&[WriteOp::Delete {
        key: key::user_mapping_key(user, server),
    }])?;
    Ok(())
}

// ── Foreign table ─────────────────────────────────────────────────────────────

/// The envelope columns prepended to every foreign (Kafka) table.
fn envelope_columns() -> Vec<Column> {
    vec![
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
    ]
}

/// Create a foreign table linked to an existing server.
///
/// The server must already exist (returns `UndefinedObject` otherwise).
/// Envelope columns are prepended; user-supplied value columns follow.
pub fn create_foreign_table(
    kv: &dyn Kv,
    name: &str,
    value_columns: Vec<Column>,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<TableId, CatalogError> {
    // Validate the server exists.
    let _ = get_server(kv, server)?;

    if kv.get(&key::catalog_key(name))?.is_some() {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }

    let next = read_next_table_id(kv)?;
    let mut columns = envelope_columns();
    columns.extend(value_columns);

    let meta = ForeignTableMeta {
        server: server.to_string(),
        options,
    };

    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns, Some(&meta)),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: U32::new(next + 1).as_bytes().to_vec(),
        },
    ];
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Read the next TableId (defaults to 1 when the meta key is absent).
fn read_next_table_id(kv: &dyn Kv) -> Result<TableId, CatalogError> {
    match kv.get(&key::meta_next_table_id_key())? {
        Some(b) => {
            let (v, _) = U32::read_from_prefix(b.as_slice())
                .map_err(|_| KvError::CorruptRow("next_table_id is not u32".into()))?;
            Ok(v.get())
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
        assert_eq!(t.column_index("id"), Some(0));
        assert_eq!(t.column_index("name"), Some(1));
        assert_eq!(t.column_index("nope"), None);
        assert!(t.foreign.is_none());
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

    fn check_fdw_crud(kv: &dyn Kv) {
        create_server(
            kv,
            "s",
            "kafka_fdw",
            vec![("bootstrap".into(), "h:9092".into())],
        )
        .expect("create server");
        let s = get_server(kv, "s").expect("get");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(
            create_server(kv, "s", "kafka_fdw", vec![])
                .expect_err("dup")
                .sqlstate(),
            "42710"
        );
        assert_eq!(
            get_server(kv, "nope").expect_err("missing").sqlstate(),
            "42704"
        );

        let cols = vec![Column {
            name: "id".into(),
            ty: ColumnType::Int4,
        }];
        create_foreign_table(
            kv,
            "orders",
            cols,
            "s",
            vec![("topic".into(), "orders".into())],
        )
        .expect("ft");
        let t = get_table(kv, "orders").expect("get ft");
        assert!(t.foreign.is_some());
        // envelope columns prepended:
        assert_eq!(t.columns[0].name, "_partition");
        assert_eq!(t.columns[0].ty, ColumnType::Int4);
        assert_eq!(t.columns[3].name, "_key");
        assert_eq!(t.columns.last().expect("value col").name, "id");
    }

    #[test]
    fn fdw_crud_memkv() {
        check_fdw_crud(&MemKv::new());
    }

    // ── Mutation-killing targeted tests ──────────────────────────────────────

    /// `create_fdw` must actually persist (get_fdw returns the wrapper after
    /// creation) — kills the "replace create_fdw with Ok(())" mutant.
    #[test]
    fn create_fdw_persists() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        let fdw = get_fdw(&kv, "w").expect("must be persisted");
        assert_eq!(fdw.name, "w");
    }

    /// `drop_fdw` must actually delete (get_fdw returns 42704 after drop) — kills
    /// the "replace drop_fdw with Ok(())" mutant.
    #[test]
    fn drop_fdw_removes() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        drop_fdw(&kv, "w").expect("drop");
        assert_eq!(get_fdw(&kv, "w").expect_err("gone").sqlstate(), "42704");
    }

    /// `drop_server` must actually delete — kills the "replace drop_server with Ok(())" mutant.
    #[test]
    fn drop_server_removes() {
        let kv = MemKv::new();
        create_server(&kv, "s", "fdw", vec![]).expect("create");
        drop_server(&kv, "s").expect("drop");
        assert_eq!(get_server(&kv, "s").expect_err("gone").sqlstate(), "42704");
    }

    /// `create_user_mapping` must actually persist — kills the
    /// "replace create_user_mapping with Ok(())" mutant.
    #[test]
    fn create_user_mapping_persists() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "alice", "s", vec![("k".into(), "v".into())]).expect("create");
        let m = get_user_mapping(&kv, "alice", "s").expect("must be persisted");
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "s");
    }

    /// `drop_user_mapping` must actually delete — kills the
    /// "replace drop_user_mapping with Ok(())" mutant.
    #[test]
    fn drop_user_mapping_removes() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "bob", "s2", vec![]).expect("create");
        drop_user_mapping(&kv, "bob", "s2").expect("drop");
        assert_eq!(
            get_user_mapping(&kv, "bob", "s2")
                .expect_err("gone")
                .sqlstate(),
            "42704"
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
