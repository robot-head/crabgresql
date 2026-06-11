//! In-memory system catalog: tables, their columns, and CRUD with PostgreSQL
//! error codes. Persistence arrives in SP3; no pg_catalog SQL views in SP2.

use std::collections::HashMap;
use std::sync::RwLock;

use pgtypes::ColumnType;

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
}

impl CatalogError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_) => "42P07",
            CatalogError::UndefinedTable(_) => "42P01",
            CatalogError::UndefinedColumn(_) => "42703",
        }
    }
}

struct Inner {
    next_id: TableId,
    by_name: HashMap<String, Table>,
}

/// The catalog. Cheap to share behind an `Arc`; internally `RwLock`-guarded.
pub struct Catalog {
    inner: RwLock<Inner>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                next_id: 1,
                by_name: HashMap::new(),
            }),
        }
    }

    pub fn create_table(&self, name: &str, columns: Vec<Column>) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.write().expect("catalog lock");
        if inner.by_name.contains_key(name) {
            return Err(CatalogError::DuplicateTable(name.to_string()));
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.by_name.insert(
            name.to_string(),
            Table {
                id,
                name: name.to_string(),
                columns,
            },
        );
        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.write().expect("catalog lock");
        if inner.by_name.remove(name).is_none() {
            return Err(CatalogError::UndefinedTable(name.to_string()));
        }
        Ok(())
    }

    /// Snapshot of a table's metadata by name.
    pub fn get_table(&self, name: &str) -> Result<Table, CatalogError> {
        self.inner
            .read()
            .expect("catalog lock")
            .by_name
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn create_lookup_drop() {
        let cat = Catalog::new();
        let id = cat.create_table("t", cols()).expect("create");
        let t = cat.get_table("t").expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("name"), Some(1));
        assert_eq!(t.column_index("missing"), None);
        cat.drop_table("t").expect("drop");
        assert!(matches!(
            cat.get_table("t"),
            Err(CatalogError::UndefinedTable(_))
        ));
    }

    #[test]
    #[allow(non_snake_case)]
    fn duplicate_table_is_42P07() {
        let cat = Catalog::new();
        cat.create_table("t", cols()).expect("create");
        let err = cat.create_table("t", cols()).expect_err("dup");
        assert_eq!(err.sqlstate(), "42P07");
    }

    #[test]
    #[allow(non_snake_case)]
    fn drop_missing_is_42P01() {
        let cat = Catalog::new();
        let err = cat.drop_table("nope").expect_err("missing");
        assert_eq!(err.sqlstate(), "42P01");
    }

    #[test]
    fn table_ids_are_distinct_and_nonzero() {
        let cat = Catalog::new();
        let a = cat.create_table("a", cols()).expect("a");
        let b = cat.create_table("b", cols()).expect("b");
        assert_ne!(a, b);
        assert!(a >= 1 && b >= 1);
    }
}
