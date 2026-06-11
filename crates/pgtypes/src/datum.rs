//! The runtime value type and the SQL column types of the SP2 slice.

/// PostgreSQL type OIDs (from pg_type.dat) for the slice's types.
pub mod oids {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
}

/// A SQL column type in the SP2 slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int4,
    Int8,
    Text,
}

impl ColumnType {
    pub fn from_sql_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "int4" | "integer" | "int" => Some(ColumnType::Int4),
            "int8" | "bigint" => Some(ColumnType::Int8),
            "text" => Some(ColumnType::Text),
            "bool" | "boolean" => Some(ColumnType::Bool),
            _ => None,
        }
    }

    pub fn oid(self) -> u32 {
        match self {
            ColumnType::Bool => oids::BOOL,
            ColumnType::Int8 => oids::INT8,
            ColumnType::Int4 => oids::INT4,
            ColumnType::Text => oids::TEXT,
        }
    }

    /// PostgreSQL type name (for error messages and FieldDescription debugging).
    pub fn name(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int8 => "bigint",
            ColumnType::Int4 => "integer",
            ColumnType::Text => "text",
        }
    }

    /// pg_type.typlen: fixed sizes, -1 for variable-length text.
    pub fn type_size(self) -> i16 {
        match self {
            ColumnType::Bool => 1,
            ColumnType::Int8 => 8,
            ColumnType::Int4 => 4,
            ColumnType::Text => -1,
        }
    }
}

/// A runtime value.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
}

impl Datum {
    /// The non-null column type of this value (None for NULL).
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(ColumnType::Bool),
            Datum::Int4(_) => Some(ColumnType::Int4),
            Datum::Int8(_) => Some(ColumnType::Int8),
            Datum::Text(_) => Some(ColumnType::Text),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_from_sql_names_and_aliases() {
        assert_eq!(ColumnType::from_sql_name("int4"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("integer"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("INT"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("int8"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("bigint"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("text"), Some(ColumnType::Text));
        assert_eq!(ColumnType::from_sql_name("bool"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("boolean"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("widget"), None);
    }

    #[test]
    fn column_type_oids_match_postgres() {
        assert_eq!(ColumnType::Bool.oid(), 16);
        assert_eq!(ColumnType::Int8.oid(), 20);
        assert_eq!(ColumnType::Int4.oid(), 23);
        assert_eq!(ColumnType::Text.oid(), 25);
    }

    #[test]
    fn datum_reports_its_column_type() {
        assert_eq!(Datum::Int4(1).column_type(), Some(ColumnType::Int4));
        assert_eq!(Datum::Null.column_type(), None);
    }
}
