//! Map lower-crate error enums onto wire `PgError`s with the right SQLSTATE.

use catalog::CatalogError;
use kv::KvError;
use pgparser::ParseError;
use pgtypes::TypeError;
use pgwire::error::PgError;

/// Executor-level error; converts to a non-fatal `PgError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    Parse(ParseError),
    Catalog(CatalogError),
    Type(TypeError),
    Kv(KvError),
    /// Column referenced that the row/table doesn't have (42703).
    UndefinedColumn(String),
    /// In-grammar but unimplemented (0A000) — e.g. $1 parameters.
    Unsupported(String),
    /// Wrong type in a context that demands a specific one (42804) — e.g. a
    /// non-boolean WHERE.
    TypeMismatch(String),
}

impl ExecError {
    pub fn into_pg(self) -> PgError {
        match self {
            ExecError::Parse(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Catalog(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Type(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Kv(e) => PgError::error("XX000", e.to_string()),
            ExecError::UndefinedColumn(c) => {
                PgError::error("42703", format!("column \"{c}\" does not exist"))
            }
            ExecError::Unsupported(m) => PgError::error("0A000", m),
            ExecError::TypeMismatch(m) => PgError::error("42804", m),
        }
    }
}

impl From<ParseError> for ExecError {
    fn from(e: ParseError) -> Self {
        ExecError::Parse(e)
    }
}
impl From<CatalogError> for ExecError {
    fn from(e: CatalogError) -> Self {
        ExecError::Catalog(e)
    }
}
impl From<TypeError> for ExecError {
    fn from(e: TypeError) -> Self {
        ExecError::Type(e)
    }
}
impl From<KvError> for ExecError {
    fn from(e: KvError) -> Self {
        ExecError::Kv(e)
    }
}
