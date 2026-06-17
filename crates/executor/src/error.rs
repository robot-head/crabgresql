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
    /// A column reference matched more than one table in scope (42702).
    AmbiguousColumn(String),
    /// A qualified reference named a table not in the FROM clause (42P01).
    MissingFromEntry(String),
    /// The same table name/alias appears twice in one FROM clause (42712).
    DuplicateAlias(String),
    /// In-grammar but unimplemented (0A000) — e.g. $1 parameters.
    Unsupported(String),
    /// Wrong type in a context that demands a specific one (42804) — e.g. a
    /// non-boolean WHERE.
    TypeMismatch(String),
    /// A grouping/aggregation rule was violated (42803) — e.g. a column that is
    /// neither grouped nor inside an aggregate, or a nested aggregate.
    Grouping(String),
    /// A call to a function that does not exist (42883) — e.g. an unknown name
    /// or an aggregate applied to an argument type/arity it does not accept.
    UndefinedFunction(String),
    /// An object was used in a way its kind does not allow (42809) — e.g.
    /// `DISTINCT`/`ALL` applied to a scalar (non-aggregate) function.
    WrongObjectType(String),
    /// A scalar subquery returned more than one row (21000).
    CardinalityViolation,
    /// A subquery used as an expression / IN / quantified source returned more than
    /// one column (42601).
    SubqueryColumns,
    /// A statement was issued in an aborted transaction block (25P02): every
    /// command after an error (until COMMIT/ROLLBACK) is rejected.
    InFailedTransaction,
    /// A write conflicted with a concurrently-committed change under REPEATABLE
    /// READ (40001) — the client should retry the transaction.
    SerializationFailure,
    /// A deadlock was detected and this transaction was chosen as the victim (40P01).
    Deadlock,
    /// The write hit a node that is not the Raft leader; the client should retry.
    NotLeader,
    /// The write could not reach a majority (partition/timeout); no partial state
    /// was applied; the client should retry.
    Unavailable,
    /// SP37: a `SET`/`RESET` supplied a value the parameter cannot accept (22023) —
    /// e.g. an unknown time-zone name, or a non-default `datestyle`.
    InvalidParameterValue(String),
    /// SP37: a `SET`/`SHOW`/`RESET` named a configuration parameter that does not
    /// exist (42704).
    UnrecognizedParameter(String),
}

impl ExecError {
    pub fn into_pg(self) -> PgError {
        match self {
            ExecError::Parse(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Catalog(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Type(e) => PgError::error(e.sqlstate(), e.to_string()),
            ExecError::Kv(e) => match e {
                kv::KvError::Io(msg) => {
                    PgError::error("58030", format!("storage I/O error: {msg}"))
                }
                kv::KvError::CorruptRow(msg) => {
                    PgError::error("XX000", format!("corrupt storage: {msg}"))
                }
            },
            ExecError::UndefinedColumn(c) => {
                PgError::error("42703", format!("column \"{c}\" does not exist"))
            }
            ExecError::AmbiguousColumn(c) => {
                PgError::error("42702", format!("column reference \"{c}\" is ambiguous"))
            }
            ExecError::CardinalityViolation => PgError::error(
                "21000",
                "more than one row returned by a subquery used as an expression",
            ),
            ExecError::SubqueryColumns => {
                PgError::error("42601", "subquery must return only one column")
            }
            ExecError::MissingFromEntry(t) => PgError::error(
                "42P01",
                format!("missing FROM-clause entry for table \"{t}\""),
            ),
            ExecError::DuplicateAlias(t) => PgError::error(
                "42712",
                format!("table name \"{t}\" specified more than once"),
            ),
            ExecError::Unsupported(m) => PgError::error("0A000", m),
            ExecError::TypeMismatch(m) => PgError::error("42804", m),
            ExecError::Grouping(m) => PgError::error("42803", m),
            ExecError::UndefinedFunction(m) => PgError::error("42883", m),
            ExecError::WrongObjectType(m) => PgError::error("42809", m),
            ExecError::InFailedTransaction => PgError::error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
            ExecError::SerializationFailure => PgError::error(
                "40001",
                "could not serialize access due to concurrent update",
            ),
            ExecError::Deadlock => PgError::error("40P01", "deadlock detected"),
            ExecError::NotLeader => {
                PgError::error("40001", "could not complete: not the leader, retry")
            }
            ExecError::Unavailable => PgError::error("08006", "connection failure: no quorum"),
            ExecError::InvalidParameterValue(v) => {
                PgError::error("22023", format!("invalid value for parameter: \"{v}\""))
            }
            ExecError::UnrecognizedParameter(n) => PgError::error(
                "42704",
                format!("unrecognized configuration parameter \"{n}\""),
            ),
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
