//! The crabgresql AST for the SP2 slice.

use pgtypes::ColumnType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select(SelectStmt),
    Begin {
        isolation: Option<IsolationLevel>,
    },
    Commit,
    Rollback,
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
}

/// Transaction isolation levels supported by SP4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockStrength {
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub projection: Vec<SelectItem>,
    pub from: Option<String>,
    pub filter: Option<Expr>,
    /// SP28: `SELECT DISTINCT` — dedup the projected output rows.
    pub distinct: bool,
    /// SP27: `GROUP BY <expr-list>` (empty when absent).
    pub group_by: Vec<Expr>,
    /// SP27: `HAVING <predicate>` (evaluated per group).
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    /// SP28: `OFFSET <n>` — skip the first `n` output rows (before LIMIT).
    pub offset: Option<i64>,
    pub locking: Option<RowLockStrength>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLiteral(String),
    StringLiteral(String),
    BoolLiteral(bool),
    NullLiteral,
    Column(String),
    Param(u32),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// SP27: a function call, e.g. `count(*)`, `sum(a + 1)`, `count(DISTINCT x)`.
    /// Whether a name is an aggregate (vs. an unknown/undefined function) is
    /// decided by the executor, not the parser.
    Func(FuncCall),
    /// SP28: `expr IS [NOT] NULL`. Never evaluates to NULL itself.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] IN (e1, e2, …)` — value-list membership (not a subquery).
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] BETWEEN low AND high` (bounds inclusive).
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] LIKE pat` / `[NOT] ILIKE pat`. `%`/`_` wildcards with a
    /// `\` escape; `case_insensitive` is the ILIKE form.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    /// SP28: a `CASE` expression. `operand` is `Some` for the simple form
    /// (`CASE x WHEN v THEN r …`) and `None` for the searched form
    /// (`CASE WHEN cond THEN r …`). `whens` is non-empty (parser-enforced).
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
}

/// SP27: a parsed function call. `name` is lowercased by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub struct FuncCall {
    pub name: String,
    /// `true` for `f(DISTINCT …)`. `ALL` (the default) parses to `false`.
    pub distinct: bool,
    pub args: FuncArgs,
}

/// SP27: a function call's argument list. `Star` is the `f(*)` form (only
/// `count(*)` is meaningful); `Exprs` is a (possibly empty) positional list.
#[derive(Debug, Clone, PartialEq)]
pub enum FuncArgs {
    Star,
    Exprs(Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    /// SP29: `||` string concatenation.
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
