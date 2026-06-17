//! The crabgresql AST for the SP2 slice.

use pgtypes::{ColumnType, Datum};

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
    /// SP37: `SET [LOCAL] <name> = <value>` / `SET <name> TO <value>` / `SET TIME ZONE ...`.
    Set {
        local: bool,
        name: String,
        value: SetValue,
    },
    /// SP37: `SHOW <name>` / `SHOW TIME ZONE`.
    Show {
        name: String,
    },
    /// SP37: `RESET <name>`.
    Reset {
        name: String,
    },
}

/// SP37: the right-hand side of a `SET` (or the value form of `SET TIME ZONE`).
/// `Default` is `SET ... = DEFAULT` / `SET TIME ZONE { DEFAULT | LOCAL }` (resets
/// the parameter to its built-in default); `Value` is a literal/identifier value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    Default,
    Value(String),
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
    /// SP33: the FROM clause — a list of join trees. Empty for a FROM-less SELECT;
    /// the comma form (`FROM a, b`) is a `Vec<TableExpr>` with len > 1 (implicit
    /// cross join).
    pub from: Vec<TableExpr>,
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
    /// SP33: `a.*` — every column of one table in scope.
    QualifiedWildcard(String),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

/// SP33: one entry in the FROM clause — a base table, a derived table
/// (subquery), or a join of two table-exprs. The comma form (`FROM a, b`) is a
/// `Vec<TableExpr>` with len > 1 (implicit cross join).
#[derive(Debug, Clone, PartialEq)]
pub enum TableExpr {
    Table {
        name: String,
        alias: Option<String>,
    },
    Derived {
        subquery: Box<SelectStmt>,
        alias: String, // PG requires a derived table to be aliased
    },
    Join {
        left: Box<TableExpr>,
        right: Box<TableExpr>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
    Natural,
    None, // CROSS JOIN / comma
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLiteral(String),
    /// SP32: a decimal/exponent literal. PostgreSQL types these as `numeric`
    /// (SP30 typed them `float8`; SP32 introduced `numeric`, so a bare `1.5`/`1e3`
    /// is now scale-faithful `numeric` — `float8` requires an explicit cast).
    NumericLiteral(String),
    StringLiteral(String),
    BoolLiteral(bool),
    NullLiteral,
    /// SP33: a column reference, optionally table-qualified (`a.col`). `table` is
    /// `None` for a bare `col`.
    Column {
        table: Option<String>,
        name: String,
    },
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
    /// SP31: an explicit cast — `CAST(expr AS ty)` or `expr::ty`. The target type
    /// is resolved to a [`ColumnType`] by the parser (an unknown type name is a
    /// parse error); the executor performs the value conversion.
    Cast {
        expr: Box<Expr>,
        ty: ColumnType,
    },
    /// SP34: a scalar subquery `(SELECT …)` — one row, one column, usable as an
    /// expression. Resolved (uncorrelated) to `Const` by the executor pre-pass.
    ScalarSubquery(Box<SelectStmt>),
    /// SP34: `EXISTS (SELECT …)` — true iff the subquery returns ≥1 row. `NOT
    /// EXISTS` is the prefix `NOT` wrapping this.
    Exists(Box<SelectStmt>),
    /// SP34: `expr [NOT] IN (SELECT …)` — subquery membership (single-column subquery).
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    /// SP34: `expr op ANY|SOME|ALL (SELECT …)`. `all` is the `ALL` form; `ANY`/`SOME`
    /// are `all == false`. The subquery is single-column.
    Quantified {
        expr: Box<Expr>,
        op: BinaryOp,
        all: bool,
        subquery: Box<SelectStmt>,
    },
    /// SP34: an executor-produced literal — a resolved subquery folded to a value
    /// carrying its static type. The parser NEVER emits this; `ty` matters because a
    /// zero-row scalar subquery is a typed NULL.
    Const {
        value: Datum,
        ty: ColumnType,
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
