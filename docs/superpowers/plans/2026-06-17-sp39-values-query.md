# SP39 VALUES Query Expressions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PostgreSQL-compatible `VALUES` query expressions as standalone statements, set-operation branches, and non-correlated derived table row sources.

**Architecture:** Generalize the parser's set-operation leaf from `SelectStmt` to a `QueryBody` enum containing `SELECT` and `VALUES`. Add one executor `values` module that materializes values rows into the existing `Relation` shape, then reuse that helper from standalone statement execution, set operations, derived tables, and describe. Treat table-free values queries as range-neutral in the router while preserving the existing single-range statement invariant.

**Tech Stack:** Rust 2024, hand-written `pgparser` lexer/parser, `executor` relation pipeline (`Scope`, `Relation`, `select_to_relation`, `setops`), `pgtypes` casts/type inference, `cluster` range router, `tokio-postgres` wire tests, PostgreSQL conformance corpus.

**Spec:** `docs/superpowers/specs/2026-06-17-crabgresql-sp39-values-query-design.md`

---

## Conventions For Every Task

- Worktree root: `/home/matt/.codex/worktrees/a768/crabgresql`.
- Use `cargo fmt` before every commit.
- Use nextest for crate tests: `cargo nextest run -p <crate>`.
- Use targeted tests first, then broaden.
- Avoid Windows UAC target-name substrings in any new integration test filename: `setup`, `install`, `update`, `patch`, `upgrad`.
- Commit after each task with the exact files listed for that task.

---

## File Structure

- `crates/pgparser/src/token.rs`: add `VALUES` as a keyword.
- `crates/pgparser/src/ast.rs`: add `ValuesStmt`, `QueryBody`, `ValuesQuery`, and derived-table column aliases; update `SetExpr` leaves.
- `crates/pgparser/src/parser.rs`: parse `VALUES`, general query primaries, set-op leaves, and `FROM (VALUES ...) AS alias(cols...)`.
- `crates/executor/src/values.rs`: new focused module for values relation building, type resolution, coercion, describe schema, and result ordering.
- `crates/executor/src/exec.rs`: route derived-table `QueryBody`, describe `VALUES`, and reuse existing relation helpers.
- `crates/executor/src/setops.rs`: fold `QueryBody` leaves instead of select-only leaves and reuse values type resolution.
- `crates/executor/src/session.rs`: dispatch standalone values queries through the read context.
- `crates/executor/src/error.rs`: add value/alias arity error surfaces.
- `crates/executor/src/lib.rs`: register `mod values`.
- `crates/cluster/src/range/router.rs`: collect ranges from `QueryBody` while treating values as range-neutral.
- `crates/executor/tests/values_query.rs`: new wire integration test.
- `crates/conformance/corpus/values.sql`: new PostgreSQL parity corpus.
- `crates/pgparser/tests/libpg_query_oracle.rs`: add accepted `VALUES` forms.
- `Cargo.toml`: add the new executor integration test target if this repo uses explicit test targets.
- `CLAUDE.md`: add SP39 completion note after implementation, including target-name audit.

---

## Task 1: Parser AST And `VALUES` Keyword

**Files:**
- Modify: `crates/pgparser/src/token.rs`
- Modify: `crates/pgparser/src/ast.rs`
- Test: `crates/pgparser/src/token.rs`

- [ ] **Step 1: Add failing token test coverage.**

In `crates/pgparser/src/token.rs`, extend the existing keyword round-trip test list with:

```rust
            ("values", Keyword::Values),
```

- [ ] **Step 2: Run the keyword test and confirm it fails.**

Run:

```bash
cargo nextest run -p pgparser token::
```

Expected: FAIL because `Keyword::Values` does not exist yet.

- [ ] **Step 3: Add the keyword.**

In `crates/pgparser/src/token.rs`, add to `enum Keyword` near the other query keywords:

```rust
    Values,
```

Add to `Keyword::from_word`:

```rust
            "values" => Keyword::Values,
```

- [ ] **Step 4: Add query-body AST nodes.**

In `crates/pgparser/src/ast.rs`, change `Statement` by adding this variant after `Select(SelectStmt)`:

```rust
    /// SP39: a standalone VALUES query expression with result-level ORDER BY/LIMIT/OFFSET.
    Values(ValuesQuery),
```

Add these types after `SelectStmt`:

```rust
/// SP39: a complete standalone VALUES query. Set-operation queries still use
/// `SetQuery`; this variant is only for a lone VALUES statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesQuery {
    pub body: ValuesStmt,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// SP39: a VALUES row constructor list. Every row is non-empty; cross-row arity
/// is checked during executor analysis so it gets PostgreSQL's analysis SQLSTATE.
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesStmt {
    pub rows: Vec<Vec<Expr>>,
}

/// SP39: query bodies that may appear as set-operation leaves or derived tables.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    Select(Box<SelectStmt>),
    Values(ValuesStmt),
}
```

Change `SetExpr` from a select-only leaf to a query-body leaf:

```rust
pub enum SetExpr {
    Query(QueryBody),
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}
```

Change `TableExpr::Derived` to carry a `QueryBody` and optional derived column aliases:

```rust
    Derived {
        subquery: QueryBody,
        alias: String,
        columns: Option<Vec<String>>,
    },
```

- [ ] **Step 5: Update direct compile fallout inside `pgparser` tests.**

Where existing parser tests match `SetExpr::Select(s)`, change those matches to:

```rust
SetExpr::Query(QueryBody::Select(s))
```

Where existing tests construct or match `TableExpr::Derived { subquery, alias }`, update to include `columns`:

```rust
TableExpr::Derived { subquery, alias, columns }
```

- [ ] **Step 6: Verify the parser crate compiles far enough for AST changes.**

Run:

```bash
cargo nextest run -p pgparser token::
```

Expected: PASS for token tests. Parser tests may still fail to compile if they rely on old `SetExpr::Select`; fix only the direct enum-shape references in `crates/pgparser` during this task.

- [ ] **Step 7: Format and commit.**

Run:

```bash
cargo fmt
git add crates/pgparser/src/token.rs crates/pgparser/src/ast.rs crates/pgparser/src/parser.rs
git commit -m "SP39: add VALUES query AST"
```

---

## Task 2: Parser Grammar For `VALUES`, Set-Op Branches, And Derived Tables

**Files:**
- Modify: `crates/pgparser/src/parser.rs`
- Test: `crates/pgparser/src/parser.rs`
- Test: `crates/pgparser/tests/libpg_query_oracle.rs`

- [ ] **Step 1: Add failing parser tests.**

Add these tests to `crates/pgparser/src/parser.rs` `mod tests`:

```rust
    #[test]
    fn parses_standalone_values_query() {
        use crate::ast::{Expr, Statement};
        let s = crate::parse("VALUES (1, 'a'), (2, 'b') ORDER BY 1 LIMIT 1 OFFSET 1").unwrap();
        let Statement::Values(q) = &s[0] else { panic!("expected VALUES, got {:?}", s[0]) };
        assert_eq!(q.body.rows.len(), 2);
        assert_eq!(q.body.rows[0].len(), 2);
        assert!(matches!(q.body.rows[0][0], Expr::IntLiteral(_)));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn parses_values_as_set_operation_branch() {
        use crate::ast::{QueryBody, SetExpr, SetOp, Statement};
        let s = crate::parse("VALUES (1) UNION ALL SELECT 2").unwrap();
        let Statement::SetOperation(q) = &s[0] else { panic!("expected set op") };
        let SetExpr::SetOp { op, all, left, right } = &q.body else { panic!("expected set op body") };
        assert_eq!(*op, SetOp::Union);
        assert!(*all);
        assert!(matches!(&**left, SetExpr::Query(QueryBody::Values(_))));
        assert!(matches!(&**right, SetExpr::Query(QueryBody::Select(_))));
    }

    #[test]
    fn parses_values_derived_table_with_column_aliases() {
        use crate::ast::{QueryBody, Statement, TableExpr};
        let s = crate::parse("SELECT id, name FROM (VALUES (1, 'a')) AS v(id, name)").unwrap();
        let Statement::Select(sel) = &s[0] else { panic!("expected select") };
        let TableExpr::Derived { subquery, alias, columns } = &sel.from[0] else {
            panic!("expected derived table")
        };
        assert!(matches!(subquery, QueryBody::Values(_)));
        assert_eq!(alias, "v");
        assert_eq!(columns.as_ref().unwrap(), &vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn values_rows_must_have_at_least_one_expr() {
        assert!(crate::parse("VALUES ()").is_err());
    }
```

- [ ] **Step 2: Add libpg_query accepted forms.**

In `crates/pgparser/tests/libpg_query_oracle.rs`, add to `ACCEPTED`:

```rust
    "VALUES (1), (2)",
    "VALUES (1) UNION SELECT 2",
    "SELECT x FROM (VALUES (1), (2)) AS v(x)",
```

- [ ] **Step 3: Run parser tests and confirm they fail.**

Run:

```bash
cargo nextest run -p pgparser parses_standalone_values_query parses_values_as_set_operation_branch parses_values_derived_table_with_column_aliases values_rows_must_have_at_least_one_expr
```

Expected: FAIL because the grammar does not parse `VALUES` yet.

- [ ] **Step 4: Add `values_stmt` parser helper.**

In `crates/pgparser/src/parser.rs`, add near `select_core`:

```rust
    fn values_stmt(&mut self) -> Result<crate::ast::ValuesStmt, ParseError> {
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            if *self.peek() == Token::RParen {
                return Err(ParseError::new("VALUES row must have at least one expression", self.peek_pos()));
            }
            let mut row = vec![self.expr(0)?];
            while self.eat_comma() {
                row.push(self.expr(0)?);
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if !self.eat_comma() {
                break;
            }
        }
        Ok(crate::ast::ValuesStmt { rows })
    }
```

- [ ] **Step 5: Route statement dispatch for `VALUES`.**

In `statement()`, route `VALUES` to `query_stmt()`:

```rust
Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values) | Token::LParen => {
    self.query_stmt()
}
```

- [ ] **Step 6: Generalize `query_stmt` and set primaries.**

Update imports and body matching in `query_stmt`:

```rust
use crate::ast::{QueryBody, SetExpr, SetQuery, Statement, ValuesQuery};
let body = self.set_expr(0)?;
let (order_by, limit, offset) = self.parse_set_tail()?;
match body {
    SetExpr::Query(QueryBody::Select(mut s)) => {
        s.order_by = order_by;
        s.limit = limit;
        s.offset = offset;
        s.locking = self.parse_locking()?;
        Ok(Statement::Select(*s))
    }
    SetExpr::Query(QueryBody::Values(v)) => Ok(Statement::Values(ValuesQuery {
        body: v,
        order_by,
        limit,
        offset,
    })),
    body => {
        if matches!(self.peek(), Token::Keyword(Keyword::For)) {
            return Err(ParseError::new(
                "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
                self.peek_pos(),
            ));
        }
        Ok(Statement::SetOperation(SetQuery { body, order_by, limit, offset }))
    }
}
```

Update `set_primary`:

```rust
if *self.peek() == Token::LParen {
    self.bump();
    let inner = self.set_expr(0)?;
    let inner = self.attach_paren_tail(inner)?;
    self.expect(&Token::RParen)?;
    Ok(inner)
} else if *self.peek() == Token::Keyword(Keyword::Values) {
    Ok(SetExpr::Query(QueryBody::Values(self.values_stmt()?)))
} else {
    Ok(SetExpr::Query(QueryBody::Select(Box::new(self.select_core()?))))
}
```

Update `attach_paren_tail` so only a single `SELECT` or single `VALUES` query body can receive a parenthesized tail:

```rust
match inner {
    SetExpr::Query(QueryBody::Select(mut s)) => {
        let (order_by, limit, offset) = self.parse_set_tail()?;
        s.order_by = order_by;
        s.limit = limit;
        s.offset = offset;
        Ok(SetExpr::Query(QueryBody::Select(s)))
    }
    SetExpr::Query(QueryBody::Values(v)) => {
        let (_order_by, _limit, _offset) = self.parse_set_tail()?;
        Err(ParseError::new(
            "ORDER BY/LIMIT on a parenthesized VALUES branch is not supported",
            self.peek_pos(),
        ))
    }
    _ => Err(ParseError::new(
        "ORDER BY/LIMIT on a parenthesized set-operation subtree is not supported",
        self.peek_pos(),
    )),
}
```

This task deliberately rejects a parenthesized branch-local `VALUES` tail. The approved scope requires `ORDER BY` / `LIMIT` / `OFFSET` on standalone `VALUES` and on combined set-operation results, not on parenthesized values branches.

- [ ] **Step 7: Parse derived `VALUES` with optional column aliases.**

In `table_factor`, after consuming `(`, accept `SELECT` or `VALUES`:

```rust
if matches!(self.peek(), Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values)) {
    let subquery = if *self.peek() == Token::Keyword(Keyword::Select) {
        crate::ast::QueryBody::Select(Box::new(self.select_inner()?))
    } else {
        crate::ast::QueryBody::Values(self.values_stmt()?)
    };
    self.expect(&Token::RParen)?;
    let alias = self.opt_alias()?.ok_or_else(|| {
        ParseError::new("subquery in FROM must have an alias", self.peek_pos())
    })?;
    let columns = self.opt_column_aliases()?;
    return Ok(TableExpr::Derived { subquery, alias, columns });
}
```

Add the helper near `opt_alias`:

```rust
    fn opt_column_aliases(&mut self) -> Result<Option<Vec<String>>, ParseError> {
        if *self.peek() != Token::LParen {
            return Ok(None);
        }
        self.bump();
        let mut cols = vec![self.expect_ident()?];
        while self.eat_comma() {
            cols.push(self.expect_ident()?);
        }
        self.expect(&Token::RParen)?;
        Ok(Some(cols))
    }
```

- [ ] **Step 8: Run parser and oracle tests.**

Run:

```bash
cargo nextest run -p pgparser
```

Expected: PASS.

- [ ] **Step 9: Format and commit.**

Run:

```bash
cargo fmt
git add crates/pgparser/src/parser.rs crates/pgparser/tests/libpg_query_oracle.rs
git commit -m "SP39: parse VALUES query expressions"
```

---

## Task 3: Values Relation Builder And Describe Type Pass

**Files:**
- Create: `crates/executor/src/values.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/error.rs`
- Test: `crates/executor/src/values.rs`

- [ ] **Step 1: Register the module and add error variants.**

In `crates/executor/src/lib.rs`, add:

```rust
mod values;
```

In `crates/executor/src/error.rs`, add to `ExecError`:

```rust
    /// SP39: VALUES rows have different column counts (42601).
    ValuesColumnCount,
    /// SP39: a derived table column alias list has the wrong number of names (42601).
    DerivedColumnAliasCount { expected: usize, got: usize },
```

Add `into_pg` arms:

```rust
            ExecError::ValuesColumnCount => PgError::error(
                "42601",
                "VALUES lists must all be the same length",
            ),
            ExecError::DerivedColumnAliasCount { expected, got } => PgError::error(
                "42601",
                format!("table has {expected} columns available but {got} columns specified"),
            ),
```

- [ ] **Step 2: Create failing unit tests in `values.rs`.**

Create `crates/executor/src/values.rs` with this skeleton and tests:

```rust
use pgparser::ast::{Expr, ValuesStmt};
use pgtypes::{ColumnType, Datum};

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::scope::{ColumnBinding, Scope};

#[derive(Debug, Clone)]
pub(crate) struct ValuesSchema {
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<ColumnType>,
}

pub(crate) fn describe_values(v: &ValuesStmt) -> Result<ValuesSchema, ExecError> {
    analyze_values(v)
}

pub(crate) fn values_to_relation(v: &ValuesStmt, ctx: &EvalCtx) -> Result<crate::exec::Relation, ExecError> {
    let schema = analyze_values(v)?;
    let mut rows = Vec::with_capacity(v.rows.len());
    for row in &v.rows {
        let mut out = Vec::with_capacity(row.len());
        for (expr, ty) in row.iter().zip(&schema.types) {
            let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
            out.push(pgtypes::cast::cast(value, *ty)?);
        }
        rows.push(out);
    }
    Ok(crate::exec::Relation {
        scope: scope_from_schema(&schema, None),
        rows,
    })
}

fn scope_from_schema(schema: &ValuesSchema, qualifier: Option<&str>) -> Scope {
    Scope {
        columns: schema.names.iter().zip(&schema.types).map(|(name, ty)| ColumnBinding {
            qualifier: qualifier.map(str::to_string),
            name: name.clone(),
            ty: *ty,
        }).collect(),
    }
}

fn analyze_values(_v: &ValuesStmt) -> Result<ValuesSchema, ExecError> {
    Err(ExecError::TypeMismatch("forced failing stub before VALUES analysis is added".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(s: &str) -> Expr {
        Expr::IntLiteral(s.to_string())
    }

    fn str_lit(s: &str) -> Expr {
        Expr::StringLiteral(s.to_string())
    }

    #[test]
    fn default_names_and_types_are_resolved() {
        let v = ValuesStmt { rows: vec![vec![int("1"), str_lit("a")], vec![int("2"), str_lit("b")]] };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.names, vec!["column1", "column2"]);
        assert_eq!(schema.types, vec![ColumnType::Int4, ColumnType::Text]);
    }

    #[test]
    fn row_arity_mismatch_is_42601() {
        let v = ValuesStmt { rows: vec![vec![int("1")], vec![int("2"), int("3")]] };
        assert_eq!(describe_values(&v), Err(ExecError::ValuesColumnCount));
    }

    #[test]
    fn null_unknown_resolves_to_peer_type() {
        let v = ValuesStmt { rows: vec![vec![Expr::NullLiteral], vec![int("2")]] };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.types, vec![ColumnType::Int4]);
    }

    #[test]
    fn all_unknown_resolves_to_text() {
        let v = ValuesStmt { rows: vec![vec![Expr::NullLiteral], vec![str_lit("x")]] };
        let schema = describe_values(&v).expect("schema");
        assert_eq!(schema.types, vec![ColumnType::Text]);
    }

    #[test]
    fn evaluates_and_coerces_rows() {
        let v = ValuesStmt { rows: vec![vec![Expr::NullLiteral], vec![int("2")]] };
        let rel = values_to_relation(&v, &EvalCtx::test_default()).expect("relation");
        assert_eq!(rel.rows, vec![vec![Datum::Null], vec![Datum::Int4(2)]]);
        assert_eq!(rel.scope.columns[0].name, "column1");
    }
}
```

- [ ] **Step 3: Run the new unit tests and confirm they fail.**

Run:

```bash
cargo nextest run -p executor values::
```

Expected: FAIL because `analyze_values` returns `Unsupported`.

- [ ] **Step 4: Implement common-type analysis.**

Replace `analyze_values` and add helpers:

```rust
fn analyze_values(v: &ValuesStmt) -> Result<ValuesSchema, ExecError> {
    let width = v.rows.first().map(Vec::len).unwrap_or(0);
    let mut cols: Vec<(ColumnType, bool)> = vec![(ColumnType::Text, true); width];
    for row in &v.rows {
        if row.len() != width {
            return Err(ExecError::ValuesColumnCount);
        }
        for (idx, expr) in row.iter().enumerate() {
            let ty = infer_values_expr_type(expr)?;
            let unknown = is_unknown_literal(expr);
            cols[idx] = unify_values_col(cols[idx].0, cols[idx].1, ty, unknown)?;
        }
    }
    let types = cols
        .into_iter()
        .map(|(ty, unknown)| if unknown { ColumnType::Text } else { ty })
        .collect::<Vec<_>>();
    let names = (1..=width).map(|n| format!("column{n}")).collect();
    Ok(ValuesSchema { names, types })
}

fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::NullLiteral | Expr::StringLiteral(_))
}

fn infer_values_expr_type(e: &Expr) -> Result<ColumnType, ExecError> {
    crate::eval::infer_type(e, &Scope::empty())
}

fn unify_values_col(
    lt: ColumnType,
    lunk: bool,
    rt: ColumnType,
    runk: bool,
) -> Result<(ColumnType, bool), ExecError> {
    Ok(match (lunk, runk) {
        (true, true) => (lt, true),
        (true, false) => (rt, false),
        (false, true) => (lt, false),
        (false, false) => (crate::eval::unify_types(lt, rt)?, false),
    })
}
```

- [ ] **Step 5: Run tests and fix import/signature fallout.**

Run:

```bash
cargo nextest run -p executor values::
```

Expected: PASS.

- [ ] **Step 6: Format and commit.**

Run:

```bash
cargo fmt
git add crates/executor/src/lib.rs crates/executor/src/error.rs crates/executor/src/values.rs
git commit -m "SP39: add VALUES relation builder"
```

---

## Task 4: Standalone `VALUES` Execution, Describe, Ordering, And Wire Test

**Files:**
- Modify: `crates/executor/src/session.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/values.rs`
- Create: `crates/executor/tests/values_query.rs`
- Modify: `Cargo.toml` or `crates/executor/Cargo.toml` if explicit integration-test entries are required.

- [ ] **Step 1: Add failing wire tests.**

Create `crates/executor/tests/values_query.rs`:

```rust
use executor::{SqlEngine, SqlSession};
use pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) -> QueryResult {
    s.simple_query(sql).await.expect(sql)
}

#[tokio::test]
async fn standalone_values_orders_limits_and_names_columns() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let res = run(&mut s, "VALUES (2, 'b'), (1, 'a'), (3, 'c') ORDER BY 1 LIMIT 2 OFFSET 1").await;
    let QueryResult::Rows { fields, rows, .. } = res else { panic!("rows") };
    assert_eq!(fields[0].name, "column1");
    assert_eq!(fields[1].name, "column2");
    assert_eq!(rows, vec![
        vec![Cell::Int4(2), Cell::Text("b".into())],
        vec![Cell::Int4(3), Cell::Text("c".into())],
    ]);
}

#[tokio::test]
async fn describe_values_reports_fields_without_execution() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let fields = s.describe("VALUES (1, 'x')").await.expect("describe");
    assert_eq!(fields[0].name, "column1");
    assert_eq!(fields[1].name, "column2");
}

#[tokio::test]
async fn values_row_arity_error_is_42601_and_session_survives() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let err = s.simple_query("VALUES (1), (2, 3)").await.expect_err("arity");
    assert_eq!(err.sqlstate, "42601");
    let ok = run(&mut s, "VALUES (1)").await;
    assert!(matches!(ok, QueryResult::Rows { .. }));
}
```

- [ ] **Step 2: Run wire tests and confirm they fail.**

Run:

```bash
cargo nextest run -p executor --test values_query
```

Expected: FAIL because `Statement::Values` is not dispatched or described.

- [ ] **Step 3: Add ordering/result helper in `values.rs`.**

Add:

```rust
pub(crate) fn describe_values_query(
    q: &pgparser::ast::ValuesQuery,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let schema = describe_values(&q.body)?;
    Ok(schema.names.iter().zip(&schema.types).map(|(name, ty)| crate::exec::field(name, *ty)).collect())
}

pub(crate) fn execute_values_query(
    q: &pgparser::ast::ValuesQuery,
    ctx: &EvalCtx,
) -> Result<pgwire::engine::QueryResult, ExecError> {
    let mut rel = values_to_relation(&q.body, ctx)?;
    apply_query_order(&mut rel, &q.order_by, q.offset, q.limit, ctx)?;
    let fields = rel.scope.columns.iter().map(|c| crate::exec::field(&c.name, c.ty)).collect();
    Ok(crate::exec::rows_result(fields, &rel.rows, &ctx.time_zone))
}

pub(crate) fn apply_query_order(
    rel: &mut crate::exec::Relation,
    order_by: &[pgparser::ast::OrderItem],
    offset: Option<i64>,
    limit: Option<i64>,
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    if !order_by.is_empty() {
        let mut keyed = Vec::with_capacity(rel.rows.len());
        for row in rel.rows.drain(..) {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(output_order_key(&item.expr, &rel.scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, order_by));
        rel.rows = keyed.into_iter().map(|(_, row)| row).collect();
    }
    crate::exec::apply_offset_limit(&mut rel.rows, offset, limit);
    Ok(())
}

fn output_order_key(
    expr: &Expr,
    scope: &Scope,
    row: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if let Expr::IntLiteral(s) = expr {
        if let Ok(n) = s.parse::<usize>() {
            if (1..=row.len()).contains(&n) {
                return Ok(row[n - 1].clone());
            }
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {n} is not in select list"
            )));
        }
    }
    crate::eval::eval(expr, scope, row, ctx)
}
```

- [ ] **Step 4: Dispatch standalone values in `session.rs`.**

Add a match arm near `Statement::Select(_)`:

```rust
            Statement::Values(_) => self.run_values(stmt).await,
```

Add method near `run_set_operation`:

```rust
    async fn run_values(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let Statement::Values(q) = stmt else {
            unreachable!("run_one only routes a Values here");
        };
        let (_snapshot, _own, _gsnap) = self.read_context().await?;
        let ctx = self.eval_ctx();
        crate::values::execute_values_query(q, &ctx)
    }
```

- [ ] **Step 5: Describe values in `exec.rs`.**

In `describe`, add before the `Select` arm:

```rust
    if let Some(Statement::Values(q)) = stmt {
        return crate::values::describe_values_query(q);
    }
```

- [ ] **Step 6: Run tests.**

Run:

```bash
cargo nextest run -p executor --test values_query
cargo nextest run -p executor values::
```

Expected: PASS.

- [ ] **Step 7: Format and commit.**

Run:

```bash
cargo fmt
git add crates/executor/src/session.rs crates/executor/src/exec.rs crates/executor/src/values.rs crates/executor/tests/values_query.rs Cargo.toml crates/executor/Cargo.toml
git commit -m "SP39: execute standalone VALUES queries"
```

---

## Task 5: Derived Table `VALUES` And Column Alias Semantics

**Files:**
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/values.rs`
- Test: `crates/executor/tests/values_query.rs`

- [ ] **Step 1: Add failing wire tests.**

Append to `crates/executor/tests/values_query.rs`:

```rust
#[tokio::test]
async fn values_derived_table_uses_alias_column_names() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let res = run(
        &mut s,
        "SELECT id, name FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, name) ORDER BY id",
    ).await;
    let QueryResult::Rows { fields, rows, .. } = res else { panic!("rows") };
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[1].name, "name");
    assert_eq!(rows, vec![
        vec![Cell::Int4(1), Cell::Text("a".into())],
        vec![Cell::Int4(2), Cell::Text("b".into())],
    ]);
}

#[tokio::test]
async fn values_derived_column_alias_count_error_is_42601() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let err = s
        .simple_query("SELECT * FROM (VALUES (1, 2)) AS v(one)")
        .await
        .expect_err("alias count");
    assert_eq!(err.sqlstate, "42601");
}

#[tokio::test]
async fn values_derived_table_is_not_correlated() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1)").await;
    let err = s
        .simple_query("SELECT * FROM t, (VALUES (t.id)) AS v(x)")
        .await
        .expect_err("correlation");
    assert_eq!(err.sqlstate, "42P01");
}
```

- [ ] **Step 2: Run tests and confirm failure.**

Run:

```bash
cargo nextest run -p executor --test values_query values_derived
```

Expected: FAIL because `TableExpr::Derived` is still select-only in executor paths.

- [ ] **Step 3: Add relation requalification helper in `values.rs`.**

Add:

```rust
pub(crate) fn requalify_derived(
    mut rel: crate::exec::Relation,
    alias: &str,
    columns: &Option<Vec<String>>,
) -> Result<crate::exec::Relation, ExecError> {
    if let Some(names) = columns {
        if names.len() != rel.scope.columns.len() {
            return Err(ExecError::DerivedColumnAliasCount {
                expected: rel.scope.columns.len(),
                got: names.len(),
            });
        }
        for (binding, name) in rel.scope.columns.iter_mut().zip(names) {
            binding.name = name.clone();
        }
    }
    for binding in &mut rel.scope.columns {
        binding.qualifier = Some(alias.to_string());
    }
    Ok(rel)
}
```

- [ ] **Step 4: Generalize derived table execution in `exec.rs`.**

In `build_table_expr`, replace the `TableExpr::Derived` arm with:

```rust
        TableExpr::Derived { subquery, alias, columns } => {
            let inner = match subquery {
                pgparser::ast::QueryBody::Select(s) => {
                    select_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, s, ctx)?
                }
                pgparser::ast::QueryBody::Values(v) => crate::values::values_to_relation(v, ctx)?,
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
```

In `build_table_expr_schema`, replace the derived arm with:

```rust
        TableExpr::Derived { subquery, alias, columns } => {
            let inner = match subquery {
                pgparser::ast::QueryBody::Select(s) => {
                    let inner_scope = if s.from.is_empty() {
                        Scope::empty()
                    } else {
                        build_from_schema(catalog_kv, &s.from)?.scope
                    };
                    let projection = crate::subquery::resolve_types_in_projection(catalog_kv, &s.projection)?;
                    let (fields, _exprs, tys) = resolve_projection(&projection, &inner_scope)?;
                    crate::exec::Relation {
                        scope: Scope {
                            columns: fields.iter().zip(&tys).map(|(f, ty)| ColumnBinding {
                                qualifier: None,
                                name: f.name.clone(),
                                ty: *ty,
                            }).collect(),
                        },
                        rows: Vec::new(),
                    }
                }
                pgparser::ast::QueryBody::Values(v) => {
                    let schema = crate::values::describe_values(v)?;
                    crate::exec::Relation {
                        scope: Scope {
                            columns: schema.names.iter().zip(&schema.types).map(|(name, ty)| ColumnBinding {
                                qualifier: None,
                                name: name.clone(),
                                ty: *ty,
                            }).collect(),
                        },
                        rows: Vec::new(),
                    }
                }
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
```

- [ ] **Step 5: Run derived tests.**

Run:

```bash
cargo nextest run -p executor --test values_query values_derived
```

Expected: PASS.

- [ ] **Step 6: Format and commit.**

Run:

```bash
cargo fmt
git add crates/executor/src/exec.rs crates/executor/src/values.rs crates/executor/tests/values_query.rs
git commit -m "SP39: support VALUES derived tables"
```

---

## Task 6: Set Operations Over `VALUES`

**Files:**
- Modify: `crates/executor/src/setops.rs`
- Modify: `crates/executor/tests/values_query.rs`

- [ ] **Step 1: Add failing set-op wire tests.**

Append to `crates/executor/tests/values_query.rs`:

```rust
#[tokio::test]
async fn values_can_participate_in_set_operations() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let res = run(&mut s, "VALUES (1), (2) UNION SELECT 2 ORDER BY 1").await;
    let QueryResult::Rows { rows, .. } = res else { panic!("rows") };
    assert_eq!(rows, vec![vec![Cell::Int4(1)], vec![Cell::Int4(2)]]);
}

#[tokio::test]
async fn values_set_ops_share_unknown_resolution() {
    let e = SqlEngine::open_mem().expect("engine");
    let mut s = e.connect().await.expect("session");
    let res = run(&mut s, "VALUES (NULL), ('5') UNION SELECT 2 ORDER BY 1").await;
    let QueryResult::Rows { rows, .. } = res else { panic!("rows") };
    assert_eq!(rows, vec![vec![Cell::Int4(2)], vec![Cell::Int4(5)], vec![Cell::Null]]);
}
```

- [ ] **Step 2: Run tests and confirm failure.**

Run:

```bash
cargo nextest run -p executor --test values_query values_can_participate values_set_ops
```

Expected: FAIL because `setops` only accepts `SetExpr::Select`.

- [ ] **Step 3: Add query-body column resolution in `setops.rs`.**

Replace the `SetExpr::Select(s)` arm inside `resolve_set_columns` with:

```rust
        SetExpr::Query(body) => match body {
            pgparser::ast::QueryBody::Select(s) => {
                let scope = if s.from.is_empty() {
                    Scope::empty()
                } else {
                    crate::exec::build_from_schema(catalog_kv, &s.from)?.scope
                };
                let projection =
                    crate::subquery::resolve_types_in_projection(catalog_kv, &s.projection)?;
                let (fields, exprs, tys) = crate::exec::resolve_projection(&projection, &scope)?;
                Ok(fields
                    .into_iter()
                    .zip(tys)
                    .zip(exprs)
                    .map(|((f, ty), e)| ResolvedCol {
                        name: f.name,
                        ty,
                        unknown: is_unknown_literal(&e),
                    })
                    .collect())
            }
            pgparser::ast::QueryBody::Values(v) => {
                let schema = crate::values::describe_values(v)?;
                Ok(schema
                    .names
                    .into_iter()
                    .zip(schema.types)
                    .map(|(name, ty)| ResolvedCol {
                        name,
                        ty,
                        unknown: false,
                    })
                    .collect())
            }
        },
```

- [ ] **Step 4: Add query-body leaf execution in `fold`.**

Replace the `SetExpr::Select(s)` arm in `fold` with:

```rust
        SetExpr::Query(body) => {
            let rel = match body {
                pgparser::ast::QueryBody::Select(s) => crate::exec::select_to_relation(
                    catalog_kv, kv, global, gsnap, snapshot, own, s, ctx,
                )?,
                pgparser::ast::QueryBody::Values(v) => crate::values::values_to_relation(v, ctx)?,
            };
            coerce_rows(rel.rows, &rel.scope, out_tys)
        }
```

Keep the existing `SetExpr::SetOp` combine arm unchanged.

- [ ] **Step 5: Run set-op tests.**

Run:

```bash
cargo nextest run -p executor --test values_query values_can_participate values_set_ops
cargo nextest run -p executor setops::
```

Expected: PASS.

- [ ] **Step 6: Format and commit.**

Run:

```bash
cargo fmt
git add crates/executor/src/setops.rs crates/executor/tests/values_query.rs
git commit -m "SP39: allow VALUES in set operations"
```

---

## Task 7: Router Range Collection For Query Bodies

**Files:**
- Modify: `crates/cluster/src/range/router.rs`
- Test: `crates/cluster/src/range/router.rs`

- [ ] **Step 1: Add failing router tests.**

In `crates/cluster/src/range/router.rs` tests, add:

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn values_queries_are_range_neutral_and_set_ops_still_check_select_ranges() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router.simple("VALUES (1), (2)").await.expect("standalone values");
        router.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        router.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        router.simple("INSERT INTO a VALUES (1)").await.expect("insert a");

        assert_eq!(
            router
                .scan_one_i32("VALUES (2) UNION SELECT id FROM a ORDER BY 1")
                .await,
            vec![1, 2]
        );

        let err = router
            .simple("SELECT id FROM a UNION SELECT id FROM b")
            .await
            .expect_err("cross-range set op rejected");
        assert_eq!(err.code, "0A000", "got {err:?}");
        assert!(
            err.message.contains("set operations spanning ranges"),
            "got {err:?}"
        );
    }
```

- [ ] **Step 2: Run the focused router test and confirm failure.**

Run:

```bash
cargo nextest run -p cluster values_queries_are_range_neutral
```

Expected: FAIL because router range collection still assumes select-only set-operation leaves and select-only derived tables.

- [ ] **Step 3: Add query-body range collection.**

In `crates/cluster/src/range/router.rs`, add helper:

```rust
fn collect_query_body_ranges(
    body: &pgparser::ast::QueryBody,
    router: &RangeRouter,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    match body {
        pgparser::ast::QueryBody::Select(s) => collect_select_ranges(router, s, out),
        pgparser::ast::QueryBody::Values(_) => Ok(()),
    }
}
```

- [ ] **Step 4: Update set-op and derived-table range walkers.**

In `collect_set_expr_ranges`, replace select-only leaf handling with:

```rust
SetExpr::Query(body) => collect_query_body_ranges(body, router, out),
```

In the table-expression walker, replace `TableExpr::Derived { subquery, .. }` handling with:

```rust
TableExpr::Derived { subquery, .. } => collect_query_body_ranges(subquery, router, out),
```

- [ ] **Step 5: Run router tests.**

Run:

```bash
cargo nextest run -p cluster values_queries_are_range_neutral
cargo nextest run -p cluster router
```

Expected: PASS.

- [ ] **Step 6: Format and commit.**

Run:

```bash
cargo fmt
git add crates/cluster/src/range/router.rs
git commit -m "SP39: treat VALUES as range-neutral"
```

---

## Task 8: Conformance Corpus And Final Documentation

**Files:**
- Create: `crates/conformance/corpus/values.sql`
- Modify: `CLAUDE.md`
- Test: conformance command

- [ ] **Step 1: Add the conformance corpus.**

Create `crates/conformance/corpus/values.sql`:

```sql
-- SP39: VALUES query expressions and derived row sources.

VALUES (1, 'a'), (2, 'b') ORDER BY 1;
VALUES (2), (1), (3) ORDER BY 1 LIMIT 2 OFFSET 1;
VALUES (NULL), (2) ORDER BY 1;
VALUES ('5'), (2) ORDER BY 1;

SELECT id, name
FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, name)
ORDER BY id;

VALUES (1), (2)
UNION
SELECT 2
ORDER BY 1;

VALUES (1), (1), (2)
UNION ALL
VALUES (2), (3)
ORDER BY 1;
```

- [ ] **Step 2: Run conformance locally if the PostgreSQL oracle is available.**

Run:

```bash
cargo run -p conformance -- --corpus crates/conformance/corpus/values.sql --out /tmp/crabgresql-values-parity.json
```

Expected: PASS with every statement matching PostgreSQL. If the local oracle is not running, start it using the repository's existing script:

```bash
scripts/oracle-up.sh
cargo run -p conformance -- --corpus crates/conformance/corpus/values.sql --out /tmp/crabgresql-values-parity.json
```

- [ ] **Step 3: Run full targeted test set.**

Run:

```bash
cargo nextest run -p pgparser
cargo nextest run -p executor --test values_query
cargo nextest run -p executor setops:: values::
cargo nextest run -p cluster values_queries_are_range_neutral
cargo nextest run -p conformance
cargo clippy -p pgparser -p executor -p cluster -p conformance --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Add CLAUDE.md implementation note.**

Append a concise SP39 note near the existing SQL breadth-wave entries:

```markdown
**SP39 (2026-06-17):** SQL breadth wave 11 — **VALUES query expressions**. Adds standalone `VALUES`, `VALUES` branches in set operations, and non-correlated `FROM (VALUES ...) AS alias(cols...)` derived tables. One new executor integration-test binary — `values_query` — is UAC-safe. Proven by parser tests, libpg_query accepted forms, `executor::values` unit tests, wire tests, router range-neutral tests, and `conformance/corpus/values.sql`. No Stateright model: pure query expression evaluation under one statement context, no lock/write/MVCC/leadership interleaving.
```

- [ ] **Step 5: Format and commit.**

Run:

```bash
cargo fmt
git add crates/conformance/corpus/values.sql CLAUDE.md
git commit -m "SP39: add VALUES conformance corpus"
```

---

## Task 9: Final Verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run the no-forbidden-target guard.**

Run:

```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```

Expected: no output. Exit code may be `1` because grep found no matches.

- [ ] **Step 2: Run workspace verification.**

Run:

```bash
cargo fmt --check
cargo nextest run --workspace
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 3: Check git state.**

Run:

```bash
git status --short
```

Expected: clean worktree.

---

## Self-Review Notes

- Spec coverage: standalone `VALUES`, result ordering/limit/offset, set-op branches, derived-table row source, default names, common-type and unknown-literal resolution, describe, router range-neutral behavior, conformance, and no-Stateright rationale are all covered by tasks.
- Placeholder scan: no placeholder markers or unassigned edge-case work remains. The intentionally failing Task 3 stub returns a concrete `TypeMismatch` error and is replaced in the next step.
- Type consistency: `ValuesStmt`, `ValuesQuery`, `QueryBody`, `SetExpr::Query`, `values_to_relation`, `describe_values`, and `requalify_derived` are introduced before use in later tasks.
