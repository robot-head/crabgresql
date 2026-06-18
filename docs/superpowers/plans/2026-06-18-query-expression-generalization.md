# Query Expression Generalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize row-producing SQL so top-level queries, derived tables, and uncorrelated expression subqueries all use one `QueryExpr` AST and executor relation contract.

**Architecture:** Replace the three top-level read statement shapes with `Statement::Query(QueryExpr)`. Parse, describe, route, and execute every row-producing SQL expression through that shape while preserving the existing `SelectStmt`, `ValuesStmt`, and `SetExpr` internals. Add a focused `executor::query` module that materializes any `QueryExpr` to a `Relation`; keep SELECT, VALUES, and set-op logic in their current modules as implementation helpers.

**Tech Stack:** Rust 2024, pgparser recursive-descent parser, executor MVCC/read-snapshot pipeline, pgwire simple/extended protocol tests, cluster `RangeRouter`, PostgreSQL 18 conformance corpus.

---

## File Structure

- Modify `crates/pgparser/src/ast.rs`: add `QueryExpr`; replace `Statement::{Select,Values,SetOperation}` with `Statement::Query`; make nested subquery and derived-table nodes hold `QueryExpr`.
- Modify `crates/pgparser/src/parser.rs`: parse all row-producing contexts with `query_expr`; update parser tests to inspect the unified query shape.
- Create `crates/executor/src/query.rs`: one `query_to_relation` and `describe_query_expr` contract, plus tail application helpers for query-expression outputs.
- Modify `crates/executor/src/lib.rs`: register the new `query` module.
- Modify `crates/executor/src/session.rs`: replace `run_select` / `run_values` / `run_set_operation` dispatch with `run_query`; preserve locking SELECT dispatch.
- Modify `crates/executor/src/exec.rs`: move top-level query execution to `executor::query`, keep SELECT relation helpers available for the new module, and describe row-producing statements through `describe_query_expr`.
- Modify `crates/executor/src/setops.rs`: expose schema and row helpers that operate on `SetExpr` under a caller-supplied `QueryExpr` tail.
- Modify `crates/executor/src/subquery.rs`: resolve nested `QueryExpr` subqueries through `query_to_relation` and type them through `describe_query_expr`.
- Modify `crates/executor/src/agg.rs` and `crates/executor/src/eval.rs`: update subquery AST type references only; behavior remains the same.
- Create `crates/executor/tests/query_expressions.rs`: end-to-end wire tests for nested set-op / VALUES query expressions and extended Describe.
- Modify `crates/cluster/src/range/router.rs`: route `Statement::Query` by walking one `QueryExpr` tree, including derived query expressions and expression subqueries.
- Add `crates/conformance/corpus/nested_query_expressions.sql`: PostgreSQL-oracle corpus for nested query-expression parity.

---

### Task 1: Introduce `QueryExpr` In The Parser AST

**Files:**
- Modify: `crates/pgparser/src/ast.rs`
- Modify: `crates/pgparser/src/parser.rs`

- [ ] **Step 1: Write failing parser AST tests**

Append these tests inside `mod tests` in `crates/pgparser/src/parser.rs`:

```rust
    fn only_query(sql: &str) -> crate::ast::QueryExpr {
        let statements = crate::parse(sql).expect("parse ok");
        assert_eq!(statements.len(), 1);
        match statements.into_iter().next().expect("one statement") {
            Statement::Query(q) => q,
            other => panic!("expected Statement::Query, got {other:?}"),
        }
    }

    #[test]
    fn row_producing_statements_share_query_expr_shape() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("SELECT 1 ORDER BY 1 LIMIT 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Select(_))));

        let q = only_query("VALUES (1), (2) ORDER BY 1 OFFSET 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.offset, Some(1));
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("SELECT 1 UNION ALL VALUES (2) ORDER BY 1");
        assert_eq!(q.order_by.len(), 1);
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
    }

    #[test]
    fn derived_and_expression_subqueries_accept_query_exprs() {
        use crate::ast::{Expr, QueryBody, SelectItem, SetExpr, TableExpr};

        let outer = only_query(
            "SELECT t.x FROM (SELECT 1 AS x UNION SELECT 2) AS t ORDER BY t.x",
        );
        let SetExpr::Query(QueryBody::Select(select)) = outer.body else {
            panic!("expected outer SELECT query body");
        };
        let [TableExpr::Derived { subquery, alias, .. }] = select.from.as_slice() else {
            panic!("expected one derived table");
        };
        assert_eq!(alias, "t");
        assert!(matches!(subquery.body, SetExpr::SetOp { .. }));

        let scalar = only_query("SELECT (VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1)");
        let SetExpr::Query(QueryBody::Select(select)) = scalar.body else {
            panic!("expected SELECT");
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!("expected expression projection");
        };
        let Expr::ScalarSubquery(q) = expr else {
            panic!("expected scalar query expression");
        };
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
        assert_eq!(q.limit, Some(1));
    }

    #[test]
    fn parenthesized_query_expr_tail_is_preserved_for_values_and_setops() {
        use crate::ast::{QueryBody, SetExpr, TableExpr};

        let q = only_query("SELECT v.x FROM (VALUES (2), (1) ORDER BY 1 LIMIT 1) AS v(x)");
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT");
        };
        let [TableExpr::Derived { subquery, .. }] = select.from.as_slice() else {
            panic!("expected one derived table");
        };
        assert!(matches!(subquery.body, SetExpr::Query(QueryBody::Values(_))));
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(1));

        let q = only_query(
            "SELECT s.x FROM (SELECT 2 AS x UNION SELECT 1 ORDER BY 1 LIMIT 1) AS s",
        );
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT");
        };
        let [TableExpr::Derived { subquery, .. }] = select.from.as_slice() else {
            panic!("expected one derived table");
        };
        assert!(matches!(subquery.body, SetExpr::SetOp { .. }));
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(1));
    }
```

- [ ] **Step 2: Run parser tests to verify RED**

Run:

```bash
cargo test -p pgparser row_producing_statements_share_query_expr_shape derived_and_expression_subqueries_accept_query_exprs parenthesized_query_expr_tail_is_preserved_for_values_and_setops
```

Expected: compile failure mentioning `Statement::Query` and `QueryExpr` do not exist.

- [ ] **Step 3: Update AST types**

In `crates/pgparser/src/ast.rs`, replace the three row-producing statement variants with one `Query` variant:

```rust
    Query(QueryExpr),
```

Delete the old `Statement::Select`, `Statement::Values`, and `Statement::SetOperation` variants. Delete the now-obsolete `ValuesQuery` and `SetQuery` structs.

Add this struct after `SelectStmt`:

```rust
/// A complete row-producing SQL query expression. The body may be a lone SELECT,
/// a lone VALUES list, or a set-operation tree. The tail applies to the complete
/// query expression.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExpr {
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub locking: Option<RowLockStrength>,
}
```

Change nested nodes to carry `QueryExpr`:

```rust
    ScalarSubquery(Box<QueryExpr>),
    Exists(Box<QueryExpr>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryExpr>,
        negated: bool,
    },
    Quantified {
        expr: Box<Expr>,
        op: BinaryOp,
        all: bool,
        subquery: Box<QueryExpr>,
    },
```

Change derived tables:

```rust
    Derived {
        subquery: QueryExpr,
        alias: String,
        columns: Option<Vec<String>>,
    },
```

- [ ] **Step 4: Implement parser `query_expr`**

In `crates/pgparser/src/parser.rs`, change statement dispatch to call `query_statement`:

```rust
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values) | Token::LParen => {
                self.query_statement()
            }
```

Replace `query_stmt` with:

```rust
    fn query_statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        Ok(crate::ast::Statement::Query(self.query_expr()?))
    }

    fn query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        use crate::ast::{QueryExpr, QueryBody, SetExpr};
        let body = self.set_expr(0)?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        let locking = self.parse_locking()?;
        if locking.is_some() {
            match &body {
                SetExpr::Query(QueryBody::Select(_)) => {}
                SetExpr::Query(QueryBody::Values(_)) => {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with VALUES",
                        self.peek_pos(),
                    ));
                }
                SetExpr::SetOp { .. } => {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(QueryExpr {
            body,
            order_by,
            limit,
            offset,
            locking,
        })
    }
```

Change `set_primary` so a parenthesized query expression returns a `SetExpr` with its tail preserved by wrapping the inner body in a `QueryBody::Select` only when necessary is not enough. Instead add a helper that parses a parenthesized query expression body plus tail into a `QueryExpr`, then converts it to a `SetExpr` only when it has no tail:

```rust
    fn parenthesized_query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        use crate::ast::QueryExpr;
        self.expect(&Token::LParen)?;
        let body = self.set_expr(0)?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        self.expect(&Token::RParen)?;
        Ok(QueryExpr {
            body,
            order_by,
            limit,
            offset,
            locking: None,
        })
    }
```

Then update `set_primary` to keep the existing set-operation leaf behavior for branch parsing:

```rust
    fn set_primary(&mut self) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
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
    }
```

Then replace `attach_paren_tail` so all bodies accept tails by returning a `SetExpr::Query(QueryBody::Select(...))` only for a lone SELECT. For a tailed `VALUES` or multi-branch set-op, leave the tail for nested `QueryExpr` contexts and forbid it only while parsing a branch inside a larger set-op:

```rust
    fn attach_paren_tail(
        &mut self,
        inner: crate::ast::SetExpr,
    ) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        let has_tail = matches!(
            self.peek(),
            Token::Keyword(Keyword::Order)
                | Token::Keyword(Keyword::Limit)
                | Token::Keyword(Keyword::Offset)
        );
        if !has_tail {
            return Ok(inner);
        }
        match inner {
            SetExpr::Query(QueryBody::Select(mut s)) => {
                let (order_by, limit, offset) = self.parse_set_tail()?;
                s.order_by = order_by;
                s.limit = limit;
                s.offset = offset;
                Ok(SetExpr::Query(QueryBody::Select(s)))
            }
            _ => Err(ParseError::new(
                "ORDER BY/LIMIT/OFFSET on this parenthesized query must be parsed as a nested query expression",
                self.peek_pos(),
            )),
        }
    }
```

This preserves existing branch parsing. Nested tailed `VALUES` and set-op forms are enabled in Step 5 through table/expression contexts that call `query_expr_in_parens`, not through set-op branch parsing.

- [ ] **Step 5: Parse nested contexts as `QueryExpr`**

Add this helper near `table_factor`:

```rust
    fn query_expr_in_parens(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        use crate::ast::{QueryExpr, QueryBody, SetExpr};
        self.expect(&Token::LParen)?;
        let body = self.set_expr(0)?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        self.expect(&Token::RParen)?;
        Ok(QueryExpr {
            body,
            order_by,
            limit,
            offset,
            locking: None,
        })
    }

    fn starts_query_expr(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values) | Token::LParen
        )
    }
```

Update scalar subquery parsing in `prefix`:

```rust
            Token::LParen => {
                if matches!(
                    self.peek2(),
                    Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values)
                ) {
                    let sub = self.query_expr_in_parens()?;
                    Ok(Expr::ScalarSubquery(Box::new(sub)))
                } else {
                    self.bump();
                    let e = self.expr(0)?;
                    self.expect(&Token::RParen)?;
                    Ok(e)
                }
            }
```

Update `EXISTS`:

```rust
            Token::Keyword(Keyword::Exists) => {
                self.bump();
                let sub = self.query_expr_in_parens()?;
                Ok(Expr::Exists(Box::new(sub)))
            }
```

Update `parse_in`:

```rust
        if matches!(
            self.peek(),
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values)
        ) {
            let body = self.set_expr(0)?;
            let (order_by, limit, offset) = self.parse_set_tail()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: Box::new(crate::ast::QueryExpr {
                    body,
                    order_by,
                    limit,
                    offset,
                    locking: None,
                }),
                negated,
            });
        }
```

Update quantified subquery parsing where `Expr::Quantified` is built:

```rust
                    let subquery = Box::new(crate::ast::QueryExpr {
                        body: self.set_expr(0)?,
                        order_by: {
                            let (order_by, limit, offset) = self.parse_set_tail()?;
                            if limit.is_some() || offset.is_some() {
                                return Err(ParseError::new(
                                    "LIMIT/OFFSET is not allowed in quantified subquery here",
                                    self.peek_pos(),
                                ));
                            }
                            order_by
                        },
                        limit: None,
                        offset: None,
                        locking: None,
                    });
```

If the current quantified parser already consumes the surrounding parentheses before `select_inner`, keep that structure and replace only the inner `select_inner()` call with a `QueryExpr` build.

Update `table_factor` derived parsing:

```rust
            if matches!(
                self.peek(),
                Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values)
            ) {
                let body = self.set_expr(0)?;
                let (order_by, limit, offset) = self.parse_set_tail()?;
                self.expect(&Token::RParen)?;
                let alias = self.opt_alias()?.ok_or_else(|| {
                    ParseError::new("subquery in FROM must have an alias", self.peek_pos())
                })?;
                let columns = self.opt_column_aliases()?;
                return Ok(TableExpr::Derived {
                    subquery: crate::ast::QueryExpr {
                        body,
                        order_by,
                        limit,
                        offset,
                        locking: None,
                    },
                    alias,
                    columns,
                });
            }
```

- [ ] **Step 6: Run parser tests to verify GREEN for new tests**

Run:

```bash
cargo test -p pgparser row_producing_statements_share_query_expr_shape derived_and_expression_subqueries_accept_query_exprs parenthesized_query_expr_tail_is_preserved_for_values_and_setops
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/pgparser/src/ast.rs crates/pgparser/src/parser.rs
git commit -m "[codex] Unify query expression AST"
```

---

### Task 2: Migrate Parser Regression Tests And Oracle Cases

**Files:**
- Modify: `crates/pgparser/src/parser.rs`
- Modify: `crates/pgparser/tests/libpg_query_oracle.rs`

- [ ] **Step 1: Write failing compatibility tests for old syntax**

Add parser tests that assert old top-level syntax still parses after the shape change:

```rust
    #[test]
    fn legacy_query_forms_still_parse_after_query_unification() {
        for sql in [
            "SELECT a, b FROM t WHERE a > 1 ORDER BY b LIMIT 5",
            "VALUES (1), (2) ORDER BY 1",
            "(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2 ORDER BY 1",
            "SELECT * FROM (SELECT 1 AS x) AS d",
            "SELECT * FROM (VALUES (1, 'a')) AS v(id, name)",
        ] {
            let q = only_query(sql);
            assert!(q.locking.is_none());
        }
    }
```

- [ ] **Step 2: Run the parser crate to verify RED**

Run:

```bash
cargo test -p pgparser
```

Expected: FAIL with existing parser tests still matching `Statement::Select`, `Statement::Values`, or `Statement::SetOperation`.

- [ ] **Step 3: Update parser tests to unwrap `Statement::Query`**

In `crates/pgparser/src/parser.rs`, add these test helpers inside `mod tests` if Task 1 did not already add them:

```rust
    fn query(sql: &str) -> crate::ast::QueryExpr {
        let statements = crate::parse(sql).expect("parse ok");
        assert_eq!(statements.len(), 1);
        match statements.into_iter().next().expect("one statement") {
            Statement::Query(q) => q,
            other => panic!("expected query statement, got {other:?}"),
        }
    }

    fn lone_select(sql: &str) -> crate::ast::SelectStmt {
        use crate::ast::{QueryBody, SetExpr};
        let q = query(sql);
        match q.body {
            SetExpr::Query(QueryBody::Select(s)) => *s,
            other => panic!("expected lone select, got {other:?}"),
        }
    }
```

Mechanically replace test patterns:

```rust
Statement::Select(s) => { ... }
```

with:

```rust
let s = lone_select(sql);
```

Replace `Statement::Values(q)` assertions with:

```rust
let q = query(sql);
assert!(matches!(q.body, SetExpr::Query(QueryBody::Values(_))));
```

Replace `Statement::SetOperation(q)` assertions with:

```rust
let q = query(sql);
assert!(matches!(q.body, SetExpr::SetOp { .. }));
```

Update oracle accepted forms in `crates/pgparser/tests/libpg_query_oracle.rs` to include:

```rust
    "SELECT x FROM (SELECT 1 AS x UNION SELECT 2) AS s ORDER BY x",
    "SELECT x FROM (VALUES (2), (1) ORDER BY 1 LIMIT 1) AS v(x)",
    "SELECT (VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1)",
    "SELECT 2 IN (VALUES (1), (2))",
    "SELECT EXISTS (SELECT 1 EXCEPT SELECT 2)",
```

- [ ] **Step 4: Run parser tests and oracle**

Run:

```bash
cargo test -p pgparser
cargo test --locked -p pgparser --features oracle
```

Expected: PASS. If the oracle feature cannot build locally because libpg_query tooling is unavailable, record the toolchain error in the task notes and continue with the non-oracle parser suite green.

- [ ] **Step 5: Commit**

```bash
git add crates/pgparser/src/parser.rs crates/pgparser/tests/libpg_query_oracle.rs
git commit -m "[codex] Migrate parser tests to QueryExpr"
```

---

### Task 3: Add The Executor Query Relation Contract

**Files:**
- Create: `crates/executor/src/query.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/setops.rs`
- Modify: `crates/executor/src/values.rs`

- [ ] **Step 1: Write failing executor unit tests for `query_to_relation`**

Create `crates/executor/src/query.rs` with tests first:

```rust
use kv::Kv;
use mvcc::visibility::Snapshot;
use pgparser::ast::{QueryBody, QueryExpr, SetExpr};
use pgtypes::{ColumnType, Datum};

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;
use crate::scope::{ColumnBinding, Scope};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlEngine;
    use pgwire::engine::{Engine, QueryResult, Session};

    async fn run(sql: &str) -> QueryResult {
        SqlEngine::new()
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn cells(result: QueryResult) -> Vec<Vec<Option<String>>> {
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8")))
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn top_level_select_values_and_setops_use_query_pipeline() {
        assert_eq!(cells(run("SELECT 1").await), vec![vec![Some("1".into())]]);
        assert_eq!(
            cells(run("VALUES (2), (1) ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            cells(run("SELECT 1 UNION SELECT 2 ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
    }
}
```

- [ ] **Step 2: Run the test to verify RED**

Run:

```bash
cargo test -p executor query::tests::top_level_select_values_and_setops_use_query_pipeline
```

Expected: compile failure because `mod query` is not registered and executor still matches old statement variants.

- [ ] **Step 3: Register `executor::query`**

In `crates/executor/src/lib.rs`, add:

```rust
mod query;
```

- [ ] **Step 4: Expose relation-producing helpers**

In `crates/executor/src/exec.rs`, make these helpers visible to `query.rs` if they are not already:

```rust
pub(crate) fn select_to_relation(/* existing signature unchanged */) -> Result<Relation, ExecError>
pub(crate) fn build_from_schema(/* existing signature unchanged */) -> Result<Relation, ExecError>
pub(crate) fn resolve_projection(/* existing signature unchanged */)
pub(crate) fn field(name: &str, ty: ColumnType) -> FieldDescription
pub(crate) fn rows_result(/* existing signature unchanged */) -> QueryResult
pub(crate) fn order_cmp(/* existing signature unchanged */) -> std::cmp::Ordering
pub(crate) fn apply_offset_limit(/* existing signature unchanged */)
```

In `crates/executor/src/values.rs`, keep these functions `pub(crate)`:

```rust
pub(crate) fn describe_values(v: &ValuesStmt) -> Result<ValuesSchema, ExecError>
pub(crate) fn values_to_relation(v: &ValuesStmt, ctx: &EvalCtx) -> Result<Relation, ExecError>
pub(crate) fn apply_query_order(
    rel: &mut Relation,
    order_by: &[pgparser::ast::OrderItem],
    offset: Option<i64>,
    limit: Option<i64>,
    ctx: &EvalCtx,
) -> Result<(), ExecError>
```

- [ ] **Step 5: Add `query_to_relation` and `describe_query_expr`**

Replace the body of `crates/executor/src/query.rs` above the tests with:

```rust
use kv::Kv;
use mvcc::visibility::Snapshot;
use pgparser::ast::{QueryBody, QueryExpr, SetExpr};
use pgtypes::{ColumnType, Datum};
use pgwire::engine::FieldDescription;

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;
use crate::scope::{ColumnBinding, Scope};

#[allow(clippy::too_many_arguments)]
pub(crate) fn query_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &QueryExpr,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut s = (**s).clone();
            s.order_by = q.order_by.clone();
            s.limit = q.limit;
            s.offset = q.offset;
            s.locking = q.locking;
            crate::exec::select_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, &s, ctx)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let mut rel = crate::values::values_to_relation(v, ctx)?;
            crate::values::apply_query_order(&mut rel, &q.order_by, q.offset, q.limit, ctx)?;
            Ok(rel)
        }
        SetExpr::SetOp { .. } => crate::setops::set_expr_to_relation(
            catalog_kv,
            kv,
            global,
            gsnap,
            snapshot,
            own,
            &q.body,
            &q.order_by,
            q.offset,
            q.limit,
            ctx,
        ),
    }
}

pub(crate) fn describe_query_expr(
    catalog_kv: &dyn Kv,
    q: &QueryExpr,
) -> Result<Vec<FieldDescription>, ExecError> {
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            let scope = if s.from.is_empty() {
                Scope::empty()
            } else {
                crate::exec::build_from_schema(catalog_kv, &s.from)?.scope
            };
            let projection =
                crate::subquery::resolve_types_in_projection(catalog_kv, &s.projection)?;
            let (fields, _exprs, _tys) = crate::exec::resolve_projection(&projection, &scope)?;
            Ok(fields)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let schema = crate::values::describe_values(v)?;
            Ok(schema
                .names
                .iter()
                .zip(&schema.types)
                .map(|(name, ty)| crate::exec::field(name, *ty))
                .collect())
        }
        SetExpr::SetOp { .. } => crate::setops::describe_set_expr(catalog_kv, &q.body),
    }
}

pub(crate) fn relation_to_rows_result(
    rel: Relation,
    ctx: &EvalCtx,
) -> pgwire::engine::QueryResult {
    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    crate::exec::rows_result(fields, &rel.rows, &ctx.time_zone)
}
```

- [ ] **Step 6: Refactor setops to expose `SetExpr` helpers**

In `crates/executor/src/setops.rs`, replace `describe_set_query` with:

```rust
pub(crate) fn describe_set_expr(
    catalog_kv: &dyn Kv,
    body: &SetExpr,
) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
    let cols = resolve_set_columns(catalog_kv, body, 0)?;
    Ok(cols
        .iter()
        .map(|c| crate::exec::field(&c.name, output_type(c)))
        .collect())
}
```

Add:

```rust
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_expr_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    body: &SetExpr,
    order_by: &[pgparser::ast::OrderItem],
    offset: Option<i64>,
    limit: Option<i64>,
    ctx: &EvalCtx,
) -> Result<crate::join::Relation, ExecError> {
    let cols = resolve_set_columns(catalog_kv, body, 0)?;
    let out_tys: Vec<ColumnType> = cols.iter().map(output_type).collect();
    let mut rows = fold(
        catalog_kv, kv, global, gsnap, snapshot, own, body, &out_tys, ctx, 0,
    )?;

    let scope = Scope {
        columns: cols
            .iter()
            .map(|c| ColumnBinding {
                qualifier: None,
                name: c.name.clone(),
                ty: output_type(c),
            })
            .collect(),
    };

    if !order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(order_key(&item.expr, &scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, order_by));
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    crate::exec::apply_offset_limit(&mut rows, offset, limit);

    Ok(crate::join::Relation { scope, rows })
}
```

Then make `execute_set_operation` call `set_expr_to_relation` and render with `query::relation_to_rows_result`, or delete `execute_set_operation` after `session.rs` no longer uses it.

- [ ] **Step 7: Run focused executor query tests**

Run:

```bash
cargo test -p executor query::tests::top_level_select_values_and_setops_use_query_pipeline
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/lib.rs crates/executor/src/query.rs crates/executor/src/exec.rs crates/executor/src/setops.rs crates/executor/src/values.rs
git commit -m "[codex] Add query expression executor pipeline"
```

---

### Task 4: Migrate Session Dispatch And Describe

**Files:**
- Modify: `crates/executor/src/session.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/agg.rs`

- [ ] **Step 1: Write failing top-level wire tests**

Create `crates/executor/tests/query_expressions.rs`:

```rust
use std::sync::Arc;

use executor::SqlEngine;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect_new() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(spawn().await)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push((0..row.len()).map(|i| row.get(i).map(str::to_string)).collect());
        }
    }
    out
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err("expected error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn top_level_queries_keep_existing_behavior() {
    let c = connect_new().await;
    assert_eq!(rows(&c, "SELECT 1").await, vec![vec![Some("1".into())]]);
    assert_eq!(
        rows(&c, "VALUES (2), (1) ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 1 UNION SELECT 2 ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn describe_top_level_query_exprs() {
    let c = connect_new().await;

    let stmt = c.prepare("SELECT 1 AS one").await.expect("describe select");
    assert_eq!(stmt.columns()[0].name(), "one");

    let stmt = c.prepare("VALUES (1, 'a')").await.expect("describe values");
    assert_eq!(stmt.columns()[0].name(), "column1");
    assert_eq!(stmt.columns()[1].name(), "column2");

    let stmt = c
        .prepare("SELECT 1 AS x UNION SELECT 2")
        .await
        .expect("describe set op");
    assert_eq!(stmt.columns()[0].name(), "x");
}

#[tokio::test]
async fn locking_select_still_uses_locking_path() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    c.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    assert_eq!(
        rows(&c, "SELECT id FROM t FOR UPDATE").await,
        vec![vec![Some("1".into())]]
    );
    assert_eq!(err_code(&c, "VALUES (1) FOR UPDATE").await, "42601");
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
cargo test -p executor --test query_expressions top_level_queries_keep_existing_behavior describe_top_level_query_exprs locking_select_still_uses_locking_path
```

Expected: compile failure from remaining matches on deleted statement variants.

- [ ] **Step 3: Update session dispatch**

In `crates/executor/src/session.rs`, replace:

```rust
            Statement::Select(s) if s.locking.is_some() => self.run_select_locking(s).await,
            Statement::Select(_) => self.run_select(stmt).await,
            Statement::Values(_) => self.run_values(stmt).await,
            Statement::SetOperation(_) => self.run_set_operation(stmt).await,
```

with:

```rust
            Statement::Query(q) if q.locking.is_some() => self.run_query_locking(q).await,
            Statement::Query(_) => self.run_query(stmt).await,
```

Replace `run_select`, `run_values`, and `run_set_operation` with:

```rust
    async fn run_query(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let Statement::Query(q) = stmt else {
            unreachable!("run_one only routes a Query here");
        };
        let (snapshot, own, gsnap) = self.read_context().await?;
        let ctx = self.eval_ctx();
        let rel = crate::query::query_to_relation(
            &*self.catalog_kv,
            &*self.kv,
            &*self.catalog_kv,
            &gsnap,
            &snapshot,
            own,
            q,
            &ctx,
        )?;
        Ok(crate::query::relation_to_rows_result(rel, &ctx))
    }

    async fn run_query_locking(
        &mut self,
        q: &pgparser::ast::QueryExpr,
    ) -> Result<QueryResult, ExecError> {
        let pgparser::ast::SetExpr::Query(pgparser::ast::QueryBody::Select(s)) = &q.body else {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE is only supported for SELECT".into(),
            ));
        };
        self.run_select_locking_with_query_tail(q, s).await
    }
```

Rename the existing `run_select_locking` to `run_select_locking_with_query_tail` and build a `SelectStmt` clone with the `QueryExpr` tail before calling `execute_read_locking`:

```rust
    async fn run_select_locking_with_query_tail(
        &mut self,
        q: &pgparser::ast::QueryExpr,
        s: &pgparser::ast::SelectStmt,
    ) -> Result<QueryResult, ExecError> {
        let mut s = s.clone();
        s.order_by = q.order_by.clone();
        s.limit = q.limit;
        s.offset = q.offset;
        s.locking = q.locking;
        // keep the rest of the existing run_select_locking body, using `&s`
    }
```

- [ ] **Step 4: Update describe**

In `crates/executor/src/exec.rs`, replace the beginning of `describe` with:

```rust
    let statements = pgparser::parse(sql)?;
    let stmt = statements.first();
    let Some(Statement::Query(q)) = stmt else {
        return Ok(Vec::new());
    };
    crate::query::describe_query_expr(catalog_kv, q)
```

- [ ] **Step 5: Update aggregate parser-test helpers**

In `crates/executor/src/agg.rs`, update test code that destructures parsed statements. Use this helper inside the test module:

```rust
    fn parse_select(sql: &str) -> pgparser::ast::SelectStmt {
        use pgparser::ast::{QueryBody, SetExpr, Statement};
        let stmt = pgparser::parse(sql).expect("parse").into_iter().next().expect("stmt");
        let Statement::Query(q) = stmt else {
            panic!("expected query statement");
        };
        let SetExpr::Query(QueryBody::Select(s)) = q.body else {
            panic!("expected select query body");
        };
        *s
    }
```

Replace local `Statement::Select` destructuring in tests with `parse_select`.

- [ ] **Step 6: Run focused wire tests**

Run:

```bash
cargo test -p executor --test query_expressions
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/session.rs crates/executor/src/exec.rs crates/executor/src/agg.rs crates/executor/tests/query_expressions.rs
git commit -m "[codex] Route top-level reads through QueryExpr"
```

---

### Task 5: Generalize Uncorrelated Subquery Resolution

**Files:**
- Modify: `crates/executor/src/subquery.rs`
- Modify: `crates/executor/src/eval.rs`
- Modify: `crates/executor/src/agg.rs`
- Modify: `crates/executor/tests/query_expressions.rs`

- [ ] **Step 1: Add failing nested expression-subquery wire tests**

Append to `crates/executor/tests/query_expressions.rs`:

```rust
#[tokio::test]
async fn expression_subqueries_accept_values_and_setops() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    c.simple_query("INSERT INTO t VALUES (1), (2), (3)").await.expect("insert");

    assert_eq!(
        rows(&c, "SELECT (VALUES (2) UNION SELECT 1 ORDER BY 1 LIMIT 1)").await,
        vec![vec![Some("1".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT id FROM t WHERE id IN (VALUES (1), (3)) ORDER BY id").await,
        vec![vec![Some("1".into())], vec![Some("3".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT EXISTS (SELECT 1 EXCEPT SELECT 1)").await,
        vec![vec![Some("false".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 3 > ALL (VALUES (1), (2))").await,
        vec![vec![Some("true".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 2 = ANY (SELECT 1 UNION SELECT 2)").await,
        vec![vec![Some("true".into())]]
    );
}

#[tokio::test]
async fn expression_subquery_error_surface_is_preserved() {
    let c = connect_new().await;
    assert_eq!(err_code(&c, "SELECT (VALUES (1), (2))").await, "21000");
    assert_eq!(err_code(&c, "SELECT (VALUES (1, 2))").await, "42601");
    assert_eq!(err_code(&c, "SELECT 1 IN (VALUES (1, 2))").await, "42601");
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p executor --test query_expressions expression_subqueries_accept_values_and_setops expression_subquery_error_surface_is_preserved
```

Expected: FAIL or compile error because subquery execution still expects `SelectStmt`.

- [ ] **Step 3: Update subquery execution to use `QueryExpr`**

In `crates/executor/src/subquery.rs`, change imports:

```rust
use pgparser::ast::{BinaryOp, Expr, FuncArgs, FuncCall, QueryExpr, SelectItem, SelectStmt};
```

Replace `no_locking` with:

```rust
fn no_locking(q: &QueryExpr) -> Result<(), ExecError> {
    if q.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed inside a subquery".into(),
        ));
    }
    Ok(())
}
```

Replace `run_relation`, `run_rows`, `run_scalar`, and `run_single_column` signatures and bodies:

```rust
fn run_relation(ctx: &SubCtx, q: &QueryExpr) -> Result<crate::join::Relation, ExecError> {
    no_locking(q)?;
    crate::query::query_to_relation(
        ctx.catalog_kv,
        ctx.kv,
        ctx.global,
        ctx.gsnap,
        ctx.snapshot,
        ctx.own,
        q,
        ctx.eval_ctx,
    )
}

fn run_rows(ctx: &SubCtx, q: &QueryExpr) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(run_relation(ctx, q)?.rows)
}

fn run_scalar(ctx: &SubCtx, q: &QueryExpr) -> Result<(Datum, ColumnType), ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    if rel.rows.len() > 1 {
        return Err(ExecError::CardinalityViolation);
    }
    let value = rel
        .rows
        .into_iter()
        .next()
        .map(|mut r| r.remove(0))
        .unwrap_or(Datum::Null);
    Ok((value, ty))
}

fn run_single_column(ctx: &SubCtx, q: &QueryExpr) -> Result<(ColumnType, Vec<Datum>), ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    let col = rel.rows.into_iter().map(|mut r| r.remove(0)).collect();
    Ok((ty, col))
}
```

Replace `scalar_subquery_type` with:

```rust
fn scalar_subquery_type(catalog_kv: &dyn kv::Kv, q: &QueryExpr) -> Result<ColumnType, ExecError> {
    let fields = crate::query::describe_query_expr(catalog_kv, q)?;
    if fields.len() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    pgtypes::ColumnType::from_oid(fields[0].type_oid)
        .ok_or_else(|| ExecError::Unsupported(format!("unknown type oid {}", fields[0].type_oid)))
}
```

If `ColumnType::from_oid` does not exist, add this helper in `subquery.rs`:

```rust
fn field_type(field: &pgwire::engine::FieldDescription) -> Result<ColumnType, ExecError> {
    ColumnType::from_sql_name(match field.type_oid {
        16 => "bool",
        20 => "int8",
        23 => "int4",
        25 => "text",
        701 => "float8",
        1700 => "numeric",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        oid => return Err(ExecError::Unsupported(format!("unknown type oid {oid}"))),
    })
    .ok_or_else(|| ExecError::Unsupported("field type oid did not resolve".into()))
}
```

Then use `field_type(&fields[0])`.

- [ ] **Step 4: Update eval and aggregate matches**

In `crates/executor/src/eval.rs` and `crates/executor/src/agg.rs`, keep behavior but update any type-specific assumptions to compile with `Expr::*Subquery(Box<QueryExpr>)`. The match arms remain:

```rust
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. } => Err(ExecError::Unsupported(
            "subquery expression was not resolved before evaluation".into(),
        )),
```

- [ ] **Step 5: Run nested expression-subquery tests**

Run:

```bash
cargo test -p executor --test query_expressions expression_subqueries_accept_values_and_setops expression_subquery_error_surface_is_preserved
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/subquery.rs crates/executor/src/eval.rs crates/executor/src/agg.rs crates/executor/tests/query_expressions.rs
git commit -m "[codex] Execute nested QueryExpr subqueries"
```

---

### Task 6: Generalize Derived Tables

**Files:**
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/tests/query_expressions.rs`

- [ ] **Step 1: Add failing derived query-expression tests**

Append to `crates/executor/tests/query_expressions.rs`:

```rust
#[tokio::test]
async fn derived_tables_accept_setops_and_tailed_values() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    c.simple_query("INSERT INTO t VALUES (1), (2), (3)").await.expect("insert");

    assert_eq!(
        rows(
            &c,
            "SELECT s.x FROM (SELECT id AS x FROM t WHERE id < 3 UNION SELECT 9 ORDER BY x DESC LIMIT 2) AS s ORDER BY s.x"
        )
        .await,
        vec![vec![Some("2".into())], vec![Some("9".into())]]
    );

    assert_eq!(
        rows(
            &c,
            "SELECT v.x FROM (VALUES (3), (1), (2) ORDER BY 1 LIMIT 2) AS v(x) ORDER BY v.x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn derived_query_expr_describe_uses_alias_columns() {
    let c = connect_new().await;
    let stmt = c
        .prepare("SELECT d.x FROM (SELECT 1 AS original UNION SELECT 2) AS d(x)")
        .await
        .expect("describe derived set op");
    assert_eq!(stmt.columns()[0].name(), "x");
    assert_eq!(stmt.columns()[0].type_().oid(), 23);
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p executor --test query_expressions derived_tables_accept_setops_and_tailed_values derived_query_expr_describe_uses_alias_columns
```

Expected: FAIL because derived table execution/schema still expects `QueryBody`.

- [ ] **Step 3: Update runtime derived-table execution**

In `crates/executor/src/exec.rs`, update `build_table_expr` derived branch:

```rust
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let inner = crate::query::query_to_relation(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                subquery,
                ctx,
            )?;
            crate::values::requalify_derived(inner, alias, columns)
        }
```

- [ ] **Step 4: Update schema-only derived-table build**

In `build_table_expr_schema`, update the derived branch:

```rust
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let fields = crate::query::describe_query_expr(catalog_kv, subquery)?;
            let columns_out = fields
                .into_iter()
                .map(|f| ColumnBinding {
                    qualifier: None,
                    name: f.name,
                    ty: column_type_from_oid(f.type_oid)?,
                })
                .collect();
            crate::values::requalify_derived(
                Relation {
                    scope: Scope {
                        columns: columns_out,
                    },
                    rows: Vec::new(),
                },
                alias,
                columns,
            )
        }
```

Add this helper near `build_table_expr_schema` if no shared OID conversion exists:

```rust
fn column_type_from_oid(oid: u32) -> Result<ColumnType, ExecError> {
    let name = match oid {
        16 => "bool",
        20 => "int8",
        23 => "int4",
        25 => "text",
        701 => "float8",
        1700 => "numeric",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        _ => return Err(ExecError::Unsupported(format!("unknown type oid {oid}"))),
    };
    ColumnType::from_sql_name(name)
        .ok_or_else(|| ExecError::Unsupported(format!("unknown type name {name}")))
}
```

- [ ] **Step 5: Run derived query-expression tests**

Run:

```bash
cargo test -p executor --test query_expressions derived_tables_accept_setops_and_tailed_values derived_query_expr_describe_uses_alias_columns
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/exec.rs crates/executor/tests/query_expressions.rs
git commit -m "[codex] Materialize derived QueryExpr tables"
```

---

### Task 7: Generalize Router Range Collection

**Files:**
- Modify: `crates/cluster/src/range/router.rs`

- [ ] **Step 1: Add failing router tests**

Append to the router test module in `crates/cluster/src/range/router.rs`:

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn nested_query_exprs_route_or_reject_by_all_referenced_ranges() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router.simple("CREATE TABLE a (id int4)").await.expect("a");
        router.simple("CREATE TABLE b (id int4)").await.expect("b");
        router.simple("INSERT INTO a VALUES (1)").await.expect("seed a");
        router.simple("INSERT INTO b VALUES (2)").await.expect("seed b");

        assert_eq!(
            router
                .scan_one_i32("SELECT x FROM (SELECT id AS x FROM a UNION VALUES (3)) AS s ORDER BY x")
                .await,
            vec![1, 3]
        );

        let err = router
            .simple("SELECT id FROM a WHERE id IN (SELECT id FROM b)")
            .await
            .expect_err("cross-range expression subquery rejected");
        assert_eq!(err.code, "0A000", "got {err:?}");

        let err = router
            .simple("SELECT x FROM (SELECT id AS x FROM a UNION SELECT id FROM b) AS s")
            .await
            .expect_err("cross-range derived set-op rejected");
        assert_eq!(err.code, "0A000", "got {err:?}");

        router
            .simple("SELECT x FROM (VALUES (1), (2)) AS v(x)")
            .await
            .expect("values-only derived query is range-neutral");
    }
```

- [ ] **Step 2: Run router test to verify RED**

Run:

```bash
cargo test -p cluster nested_query_exprs_route_or_reject_by_all_referenced_ranges
```

Expected: compile failure or behavior failure because router still matches old AST shapes.

- [ ] **Step 3: Update `pinning_range`**

In `pinning_range`, replace `Statement::Select`, `Statement::SetOperation`, and `Statement::Values` branches with:

```rust
            Statement::Query(q) => {
                let mut ranges = std::collections::BTreeSet::new();
                collect_query_expr_ranges(self, q, &mut ranges)?;
                match ranges.len() {
                    0 => Ok(None),
                    1 => Ok(Some(*ranges.iter().next().expect("len()==1 has one element"))),
                    _ => Err(ExecError::Unsupported(
                        "cross-range query expressions are not supported".into(),
                    )),
                }
            }
```

- [ ] **Step 4: Add unified range walkers**

Replace `collect_query_body_ranges`, `collect_set_expr_ranges`, and nested-subquery calls with:

```rust
fn collect_query_expr_ranges(
    router: &RangeRouter,
    q: &pgparser::ast::QueryExpr,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    collect_set_expr_ranges(router, &q.body, out)
}

fn collect_query_body_ranges(
    router: &RangeRouter,
    body: &pgparser::ast::QueryBody,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    match body {
        pgparser::ast::QueryBody::Select(s) => collect_select_ranges(router, s, out),
        pgparser::ast::QueryBody::Values(_) => Ok(()),
    }
}
```

Update derived table walking:

```rust
        TableExpr::Derived { subquery, .. } => {
            collect_query_expr_ranges(router, subquery, out)?;
        }
```

Update expression subquery walking:

```rust
        Expr::ScalarSubquery(q) | Expr::Exists(q) => {
            collect_query_expr_ranges(router, q, out)?;
        }
        Expr::InSubquery { expr, subquery, .. }
        | Expr::Quantified { expr, subquery, .. } => {
            collect_expr_ranges(router, expr, out)?;
            collect_query_expr_ranges(router, subquery, out)?;
        }
```

- [ ] **Step 5: Run router tests**

Run:

```bash
cargo test -p cluster nested_query_exprs_route_or_reject_by_all_referenced_ranges
cargo test -p cluster a_cross_range_set_op_is_rejected_while_colocated_runs values_queries_are_range_neutral_and_set_ops_still_check_select_ranges
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/cluster/src/range/router.rs
git commit -m "[codex] Route unified query expressions"
```

---

### Task 8: Add Conformance Corpus And Full Regression

**Files:**
- Add: `crates/conformance/corpus/nested_query_expressions.sql`
- Modify: `crates/executor/tests/query_expressions.rs`

- [ ] **Step 1: Add conformance corpus**

Create `crates/conformance/corpus/nested_query_expressions.sql`:

```sql
-- Query-expression generalization: nested SELECT / VALUES / set-operation
-- expressions in derived tables and uncorrelated expression subqueries, diffed
-- against PostgreSQL 18.

CREATE TABLE nq_a (id int4, name text);
INSERT INTO nq_a VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT s.x
FROM (SELECT id AS x FROM nq_a WHERE id < 3 UNION SELECT 9 ORDER BY x DESC LIMIT 2) AS s
ORDER BY s.x;

SELECT v.x
FROM (VALUES (3), (1), (2) ORDER BY 1 LIMIT 2) AS v(x)
ORDER BY v.x;

SELECT (VALUES (2) UNION SELECT 1 ORDER BY 1 LIMIT 1);

SELECT id
FROM nq_a
WHERE id IN (VALUES (1), (3))
ORDER BY id;

SELECT EXISTS (SELECT 1 EXCEPT SELECT 1);

SELECT 3 > ALL (VALUES (1), (2));
SELECT 2 = ANY (SELECT 1 UNION SELECT 2);

SELECT d.x
FROM (SELECT 1 AS x UNION SELECT 2) AS d
WHERE d.x IN (VALUES (2))
ORDER BY d.x;

-- error parity (same SQLSTATE on both sides)
SELECT (VALUES (1), (2));
SELECT (VALUES (1, 2));
SELECT 1 IN (VALUES (1, 2));
```

- [ ] **Step 2: Run conformance target locally if oracle is available**

Start the oracle and subject using the established scripts in separate terminals or background jobs:

```bash
scripts/oracle-up.sh
cargo run --bin crabgresql -- --listen 127.0.0.1:5433
```

Then run:

```bash
cargo run -p conformance -- \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
  --subject-url "host=127.0.0.1 port=5433 user=crab dbname=crab" \
  --corpus crates/conformance/corpus \
  --out parity.json \
  --summary parity.md
```

Expected: the new `nested_query_expressions.sql` cases match PostgreSQL 18. Existing unrelated corpus mismatches, if any, must match the known baseline from recent SQL breadth slices; do not edit this corpus to hide a new mismatch.

- [ ] **Step 3: Run full targeted regression**

Run:

```bash
cargo fmt --all -- --check
cargo test -p pgparser
cargo test -p executor --test query_expressions
cargo test -p executor --test set_operations
cargo test -p executor --test values_query
cargo test -p executor --test subqueries
cargo test -p executor --test joins
cargo test -p executor --test ordering
cargo test -p cluster nested_query_exprs_route_or_reject_by_all_referenced_ranges
```

Expected: PASS.

- [ ] **Step 4: Run workspace-level regression**

Run:

```bash
cargo nextest run --workspace
cargo test --workspace --doc
```

Expected: PASS. If a distributed suite fails, inspect the failing assertion and logs; do not retry blindly or add sleeps.

- [ ] **Step 5: Run UAC-safe target-name guard**

Run:

```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```

Expected: no output. The new `query_expressions.rs` target name is UAC-safe.

- [ ] **Step 6: Commit**

```bash
git add crates/conformance/corpus/nested_query_expressions.sql crates/executor/tests/query_expressions.rs
git commit -m "[codex] Add nested query expression conformance"
```

---

### Task 9: Final Cleanup And Compatibility Audit

**Files:**
- Modify: `crates/pgparser/src/ast.rs`
- Modify: `crates/pgparser/src/parser.rs`
- Modify: `crates/executor/src/agg.rs`
- Modify: `crates/executor/src/eval.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/query.rs`
- Modify: `crates/executor/src/session.rs`
- Modify: `crates/executor/src/setops.rs`
- Modify: `crates/executor/src/subquery.rs`
- Modify: `crates/executor/src/values.rs`
- Modify: `crates/cluster/src/range/router.rs`
- Modify: `docs/superpowers/specs/2026-06-18-crabgresql-query-expression-generalization-design.md`

- [ ] **Step 1: Search for obsolete AST variants**

Run:

```bash
rg -n "Statement::Select|Statement::Values|Statement::SetOperation|ValuesQuery|SetQuery|Box<SelectStmt>|subquery: QueryBody" crates
```

Expected: no matches except historical comments that should be updated in the same task. If comments mention the old public shape, edit them to say `Statement::Query(QueryExpr)`.

- [ ] **Step 2: Search for stale deferral text**

Run:

```bash
rg -n "parenthesized.*set-operation subtree|parenthesized VALUES branch|nested set ops|VALUES as a branch|deferred.*VALUES|deferred.*set" crates docs/superpowers/specs/2026-06-18-crabgresql-query-expression-generalization-design.md
```

Expected: no stale comments claiming nested set operations or parenthesized query tails are unsupported for the now-supported contexts. Keep comments that document still-deferred CTEs, windows, correlation, cross-range distributed query execution, collations, and `NULLS FIRST/LAST`.

- [ ] **Step 3: Run clippy**

Run:

```bash
cargo clippy --locked --workspace --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Record model-checking decision**

Do not add a Stateright model for this slice. Record this in the implementation
notes or PR description:

```text
No Stateright model: query-expression generalization is a parser/executor
read-path refactor over one existing statement snapshot. It adds no write path,
lock protocol, MVCC visibility rule, leadership interaction, recovery behavior,
or distributed interleaving; cross-range query expressions remain rejected by the
router.
```

- [ ] **Step 5: Run final formatting**

Run:

```bash
cargo fmt --all
git diff --check
```

Expected: `git diff --check` produces no output.

- [ ] **Step 6: Commit cleanup if any files changed**

If Step 1-5 changed files:

```bash
git add crates docs
git commit -m "[codex] Clean up QueryExpr compatibility references"
```

If no files changed, record in the task notes: "No cleanup commit needed."
