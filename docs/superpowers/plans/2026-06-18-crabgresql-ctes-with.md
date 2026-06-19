# CTEs / WITH SQL Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add non-recursive, read-only `WITH` / CTE support for query statements, matching PostgreSQL's ordinary CTE visibility and shadowing rules inside crabgresql's current single-range query model.

**Architecture:** Parse `WITH` into query-local AST nodes while preserving no-`WITH` statement variants. Evaluate CTEs left-to-right into a scoped `CteContext` of materialized `Relation`s; relation builders resolve CTE names before catalog tables and pass the context through derived tables, subqueries, set operations, Describe, and router range collection.

**Tech Stack:** Rust 2024 workspace, hand-written `pgparser`, `executor::Relation`, `pgwire` integration tests with `tokio-postgres`, `cargo nextest`, PostgreSQL 18 conformance corpus.

---

## Scope Check

This is one coherent slice. Parser, executor, router, and conformance changes all serve the same query-local CTE feature, and each task below produces a working, testable increment.

## File Structure

- Modify `crates/pgparser/src/ast.rs`: add `WithClause`, `Cte`, `QueryExpr`, and optional `with` fields on query forms.
- Modify `crates/pgparser/src/token.rs`: add `Keyword::{With, Recursive}`.
- Modify `crates/pgparser/src/parser.rs`: parse optional `WITH`, CTE definitions, and query expressions in CTE bodies.
- Modify `crates/pgparser/tests/libpg_query_oracle.rs`: add accepted raw-parse forms for non-recursive CTE grammar.
- Create `crates/executor/src/cte.rs`: CTE context, duplicate detection, materialization orchestration, relation requalification, and schema-only context.
- Modify `crates/executor/src/lib.rs`: add `mod cte;`.
- Modify `crates/executor/src/exec.rs`: thread CTE context through relation builders, `execute_read`, `select_to_relation`, `build_from_schema`, and `describe`.
- Modify `crates/executor/src/setops.rs`: thread CTE context through set-operation leaf evaluation and schema resolution.
- Modify `crates/executor/src/subquery.rs`: pass CTE context into nested subquery resolution.
- Modify `crates/executor/src/values.rs`: reuse derived-table requalification helper for CTE output aliases.
- Create `crates/executor/tests/ctes.rs`: over-the-wire CTE behavior tests.
- Modify `crates/cluster/src/range/router.rs`: collect ranges with CTE scope and CTE shadowing.
- Add `crates/conformance/corpus/ctes.sql`: PostgreSQL-diffed coverage.

## Task 1: Parser AST And Top-Level WITH

**Files:**
- Modify: `crates/pgparser/src/ast.rs`
- Modify: `crates/pgparser/src/token.rs`
- Modify: `crates/pgparser/src/parser.rs`
- Modify: `crates/pgparser/tests/libpg_query_oracle.rs`

- [ ] **Step 1: Add failing parser tests**

Append these tests to `crates/pgparser/src/parser.rs` inside the existing `#[cfg(test)] mod tests`:

```rust
#[test]
fn parses_with_select_values_and_setop_bodies() {
    use crate::ast::{QueryExpr, Statement};

    let s = parse("WITH a AS (SELECT 1 AS x), b(y) AS (VALUES (2)) SELECT x FROM a")
        .expect("parse cte select");
    let Statement::Select(sel) = &s[0] else {
        panic!("expected SELECT with WITH, got {:?}", s[0]);
    };
    let with = sel.with.as_ref().expect("with clause");
    assert!(!with.recursive);
    assert_eq!(with.ctes.len(), 2);
    assert_eq!(with.ctes[0].name, "a");
    assert!(with.ctes[0].columns.is_none());
    assert_eq!(with.ctes[1].name, "b");
    assert_eq!(with.ctes[1].columns.as_deref(), Some(&["y".to_string()][..]));
    assert!(matches!(with.ctes[0].query, QueryExpr::Select(_)));
    assert!(matches!(with.ctes[1].query, QueryExpr::Values(_)));

    let s = parse("WITH u AS (SELECT 1 UNION SELECT 2) SELECT * FROM u")
        .expect("parse setop cte");
    let Statement::Select(sel) = &s[0] else {
        panic!("expected SELECT with setop CTE, got {:?}", s[0]);
    };
    assert!(matches!(
        sel.with.as_ref().expect("with").ctes[0].query,
        QueryExpr::SetOperation(_)
    ));
}

#[test]
fn parses_with_recursive_and_rejects_duplicate_cte_names() {
    let s = parse("WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r").expect("parse recursive");
    let crate::ast::Statement::Select(sel) = &s[0] else {
        panic!("expected SELECT, got {:?}", s[0]);
    };
    assert!(sel.with.as_ref().expect("with").recursive);

    let err = parse("WITH a AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a")
        .expect_err("duplicate CTE names rejected during parse");
    assert_eq!(err.sqlstate(), "42712");
}
```

- [ ] **Step 2: Run parser tests and confirm the expected failure**

Run:

```bash
cargo test -p pgparser parses_with_select_values_and_setop_bodies parses_with_recursive_and_rejects_duplicate_cte_names --lib
```

Expected: FAIL with compile errors for missing `with`, `QueryExpr`, and CTE AST types.

- [ ] **Step 3: Add AST nodes and optional `with` fields**

In `crates/pgparser/src/ast.rs`, add these types after `ValuesStmt`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: QueryExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    Select(Box<SelectStmt>),
    Values(ValuesQuery),
    SetOperation(SetQuery),
}
```

Add `pub with: Option<WithClause>,` as the first field of `SelectStmt`, `ValuesQuery`, and `SetQuery`. Existing constructors must initialize it to `None`.

- [ ] **Step 4: Add keywords**

In `crates/pgparser/src/token.rs`, add variants:

```rust
With,
Recursive,
```

Add `from_word` mappings:

```rust
"with" => Keyword::With,
"recursive" => Keyword::Recursive,
```

- [ ] **Step 5: Parse optional WITH and CTE definitions**

In `crates/pgparser/src/parser.rs`, route `WITH` to query parsing:

```rust
Token::Keyword(Keyword::Select)
| Token::Keyword(Keyword::Values)
| Token::Keyword(Keyword::With)
| Token::LParen => self.query_stmt()
```

Add parser helpers:

```rust
fn parse_with_clause(&mut self) -> Result<Option<crate::ast::WithClause>, ParseError> {
    use crate::ast::{Cte, WithClause};
    if !self.eat_keyword(Keyword::With) {
        return Ok(None);
    }
    let recursive = self.eat_keyword(Keyword::Recursive);
    let mut ctes = Vec::new();
    loop {
        let name = self.expect_ident()?;
        if ctes.iter().any(|c: &Cte| c.name.eq_ignore_ascii_case(&name)) {
            return Err(ParseError::new_sqlstate(
                "42712",
                format!("table name \"{name}\" specified more than once"),
                self.peek_pos(),
            ));
        }
        let columns = if *self.peek() == Token::LParen {
            self.bump();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::As))?;
        self.expect(&Token::LParen)?;
        let query = self.query_expr()?;
        self.expect(&Token::RParen)?;
        ctes.push(Cte { name, columns, query });
        if !self.eat_comma() {
            break;
        }
    }
    Ok(Some(WithClause { recursive, ctes }))
}

fn query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
    use crate::ast::{QueryBody, QueryExpr, SetExpr, SetQuery, ValuesQuery};
    let with = self.parse_with_clause()?;
    let body = self.set_expr(0)?;
    let (order_by, limit, offset) = self.parse_set_tail()?;
    match body {
        SetExpr::Query(QueryBody::Select(mut s)) => {
            s.with = with;
            s.order_by = order_by;
            s.limit = limit;
            s.offset = offset;
            s.locking = self.parse_locking()?;
            Ok(QueryExpr::Select(s))
        }
        SetExpr::Query(QueryBody::Values(v)) => Ok(QueryExpr::Values(ValuesQuery {
            with,
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
            Ok(QueryExpr::SetOperation(SetQuery {
                with,
                body,
                order_by,
                limit,
                offset,
            }))
        }
    }
}
```

Replace `query_stmt` with a thin wrapper:

```rust
fn query_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
    match self.query_expr()? {
        crate::ast::QueryExpr::Select(s) => Ok(crate::ast::Statement::Select(*s)),
        crate::ast::QueryExpr::Values(v) => Ok(crate::ast::Statement::Values(v)),
        crate::ast::QueryExpr::SetOperation(q) => Ok(crate::ast::Statement::SetOperation(q)),
    }
}
```

In `select_core`, initialize `with: None`. In all test/manual `SelectStmt`, `ValuesQuery`, and `SetQuery` construction sites, add `with: None`.

- [ ] **Step 6: Add oracle accepted forms**

Add these strings to `ACCEPTED` in `crates/pgparser/tests/libpg_query_oracle.rs`:

```rust
"WITH c AS (SELECT 1) SELECT * FROM c",
"WITH a AS (VALUES (1)), b AS (SELECT * FROM a) SELECT * FROM b",
"WITH u AS (SELECT 1 UNION SELECT 2) SELECT * FROM u",
"WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
```

- [ ] **Step 7: Verify parser task**

Run:

```bash
cargo test -p pgparser parses_with_select_values_and_setop_bodies parses_with_recursive_and_rejects_duplicate_cte_names --lib
cargo test -p pgparser
```

Expected: PASS.

- [ ] **Step 8: Commit parser support**

```bash
git add crates/pgparser/src/ast.rs crates/pgparser/src/token.rs crates/pgparser/src/parser.rs crates/pgparser/tests/libpg_query_oracle.rs
git commit -m "feat: parse read-only CTE queries"
```

## Task 2: CTE Context And SELECT Execution

**Files:**
- Create: `crates/executor/src/cte.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/subquery.rs`
- Test: `crates/executor/tests/ctes.rs`

- [ ] **Step 1: Add failing wire tests for SELECT CTE behavior**

Create `crates/executor/tests/ctes.rs`:

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
async fn simple_cte_later_cte_and_forward_reference() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        err_code(&c, "WITH b AS (SELECT * FROM a), a AS (SELECT 1 AS x) SELECT * FROM b").await,
        "42P01"
    );
}

#[tokio::test]
async fn cte_shadows_base_table_and_can_be_reused() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE src (x int4)").await.expect("create src");
    c.simple_query("INSERT INTO src VALUES (9)").await.expect("insert src");
    assert_eq!(
        rows(&c, "WITH src AS (SELECT 1 AS x) SELECT a.x, b.x FROM src a, src b").await,
        vec![vec![Some("1".into()), Some("1".into())]]
    );
}

#[tokio::test]
async fn cte_column_aliases_and_recursive_error() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "WITH c(y) AS (SELECT 7 AS x) SELECT y FROM c").await,
        vec![vec![Some("7".into())]]
    );
    assert_eq!(
        err_code(&c, "WITH c(y, z) AS (SELECT 7 AS x) SELECT * FROM c").await,
        "42601"
    );
    assert_eq!(
        err_code(&c, "WITH RECURSIVE r AS (SELECT 1 AS x) SELECT * FROM r").await,
        "0A000"
    );
}
```

- [ ] **Step 2: Run CTE wire tests and confirm the expected failure**

Run:

```bash
cargo test -p executor --test ctes
```

Expected: FAIL with compile errors or runtime `42P01` because CTEs are parsed but not evaluated.

- [ ] **Step 3: Create CTE context module**

Create `crates/executor/src/cte.rs`:

```rust
use std::collections::BTreeSet;

use pgparser::ast::{Cte, QueryExpr, WithClause};

use crate::error::ExecError;
use crate::join::Relation;

#[derive(Debug, Clone, Default)]
pub(crate) struct CteContext {
    entries: Vec<(String, Relation)>,
}

impl CteContext {
    pub(crate) fn empty() -> Self {
        Self { entries: Vec::new() }
    }

    pub(crate) fn child(&self) -> Self {
        self.clone()
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<&Relation> {
        self.entries
            .iter()
            .rev()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, rel)| rel)
    }

    pub(crate) fn insert_current_scope(
        &mut self,
        names_in_scope: &mut BTreeSet<String>,
        name: &str,
        rel: Relation,
    ) -> Result<(), ExecError> {
        let key = name.to_ascii_lowercase();
        if !names_in_scope.insert(key) {
            return Err(ExecError::DuplicateAlias(name.to_string()));
        }
        self.entries.push((name.to_string(), rel));
        Ok(())
    }
}

pub(crate) fn reject_recursive(with: &Option<WithClause>) -> Result<(), ExecError> {
    if with.as_ref().is_some_and(|w| w.recursive) {
        return Err(ExecError::Unsupported(
            "WITH RECURSIVE is not supported".into(),
        ));
    }
    Ok(())
}

pub(crate) fn requalify_cte(
    mut rel: Relation,
    cte_name: &str,
    alias: Option<&str>,
) -> Relation {
    let qualifier = alias.unwrap_or(cte_name).to_string();
    for col in &mut rel.scope.columns {
        col.qualifier = Some(qualifier.clone());
    }
    rel
}

pub(crate) fn apply_cte_column_aliases(
    rel: Relation,
    columns: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    crate::values::requalify_derived(rel, "__cte_alias_check__", columns).map(|mut rel| {
        for col in &mut rel.scope.columns {
            col.qualifier = None;
        }
        rel
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_with_clause(
    catalog_kv: &dyn kv::Kv,
    kv: &dyn kv::Kv,
    global: &dyn kv::Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    outer: &CteContext,
    with: &Option<WithClause>,
    ctx: &crate::clock::EvalCtx,
) -> Result<CteContext, ExecError> {
    reject_recursive(with)?;
    let mut out = outer.child();
    let mut names = BTreeSet::new();
    if let Some(w) = with {
        for cte in &w.ctes {
            let rel = evaluate_cte_query(catalog_kv, kv, global, gsnap, snapshot, own, &out, cte, ctx)?;
            out.insert_current_scope(&mut names, &cte.name, rel)?;
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_cte_query(
    catalog_kv: &dyn kv::Kv,
    kv: &dyn kv::Kv,
    global: &dyn kv::Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    visible: &CteContext,
    cte: &Cte,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    let rel = match &cte.query {
        QueryExpr::Select(s) => crate::exec::select_to_relation_with_ctes(
            catalog_kv, kv, global, gsnap, snapshot, own, s, visible, ctx,
        )?,
        QueryExpr::Values(v) => crate::values::values_to_relation(&v.body, ctx)?,
        QueryExpr::SetOperation(q) => crate::setops::set_query_to_relation(
            catalog_kv, kv, global, gsnap, snapshot, own, q, visible, ctx,
        )?,
    };
    apply_cte_column_aliases(rel, &cte.columns)
}
```

Add `mod cte;` to `crates/executor/src/lib.rs`.

- [ ] **Step 4: Thread context through SELECT relation building**

In `crates/executor/src/exec.rs`, keep the public existing function as a wrapper:

```rust
pub(crate) fn select_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    s: &SelectStmt,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    select_to_relation_with_ctes(
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        s,
        &crate::cte::CteContext::empty(),
        ctx,
    )
}
```

Rename the current implementation body to:

```rust
#[allow(clippy::too_many_arguments)]
pub(crate) fn select_to_relation_with_ctes(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &mvcc::visibility::Snapshot,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    let query_ctes = crate::cte::evaluate_with_clause(
        catalog_kv, kv, global, gsnap, snapshot, own, ctes, &s.with, ctx,
    )?;
    let sub_ctx = crate::subquery::SubCtx {
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        eval_ctx: ctx,
        ctes: &query_ctes,
    };
    let resolved = crate::subquery::resolve_in_select(&sub_ctx, s)?;
    let s = &resolved;
    let relation = if s.from.is_empty() {
        Relation { scope: Scope::empty(), rows: vec![vec![]] }
    } else {
        build_from(catalog_kv, kv, global, gsnap, snapshot, own, &s.from, &query_ctes, ctx)?
    };
    let mut kept = Vec::new();
    for row in &relation.rows {
        if row_matches(s.filter.as_ref(), &relation.scope, row, ctx)? {
            kept.push(row.clone());
        }
    }
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, &relation.scope)?;
    let out_scope = Scope {
        columns: fields
            .iter()
            .zip(&tys)
            .map(|(f, ty)| ColumnBinding {
                qualifier: None,
                name: f.name.clone(),
                ty: *ty,
            })
            .collect(),
    };
    let rows = if crate::agg::is_aggregate_query(s) {
        crate::agg::aggregate_rows(s, &relation.scope, kept, ctx)?
    } else {
        project_rows_ordered(s, &relation.scope, &fields, &out_exprs, kept, ctx)?
    };
    Ok(Relation {
        scope: out_scope,
        rows,
    })
}
```

Change `build_from` and `build_table_expr` to accept `ctes: &crate::cte::CteContext`. In `build_table_expr`, add the CTE lookup before catalog lookup:

```rust
TableExpr::Table { name, alias } => {
    if let Some(rel) = ctes.lookup(name) {
        return Ok(crate::cte::requalify_cte(rel.clone(), name, alias.as_deref()));
    }
    let t = catalog::get_table(catalog_kv, name)?;
    let qualifier = alias.as_deref().unwrap_or(&t.name);
    let scope = Scope::single(&t, qualifier);
    let rows = scan_live(kv, global, gsnap, snapshot, own, &t)?
        .into_iter()
        .map(|(_, _, row)| row)
        .collect();
    Ok(Relation { scope, rows })
}
```

For derived `QueryBody::Select`, call `select_to_relation_with_ctes(..., ctes, ctx)`.

- [ ] **Step 5: Thread context through subquery resolution**

In `crates/executor/src/subquery.rs`, add a field to `SubCtx`:

```rust
pub(crate) ctes: &'a crate::cte::CteContext,
```

Update each call that evaluates a subquery to use `select_to_relation_with_ctes(..., ctx.ctes, ctx.eval_ctx)` instead of `select_to_relation(...)`.

- [ ] **Step 6: Update `execute_read` wrapper**

In `execute_read`, evaluate the top-level CTE context and pass it into the same relation flow. The cleanest implementation is to replace the current duplicated body with:

```rust
let Statement::Select(s) = stmt else {
    return Err(ExecError::Unsupported("not a SELECT".into()));
};
let rel = select_to_relation_with_ctes(
    catalog_kv,
    kv,
    global,
    gsnap,
    snapshot,
    own,
    s,
    &crate::cte::CteContext::empty(),
    ctx,
)?;
let fields = rel
    .scope
    .columns
    .iter()
    .map(|c| field(&c.name, c.ty))
    .collect();
Ok(rows_result(fields, &rel.rows, &ctx.time_zone))
```

Keep `execute_read_locking` rejecting CTE-backed `FOR UPDATE` by preserving the existing locking path; parser already only attaches locking to a `SelectStmt`.

- [ ] **Step 7: Verify SELECT CTE task**

Run:

```bash
cargo test -p executor --test ctes simple_cte_later_cte_and_forward_reference cte_shadows_base_table_and_can_be_reused cte_column_aliases_and_recursive_error
```

Expected: PASS.

- [ ] **Step 8: Commit SELECT CTE execution**

```bash
git add crates/executor/src/cte.rs crates/executor/src/lib.rs crates/executor/src/exec.rs crates/executor/src/subquery.rs crates/executor/tests/ctes.rs
git commit -m "feat: execute SELECT CTEs"
```

## Task 3: VALUES And Set-Operation CTE Bodies

**Files:**
- Modify: `crates/executor/src/cte.rs`
- Modify: `crates/executor/src/setops.rs`
- Modify: `crates/executor/tests/ctes.rs`

- [ ] **Step 1: Add failing tests for VALUES and set-op CTEs**

Append to `crates/executor/tests/ctes.rs`:

```rust
#[tokio::test]
async fn values_and_set_operation_ctes_work() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "WITH v(x) AS (VALUES (2), (1)) SELECT x FROM v ORDER BY x").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH u(x) AS (SELECT 1 UNION SELECT 2) SELECT x FROM u ORDER BY x DESC"
        )
        .await,
        vec![vec![Some("2".into())], vec![Some("1".into())]]
    );
}
```

- [ ] **Step 2: Run test and confirm expected failure**

Run:

```bash
cargo test -p executor --test ctes values_and_set_operation_ctes_work
```

Expected: FAIL until set-operation relation conversion accepts CTE context.

- [ ] **Step 3: Add relation-returning set-operation helper**

In `crates/executor/src/setops.rs`, add:

```rust
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_query_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &SetQuery,
    ctes: &crate::cte::CteContext,
    ctx: &EvalCtx,
) -> Result<crate::join::Relation, ExecError> {
    crate::cte::reject_recursive(&q.with)?;
    let query_ctes = crate::cte::evaluate_with_clause(
        catalog_kv, kv, global, gsnap, snapshot, own, ctes, &q.with, ctx,
    )?;
    let cols = resolve_set_columns_with_ctes(catalog_kv, &q.body, &query_ctes, 0)?;
    let out_tys: Vec<ColumnType> = cols.iter().map(output_type).collect();
    let mut rows = fold_with_ctes(
        catalog_kv, kv, global, gsnap, snapshot, own, &q.body, &out_tys, &query_ctes, ctx, 0,
    )?;
    apply_set_query_order(&mut rows, &cols, q, ctx)?;
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
    Ok(crate::join::Relation { scope, rows })
}
```

Refactor existing `execute_set_operation` to call `set_query_to_relation(..., &CteContext::empty(), ctx)` and then render the returned relation with `rows_result`.

Rename `resolve_set_columns` to `resolve_set_columns_with_ctes` and add a `ctes` parameter. For `QueryBody::Select(s)`, use `build_from_schema_with_ctes(catalog_kv, &s.from, ctes)` for the input scope. Rename `fold` to `fold_with_ctes` and pass `ctes`; for `QueryBody::Select(s)`, call `select_to_relation_with_ctes(..., ctes, ctx)`.

- [ ] **Step 4: Verify VALUES and set-operation CTEs**

Run:

```bash
cargo test -p executor --test ctes values_and_set_operation_ctes_work
cargo test -p executor --test set_operations
cargo test -p executor --test values_query
```

Expected: PASS.

- [ ] **Step 5: Commit VALUES and set-op CTE bodies**

```bash
git add crates/executor/src/cte.rs crates/executor/src/setops.rs crates/executor/tests/ctes.rs
git commit -m "feat: support VALUES and set-op CTE bodies"
```

## Task 4: Nested WITH And Describe

**Files:**
- Modify: `crates/executor/src/cte.rs`
- Modify: `crates/executor/src/exec.rs`
- Modify: `crates/executor/src/setops.rs`
- Modify: `crates/executor/tests/ctes.rs`

- [ ] **Step 1: Add failing tests for nested scope and Describe**

Append to `crates/executor/tests/ctes.rs`:

```rust
#[tokio::test]
async fn nested_with_scopes_through_derived_tables_subqueries_and_describe() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT * FROM (WITH c AS (VALUES (2)) SELECT * FROM c) AS d(x)"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT EXISTS (WITH d AS (SELECT * FROM c) SELECT 1 FROM d)"
        )
        .await,
        vec![vec![Some("t".into())]]
    );
    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) SELECT x FROM c")
        .await
        .expect("describe CTE select");
    let names: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["x"]);
}
```

- [ ] **Step 2: Run test and confirm expected failure**

Run:

```bash
cargo test -p executor --test ctes nested_with_scopes_through_derived_tables_subqueries_and_describe
```

Expected: FAIL until schema-only CTE context and nested WITH context are complete.

- [ ] **Step 3: Add schema-only CTE evaluation**

In `crates/executor/src/cte.rs`, add:

```rust
pub(crate) fn evaluate_with_clause_schema(
    catalog_kv: &dyn kv::Kv,
    outer: &CteContext,
    with: &Option<WithClause>,
) -> Result<CteContext, ExecError> {
    reject_recursive(with)?;
    let mut out = outer.child();
    let mut names = BTreeSet::new();
    if let Some(w) = with {
        for cte in &w.ctes {
            let rel = evaluate_cte_query_schema(catalog_kv, &out, cte)?;
            out.insert_current_scope(&mut names, &cte.name, rel)?;
        }
    }
    Ok(out)
}

fn evaluate_cte_query_schema(
    catalog_kv: &dyn kv::Kv,
    visible: &CteContext,
    cte: &Cte,
) -> Result<Relation, ExecError> {
    let rel = match &cte.query {
        QueryExpr::Select(s) => crate::exec::select_schema_relation_with_ctes(catalog_kv, s, visible)?,
        QueryExpr::Values(v) => crate::values_schema_relation(&v.body)?,
        QueryExpr::SetOperation(q) => crate::setops::set_query_schema_relation(catalog_kv, q, visible)?,
    };
    apply_cte_column_aliases(rel, &cte.columns)
}
```

If `values_schema_relation` does not exist, implement it in `values.rs` as:

```rust
pub(crate) fn values_schema_relation(v: &ValuesStmt) -> Result<crate::join::Relation, ExecError> {
    let schema = describe_values(v)?;
    let columns = schema
        .names
        .iter()
        .zip(&schema.types)
        .map(|(name, ty)| ColumnBinding {
            qualifier: None,
            name: name.clone(),
            ty: *ty,
        })
        .collect();
    Ok(crate::join::Relation {
        scope: Scope { columns },
        rows: Vec::new(),
    })
}
```

- [ ] **Step 4: Add schema relation helpers in `exec.rs`**

Add wrappers:

```rust
pub(crate) fn build_from_schema_with_ctes(
    catalog_kv: &dyn Kv,
    from: &[pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from_schema on empty FROM".into()))?;
    let mut acc = build_table_expr_schema_with_ctes(catalog_kv, first, ctes)?;
    for te in iter {
        let next = build_table_expr_schema_with_ctes(catalog_kv, te, ctes)?;
        acc = join_relations(
            acc,
            next,
            pgparser::ast::JoinKind::Cross,
            &pgparser::ast::JoinConstraint::None,
            &crate::clock::EvalCtx::test_default(),
        )?;
    }
    Ok(acc)
}

fn build_table_expr_schema_with_ctes(
    catalog_kv: &dyn Kv,
    te: &pgparser::ast::TableExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    use pgparser::ast::TableExpr;
    match te {
        TableExpr::Table { name, alias } => {
            if let Some(rel) = ctes.lookup(name) {
                return Ok(crate::cte::requalify_cte(rel.clone(), name, alias.as_deref()));
            }
            let t = catalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name);
            Ok(Relation {
                scope: Scope::single(&t, qualifier),
                rows: Vec::new(),
            })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let l = build_table_expr_schema_with_ctes(catalog_kv, left, ctes)?;
            let r = build_table_expr_schema_with_ctes(catalog_kv, right, ctes)?;
            join_relations(
                l,
                r,
                *kind,
                constraint,
                &crate::clock::EvalCtx::test_default(),
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let inner = match subquery {
                QueryBody::Select(s) => select_schema_relation_with_ctes(catalog_kv, s, ctes)?,
                QueryBody::Values(v) => crate::values::values_schema_relation(v)?,
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
    }
}

pub(crate) fn select_schema_relation_with_ctes(
    catalog_kv: &dyn Kv,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    let query_ctes = crate::cte::evaluate_with_clause_schema(catalog_kv, ctes, &s.with)?;
    let scope = if s.from.is_empty() {
        Scope::empty()
    } else {
        build_from_schema_with_ctes(catalog_kv, &s.from, &query_ctes)?.scope
    };
    let projection = crate::subquery::resolve_types_in_projection(catalog_kv, &s.projection)?;
    let (fields, _exprs, tys) = resolve_projection(&projection, &scope)?;
    let columns = fields
        .iter()
        .zip(&tys)
        .map(|(f, ty)| ColumnBinding {
            qualifier: None,
            name: f.name.clone(),
            ty: *ty,
        })
        .collect();
    Ok(Relation {
        scope: Scope { columns },
        rows: Vec::new(),
    })
}
```

In schema table lookup, check `ctes.lookup(name)` before catalog lookup and call `requalify_cte`.

- [ ] **Step 5: Update `describe`**

In `describe`, route SELECT through `select_schema_relation_with_ctes`:

```rust
let rel = select_schema_relation_with_ctes(catalog_kv, s, &crate::cte::CteContext::empty())?;
Ok(rel
    .scope
    .columns
    .iter()
    .map(|c| field(&c.name, c.ty))
    .collect())
```

Update `describe_set_query` to use `set_query_schema_relation(..., &CteContext::empty())`.

- [ ] **Step 6: Verify nested WITH and Describe**

Run:

```bash
cargo test -p executor --test ctes nested_with_scopes_through_derived_tables_subqueries_and_describe
cargo test -p executor exec::tests::describe --lib
```

Expected: PASS.

- [ ] **Step 7: Commit nested scope and Describe**

```bash
git add crates/executor/src/cte.rs crates/executor/src/exec.rs crates/executor/src/setops.rs crates/executor/src/values.rs crates/executor/tests/ctes.rs
git commit -m "feat: describe CTE queries"
```

## Task 5: Router Range Collection With CTE Shadowing

**Files:**
- Modify: `crates/cluster/src/range/router.rs`

- [ ] **Step 1: Add failing router tests**

Append to the existing tests module in `crates/cluster/src/range/router.rs`:

```rust
#[tokio::test]
async fn cte_ranges_are_collected_and_shadowing_is_honored() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    let mut r = c.router().await;
    r.simple_query("CREATE TABLE a (id int4)").await.expect("create a");
    r.simple_query("CREATE TABLE b (id int4)").await.expect("create b");
    r.simple_query("INSERT INTO a VALUES (1)").await.expect("insert a");
    r.simple_query("INSERT INTO b VALUES (2)").await.expect("insert b");

    r.simple_query("WITH x AS (SELECT id FROM a) SELECT id FROM x")
        .await
        .expect("single-range CTE routes");

    let err = r
        .simple_query("WITH x AS (SELECT id FROM a) SELECT id FROM b UNION SELECT id FROM x")
        .await
        .expect_err("cross-range CTE query rejected");
    assert_eq!(err.code, "0A000");

    r.simple_query("WITH b AS (VALUES (9)) SELECT * FROM b")
        .await
        .expect("CTE named b shadows base table b and is range-neutral");
}
```

- [ ] **Step 2: Run router test and confirm expected failure**

Run:

```bash
cargo test -p cluster cte_ranges_are_collected_and_shadowing_is_honored --lib
```

Expected: FAIL until router range walking understands CTE scopes.

- [ ] **Step 3: Add router CTE scope**

In `crates/cluster/src/range/router.rs`, add a small range-collection context near the helper functions:

```rust
#[derive(Clone, Default)]
struct CteRangeCtx {
    names: Vec<String>,
}

impl CteRangeCtx {
    fn child(&self) -> Self {
        self.clone()
    }

    fn contains(&self, name: &str) -> bool {
        self.names.iter().rev().any(|n| n.eq_ignore_ascii_case(name))
    }

    fn push(&mut self, name: &str) {
        self.names.push(name.to_string());
    }
}
```

Add:

```rust
fn collect_with_ranges(
    router: &RangeRouter,
    with: &Option<pgparser::ast::WithClause>,
    ctx: &CteRangeCtx,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<CteRangeCtx, ExecError> {
    if with.as_ref().is_some_and(|w| w.recursive) {
        return Err(ExecError::Unsupported("WITH RECURSIVE is not supported".into()));
    }
    let mut next = ctx.child();
    if let Some(w) = with {
        for cte in &w.ctes {
            collect_query_expr_ranges(router, &cte.query, &next, out)?;
            next.push(&cte.name);
        }
    }
    Ok(next)
}

fn collect_query_expr_ranges(
    router: &RangeRouter,
    q: &pgparser::ast::QueryExpr,
    ctx: &CteRangeCtx,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    match q {
        pgparser::ast::QueryExpr::Select(s) => collect_select_ranges_with_ctes(router, s, ctx, out),
        pgparser::ast::QueryExpr::Values(_) => Ok(()),
        pgparser::ast::QueryExpr::SetOperation(q) => collect_set_query_ranges(router, q, ctx, out),
    }
}
```

Rename current `collect_select_ranges` to `collect_select_ranges_with_ctes` and begin it with:

```rust
let query_ctx = collect_with_ranges(router, &s.with, ctx, out)?;
```

Use `query_ctx` for walking `FROM`, expression subqueries, and derived tables.

In `collect_table_expr_ranges`, for a base table:

```rust
TableExpr::Table { name, .. } => {
    if !ctx.contains(name) {
        out.insert(router.range_of(name)?);
    }
}
```

Add wrappers so existing call sites can use an empty context:

```rust
fn collect_select_ranges(
    router: &RangeRouter,
    s: &pgparser::ast::SelectStmt,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    collect_select_ranges_with_ctes(router, s, &CteRangeCtx::default(), out)
}
```

Update set-operation collection to evaluate `q.with` first, then walk `q.body` with the resulting context.

- [ ] **Step 4: Verify router task**

Run:

```bash
cargo test -p cluster cte_ranges_are_collected_and_shadowing_is_honored --lib
cargo test -p cluster range::router::tests::a_cross_range_set_op_is_rejected_while_colocated_runs --lib
cargo test -p cluster range::router::tests::a_cross_range_subquery_is_rejected_while_colocated_runs --lib
```

Expected: PASS.

- [ ] **Step 5: Commit router support**

```bash
git add crates/cluster/src/range/router.rs
git commit -m "feat: route CTE queries by referenced ranges"
```

## Task 6: Conformance Corpus And Full Verification

**Files:**
- Add: `crates/conformance/corpus/ctes.sql`

- [ ] **Step 1: Add CTE conformance corpus**

Create `crates/conformance/corpus/ctes.sql`:

```sql
-- Non-recursive, read-only CTEs. Explicit ORDER BY keeps row order deterministic.

WITH c AS (SELECT 1 AS x)
SELECT x FROM c;

WITH a AS (VALUES (1), (2)), b AS (SELECT column1 + 10 AS y FROM a)
SELECT y FROM b ORDER BY y;

WITH c(x) AS (VALUES (3), (1), (2))
SELECT x FROM c ORDER BY x;

WITH u(x) AS (SELECT 1 UNION SELECT 2)
SELECT x FROM u ORDER BY x DESC;

CREATE TABLE cte_base (id int4);
INSERT INTO cte_base VALUES (9);
WITH cte_base AS (SELECT 1 AS id)
SELECT id FROM cte_base;

WITH outer_cte AS (VALUES (1))
SELECT * FROM (WITH outer_cte AS (VALUES (2)) SELECT * FROM outer_cte) AS d(x);

WITH c AS (VALUES (1))
SELECT EXISTS (WITH d AS (SELECT * FROM c) SELECT 1 FROM d);
```

- [ ] **Step 2: Run focused tests**

Run:

```bash
cargo test -p pgparser
cargo test -p executor --test ctes
cargo test -p executor --test values_query
cargo test -p executor --test set_operations
cargo test -p executor --test subqueries
cargo test -p cluster cte_ranges_are_collected_and_shadowing_is_honored --lib
```

Expected: PASS.

- [ ] **Step 3: Run parser oracle when feature is available**

Run:

```bash
cargo test -p pgparser --features oracle --test libpg_query_oracle
```

Expected: PASS. If the local environment lacks libpg_query build prerequisites, record the toolchain error and rely on CI for this oracle check.

- [ ] **Step 4: Run conformance for the new corpus**

Run the focused PostgreSQL 18 conformance check:

```bash
cargo build -p crabgresql -p conformance
tmp_corpus="$(mktemp -d)"
cp crates/conformance/corpus/ctes.sql "${tmp_corpus}/ctes.sql"
./scripts/oracle-up.sh
./target/debug/crabgresql --listen 127.0.0.1:54333 &
subject_pid=$!
for _ in $(seq 1 50); do
    if (: > /dev/tcp/127.0.0.1/54333) >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
./target/debug/conformance \
    --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
    --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" \
    --corpus "${tmp_corpus}" \
    --out /tmp/ctes-parity.json \
    --summary /tmp/ctes-parity.md
kill "${subject_pid}"
grep -q "Parity: 100.0%" /tmp/ctes-parity.md
```

Expected: the conformance command prints `parity: 100.0%`, and the final `grep` succeeds.

- [ ] **Step 5: Run workspace checks**

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
```

Expected: PASS.

- [ ] **Step 6: Commit conformance and final fixes**

```bash
git add crates/conformance/corpus/ctes.sql
git add crates/pgparser/src crates/executor/src crates/executor/tests crates/cluster/src/range/router.rs crates/pgparser/tests/libpg_query_oracle.rs
git commit -m "test: add CTE conformance coverage"
```

## Traceability

- Non-recursive read-only CTEs: Tasks 1, 2, 3, 6.
- Multiple CTEs and left-to-right visibility: Task 2.
- CTE shadowing of base tables: Tasks 2 and 5.
- `VALUES` and set-operation CTE bodies: Task 3.
- Nested `WITH` in derived tables/subqueries: Task 4.
- Describe support: Task 4.
- Router single-range invariant: Task 5.
- PostgreSQL conformance corpus: Task 6.
- No Stateright model: covered by the design doc rationale; this plan adds no distributed state transition.
