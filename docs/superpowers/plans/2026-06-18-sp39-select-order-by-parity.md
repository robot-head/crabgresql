# SP39 Select ORDER BY Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make ordinary `SELECT ... ORDER BY ...` match PostgreSQL for output positions, aliases/names, `SELECT DISTINCT`, and aggregate/grouped edge cases.

**Architecture:** Add a shared output-aware ORDER BY resolver in `executor::exec`. Row queries use it to decide whether each key reads a projected output expression or the source row; aggregate queries use the same resolver and either read the projected group output or evaluate a grouped expression. Error mapping is made PostgreSQL-exact for bad ordinals, non-integer ordinal constants, non-equivalent duplicate output names, and DISTINCT ordering violations.

**Tech Stack:** Rust 2024 workspace, `pgparser` AST, `executor` row/aggregate execution, `pgwire` result/error mapping, `cargo nextest`, PostgreSQL 18 conformance corpus.

---

## File Structure

- Modify `crates/executor/src/error.rs`: add `Syntax` (`42601`) and `AmbiguousOrderBy` (`42702`) error variants plus unit tests for mapping.
- Modify `crates/executor/src/exec.rs`: import `OrderItem`, add `SelectOrderKey` plus resolver helpers, update row-query ordering, and add row/resolver unit tests.
- Modify `crates/executor/src/agg.rs`: resolve ORDER BY keys before aggregate validation/finalization, and add aggregate ordering tests.
- Create `crates/executor/tests/ordering.rs`: over-the-wire integration tests for the user-visible ORDER BY parity surface. The target name `ordering` is UAC-safe.
- Create `crates/conformance/corpus/order_by.sql`: PostgreSQL 18 differential corpus for success and SQLSTATE cases.
- Modify `CLAUDE.md`: add the SP39 audit paragraph after implementation.

---

### Task 1: Error Surface

**Files:**
- Modify: `crates/executor/src/error.rs`

- [ ] **Step 1: Write failing error-mapping tests**

Append this test module to the bottom of `crates/executor/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_maps_to_42601() {
        let pg = ExecError::Syntax("non-integer constant in ORDER BY".into()).into_pg();
        assert_eq!(pg.code, "42601");
        assert_eq!(pg.message, "non-integer constant in ORDER BY");
    }

    #[test]
    fn ambiguous_order_by_maps_to_pg_message() {
        let pg = ExecError::AmbiguousOrderBy("x".into()).into_pg();
        assert_eq!(pg.code, "42702");
        assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
    }
}
```

- [ ] **Step 2: Run error tests to verify they fail**

Run:

```bash
cargo test -p executor error::tests --lib
```

Expected: FAIL with compile errors saying `ExecError::Syntax` and `ExecError::AmbiguousOrderBy` do not exist.

- [ ] **Step 3: Add the error variants**

In `crates/executor/src/error.rs`, add these variants after `SubqueryColumns`:

```rust
    /// PostgreSQL syntax/parse-analysis error surfaced by executor analysis
    /// (42601), used for SQL92 ORDER BY integer constants that cannot fit in
    /// a positional reference.
    Syntax(String),
    /// A bare ORDER BY output label matched more than one projected column
    /// (42702). PostgreSQL's message differs from generic column ambiguity.
    AmbiguousOrderBy(String),
```

In `ExecError::into_pg`, add these match arms before `SetOpColumnCount`:

```rust
            ExecError::Syntax(m) => PgError::error("42601", m),
            ExecError::AmbiguousOrderBy(n) => {
                PgError::error("42702", format!("ORDER BY \"{n}\" is ambiguous"))
            }
```

- [ ] **Step 4: Run error tests to verify they pass**

Run:

```bash
cargo test -p executor error::tests --lib
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/error.rs
git commit -m "feat: add ORDER BY parity error variants"
```

---

### Task 2: Shared SELECT ORDER BY Resolver

**Files:**
- Modify: `crates/executor/src/exec.rs`

- [ ] **Step 1: Write failing resolver tests**

Inside `#[cfg(test)] mod tests` in `crates/executor/src/exec.rs`, add these helper and tests near the other SELECT ordering tests:

```rust
    use crate::scope::{ColumnBinding, Scope};
    use pgparser::ast::{SelectStmt, Statement};

    fn order_scope() -> Scope {
        Scope {
            columns: vec![
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "a".into(),
                    ty: pgtypes::ColumnType::Int4,
                },
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "b".into(),
                    ty: pgtypes::ColumnType::Int4,
                },
            ],
        }
    }

    fn parsed_select(sql: &str) -> SelectStmt {
        match pgparser::parse(sql).expect("parse").pop().expect("one") {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn select_order_keys_resolve_positions_aliases_and_source_fallback() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let s = parsed_select(
            "SELECT a AS x, b FROM t ORDER BY 1, x DESC, t.b, b + 0",
        );
        let scope = order_scope();
        let (fields, out_exprs, _) =
            super::resolve_projection(&s.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&s.order_by, &fields, &out_exprs, false)
            .expect("order keys");

        assert!(matches!(keys[0], SelectOrderKey::Output(0)));
        assert!(matches!(keys[1], SelectOrderKey::Output(0)));
        assert!(matches!(keys[2], SelectOrderKey::SourceExpr(_)));
        assert!(matches!(keys[3], SelectOrderKey::SourceExpr(_)));
    }

    #[test]
    fn select_order_keys_report_pg_errors() {
        use super::resolve_select_order_keys;

        let scope = order_scope();

        let bad_pos = parsed_select("SELECT a FROM t ORDER BY 0");
        let (fields, out_exprs, _) =
            super::resolve_projection(&bad_pos.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&bad_pos.order_by, &fields, &out_exprs, false)
            .expect_err("bad position");
        assert_eq!(err.into_pg().code, "42P10");

        let overflow = parsed_select("SELECT a FROM t ORDER BY 999999999999999999999999999");
        let (fields, out_exprs, _) =
            super::resolve_projection(&overflow.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&overflow.order_by, &fields, &out_exprs, false)
            .expect_err("overflow");
        assert_eq!(err.into_pg().code, "42601");

        let duplicate = parsed_select("SELECT a AS x, b AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&duplicate.order_by, &fields, &out_exprs, false)
            .expect_err("ambiguous output label");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42702");
        assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
    }

    #[test]
    fn select_distinct_order_keys_require_output_columns() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let scope = order_scope();

        let by_alias = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_alias.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&by_alias.order_by, &fields, &out_exprs, true)
            .expect("alias is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let by_select_expr = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_select_expr.projection, &scope).expect("projection");
        let keys =
            resolve_select_order_keys(&by_select_expr.order_by, &fields, &out_exprs, true)
                .expect("select-list expression is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let source_only = parsed_select("SELECT DISTINCT a FROM t ORDER BY b");
        let (fields, out_exprs, _) =
            super::resolve_projection(&source_only.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&source_only.order_by, &fields, &out_exprs, true)
            .expect_err("source-only key");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42P10");
        assert_eq!(
            pg.message,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
        );
    }
```

- [ ] **Step 2: Run resolver tests to verify they fail**

Run:

```bash
cargo test -p executor exec::tests::select_order_keys --lib
```

Expected: FAIL with compile errors for missing `SelectOrderKey` and `resolve_select_order_keys`.

- [ ] **Step 3: Import `OrderItem`**

Change the AST import at the top of `crates/executor/src/exec.rs` to:

```rust
use pgparser::ast::{Expr, OrderItem, SelectItem, SelectStmt, Statement};
```

- [ ] **Step 4: Add resolver types and helpers**

Replace the old `distinct_order_indices` helper with this code:

```rust
/// One resolved ORDER BY key for a plain SELECT. SQL92-style output references
/// (`ORDER BY 1`, `ORDER BY alias`) are represented as output indices; all other
/// expressions are evaluated against the source/group scope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectOrderKey {
    Output(usize),
    SourceExpr(Expr),
}

/// Resolve SELECT ORDER BY items using PostgreSQL's SQL92 rules:
/// integer constant -> output ordinal, bare output label -> output column, and
/// everything else -> source expression unless `require_output` is true.
pub(crate) fn resolve_select_order_keys(
    order_by: &[OrderItem],
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<Vec<SelectOrderKey>, ExecError> {
    order_by
        .iter()
        .map(|item| resolve_select_order_key(item, fields, out_exprs, require_output))
        .collect()
}

fn resolve_select_order_key(
    item: &OrderItem,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<SelectOrderKey, ExecError> {
    if let Expr::IntLiteral(s) = &item.expr {
        let pos: usize = s
            .parse()
            .map_err(|_| ExecError::Syntax("non-integer constant in ORDER BY".into()))?;
        if pos == 0 || pos > fields.len() {
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {pos} is not in select list"
            )));
        }
        return Ok(SelectOrderKey::Output(pos - 1));
    }

    if let Expr::Column { table: None, name } = &item.expr {
        if let Some(i) = output_label_index(fields, name)? {
            return Ok(SelectOrderKey::Output(i));
        }
    }

    if require_output {
        if let Some(i) = out_exprs.iter().position(|e| e == &item.expr) {
            return Ok(SelectOrderKey::Output(i));
        }
        return Err(ExecError::InvalidColumnReference(
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
        ));
    }

    Ok(SelectOrderKey::SourceExpr(item.expr.clone()))
}

fn output_label_index(
    fields: &[FieldDescription],
    name: &str,
) -> Result<Option<usize>, ExecError> {
    let mut found = None;
    for (i, f) in fields.iter().enumerate() {
        if f.name == name {
            if found.is_some() {
                return Err(ExecError::AmbiguousOrderBy(name.to_string()));
            }
            found = Some(i);
        }
    }
    Ok(found)
}
```

- [ ] **Step 5: Run resolver tests to verify they pass**

Run:

```bash
cargo test -p executor exec::tests::select_order_keys --lib
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/exec.rs
git commit -m "feat: resolve SELECT ORDER BY output keys"
```

---

### Task 3: Row SELECT ORDER BY Execution

**Files:**
- Modify: `crates/executor/src/exec.rs`

- [ ] **Step 1: Write failing row-query tests**

Inside `#[cfg(test)] mod tests` in `crates/executor/src/exec.rs`, add:

```rust
    #[tokio::test]
    async fn plain_select_order_by_position_and_alias_use_output() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c')").await;

        let r = &run(&engine, "SELECT name FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("c".into()), Some("b".into()), Some("a".into())]);

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("1".into()), Some("2".into()), Some("3".into())]);

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY t.b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into()), Some("3".into())]);

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b + 0").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into()), Some("3".into())]);
    }

    #[tokio::test]
    async fn plain_select_order_by_pg_error_surface() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(2,10)").await;

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 0")
            .await
            .expect_err("position zero");
        assert_eq!(err.code, "42P10");

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 999999999999999999999999999")
            .await
            .expect_err("overflow position");
        assert_eq!(err.code, "42601");

        let err = engine
            .connect()
            .simple_query("SELECT a AS x, b AS x FROM t ORDER BY x")
            .await
            .expect_err("ambiguous output label");
        assert_eq!(err.code, "42702");
    }

    #[tokio::test]
    async fn distinct_select_order_by_uses_output_only() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(1,10),(2,30)").await;

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY x DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let err = engine
            .connect()
            .simple_query("SELECT DISTINCT a FROM t ORDER BY b")
            .await
            .expect_err("source-only distinct key");
        assert_eq!(err.code, "42P10");
    }
```

- [ ] **Step 2: Run row-query tests to verify they fail**

Run:

```bash
cargo test -p executor plain_select_order_by --lib
cargo test -p executor distinct_select_order_by --lib
```

Expected: FAIL. The first test should expose current constant-sort/source-column behavior; the DISTINCT error should still be `0A000`.

- [ ] **Step 3: Pass output fields into `project_rows_ordered` from `select_to_relation`**

In `select_to_relation`, change:

```rust
        project_rows_ordered(s, &relation.scope, &out_exprs, kept, ctx)?
```

to:

```rust
        project_rows_ordered(s, &relation.scope, &fields, &out_exprs, kept, ctx)?
```

- [ ] **Step 4: Pass output fields into `project_rows_ordered` from `project_order_limit`**

In `project_order_limit`, change:

```rust
    let rows = project_rows_ordered(s, scope, &out_exprs, kept, ctx)?;
```

to:

```rust
    let rows = project_rows_ordered(s, scope, &fields, &out_exprs, kept, ctx)?;
```

- [ ] **Step 5: Replace `project_rows_ordered` with output-aware ordering**

Replace the full `project_rows_ordered` function in `crates/executor/src/exec.rs` with:

```rust
fn project_rows_ordered(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    mut kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let order_keys = resolve_select_order_keys(&s.order_by, fields, out_exprs, s.distinct)?;

    // SP39: SELECT DISTINCT projects FIRST, dedups output rows, then ORDER BY
    // sorts the deduped output. PostgreSQL requires every sort key to refer to
    // the select-list output (ordinal, alias/name, or the exact select expression).
    if s.distinct {
        let mut projected = project_rows(out_exprs, scope, &kept, ctx)?;
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|r| seen.insert(r.clone()));
        if !s.order_by.is_empty() {
            let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = projected
                .into_iter()
                .map(|r| {
                    let keys = order_keys
                        .iter()
                        .map(|k| match k {
                            SelectOrderKey::Output(i) => r[*i].clone(),
                            SelectOrderKey::SourceExpr(_) => {
                                unreachable!("DISTINCT order keys are output-only")
                            }
                        })
                        .collect();
                    (keys, r)
                })
                .collect();
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
            projected = keyed.into_iter().map(|(_, r)| r).collect();
        }
        apply_offset_limit(&mut projected, s.offset, s.limit);
        return Ok(projected);
    }

    // Non-DISTINCT keeps the existing source-row ordering shape so non-projected
    // source expressions still work, but output ordinals/labels evaluate the
    // corresponding projection expression for each source row.
    if !s.order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        for row in kept {
            let mut keys = Vec::with_capacity(order_keys.len());
            for key in &order_keys {
                keys.push(match key {
                    SelectOrderKey::Output(i) => {
                        crate::eval::eval(&out_exprs[*i], scope, &row, ctx)?
                    }
                    SelectOrderKey::SourceExpr(expr) => {
                        crate::eval::eval(expr, scope, &row, ctx)?
                    }
                });
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        kept = keyed.into_iter().map(|(_, row)| row).collect();
    }
    apply_offset_limit(&mut kept, s.offset, s.limit);
    project_rows(out_exprs, scope, &kept, ctx)
}
```

- [ ] **Step 6: Run row-query tests to verify they pass**

Run:

```bash
cargo test -p executor plain_select_order_by --lib
cargo test -p executor distinct_select_order_by --lib
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/exec.rs
git commit -m "feat: apply ORDER BY parity to row selects"
```

---

### Task 4: Aggregate SELECT ORDER BY Execution

**Files:**
- Modify: `crates/executor/src/agg.rs`

- [ ] **Step 1: Write failing aggregate tests**

Inside `#[cfg(test)] mod tests` in `crates/executor/src/agg.rs`, add:

```rust
    #[test]
    fn aggregate_order_by_position_and_alias_use_projected_output() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(5)]),
            r(&[Datum::Int4(2), Datum::Int4(30)]),
        ];

        assert_eq!(
            agg(
                "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY 2 DESC",
                Some(&t),
                rows.clone()
            )
            .expect("ordinal aggregate order"),
            vec![vec![int(2), int(35)], vec![int(1), int(10)]]
        );

        assert_eq!(
            agg(
                "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY total DESC",
                Some(&t),
                rows.clone()
            )
            .expect("alias aggregate order"),
            vec![vec![int(2), int(35)], vec![int(1), int(10)]]
        );

        assert_eq!(
            agg(
                "SELECT k AS g, sum(v) FROM t GROUP BY k ORDER BY g DESC",
                Some(&t),
                rows
            )
            .expect("group alias order"),
            vec![vec![int(2), int(35)], vec![int(1), int(10)]]
        );
    }

    #[test]
    fn aggregate_distinct_order_by_requires_output() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(10)]),
            r(&[Datum::Int4(3), Datum::Int4(20)]),
        ];

        assert_eq!(
            agg(
                "SELECT DISTINCT sum(v) AS total FROM t GROUP BY k ORDER BY total DESC",
                Some(&t),
                rows.clone()
            )
            .expect("distinct aggregate alias order"),
            vec![vec![int(20)], vec![int(10)]]
        );

        let err = agg(
            "SELECT DISTINCT sum(v) FROM t GROUP BY k ORDER BY k",
            Some(&t),
            rows,
        )
        .expect_err("source-only DISTINCT aggregate key");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42P10");
        assert_eq!(
            pg.message,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
        );
    }
```

- [ ] **Step 2: Run aggregate tests to verify they fail**

Run:

```bash
cargo test -p executor aggregate_order_by --lib
cargo test -p executor aggregate_distinct_order_by --lib
```

Expected: FAIL. Alias/ordinal keys should currently be validated/evaluated as source/group expressions.

- [ ] **Step 3: Resolve aggregate ORDER BY keys before validation**

In `aggregate_rows`, replace:

```rust
    let (_fields, out_exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
```

with:

```rust
    let (fields, out_exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    let order_keys =
        crate::exec::resolve_select_order_keys(&s.order_by, &fields, &out_exprs, s.distinct)?;
```

- [ ] **Step 4: Validate only source/group ORDER BY expressions**

Replace this validation loop:

```rust
    for e in out_exprs
        .iter()
        .chain(s.having.iter())
        .chain(s.order_by.iter().map(|o| &o.expr))
    {
        collect_specs(e, scope, &mut specs)?;
        validate_grouped(e, &s.group_by)?;
    }
```

with:

```rust
    for e in out_exprs
        .iter()
        .chain(s.having.iter())
        .chain(order_keys.iter().filter_map(|k| match k {
            crate::exec::SelectOrderKey::Output(_) => None,
            crate::exec::SelectOrderKey::SourceExpr(expr) => Some(expr),
        }))
    {
        collect_specs(e, scope, &mut specs)?;
        validate_grouped(e, &s.group_by)?;
    }
```

- [ ] **Step 5: Compute projected output before order keys per group**

In the "Finalize each group" loop, replace:

```rust
        let mut order_keys = Vec::with_capacity(s.order_by.len());
        for o in &s.order_by {
            order_keys.push(eval_grouped(
                &o.expr,
                scope,
                &s.group_by,
                key,
                &specs,
                &results,
                ctx,
            )?);
        }
        let mut projected = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            projected.push(eval_grouped(
                e,
                scope,
                &s.group_by,
                key,
                &specs,
                &results,
                ctx,
            )?);
        }
        out.push((order_keys, projected));
```

with:

```rust
        let mut projected = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            projected.push(eval_grouped(
                e,
                scope,
                &s.group_by,
                key,
                &specs,
                &results,
                ctx,
            )?);
        }

        let mut sort_keys = Vec::with_capacity(order_keys.len());
        for order_key in &order_keys {
            sort_keys.push(match order_key {
                crate::exec::SelectOrderKey::Output(i) => projected[*i].clone(),
                crate::exec::SelectOrderKey::SourceExpr(expr) => eval_grouped(
                    expr,
                    scope,
                    &s.group_by,
                    key,
                    &specs,
                    &results,
                    ctx,
                )?,
            });
        }
        out.push((sort_keys, projected));
```

- [ ] **Step 6: Run aggregate tests to verify they pass**

Run:

```bash
cargo test -p executor aggregate_order_by --lib
cargo test -p executor aggregate_distinct_order_by --lib
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/agg.rs
git commit -m "feat: apply ORDER BY parity to aggregates"
```

---

### Task 5: Wire Test, Conformance Corpus, And Audit Docs

**Files:**
- Create: `crates/executor/tests/ordering.rs`
- Create: `crates/conformance/corpus/order_by.sql`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the wire-level integration test**

Create `crates/executor/tests/ordering.rs` with:

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

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn plain_select_order_by_position_alias_and_source_fallback() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (a int4, b int4, name text);
             INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c');",
        )
        .await
        .expect("seed");

    let rows = client
        .query("SELECT name FROM t ORDER BY 1 DESC", &[])
        .await
        .expect("position");
    assert_eq!(rows.iter().map(|r| r.get::<_, &str>(0)).collect::<Vec<_>>(), vec!["c", "b", "a"]);

    let rows = client
        .query("SELECT a AS b FROM t ORDER BY b", &[])
        .await
        .expect("alias");
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), vec![1, 2, 3]);

    let rows = client
        .query("SELECT a AS b FROM t ORDER BY t.b", &[])
        .await
        .expect("qualified source");
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), vec![2, 1, 3]);
}

#[tokio::test]
async fn distinct_and_aggregate_order_by_parity() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (a int4, b int4);
             INSERT INTO t VALUES (1,20),(1,10),(2,30),(3,5);",
        )
        .await
        .expect("seed");

    let rows = client
        .query("SELECT DISTINCT a AS x FROM t ORDER BY x DESC", &[])
        .await
        .expect("distinct alias");
    assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), vec![3, 2, 1]);

    let rows = client
        .query("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY c DESC, a", &[])
        .await
        .expect("aggregate alias");
    let got = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1)))
        .collect::<Vec<_>>();
    assert_eq!(got, vec![(1, 2), (2, 1), (3, 1)]);

    let rows = client
        .query("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY 2 DESC, 1", &[])
        .await
        .expect("aggregate ordinal");
    let got = rows
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1)))
        .collect::<Vec<_>>();
    assert_eq!(got, vec![(1, 2), (2, 1), (3, 1)]);
}

#[tokio::test]
async fn order_by_pg_error_surface() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (a int4, b int4);
             INSERT INTO t VALUES (1,20),(2,10);",
        )
        .await
        .expect("seed");

    let err = client
        .batch_execute("SELECT a FROM t ORDER BY 0")
        .await
        .expect_err("bad ordinal");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P10");

    let err = client
        .batch_execute("SELECT a FROM t ORDER BY 999999999999999999999999999")
        .await
        .expect_err("non-integer ordinal");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42601");

    let err = client
        .batch_execute("SELECT a AS x, b AS x FROM t ORDER BY x")
        .await
        .expect_err("ambiguous output name");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42702");

    let err = client
        .batch_execute("SELECT DISTINCT a FROM t ORDER BY b")
        .await
        .expect_err("distinct source-only key");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P10");
}
```

- [ ] **Step 2: Run the new integration test**

Run:

```bash
cargo nextest run -p executor --test ordering
```

Expected: PASS.

- [ ] **Step 3: Add the conformance corpus**

Create `crates/conformance/corpus/order_by.sql`:

```sql
-- SP39: plain SELECT ORDER BY parity, diffed against PostgreSQL 18.
CREATE TABLE ob (a int4, b int4, name text);
INSERT INTO ob VALUES (1,20,'a'),(2,10,'b'),(3,30,'c'),(1,40,'d');

SELECT name FROM ob ORDER BY 1 DESC;
SELECT a AS b FROM ob ORDER BY b;
SELECT a AS b FROM ob ORDER BY ob.b;
SELECT a AS b FROM ob ORDER BY b + 0;
SELECT DISTINCT a AS x FROM ob ORDER BY x DESC;
SELECT DISTINCT a AS x FROM ob ORDER BY 1 DESC;
SELECT a, count(*) AS c FROM ob GROUP BY a ORDER BY c DESC, a;
SELECT a, count(*) AS c FROM ob GROUP BY a ORDER BY 2 DESC, 1;
SELECT a FROM ob ORDER BY 0;
SELECT a FROM ob ORDER BY 9;
SELECT a FROM ob ORDER BY 999999999999999999999999999;
SELECT a AS x, b AS x FROM ob ORDER BY x;
SELECT a AS b, b FROM ob ORDER BY b;
SELECT DISTINCT a FROM ob ORDER BY b;
```

- [ ] **Step 4: Run the focused PostgreSQL 18 conformance check**

Run:

```bash
tmp_corpus="$(mktemp -d)"
cp crates/conformance/corpus/order_by.sql "${tmp_corpus}/order_by.sql"
./scripts/oracle-up.sh
cargo build -p crabgresql
./target/debug/crabgresql --listen 127.0.0.1:54333 &
subject_pid=$!
trap 'kill "${subject_pid}" 2>/dev/null || true; docker rm -f crabgresql-oracle >/dev/null 2>&1 || true; rm -rf "${tmp_corpus}"' EXIT
for _ in $(seq 1 50); do
    if (: > /dev/tcp/127.0.0.1/54333) >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done
cargo run -p conformance -- \
    --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
    --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" \
    --corpus "${tmp_corpus}" \
    --out target/order_by-parity.json \
    --summary target/order_by-parity.md
grep "Parity: 100.0%" target/order_by-parity.md
```

Expected: the conformance command prints `parity: 100.0%`, and the final `grep` succeeds.

- [ ] **Step 5: Add the CLAUDE.md audit paragraph**

Append this paragraph near the existing SQL breadth entries in `CLAUDE.md`:

```markdown
**SP39 (2026-06-18):** breadth query-expression runway — **plain `SELECT ORDER BY` PostgreSQL parity**. Closes SP38's documented deferral where positional `ORDER BY` worked only for set-operation outputs: ordinary row and aggregate SELECTs now resolve SQL92-style order keys (`ORDER BY 1`, output aliases/names, duplicate-output-name ambiguity only for non-equivalent expressions) before falling back to source expressions, while preserving `SELECT a FROM t ORDER BY b`. `SELECT DISTINCT` now enforces PostgreSQL's output-only ordering rule with `42P10` while accepting equivalent output expressions such as qualified `t.a`, bad positions use `42P10`, non-equivalent duplicate output labels use `42702` with the PG `ORDER BY "x" is ambiguous` message, and an oversized integer order constant uses `42601` ("non-integer constant in ORDER BY"). No parser/storage/wire-format change; no Stateright model for the same pure-data/single-engine reason as SP27-SP38. Proven by `executor::exec` row/resolver unit tests, `executor::agg` grouped/aggregate tests, new over-the-wire `executor::ordering` integration test (UAC-safe target name), and `conformance/corpus/order_by.sql` diffed against PostgreSQL 18. Deferred remains: `VALUES`, nested set ops in subquery/derived positions, CTEs, windows, `NULLS FIRST/LAST`, collations, ordered-set aggregates, and planner optimizations.
```

- [ ] **Step 6: Run the UAC target-name guard**

Run:

```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```

Expected: no output and exit code 1 from `grep`.

- [ ] **Step 7: Run focused executor tests**

Run:

```bash
cargo nextest run -p executor
```

Expected: PASS.

- [ ] **Step 8: Run parser and conformance split sanity tests**

Run:

```bash
cargo test -p pgparser
cargo test -p conformance
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/executor/tests/ordering.rs crates/conformance/corpus/order_by.sql CLAUDE.md
git commit -m "test: cover SELECT ORDER BY parity"
```

---

### Task 6: Full Verification

**Files:**
- No code changes.

- [ ] **Step 1: Run workspace tests**

Run:

```bash
cargo nextest run --workspace
```

Expected: PASS.

- [ ] **Step 2: Run doctests**

Run:

```bash
cargo test --workspace --doc
```

Expected: PASS.

- [ ] **Step 3: Check final status**

Run:

```bash
git status --short --branch
```

Expected: clean worktree on the current detached `HEAD` or feature branch.
