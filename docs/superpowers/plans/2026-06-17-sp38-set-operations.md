# SP38 — Set Operations (UNION / INTERSECT / EXCEPT) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add top-level `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]` to the SQL surface, with PostgreSQL precedence, NULL-equal duplicate semantics, cross-branch type unification, and result-level `ORDER BY` / `LIMIT` / `OFFSET`.

**Architecture:** A new `Statement::SetOperation(SetQuery)` variant with a recursive `SetExpr` tree whose leaves are the existing `SelectStmt` — the single-`SELECT` path is untouched. The executor evaluates each leaf via the existing `select_to_relation`, then folds the tree with a pure combine (reusing `Datum`'s grouping `Eq`/`Hash` for NULL-equal dedup) and applies a query-level ORDER BY/LIMIT/OFFSET over the combined output. Cross-range set ops are rejected `0A000` at the router. No Stateright model (pure-data fold; same carve-out as SP27–SP37).

**Tech Stack:** Rust 2024, `pgparser` (hand-written lexer + Pratt parser), `executor` (`Scope`/`Relation`/`select_to_relation`), `pgtypes` (`cast`/`unify`), `cluster` router, `tokio-postgres` wire tests, `conformance` corpus diffed vs PostgreSQL.

**Spec:** `docs/superpowers/specs/2026-06-17-crabgresql-sp38-set-operations-design.md`

---

## Conventions for every task

- Run the full crate test with nextest: `cargo nextest run -p <crate>`. Doctests (if any): `cargo test -p <crate> --doc`.
- Before each commit run `cargo fmt` and `cargo clippy -p <crate> --all-targets -- -D warnings` (the implementer's per-task verify must include `cargo fmt` — see the memory note "subagent plans include cargo fmt").
- Worktree-absolute paths: this plan executes in `C:\Users\Matt Stone\git\crabgresql\.claude\worktrees\youthful-sammet-eb9a90`. Stay on branch `claude/youthful-sammet-eb9a90`.

---

## Task 1: AST nodes + set-op keywords

**Files:**
- Modify: `crates/pgparser/src/token.rs` (Keyword enum + `from_word` + round-trip test list)
- Modify: `crates/pgparser/src/ast.rs` (new `Statement::SetOperation`, `SetQuery`, `SetExpr`, `SetOp`)

- [ ] **Step 1: Add the three keywords to the enum.** In `crates/pgparser/src/token.rs`, after `Some,` (the SP34 block, ~line 111) add:

```rust
    // SP38: set operations
    Union,
    Intersect,
    Except,
```

- [ ] **Step 2: Add their `from_word` arms.** After `"some" => Keyword::Some,` (~line 189):

```rust
            // SP38: set operations
            "union" => Keyword::Union,
            "intersect" => Keyword::Intersect,
            "except" => Keyword::Except,
```

- [ ] **Step 3: Add them to the round-trip test list.** In `token.rs` `mod tests`, after `("some", Keyword::Some),` (~line 260):

```rust
            ("union", Keyword::Union),
            ("intersect", Keyword::Intersect),
            ("except", Keyword::Except),
```

- [ ] **Step 4: Add the AST nodes.** In `crates/pgparser/src/ast.rs`, add a variant to `enum Statement` (after `Reset { name: String }`, ~line 47):

```rust
    /// SP38: a set-operation query — `<select> UNION|INTERSECT|EXCEPT [ALL] <select> …`
    /// with a result-level ORDER BY / LIMIT / OFFSET. A plain single SELECT stays
    /// `Statement::Select`; only a query containing a set-op keyword lands here.
    SetOperation(SetQuery),
```

Then, after the `SelectStmt` struct (~line 97), add:

```rust
/// SP38: a complete set-operation query expression. `body` is the operator tree
/// (leaves are `SelectStmt`); `order_by` / `limit` / `offset` apply to the COMBINED
/// result (PostgreSQL allows them only at the top of the query, or inside parens).
#[derive(Debug, Clone, PartialEq)]
pub struct SetQuery {
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// SP38: a node in the set-operation tree. A `Select` leaf is one query block; a
/// `SetOp` combines two sub-trees. INTERSECT binds tighter than UNION/EXCEPT;
/// UNION/EXCEPT are left-associative (the parser encodes this in the tree shape).
#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<SelectStmt>),
    SetOp {
        op: SetOp,
        /// `true` for `… ALL …` (keep duplicates); `false` for the default
        /// (duplicate-eliminating) form.
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}
```

- [ ] **Step 5: Verify it compiles and the keyword test passes.** Run: `cargo nextest run -p pgparser token::`
  Expected: PASS (round-trip test includes the three new keywords). The new `Statement::SetOperation` arm will cause exhaustive `match Statement` sites in OTHER crates to fail to compile — those are fixed in Tasks 2, 6, 7, 8. Within `pgparser`, confirm the crate still builds: `cargo build -p pgparser` → PASS.

- [ ] **Step 6: Commit.**

```bash
git add crates/pgparser/src/token.rs crates/pgparser/src/ast.rs
git commit -m "SP38: AST nodes + UNION/INTERSECT/EXCEPT keywords"
```

---

## Task 2: Parser — set-op grammar (precedence climbing) + statement dispatch

**Files:**
- Modify: `crates/pgparser/src/parser.rs` (refactor `select_inner` → `select_core` + tail; add `set_expr`, `query_stmt`, `parse_set_tail`; reroute `statement()`)
- Test: `crates/pgparser/src/parser.rs` (`mod tests`)

The current `select_inner` (parser.rs:1049-1177) parses projection→HAVING then a trailing tail (ORDER BY / LIMIT / OFFSET / locking) inline, then builds the `SelectStmt`. We split it so the tail can be owned by the *whole* set-op query, while keeping `select_inner` byte-for-byte equivalent for its recursive callers (derived tables, subquery expressions).

- [ ] **Step 1: Write failing parser tests.** Add to `crates/pgparser/src/parser.rs` `mod tests`:

```rust
    #[test]
    fn parses_union_all_and_precedence() {
        use crate::ast::{SetExpr, SetOp, Statement};
        // INTERSECT binds tighter than UNION: A UNION B INTERSECT C => A UNION (B INTERSECT C)
        let s = crate::parse("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3").unwrap();
        let Statement::SetOperation(q) = &s[0] else { panic!("expected set op, got {:?}", s[0]) };
        let SetExpr::SetOp { op, all, right, .. } = &q.body else { panic!("expected top SetOp") };
        assert_eq!(*op, SetOp::Union);
        assert!(!*all);
        assert!(matches!(&**right, SetExpr::SetOp { op: SetOp::Intersect, .. }));

        // UNION ALL sets `all`; left-associativity: A UNION B UNION C => (A UNION B) UNION C
        let s = crate::parse("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3").unwrap();
        let Statement::SetOperation(q) = &s[0] else { panic!() };
        let SetExpr::SetOp { all, left, .. } = &q.body else { panic!() };
        assert!(*all);
        assert!(matches!(&**left, SetExpr::SetOp { op: SetOp::Union, .. }));
    }

    #[test]
    fn union_order_by_limit_bind_to_whole_query() {
        use crate::ast::Statement;
        let s = crate::parse("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5 OFFSET 1").unwrap();
        let Statement::SetOperation(q) = &s[0] else { panic!() };
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn parenthesized_branch_keeps_its_own_order_limit() {
        use crate::ast::{SetExpr, Statement};
        let s = crate::parse("(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2").unwrap();
        let Statement::SetOperation(q) = &s[0] else { panic!() };
        let SetExpr::SetOp { left, .. } = &q.body else { panic!() };
        let SetExpr::Select(b) = &**left else { panic!("left branch is a SELECT leaf") };
        assert_eq!(b.limit, Some(1));
        assert_eq!(b.order_by.len(), 1);
    }

    #[test]
    fn plain_select_is_unchanged() {
        use crate::ast::Statement;
        // No set-op keyword => still Statement::Select, tail on the struct.
        let s = crate::parse("SELECT a FROM t ORDER BY a LIMIT 3").unwrap();
        let Statement::Select(sel) = &s[0] else { panic!("plain select must stay Statement::Select") };
        assert_eq!(sel.limit, Some(3));
        assert_eq!(sel.order_by.len(), 1);
    }

    #[test]
    fn for_update_with_set_op_is_rejected() {
        assert!(crate::parse("SELECT 1 UNION SELECT 2 FOR UPDATE").is_err());
    }

    #[test]
    fn order_by_on_parenthesized_set_op_subtree_is_rejected() {
        // Deferred non-goal: a tail on a parenthesized MULTI-branch subtree.
        assert!(crate::parse("(SELECT 1 UNION SELECT 2 ORDER BY 1) UNION SELECT 3").is_err());
    }
```

- [ ] **Step 2: Run them to confirm they fail.** Run: `cargo nextest run -p pgparser parses_union_all_and_precedence union_order_by parenthesized_branch plain_select_is_unchanged for_update_with_set_op order_by_on_parenthesized`
  Expected: FAIL (compile error — `parse` does not yet produce `SetOperation`).

- [ ] **Step 3: Extract `select_core` + a tail parser.** Replace the body of `select_inner` (parser.rs:1049). Keep `select_inner` as a thin wrapper so recursive callers are unchanged:

```rust
    /// Parse a single SELECT body INCLUDING its trailing ORDER BY / LIMIT / OFFSET /
    /// locking. Unchanged behavior for the recursive callers (derived tables,
    /// subquery expressions) — they still get a fully-populated `SelectStmt`.
    fn select_inner(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        let mut s = self.select_core()?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        s.order_by = order_by;
        s.limit = limit;
        s.offset = offset;
        s.locking = self.parse_locking()?;
        Ok(s)
    }
```

Now add `select_core` — the existing projection→HAVING logic, leaving the tail fields empty. Move lines 1050-1120 (projection through `having`) into it, and build the struct with empty tail:

```rust
    /// Parse projection → HAVING. Leaves order_by / limit / offset / locking empty;
    /// the caller (single SELECT or set-op query) owns the tail.
    fn select_core(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        use crate::ast::{SelectItem, SelectStmt};
        self.expect(&Token::Keyword(Keyword::Select))?;
        let distinct = self.eat_keyword(Keyword::Distinct);
        if !distinct {
            self.eat_keyword(Keyword::All);
        }
        // ... (UNCHANGED: the existing projection loop, FROM, WHERE, GROUP BY, HAVING
        //      from the current select_inner, lines 1057-1120) ...
        Ok(SelectStmt {
            projection,
            from,
            filter,
            distinct,
            group_by,
            having,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            locking: None,
        })
    }
```

Then extract the ORDER BY / LIMIT / OFFSET parsing (current lines 1121-1150) into `parse_set_tail`, and the FOR UPDATE/SHARE parsing (current lines 1151-1164) into `parse_locking`:

```rust
    /// Parse an optional `ORDER BY …`, then `LIMIT`/`OFFSET` in either order.
    fn parse_set_tail(
        &mut self,
    ) -> Result<(Vec<crate::ast::OrderItem>, Option<i64>, Option<i64>), ParseError> {
        use crate::ast::OrderItem;
        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                let expr = self.expr(0)?;
                let asc = if self.eat_keyword(Keyword::Desc) {
                    false
                } else {
                    self.eat_keyword(Keyword::Asc);
                    true
                };
                order_by.push(OrderItem { expr, asc });
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let mut limit = None;
        let mut offset = None;
        loop {
            if limit.is_none() && self.eat_keyword(Keyword::Limit) {
                limit = Some(self.expect_int_count("LIMIT")?);
            } else if offset.is_none() && self.eat_keyword(Keyword::Offset) {
                offset = Some(self.expect_int_count("OFFSET")?);
            } else {
                break;
            }
        }
        Ok((order_by, limit, offset))
    }

    /// Parse an optional `FOR UPDATE` / `FOR SHARE` row-locking clause.
    fn parse_locking(&mut self) -> Result<Option<crate::ast::RowLockStrength>, ParseError> {
        if self.eat_keyword(Keyword::For) {
            if self.eat_keyword(Keyword::Update) {
                Ok(Some(crate::ast::RowLockStrength::ForUpdate))
            } else if self.eat_keyword(Keyword::Share) {
                Ok(Some(crate::ast::RowLockStrength::ForShare))
            } else {
                Err(ParseError::new("expected UPDATE or SHARE after FOR", self.peek_pos()))
            }
        } else {
            Ok(None)
        }
    }
```

- [ ] **Step 4: Add the set-op tree parser + top-level `query_stmt`.** Add near `select_inner`:

```rust
    /// SP38: parse a full set-operation query (the statement entry for SELECT / `(`).
    /// `set_expr(0)` builds the operator tree; the trailing tail binds to the whole
    /// query. A lone Select (no set-op) collapses back to `Statement::Select` so the
    /// single-SELECT shape — including FOR UPDATE — is byte-for-byte unchanged.
    fn query_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{SetExpr, SetQuery, Statement};
        let body = self.set_expr(0)?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        match body {
            SetExpr::Select(mut s) => {
                // A single SELECT: attach the tail + locking to the struct.
                s.order_by = order_by;
                s.limit = limit;
                s.offset = offset;
                s.locking = self.parse_locking()?;
                Ok(Statement::Select(*s))
            }
            body => {
                // FOR UPDATE is not allowed with a set operation (PG-faithful).
                if matches!(self.peek(), Token::Keyword(Keyword::For)) {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
                        self.peek_pos(),
                    ));
                }
                Ok(Statement::SetOperation(SetQuery { body, order_by, limit, offset }))
            }
        }
    }

    /// Precedence-climbing set-op tree. INTERSECT = 2, UNION/EXCEPT = 1; all
    /// left-associative (recurse for the RHS at `prec + 1`).
    fn set_expr(&mut self, min_prec: u8) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{SetExpr, SetOp};
        let mut left = self.set_primary()?;
        loop {
            let (op, prec) = match self.peek() {
                Token::Keyword(Keyword::Union) => (SetOp::Union, 1u8),
                Token::Keyword(Keyword::Except) => (SetOp::Except, 1u8),
                Token::Keyword(Keyword::Intersect) => (SetOp::Intersect, 2u8),
                _ => break,
            };
            if prec < min_prec {
                break;
            }
            self.bump(); // the operator keyword
            let all = self.eat_keyword(Keyword::All);
            if !all {
                self.eat_keyword(Keyword::Distinct); // explicit default modifier
            }
            let right = self.set_expr(prec + 1)?;
            left = SetExpr::SetOp { op, all, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    /// A set-op primary: a parenthesized sub-query (precedence grouping, or a
    /// parenthesized single SELECT that keeps its own ORDER BY / LIMIT), or a bare
    /// SELECT branch (`select_core`, no tail — the query owns the tail).
    fn set_primary(&mut self) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::SetExpr;
        if *self.peek() == Token::LParen {
            self.bump(); // (
            let inner = self.set_expr(0)?;
            // An optional tail is allowed ONLY on a lone-SELECT inner (top-N per
            // branch); a tail on a multi-branch subtree is a deferred non-goal.
            let inner = self.attach_paren_tail(inner)?;
            self.expect(&Token::RParen)?;
            Ok(inner)
        } else {
            Ok(SetExpr::Select(Box::new(self.select_core()?)))
        }
    }

    /// If an ORDER BY / LIMIT / OFFSET follows inside parentheses, attach it to a
    /// lone-SELECT inner; reject it on a multi-branch subtree (deferred).
    fn attach_paren_tail(&mut self, inner: crate::ast::SetExpr) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::SetExpr;
        let has_tail = matches!(
            self.peek(),
            Token::Keyword(Keyword::Order) | Token::Keyword(Keyword::Limit) | Token::Keyword(Keyword::Offset)
        );
        if !has_tail {
            return Ok(inner);
        }
        match inner {
            SetExpr::Select(mut s) => {
                let (order_by, limit, offset) = self.parse_set_tail()?;
                s.order_by = order_by;
                s.limit = limit;
                s.offset = offset;
                Ok(SetExpr::Select(s))
            }
            _ => Err(ParseError::new(
                "ORDER BY/LIMIT on a parenthesized set-operation subtree is not supported",
                self.peek_pos(),
            )),
        }
    }
```

- [ ] **Step 5: Reroute the statement dispatch.** In `statement()` (parser.rs:739), change the SELECT arm and add a `(` arm:

```rust
            Token::Keyword(Keyword::Select) | Token::LParen => self.query_stmt(),
```

(Delete the now-unused `fn select(&mut self)` wrapper at parser.rs:1043, or leave it; if clippy flags it as dead, delete it.)

- [ ] **Step 6: Run the Task-2 tests.** Run: `cargo nextest run -p pgparser`
  Expected: PASS — all six new tests plus the entire existing parser suite (the `select_core`/tail refactor must not change any existing SelectStmt output).

- [ ] **Step 7: Commit.**

```bash
git add crates/pgparser/src/parser.rs
git commit -m "SP38: parse UNION/INTERSECT/EXCEPT with PG precedence + result-level tail"
```

---

## Task 3: libpg_query oracle — accepted set-op forms

**Files:**
- Modify: `crates/pgparser/tests/libpg_query_oracle.rs` (ACCEPTED list)

Per the memory note "libpg_query oracle is raw-parse, not analysis": only raw-grammar-valid forms go here. A column-count mismatch is an analysis error (libpg_query accepts it), so it does NOT go in a REJECTED list — it is covered by a unit test in Task 5.

- [ ] **Step 1: Add accepted statements.** Find the accepted-SQL list in `crates/pgparser/tests/libpg_query_oracle.rs` and add:

```rust
    "SELECT 1 UNION SELECT 2",
    "SELECT 1 UNION ALL SELECT 2",
    "SELECT a FROM t UNION SELECT a FROM u ORDER BY a",
    "SELECT 1 INTERSECT SELECT 2",
    "SELECT 1 EXCEPT ALL SELECT 2",
    "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
    "(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2",
```

(Match the exact array/format the file already uses — inspect the file first and follow its existing structure.)

- [ ] **Step 2: Run the oracle test.** Run: `cargo nextest run -p pgparser --test libpg_query_oracle`
  Expected: PASS (our parser accepts each form that libpg_query also accepts).

- [ ] **Step 3: Commit.**

```bash
git add crates/pgparser/tests/libpg_query_oracle.rs
git commit -m "SP38: libpg_query oracle — accept set-operation forms"
```

---

## Task 4: Executor prep — error variant + shared-helper visibility

**Files:**
- Modify: `crates/executor/src/error.rs` (new `SetOpColumnCount` → 42601)
- Modify: `crates/executor/src/exec.rs` (`order_cmp` takes `&[OrderItem]`; make `field` `pub(crate)`)

- [ ] **Step 1: Add the error variant.** In `crates/executor/src/error.rs`, add to `enum ExecError` (after `SubqueryColumns`):

```rust
    /// SP38: the branches of a UNION/INTERSECT/EXCEPT have different column counts
    /// (42601).
    SetOpColumnCount { left: usize, right: usize },
```

And its `into_pg` arm (near the `SubqueryColumns` arm):

```rust
            ExecError::SetOpColumnCount { left, right } => PgError::error(
                "42601",
                format!(
                    "each UNION/INTERSECT/EXCEPT query must have the same number of columns \
                     (got {left} and {right})"
                ),
            ),
```

> **Implementation-time check (spec §Executor step 2):** confirm `42601` and the message wording against the local PostgreSQL oracle; adjust the SQLSTATE/message if PG differs.

- [ ] **Step 2: Refactor `order_cmp` to take `&[OrderItem]`.** In `crates/executor/src/exec.rs`, change the signature (line 1235) and its body's loop source:

```rust
pub(crate) fn order_cmp(
    a: &[Datum],
    b: &[Datum],
    order_by: &[pgparser::ast::OrderItem],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in order_by.iter().enumerate() {
        // ... body unchanged (it already only reads `item.asc`) ...
```

Update its two call sites in `project_rows_ordered` (lines ~1029 and ~1045): `order_cmp(&a.0, &b.0, &s.order_by)`.

- [ ] **Step 3: Make `field` reusable.** Change `fn field(name: &str, ty: ColumnType) -> FieldDescription` (exec.rs:1211) to `pub(crate) fn field(...)`.

- [ ] **Step 4: Verify the crate still builds + existing tests pass.** Run: `cargo nextest run -p executor`
  Expected: the executor crate will NOT yet compile because `session.rs` and `exec.rs` have exhaustive `match`/`let-else` that don't handle `Statement::SetOperation` — that is fixed in Task 6. For THIS task, confirm only that `error.rs` and the `order_cmp` change are internally consistent by building the lib up to the new arm: `cargo build -p executor 2>&1 | head` and check the only errors are the missing `SetOperation` dispatch (expected), not signature mismatches in `order_cmp`.

- [ ] **Step 5: Commit.**

```bash
git add crates/executor/src/error.rs crates/executor/src/exec.rs
git commit -m "SP38: executor prep — SetOpColumnCount(42601); order_cmp takes &[OrderItem]; pub(crate) field"
```

---

## Task 5: Executor — `setops` combine core (pure, unit-tested)

**Files:**
- Create: `crates/executor/src/setops.rs`
- Modify: `crates/executor/src/lib.rs` (add `mod setops;`)

This task builds and unit-tests the PURE combine: column-count check, type unification + coercion, and the six combine modes. Execution wiring is Task 6.

- [ ] **Step 1: Create the module with the combine core + failing unit tests.** Create `crates/executor/src/setops.rs`:

```rust
//! SP38: set operations — UNION / INTERSECT / EXCEPT [ALL].
//!
//! A set operation folds the outputs of two or more SELECT branches. Each leaf is
//! evaluated to a `Relation` via the existing `exec::select_to_relation` (Task 6);
//! this module supplies the pure combine: column-count check, cross-branch type
//! unification + value coercion, and the duplicate semantics. Duplicate matching
//! reuses `Datum`'s grouping `Eq`/`Hash` (NULL = NULL), which is exactly PG's
//! "not distinct" rule for set operations.

use std::collections::HashMap;

use pgparser::ast::SetOp;
use pgtypes::{ColumnType, Datum};

use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::join::Relation;
use crate::scope::{ColumnBinding, Scope};

/// Combine two child relations under one set operator into a single relation.
/// Output column NAMES come from the left child; TYPES are the per-column
/// unification of both children (numeric tower + identical types; incompatible →
/// 42804). Rows of both sides are coerced to the unified types before combining.
pub(crate) fn combine(
    op: SetOp,
    all: bool,
    left: Relation,
    right: Relation,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    let (lw, rw) = (left.scope.width(), right.scope.width());
    if lw != rw {
        return Err(ExecError::SetOpColumnCount { left: lw, right: rw });
    }
    // Unified per-column type.
    let mut out_cols = Vec::with_capacity(lw);
    let mut tys = Vec::with_capacity(lw);
    for i in 0..lw {
        let ty = crate::eval::unify_types(left.scope.ty_at(i), right.scope.ty_at(i))?;
        tys.push(ty);
        out_cols.push(ColumnBinding {
            qualifier: None,
            name: left.scope.columns[i].name.clone(),
            ty,
        });
    }
    let lrows = coerce_rows(left.rows, &left.scope, &tys, ctx)?;
    let rrows = coerce_rows(right.rows, &right.scope, &tys, ctx)?;

    let rows = match op {
        SetOp::Union if all => {
            let mut v = lrows;
            v.extend(rrows);
            v
        }
        SetOp::Union => dedup_keep_order(lrows.into_iter().chain(rrows)),
        SetOp::Intersect => intersect(lrows, rrows, all),
        SetOp::Except => except(lrows, rrows, all),
    };
    Ok(Relation { scope: Scope { columns: out_cols }, rows })
}

/// Coerce each row's cells from the child's column types to the unified `tys`.
fn coerce_rows(
    rows: Vec<Vec<Datum>>,
    scope: &Scope,
    tys: &[ColumnType],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(row.len());
        for (i, cell) in row.into_iter().enumerate() {
            if scope.ty_at(i) == tys[i] || cell.is_null() {
                cells.push(cell);
            } else {
                cells.push(pgtypes::cast::cast(&cell, tys[i], &ctx.time_zone)?);
            }
        }
        out.push(cells);
    }
    Ok(out)
}

/// Distinct, preserving first-seen order (UNION).
fn dedup_keep_order<I: Iterator<Item = Vec<Datum>>>(it: I) -> Vec<Vec<Datum>> {
    let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in it {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

/// Multiset count of each distinct row.
fn counts(rows: &[Vec<Datum>]) -> HashMap<Vec<Datum>, usize> {
    let mut m: HashMap<Vec<Datum>, usize> = HashMap::new();
    for r in rows {
        *m.entry(r.clone()).or_insert(0) += 1;
    }
    m
}

/// INTERSECT: rows in both. distinct → once per distinct row present in both;
/// ALL → min(Lₙ, Rₙ). Distinct left rows are processed in first-seen order.
fn intersect(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows);
    let rc = counts(&rrows);
    let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue; // each distinct left row handled once, in order
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        if rcount == 0 {
            continue; // not present in right
        }
        let mult = if all { lc[row].min(rcount) } else { 1 };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

/// EXCEPT: distinct → distinct left rows ABSENT from right (count_R == 0), once;
/// ALL → max(0, Lₙ − Rₙ). Distinct left rows are processed in first-seen order.
fn except(lrows: Vec<Vec<Datum>>, rrows: Vec<Vec<Datum>>, all: bool) -> Vec<Vec<Datum>> {
    let lc = counts(&lrows);
    let rc = counts(&rrows);
    let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in &lrows {
        if !seen.insert(row.clone()) {
            continue;
        }
        let rcount = *rc.get(row).unwrap_or(&0);
        let mult = if all {
            lc[row].saturating_sub(rcount)
        } else if rcount == 0 {
            1
        } else {
            0
        };
        for _ in 0..mult {
            out.push(row.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::ColumnBinding;

    fn rel(name: &str, ty: ColumnType, rows: Vec<Vec<Datum>>) -> Relation {
        Relation {
            scope: Scope { columns: vec![ColumnBinding { qualifier: None, name: name.into(), ty }] },
            rows,
        }
    }
    fn i4(n: i32) -> Vec<Datum> { vec![Datum::Int4(n)] }

    #[test]
    fn union_dedups_union_all_keeps() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1), i4(2)]);
        let r = rel("a", ColumnType::Int4, vec![i4(2), i4(3)]);
        let u = combine(SetOp::Union, false, l.clone(), r.clone(), &ctx).unwrap();
        assert_eq!(u.rows, vec![i4(1), i4(2), i4(3)]);
        let ua = combine(SetOp::Union, true, l, r, &ctx).unwrap();
        assert_eq!(ua.rows, vec![i4(1), i4(2), i4(2), i4(3)]);
    }

    #[test]
    fn intersect_and_except_multiplicity() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1), i4(1), i4(2)]);
        let r = rel("a", ColumnType::Int4, vec![i4(1), i4(3)]);
        assert_eq!(combine(SetOp::Intersect, false, l.clone(), r.clone(), &ctx).unwrap().rows, vec![i4(1)]);
        assert_eq!(combine(SetOp::Intersect, true, l.clone(), r.clone(), &ctx).unwrap().rows, vec![i4(1)]);
        // EXCEPT distinct: {2}; EXCEPT ALL: two 1s minus one 1 = one 1, plus 2 => [1,2]
        assert_eq!(combine(SetOp::Except, false, l.clone(), r.clone(), &ctx).unwrap().rows, vec![i4(2)]);
        assert_eq!(combine(SetOp::Except, true, l, r, &ctx).unwrap().rows, vec![i4(1), i4(2)]);
    }

    #[test]
    fn null_equals_null_in_dedup() {
        let ctx = EvalCtx::test_default();
        let n = || vec![Datum::Null];
        let l = rel("a", ColumnType::Int4, vec![n(), n()]);
        let r = rel("a", ColumnType::Int4, vec![n()]);
        // UNION dedups the two NULLs and the third into one.
        assert_eq!(combine(SetOp::Union, false, l, r, &ctx).unwrap().rows, vec![n()]);
    }

    #[test]
    fn unifies_int4_and_int8_to_int8() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = rel("a", ColumnType::Int8, vec![vec![Datum::Int8(2)]]);
        let u = combine(SetOp::Union, true, l, r, &ctx).unwrap();
        assert_eq!(u.scope.ty_at(0), ColumnType::Int8);
        assert_eq!(u.rows, vec![vec![Datum::Int8(1)], vec![Datum::Int8(2)]]);
    }

    #[test]
    fn column_count_mismatch_errors() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = Relation {
            scope: Scope {
                columns: vec![
                    ColumnBinding { qualifier: None, name: "a".into(), ty: ColumnType::Int4 },
                    ColumnBinding { qualifier: None, name: "b".into(), ty: ColumnType::Int4 },
                ],
            },
            rows: vec![vec![Datum::Int4(1), Datum::Int4(2)]],
        };
        assert_eq!(
            combine(SetOp::Union, false, l, r, &ctx).unwrap_err(),
            ExecError::SetOpColumnCount { left: 1, right: 2 }
        );
    }

    #[test]
    fn incompatible_types_error_42804() {
        let ctx = EvalCtx::test_default();
        let l = rel("a", ColumnType::Int4, vec![i4(1)]);
        let r = rel("a", ColumnType::Text, vec![vec![Datum::Text("x".into())]]);
        assert!(matches!(
            combine(SetOp::Union, false, l, r, &ctx).unwrap_err(),
            ExecError::TypeMismatch(_)
        ));
    }
}
```

> Confirm the exact `Datum` variant constructors (`Datum::Int4`, `Datum::Int8`, `Datum::Text`, `Datum::Null`) and `ColumnType` names against `crates/pgtypes/src/datum.rs` before running; adjust if the `Text` payload type differs (e.g. `String` vs `Box<str>`).

- [ ] **Step 2: Register the module.** In `crates/executor/src/lib.rs`, add `mod setops;` alongside the other `mod` declarations.

- [ ] **Step 3: Run the unit tests.** Run: `cargo nextest run -p executor setops::`
  Expected: PASS (6 tests). The full executor crate may still fail to build pending Task 6's dispatch — if so, run just this module's tests by temporarily ensuring the crate compiles; otherwise complete Task 6 first and run together. (Recommended: do Tasks 5 and 6 back-to-back; commit at the end of each.)

- [ ] **Step 4: Commit.**

```bash
git add crates/executor/src/setops.rs crates/executor/src/lib.rs
git commit -m "SP38: setops combine core — union/intersect/except [all], unify+coerce, NULL-equal dedup"
```

---

## Task 6: Executor — execute the set-op query + session dispatch

**Files:**
- Modify: `crates/executor/src/setops.rs` (add `execute_set_operation` + `fold` + query-level ORDER BY)
- Modify: `crates/executor/src/session.rs` (dispatch arm + `run_set_operation`)

- [ ] **Step 1: Add the executor entry to `setops.rs`.** Append:

```rust
use kv::Kv;
use mvcc::visibility::Snapshot;
use pgparser::ast::{Expr, SetExpr, SetQuery};
use pgwire::engine::QueryResult;

/// Evaluate a complete set-operation query to a wire result. Each leaf runs through
/// the existing single-SELECT read path (`exec::select_to_relation`) under the
/// statement's snapshot handles; the tree folds via `combine`; the query-level
/// ORDER BY / OFFSET / LIMIT then apply to the combined output.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_set_operation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &SetQuery,
    ctx: &EvalCtx,
) -> Result<QueryResult, ExecError> {
    let rel = fold(catalog_kv, kv, global, gsnap, snapshot, own, &q.body, ctx)?;
    let mut rows = rel.rows;

    // Query-level ORDER BY over the OUTPUT columns: a bare integer is a 1-based
    // position; anything else is evaluated against the output scope.
    if !q.order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(q.order_by.len());
            for item in &q.order_by {
                keys.push(order_key(&item.expr, &rel.scope, &row, ctx)?);
            }
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, &q.order_by));
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    crate::exec::apply_offset_limit(&mut rows, q.offset, q.limit);

    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    Ok(crate::exec::rows_result(fields, &rows, &ctx.time_zone))
}

/// One ORDER BY key for the set-op output: integer literal → 1-based position;
/// otherwise evaluate against the output scope (output column name / expression).
fn order_key(expr: &Expr, scope: &Scope, row: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let Expr::IntLiteral(s) = expr {
        let pos: usize = s.parse().map_err(|_| {
            ExecError::Unsupported(format!("invalid ORDER BY position {s}"))
        })?;
        if pos == 0 || pos > scope.width() {
            return Err(ExecError::Unsupported(format!(
                "ORDER BY position {pos} is out of range (1..{})",
                scope.width()
            )));
        }
        return Ok(row[pos - 1].clone());
    }
    crate::eval::eval(expr, scope, row, ctx)
}

#[allow(clippy::too_many_arguments)]
fn fold(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    e: &SetExpr,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    match e {
        SetExpr::Select(s) => {
            crate::exec::select_to_relation(catalog_kv, kv, global, gsnap, snapshot, own, s, ctx)
        }
        SetExpr::SetOp { op, all, left, right } => {
            let l = fold(catalog_kv, kv, global, gsnap, snapshot, own, left, ctx)?;
            let r = fold(catalog_kv, kv, global, gsnap, snapshot, own, right, ctx)?;
            combine(*op, *all, l, r, ctx)
        }
    }
}
```

> The `order_key` integer-position rule is PG-faithful and scoped to the set-op path (spec non-goal: plain-SELECT positional ORDER BY unchanged). Confirm `Expr::IntLiteral` holds a `String` (per ast.rs:156); adjust the parse if it holds an `i64`.

- [ ] **Step 2: Add the session dispatch arm + handler.** In `crates/executor/src/session.rs` `run_one` (line ~400), add an arm after the `Statement::Select(_)` arms:

```rust
            Statement::SetOperation(_) => self.run_set_operation(stmt).await,
```

And add the method next to `run_select` (~line 561):

```rust
    async fn run_set_operation(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let Statement::SetOperation(q) = stmt else {
            unreachable!("run_one only routes a SetOperation here");
        };
        let (snapshot, own, gsnap) = self.read_context().await?;
        let ctx = self.eval_ctx();
        crate::setops::execute_set_operation(
            &*self.catalog_kv,
            &*self.kv,
            &*self.catalog_kv,
            &gsnap,
            &snapshot,
            own,
            q,
            &ctx,
        )
    }
```

- [ ] **Step 3: Add a focused unit test for end-to-end execution.** Add to `setops.rs` `mod tests` a test driving a real engine through a `UNION`. Mirror the existing in-crate engine test pattern in `exec.rs`/`agg.rs` (they build a `SqlEngine` + `SqlSession` and call `run`). Example shape:

```rust
    #[tokio::test]
    async fn union_runs_end_to_end() {
        use crate::{SqlEngine, SqlSession};
        use pgwire::engine::{Engine, QueryResult, Session};
        let engine = SqlEngine::new();
        let mut s = engine.session();
        for sql in [
            "CREATE TABLE t (a int4)",
            "INSERT INTO t VALUES (1),(2),(2)",
            "CREATE TABLE u (a int4)",
            "INSERT INTO u VALUES (2),(3)",
        ] { s.run(sql).await.unwrap(); }
        let r = s.run("SELECT a FROM t UNION SELECT a FROM u ORDER BY a").await.unwrap();
        let QueryResult::Rows { rows, .. } = r else { panic!() };
        let got: Vec<_> = rows.iter().map(|row| row[0].as_ref().unwrap().text.clone()).collect();
        assert_eq!(got, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }
```

> Inspect an existing async engine test in `exec.rs`/`agg.rs` for the exact `session()`/`run()` API and `Cell.text` access; copy that idiom precisely.

- [ ] **Step 4: Build + run the executor suite.** Run: `cargo nextest run -p executor`
  Expected: PASS — the crate now compiles (the `SetOperation` arm closes the exhaustive `match` in `session.rs`), the new execution test passes, and all existing executor tests still pass.

- [ ] **Step 5: Commit.**

```bash
git add crates/executor/src/setops.rs crates/executor/src/session.rs
git commit -m "SP38: execute set-op query — fold leaves, result-level ORDER BY/LIMIT, session dispatch"
```

---

## Task 7: Executor — Describe (extended protocol) for set-op queries

**Files:**
- Modify: `crates/executor/src/exec.rs` (`describe` handles `Statement::SetOperation`)
- Modify: `crates/executor/src/setops.rs` (schema-only field resolution)

- [ ] **Step 1: Write a failing describe test.** Add to `exec.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn describe_set_op_returns_first_branch_fields() {
        use kv::MemKv;
        let cat = MemKv::new();
        // Build a table so a branch references a known column type.
        catalog::create_table(&cat, /* match existing test helpers in this module */).unwrap();
        let fields = super::describe(&cat, &cat, "SELECT 1 AS x UNION SELECT 2").unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "x"); // name from the FIRST branch
    }
```

> Match the exact catalog/`MemKv` setup the other `describe_*` tests in `exec.rs` use (e.g. `describe_select_returns_field_types_without_executing` at exec.rs:1852). If the simplest case is FROM-less (`SELECT 1 AS x UNION SELECT 2`), no table is needed — prefer that and drop the `create_table` line.

- [ ] **Step 2: Add schema-only field resolution to `setops.rs`.**

```rust
use pgwire::engine::FieldDescription;

/// Schema-only RowDescription for a set-op query (extended-protocol Describe, no
/// execution): field NAMES from the first leaf, TYPES unified across all leaves.
pub(crate) fn describe_set_query(
    catalog_kv: &dyn Kv,
    q: &SetQuery,
) -> Result<Vec<FieldDescription>, ExecError> {
    let cols = set_expr_schema(catalog_kv, &q.body)?;
    Ok(cols.iter().map(|(name, ty)| crate::exec::field(name, *ty)).collect())
}

/// (name, type) per output column for a set-op subtree, schema-only.
fn set_expr_schema(catalog_kv: &dyn Kv, e: &SetExpr) -> Result<Vec<(String, ColumnType)>, ExecError> {
    match e {
        SetExpr::Select(s) => {
            let scope = if s.from.is_empty() {
                Scope::empty()
            } else {
                crate::exec::build_from_schema(catalog_kv, &s.from)?.scope
            };
            let (fields, _exprs, tys) = crate::exec::resolve_projection(&s.projection, &scope)?;
            Ok(fields.into_iter().map(|f| f.name).zip(tys).collect())
        }
        SetExpr::SetOp { left, right, .. } => {
            let l = set_expr_schema(catalog_kv, left)?;
            let r = set_expr_schema(catalog_kv, right)?;
            if l.len() != r.len() {
                return Err(ExecError::SetOpColumnCount { left: l.len(), right: r.len() });
            }
            l.into_iter()
                .zip(r)
                .map(|((ln, lt), (_rn, rt))| Ok((ln, crate::eval::unify_types(lt, rt)?)))
                .collect()
        }
    }
}
```

> `build_from_schema` and `resolve_projection` are `pub(crate)` in `exec.rs` (confirmed). If `resolve_projection`'s return tuple shape differs, adapt the `.zip`.

- [ ] **Step 3: Wire it into `exec::describe`.** Change the early-return guard (exec.rs:1287) to handle the set-op case:

```rust
    let stmt = statements.first();
    if let Some(Statement::SetOperation(q)) = stmt {
        return crate::setops::describe_set_query(catalog_kv, q);
    }
    let Some(Statement::Select(s)) = stmt else {
        return Ok(Vec::new());
    };
```

- [ ] **Step 4: Run.** Run: `cargo nextest run -p executor describe`
  Expected: PASS (the new describe test + the existing `describe_*` tests).

- [ ] **Step 5: Commit.**

```bash
git add crates/executor/src/exec.rs crates/executor/src/setops.rs
git commit -m "SP38: extended-protocol Describe for set-op queries (first-branch names, unified types)"
```

---

## Task 8: Router — cross-range co-location for set ops

**Files:**
- Modify: `crates/cluster/src/range/router.rs` (`pinning_range` arm + `collect_set_expr_ranges`)
- Test: `crates/cluster/src/range/router.rs` (`mod tests`)

- [ ] **Step 1: Write failing router tests.** Add to `router.rs` `mod tests`, mirroring the SP34 subquery test `a_cross_range_subquery_is_rejected_while_colocated_runs` (find it in the same module and copy its harness setup):

```rust
    #[tokio::test]
    async fn a_cross_range_set_op_is_rejected_while_colocated_runs() {
        // Build the same two-range fixture the subquery co-location test uses:
        // table `a` on range 0, table `b` on range 1.
        // ... (copy the fixture setup from a_cross_range_subquery_is_rejected_while_colocated_runs) ...

        // Co-located: both branches on range 0 -> runs.
        let ok = router.dispatch(&parse_one("SELECT id FROM a UNION SELECT id FROM a")).await;
        assert!(ok.is_ok(), "co-located set op runs, got {ok:?}");

        // Cross-range: a (range 0) UNION b (range 1) -> rejected 0A000.
        let err = router
            .dispatch(&parse_one("SELECT id FROM a UNION SELECT id FROM b"))
            .await
            .unwrap_err();
        assert_eq!(err.code, "0A000", "got {err:?}");
    }
```

> Use the exact helper names the existing test uses (`parse_one`, the router builder). Read `a_cross_range_subquery_is_rejected_while_colocated_runs` first and clone its scaffolding verbatim, changing only the SQL.

- [ ] **Step 2: Run to confirm failure.** Run: `cargo nextest run -p cluster a_cross_range_set_op`
  Expected: FAIL (compile error: `pinning_range` has no `SetOperation` arm).

- [ ] **Step 3: Add the `pinning_range` arm.** In `router.rs` `pinning_range` (line ~295), add after the `Statement::Select(s)` arm:

```rust
            Statement::SetOperation(q) => {
                let mut ranges = std::collections::BTreeSet::new();
                collect_set_expr_ranges(self, &q.body, &mut ranges)?;
                match ranges.len() {
                    0 => Ok(None),
                    1 => Ok(Some(*ranges.iter().next().expect("len()==1 has one element"))),
                    _ => Err(ExecError::Unsupported(
                        "set operations spanning ranges are not supported".into(),
                    )),
                }
            }
```

- [ ] **Step 4: Add the recursive collector.** Next to `collect_select_ranges` (router.rs:624):

```rust
/// SP38: collect every base-table range a set-operation tree references by walking
/// each leaf SELECT through `collect_select_ranges`.
fn collect_set_expr_ranges(
    router: &RangeRouter,
    e: &pgparser::ast::SetExpr,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    use pgparser::ast::SetExpr;
    match e {
        SetExpr::Select(s) => collect_select_ranges(router, s, out),
        SetExpr::SetOp { left, right, .. } => {
            collect_set_expr_ranges(router, left, out)?;
            collect_set_expr_ranges(router, right, out)
        }
    }
}
```

Confirm `dispatch` needs no change: the write-gate `matches!(stmt, Insert|Update|Delete)` already excludes `SetOperation`, and the `Pin` routing uses `pinning.unwrap_or(0)` — correct for a read.

- [ ] **Step 5: Run.** Run: `cargo nextest run -p cluster`
  Expected: PASS (new test + existing router/cluster suite).

- [ ] **Step 6: Commit.**

```bash
git add crates/cluster/src/range/router.rs
git commit -m "SP38: router — co-locate set-op leaves; reject cross-range 0A000"
```

---

## Task 9: End-to-end wire test

**Files:**
- Create: `crates/executor/tests/set_operations.rs`

Target name `set_operations` is UAC-safe (no `setup`/`install`/`update`/`patch`/`upgrad`).

- [ ] **Step 1: Create the wire test.** Copy the harness preamble (spawn/connect/col0/err_code/seed) from `crates/executor/tests/subqueries.rs:1-75`, then add cases:

```rust
#[tokio::test]
async fn union_intersect_except_over_the_wire() {
    let port = spawn().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE t (a int4)").await.unwrap();
    c.simple_query("INSERT INTO t VALUES (1),(2),(2),(3)").await.unwrap();
    c.simple_query("CREATE TABLE u (a int4)").await.unwrap();
    c.simple_query("INSERT INTO u VALUES (2),(3),(4)").await.unwrap();

    assert_eq!(
        col0(&c, "SELECT a FROM t UNION SELECT a FROM u ORDER BY a").await,
        vec![Some("1".into()), Some("2".into()), Some("3".into()), Some("4".into())]
    );
    assert_eq!(
        col0(&c, "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a").await,
        vec![Some("1".into()), Some("2".into()), Some("2".into()), Some("3".into()),
             Some("2".into()), Some("3".into()), Some("4".into())]
            .into_iter()
            .collect::<Vec<_>>()
            // NOTE: ORDER BY a => sorted; rewrite the expected vector sorted:
    );
    assert_eq!(
        col0(&c, "SELECT a FROM t INTERSECT SELECT a FROM u ORDER BY a").await,
        vec![Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(&c, "SELECT a FROM t EXCEPT SELECT a FROM u ORDER BY a").await,
        vec![Some("1".into())]
    );
}

#[tokio::test]
async fn set_op_type_unification_and_naming() {
    let port = spawn().await;
    let c = connect(port).await;
    // int4 ∪ int8 → int8 column; first-branch name wins.
    let rows = c.simple_query("SELECT 1 AS x UNION SELECT 2 ORDER BY x").await.unwrap();
    // inspect RowDescription column name == "x" via the typed query path if needed.
    let _ = rows;
    assert_eq!(col0(&c, "SELECT 1 AS x UNION SELECT 2 ORDER BY x").await,
               vec![Some("1".into()), Some("2".into())]);
}

#[tokio::test]
async fn set_op_error_surface() {
    let port = spawn().await;
    let c = connect(port).await;
    // column-count mismatch
    assert_eq!(err_code(&c, "SELECT 1 UNION SELECT 1, 2").await, "42601");
    // incompatible types
    assert_eq!(err_code(&c, "SELECT 1 UNION SELECT 'x'").await, "42804");
}
```

> Fix the `UNION ALL` expected vector to the correctly-sorted multiset (`1,2,2,2,3,3,4`). Use literal expected values, not a comment.

- [ ] **Step 2: Run.** Run: `cargo nextest run -p executor --test set_operations`
  Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add crates/executor/tests/set_operations.rs
git commit -m "SP38: end-to-end wire test for UNION/INTERSECT/EXCEPT"
```

---

## Task 10: Conformance corpus

**Files:**
- Create: `crates/conformance/corpus/set_operations.sql`

The corpus dir is auto-discovered (`read_dir`); no manifest edit. Every set-op query MUST carry an explicit `ORDER BY` (PG does not guarantee set-op output order).

- [ ] **Step 1: Create the corpus file.**

```sql
-- SP38: set operations — UNION/INTERSECT/EXCEPT [ALL] — diffed against PostgreSQL
-- 18. Every set-op query carries an explicit ORDER BY for deterministic row order.
-- All tables live on one range in the single-engine run (no cross-range set op).
CREATE TABLE a (id int4, label text);
INSERT INTO a VALUES (1, 'x'), (2, 'y'), (2, 'y'), (3, 'z');
CREATE TABLE b (id int4, label text);
INSERT INTO b VALUES (2, 'y'), (3, 'z'), (4, 'w');

-- UNION dedups; UNION ALL keeps duplicates
SELECT id FROM a UNION SELECT id FROM b ORDER BY id;
SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id;

-- INTERSECT / INTERSECT ALL
SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id;
SELECT id FROM a INTERSECT ALL SELECT id FROM b ORDER BY id;

-- EXCEPT / EXCEPT ALL
SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id;
SELECT id FROM a EXCEPT ALL SELECT id FROM b ORDER BY id;

-- multi-column rows; first-branch column names
SELECT id, label FROM a UNION SELECT id, label FROM b ORDER BY id, label;

-- precedence: INTERSECT binds tighter than UNION
SELECT id FROM a UNION SELECT id FROM b INTERSECT SELECT 3 ORDER BY id;

-- result-level LIMIT/OFFSET over the combined output
SELECT id FROM a UNION SELECT id FROM b ORDER BY id LIMIT 2 OFFSET 1;

-- top-N per parenthesized branch
(SELECT id FROM a ORDER BY id LIMIT 1) UNION (SELECT id FROM b ORDER BY id DESC LIMIT 1) ORDER BY id;

-- cross-branch type unification: int4 ∪ int8 → int8
SELECT id FROM a UNION SELECT 9999999999 ORDER BY id;

-- ORDER BY by 1-based position
SELECT id FROM a UNION SELECT id FROM b ORDER BY 1;

-- error surface (SQLSTATE matched by the oracle)
SELECT 1 UNION SELECT 1, 2;
SELECT 1 UNION SELECT 'x';
```

- [ ] **Step 2: Validate locally against the PG oracle.** Per the memory note "validate conformance corpus locally vs PG", run the local PG-oracle vs crabgresql diff for just this file. Use the project's recorded procedure (see `crates/conformance/src/main.rs` / `bin/record.rs` for the CLI). Resolve any diff: if PG's column-count SQLSTATE or message differs from `42601`, fix `ExecError::SetOpColumnCount` (Task 4) and re-run. If `SELECT NULL`-typed unification appears, keep NULLs typed (`NULL::int4`) per the documented deviation, or adjust the corpus.
  Expected: 0 diffs (every statement's row text + SQLSTATE matches PG).

- [ ] **Step 3: Commit.**

```bash
git add crates/conformance/corpus/set_operations.sql
git commit -m "SP38: conformance corpus — set operations (validated vs PostgreSQL)"
```

---

## Task 11: CLAUDE.md SP38 entry + final sweep

**Files:**
- Modify: `CLAUDE.md` (append the SP38 entry; UAC audit)

- [ ] **Step 1: Append the SP38 paragraph** to the bottom of the `## Windows UAC-safe target names` section in `CLAUDE.md`, in the same style as SP37, recording: the new keywords/AST, the `Statement::SetOperation` variant, the `setops` module, the no-Stateright justification, the one new test binary `executor::set_operations` (UAC-safe), the updated `executor` integration-test list (now includes `set_operations`), the documented deviations (NULL-typed-branch unification, positional ORDER BY scoped to set-op path, deferred set-ops-in-subqueries / tail-on-parenthesized-subtree), and the guard result. End with: the full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

- [ ] **Step 2: Run the UAC guard.** Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
  Expected: empty output (`set_operations` is clean).

- [ ] **Step 3: Full workspace sweep.**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
```

Expected: fmt clean, clippy clean, all tests PASS (nextest + doctests).

- [ ] **Step 4: Commit.**

```bash
git add CLAUDE.md
git commit -m "SP38: document set operations slice (CLAUDE.md) + UAC audit"
```

---

## Self-review checklist (run after writing, before execution)

- **Spec coverage:** UNION/INTERSECT/EXCEPT [ALL] (T1,T5), precedence + parens (T2), result ORDER BY/LIMIT/OFFSET incl. positional (T2,T6), type unification + count error (T4,T5), NULL-equal dedup (T5), describe (T7), cross-range 0A000 (T8), wire test (T9), corpus (T10), CLAUDE.md + no-Stateright (T11). ✓
- **Type consistency:** `combine(op, all, left, right, ctx)`, `fold(...)`, `execute_set_operation(...)`, `describe_set_query(...)`, `collect_set_expr_ranges(...)`, `ExecError::SetOpColumnCount { left, right }`, `order_cmp(a, b, &[OrderItem])`, `crate::exec::field` (now `pub(crate)`) — names match across tasks. ✓
- **Deferred / flagged for oracle confirmation:** column-count SQLSTATE (T4/T10), NULL-typed-branch unification (T10 corpus uses typed NULLs), `Datum`/`Cell` constructor shapes (T5/T6 notes). ✓
