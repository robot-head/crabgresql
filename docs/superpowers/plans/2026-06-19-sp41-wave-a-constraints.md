# SP41 Wave A — Pure-Data Constraints (NOT NULL, DEFAULT, CHECK) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NOT NULL`, `DEFAULT <expr>`, and `CHECK (<expr>)` constraints to `CREATE TABLE`, enforced at write time on `INSERT` and `UPDATE`, matching PostgreSQL's semantics, SQLSTATEs, and message text.

**Architecture:** The parser grows column-level and table-level constraint syntax and captures `DEFAULT`/`CHECK` expressions as **source text**. The catalog `Column` gains `nullable` + `default` attributes, `Table` gains a `constraints` list, and `catalog::serde` bumps to schema version 3 (reading v2 for back-compat). The executor desugars AST constraints into the catalog model and validates expressions at DDL time; the INSERT/UPDATE paths fill defaults, then enforce `NOT NULL` (`23502`) and `CHECK` (`23514`). No index, no locks, no concurrency, **no Stateright model** (the pure-data / single-node carve-out).

**Tech Stack:** Rust 2024, `cargo-nextest`, crates `pgparser` / `catalog` / `kv` / `pgtypes` / `executor`, conformance corpus diffed against PostgreSQL 18.

## Global Constraints

- **Match PostgreSQL observable behavior identically** — SQL surface, semantics, SQLSTATEs, output text, and quirks. `CHECK` passes when the result is **TRUE or NULL**; only FALSE fails.
- **Rust 2024 edition.** Follow surrounding code's idiom, naming, and comment density.
- **Test runner is `cargo nextest run`.** Per-crate: `cargo nextest run -p <crate>`. Doctests run separately via `cargo test -p <crate> --doc`. nextest does **not** run doctests.
- **No `sleep` in tests.** (Not applicable in Wave A — no concurrency — but holds project-wide.)
- **UAC-safe target names:** no `[[test]]`/`[[bin]]` target name or `crates/*/tests/*.rs` filename may contain `setup`, `install`, `update`, `patch`, or `upgrad`. The new wire test is named `constraints.rs` (safe). Guard must stay empty: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`.
- **Append-only / versioned storage discipline.** The catalog schema format bumps `SCHEMA_VERSION` 2 → 3; a v2 payload must still decode.
- **Conformance corpus** lives in `crates/conformance/corpus/*.sql`, diffed against a real PostgreSQL oracle in CI.
- **Spec:** `docs/superpowers/specs/2026-06-19-crabgresql-sp41-table-constraints-design.md`. This plan implements **Wave A only**; Waves B (unique index + value lock) and C (failover) get their own plans once Wave A lands.

---

## File Structure

- `crates/pgparser/src/token.rs` — add `Keyword::{Primary, Key, Default, Check, Constraint}`.
- `crates/pgparser/src/lexer.rs` — register the new keyword spellings.
- `crates/pgparser/src/ast.rs` — `ColumnConstraint`, `TableConstraint` enums; extend `ColumnDef` and `Statement::CreateTable`.
- `crates/pgparser/src/parser.rs` — thread source text into `Parser`; parse column-level + table-level constraints; capture `DEFAULT`/`CHECK` source text.
- `crates/pgparser/src/lib.rs` — export a production `parse_expr`.
- `crates/catalog/src/lib.rs` — extend `Column`, add `Constraint`/`ConstraintKind`, extend `Table`; update `create_table_ops`/`get_table`; structural DDL validation.
- `crates/catalog/src/serde.rs` — schema v3: per-column `nullable`+`default`, a constraints section; v2 back-read.
- `crates/executor/src/error.rs` — `ExecError::{NotNullViolation, CheckViolation}` + SQLSTATE mapping.
- `crates/executor/src/constraints.rs` (new) — desugar AST → catalog model; DDL-time expr validation; the runtime enforcement helpers (default fill, NOT NULL, CHECK).
- `crates/executor/src/exec.rs` — `CreateTable` arm desugar+validate; `Insert`/`Update` arms call the enforcement helpers.
- `crates/executor/tests/constraints.rs` (new) — over-the-wire integration test.
- `crates/conformance/corpus/constraints_basic.sql` (new) — corpus, diffed vs PG 18.

---

## Task 1: Parser scaffolding — source threading, keywords, AST types

Make the parser carry the source string and define the constraint AST, with `create_table` still producing empty constraint lists so existing behavior is unchanged.

**Files:**
- Modify: `crates/pgparser/src/parser.rs` (`Parser` struct ~42-92, the three `Parser::new(lex(sql)?)` call sites at ~2168, ~2181, ~2191, and `create_table` ~1114-1132)
- Modify: `crates/pgparser/src/token.rs` (`Keyword` enum)
- Modify: `crates/pgparser/src/lexer.rs` (keyword table)
- Modify: `crates/pgparser/src/ast.rs` (`ColumnDef`, `Statement::CreateTable`, new enums)
- Test: `crates/pgparser/src/parser.rs` (unit tests module)

**Interfaces:**
- Produces:
  - `ast::ColumnConstraint` — `enum { NotNull, Null, Default(String), PrimaryKey, Unique, Check(String), Named(String, Box<ColumnConstraint>) }` (the `Default`/`Check` `String` is the captured expression **source text**).
  - `ast::TableConstraint` — `struct { name: Option<String>, kind: TableConstraintKind }` with `enum TableConstraintKind { PrimaryKey(Vec<String>), Unique(Vec<String>), Check(String) }`.
  - `ast::ColumnDef { name: String, ty: ColumnType, constraints: Vec<ColumnConstraint> }`.
  - `ast::Statement::CreateTable { name: String, columns: Vec<ColumnDef>, table_constraints: Vec<TableConstraint> }`.
  - `Parser` now holds `src: String`; `Parser::new(toks: Vec<(Token, usize)>, src: &str)`.
  - `Parser::take_expr_text(&mut self) -> Result<String, ParseError>` — parse one expression and return its trimmed source slice.

- [ ] **Step 1: Add the new keywords to the token enum**

In `crates/pgparser/src/token.rs`, add to the `Keyword` enum (keep alphabetical grouping with neighbors): `Check`, `Constraint`, `Default`, `Key`, `Primary`. (`Null` and `Unique` already exist.)

```rust
// in `pub enum Keyword { ... }`
Check,
Constraint,
Default,
Key,
Primary,
```

- [ ] **Step 2: Register the keyword spellings in the lexer**

In `crates/pgparser/src/lexer.rs`, find the keyword-lookup table (the `match` on the lowercased identifier that maps spellings to `Keyword`). Add arms:

```rust
"check" => Keyword::Check,
"constraint" => Keyword::Constraint,
"default" => Keyword::Default,
"key" => Keyword::Key,
"primary" => Keyword::Primary,
```

- [ ] **Step 3: Add the AST types and extend `ColumnDef` / `CreateTable`**

In `crates/pgparser/src/ast.rs`:

```rust
/// A column-level constraint clause (SP41 Wave A: NOT NULL/DEFAULT/CHECK;
/// PRIMARY KEY/UNIQUE are parsed here and desugared to table constraints by the
/// executor). `Default`/`Check` carry the constraint expression's SOURCE TEXT,
/// re-parsed on table load.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    Default(String),
    PrimaryKey,
    Unique,
    Check(String),
    /// `CONSTRAINT name <inner>` — an explicit name attached to one of the above.
    Named(String, Box<ColumnConstraint>),
}

/// A table-level constraint clause.
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    Check(String),
}
```

Extend `ColumnDef`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub constraints: Vec<ColumnConstraint>,
}
```

(Note: `ColumnDef` loses `Eq` because `ColumnConstraint` holds `String` only — `String` is `Eq`, so keep `Eq` if the derive still holds; `Expr` is not stored, only text, so `#[derive(Debug, Clone, PartialEq, Eq)]` remains valid.)

Extend the `Statement::CreateTable` variant:

```rust
CreateTable {
    name: String,
    columns: Vec<ColumnDef>,
    table_constraints: Vec<TableConstraint>,
},
```

- [ ] **Step 4: Thread source text into `Parser`**

In `crates/pgparser/src/parser.rs`, add a field and update `new`:

```rust
pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    pos: usize,
    src: String,
    depth: Rc<Cell<usize>>,
}

impl Parser {
    pub(crate) fn new(toks: Vec<(Token, usize)>, src: &str) -> Self {
        Self {
            toks,
            pos: 0,
            src: src.to_string(),
            depth: Rc::new(Cell::new(0)),
        }
    }
```

Update the three call sites (`~2168`, `~2181`, `~2191`) from `Parser::new(lex(sql)?)` to `Parser::new(lex(sql)?, sql)`.

- [ ] **Step 5: Add the `take_expr_text` helper**

In `crates/pgparser/src/parser.rs` (near `expr`), add:

```rust
/// Parse one expression and return its trimmed SOURCE TEXT (for storing a
/// DEFAULT/CHECK constraint, which is re-parsed on table load). The slice runs
/// from the expression's first token to the start of the following token, so a
/// trailing `,`/`)`/keyword is excluded and only whitespace needs trimming.
fn take_expr_text(&mut self) -> Result<String, ParseError> {
    let start = self.peek_pos();
    let _ = self.expr(0)?;
    let end = self.peek_pos();
    Ok(self.src[start..end].trim().to_string())
}
```

- [ ] **Step 6: Keep `create_table` compiling with empty constraints**

Update `create_table` (~1114) so it still builds `ColumnDef`s (now with `constraints: Vec::new()`) and passes `table_constraints: Vec::new()`:

```rust
fn create_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
    use crate::ast::{ColumnDef, Statement};
    self.expect(&Token::Keyword(Keyword::Create))?;
    self.expect(&Token::Keyword(Keyword::Table))?;
    let name = self.expect_ident()?;
    self.expect(&Token::LParen)?;
    let mut columns = Vec::new();
    loop {
        let col_name = self.expect_ident()?;
        let ty = self.parse_type_name()?;
        columns.push(ColumnDef {
            name: col_name,
            ty,
            constraints: Vec::new(),
        });
        if self.eat_comma() {
            continue;
        }
        break;
    }
    self.expect(&Token::RParen)?;
    Ok(Statement::CreateTable {
        name,
        columns,
        table_constraints: Vec::new(),
    })
}
```

Also update any other constructor of `Statement::CreateTable` or `ColumnDef` in the crate (e.g. `create_foreign_table` reuses `ColumnDef` — add `constraints: Vec::new()` there too; foreign tables take no constraints). Search: `grep -n "ColumnDef {" crates/pgparser/src/parser.rs`.

- [ ] **Step 7: Write the failing test**

Add to the parser test module in `crates/pgparser/src/parser.rs`:

```rust
#[test]
fn create_table_without_constraints_still_parses_with_empty_constraint_lists() {
    let stmts = crate::parse("CREATE TABLE t (a int, b text)").expect("parse");
    let crate::ast::Statement::CreateTable {
        columns,
        table_constraints,
        ..
    } = &stmts[0]
    else {
        panic!("expected CreateTable");
    };
    assert!(table_constraints.is_empty());
    assert!(columns.iter().all(|c| c.constraints.is_empty()));
}
```

- [ ] **Step 8: Run the test (and the crate) to verify it passes and nothing broke**

Run: `cargo nextest run -p pgparser`
Expected: PASS — all existing parser tests plus the new one. (If other crates reference `Statement::CreateTable`/`ColumnDef` fields, they won't compile yet; that's expected and handled in Task 4/6. To check pgparser in isolation here, build just the crate: `cargo nextest run -p pgparser` compiles only `pgparser` and its tests.)

- [ ] **Step 9: Commit**

```bash
git add crates/pgparser/src/token.rs crates/pgparser/src/lexer.rs crates/pgparser/src/ast.rs crates/pgparser/src/parser.rs
git commit -m "parser(sp41): constraint AST scaffolding + source threading"
```

---

## Task 2: Parse column-level constraints

Parse `NOT NULL | NULL | DEFAULT <expr> | PRIMARY KEY | UNIQUE | CHECK (<expr>) | CONSTRAINT name <one-of-those>` after a column's type, in any order, repeatable.

**Files:**
- Modify: `crates/pgparser/src/parser.rs` (`create_table` column loop; new `parse_column_constraints`)
- Test: `crates/pgparser/src/parser.rs` (tests)

**Interfaces:**
- Consumes: `ast::ColumnConstraint`, `Parser::take_expr_text` (Task 1).
- Produces: `Parser::parse_column_constraints(&mut self) -> Result<Vec<ColumnConstraint>, ParseError>`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parses_column_level_constraints() {
    use crate::ast::{ColumnConstraint as CC, Statement};
    let stmts = crate::parse(
        "CREATE TABLE t (\
           id int PRIMARY KEY, \
           a int NOT NULL DEFAULT 0, \
           b text UNIQUE, \
           c int CHECK (c > 0), \
           d int CONSTRAINT d_nn NOT NULL, \
           e int NULL)",
    )
    .expect("parse");
    let Statement::CreateTable { columns, .. } = &stmts[0] else {
        panic!("CreateTable");
    };
    assert_eq!(columns[0].constraints, vec![CC::PrimaryKey]);
    assert_eq!(
        columns[1].constraints,
        vec![CC::NotNull, CC::Default("0".into())]
    );
    assert_eq!(columns[2].constraints, vec![CC::Unique]);
    assert_eq!(columns[3].constraints, vec![CC::Check("c > 0".into())]);
    assert_eq!(
        columns[4].constraints,
        vec![CC::Named("d_nn".into(), Box::new(CC::NotNull))]
    );
    assert_eq!(columns[5].constraints, vec![CC::Null]);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p pgparser parses_column_level_constraints`
Expected: FAIL — column constraints are empty (Task 1 left them `Vec::new()`).

- [ ] **Step 3: Implement `parse_column_constraints` and call it**

Add to `crates/pgparser/src/parser.rs`:

```rust
/// Parse zero or more column-level constraint clauses following a column type.
/// Stops at `,` or `)` (the end of the column / column list).
fn parse_column_constraints(&mut self) -> Result<Vec<crate::ast::ColumnConstraint>, ParseError> {
    use crate::ast::ColumnConstraint as CC;
    let mut out = Vec::new();
    loop {
        let c = match self.peek() {
            Token::Keyword(Keyword::Constraint) => {
                self.bump();
                let name = self.expect_ident()?;
                let inner = self.parse_one_column_constraint()?.ok_or_else(|| {
                    ParseError::new("expected a constraint after CONSTRAINT name", self.peek_pos())
                })?;
                CC::Named(name, Box::new(inner))
            }
            _ => match self.parse_one_column_constraint()? {
                Some(c) => c,
                None => break,
            },
        };
        out.push(c);
    }
    Ok(out)
}

/// Parse a single (unnamed) column constraint, or `None` if the next token is not
/// the start of one.
fn parse_one_column_constraint(
    &mut self,
) -> Result<Option<crate::ast::ColumnConstraint>, ParseError> {
    use crate::ast::ColumnConstraint as CC;
    let c = match self.peek() {
        Token::Keyword(Keyword::Not) => {
            self.bump();
            self.expect(&Token::Keyword(Keyword::Null))?;
            CC::NotNull
        }
        Token::Keyword(Keyword::Null) => {
            self.bump();
            CC::Null
        }
        Token::Keyword(Keyword::Default) => {
            self.bump();
            CC::Default(self.take_expr_text()?)
        }
        Token::Keyword(Keyword::Primary) => {
            self.bump();
            self.expect(&Token::Keyword(Keyword::Key))?;
            CC::PrimaryKey
        }
        Token::Keyword(Keyword::Unique) => {
            self.bump();
            CC::Unique
        }
        Token::Keyword(Keyword::Check) => {
            self.bump();
            self.expect(&Token::LParen)?;
            let text = self.take_expr_text()?;
            self.expect(&Token::RParen)?;
            CC::Check(text)
        }
        _ => return Ok(None),
    };
    Ok(Some(c))
}
```

In `create_table`, replace `let ty = self.parse_type_name()?;` block's push with:

```rust
let ty = self.parse_type_name()?;
let constraints = self.parse_column_constraints()?;
columns.push(ColumnDef {
    name: col_name,
    ty,
    constraints,
});
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p pgparser parses_column_level_constraints`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/pgparser/src/parser.rs
git commit -m "parser(sp41): column-level constraints (NOT NULL/DEFAULT/CHECK/PK/UNIQUE/CONSTRAINT)"
```

---

## Task 3: Parse table-level constraints

Parse `[CONSTRAINT name] PRIMARY KEY (cols) | UNIQUE (cols) | CHECK (expr)` as comma-separated items mixed with column definitions inside the `CREATE TABLE (...)` list. A list item is a table constraint when it begins with `CONSTRAINT`, `PRIMARY`, `UNIQUE`, or `CHECK`; otherwise it's a column definition.

**Files:**
- Modify: `crates/pgparser/src/parser.rs` (`create_table` loop; new `parse_table_constraint`)
- Test: `crates/pgparser/src/parser.rs` (tests)

**Interfaces:**
- Consumes: `ast::{TableConstraint, TableConstraintKind}` (Task 1), `Parser::take_expr_text` (Task 1).
- Produces: `Parser::parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError>`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn parses_table_level_constraints() {
    use crate::ast::{Statement, TableConstraintKind as TK};
    let stmts = crate::parse(
        "CREATE TABLE t (\
           a int, b int, c int, \
           PRIMARY KEY (a, b), \
           CONSTRAINT u_bc UNIQUE (b, c), \
           CHECK (a < b))",
    )
    .expect("parse");
    let Statement::CreateTable {
        columns,
        table_constraints,
        ..
    } = &stmts[0]
    else {
        panic!("CreateTable");
    };
    assert_eq!(columns.len(), 3, "three columns, not five list items");
    assert_eq!(table_constraints.len(), 3);
    assert_eq!(table_constraints[0].name, None);
    assert_eq!(
        table_constraints[0].kind,
        TK::PrimaryKey(vec!["a".into(), "b".into()])
    );
    assert_eq!(table_constraints[1].name, Some("u_bc".into()));
    assert_eq!(
        table_constraints[1].kind,
        TK::Unique(vec!["b".into(), "c".into()])
    );
    assert_eq!(table_constraints[2].kind, TK::Check("a < b".into()));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p pgparser parses_table_level_constraints`
Expected: FAIL — `create_table` treats every list item as a column (`PRIMARY` is not a valid column name → a parse error or wrong shape).

- [ ] **Step 3: Implement table-constraint parsing**

Add to `crates/pgparser/src/parser.rs`:

```rust
/// True if the next token begins a table-level constraint list item.
fn at_table_constraint(&self) -> bool {
    matches!(
        self.peek(),
        Token::Keyword(Keyword::Constraint)
            | Token::Keyword(Keyword::Primary)
            | Token::Keyword(Keyword::Unique)
            | Token::Keyword(Keyword::Check)
    )
}

/// Parse one table-level constraint (`at_table_constraint()` is true).
fn parse_table_constraint(&mut self) -> Result<crate::ast::TableConstraint, ParseError> {
    use crate::ast::{TableConstraint, TableConstraintKind as TK};
    let name = if self.eat_keyword(Keyword::Constraint) {
        Some(self.expect_ident()?)
    } else {
        None
    };
    let kind = match self.peek() {
        Token::Keyword(Keyword::Primary) => {
            self.bump();
            self.expect(&Token::Keyword(Keyword::Key))?;
            TK::PrimaryKey(self.parse_paren_ident_list()?)
        }
        Token::Keyword(Keyword::Unique) => {
            self.bump();
            TK::Unique(self.parse_paren_ident_list()?)
        }
        Token::Keyword(Keyword::Check) => {
            self.bump();
            self.expect(&Token::LParen)?;
            let text = self.take_expr_text()?;
            self.expect(&Token::RParen)?;
            TK::Check(text)
        }
        other => {
            return Err(ParseError::new(
                format!("expected a table constraint, found {other:?}"),
                self.peek_pos(),
            ));
        }
    };
    Ok(TableConstraint { name, kind })
}

/// Parse `(ident, ident, ...)`.
fn parse_paren_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
    self.expect(&Token::LParen)?;
    let mut out = Vec::new();
    loop {
        out.push(self.expect_ident()?);
        if self.eat_comma() {
            continue;
        }
        break;
    }
    self.expect(&Token::RParen)?;
    Ok(out)
}
```

Rewrite the `create_table` list loop to dispatch per item:

```rust
self.expect(&Token::LParen)?;
let mut columns = Vec::new();
let mut table_constraints = Vec::new();
loop {
    if self.at_table_constraint() {
        table_constraints.push(self.parse_table_constraint()?);
    } else {
        let col_name = self.expect_ident()?;
        let ty = self.parse_type_name()?;
        let constraints = self.parse_column_constraints()?;
        columns.push(ColumnDef {
            name: col_name,
            ty,
            constraints,
        });
    }
    if self.eat_comma() {
        continue;
    }
    break;
}
self.expect(&Token::RParen)?;
Ok(Statement::CreateTable {
    name,
    columns,
    table_constraints,
})
```

- [ ] **Step 4: Run to verify it passes (and the whole crate is green)**

Run: `cargo nextest run -p pgparser`
Expected: PASS — all parser tests including Tasks 1-3.

- [ ] **Step 5: Add a libpg_query oracle smoke (if the oracle test enumerates accepted forms)**

Check `crates/pgparser/tests/` for the libpg_query oracle harness (`grep -rn "libpg_query" crates/pgparser/tests`). If it has an accepted-forms list, add the three statements from this task's test so the oracle confirms PG also parses them. If the harness auto-discovers from the corpus, skip — Task 10's corpus covers it.

- [ ] **Step 6: Commit**

```bash
git add crates/pgparser/src/parser.rs
git commit -m "parser(sp41): table-level PRIMARY KEY / UNIQUE / CHECK constraints"
```

---

## Task 4: Catalog model — Column attributes, Constraint type, Table list

Grow the catalog data model. (Serde is Task 5; executor desugar is Task 6.)

**Files:**
- Modify: `crates/catalog/src/lib.rs` (`Column` ~20, `Table` ~62, new `Constraint`/`ConstraintKind`)
- Test: `crates/catalog/src/lib.rs` (unit tests)

**Interfaces:**
- Produces:
  - `catalog::Column { name: String, ty: ColumnType, nullable: bool, default: Option<String> }`.
  - `catalog::Constraint { name: String, id: u32, kind: ConstraintKind }`.
  - `catalog::ConstraintKind` — `enum { PrimaryKey { columns: Vec<usize> }, Unique { columns: Vec<usize> }, Check { expr: String } }`.
  - `catalog::Table { id, name, columns, foreign, constraints: Vec<Constraint> }`.

- [ ] **Step 1: Extend `Column`, add `Constraint`/`ConstraintKind`, extend `Table`**

In `crates/catalog/src/lib.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    /// false ⇒ NOT NULL. (PostgreSQL `pg_attribute.attnotnull`.)
    pub nullable: bool,
    /// Source text of the column DEFAULT expression, re-parsed on load. None ⇒ no
    /// default (omitted column → NULL).
    pub default: Option<String>,
}

/// A table-level constraint object (CHECK / UNIQUE / PRIMARY KEY). NOT NULL and
/// DEFAULT live on `Column`, not here (mirroring PostgreSQL's catalog split).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    pub name: String,
    /// Stable per-table id; the discriminator for this constraint's index
    /// keyspace in Wave B. Assigned densely (0, 1, 2, …) at CREATE TABLE.
    pub id: u32,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    PrimaryKey { columns: Vec<usize> },
    Unique { columns: Vec<usize> },
    Check { expr: String },
}
```

Extend `Table`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign: Option<ForeignTableMeta>,
    pub constraints: Vec<Constraint>,
}
```

- [ ] **Step 2: Fix every `Column { ... }` / `Table { ... }` constructor in the crate**

Search `grep -n "Column {" crates/catalog/src/lib.rs crates/catalog/src/serde.rs` and `grep -n "Table {" crates/catalog/src/lib.rs`. For each literal, add `nullable: true, default: None` (Columns) and `constraints: Vec::new()` (Tables). This includes `get_table` (~158) — leave it building `constraints: Vec::new()` for now (Task 5 wires real values from serde). The crate won't fully build until call sites in other crates are updated (Task 6); compile just this crate's tests in Step 4.

- [ ] **Step 3: Write the model unit test**

Add to the catalog test module:

```rust
#[test]
fn column_and_table_carry_constraint_metadata() {
    let col = Column {
        name: "a".into(),
        ty: ColumnType::Int4,
        nullable: false,
        default: Some("0".into()),
    };
    assert!(!col.nullable);
    let c = Constraint {
        name: "t_pkey".into(),
        id: 0,
        kind: ConstraintKind::PrimaryKey { columns: vec![0] },
    };
    assert_eq!(c.id, 0);
}
```

- [ ] **Step 4: Run to verify it compiles and passes**

Run: `cargo nextest run -p catalog`
Expected: PASS (catalog's own tests). If `serde.rs` constructors aren't updated yet, fix them per Step 2.

- [ ] **Step 5: Commit**

```bash
git add crates/catalog/src/lib.rs
git commit -m "catalog(sp41): Column nullable/default + Constraint model on Table"
```

---

## Task 5: Catalog serde v3 — persist constraints, read v2

Serialize the new fields; bump `SCHEMA_VERSION` to 3; keep decoding v2.

**Files:**
- Modify: `crates/catalog/src/serde.rs` (`SCHEMA_VERSION`, `serialize_schema` ~192, `deserialize_schema` ~221)
- Modify: `crates/catalog/src/lib.rs` (`create_table_ops` ~114, `get_table` ~157 — thread constraints through)
- Test: `crates/catalog/src/serde.rs` (round-trip tests)

**Interfaces:**
- Consumes: `Column`, `Constraint`, `ConstraintKind` (Task 4).
- Produces:
  - `serialize_schema(table_id: TableId, columns: &[Column], foreign: Option<&ForeignTableMeta>, constraints: &[Constraint]) -> Vec<u8>`.
  - `deserialize_schema(bytes) -> Result<(TableId, Vec<Column>, Option<ForeignTableMeta>, Vec<Constraint>), KvError>`.
  - `create_table_ops(kv, name, columns: Vec<Column>, constraints: Vec<Constraint>) -> Result<(TableId, Vec<WriteOp>), CatalogError>`.

- [ ] **Step 1: Write the failing round-trip tests**

In `crates/catalog/src/serde.rs` tests:

```rust
#[test]
fn schema_v3_roundtrips_nullable_default_and_constraints() {
    let cols = vec![
        Column { name: "id".into(), ty: ColumnType::Int4, nullable: false, default: None },
        Column { name: "a".into(), ty: ColumnType::Int4, nullable: true, default: Some("0".into()) },
    ];
    let cons = vec![
        Constraint { name: "t_pkey".into(), id: 0, kind: ConstraintKind::PrimaryKey { columns: vec![0] } },
        Constraint { name: "t_a_check".into(), id: 1, kind: ConstraintKind::Check { expr: "a >= 0".into() } },
    ];
    let bytes = serialize_schema(7, &cols, None, &cons);
    let (id, got_cols, foreign, got_cons) = deserialize_schema(&bytes).expect("decode");
    assert_eq!(id, 7);
    assert_eq!(got_cols, cols);
    assert!(foreign.is_none());
    assert_eq!(got_cons, cons);
}

#[test]
fn schema_v2_payload_still_decodes_as_unconstrained() {
    // Hand-build a v2 payload: version 2, table_id, 1 column "a" INT4, foreign flag 0.
    let mut v2 = vec![2u8];
    v2.extend_from_slice(&7u32.to_be_bytes()); // table_id
    v2.extend_from_slice(&1u32.to_be_bytes()); // column count
    v2.extend_from_slice(&1u32.to_be_bytes()); // name len
    v2.push(b'a');
    v2.push(1); // type_tag::INT4
    v2.push(0); // ordinary-table flag
    let (id, cols, foreign, cons) = deserialize_schema(&v2).expect("v2 decode");
    assert_eq!(id, 7);
    assert_eq!(cols.len(), 1);
    assert!(cols[0].nullable, "v2 columns default to nullable");
    assert_eq!(cols[0].default, None);
    assert!(foreign.is_none());
    assert!(cons.is_empty(), "v2 has no constraints");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p catalog schema_v`
Expected: FAIL — `serialize_schema`/`deserialize_schema` have the old arity/return type.

- [ ] **Step 3: Bump the version and define the v3 layout**

In `crates/catalog/src/serde.rs`, change `pub const SCHEMA_VERSION: u8 = 2;` to `3`. Update the module doc comment to describe v3: per-column payload now ends with a `nullable` byte (`1`/`0`) and a `default` field (`u8` present-flag; if `1`, a `u32` length + UTF-8 bytes); after the `foreign` section comes a constraints section (`u32` count, then per constraint: `u32` id, `u32` name-len + name bytes, `u8` kind tag, kind payload). Kind payloads: PrimaryKey/Unique = `u32` column count + each `u32` ordinal; Check = `u32` expr-len + expr bytes.

Add a small writer/reader for the new pieces (mirror the existing `take_u8`/`take_n` helpers):

```rust
mod cons_tag {
    pub const PRIMARY_KEY: u8 = 0;
    pub const UNIQUE: u8 = 1;
    pub const CHECK: u8 = 2;
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_str(cur: &mut &[u8]) -> Result<String, KvError> {
    let len = u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")) as usize;
    let bytes = take_n(cur, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| KvError::CorruptRow("bad utf8".into()))
}
```

- [ ] **Step 4: Rewrite `serialize_schema` and `deserialize_schema`**

`serialize_schema` — new signature + per-column nullable/default + trailing constraints section:

```rust
pub fn serialize_schema(
    table_id: TableId,
    columns: &[Column],
    foreign: Option<&ForeignTableMeta>,
    constraints: &[Constraint],
) -> Vec<u8> {
    let mut out = vec![SCHEMA_VERSION];
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for c in columns {
        write_str(&mut out, &c.name);
        write_type(&mut out, c.ty);
        out.push(u8::from(c.nullable));
        match &c.default {
            Some(text) => {
                out.push(1);
                write_str(&mut out, text);
            }
            None => out.push(0),
        }
    }
    // foreign flag + payload (unchanged from v2)
    match foreign {
        None => out.push(0),
        Some(meta) => {
            out.push(1);
            write_str(&mut out, &meta.server);
            out.extend_from_slice(&(meta.options.len() as u32).to_be_bytes());
            for (k, v) in &meta.options {
                write_str(&mut out, k);
                write_str(&mut out, v);
            }
        }
    }
    // constraints section
    out.extend_from_slice(&(constraints.len() as u32).to_be_bytes());
    for c in constraints {
        out.extend_from_slice(&c.id.to_be_bytes());
        write_str(&mut out, &c.name);
        match &c.kind {
            ConstraintKind::PrimaryKey { columns } => {
                out.push(cons_tag::PRIMARY_KEY);
                out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
                for &i in columns {
                    out.extend_from_slice(&(i as u32).to_be_bytes());
                }
            }
            ConstraintKind::Unique { columns } => {
                out.push(cons_tag::UNIQUE);
                out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
                for &i in columns {
                    out.extend_from_slice(&(i as u32).to_be_bytes());
                }
            }
            ConstraintKind::Check { expr } => {
                out.push(cons_tag::CHECK);
                write_str(&mut out, expr);
            }
        }
    }
    out
}
```

(Use the existing name/option encoding if the current code already has a string writer; reuse it instead of `write_str` to stay DRY. Adapt the foreign block to match the existing exact encoding — copy it verbatim from the current `serialize_schema`, only moving it after the per-column nullable/default additions.)

`deserialize_schema` — version-aware: read v2 OR v3 columns, then foreign, then (v3 only) constraints:

```rust
pub fn deserialize_schema(
    bytes: &[u8],
) -> Result<(TableId, Vec<Column>, Option<ForeignTableMeta>, Vec<Constraint>), KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != 2 && version != SCHEMA_VERSION {
        return Err(KvError::CorruptRow(format!("unknown schema version {version}")));
    }
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let ncols = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let mut columns = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let name = read_str(&mut cur)?;
        let ty = read_type(&mut cur)?;
        let (nullable, default) = if version >= 3 {
            let nullable = take_u8(&mut cur)? != 0;
            let default = if take_u8(&mut cur)? == 1 {
                Some(read_str(&mut cur)?)
            } else {
                None
            };
            (nullable, default)
        } else {
            (true, None) // v2: all columns nullable, no defaults
        };
        columns.push(Column { name, ty, nullable, default });
    }
    let foreign = match take_u8(&mut cur)? {
        0 => None,
        1 => {
            // ... copy the existing foreign-meta read verbatim ...
            Some(/* ForeignTableMeta { ... } */)
        }
        flag => return Err(KvError::CorruptRow(format!("unknown foreign flag {flag}"))),
    };
    let constraints = if version >= 3 {
        let n = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
        let mut cs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            let name = read_str(&mut cur)?;
            let kind = match take_u8(&mut cur)? {
                cons_tag::PRIMARY_KEY => ConstraintKind::PrimaryKey { columns: read_ordinals(&mut cur)? },
                cons_tag::UNIQUE => ConstraintKind::Unique { columns: read_ordinals(&mut cur)? },
                cons_tag::CHECK => ConstraintKind::Check { expr: read_str(&mut cur)? },
                other => return Err(KvError::CorruptRow(format!("unknown constraint tag {other}"))),
            };
            cs.push(Constraint { id, name, kind });
        }
        cs
    } else {
        Vec::new()
    };
    Ok((table_id, columns, foreign, constraints))
}

fn read_ordinals(cur: &mut &[u8]) -> Result<Vec<usize>, KvError> {
    let n = u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4"));
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        out.push(u32::from_be_bytes(take_n(cur, 4)?.try_into().expect("4")) as usize);
    }
    Ok(out)
}
```

(Keep the existing foreign-meta read body exactly; only the surrounding column-loop and trailing constraints are new.)

- [ ] **Step 5: Thread constraints through `create_table_ops` and `get_table`**

In `crates/catalog/src/lib.rs`, change `create_table_ops` to accept and persist constraints:

```rust
pub fn create_table_ops(
    kv: &dyn Kv,
    name: &str,
    columns: Vec<Column>,
    constraints: Vec<Constraint>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if kv.get(&key::catalog_key(name))?.is_some() {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let next = read_next_table_id(kv)?;
    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns, None, &constraints),
        },
        WriteOp::Put { key: key::seq_key(next), value: U64::new(1).as_bytes().to_vec() },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: U32::new(next + 1).as_bytes().to_vec(),
        },
    ];
    Ok((next, batch))
}
```

Update `create_table` (the convenience wrapper, ~142) to take + forward `constraints: Vec<Constraint>`. Update `get_table` (~157):

```rust
let (id, columns, foreign, constraints) = deserialize_schema(&bytes)?;
Ok(Table { id, name: name.to_string(), columns, foreign, constraints })
```

Fix any other `serialize_schema(` call (e.g. the foreign-table create path) to pass `&[]` for constraints (foreign tables have none). Search `grep -rn "serialize_schema\|create_table_ops\|create_table(" crates/catalog/src`.

- [ ] **Step 6: Run to verify it passes**

Run: `cargo nextest run -p catalog`
Expected: PASS — round-trip + v2 back-read + existing catalog tests (the existing tests calling `deserialize_schema` must destructure the new 4-tuple; update them).

- [ ] **Step 7: Commit**

```bash
git add crates/catalog/src/serde.rs crates/catalog/src/lib.rs
git commit -m "catalog(sp41): schema v3 serde (nullable/default/constraints) with v2 back-read"
```

---

## Task 6: Executor — desugar AST constraints + DDL-time validation

Translate parsed AST constraints into the catalog model in the `CreateTable` arm, validate `DEFAULT`/`CHECK` expressions at DDL time, generate PG-style names, and reject a second PK / unknown columns.

**Files:**
- Create: `crates/executor/src/constraints.rs`
- Modify: `crates/executor/src/exec.rs` (`CreateTable` arm ~69; add `mod constraints` to `crates/executor/src/lib.rs`)
- Modify: `crates/pgparser/src/lib.rs` (export `parse_expr`)
- Modify: `crates/executor/src/error.rs` (`ExecError::{NotNullViolation, CheckViolation}`)
- Test: `crates/executor/src/constraints.rs` (unit tests)

**Interfaces:**
- Consumes: `ast::{ColumnDef, ColumnConstraint, TableConstraint, TableConstraintKind}`, `catalog::{Column, Constraint, ConstraintKind}`, `pgparser::parse_expr`.
- Produces:
  - `pgparser::parse_expr(sql: &str) -> Result<ast::Expr, ParseError>`.
  - `constraints::build_catalog_schema(columns: &[ast::ColumnDef], table_constraints: &[ast::TableConstraint], table_name: &str) -> Result<(Vec<catalog::Column>, Vec<catalog::Constraint>), ExecError>`.
  - `ExecError::NotNullViolation { column: String, table: String }` → `23502`.
  - `ExecError::CheckViolation { constraint: String, table: String }` → `23514`.

- [ ] **Step 1: Export a production `parse_expr` from pgparser**

In `crates/pgparser/src/lib.rs`, add (alongside the `parse` re-export):

```rust
pub use crate::parser::parse_expr;
```

In `crates/pgparser/src/parser.rs`, add a public production entry (rename-free — keep `parse_expr_for_test` for existing tests, or have it delegate):

```rust
/// Parse a single standalone SQL expression. Used by the executor to re-parse a
/// stored DEFAULT/CHECK constraint's source text.
pub fn parse_expr(sql: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(lex(sql)?, sql);
    let e = p.expr(0)?;
    p.expect_eof()?; // reuse the existing trailing-token check used by parse()
    Ok(e)
}
```

(If `parse_expr_for_test` already does exactly this, make it call `parse_expr`. If there's no `expect_eof`, mirror whatever `parse_expr_for_test` does to reject trailing tokens.)

- [ ] **Step 2: Add the two `ExecError` variants and their SQLSTATE mapping**

In `crates/executor/src/error.rs`, add to the `ExecError` enum:

```rust
/// A NULL written to a NOT NULL column (23502).
NotNullViolation { column: String, table: String },
/// A CHECK constraint evaluated to FALSE (23514).
CheckViolation { constraint: String, table: String },
```

In the `PgError` mapping (the `match` near line 98-180), add:

```rust
ExecError::NotNullViolation { column, table } => PgError::error(
    "23502",
    format!("null value in column \"{column}\" of relation \"{table}\" violates not-null constraint"),
),
ExecError::CheckViolation { constraint, table } => PgError::error(
    "23514",
    format!("new row for relation \"{table}\" violates check constraint \"{constraint}\""),
),
```

- [ ] **Step 3: Write the failing desugar tests**

Create `crates/executor/src/constraints.rs` with a test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pgparser::ast::{
        ColumnConstraint as CC, ColumnDef, TableConstraint, TableConstraintKind as TK,
    };
    use pgtypes::ColumnType;

    fn col(name: &str, ty: ColumnType, cs: Vec<CC>) -> ColumnDef {
        ColumnDef { name: name.into(), ty, constraints: cs }
    }

    #[test]
    fn pk_implies_not_null_and_a_unique_constraint() {
        let cols = vec![col("id", ColumnType::Int4, vec![CC::PrimaryKey])];
        let (out_cols, cons) = build_catalog_schema(&cols, &[], "t").expect("ok");
        assert!(!out_cols[0].nullable, "PRIMARY KEY column is NOT NULL");
        assert_eq!(cons.len(), 1);
        assert_eq!(cons[0].name, "t_pkey");
        assert!(matches!(cons[0].kind, catalog::ConstraintKind::PrimaryKey { .. }));
    }

    #[test]
    fn default_and_not_null_land_on_the_column() {
        let cols = vec![col("a", ColumnType::Int4, vec![CC::NotNull, CC::Default("0".into())])];
        let (out_cols, cons) = build_catalog_schema(&cols, &[], "t").expect("ok");
        assert!(!out_cols[0].nullable);
        assert_eq!(out_cols[0].default.as_deref(), Some("0"));
        assert!(cons.is_empty());
    }

    #[test]
    fn auto_names_follow_postgres_convention() {
        let cols = vec![
            col("a", ColumnType::Int4, vec![CC::Unique]),
            col("b", ColumnType::Int4, vec![CC::Check("b > 0".into())]),
        ];
        let (_c, cons) = build_catalog_schema(&cols, &[], "t").expect("ok");
        assert_eq!(cons[0].name, "t_a_key");
        assert_eq!(cons[1].name, "t_b_check");
    }

    #[test]
    fn second_primary_key_is_42P16() {
        let cols = vec![col("a", ColumnType::Int4, vec![CC::PrimaryKey])];
        let tc = vec![TableConstraint { name: None, kind: TK::PrimaryKey(vec!["a".into()]) }];
        let err = build_catalog_schema(&cols, &tc, "t").unwrap_err();
        assert_eq!(err.to_pg_error().code, "42P16");
    }

    #[test]
    fn unknown_constraint_column_is_42703() {
        let tc = vec![TableConstraint { name: None, kind: TK::Unique(vec!["nope".into()]) }];
        let err = build_catalog_schema(&[col("a", ColumnType::Int4, vec![])], &tc, "t").unwrap_err();
        assert_eq!(err.to_pg_error().code, "42703");
    }

    #[test]
    fn bad_default_expression_is_rejected_at_ddl_time() {
        // unparsable default → error now, not at first INSERT
        let cols = vec![col("a", ColumnType::Int4, vec![CC::Default("(".into())])];
        assert!(build_catalog_schema(&cols, &[], "t").is_err());
    }
}
```

(Adjust `err.to_pg_error().code` to the project's actual accessor — check `crates/executor/src/error.rs` for how a test reads the SQLSTATE off a `PgError`; reuse that pattern.)

- [ ] **Step 4: Run to verify it fails**

Run: `cargo nextest run -p executor build_catalog`
Expected: FAIL — `build_catalog_schema` doesn't exist.

- [ ] **Step 5: Implement `build_catalog_schema`**

In `crates/executor/src/constraints.rs`:

```rust
//! SP41 Wave A: desugar parsed CREATE TABLE constraints into the catalog model,
//! validate DEFAULT/CHECK expressions at DDL time, and enforce NOT NULL/CHECK
//! on the write path.

use catalog::{Column, Constraint, ConstraintKind};
use pgparser::ast::{ColumnConstraint, ColumnDef, TableConstraint, TableConstraintKind};

use crate::error::ExecError;

/// Translate AST columns + table constraints into catalog `Column`s and
/// `Constraint`s, generating PostgreSQL-style auto names, validating every
/// DEFAULT/CHECK expression parses, and rejecting a second PRIMARY KEY (42P16) or
/// a constraint over an unknown column (42703).
pub(crate) fn build_catalog_schema(
    columns: &[ColumnDef],
    table_constraints: &[TableConstraint],
    table_name: &str,
) -> Result<(Vec<Column>, Vec<Constraint>), ExecError> {
    let mut cols: Vec<Column> = columns
        .iter()
        .map(|c| Column { name: c.name.clone(), ty: c.ty, nullable: true, default: None })
        .collect();
    let col_index = |name: &str| cols.iter().position(|c| c.name == name);

    let mut constraints: Vec<Constraint> = Vec::new();
    let mut next_id: u32 = 0;
    let mut have_pk = false;

    // Column-level constraints.
    for (i, def) in columns.iter().enumerate() {
        for raw in &def.constraints {
            let (explicit_name, c) = unwrap_named(raw);
            match c {
                ColumnConstraint::NotNull => cols[i].nullable = false,
                ColumnConstraint::Null => {} // explicit NULL: leave nullable
                ColumnConstraint::Default(text) => {
                    validate_expr(text)?;
                    cols[i].default = Some(text.clone());
                }
                ColumnConstraint::PrimaryKey => {
                    if have_pk {
                        return Err(ExecError::Syntax(
                            "multiple primary keys for table are not allowed".into(),
                        ));
                    }
                    have_pk = true;
                    cols[i].nullable = false;
                    let name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{table_name}_pkey"));
                    constraints.push(Constraint {
                        id: next_id,
                        name,
                        kind: ConstraintKind::PrimaryKey { columns: vec![i] },
                    });
                    next_id += 1;
                }
                ColumnConstraint::Unique => {
                    let name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{table_name}_{}_key", def.name));
                    constraints.push(Constraint {
                        id: next_id,
                        name,
                        kind: ConstraintKind::Unique { columns: vec![i] },
                    });
                    next_id += 1;
                }
                ColumnConstraint::Check(text) => {
                    validate_expr(text)?;
                    let name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{table_name}_{}_check", def.name));
                    constraints.push(Constraint {
                        id: next_id,
                        name,
                        kind: ConstraintKind::Check { expr: text.clone() },
                    });
                    next_id += 1;
                }
                ColumnConstraint::Named(..) => unreachable!("unwrapped above"),
            }
        }
    }

    // Table-level constraints.
    for tc in table_constraints {
        match &tc.kind {
            TableConstraintKind::PrimaryKey(names) => {
                if have_pk {
                    return Err(ExecError::Syntax(
                        "multiple primary keys for table are not allowed".into(),
                    ));
                }
                have_pk = true;
                let cols_idx = resolve_columns(names, &col_index)?;
                for &i in &cols_idx {
                    cols[i].nullable = false;
                }
                let name = tc.name.clone().unwrap_or_else(|| format!("{table_name}_pkey"));
                constraints.push(Constraint { id: next_id, name, kind: ConstraintKind::PrimaryKey { columns: cols_idx } });
                next_id += 1;
            }
            TableConstraintKind::Unique(names) => {
                let cols_idx = resolve_columns(names, &col_index)?;
                let name = tc
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_{}_key", names.join("_")));
                constraints.push(Constraint { id: next_id, name, kind: ConstraintKind::Unique { columns: cols_idx } });
                next_id += 1;
            }
            TableConstraintKind::Check(text) => {
                validate_expr(text)?;
                let name = tc.name.clone().unwrap_or_else(|| format!("{table_name}_check"));
                constraints.push(Constraint { id: next_id, name, kind: ConstraintKind::Check { expr: text.clone() } });
                next_id += 1;
            }
        }
    }

    Ok((cols, constraints))
}

fn unwrap_named(c: &ColumnConstraint) -> (Option<String>, &ColumnConstraint) {
    match c {
        ColumnConstraint::Named(name, inner) => (Some(name.clone()), inner.as_ref()),
        other => (None, other),
    }
}

fn resolve_columns(
    names: &[String],
    col_index: &impl Fn(&str) -> Option<usize>,
) -> Result<Vec<usize>, ExecError> {
    names
        .iter()
        .map(|n| col_index(n).ok_or_else(|| ExecError::UndefinedColumn(n.clone())))
        .collect()
}

/// DDL-time check that a DEFAULT/CHECK expression at least parses. (Deeper
/// type-checking happens against a real scope at the first write; PostgreSQL also
/// resolves types at DDL time, but a parse check catches the common errors and
/// keeps Wave A free of a catalog→executor type dependency.)
fn validate_expr(text: &str) -> Result<(), ExecError> {
    pgparser::parse_expr(text).map(|_| ()).map_err(ExecError::from)
}
```

Register the module: in `crates/executor/src/lib.rs` add `mod constraints;` (near the other `mod` declarations).

- [ ] **Step 6: Run to verify it passes**

Run: `cargo nextest run -p executor build_catalog auto_names pk_implies default_and second_primary unknown_constraint bad_default`
Expected: PASS. (Confirm `ExecError::Syntax` maps to `42P16` is wrong — `Syntax` is `42601`. Fix: add a dedicated `ExecError::DuplicatePrimaryKey` → `42P16` variant if the duplicate-PK test expects `42P16`. See Step 7.)

- [ ] **Step 7: Add the `42P16` error variant for duplicate PK**

In `crates/executor/src/error.rs` add `InvalidTableDefinition(String)` and map it:

```rust
ExecError::InvalidTableDefinition(m) => PgError::error("42P16", m),
```

In `constraints.rs`, replace the two `ExecError::Syntax("multiple primary keys ...")` with `ExecError::InvalidTableDefinition("multiple primary keys for table are not allowed".into())`. Re-run Step 6 — expect PASS.

- [ ] **Step 8: Wire the `CreateTable` executor arm**

In `crates/executor/src/exec.rs`, replace the `Statement::CreateTable` arm (~69):

```rust
Statement::CreateTable { name, columns, table_constraints } => {
    let (cols, constraints) =
        crate::constraints::build_catalog_schema(columns, table_constraints, name)?;
    let (_id, ops) = catalog::create_table_ops(kv, name, cols, constraints)?;
    Ok((QueryResult::Command { tag: "CREATE TABLE".into() }, ops))
}
```

Fix any other `catalog::create_table_ops(` / `catalog::create_table(` call in the executor and elsewhere (foreign-table path passes `Vec::new()` for constraints). Search `grep -rn "create_table_ops\|create_table(" crates/executor/src crates/cluster/src crates/crabgresql/src`.

- [ ] **Step 9: Run the executor crate green**

Run: `cargo nextest run -p executor`
Expected: PASS — all executor tests (existing `CreateTable` callers now pass through the desugar).

- [ ] **Step 10: Commit**

```bash
git add crates/pgparser/src/lib.rs crates/pgparser/src/parser.rs crates/executor/src/constraints.rs crates/executor/src/exec.rs crates/executor/src/error.rs crates/executor/src/lib.rs
git commit -m "executor(sp41): desugar CREATE TABLE constraints + DDL-time validation + error variants"
```

---

## Task 7: INSERT enforcement — DEFAULT fill, NOT NULL, CHECK

Fill omitted-column defaults, then enforce NOT NULL and CHECK on each inserted row.

**Files:**
- Modify: `crates/executor/src/constraints.rs` (enforcement helpers)
- Modify: `crates/executor/src/exec.rs` (`Statement::Insert` arm ~294-341)
- Test: `crates/executor/src/exec.rs` (unit tests)

**Interfaces:**
- Consumes: `catalog::Table`, `crate::eval::eval`, `crate::scope::Scope`, `crate::clock::EvalCtx`, `crate::exec::coerce`.
- Produces:
  - `constraints::eval_default(table: &Table, col: usize, ctx: &EvalCtx) -> Result<Datum, ExecError>` — evaluate a column's DEFAULT (or `Datum::Null` if none).
  - `constraints::check_row(table: &Table, row: &[Datum], ctx: &EvalCtx) -> Result<(), ExecError>` — run NOT NULL + CHECK against a fully-built row.

- [ ] **Step 1: Write the failing tests**

Add to the `exec.rs` test module (mirror the existing `insert_*` async tests' harness — `grep -n "async fn insert_then_count_via_kv" crates/executor/src/exec.rs` for the setup pattern):

```rust
#[tokio::test]
async fn insert_fills_default_for_omitted_column() {
    // CREATE TABLE t (a int, b int DEFAULT 7); INSERT INTO t (a) VALUES (1);
    // SELECT a, b → (1, 7)
    // (use the existing in-test harness to run these statements and read back)
}

#[tokio::test]
async fn insert_null_into_not_null_column_is_23502() {
    // CREATE TABLE t (a int NOT NULL); INSERT INTO t (a) VALUES (NULL) → 23502
}

#[tokio::test]
async fn insert_violating_check_is_23514() {
    // CREATE TABLE t (a int CHECK (a > 0)); INSERT INTO t VALUES (0) → 23514
}

#[tokio::test]
async fn insert_check_passes_when_predicate_is_null() {
    // CREATE TABLE t (a int CHECK (a > 0)); INSERT INTO t VALUES (NULL) → OK
    // (NULL predicate ⇒ not a violation; PostgreSQL quirk)
}
```

Flesh each test out using the existing harness (the same one `insert_writes_a_versioned_row_visible_to_select` uses). Assert the SQLSTATE on the error path via the harness's error accessor.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo nextest run -p executor insert_fills_default insert_null_into insert_violating_check insert_check_passes`
Expected: FAIL — no default fill, no NOT NULL/CHECK enforcement yet.

- [ ] **Step 3: Implement the enforcement helpers**

Add to `crates/executor/src/constraints.rs`:

```rust
use catalog::{ConstraintKind, Table};
use pgtypes::Datum;

use crate::clock::EvalCtx;
use crate::scope::Scope;

/// Evaluate column `col`'s DEFAULT expression (re-parsed from its source text),
/// or `Datum::Null` if it has none. Defaults see no row/column scope (PostgreSQL
/// forbids column references in a DEFAULT).
pub(crate) fn eval_default(
    table: &Table,
    col: usize,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match &table.columns[col].default {
        None => Ok(Datum::Null),
        Some(text) => {
            let expr = pgparser::parse_expr(text)?;
            crate::eval::eval(&expr, &Scope::empty(), &[], ctx)
        }
    }
}

/// Enforce NOT NULL and CHECK against a fully-built row (one Datum per column, in
/// table column order). NOT NULL fires first (column order), then each CHECK in
/// constraint order. A CHECK passes when its result is TRUE or NULL.
pub(crate) fn check_row(
    table: &Table,
    row: &[Datum],
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    for (i, c) in table.columns.iter().enumerate() {
        if !c.nullable && matches!(row[i], Datum::Null) {
            return Err(ExecError::NotNullViolation {
                column: c.name.clone(),
                table: table.name.clone(),
            });
        }
    }
    let scope = Scope::single(table, &table.name);
    for cons in &table.constraints {
        if let ConstraintKind::Check { expr } = &cons.kind {
            let parsed = pgparser::parse_expr(expr)?;
            match crate::eval::eval(&parsed, &scope, row, ctx)? {
                Datum::Bool(false) => {
                    return Err(ExecError::CheckViolation {
                        constraint: cons.name.clone(),
                        table: table.name.clone(),
                    });
                }
                _ => {} // TRUE or NULL ⇒ pass
            }
        }
    }
    Ok(())
}
```

(Confirm `crate::eval::eval` reads columns from the `row` slice positionally via the `Scope` — the UPDATE path at `exec.rs` already calls `row_matches(filter, &scope, &scanned_row, ctx)` with a `Scope::single(&t, &t.name)`, so a per-row column scope keyed by position is the established pattern. `check_row` mirrors it.)

- [ ] **Step 4: Wire default-fill + check into the `Insert` arm**

In `crates/executor/src/exec.rs`, modify the per-row loop inside `Statement::Insert` (~318-334). After building `full` from the supplied expressions, fill defaults for omitted columns and run `check_row`:

```rust
let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
let supplied: std::collections::HashSet<usize> = target_idx.iter().copied().collect();
for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
    let v = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
    full[*slot] = coerce(v, t.columns[*slot].ty, ctx)?;
}
// DEFAULT fill for columns not named in the INSERT.
for col in 0..t.columns.len() {
    if !supplied.contains(&col) {
        let d = crate::constraints::eval_default(&t, col, ctx)?;
        full[col] = coerce(d, t.columns[col].ty, ctx)?;
    }
}
crate::constraints::check_row(&t, &full, ctx)?;
ops.push(kv::WriteOp::Put {
    key: mvcc::version::version_key_xid(t.id, rowid, xid),
    value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &full),
});
```

- [ ] **Step 5: Run to verify they pass**

Run: `cargo nextest run -p executor insert_fills_default insert_null_into insert_violating_check insert_check_passes`
Expected: PASS.

- [ ] **Step 6: Run the whole executor crate**

Run: `cargo nextest run -p executor`
Expected: PASS — no regression in existing insert/select/transaction tests.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/constraints.rs crates/executor/src/exec.rs
git commit -m "executor(sp41): INSERT enforces DEFAULT fill + NOT NULL (23502) + CHECK (23514)"
```

---

## Task 8: UPDATE enforcement — NOT NULL + CHECK on the new row image; SET col = DEFAULT

Re-run NOT NULL + CHECK on the post-update row, and support `SET col = DEFAULT`.

**Files:**
- Modify: `crates/executor/src/exec.rs` (`Statement::Update` arm ~342 onward, where the new row image is built)
- Modify: `crates/pgparser/src/parser.rs` (the UPDATE assignment parser — accept `DEFAULT` as a value) and `crates/pgparser/src/ast.rs` if assignments need a `DEFAULT` marker
- Test: `crates/executor/src/exec.rs` (unit tests)

**Interfaces:**
- Consumes: `constraints::check_row`, `constraints::eval_default` (Task 7).
- Produces: UPDATE assignment values may be `DEFAULT` (resolved to the column default).

- [ ] **Step 1: Decide the `SET col = DEFAULT` representation**

Inspect the UPDATE assignment parser (`grep -n "fn update\|assignments" crates/pgparser/src/parser.rs`). Assignments are `Vec<(String, Expr)>`. Represent `DEFAULT` as a sentinel: add `Expr::DefaultValue` to `ast.rs` (a marker the executor resolves), or reuse the existing `SetValue::Default` pattern. Simplest: add `Expr::DefaultValue` and parse it when the assignment RHS is the bare `DEFAULT` keyword.

```rust
// ast.rs, in `enum Expr`
/// `UPDATE ... SET col = DEFAULT` (and reserved for INSERT VALUES DEFAULT).
DefaultValue,
```

In the UPDATE assignment parser, before `self.expr(0)`, peek for `Keyword::Default`:

```rust
let value = if self.eat_keyword(Keyword::Default) {
    crate::ast::Expr::DefaultValue
} else {
    self.expr(0)?
};
```

In `crates/executor/src/eval.rs`, add an arm so a stray `Expr::DefaultValue` outside an assignment is a clear error (it should never be evaluated directly):

```rust
Expr::DefaultValue => Err(ExecError::Syntax("DEFAULT is not allowed in this context".into())),
```

- [ ] **Step 2: Write the failing tests**

```rust
#[tokio::test]
async fn update_violating_not_null_is_23502() {
    // CREATE TABLE t (a int NOT NULL); INSERT VALUES (1); UPDATE t SET a = NULL → 23502
}

#[tokio::test]
async fn update_violating_check_is_23514() {
    // CREATE TABLE t (a int CHECK (a > 0)); INSERT VALUES (1); UPDATE t SET a = 0 → 23514
}

#[tokio::test]
async fn update_set_default_resolves_to_column_default() {
    // CREATE TABLE t (a int, b int DEFAULT 9); INSERT VALUES (1, 2);
    // UPDATE t SET b = DEFAULT; SELECT b → 9
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `cargo nextest run -p executor update_violating update_set_default`
Expected: FAIL.

- [ ] **Step 4: Resolve DEFAULT and enforce in the UPDATE arm**

In the `Statement::Update` arm of `exec.rs`, where the new row image is assembled from the assignments (after EvalPlanQual produces `cur_row`), resolve each assignment value (handling `Expr::DefaultValue`), build the new row, then call `check_row`:

```rust
// when applying an assignment (idx, expr) to build the new row image:
let new_val = match expr {
    Expr::DefaultValue => crate::constraints::eval_default(&t, *idx, ctx)?,
    other => crate::eval::eval(other, &scope, &cur_row, ctx)?,
};
new_row[*idx] = coerce(new_val, t.columns[*idx].ty, ctx)?;
// ... after all assignments applied to new_row:
crate::constraints::check_row(&t, &new_row, ctx)?;
```

(Adapt to the exact local variable names the UPDATE arm uses for the new row image and the per-assignment loop — read ~342-460 of `exec.rs` and match its structure. The new lines are: the `Expr::DefaultValue` branch and the `check_row` call before the row's `Put` is emitted.)

- [ ] **Step 5: Run to verify they pass**

Run: `cargo nextest run -p executor update_violating update_set_default`
Expected: PASS.

- [ ] **Step 6: Run executor + pgparser crates green**

Run: `cargo nextest run -p executor -p pgparser`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/pgparser/src/ast.rs crates/pgparser/src/parser.rs crates/executor/src/eval.rs crates/executor/src/exec.rs
git commit -m "executor(sp41): UPDATE enforces NOT NULL + CHECK; SET col = DEFAULT"
```

---

## Task 9: Over-the-wire integration test

Prove the feature end-to-end through the pgwire server, the way a real client sees it.

**Files:**
- Create: `crates/executor/tests/constraints.rs`

**Interfaces:**
- Consumes: the existing executor integration-test harness (mirror `crates/executor/tests/mutation_semantics.rs` for the connect/exec/query helpers).

- [ ] **Step 1: Write the integration test**

Mirror the harness used by an existing `crates/executor/tests/*.rs` (read `crates/executor/tests/mutation_semantics.rs` for the exact connection/exec/error-code helpers). Cover:

```rust
// 1. CREATE TABLE t (id int PRIMARY KEY, qty int NOT NULL DEFAULT 1, CHECK (qty > 0))
//    succeeds.
// 2. INSERT INTO t (id) VALUES (1); SELECT qty → 1 (default fill).
// 3. INSERT INTO t (id, qty) VALUES (2, NULL) → SQLSTATE 23502.
// 4. INSERT INTO t (id, qty) VALUES (3, 0) → SQLSTATE 23514.
// 5. INSERT INTO t (id, qty) VALUES (4, 5); UPDATE t SET qty = 0 WHERE id = 4 → 23514.
// 6. UPDATE t SET qty = DEFAULT WHERE id = 1; SELECT qty WHERE id = 1 → 1.
// 7. CREATE TABLE bad (a int PRIMARY KEY, b int PRIMARY KEY) → SQLSTATE 42P16.
```

- [ ] **Step 2: Verify the target name is UAC-safe and run**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
Expected: empty (the file is `constraints.rs`).

Run: `cargo nextest run -p executor --test constraints`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/executor/tests/constraints.rs
git commit -m "test(sp41): over-the-wire constraints integration test"
```

---

## Task 10: Conformance corpus

Add a corpus file diffed against PostgreSQL 18 in CI.

**Files:**
- Create: `crates/conformance/corpus/constraints_basic.sql`

**Interfaces:**
- Consumes: the conformance harness (auto-discovers `corpus/*.sql`).

- [ ] **Step 1: Write the corpus**

Create `crates/conformance/corpus/constraints_basic.sql` exercising the Wave A surface with outputs PostgreSQL 18 and crabgresql both produce identically. Keep it deterministic (no `now()` in a SELECTed value). Example shape (mirror an existing corpus file's comment/statement style):

```sql
CREATE TABLE items (
    id int PRIMARY KEY,
    name text NOT NULL,
    qty int NOT NULL DEFAULT 1,
    price numeric CHECK (price >= 0),
    CONSTRAINT items_qty_pos CHECK (qty > 0)
);
INSERT INTO items (id, name) VALUES (1, 'a');
INSERT INTO items (id, name, qty, price) VALUES (2, 'b', 3, 4.50);
SELECT id, name, qty, price FROM items ORDER BY id;
-- not-null violation
INSERT INTO items (id, name) VALUES (3, NULL);
-- check violation (column-level)
INSERT INTO items (id, name, price) VALUES (4, 'd', -1);
-- check violation (table-level)
INSERT INTO items (id, name, qty) VALUES (5, 'e', 0);
-- default fill + SET DEFAULT
UPDATE items SET qty = DEFAULT WHERE id = 2;
SELECT id, qty FROM items ORDER BY id;
-- duplicate primary key declaration
CREATE TABLE dup (a int PRIMARY KEY, b int PRIMARY KEY);
```

- [ ] **Step 2: Validate locally against PostgreSQL (if a local PG is available)**

If a local PostgreSQL is reachable, run the conformance comparison for this file per the project's usual invocation (`grep -rn "corpus" crates/conformance/ --include=*.rs -l` then read the harness for the run command). Confirm crabgresql's output matches PG 18 statement-for-statement (including the `23502`/`23514`/`42P16` error lines). Fix any divergence in message text to match PG exactly.

- [ ] **Step 3: Run the conformance crate**

Run: `cargo nextest run -p conformance`
Expected: PASS (or, if the oracle diff requires a live PG that CI provides, ensure the file at least parses/executes without panic locally).

- [ ] **Step 4: Commit**

```bash
git add crates/conformance/corpus/constraints_basic.sql
git commit -m "conformance(sp41): constraints_basic corpus (NOT NULL/DEFAULT/CHECK)"
```

---

## Task 11: Workspace green + CLAUDE.md slice note

Final integration: whole workspace builds and tests, and the slice is documented.

**Files:**
- Modify: `CLAUDE.md` (append an SP41 Wave A note in the slice-log style)

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo nextest run --workspace`
Expected: PASS. Fix any remaining call sites that didn't compile (other crates constructing `ColumnDef`/`Statement::CreateTable`/`Column`/`Table`/`create_table_ops`/`deserialize_schema` — search each symbol across the workspace and update).

- [ ] **Step 2: Run doctests**

Run: `cargo test --workspace --doc`
Expected: PASS.

- [ ] **Step 3: Run the UAC guard**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
Expected: empty.

- [ ] **Step 4: Append the slice note to CLAUDE.md**

Add a short SP41 Wave A entry in the existing slice-log style under the "Windows UAC-safe target names" log: record the new test binary `executor::constraints`, the schema v3 bump (with v2 back-read), the new `ExecError` SQLSTATEs (`23502`/`23514`/`42P16`), and that Wave A ships no Stateright model (pure-data carve-out), with Waves B/C deferred to their own plans.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(sp41): Wave A slice note (NOT NULL/DEFAULT/CHECK)"
```

---

## Self-Review

**Spec coverage (Wave A scope only):**
- `NOT NULL` parse + store + enforce (INSERT/UPDATE) → Tasks 2, 4-5, 7-8. ✓
- `DEFAULT` parse (source text) + store + per-row fill → Tasks 2, 5, 7; `SET col = DEFAULT` → Task 8. ✓
- `CHECK` parse + store + TRUE/NULL-pass enforcement → Tasks 2-3, 5, 7-8. ✓
- `PRIMARY KEY`/`UNIQUE` **parsing + desugar + catalog persistence** (enforcement is Wave B) → Tasks 2-6. PK implies NOT NULL → Task 6. ✓
- Auto-naming, duplicate-PK `42P16`, unknown-column `42703`, DDL-time expr validation → Task 6. ✓
- Schema v3 serde + v2 back-read → Task 5. ✓
- Error surface `23502`/`23514`/`42P16` → Tasks 6-8. ✓
- Wire test + corpus → Tasks 9-10. ✓
- No Stateright model in Wave A (pure-data) — consistent with the spec. ✓
- *Deferred to Wave B/C (correctly absent here):* the unique index keyspace, value lock, `23505` enforcement, failover. ✓

**Placeholder scan:** Tasks 7-9 reference "use the existing harness / mirror `mutation_semantics.rs`" rather than pasting the full async test scaffold — this is a deliberate pointer to a concrete existing pattern (the harness is large and codebase-specific), with the exact statements and expected SQLSTATEs spelled out. All code-bearing steps show real code.

**Type consistency:** `build_catalog_schema(&[ColumnDef], &[TableConstraint], &str) -> (Vec<catalog::Column>, Vec<catalog::Constraint>)`, `eval_default(&Table, usize, &EvalCtx) -> Result<Datum, ExecError>`, `check_row(&Table, &[Datum], &EvalCtx) -> Result<(), ExecError>`, `serialize_schema(TableId, &[Column], Option<&ForeignTableMeta>, &[Constraint])`, `deserialize_schema(&[u8]) -> (TableId, Vec<Column>, Option<ForeignTableMeta>, Vec<Constraint>)`, `create_table_ops(kv, &str, Vec<Column>, Vec<Constraint>)` — used consistently across tasks. `ExecError::{NotNullViolation, CheckViolation, InvalidTableDefinition}` defined in Task 6 and used in 7-8.

**Note for the implementer:** several steps end with "search `grep …` and update all call sites." These are real — extending `Column`/`Table`/`ColumnDef`/`CreateTable`/`create_table_ops`/`deserialize_schema` touches every constructor across the workspace. Task 11 Step 1 is the backstop that the whole workspace compiles.
