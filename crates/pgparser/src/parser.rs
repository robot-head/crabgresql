//! Recursive-descent statement parser with Pratt expression parsing.

use std::cell::Cell;
use std::rc::Rc;

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::error::ParseError;
use crate::lexer::lex;
use crate::token::{Keyword, Token};

/// Maximum nesting depth the parser will build before returning `54001`
/// (statement_too_complex). This bounds BOTH crash modes:
///   * mode 1 — deep parse recursion (nested parens / subqueries / CASE / NOT /
///     unary minus, all of which funnel through `expr`/`select_inner`), and
///   * mode 2 — a flat left-associative chain (`1+1+1+…`) whose Pratt loop is
///     iterative but builds an N-deep left-nested AST that would overflow later
///     in eval AND on recursive `Box` `Drop`; capping the loop iteration count
///     stops the over-deep tree from ever being built.
///
/// Chosen empirically (see the `at_limit_*` crash-safety tests): the server runs
/// on tokio's default ~2 MiB worker stack, and a query nested at `MAX_DEPTH` must
/// parse AND evaluate without overflowing while a deeper one returns a clean
/// error. Measured on that 2 MiB stack (both plain-debug AND llvm-cov-
/// instrumented builds, since CI runs `cargo llvm-cov nextest`), a deeply-nested
/// `(((…)))` paren parse — the heaviest recursion, an `expr`→`prefix`→`expr`
/// round-trip per level — overflows the stack at a nesting depth of ~133. `50`
/// leaves ~2.6x headroom below that ceiling; the executor's eval recursion
/// (ceiling >12 000 on the same stack) and the AST's recursive `Box` `Drop` are
/// nowhere near it. Real queries nest well under ~50 levels. This cap is
/// deliberately MUCH more conservative than PostgreSQL's own (far higher)
/// `max_stack_depth` — both return `54001` for sufficiently deep input, which is
/// what matters for closing the DoS.
pub(crate) const MAX_DEPTH: usize = 50;

pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    pos: usize,
    /// Current recursion depth of the recursive productions (`expr`,
    /// `select_inner`). Held behind an `Rc<Cell<…>>` so the RAII [`DepthGuard`]
    /// can hold an OWNED clone of the handle rather than a borrow of `self` —
    /// that lets the guarded method keep calling `&mut self` methods freely while
    /// the guard is alive (a `&self.depth` borrow would conflict with `&mut self`
    /// for the guard's whole lifetime). The guard's `Drop` decrements on EVERY
    /// exit path, including a `?` early-return, so the depth is always restored.
    depth: Rc<Cell<usize>>,
}

/// RAII depth counter: increments the shared depth `Cell` on construction and
/// decrements it on `Drop` (so a `?` early-return still restores the count).
/// Holds an owned `Rc` clone, so it does not borrow the `Parser` and never fights
/// the borrow checker with the `&mut self` method calls in the guarded body.
struct DepthGuard {
    depth: Rc<Cell<usize>>,
}

impl DepthGuard {
    /// Enter one recursion level, erroring with `54001` if it would exceed
    /// `MAX_DEPTH`. On error the guard is NOT created (the count is not bumped for
    /// a frame that never ran); the caller returns the error immediately.
    fn enter(depth: &Rc<Cell<usize>>, position: usize) -> Result<Self, ParseError> {
        let next = depth.get() + 1;
        if next > MAX_DEPTH {
            return Err(ParseError::too_deep(position));
        }
        depth.set(next);
        Ok(Self {
            depth: Rc::clone(depth),
        })
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        self.depth.set(self.depth.get() - 1);
    }
}

impl Parser {
    pub(crate) fn new(toks: Vec<(Token, usize)>) -> Self {
        Self {
            toks,
            pos: 0,
            depth: Rc::new(Cell::new(0)),
        }
    }

    fn peek(&self) -> &Token {
        &self.toks[self.pos].0
    }

    /// The token *after* the current one (saturates at EOF). Used for the SP28
    /// two-token lookahead that disambiguates infix `NOT IN`/`NOT BETWEEN`/
    /// `NOT LIKE` from the prefix `NOT` operator.
    fn peek2(&self) -> &Token {
        let i = (self.pos + 1).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    /// The token `n` positions ahead of the current one (saturates at EOF).
    fn peek_n(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    /// The token two positions after the current one (saturates at EOF). Used by
    /// the SP37 `AT TIME ZONE` postfix, whose three-token lead-in (`at time zone`)
    /// needs a three-token lookahead so a bare column named `at` is never mistaken
    /// for the operator.
    fn peek3(&self) -> &Token {
        let i = (self.pos + 2).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    fn peek_pos(&self) -> usize {
        self.toks[self.pos].1
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if *self.peek() == Token::Keyword(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, want: &Token) -> Result<(), ParseError> {
        if self.peek() == want {
            self.bump();
            Ok(())
        } else {
            Err(ParseError::new(
                format!("expected {want:?}, found {:?}", self.peek()),
                self.peek_pos(),
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s),
            other => Err(ParseError::new(
                format!("expected identifier, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// Consume an identifier that must equal `want` (case-insensitively). Used by
    /// the SP37 keyword-free multi-word type fold (`time`/`zone`) so those words
    /// stay non-reserved (still usable as identifiers elsewhere).
    fn expect_ident_eq(&mut self, want: &str) -> Result<(), ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::Ident(s) if s.eq_ignore_ascii_case(want) => Ok(()),
            other => Err(ParseError::new(
                format!("expected `{want}`, found {other:?}"),
                pos,
            )),
        }
    }

    /// Parse a SQL type name into a [`ColumnType`] — shared by `CREATE TABLE`
    /// column definitions and the SP31 cast target (`CAST(_ AS ty)` / `_::ty`).
    /// Folds the two-word `double precision` (SP30) into one normalized name; an
    /// unknown type name is a 42601 parse error (PostgreSQL: 42704 — a documented
    /// deviation, consistent with the column-type path).
    fn parse_type_name(&mut self) -> Result<pgtypes::ColumnType, ParseError> {
        let type_pos = self.peek_pos();
        let mut type_word = self.expect_ident()?;
        if type_word.eq_ignore_ascii_case("double")
            && matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("precision"))
        {
            self.bump();
            type_word = "double precision".to_string();
        }
        // SP37: fold the multi-word `timestamp`/`time` { with | without } `time zone`
        // spellings into one normalized name (keyword-free — the lexer lowercases
        // idents, so the three trailing words are matched as plain `Token::Ident`s).
        // `timestamp with time zone` / `timestamp without time zone` /
        // `time with time zone` / `time without time zone`.
        if (type_word.eq_ignore_ascii_case("timestamp") || type_word.eq_ignore_ascii_case("time"))
            && matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("with") || w.eq_ignore_ascii_case("without"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
        {
            // Consume `{with|without}`; require `time` `zone` to follow.
            let with_zone =
                matches!(self.bump(), Token::Ident(w) if w.eq_ignore_ascii_case("with"));
            self.expect_ident_eq("time")?;
            self.expect_ident_eq("zone")?;
            let qualifier = if with_zone { "with" } else { "without" };
            type_word = format!("{} {qualifier} time zone", type_word.to_ascii_lowercase());
        }
        let ty = pgtypes::ColumnType::from_sql_name(&type_word)
            .ok_or_else(|| ParseError::new(format!("unknown type \"{type_word}\""), type_pos))?;
        // SP32: `numeric`/`decimal` may carry a `(precision[, scale])` modifier.
        if ty.is_numeric() && *self.peek() == Token::LParen {
            return self.parse_numeric_typmod();
        }
        Ok(ty)
    }

    /// Parse a `numeric(precision[, scale])` modifier, positioned at `(`. `scale`
    /// defaults to 0 (PostgreSQL `numeric(p)` ≡ `numeric(p, 0)`).
    fn parse_numeric_typmod(&mut self) -> Result<pgtypes::ColumnType, ParseError> {
        self.expect(&Token::LParen)?;
        let precision = self.expect_u16("numeric precision")?;
        let scale = if self.eat_comma() {
            self.expect_u16("numeric scale")?
        } else {
            0
        };
        self.expect(&Token::RParen)?;
        Ok(pgtypes::ColumnType::Numeric(Some(
            pgtypes::numeric::Typmod { precision, scale },
        )))
    }

    /// Parse a small unsigned integer literal (a `numeric` precision/scale).
    fn expect_u16(&mut self, what: &str) -> Result<u16, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(s) => s
                .parse::<u16>()
                .map_err(|_| ParseError::new(format!("invalid {what}"), pos)),
            other => Err(ParseError::new(
                format!("expected {what}, found {other:?}"),
                pos,
            )),
        }
    }

    /// Pratt expression parser. `min_bp` is the minimum left binding power.
    pub(crate) fn expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        // Mode-1 guard: every recursive expression production (parens, NOT, unary
        // minus, CASE, CAST, IN-list, BETWEEN, LIKE, function args, subqueries)
        // funnels back through `expr`, so bounding the recursion depth here caps
        // all of them. The RAII guard decrements on every exit path, `?` included.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        let mut lhs = self.prefix()?;
        // Mode-2 guard: the Pratt loop is iterative, but each iteration adds one
        // level of left-nesting to the result tree (`1+1+1+…`). Capping the
        // iteration count caps the built tree's depth, so it can never grow deep
        // enough to overflow eval/fold/router-walk or recursive `Box` `Drop`.
        let mut iterations: usize = 0;
        loop {
            iterations += 1;
            if iterations > MAX_DEPTH {
                return Err(ParseError::too_deep(self.peek_pos()));
            }
            // SP31: `::` is the tightest-binding operator (tighter than unary
            // minus and every arithmetic/comparison operator), so it is consumed
            // unconditionally here — no `min_bp` gate — and left-associatively
            // (`a::int::text` == `(a::int)::text`). `-2::int` still parses as
            // `-(2::int)` because the unary-minus prefix recurses into `expr`,
            // whose innermost frame grabs the `::` before the minus is applied.
            if *self.peek() == Token::TypeCast {
                self.bump();
                let ty = self.parse_type_name()?;
                lhs = Expr::Cast {
                    expr: Box::new(lhs),
                    ty,
                };
                continue;
            }
            // SP37: `x AT TIME ZONE z` — a postfix operator that lowers onto PG's
            // internal `timezone(z, x)` form (note arg ORDER: zone first, value
            // second). It binds TIGHTER than every binary operator (so
            // `ts AT TIME ZONE 'UTC' = y` groups as `(ts AT TIME ZONE 'UTC') = y`),
            // so — like `::` — it is consumed unconditionally (no `min_bp` gate).
            // Keyword-free: `at`/`time`/`zone` are matched as lowercased idents via
            // a three-token lookahead, so a bare column named `at` is never the
            // operator. The zone operand is parsed at bp 11 (a high-precedence
            // operand, like the `*`/`/` level), and recursion terminates because
            // each iteration consumes the `at time zone` lead-in before recursing.
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("at"))
                && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
                && matches!(self.peek3(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
            {
                self.bump(); // at
                self.bump(); // time
                self.bump(); // zone
                let zone = self.expr(11)?;
                lhs = Expr::Func(crate::ast::FuncCall {
                    name: "timezone".into(),
                    distinct: false,
                    args: crate::ast::FuncArgs::Exprs(vec![zone, lhs]),
                });
                continue;
            }
            // SP28: postfix predicates (IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN,
            // [NOT] LIKE/ILIKE) bind at the comparison level (l_bp = 5). They are
            // handled before the binary-operator match so `a = 1 AND b IN (1,2)`
            // groups as `(a=1) AND (b IN (1,2))`.
            if 5 >= min_bp {
                match self.peek() {
                    Token::Keyword(Keyword::Is) => {
                        lhs = self.parse_is_null(lhs)?;
                        continue;
                    }
                    Token::Keyword(Keyword::In) => {
                        lhs = self.parse_in(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Between) => {
                        lhs = self.parse_between(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Like) => {
                        lhs = self.parse_like(lhs, false, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Ilike) => {
                        lhs = self.parse_like(lhs, false, true)?;
                        continue;
                    }
                    // Infix `NOT` only when it leads a negated predicate
                    // (`x NOT IN/BETWEEN/LIKE/ILIKE …`); otherwise `NOT` is the
                    // prefix operator handled in `prefix`. Two-token lookahead.
                    Token::Keyword(Keyword::Not)
                        if matches!(
                            self.peek2(),
                            Token::Keyword(
                                Keyword::In | Keyword::Between | Keyword::Like | Keyword::Ilike
                            )
                        ) =>
                    {
                        self.bump(); // NOT
                        lhs = match self.peek() {
                            Token::Keyword(Keyword::In) => self.parse_in(lhs, true)?,
                            Token::Keyword(Keyword::Between) => self.parse_between(lhs, true)?,
                            Token::Keyword(Keyword::Like) => self.parse_like(lhs, true, false)?,
                            Token::Keyword(Keyword::Ilike) => self.parse_like(lhs, true, true)?,
                            _ => unreachable!("lookahead guaranteed a negated predicate"),
                        };
                        continue;
                    }
                    _ => {}
                }
            }
            // SP29 inserts `||` (BinaryOp::Concat) between the comparison level
            // (5/6) and the additive level: like PostgreSQL, `||` binds TIGHTER
            // than `< > = <= >= <>`, `BETWEEN/IN/LIKE`, `AND`/`OR` but LOOSER than
            // `+ - * /`. So `+ - * /` and the unary-minus operand power shift up by
            // two to make room (odd l_bp / even r_bp preserved).
            let (op, l_bp, r_bp) = match self.peek() {
                Token::Keyword(Keyword::Or) => (BinaryOp::Or, 1, 2),
                Token::Keyword(Keyword::And) => (BinaryOp::And, 3, 4),
                Token::Eq => (BinaryOp::Eq, 5, 6),
                Token::Ne => (BinaryOp::Ne, 5, 6),
                Token::Lt => (BinaryOp::Lt, 5, 6),
                Token::Le => (BinaryOp::Le, 5, 6),
                Token::Gt => (BinaryOp::Gt, 5, 6),
                Token::Ge => (BinaryOp::Ge, 5, 6),
                Token::Concat => (BinaryOp::Concat, 7, 8),
                Token::Plus => (BinaryOp::Add, 9, 10),
                Token::Minus => (BinaryOp::Sub, 9, 10),
                Token::Star => (BinaryOp::Mul, 11, 12),
                Token::Slash => (BinaryOp::Div, 11, 12),
                _ => break,
            };
            if l_bp < min_bp {
                break;
            }
            self.bump();
            // SP34: `op ANY|SOME|ALL ( SELECT … )` — a quantified comparison. Only
            // the comparison operators take a quantifier (PostgreSQL).
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) && matches!(
                self.peek(),
                Token::Keyword(Keyword::Any | Keyword::Some | Keyword::All)
            ) {
                let all = matches!(self.peek(), Token::Keyword(Keyword::All));
                self.bump(); // ANY / SOME / ALL
                self.expect(&Token::LParen)?;
                let sub = self.select_inner()?;
                self.expect(&Token::RParen)?;
                lhs = Expr::Quantified {
                    expr: Box::new(lhs),
                    op,
                    all,
                    subquery: Box::new(sub),
                };
                continue;
            }
            let rhs = self.expr(r_bp)?;
            lhs = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Keyword(Keyword::Not) => {
                self.bump();
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(self.expr(4)?),
                })
            }
            Token::Minus => {
                self.bump();
                // Unary minus binds tighter than `* /` (now 11/12), so its operand
                // is parsed above that level (13) — `-a * b` stays `(-a) * b`.
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(self.expr(13)?),
                })
            }
            Token::LParen => {
                // SP34: `( SELECT … )` is a scalar subquery; anything else is a
                // parenthesised (grouping) expression.
                if *self.peek2() == Token::Keyword(Keyword::Select) {
                    self.bump(); // (
                    let sub = self.select_inner()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::ScalarSubquery(Box::new(sub)))
                } else {
                    self.bump();
                    let e = self.expr(0)?;
                    self.expect(&Token::RParen)?;
                    Ok(e)
                }
            }
            Token::Keyword(Keyword::Exists) => {
                self.bump(); // EXISTS
                self.expect(&Token::LParen)?;
                let sub = self.select_inner()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Exists(Box::new(sub)))
            }
            Token::IntLit(s) => {
                self.bump();
                Ok(Expr::IntLiteral(s))
            }
            Token::FloatLit(s) => {
                self.bump();
                Ok(Expr::NumericLiteral(s))
            }
            Token::StringLit(s) => {
                self.bump();
                Ok(Expr::StringLiteral(s))
            }
            Token::Keyword(Keyword::True) => {
                self.bump();
                Ok(Expr::BoolLiteral(true))
            }
            Token::Keyword(Keyword::False) => {
                self.bump();
                Ok(Expr::BoolLiteral(false))
            }
            Token::Keyword(Keyword::Null) => {
                self.bump();
                Ok(Expr::NullLiteral)
            }
            Token::Keyword(Keyword::Case) => self.case_expr(),
            Token::Keyword(Keyword::Cast) => self.cast_expr(),
            // `left`/`right` are PostgreSQL scalar functions AND (LEFT/RIGHT) join
            // keywords. In expression position they are valid only as a function
            // call — `left(s, n)` / `right(s, n)` — so route them to `func_call`.
            Token::Keyword(Keyword::Left) => self.keyword_func_call("left"),
            Token::Keyword(Keyword::Right) => self.keyword_func_call("right"),
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            Token::Ident(s) => {
                // SP37: a typed datetime literal — `DATE '…'` / `TIME '…'` /
                // `TIMESTAMP '…'` / `TIMESTAMPTZ '…'` / `INTERVAL '…'` — lowers onto
                // an explicit cast of a string literal. Only the single-word
                // spellings are typed-literal prefixes (multi-word `TIMESTAMP WITH
                // TIME ZONE '…'` is out of scope — use `'…'::timestamptz`). This is
                // checked BEFORE the function-call path so `date('…')` is not
                // shadowed (a typed literal has NO parenthesis — `peek2` is the
                // string literal, not `(`).
                let lower = s.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "date" | "time" | "timestamp" | "timestamptz" | "interval"
                ) && matches!(self.peek2(), Token::StringLit(_))
                {
                    self.bump(); // the type-name ident
                    let ty = pgtypes::ColumnType::from_sql_name(&lower)
                        .expect("single-word datetime type name resolves");
                    let string = match self.bump() {
                        Token::StringLit(v) => v,
                        _ => unreachable!("peek2 guaranteed a string literal"),
                    };
                    return Ok(Expr::Cast {
                        expr: Box::new(Expr::StringLiteral(string)),
                        ty,
                    });
                }
                self.bump();
                // SP37: niladic keyword functions — `current_date`, `current_time`,
                // `localtimestamp`, `localtime`, `current_timestamp` — have NO
                // parentheses. When one of these names is NOT followed by `(`, build
                // a zero-arg `Func` call (the executor resolves it against the session
                // clock/zone). These names are effectively reserved in PostgreSQL, so
                // shadowing a column of the same name is acceptable. The paren forms
                // (`now()`, `current_timestamp(0)`, etc.) fall through to `func_call`.
                if matches!(
                    lower.as_str(),
                    "current_date"
                        | "current_time"
                        | "localtimestamp"
                        | "localtime"
                        | "current_timestamp"
                ) && *self.peek() != Token::LParen
                {
                    return Ok(Expr::Func(crate::ast::FuncCall {
                        name: lower,
                        distinct: false,
                        args: crate::ast::FuncArgs::Exprs(vec![]),
                    }));
                }
                // SP37: `EXTRACT(field FROM source)` — a special call form that
                // lowers onto `extract('<field>', source)` (field lowercased to a
                // string literal). Checked before the generic comma-arg `func_call`
                // so the `FROM` keyword inside the parens is not mis-parsed.
                if lower == "extract" && *self.peek() == Token::LParen {
                    return self.extract_expr();
                }
                // SP27: `ident (` is a function call; a bare ident is a column.
                // SP33: `ident . ident` is a table-qualified column reference.
                if *self.peek() == Token::LParen {
                    self.func_call(s)
                } else if *self.peek() == Token::Dot {
                    self.bump();
                    let name = self.expect_ident()?;
                    Ok(Expr::Column {
                        table: Some(s),
                        name,
                    })
                } else {
                    Ok(Expr::Column {
                        table: None,
                        name: s,
                    })
                }
            }
            other => Err(ParseError::new(
                format!("unexpected token {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// Parse a function call after its name `ident`, positioned at `(`.
    /// `f(*)` yields [`FuncArgs::Star`]; `DISTINCT`/`ALL` may lead the argument
    /// list; otherwise a (possibly empty) comma-separated expression list.
    fn func_call(&mut self, name: String) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};
        self.expect(&Token::LParen)?;
        // `f(*)` — the star form (no DISTINCT, no other args).
        if *self.peek() == Token::Star {
            self.bump();
            self.expect(&Token::RParen)?;
            return Ok(Expr::Func(FuncCall {
                name,
                distinct: false,
                args: FuncArgs::Star,
            }));
        }
        let distinct = if self.eat_keyword(Keyword::Distinct) {
            true
        } else {
            // ALL is the default modifier; accept and ignore it.
            self.eat_keyword(Keyword::All);
            false
        };
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                args.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::Func(FuncCall {
            name,
            distinct,
            args: FuncArgs::Exprs(args),
        }))
    }

    /// A keyword that doubles as a scalar function name (`left`/`right`, which are
    /// also join keywords) used in expression position: positioned at the keyword,
    /// it is valid only as a function call `kw (`.
    fn keyword_func_call(&mut self, name: &str) -> Result<Expr, ParseError> {
        self.bump();
        if *self.peek() == Token::LParen {
            self.func_call(name.to_string())
        } else {
            Err(ParseError::new(
                format!("`{name}` is reserved here; use it as a function call `{name}(...)`"),
                self.peek_pos(),
            ))
        }
    }

    /// `EXTRACT(field FROM source)` — positioned at `(`, after the `extract` ident.
    /// Lowers onto PostgreSQL's internal `extract('<field>', source)` form: the
    /// field is an identifier (lowercased to a string literal), the source is a
    /// full expression. The executor resolves the field at runtime.
    fn extract_expr(&mut self) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};
        self.expect(&Token::LParen)?;
        let field = self.expect_ident()?.to_ascii_lowercase();
        self.expect(&Token::Keyword(Keyword::From))?;
        let source = self.expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Func(FuncCall {
            name: "extract".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::StringLiteral(field), source]),
        }))
    }

    /// `expr IS [NOT] NULL`, positioned at `IS`. (`IS TRUE`/`IS DISTINCT FROM`
    /// are out of scope — anything but `NULL` after `IS`/`IS NOT` is a 42601.)
    fn parse_is_null(&mut self, lhs: Expr) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Is))?;
        let negated = self.eat_keyword(Keyword::Not);
        self.expect(&Token::Keyword(Keyword::Null))?;
        Ok(Expr::IsNull {
            expr: Box::new(lhs),
            negated,
        })
    }

    /// `expr [NOT] IN (e1, e2, …)` or `expr [NOT] IN (SELECT …)`, positioned at
    /// `IN`. The value-list form has ≥1 element (`IN ()` is a 42601, matching
    /// PostgreSQL); the `SELECT` form (SP34) is a single-column subquery.
    fn parse_in(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::In))?;
        self.expect(&Token::LParen)?;
        // SP34: `IN ( SELECT … )` is a subquery; otherwise a value list.
        if *self.peek() == Token::Keyword(Keyword::Select) {
            let sub = self.select_inner()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: Box::new(sub),
                negated,
            });
        }
        let mut list = Vec::new();
        loop {
            list.push(self.expr(0)?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::InList {
            expr: Box::new(lhs),
            list,
            negated,
        })
    }

    /// `expr [NOT] BETWEEN low AND high`, positioned at `BETWEEN`. The bounds are
    /// parsed at `min_bp = 4` so the separating `AND` (left bp 3) is NOT consumed
    /// as a boolean `AND`; thus `a BETWEEN 1 AND 2 AND b` → `(a BETWEEN 1 AND 2) AND b`.
    fn parse_between(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Between))?;
        let low = self.expr(4)?;
        self.expect(&Token::Keyword(Keyword::And))?;
        let high = self.expr(4)?;
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    /// `expr [NOT] LIKE pat` / `[NOT] ILIKE pat`, positioned at `LIKE`/`ILIKE`.
    /// The pattern is parsed at `min_bp = 6` (the right bp of the comparison
    /// level) so it stays a single comparand and does not swallow a trailing
    /// `AND`/`OR`.
    fn parse_like(
        &mut self,
        lhs: Expr,
        negated: bool,
        case_insensitive: bool,
    ) -> Result<Expr, ParseError> {
        self.bump(); // LIKE or ILIKE
        let pattern = self.expr(6)?;
        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            negated,
            case_insensitive,
        })
    }

    /// A `CASE` expression. Simple form (`CASE x WHEN v THEN r …`) carries an
    /// operand; searched form (`CASE WHEN cond THEN r …`) does not. At least one
    /// `WHEN` is required; `ELSE` is optional.
    fn case_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Case))?;
        let operand = if *self.peek() == Token::Keyword(Keyword::When) {
            None
        } else {
            Some(Box::new(self.expr(0)?))
        };
        let mut whens = Vec::new();
        while self.eat_keyword(Keyword::When) {
            let cond = self.expr(0)?;
            self.expect(&Token::Keyword(Keyword::Then))?;
            let result = self.expr(0)?;
            whens.push((cond, result));
        }
        if whens.is_empty() {
            return Err(ParseError::new(
                "CASE requires at least one WHEN clause",
                self.peek_pos(),
            ));
        }
        let else_result = if self.eat_keyword(Keyword::Else) {
            Some(Box::new(self.expr(0)?))
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::End))?;
        Ok(Expr::Case {
            operand,
            whens,
            else_result,
        })
    }

    /// `CAST(expr AS type)` — positioned at `CAST`. The functional spelling of
    /// the `::` operator; the inner expression is parsed at the lowest precedence
    /// (it is delimited by the surrounding parens).
    fn cast_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Cast))?;
        self.expect(&Token::LParen)?;
        let expr = self.expr(0)?;
        self.expect(&Token::Keyword(Keyword::As))?;
        let ty = self.parse_type_name()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            ty,
        })
    }

    pub(crate) fn program(&mut self) -> Result<Vec<crate::ast::Statement>, ParseError> {
        Ok(self
            .program_spanned()?
            .into_iter()
            .map(|(s, _)| s)
            .collect())
    }

    /// Like `program`, but pairs each statement with the byte range of its source in
    /// the original input — from its first token's offset up to the trailing `;`
    /// (or end of input). Powers [`parse_with_source`].
    pub(crate) fn program_spanned(
        &mut self,
    ) -> Result<Vec<(crate::ast::Statement, std::ops::Range<usize>)>, ParseError> {
        use crate::ast::Statement;
        let mut stmts: Vec<(Statement, std::ops::Range<usize>)> = Vec::new();
        loop {
            while *self.peek() == Token::Semicolon {
                self.bump();
            }
            if *self.peek() == Token::Eof {
                break;
            }
            let start = self.peek_pos();
            let s = self.statement()?;
            let end = self.peek_pos();
            stmts.push((s, start..end));
            match self.peek() {
                Token::Semicolon => {
                    self.bump();
                }
                Token::Eof => break,
                other => {
                    return Err(ParseError::new(
                        format!("expected ; or end of input, found {other:?}"),
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(stmts)
    }

    fn statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Create) => self.create_table(),
            Token::Keyword(Keyword::Drop) => self.drop_table(),
            Token::Keyword(Keyword::Insert) => self.insert(),
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values) | Token::LParen => {
                self.query_stmt()
            }
            // SP4: transaction control
            Token::Keyword(Keyword::Begin) | Token::Keyword(Keyword::Start) => self.begin(),
            Token::Keyword(Keyword::Commit) | Token::Keyword(Keyword::End) => {
                self.bump();
                Ok(crate::ast::Statement::Commit)
            }
            Token::Keyword(Keyword::Rollback) | Token::Keyword(Keyword::Abort) => {
                self.bump();
                Ok(crate::ast::Statement::Rollback)
            }
            // SP4: DML
            Token::Keyword(Keyword::Update) => self.update(),
            Token::Keyword(Keyword::Delete) => self.delete(),
            // SP37: GUC control. `SET` is a keyword; `SHOW`/`RESET` are matched as
            // plain (lowercased) idents — keyword-free so they stay usable as names.
            Token::Keyword(Keyword::Set) => self.set_stmt(),
            Token::Ident(s) if s == "show" => self.show_stmt(),
            Token::Ident(s) if s == "reset" => self.reset_stmt(),
            other => Err(ParseError::new(
                format!("unexpected statement start {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// SP37: `SET [LOCAL] <name> (= | TO) <value>` / `SET [LOCAL] TIME ZONE <value>`.
    /// Keyword-free for `LOCAL`/`TO`/`TIME ZONE`/`DEFAULT`/`LOCAL` (the value) —
    /// they are matched as lowercased idents, so none becomes a reserved keyword.
    /// The GUC name is normalized to lowercase; `TIME ZONE` normalizes to
    /// `"timezone"`.
    fn set_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Set))?;
        // `LOCAL` is the flag only when it leads and is followed by a parameter
        // name (an ident or `TIME ZONE`). It is NEVER a flag after `TIME ZONE`
        // (there it is the value `LOCAL`), and the `set_stmt` entry is before any
        // `TIME ZONE` is consumed, so a leading `LOCAL` here is unambiguous.
        let local = matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("local"))
            && !matches!(self.peek2(), Token::Eq);
        if local {
            self.bump(); // LOCAL
        }
        // The `TIME ZONE` special spelling: `SET [LOCAL] TIME ZONE <value>`.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            let value = self.set_time_zone_value()?;
            return Ok(Statement::Set {
                local,
                name: "timezone".into(),
                value,
            });
        }
        // `SET [LOCAL] <name> (= | TO) <value>`.
        let name = self.expect_ident()?.to_ascii_lowercase();
        // `=` is a token; `TO` is a (lowercased) ident — either separates name from value.
        let sep = *self.peek() == Token::Eq
            || matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("to"));
        if !sep {
            return Err(ParseError::new(
                "expected `=` or `TO` in SET",
                self.peek_pos(),
            ));
        }
        self.bump(); // = or TO
        let value = self.set_value()?;
        Ok(Statement::Set { local, name, value })
    }

    /// The value after `=`/`TO`: a string literal, a `DEFAULT` ident (→ Default),
    /// or any other identifier (→ that ident verbatim).
    fn set_value(&mut self) -> Result<crate::ast::SetValue, ParseError> {
        use crate::ast::SetValue;
        match self.peek().clone() {
            Token::StringLit(s) => {
                self.bump();
                Ok(SetValue::Value(s))
            }
            Token::Ident(w) if w.eq_ignore_ascii_case("default") => {
                self.bump();
                Ok(SetValue::Default)
            }
            Token::Ident(w) => {
                self.bump();
                Ok(SetValue::Value(w))
            }
            other => Err(ParseError::new(
                format!("expected a SET value, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// The value after `SET [LOCAL] TIME ZONE`: like [`set_value`], but the bare
    /// idents `LOCAL` and `DEFAULT` both mean "reset to default" (PostgreSQL).
    fn set_time_zone_value(&mut self) -> Result<crate::ast::SetValue, ParseError> {
        use crate::ast::SetValue;
        match self.peek().clone() {
            Token::StringLit(s) => {
                self.bump();
                Ok(SetValue::Value(s))
            }
            Token::Ident(w)
                if w.eq_ignore_ascii_case("default") || w.eq_ignore_ascii_case("local") =>
            {
                self.bump();
                Ok(SetValue::Default)
            }
            Token::Ident(w) => {
                self.bump();
                Ok(SetValue::Value(w))
            }
            other => Err(ParseError::new(
                format!("expected a TIME ZONE value, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// SP37: `SHOW <name>` / `SHOW TIME ZONE`. Positioned at the `show` ident.
    fn show_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.bump(); // show
        // `SHOW TIME ZONE` → name `"timezone"`.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            return Ok(Statement::Show {
                name: "timezone".into(),
            });
        }
        let name = self.expect_ident()?.to_ascii_lowercase();
        Ok(Statement::Show { name })
    }

    /// SP37: `RESET <name>`. Positioned at the `reset` ident.
    fn reset_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.bump(); // reset
        // `RESET TIME ZONE` → name `"timezone"` (symmetry with SHOW; PG accepts it).
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            return Ok(Statement::Reset {
                name: "timezone".into(),
            });
        }
        let name = self.expect_ident()?.to_ascii_lowercase();
        Ok(Statement::Reset { name })
    }

    fn begin(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{IsolationLevel, Statement};
        let leading = self.bump(); // BEGIN or START
        if leading == Token::Keyword(Keyword::Start) {
            // START TRANSACTION is valid; bare START is not a statement.
            self.expect(&Token::Keyword(Keyword::Transaction))?;
        } else {
            // TRANSACTION is optional after BEGIN.
            self.eat_keyword(Keyword::Transaction);
        }
        let isolation = if self.eat_keyword(Keyword::Isolation) {
            self.expect(&Token::Keyword(Keyword::Level))?;
            if self.eat_keyword(Keyword::Repeatable) {
                self.expect(&Token::Keyword(Keyword::Read))?;
                Some(IsolationLevel::RepeatableRead)
            } else if self.eat_keyword(Keyword::Read) {
                self.expect(&Token::Keyword(Keyword::Committed))?;
                Some(IsolationLevel::ReadCommitted)
            } else {
                return Err(ParseError::new(
                    "expected REPEATABLE READ or READ COMMITTED",
                    self.peek_pos(),
                ));
            }
        } else {
            None
        };
        Ok(Statement::Begin { isolation })
    }

    fn update(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Update))?;
        let table = self.expect_ident()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.expr(0)?;
            assignments.push((col, value));
            if self.eat_comma() {
                continue;
            }
            break;
        }
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            filter,
        })
    }

    fn delete(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Delete))?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let table = self.expect_ident()?;
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Statement::Delete { table, filter })
    }

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
            columns.push(ColumnDef { name: col_name, ty });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(Statement::CreateTable { name, columns })
    }

    fn drop_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        Ok(Statement::DropTable {
            name: self.expect_ident()?,
        })
    }

    fn insert(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Insert))?;
        self.expect(&Token::Keyword(Keyword::Into))?;
        let table = self.expect_ident()?;
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
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    /// Parse a single SELECT body INCLUDING its trailing ORDER BY / LIMIT / OFFSET /
    /// locking. The refactor that split this into `select_core` + `parse_set_tail` +
    /// `parse_locking` leaves the recursive callers (derived tables, subquery
    /// expressions) unaffected: they still get a fully-parsed `SelectStmt` with any
    /// trailing ORDER BY / LIMIT / OFFSET / locking, exactly as before.
    fn select_inner(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        let mut s = self.select_core()?;
        let (order_by, limit, offset) = self.parse_set_tail()?;
        s.order_by = order_by;
        s.limit = limit;
        s.offset = offset;
        s.locking = self.parse_locking()?;
        Ok(s)
    }

    /// Parse projection → HAVING. Leaves order_by / limit / offset / locking empty;
    /// the caller (single SELECT or set-op query) owns the tail.
    fn select_core(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        use crate::ast::{SelectItem, SelectStmt};
        // Mode-1 depth guard: EVERY SELECT body funnels through `select_core` — a
        // top-level set-op branch (`set_primary → select_core`), a derived table,
        // or a scalar/IN/EXISTS subquery (`select_inner → select_core`) — so
        // guarding here bounds all nested-SELECT recursion (e.g. a derived-table
        // chain `( SELECT … FROM ( SELECT … ) )`). Subqueries also pass through
        // `expr` first; guarding both is belt-and-braces.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        self.expect(&Token::Keyword(Keyword::Select))?;
        // SP28: SELECT DISTINCT (ALL is the default modifier — accept and ignore).
        let distinct = self.eat_keyword(Keyword::Distinct);
        if !distinct {
            self.eat_keyword(Keyword::All);
        }
        let mut projection = Vec::new();
        if *self.peek() == Token::Star {
            self.bump();
            projection.push(SelectItem::Wildcard);
        } else {
            loop {
                // SP33: `a.*` qualified wildcard — only when the THREE tokens
                // `Ident Dot Star` line up (so bare `SELECT *` and `SELECT a.col`
                // are unaffected).
                if let Token::Ident(_) = self.peek()
                    && *self.peek_n(1) == Token::Dot
                    && *self.peek_n(2) == Token::Star
                {
                    let q = self.expect_ident()?;
                    self.bump(); // Dot
                    self.bump(); // Star
                    projection.push(SelectItem::QualifiedWildcard(q));
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
                let expr = self.expr(0)?;
                let alias = if self.eat_keyword(Keyword::As) {
                    Some(self.expect_ident()?)
                } else if let Token::Ident(_) = self.peek() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                projection.push(SelectItem::Expr { expr, alias });
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let from = if self.eat_keyword(Keyword::From) {
            self.parse_from()?
        } else {
            Vec::new()
        };
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        // SP27: GROUP BY <expr-list> then HAVING <expr>, between WHERE and ORDER BY.
        let mut group_by = Vec::new();
        if self.eat_keyword(Keyword::Group) {
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                group_by.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let having = if self.eat_keyword(Keyword::Having) {
            Some(self.expr(0)?)
        } else {
            None
        };
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

    fn values_stmt(&mut self) -> Result<crate::ast::ValuesStmt, ParseError> {
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            if *self.peek() == Token::RParen {
                return Err(ParseError::new(
                    "VALUES row must have at least one expression",
                    self.peek_pos(),
                ));
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

    /// Parse an optional `ORDER BY …`, then `LIMIT`/`OFFSET` in either order.
    /// The tuple is the three result-level tail components (order_by, limit, offset);
    /// a named struct would not read more clearly than the positional triple.
    #[allow(clippy::type_complexity)]
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
        // SP28: LIMIT and OFFSET in either order (PostgreSQL accepts both).
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
                Err(ParseError::new(
                    "expected UPDATE or SHARE after FOR",
                    self.peek_pos(),
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// SP38: parse a full set-operation query (the statement entry for SELECT / `(`).
    /// `set_expr(0)` builds the operator tree; the trailing tail binds to the whole
    /// query. A lone Select (no set-op) collapses back to `Statement::Select` so the
    /// single-SELECT shape — including FOR UPDATE — is byte-for-byte unchanged.
    fn query_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
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
                Ok(Statement::SetOperation(SetQuery {
                    body,
                    order_by,
                    limit,
                    offset,
                }))
            }
        }
    }

    /// Precedence-climbing set-op tree. INTERSECT = 2, UNION/EXCEPT = 1; all
    /// left-associative (recurse for the RHS at `prec + 1`).
    fn set_expr(&mut self, min_prec: u8) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{SetExpr, SetOp};
        // Mode-1 guard: a parenthesized set-op subtree recurses
        // `set_primary → set_expr → set_primary` for `(((… query …)))`, a path that
        // does NOT funnel through `expr`/`select_core`, so it needs its own guard.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        let mut left = self.set_primary()?;
        // Mode-2 cap: a flat left-assoc chain `A UNION B UNION C …` is parsed by this
        // LOOP (not recursion), building an N-deep left-nested `SetExpr` that would
        // overflow the executor's `fold`/`resolve_set_columns` AND recursive `Drop`.
        // Capping the iterations prevents the over-deep tree (mirrors the Pratt loop).
        let mut iterations: usize = 0;
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
            iterations += 1;
            if iterations > MAX_DEPTH {
                return Err(ParseError::too_deep(self.peek_pos()));
            }
            self.bump(); // the operator keyword
            let all = self.eat_keyword(Keyword::All);
            if !all {
                self.eat_keyword(Keyword::Distinct); // explicit default modifier
            }
            let right = self.set_expr(prec + 1)?;
            left = SetExpr::SetOp {
                op,
                all,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// A set-op primary: a parenthesized sub-query (precedence grouping, or a
    /// parenthesized single SELECT that keeps its own ORDER BY / LIMIT), or a bare
    /// SELECT branch (`select_core`, no tail — the query owns the tail).
    fn set_primary(&mut self) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        if *self.peek() == Token::LParen {
            self.bump(); // (
            let inner = self.set_expr(0)?;
            let inner = self.attach_paren_tail(inner)?;
            self.expect(&Token::RParen)?;
            Ok(inner)
        } else if *self.peek() == Token::Keyword(Keyword::Values) {
            Ok(SetExpr::Query(QueryBody::Values(self.values_stmt()?)))
        } else {
            Ok(SetExpr::Query(QueryBody::Select(Box::new(
                self.select_core()?,
            ))))
        }
    }

    /// If an ORDER BY / LIMIT / OFFSET follows inside parentheses, attach it to a
    /// lone-SELECT inner; reject it on a multi-branch subtree (deferred).
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
            SetExpr::Query(QueryBody::Values(_)) => Err(ParseError::new(
                "ORDER BY/LIMIT on a parenthesized VALUES branch is not supported",
                self.peek_pos(),
            )),
            _ => Err(ParseError::new(
                "ORDER BY/LIMIT on a parenthesized set-operation subtree is not supported",
                self.peek_pos(),
            )),
        }
    }

    /// Parse the FROM clause: a comma-separated list of join trees.
    fn parse_from(&mut self) -> Result<Vec<crate::ast::TableExpr>, ParseError> {
        let mut items = vec![self.join_tree()?];
        while self.eat_comma() {
            items.push(self.join_tree()?);
        }
        Ok(items)
    }

    /// A left-associative chain of joins over table factors. `JOIN` binds tighter
    /// than the top-level comma (handled by `parse_from`).
    fn join_tree(&mut self) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        let mut left = self.table_factor()?;
        loop {
            let (kind, natural) = if self.eat_keyword(Keyword::Natural) {
                (self.join_kind()?, true)
            } else if self.peek_is_join_start() {
                (self.join_kind()?, false)
            } else {
                break;
            };
            let right = self.table_factor()?;
            let constraint = if natural || kind == JoinKind::Cross {
                if natural {
                    JoinConstraint::Natural
                } else {
                    JoinConstraint::None
                }
            } else if self.eat_keyword(Keyword::On) {
                JoinConstraint::On(self.expr(0)?)
            } else if self.eat_keyword(Keyword::Using) {
                self.expect(&Token::LParen)?;
                let mut cols = vec![self.expect_ident()?];
                while self.eat_comma() {
                    cols.push(self.expect_ident()?);
                }
                self.expect(&Token::RParen)?;
                JoinConstraint::Using(cols)
            } else {
                return Err(ParseError::new(
                    "expected ON or USING after JOIN",
                    self.peek_pos(),
                ));
            };
            left = TableExpr::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                constraint,
            };
        }
        Ok(left)
    }

    /// True if the next token begins a join clause (after an optional NATURAL).
    fn peek_is_join_start(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Join)
                | Token::Keyword(Keyword::Inner)
                | Token::Keyword(Keyword::Left)
                | Token::Keyword(Keyword::Right)
                | Token::Keyword(Keyword::Full)
                | Token::Keyword(Keyword::Cross)
        )
    }

    /// Consume a join-kind prefix and the `JOIN` keyword. `INNER`/`LEFT`/`RIGHT`/
    /// `FULL` may be followed by `OUTER`; a bare `JOIN` is INNER.
    fn join_kind(&mut self) -> Result<crate::ast::JoinKind, ParseError> {
        use crate::ast::JoinKind;
        let kind = if self.eat_keyword(Keyword::Inner) {
            JoinKind::Inner
        } else if self.eat_keyword(Keyword::Left) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Left
        } else if self.eat_keyword(Keyword::Right) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Right
        } else if self.eat_keyword(Keyword::Full) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Full
        } else if self.eat_keyword(Keyword::Cross) {
            JoinKind::Cross
        } else {
            JoinKind::Inner // a bare JOIN
        };
        self.expect(&Token::Keyword(Keyword::Join))?;
        Ok(kind)
    }

    /// A table factor: a base table (`t` / `t alias` / `t AS alias`), a derived
    /// table (`( SELECT … ) alias`), or a parenthesized join (`( … )`).
    fn table_factor(&mut self) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::TableExpr;
        if *self.peek() == Token::LParen {
            self.bump();
            if matches!(
                self.peek(),
                Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::Values)
            ) {
                use crate::ast::QueryBody;
                let subquery = if *self.peek() == Token::Keyword(Keyword::Select) {
                    QueryBody::Select(Box::new(self.select_inner()?))
                } else {
                    QueryBody::Values(self.values_stmt()?)
                };
                self.expect(&Token::RParen)?;
                let alias = self.opt_alias()?.ok_or_else(|| {
                    ParseError::new("subquery in FROM must have an alias", self.peek_pos())
                })?;
                let columns = self.opt_column_aliases()?;
                return Ok(TableExpr::Derived {
                    subquery,
                    alias,
                    columns,
                });
            }
            let inner = self.join_tree()?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        let name = self.expect_ident()?;
        let alias = self.opt_alias()?;
        Ok(TableExpr::Table { name, alias })
    }

    /// An optional table alias: `AS ident`, or a bare `ident` that is not a
    /// keyword (so `FROM t JOIN …` does not read `JOIN` as an alias).
    fn opt_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_keyword(Keyword::As) {
            return Ok(Some(self.expect_ident()?));
        }
        if let Token::Ident(_) = self.peek() {
            return Ok(Some(self.expect_ident()?));
        }
        Ok(None)
    }

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

    /// Parse the integer count after a `LIMIT`/`OFFSET` keyword (`what` names it
    /// in error messages).
    fn expect_int_count(&mut self, what: &str) -> Result<i64, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(s) => s
                .parse::<i64>()
                .map_err(|_| ParseError::new(format!("{what} value out of range"), pos)),
            other => Err(ParseError::new(
                format!("expected {what} count, found {other:?}"),
                pos,
            )),
        }
    }

    fn eat_comma(&mut self) -> bool {
        if *self.peek() == Token::Comma {
            self.bump();
            true
        } else {
            false
        }
    }
}

/// Test-support entry: parse a bare expression. `pub` (not cfg(test)) so the
/// executor crate's tests can reuse it; `doc(hidden)` keeps it out of the API.
#[doc(hidden)]
pub fn parse_expr_for_test(sql: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(lex(sql)?);
    let e = p.expr(0)?;
    if *p.peek() != Token::Eof {
        return Err(ParseError::new(
            "trailing tokens after expression",
            p.peek_pos(),
        ));
    }
    Ok(e)
}

/// Public statement entry — implemented in Task 12.
pub fn parse(sql: &str) -> Result<Vec<crate::ast::Statement>, ParseError> {
    let mut p = Parser::new(lex(sql)?);
    p.program()
}

/// Parse `sql` into statements, each paired with its EXACT source text — the byte
/// slice of `sql` spanning that statement, trimmed of surrounding whitespace. The
/// multi-range gateway uses this to forward an INDIVIDUAL statement (not the whole
/// `;`-separated simple-query frame) to a remote range's leader, so a frame mixing a
/// local and a remote range never re-runs the local statement on the remote node.
pub fn parse_with_source(sql: &str) -> Result<Vec<(crate::ast::Statement, String)>, ParseError> {
    let mut p = Parser::new(lex(sql)?);
    Ok(p.program_spanned()?
        .into_iter()
        .map(|(s, r)| (s, sql[r].trim().to_string()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, Expr, IsolationLevel, UnaryOp};
    use crate::ast::{ColumnDef, SelectItem, Statement};
    use pgtypes::ColumnType;

    fn one(sql: &str) -> Statement {
        let mut v = parse(sql).expect("parse");
        assert_eq!(v.len(), 1);
        v.pop().expect("one statement")
    }

    #[test]
    fn left_and_right_keywords_parse_as_functions_in_expression_position() {
        use crate::ast::{FuncArgs, FuncCall};
        // `LEFT`/`RIGHT` are join keywords, but in expression position they are
        // the scalar functions `left(s, n)` / `right(s, n)` (PostgreSQL allows it).
        for (sql, name) in [("left('abc', 2)", "left"), ("right('abc', 2)", "right")] {
            match parse_expr_for_test(sql).expect("parse fn") {
                Expr::Func(FuncCall {
                    name: n,
                    args: FuncArgs::Exprs(a),
                    ..
                }) => {
                    assert_eq!(n, name);
                    assert_eq!(a.len(), 2);
                }
                other => panic!("expected a function call, got {other:?}"),
            }
        }
        // A bare `left`/`right` not followed by `(` is rejected (still reserved).
        assert!(parse_expr_for_test("left + 1").is_err());
        // And `LEFT JOIN` still parses as a join (keyword role preserved).
        assert!(parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id").is_ok());
    }

    #[test]
    fn parse_with_source_pairs_each_statement_with_its_exact_text() {
        let v =
            parse_with_source("INSERT INTO a VALUES (1); INSERT INTO b VALUES (2)").expect("parse");
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].1, "INSERT INTO a VALUES (1)");
        assert_eq!(v[1].1, "INSERT INTO b VALUES (2)");
        // Surrounding whitespace (and the trailing `;`) is trimmed; a single
        // statement yields its own exact text.
        let solo = parse_with_source("  SELECT 1 ;  ").expect("parse one");
        assert_eq!(solo.len(), 1);
        assert_eq!(solo[0].1, "SELECT 1");
    }

    #[test]
    fn parses_create_table() {
        assert_eq!(
            one("CREATE TABLE t (id int4, name text)"),
            Statement::CreateTable {
                name: "t".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        ty: ColumnType::Int4
                    },
                    ColumnDef {
                        name: "name".into(),
                        ty: ColumnType::Text
                    },
                ],
            }
        );
    }

    #[test]
    fn unknown_column_type_is_error() {
        let e = parse("CREATE TABLE t (x widget)").expect_err("bad type");
        assert_eq!(e.sqlstate(), "42601");
    }

    #[test]
    fn parses_float8_column_types() {
        // SP30: `float8`, `float`, and the two-word `double precision` all map to Float8.
        for sql in [
            "CREATE TABLE t (x float8)",
            "CREATE TABLE t (x float)",
            "CREATE TABLE t (x double precision)",
        ] {
            match one(sql) {
                Statement::CreateTable { columns, .. } => {
                    assert_eq!(columns[0].ty, ColumnType::Float8, "for `{sql}`");
                }
                other => panic!("expected CreateTable, got {other:?}"),
            }
        }
        // Bare `double` (without `precision`) is not a type — PG rejects it too.
        assert!(parse("CREATE TABLE t (x double)").is_err());
    }

    #[test]
    fn parses_numeric_column_types_with_optional_typmod() {
        use pgtypes::numeric::Typmod;
        let ty = |sql: &str| match one(sql) {
            Statement::CreateTable { columns, .. } => columns[0].ty,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        // Unconstrained `numeric`/`decimal`.
        assert_eq!(ty("CREATE TABLE t (x numeric)"), ColumnType::Numeric(None));
        assert_eq!(ty("CREATE TABLE t (x decimal)"), ColumnType::Numeric(None));
        // `numeric(p)` ≡ scale 0; `numeric(p, s)`.
        assert_eq!(
            ty("CREATE TABLE t (x numeric(10))"),
            ColumnType::Numeric(Some(Typmod {
                precision: 10,
                scale: 0
            }))
        );
        assert_eq!(
            ty("CREATE TABLE t (x numeric(10, 2))"),
            ColumnType::Numeric(Some(Typmod {
                precision: 10,
                scale: 2
            }))
        );
        // The cast target accepts the same modifier.
        assert!(matches!(
            expr("x::numeric(5,1)"),
            Expr::Cast {
                ty: ColumnType::Numeric(Some(Typmod {
                    precision: 5,
                    scale: 1
                })),
                ..
            }
        ));
    }

    #[test]
    fn parses_niladic_keyword_functions_without_parens() {
        use crate::ast::{FuncArgs, FuncCall};
        // `current_date` etc. parse as zero-arg func calls (no parens).
        for name in [
            "current_date",
            "current_time",
            "localtimestamp",
            "localtime",
            "current_timestamp",
        ] {
            assert_eq!(
                expr(name),
                Expr::Func(FuncCall {
                    name: name.into(),
                    distinct: false,
                    args: FuncArgs::Exprs(vec![]),
                }),
                "niladic `{name}`"
            );
        }
        // The paren forms still parse via the normal func-call path.
        assert_eq!(
            expr("now()"),
            Expr::Func(FuncCall {
                name: "now".into(),
                distinct: false,
                args: FuncArgs::Exprs(vec![]),
            })
        );
        match expr("current_timestamp(0)") {
            Expr::Func(FuncCall { name, args, .. }) => {
                assert_eq!(name, "current_timestamp");
                assert!(matches!(args, FuncArgs::Exprs(ref v) if v.len() == 1));
            }
            other => panic!("expected a Func call, got {other:?}"),
        }
    }

    #[test]
    fn parses_numeric_literals() {
        // SP32: bare decimal/exponent literals are `numeric` (was `float8` in SP30).
        assert_eq!(expr("1.5"), Expr::NumericLiteral("1.5".into()));
        assert_eq!(expr(".25"), Expr::NumericLiteral(".25".into()));
        assert_eq!(expr("1e3"), Expr::NumericLiteral("1e3".into()));
        assert_eq!(expr("42"), Expr::IntLiteral("42".into()));
        // float participates in arithmetic with the usual precedence.
        match expr("1 + 2.5 * 2") {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => assert!(matches!(
                *right,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            )),
            other => panic!("expected Add(_, Mul), got {other:?}"),
        }
    }

    #[test]
    fn parses_drop_table() {
        assert_eq!(
            one("DROP TABLE t"),
            Statement::DropTable { name: "t".into() }
        );
    }

    #[test]
    fn parses_multi_row_insert_with_columns() {
        match one("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')") {
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table, "t");
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_with_all_clauses() {
        match one("SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10") {
            Statement::Select(s) => {
                assert_eq!(s.projection.len(), 2);
                assert!(
                    matches!(s.projection[1], SelectItem::Expr { alias: Some(ref n), .. } if n == "bee")
                );
                assert!(matches!(
                    s.from.as_slice(),
                    [crate::ast::TableExpr::Table { name, alias: None }] if name == "t"
                ));
                assert!(s.filter.is_some());
                assert_eq!(s.order_by.len(), 2);
                assert!(!s.order_by[0].asc); // DESC
                assert!(s.order_by[1].asc); // default ASC
                assert_eq!(s.limit, Some(10));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_aggregates_group_by_having() {
        use crate::ast::{FuncArgs, FuncCall};
        match one("SELECT k, count(*), sum(v) FROM t WHERE v > 0 \
             GROUP BY k HAVING count(*) > 1 ORDER BY k LIMIT 5")
        {
            Statement::Select(s) => {
                assert_eq!(s.projection.len(), 3);
                // count(*)
                assert!(matches!(
                    s.projection[1],
                    SelectItem::Expr {
                        expr: Expr::Func(FuncCall { ref name, distinct: false, args: FuncArgs::Star }),
                        ..
                    } if name == "count"
                ));
                assert_eq!(
                    s.group_by,
                    vec![Expr::Column {
                        table: None,
                        name: "k".into()
                    }]
                );
                assert!(s.having.is_some());
                assert_eq!(s.order_by.len(), 1);
                assert_eq!(s.limit, Some(5));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_count_distinct_and_func_args() {
        use crate::ast::{FuncArgs, FuncCall};
        match one("SELECT count(DISTINCT a + 1) FROM t") {
            Statement::Select(s) => match &s.projection[0] {
                SelectItem::Expr {
                    expr:
                        Expr::Func(FuncCall {
                            name,
                            distinct,
                            args,
                        }),
                    ..
                } => {
                    assert_eq!(name, "count");
                    assert!(*distinct);
                    match args {
                        FuncArgs::Exprs(v) => assert_eq!(v.len(), 1),
                        other => panic!("expected Exprs, got {other:?}"),
                    }
                }
                other => panic!("expected a Func projection, got {other:?}"),
            },
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn count_distinct_star_is_rejected() {
        // PostgreSQL rejects `count(DISTINCT *)` as a syntax error; so do we.
        assert!(parse("SELECT count(DISTINCT *) FROM t").is_err());
    }

    #[test]
    fn parses_multi_key_group_by() {
        match one("SELECT a, b, max(c) FROM t GROUP BY a, b") {
            Statement::Select(s) => {
                assert_eq!(
                    s.group_by,
                    vec![
                        Expr::Column {
                            table: None,
                            name: "a".into()
                        },
                        Expr::Column {
                            table: None,
                            name: "b".into()
                        }
                    ]
                );
                assert!(s.having.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_star_no_from() {
        match one("SELECT *") {
            Statement::Select(s) => {
                assert_eq!(s.projection, vec![SelectItem::Wildcard]);
                assert!(s.from.is_empty());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_multiple_statements() {
        let v = parse("SELECT 1; SELECT 2;").expect("parse");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn trailing_garbage_is_error() {
        assert!(parse("SELECT 1 foo bar").is_err());
    }

    #[test]
    fn parses_begin_variants() {
        assert_eq!(one("BEGIN"), Statement::Begin { isolation: None });
        assert_eq!(
            one("START TRANSACTION"),
            Statement::Begin { isolation: None }
        );
        assert_eq!(
            one("BEGIN ISOLATION LEVEL REPEATABLE READ"),
            Statement::Begin {
                isolation: Some(IsolationLevel::RepeatableRead)
            }
        );
        assert_eq!(
            one("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Statement::Begin {
                isolation: Some(IsolationLevel::ReadCommitted)
            }
        );
    }

    #[test]
    fn start_requires_transaction_keyword() {
        // START TRANSACTION is valid; bare START is not a statement.
        assert_eq!(
            one("START TRANSACTION"),
            Statement::Begin { isolation: None }
        );
        assert!(parse("START").is_err());
    }

    #[test]
    fn parses_commit_rollback_aliases() {
        assert_eq!(one("COMMIT"), Statement::Commit);
        assert_eq!(one("END"), Statement::Commit);
        assert_eq!(one("ROLLBACK"), Statement::Rollback);
        assert_eq!(one("ABORT"), Statement::Rollback);
    }

    #[test]
    fn parses_update() {
        match one("UPDATE t SET a = 1, b = a + 2 WHERE id = 5") {
            Statement::Update {
                table,
                assignments,
                filter,
            } => {
                assert_eq!(table, "t");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, "a");
                assert_eq!(assignments[1].0, "b");
                assert!(filter.is_some());
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_for_update_and_share() {
        use crate::ast::RowLockStrength;
        match one("SELECT id FROM t FOR UPDATE") {
            Statement::Select(s) => assert_eq!(s.locking, Some(RowLockStrength::ForUpdate)),
            other => panic!("expected Select, got {other:?}"),
        }
        match one("SELECT id FROM t WHERE id > 1 FOR SHARE") {
            Statement::Select(s) => assert_eq!(s.locking, Some(RowLockStrength::ForShare)),
            other => panic!("expected Select, got {other:?}"),
        }
        match one("SELECT id FROM t") {
            Statement::Select(s) => assert_eq!(s.locking, None),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parses_delete() {
        match one("DELETE FROM t WHERE id > 3") {
            Statement::Delete { table, filter } => {
                assert_eq!(table, "t");
                assert!(filter.is_some());
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        assert_eq!(
            one("DELETE FROM t"),
            Statement::Delete {
                table: "t".into(),
                filter: None
            }
        );
    }

    fn expr(sql: &str) -> Expr {
        // Wrap in a SELECT so the public parse() entry can reach it once
        // statements exist; until then, use the crate-internal expr parser.
        parse_expr_for_test(sql).expect("parse expr")
    }

    #[test]
    fn every_binary_operator_parses_to_its_op() {
        // Each operator token must map to its own BinaryOp arm in `expr` — pin all
        // ten so dropping any single arm (e.g. `<>`, `<=`, `-`, `/`) is caught.
        use crate::ast::BinaryOp::*;
        for (src, want) in [
            ("a = b", Eq),
            ("a <> b", Ne),
            ("a < b", Lt),
            ("a <= b", Le),
            ("a > b", Gt),
            ("a >= b", Ge),
            ("a + b", Add),
            ("a - b", Sub),
            ("a * b", Mul),
            ("a / b", Div),
            ("a || b", Concat),
        ] {
            match expr(src) {
                Expr::Binary { op, .. } => assert_eq!(op, want, "operator in `{src}`"),
                other => panic!("`{src}` should parse to a Binary expr, got {other:?}"),
            }
        }
    }

    #[test]
    fn bump_does_not_advance_past_eof() {
        // `bump` clamps at the trailing Eof token: a statement that runs out of
        // input (no table name) makes the parser bump AT Eof and then read the
        // next position for its error message. If `bump` advanced past Eof that
        // read would be out of bounds — instead we must get a clean parse error.
        assert!(parse("DROP TABLE").is_err());
        assert!(parse("CREATE TABLE").is_err());
    }

    #[test]
    fn precedence_mul_over_add() {
        // 1 + 2 * 3  ==  1 + (2 * 3)
        let e = expr("1 + 2 * 3");
        assert_eq!(
            e,
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::IntLiteral("1".into())),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::IntLiteral("2".into())),
                    right: Box::new(Expr::IntLiteral("3".into())),
                }),
            }
        );
    }

    #[test]
    fn concat_precedence_and_associativity() {
        // `||` binds looser than `+` (PG): `a || b + c` == `a || (b + c)`.
        match expr("a || b + c") {
            Expr::Binary {
                op: BinaryOp::Concat,
                right,
                ..
            } => assert!(matches!(
                *right,
                Expr::Binary {
                    op: BinaryOp::Add,
                    ..
                }
            )),
            other => panic!("expected Concat(.., Add) , got {other:?}"),
        }
        // `||` binds tighter than `=` (PG): `a || b = c` == `(a || b) = c`.
        match expr("a || b = c") {
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Binary {
                    op: BinaryOp::Concat,
                    ..
                }
            )),
            other => panic!("expected Eq(Concat, ..), got {other:?}"),
        }
        // Left-associative: `a || b || c` == `(a || b) || c`.
        match expr("a || b || c") {
            Expr::Binary {
                op: BinaryOp::Concat,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Binary {
                    op: BinaryOp::Concat,
                    ..
                }
            )),
            other => panic!("expected left-nested Concat, got {other:?}"),
        }
        // `||` binds tighter than LIKE: `a || b LIKE p` == `(a || b) LIKE p`.
        match expr("a || b LIKE 'p'") {
            Expr::Like { expr, .. } => {
                assert!(matches!(
                    *expr,
                    Expr::Binary {
                        op: BinaryOp::Concat,
                        ..
                    }
                ))
            }
            other => panic!("expected Like over Concat, got {other:?}"),
        }
    }

    #[test]
    fn unary_minus_still_binds_tighter_than_star() {
        // After the SP29 renumber, `-a * b` must still be `(-a) * b`.
        match expr("-a * b") {
            Expr::Binary {
                op: BinaryOp::Mul,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Unary {
                    op: UnaryOp::Neg,
                    ..
                }
            )),
            other => panic!("expected Mul((-a), b), got {other:?}"),
        }
    }

    #[test]
    fn comparison_and_boolean_precedence() {
        // a = 1 AND b < 2  ==  (a = 1) AND (b < 2)
        let e = expr("a = 1 AND b < 2");
        assert!(matches!(
            e,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn not_and_or_precedence() {
        // NOT x OR y  ==  (NOT x) OR y
        let e = expr("NOT x OR y");
        match e {
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                ..
            } => {
                assert!(matches!(
                    *left,
                    Expr::Unary {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
            }
            _ => panic!("expected OR at top, got {e:?}"),
        }
    }

    #[test]
    fn unary_minus_and_parens() {
        let e = expr("-(1 + 2)");
        assert!(matches!(
            e,
            Expr::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ));
    }

    #[test]
    fn literals_columns_params() {
        assert_eq!(expr("'hi'"), Expr::StringLiteral("hi".into()));
        assert_eq!(expr("true"), Expr::BoolLiteral(true));
        assert_eq!(expr("null"), Expr::NullLiteral);
        assert_eq!(
            expr("col"),
            Expr::Column {
                table: None,
                name: "col".into()
            }
        );
        assert_eq!(expr("$2"), Expr::Param(2));
    }

    #[test]
    fn not_binds_tighter_than_and() {
        // NOT x AND y == (NOT x) AND y
        let e = expr("NOT x AND y");
        match e {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                ..
            } => {
                assert!(
                    matches!(
                        *left,
                        Expr::Unary {
                            op: UnaryOp::Not,
                            ..
                        }
                    ),
                    "left of AND must be (NOT x), got {left:?}"
                );
            }
            _ => panic!("expected AND at root, got {e:?}"),
        }
    }

    #[test]
    fn comparison_binds_tighter_than_not() {
        // NOT a = 1 == NOT (a = 1)
        let e = expr("NOT a = 1");
        match e {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => {
                assert!(
                    matches!(
                        *expr,
                        Expr::Binary {
                            op: BinaryOp::Eq,
                            ..
                        }
                    ),
                    "NOT operand must be (a = 1), got {expr:?}"
                );
            }
            _ => panic!("expected NOT at root, got {e:?}"),
        }
    }

    // ---- SP28: predicate + conditional expression breadth ----

    #[test]
    fn parses_is_null_and_is_not_null() {
        assert!(matches!(
            expr("a IS NULL"),
            Expr::IsNull { negated: false, .. }
        ));
        assert!(matches!(
            expr("a IS NOT NULL"),
            Expr::IsNull { negated: true, .. }
        ));
    }

    #[test]
    fn parses_in_and_not_in() {
        match expr("a IN (1, 2, 3)") {
            Expr::InList { list, negated, .. } => {
                assert_eq!(list.len(), 3);
                assert!(!negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }
        assert!(matches!(
            expr("a NOT IN (1, 2)"),
            Expr::InList { negated: true, .. }
        ));
    }

    #[test]
    fn empty_in_list_is_rejected() {
        assert!(parse("SELECT a FROM t WHERE a IN ()").is_err());
    }

    #[test]
    fn not_in_is_infix_but_prefix_not_wraps_in() {
        // `x NOT IN (..)` is the infix negated predicate.
        assert!(matches!(
            expr("x NOT IN (1)"),
            Expr::InList { negated: true, .. }
        ));
        // `NOT x IN (..)` is prefix NOT over (x IN ..).
        match expr("NOT x IN (1)") {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => assert!(matches!(*expr, Expr::InList { negated: false, .. })),
            other => panic!("expected NOT over InList, got {other:?}"),
        }
    }

    #[test]
    fn between_and_does_not_eat_boolean_and() {
        // `a BETWEEN 1 AND 2 AND b` == `(a BETWEEN 1 AND 2) AND b`.
        match expr("a BETWEEN 1 AND 2 AND b") {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                assert!(matches!(*left, Expr::Between { negated: false, .. }));
                assert_eq!(
                    *right,
                    Expr::Column {
                        table: None,
                        name: "b".into()
                    }
                );
            }
            other => panic!("expected AND(Between, b), got {other:?}"),
        }
        assert!(matches!(
            expr("a NOT BETWEEN 1 AND 10"),
            Expr::Between { negated: true, .. }
        ));
    }

    #[test]
    fn parses_like_ilike_all_combinations() {
        assert!(matches!(
            expr("a LIKE 'x%'"),
            Expr::Like {
                negated: false,
                case_insensitive: false,
                ..
            }
        ));
        assert!(matches!(
            expr("a NOT LIKE 'x%'"),
            Expr::Like {
                negated: true,
                case_insensitive: false,
                ..
            }
        ));
        assert!(matches!(
            expr("a ILIKE 'x%'"),
            Expr::Like {
                negated: false,
                case_insensitive: true,
                ..
            }
        ));
        assert!(matches!(
            expr("a NOT ILIKE 'x%'"),
            Expr::Like {
                negated: true,
                case_insensitive: true,
                ..
            }
        ));
    }

    #[test]
    fn parses_searched_and_simple_case() {
        match expr("CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END") {
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                assert!(operand.is_none());
                assert_eq!(whens.len(), 1);
                assert!(else_result.is_some());
            }
            other => panic!("expected searched CASE, got {other:?}"),
        }
        match expr("CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END") {
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                assert!(operand.is_some());
                assert_eq!(whens.len(), 2);
                assert!(else_result.is_none());
            }
            other => panic!("expected simple CASE, got {other:?}"),
        }
    }

    #[test]
    fn case_without_when_is_rejected() {
        assert!(parse("SELECT CASE END FROM t").is_err());
    }

    #[test]
    fn parses_select_distinct() {
        match one("SELECT DISTINCT a FROM t") {
            Statement::Select(s) => assert!(s.distinct),
            other => panic!("expected Select, got {other:?}"),
        }
        match one("SELECT a FROM t") {
            Statement::Select(s) => assert!(!s.distinct),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    // ---- SP31: explicit casts ----

    #[test]
    fn parses_cast_both_forms_to_the_same_node() {
        use pgtypes::ColumnType;
        // `expr::type` and `CAST(expr AS type)` produce the identical Cast node.
        let want = Expr::Cast {
            expr: Box::new(Expr::IntLiteral("1".into())),
            ty: ColumnType::Int8,
        };
        assert_eq!(expr("1::int8"), want);
        assert_eq!(expr("CAST(1 AS int8)"), want);
        // `double precision` (two-word) and the other spellings resolve.
        assert!(matches!(
            expr("x::double precision"),
            Expr::Cast {
                ty: ColumnType::Float8,
                ..
            }
        ));
        assert!(matches!(
            expr("CAST(x AS integer)"),
            Expr::Cast {
                ty: ColumnType::Int4,
                ..
            }
        ));
        assert!(matches!(
            expr("x::text"),
            Expr::Cast {
                ty: ColumnType::Text,
                ..
            }
        ));
    }

    #[test]
    fn cast_binds_tighter_than_unary_minus_and_arithmetic() {
        // `-2::int8` == `-(2::int8)` — the cast binds to `2`, not to `-2`.
        match expr("-2::int8") {
            Expr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => {
                assert!(matches!(*expr, Expr::Cast { .. }), "got {expr:?}");
            }
            other => panic!("expected Neg(Cast), got {other:?}"),
        }
        // `1 + 2::int8` == `1 + (2::int8)`.
        match expr("1 + 2::int8") {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => {
                assert!(matches!(*right, Expr::Cast { .. }), "got {right:?}");
            }
            other => panic!("expected Add(1, Cast), got {other:?}"),
        }
        // `a::int4 + b` == `(a::int4) + b`.
        match expr("a::int4 + b") {
            Expr::Binary {
                op: BinaryOp::Add,
                left,
                ..
            } => {
                assert!(matches!(*left, Expr::Cast { .. }), "got {left:?}");
            }
            other => panic!("expected Add(Cast, b), got {other:?}"),
        }
    }

    #[test]
    fn cast_is_left_associative_when_chained() {
        // `a::int4::text` == `(a::int4)::text`.
        match expr("a::int4::text") {
            Expr::Cast { expr: inner, ty } => {
                assert_eq!(ty, pgtypes::ColumnType::Text);
                assert!(
                    matches!(
                        *inner,
                        Expr::Cast {
                            ty: pgtypes::ColumnType::Int4,
                            ..
                        }
                    ),
                    "got {inner:?}"
                );
            }
            other => panic!("expected outer text Cast over int4 Cast, got {other:?}"),
        }
    }

    #[test]
    fn cast_to_unknown_type_is_a_parse_error() {
        assert!(parse("SELECT 1::widget").is_err());
        assert!(parse("SELECT CAST(1 AS widget)").is_err());
        // `cast` is a reserved keyword now, so `CAST(... )` requires `AS`.
        assert!(parse("SELECT CAST(1 int4)").is_err());
    }

    // ---- SP37: date/time type names, typed literals, EXTRACT, AT TIME ZONE ----

    #[test]
    fn parses_typed_datetime_literals() {
        use crate::ast::Expr;
        assert!(matches!(
            parse_expr_for_test("DATE '2024-01-01'").expect("d"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("INTERVAL '1 day'").expect("iv"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("TIMESTAMP '2024-01-01 00:00:00'").expect("ts"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("TIMESTAMPTZ '2024-01-01 00:00:00+00'").expect("tstz"),
            Expr::Cast { .. }
        ));
    }

    #[test]
    fn parses_extract_and_at_time_zone() {
        use crate::ast::Expr;
        assert!(matches!(
            parse_expr_for_test("extract(year from x)").expect("ex"),
            Expr::Func(_)
        ));
        let e = parse_expr_for_test("ts AT TIME ZONE 'UTC' = ts2").expect("attz");
        assert!(matches!(
            e,
            Expr::Binary {
                op: crate::ast::BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn parses_multiword_type_in_create_and_cast() {
        use crate::ast::{Expr, Statement};
        let stmts = crate::parser::parse(
            "CREATE TABLE t (a timestamp with time zone, b time without time zone)",
        )
        .expect("ct");
        assert!(matches!(&stmts[0], Statement::CreateTable { .. }));
        assert!(matches!(
            parse_expr_for_test("x::timestamp with time zone").expect("c"),
            Expr::Cast {
                ty: pgtypes::ColumnType::Timestamptz,
                ..
            }
        ));
    }

    // ---- SP37: SET / SHOW / RESET timezone GUC ----

    #[test]
    fn parses_set_timezone_all_spellings() {
        use crate::ast::SetValue;
        // SET timezone = '...' / SET timezone TO '...'
        assert_eq!(
            one("SET timezone = 'America/New_York'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        assert_eq!(
            one("SET timezone TO 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
        // SET TIME ZONE '...' (the special two-word spelling normalizes to `timezone`).
        assert_eq!(
            one("SET TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        // An identifier value (no quotes) is accepted too.
        assert_eq!(
            one("SET timezone TO utc"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("utc".into()),
            }
        );
        // The GUC name is normalized to lowercase.
        assert_eq!(
            one("SET TimeZone = 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
    }

    #[test]
    fn parses_set_local_flag_vs_local_value() {
        use crate::ast::SetValue;
        // `SET LOCAL timezone ...` — LOCAL is the flag (followed by a param name).
        assert_eq!(
            one("SET LOCAL timezone = 'UTC'"),
            Statement::Set {
                local: true,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
        assert_eq!(
            one("SET LOCAL TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: true,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        // `SET TIME ZONE LOCAL` — here LOCAL is the VALUE (→ Default), not the flag.
        assert_eq!(
            one("SET TIME ZONE LOCAL"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
        // `SET TIME ZONE DEFAULT` is likewise the Default value.
        assert_eq!(
            one("SET TIME ZONE DEFAULT"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
        // `SET timezone = DEFAULT` — DEFAULT as the value.
        assert_eq!(
            one("SET timezone = DEFAULT"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
    }

    #[test]
    fn parses_show_and_reset() {
        assert_eq!(
            one("SHOW timezone"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("SHOW TIME ZONE"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("SHOW TimeZone"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("RESET timezone"),
            Statement::Reset {
                name: "timezone".into()
            }
        );
    }

    #[test]
    fn set_show_reset_accept_unknown_names_at_parse_time() {
        // Name validation is the executor's job (42704); the parser accepts any name.
        use crate::ast::SetValue;
        assert_eq!(
            one("SET datestyle = 'ISO, MDY'"),
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value("ISO, MDY".into()),
            }
        );
        assert_eq!(
            one("SHOW search_path"),
            Statement::Show {
                name: "search_path".into()
            }
        );
    }

    #[test]
    fn parses_qualified_column() {
        use crate::ast::Expr;
        assert_eq!(
            expr("a.col"),
            Expr::Column {
                table: Some("a".into()),
                name: "col".into()
            }
        );
        assert_eq!(
            expr("col"),
            Expr::Column {
                table: None,
                name: "col".into()
            }
        );
    }

    #[test]
    fn parses_limit_and_offset_either_order() {
        for sql in [
            "SELECT a FROM t ORDER BY a LIMIT 5 OFFSET 10",
            "SELECT a FROM t ORDER BY a OFFSET 10 LIMIT 5",
        ] {
            match one(sql) {
                Statement::Select(s) => {
                    assert_eq!(s.limit, Some(5));
                    assert_eq!(s.offset, Some(10));
                }
                other => panic!("expected Select, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_inner_join_on() {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        match one("SELECT a.x FROM a JOIN b ON a.id = b.id") {
            Statement::Select(s) => {
                assert_eq!(s.from.len(), 1);
                match &s.from[0] {
                    TableExpr::Join {
                        kind, constraint, ..
                    } => {
                        assert_eq!(*kind, JoinKind::Inner);
                        assert!(matches!(constraint, JoinConstraint::On(_)));
                    }
                    other => panic!("expected Join, got {other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_left_join_using_and_aliases_and_comma() {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        match one("SELECT * FROM a x LEFT OUTER JOIN b AS y USING (id), c") {
            Statement::Select(s) => {
                assert_eq!(s.from.len(), 2); // comma -> two top-level items
                match &s.from[0] {
                    TableExpr::Join {
                        kind,
                        constraint,
                        left,
                        right,
                    } => {
                        assert_eq!(*kind, JoinKind::Left);
                        assert_eq!(*constraint, JoinConstraint::Using(vec!["id".into()]));
                        assert!(
                            matches!(**left, TableExpr::Table { ref alias, .. } if alias.as_deref() == Some("x"))
                        );
                        assert!(
                            matches!(**right, TableExpr::Table { ref alias, .. } if alias.as_deref() == Some("y"))
                        );
                    }
                    other => panic!("expected Join, got {other:?}"),
                }
                assert!(
                    matches!(&s.from[1], TableExpr::Table { name, alias: None } if name == "c")
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_natural_and_cross_and_derived_and_multiway() {
        use crate::ast::TableExpr;
        assert!(matches!(
            one("SELECT * FROM a NATURAL JOIN b"),
            Statement::Select(_)
        ));
        assert!(matches!(
            one("SELECT * FROM a CROSS JOIN b"),
            Statement::Select(_)
        ));
        assert!(matches!(
            one("SELECT * FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id"),
            Statement::Select(_)
        ));
        match one("SELECT d.n FROM (SELECT n FROM t) AS d") {
            Statement::Select(s) => {
                assert!(matches!(&s.from[0], TableExpr::Derived { alias, .. } if alias == "d"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_qualified_wildcard() {
        use crate::ast::SelectItem;
        match one("SELECT a.* FROM a JOIN b ON a.id=b.id") {
            Statement::Select(s) => {
                assert_eq!(s.projection[0], SelectItem::QualifiedWildcard("a".into()))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn derived_table_requires_alias() {
        assert!(parse("SELECT * FROM (SELECT 1)").is_err());
    }

    // ---- SP34: subquery expressions ----

    #[test]
    fn parses_scalar_subquery_in_expression_position() {
        match expr("(SELECT 1)") {
            Expr::ScalarSubquery(s) => {
                assert_eq!(s.projection.len(), 1);
                assert!(s.from.is_empty());
            }
            other => panic!("expected ScalarSubquery, got {other:?}"),
        }
        // Nested in arithmetic; and a plain parenthesised expr is still grouping.
        assert!(matches!(
            expr("1 + (SELECT a FROM t)"),
            Expr::Binary { right, .. } if matches!(*right, Expr::ScalarSubquery(_))
        ));
        assert!(matches!(expr("(1 + 2) * 3"), Expr::Binary { .. }));
    }

    #[test]
    fn parses_exists_and_not_exists() {
        assert!(matches!(expr("EXISTS (SELECT 1 FROM t)"), Expr::Exists(_)));
        match expr("NOT EXISTS (SELECT 1 FROM t)") {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => {
                assert!(matches!(*expr, Expr::Exists(_)))
            }
            other => panic!("expected NOT(EXISTS …), got {other:?}"),
        }
    }

    #[test]
    fn parses_in_subquery_and_keeps_in_list_working() {
        assert!(matches!(expr("a IN (1, 2, 3)"), Expr::InList { .. }));
        match expr("a IN (SELECT id FROM t)") {
            Expr::InSubquery { negated, .. } => assert!(!negated),
            other => panic!("expected InSubquery, got {other:?}"),
        }
        match expr("a NOT IN (SELECT id FROM t)") {
            Expr::InSubquery { negated, .. } => assert!(negated),
            other => panic!("expected negated InSubquery, got {other:?}"),
        }
    }

    #[test]
    fn parses_quantified_any_all_some() {
        match expr("a = ANY (SELECT id FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Eq,
                all,
                ..
            } => assert!(!all),
            other => panic!("expected ANY, got {other:?}"),
        }
        match expr("a > ALL (SELECT v FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Gt,
                all,
                ..
            } => assert!(all),
            other => panic!("expected ALL, got {other:?}"),
        }
        match expr("a <> SOME (SELECT v FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Ne,
                all,
                ..
            } => assert!(!all),
            other => panic!("expected SOME(=ANY), got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Recursion-depth guard (54001 / statement_too_complex).
    //
    // Two distinct DoS crash modes, both must return a clean 54001 — never a
    // stack overflow that aborts the whole server process:
    //   mode 1  deep PARSE recursion: nested parens/CASE/NOT/unary minus all
    //           funnel through `expr`, so a guard there bounds them.
    //   mode 2  deep AST TREE from a flat left-assoc chain (`1+1+1+…`): the
    //           Pratt loop parses iteratively but builds an N-deep left-nested
    //           tree that then overflows in eval AND on recursive Box `Drop`.
    //           Capping the loop iterations prevents the over-deep tree.
    // ------------------------------------------------------------------

    /// Mode 1: `(((…1…)))` nested far beyond `MAX_DEPTH` → clean 54001, no crash.
    #[test]
    fn deeply_nested_parens_return_54001_not_a_crash() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep parens must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
        assert_eq!(err.message, "stack depth limit exceeded");
    }

    /// Mode 1: deep prefix `NOT` chain funnels through `expr` → 54001.
    #[test]
    fn deeply_nested_not_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}true", "NOT ".repeat(n));
        let err = parse(&sql).expect_err("too-deep NOT must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 1: deeply nested scalar subqueries `(SELECT (SELECT …))` → 54001.
    #[test]
    fn deeply_nested_subqueries_return_54001() {
        let n = MAX_DEPTH * 2;
        let sql = format!("SELECT {}1{}", "(SELECT ".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep subqueries must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 2: a long flat `1+1+1+…` chain is parsed iteratively but builds an
    /// N-deep left-nested tree; capping the Pratt loop returns 54001 so the
    /// tree (and its later eval/Drop) never over-deepens.
    #[test]
    fn long_left_assoc_chain_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}1", "1+".repeat(n));
        let err = parse(&sql).expect_err("too-long additive chain must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Crash-safety floor: a query nested right up to the limit must PARSE OK
    /// (no stack overflow). If this test ABORTS the process (stack overflow rather
    /// than a clean pass), `MAX_DEPTH` is too high for the runner's ~2 MiB stack
    /// and must be lowered. Each `(` adds one `expr` frame (one `DepthGuard`
    /// level); `select_inner` + the outermost projection `expr` add 2 guard
    /// levels on top, so `MAX_DEPTH - 2` is the deepest paren query the parser
    /// admits — this test uses `MAX_DEPTH - 3` for one extra level of headroom.
    #[test]
    fn at_limit_parens_parse_ok() {
        let n = MAX_DEPTH - 3;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        parse(&sql).expect("a query nested at the limit must parse, not crash");
    }

    /// The guard actually fires near the limit (not merely far away): a paren
    /// nest a few levels OVER `MAX_DEPTH` returns 54001, while the `at_limit`
    /// test above proves a nest just UNDER it still parses — so the boundary is
    /// where it is intended to be.
    #[test]
    fn parens_just_over_limit_returns_54001() {
        let n = MAX_DEPTH + 2;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        assert_eq!(
            parse(&sql)
                .expect_err("just over the limit must error")
                .sqlstate(),
            "54001",
        );
    }

    /// A modest real-world nesting depth (well under the limit) parses fine —
    /// the guard does not reject ordinary queries.
    #[test]
    fn modest_nesting_parses_fine() {
        let sql = format!("SELECT {}1{}", "(".repeat(20), ")".repeat(20));
        parse(&sql).expect("modest nesting must parse");
        // A flat chain of 20 additions is fine too.
        parse(&format!("SELECT {}1", "1+".repeat(20))).expect("modest chain must parse");
    }

    /// Mode 2 (set ops): a long flat `… UNION ALL …` chain is parsed by the
    /// `set_expr` LOOP, building an N-deep left-nested `SetExpr` that would overflow
    /// the executor's `fold`/`resolve_set_columns` AND recursive `Drop`. The loop
    /// iteration cap returns a clean 54001.
    #[test]
    fn long_union_chain_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(n));
        let err = parse(&sql).expect_err("too-long UNION chain must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 1 (set ops): deeply nested parens around a query recurse
    /// `set_primary → set_expr → set_primary` (NOT through `expr`), so the
    /// `set_expr` guard must catch them → 54001.
    #[test]
    fn deeply_nested_query_parens_return_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("{}SELECT 1{}", "(".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep query parens must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// A modest `UNION` chain (well under the limit) parses fine — the cap does not
    /// reject ordinary set-op queries.
    #[test]
    fn modest_union_chain_parses_fine() {
        let sql = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(20));
        parse(&sql).expect("modest UNION chain must parse");
    }

    #[test]
    fn parses_standalone_values_query() {
        use crate::ast::{Expr, Statement};
        let s = crate::parse("VALUES (1, 'a'), (2, 'b') ORDER BY 1 LIMIT 1 OFFSET 1").unwrap();
        let Statement::Values(q) = &s[0] else {
            panic!("expected VALUES, got {:?}", s[0])
        };
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
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op")
        };
        let SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } = &q.body
        else {
            panic!("expected set op body")
        };
        assert_eq!(*op, SetOp::Union);
        assert!(*all);
        assert!(matches!(&**left, SetExpr::Query(QueryBody::Values(_))));
        assert!(matches!(&**right, SetExpr::Query(QueryBody::Select(_))));
    }

    #[test]
    fn parses_values_derived_table_with_column_aliases() {
        use crate::ast::{QueryBody, Statement, TableExpr};
        let s = crate::parse("SELECT id, name FROM (VALUES (1, 'a')) AS v(id, name)").unwrap();
        let Statement::Select(sel) = &s[0] else {
            panic!("expected select")
        };
        let TableExpr::Derived {
            subquery,
            alias,
            columns,
        } = &sel.from[0]
        else {
            panic!("expected derived table")
        };
        assert!(matches!(subquery, QueryBody::Values(_)));
        assert_eq!(alias, "v");
        assert_eq!(
            columns.as_ref().unwrap(),
            &vec!["id".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn parses_select_derived_table_with_column_aliases() {
        use crate::ast::{QueryBody, Statement, TableExpr};
        let s = crate::parse("SELECT n FROM (SELECT a FROM t) AS d(n)").unwrap();
        let Statement::Select(sel) = &s[0] else {
            panic!("expected select")
        };
        let TableExpr::Derived {
            subquery,
            alias,
            columns,
        } = &sel.from[0]
        else {
            panic!("expected derived table")
        };
        assert!(matches!(subquery, QueryBody::Select(_)));
        assert_eq!(alias, "d");
        assert_eq!(columns.as_ref().unwrap(), &vec!["n".to_string()]);
    }

    #[test]
    fn values_rows_must_have_at_least_one_expr() {
        assert!(crate::parse("VALUES ()").is_err());
    }

    #[test]
    fn parses_union_all_and_precedence() {
        use crate::ast::{SetExpr, SetOp, Statement};
        // INTERSECT binds tighter than UNION: A UNION B INTERSECT C => A UNION (B INTERSECT C)
        let s = crate::parse("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3").expect("parse");
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op, got {:?}", s[0])
        };
        let SetExpr::SetOp { op, all, right, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        assert_eq!(*op, SetOp::Union);
        assert!(!*all);
        assert!(matches!(
            &**right,
            SetExpr::SetOp {
                op: SetOp::Intersect,
                ..
            }
        ));

        // UNION ALL sets `all`; left-associativity: A UNION B UNION C => (A UNION B) UNION C
        let s = crate::parse("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3").expect("parse");
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op")
        };
        let SetExpr::SetOp { all, left, .. } = &q.body else {
            panic!()
        };
        assert!(*all);
        assert!(matches!(
            &**left,
            SetExpr::SetOp {
                op: SetOp::Union,
                ..
            }
        ));
    }

    #[test]
    fn union_order_by_limit_bind_to_whole_query() {
        use crate::ast::Statement;
        let s = crate::parse("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5 OFFSET 1").expect("parse");
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op")
        };
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn parenthesized_branch_keeps_its_own_order_limit() {
        use crate::ast::{QueryBody, SetExpr, Statement};
        let s = crate::parse("(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2").expect("parse");
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op")
        };
        let SetExpr::SetOp { left, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        let SetExpr::Query(QueryBody::Select(b)) = &**left else {
            panic!("left branch is a SELECT leaf")
        };
        assert_eq!(b.limit, Some(1));
        assert_eq!(b.order_by.len(), 1);
    }

    #[test]
    fn plain_select_is_unchanged() {
        use crate::ast::Statement;
        // No set-op keyword => still Statement::Select, tail on the struct.
        let s = crate::parse("SELECT a FROM t ORDER BY a LIMIT 3").expect("parse");
        let Statement::Select(sel) = &s[0] else {
            panic!("plain select must stay Statement::Select")
        };
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

    #[test]
    fn union_distinct_is_the_default_form() {
        use crate::ast::{SetExpr, Statement};
        // `UNION DISTINCT` is the explicit spelling of the default (dedup) form:
        // it parses to the same tree as a bare `UNION` (all == false).
        let s = crate::parse("SELECT 1 UNION DISTINCT SELECT 2").expect("parse");
        let Statement::SetOperation(q) = &s[0] else {
            panic!("expected set op, got {:?}", s[0])
        };
        let SetExpr::SetOp { all, .. } = &q.body else {
            panic!("expected SetOp")
        };
        assert!(!*all, "UNION DISTINCT is the dedup (all == false) form");
    }
}
