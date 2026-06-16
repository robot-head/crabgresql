//! Recursive-descent statement parser with Pratt expression parsing.

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::error::ParseError;
use crate::lexer::lex;
use crate::token::{Keyword, Token};

pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    pos: usize,
}

impl Parser {
    pub(crate) fn new(toks: Vec<(Token, usize)>) -> Self {
        Self { toks, pos: 0 }
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

    /// Pratt expression parser. `min_bp` is the minimum left binding power.
    pub(crate) fn expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.prefix()?;
        loop {
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
                self.bump();
                let e = self.expr(0)?;
                self.expect(&Token::RParen)?;
                Ok(e)
            }
            Token::IntLit(s) => {
                self.bump();
                Ok(Expr::IntLiteral(s))
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
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            Token::Ident(s) => {
                self.bump();
                // SP27: `ident (` is a function call; a bare ident is a column.
                if *self.peek() == Token::LParen {
                    self.func_call(s)
                } else {
                    Ok(Expr::Column(s))
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

    /// `expr [NOT] IN (e1, e2, …)`, positioned at `IN`. The list has ≥1 element
    /// (`IN ()` is a 42601, matching PostgreSQL). Subqueries are out of scope.
    fn parse_in(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::In))?;
        self.expect(&Token::LParen)?;
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
            Token::Keyword(Keyword::Select) => self.select(),
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
            other => Err(ParseError::new(
                format!("unexpected statement start {other:?}"),
                self.peek_pos(),
            )),
        }
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
            let type_pos = self.peek_pos();
            let type_word = self.expect_ident()?;
            let ty = pgtypes::ColumnType::from_sql_name(&type_word).ok_or_else(|| {
                ParseError::new(format!("unknown type \"{type_word}\""), type_pos)
            })?;
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

    fn select(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{OrderItem, SelectItem, SelectStmt, Statement};
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
            Some(self.expect_ident()?)
        } else {
            None
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
        let locking = if self.eat_keyword(Keyword::For) {
            if self.eat_keyword(Keyword::Update) {
                Some(crate::ast::RowLockStrength::ForUpdate)
            } else if self.eat_keyword(Keyword::Share) {
                Some(crate::ast::RowLockStrength::ForShare)
            } else {
                return Err(ParseError::new(
                    "expected UPDATE or SHARE after FOR",
                    self.peek_pos(),
                ));
            }
        } else {
            None
        };
        Ok(Statement::Select(SelectStmt {
            projection,
            from,
            filter,
            distinct,
            group_by,
            having,
            order_by,
            limit,
            offset,
            locking,
        }))
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
                assert_eq!(s.from.as_deref(), Some("t"));
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
                assert_eq!(s.group_by, vec![Expr::Column("k".into())]);
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
                    vec![Expr::Column("a".into()), Expr::Column("b".into())]
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
                assert!(s.from.is_none());
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
            } => assert!(matches!(*right, Expr::Binary { op: BinaryOp::Add, .. })),
            other => panic!("expected Concat(.., Add) , got {other:?}"),
        }
        // `||` binds tighter than `=` (PG): `a || b = c` == `(a || b) = c`.
        match expr("a || b = c") {
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                ..
            } => assert!(matches!(*left, Expr::Binary { op: BinaryOp::Concat, .. })),
            other => panic!("expected Eq(Concat, ..), got {other:?}"),
        }
        // Left-associative: `a || b || c` == `(a || b) || c`.
        match expr("a || b || c") {
            Expr::Binary {
                op: BinaryOp::Concat,
                left,
                ..
            } => assert!(matches!(*left, Expr::Binary { op: BinaryOp::Concat, .. })),
            other => panic!("expected left-nested Concat, got {other:?}"),
        }
        // `||` binds tighter than LIKE: `a || b LIKE p` == `(a || b) LIKE p`.
        match expr("a || b LIKE 'p'") {
            Expr::Like { expr, .. } => {
                assert!(matches!(*expr, Expr::Binary { op: BinaryOp::Concat, .. }))
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
            } => assert!(matches!(*left, Expr::Unary { op: UnaryOp::Neg, .. })),
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
        assert_eq!(expr("col"), Expr::Column("col".into()));
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
                assert_eq!(*right, Expr::Column("b".into()));
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
}
