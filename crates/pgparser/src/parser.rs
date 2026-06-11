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
            let (op, l_bp, r_bp) = match self.peek() {
                Token::Keyword(Keyword::Or) => (BinaryOp::Or, 1, 2),
                Token::Keyword(Keyword::And) => (BinaryOp::And, 3, 4),
                Token::Eq => (BinaryOp::Eq, 5, 6),
                Token::Ne => (BinaryOp::Ne, 5, 6),
                Token::Lt => (BinaryOp::Lt, 5, 6),
                Token::Le => (BinaryOp::Le, 5, 6),
                Token::Gt => (BinaryOp::Gt, 5, 6),
                Token::Ge => (BinaryOp::Ge, 5, 6),
                Token::Plus => (BinaryOp::Add, 7, 8),
                Token::Minus => (BinaryOp::Sub, 7, 8),
                Token::Star => (BinaryOp::Mul, 9, 10),
                Token::Slash => (BinaryOp::Div, 9, 10),
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
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(self.expr(11)?),
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
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            Token::Ident(s) => {
                self.bump();
                Ok(Expr::Column(s))
            }
            other => Err(ParseError::new(
                format!("unexpected token {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    pub(crate) fn program(&mut self) -> Result<Vec<crate::ast::Statement>, ParseError> {
        use crate::ast::Statement;
        let mut stmts: Vec<Statement> = Vec::new();
        loop {
            while *self.peek() == Token::Semicolon {
                self.bump();
            }
            if *self.peek() == Token::Eof {
                break;
            }
            stmts.push(self.statement()?);
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
            other => Err(ParseError::new(
                format!("unexpected statement start {other:?}"),
                self.peek_pos(),
            )),
        }
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
        let limit = if self.eat_keyword(Keyword::Limit) {
            let pos = self.peek_pos();
            match self.bump() {
                Token::IntLit(s) => Some(
                    s.parse::<i64>()
                        .map_err(|_| ParseError::new("LIMIT value out of range", pos))?,
                ),
                other => {
                    return Err(ParseError::new(
                        format!("expected LIMIT count, found {other:?}"),
                        pos,
                    ));
                }
            }
        } else {
            None
        };
        Ok(Statement::Select(SelectStmt {
            projection,
            from,
            filter,
            order_by,
            limit,
        }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, Expr, UnaryOp};
    use crate::ast::{ColumnDef, SelectItem, Statement};
    use pgtypes::ColumnType;

    fn one(sql: &str) -> Statement {
        let mut v = parse(sql).expect("parse");
        assert_eq!(v.len(), 1);
        v.pop().expect("one statement")
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

    fn expr(sql: &str) -> Expr {
        // Wrap in a SELECT so the public parse() entry can reach it once
        // statements exist; until then, use the crate-internal expr parser.
        parse_expr_for_test(sql).expect("parse expr")
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
}
