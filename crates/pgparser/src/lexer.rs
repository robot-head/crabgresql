//! Hand-written lexer. Produces (Token, byte-offset) pairs; offsets feed
//! 42601 error positions. Integer literals only (the slice has no float type).

use crate::error::ParseError;
use crate::token::{Keyword, Token};

pub fn lex(sql: &str) -> Result<Vec<(Token, usize)>, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 >= bytes.len() {
                    return Err(ParseError::new("unterminated block comment", start));
                }
                i += 2;
            }
            b'\'' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => return Err(ParseError::new("unterminated string literal", start)),
                        Some(&b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            s.push('\'');
                            i += 2;
                        }
                        Some(&b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::StringLit(s), start));
            }
            b'"' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => {
                            return Err(ParseError::new("unterminated quoted identifier", start));
                        }
                        Some(&b'"') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::Ident(s), start));
            }
            b'<' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Le, i));
                i += 2;
            }
            b'>' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Ge, i));
                i += 2;
            }
            b'<' if bytes.get(i + 1) == Some(&b'>') => {
                out.push((Token::Ne, i));
                i += 2;
            }
            b'(' => push1(&mut out, Token::LParen, &mut i),
            b')' => push1(&mut out, Token::RParen, &mut i),
            b',' => push1(&mut out, Token::Comma, &mut i),
            b';' => push1(&mut out, Token::Semicolon, &mut i),
            b'*' => push1(&mut out, Token::Star, &mut i),
            b'+' => push1(&mut out, Token::Plus, &mut i),
            b'-' => push1(&mut out, Token::Minus, &mut i),
            b'/' => push1(&mut out, Token::Slash, &mut i),
            b'=' => push1(&mut out, Token::Eq, &mut i),
            b'<' => push1(&mut out, Token::Lt, &mut i),
            b'>' => push1(&mut out, Token::Gt, &mut i),
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                let ds = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let n: u32 = sql[ds..i]
                    .parse()
                    .map_err(|_| ParseError::new("parameter number out of range", start))?;
                out.push((Token::Param(n), start));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                out.push((Token::IntLit(sql[start..i].to_string()), start));
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                let word = sql[start..i].to_ascii_lowercase();
                let tok = match Keyword::from_word(&word) {
                    Some(kw) => Token::Keyword(kw),
                    None => Token::Ident(word),
                };
                out.push((tok, start));
            }
            _ => {
                return Err(ParseError::new(
                    format!("unexpected character {:?}", c as char),
                    i,
                ));
            }
        }
    }
    out.push((Token::Eof, sql.len()));
    Ok(out)
}

fn push1(out: &mut Vec<(Token, usize)>, tok: Token, i: &mut usize) {
    out.push((tok, *i));
    *i += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Keyword, Token};
    use proptest::prelude::*;

    fn toks(sql: &str) -> Vec<Token> {
        lex(sql).expect("lex").into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn keywords_idents_literals() {
        assert_eq!(
            toks("SELECT id FROM t WHERE x = 'a'"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::Ident("id".into()),
                Token::Keyword(Keyword::From),
                Token::Ident("t".into()),
                Token::Keyword(Keyword::Where),
                Token::Ident("x".into()),
                Token::Eq,
                Token::StringLit("a".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn keywords_are_case_insensitive_idents_lowercased() {
        assert_eq!(toks("Select FOO")[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks("Select FOO")[1], Token::Ident("foo".into()));
    }

    #[test]
    fn quoted_ident_preserves_case() {
        assert_eq!(toks("\"MixedCase\"")[0], Token::Ident("MixedCase".into()));
    }

    #[test]
    fn string_escaping_doubles_quote() {
        assert_eq!(toks("'it''s'")[0], Token::StringLit("it's".into()));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(
            toks("1 -- c\n+ /* x */ 2"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn operators_lex() {
        assert_eq!(
            toks("<= >= <> < > = + - * / ( ) , ;"),
            vec![
                Token::Le,
                Token::Ge,
                Token::Ne,
                Token::Lt,
                Token::Gt,
                Token::Eq,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::LParen,
                Token::RParen,
                Token::Comma,
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_string_errors_with_position() {
        let e = lex("'abc").expect_err("unterminated");
        assert_eq!(e.position, 0);
    }

    #[test]
    fn lexes_parameter_placeholder() {
        assert_eq!(toks("$1")[0], Token::Param(1));
    }

    proptest! {
        #[test]
        fn lex_never_panics(s: String) {
            // The lexer must never panic on arbitrary (valid-UTF-8) input —
            // it returns Ok(tokens) or Err(ParseError), never unwinds.
            let _ = lex(&s);
        }
    }
}
