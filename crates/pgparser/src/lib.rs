//! pgparser: hand-written lexer + recursive-descent/Pratt parser producing the
//! crabgresql AST for the SP2 SQL slice.

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use error::ParseError;
pub use parser::parse;
