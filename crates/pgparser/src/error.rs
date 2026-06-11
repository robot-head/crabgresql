//! Parse/lex errors. All map to SQLSTATE 42601 (syntax_error) and carry the
//! byte offset where the problem was detected.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("syntax error at position {position}: {message}")]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl ParseError {
    pub fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: message.into(),
            position,
        }
    }

    pub fn sqlstate(&self) -> &'static str {
        "42601"
    }
}
