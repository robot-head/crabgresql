//! Errors from the type layer, each carrying the PostgreSQL SQLSTATE the
//! executor maps onto a wire ErrorResponse.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    #[error("integer out of range")]
    Overflow,
    #[error("division by zero")]
    DivisionByZero,
    #[error("invalid input syntax for type {type_name}: \"{value}\"")]
    InvalidText {
        type_name: &'static str,
        value: String,
    },
    #[error("{message}")]
    TypeMismatch { message: String },
}

impl TypeError {
    /// The five-character SQLSTATE for this error.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TypeError::Overflow => "22003",
            TypeError::DivisionByZero => "22012",
            TypeError::InvalidText { .. } => "22P02",
            TypeError::TypeMismatch { .. } => "42804",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_error_maps_to_its_postgres_sqlstate() {
        assert_eq!(TypeError::Overflow.sqlstate(), "22003");
        assert_eq!(TypeError::DivisionByZero.sqlstate(), "22012");
        assert_eq!(
            TypeError::InvalidText {
                type_name: "int4",
                value: "x".into(),
            }
            .sqlstate(),
            "22P02"
        );
        assert_eq!(
            TypeError::TypeMismatch {
                message: "boom".into(),
            }
            .sqlstate(),
            "42804"
        );
    }
}
