//! Text and binary wire encodings for Datums. NULL is signalled out-of-band by
//! the wire layer (DataRow value length -1), so encoding a NULL Datum panics —
//! it indicates a caller bug, never reachable from valid execution.

use crate::Datum;

/// PostgreSQL text-format encoding of a (non-null) value.
pub fn encode_text(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_text called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => (if *b { "t" } else { "f" }).as_bytes().to_vec(),
        Datum::Int4(n) => n.to_string().into_bytes(),
        Datum::Int8(n) => n.to_string().into_bytes(),
        Datum::Text(s) => s.clone().into_bytes(),
    }
}

/// PostgreSQL binary-format encoding of a (non-null) value.
pub fn encode_binary(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_binary called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => vec![u8::from(*b)],
        Datum::Int4(n) => n.to_be_bytes().to_vec(),
        Datum::Int8(n) => n.to_be_bytes().to_vec(),
        Datum::Text(s) => s.clone().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Datum;

    #[test]
    fn text_encoding_matches_postgres() {
        assert_eq!(encode_text(&Datum::Bool(true)), b"t");
        assert_eq!(encode_text(&Datum::Bool(false)), b"f");
        assert_eq!(encode_text(&Datum::Int4(-5)), b"-5");
        assert_eq!(encode_text(&Datum::Int8(9_000_000_000)), b"9000000000");
        assert_eq!(encode_text(&Datum::Text("hi".into())), b"hi");
    }

    #[test]
    fn binary_encoding_is_network_order() {
        assert_eq!(encode_binary(&Datum::Bool(true)), vec![1]);
        assert_eq!(encode_binary(&Datum::Bool(false)), vec![0]);
        assert_eq!(encode_binary(&Datum::Int4(1)), 1i32.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Int8(1)), 1i64.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Text("hi".into())), b"hi".to_vec());
    }

    #[test]
    #[should_panic]
    fn encoding_null_is_a_caller_error() {
        let _ = encode_text(&Datum::Null);
    }
}
