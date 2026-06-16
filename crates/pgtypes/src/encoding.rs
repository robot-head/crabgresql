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
        Datum::Float8(f) => encode_float8_text(*f).into_bytes(),
    }
}

/// PostgreSQL `float8out` text rendering. The IEEE specials are spelled exactly as
/// PostgreSQL does (`Infinity`/`-Infinity`/`NaN`); finite values use Rust's `f64`
/// `Display`, which — like PG since v12 — is the shortest round-tripping decimal, so the
/// two agree byte-for-byte for moderate magnitudes (`1.5`, `2.0`→`2`, `-0.0`→`-0`). The
/// one documented divergence is scientific notation for |x| ≥ 1e16 / 0 < |x| < 1e-4,
/// which PG emits and Rust does not.
fn encode_float8_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        format!("{f}")
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
        // IEEE-754 big-endian, matching PostgreSQL's float8send.
        Datum::Float8(f) => f.to_be_bytes().to_vec(),
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
    fn float8_text_encoding_matches_postgres() {
        // Shortest round-trip for moderate magnitudes (agrees with PG float8out).
        assert_eq!(encode_text(&Datum::Float8(1.5)), b"1.5");
        assert_eq!(encode_text(&Datum::Float8(2.0)), b"2"); // PG: `2`
        assert_eq!(encode_text(&Datum::Float8(0.1)), b"0.1");
        assert_eq!(encode_text(&Datum::Float8(-0.0)), b"-0"); // PG: `-0`
        assert_eq!(
            encode_text(&Datum::Float8(1.0 / 3.0)),
            b"0.3333333333333333"
        );
        // IEEE specials spelled as PostgreSQL spells them.
        assert_eq!(encode_text(&Datum::Float8(f64::INFINITY)), b"Infinity");
        assert_eq!(encode_text(&Datum::Float8(f64::NEG_INFINITY)), b"-Infinity");
        assert_eq!(encode_text(&Datum::Float8(f64::NAN)), b"NaN");
    }

    #[test]
    fn binary_encoding_is_network_order() {
        assert_eq!(encode_binary(&Datum::Bool(true)), vec![1]);
        assert_eq!(encode_binary(&Datum::Bool(false)), vec![0]);
        assert_eq!(encode_binary(&Datum::Int4(1)), 1i32.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Int8(1)), 1i64.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Text("hi".into())), b"hi".to_vec());
        // float8 is IEEE-754 big-endian (PG float8send).
        assert_eq!(
            encode_binary(&Datum::Float8(1.5)),
            1.5f64.to_be_bytes().to_vec()
        );
    }

    #[test]
    #[should_panic]
    fn encoding_null_is_a_caller_error() {
        let _ = encode_text(&Datum::Null);
    }
}
