//! Versioned row value encoding: a leading version byte (so SP3 can evolve the
//! format) then one tagged field per column. NOT order-preserving — values are
//! never sorted by raw bytes.

use pgtypes::Datum;

use crate::KvError;

/// Current row-value format version.
pub const ROW_VERSION: u8 = 1;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    /// SP30: `float8` (IEEE-754 big-endian f64). Append-only — no version bump.
    pub const FLOAT8: u8 = 5;
    /// SP32: `numeric` — stored as its canonical decimal text (length-prefixed),
    /// which round-trips the value AND its display scale. Append-only.
    pub const NUMERIC: u8 = 6;
    /// SP37: `date` — i32 big-endian days since the PostgreSQL epoch (2000-01-01).
    /// Append-only — no version bump.
    pub const DATE: u8 = 7;
    /// SP37: `time without time zone` — i64 big-endian microseconds since midnight.
    /// Append-only — no version bump.
    pub const TIME: u8 = 8;
    /// SP37: `timestamp without time zone` — i64 big-endian microseconds since the
    /// PostgreSQL epoch. Append-only — no version bump.
    pub const TIMESTAMP: u8 = 9;
    /// SP37: `timestamp with time zone` — i64 big-endian microseconds since the
    /// PostgreSQL epoch (UTC). Append-only — no version bump.
    pub const TIMESTAMPTZ: u8 = 10;
    /// SP37: `interval` — i64 µs ++ i32 days ++ i32 months, all big-endian (16 bytes).
    /// Append-only — no version bump.
    pub const INTERVAL: u8 = 11;
}

pub fn encode_row(cols: &[Datum]) -> Vec<u8> {
    let mut out = vec![ROW_VERSION];
    for d in cols {
        match d {
            Datum::Null => out.push(tag::NULL),
            Datum::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Datum::Int4(n) => {
                out.push(tag::INT4);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Int8(n) => {
                out.push(tag::INT8);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Text(s) => {
                out.push(tag::TEXT);
                let len = u32::try_from(s.len()).expect("text column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Float8(f) => {
                out.push(tag::FLOAT8);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Datum::Numeric(d) => {
                out.push(tag::NUMERIC);
                let s = pgtypes::numeric::to_text(d);
                let len = u32::try_from(s.len()).expect("numeric text exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Date(d) => {
                out.push(tag::DATE);
                out.extend_from_slice(&pgtypes::datetime::date_to_binary(*d));
            }
            Datum::Time(t) => {
                out.push(tag::TIME);
                out.extend_from_slice(&pgtypes::datetime::time_to_binary(*t));
            }
            Datum::Timestamp(ts) => {
                out.push(tag::TIMESTAMP);
                out.extend_from_slice(&pgtypes::datetime::timestamp_to_binary(*ts));
            }
            Datum::Timestamptz(ts) => {
                out.push(tag::TIMESTAMPTZ);
                out.extend_from_slice(&pgtypes::datetime::timestamptz_to_binary(*ts));
            }
            Datum::Interval(iv) => {
                out.push(tag::INTERVAL);
                out.extend_from_slice(&pgtypes::datetime::interval_to_binary(*iv));
            }
        }
    }
    out
}

pub fn decode_row(bytes: &[u8]) -> Result<Vec<Datum>, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != ROW_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown row version {version}"
        )));
    }
    let mut cols = Vec::new();
    while !cur.is_empty() {
        let t = take_u8(&mut cur)?;
        let d = match t {
            tag::NULL => Datum::Null,
            tag::BOOL => Datum::Bool(take_u8(&mut cur)? != 0),
            tag::INT4 => {
                let raw = take_n(&mut cur, 4)?;
                Datum::Int4(i32::from_be_bytes(raw.try_into().expect("4 bytes fit i32")))
            }
            tag::INT8 => {
                let raw = take_n(&mut cur, 8)?;
                Datum::Int8(i64::from_be_bytes(raw.try_into().expect("8 bytes fit i64")))
            }
            tag::TEXT => {
                let len_raw = take_n(&mut cur, 4)?;
                let len = u32::from_be_bytes(len_raw.try_into().expect("4 bytes fit u32")) as usize;
                let raw = take_n(&mut cur, len)?;
                Datum::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| KvError::CorruptRow("text is not valid UTF-8".into()))?,
                )
            }
            tag::FLOAT8 => {
                let raw = take_n(&mut cur, 8)?;
                Datum::Float8(f64::from_be_bytes(raw.try_into().expect("8 bytes fit f64")))
            }
            tag::NUMERIC => {
                let len_raw = take_n(&mut cur, 4)?;
                let len = u32::from_be_bytes(len_raw.try_into().expect("4 bytes fit u32")) as usize;
                let raw = take_n(&mut cur, len)?;
                let s = std::str::from_utf8(raw)
                    .map_err(|_| KvError::CorruptRow("numeric text is not valid UTF-8".into()))?;
                Datum::Numeric(
                    pgtypes::numeric::parse(s)
                        .ok_or_else(|| KvError::CorruptRow(format!("invalid numeric {s:?}")))?,
                )
            }
            tag::DATE => {
                let raw = take_n(&mut cur, 4)?;
                Datum::Date(
                    pgtypes::datetime::date_from_binary(raw)
                        .map_err(|e| KvError::CorruptRow(format!("corrupt date: {e}")))?,
                )
            }
            tag::TIME => {
                let raw = take_n(&mut cur, 8)?;
                Datum::Time(
                    pgtypes::datetime::time_from_binary(raw)
                        .map_err(|e| KvError::CorruptRow(format!("corrupt time: {e}")))?,
                )
            }
            tag::TIMESTAMP => {
                let raw = take_n(&mut cur, 8)?;
                Datum::Timestamp(
                    pgtypes::datetime::timestamp_from_binary(raw)
                        .map_err(|e| KvError::CorruptRow(format!("corrupt timestamp: {e}")))?,
                )
            }
            tag::TIMESTAMPTZ => {
                let raw = take_n(&mut cur, 8)?;
                Datum::Timestamptz(
                    pgtypes::datetime::timestamptz_from_binary(raw)
                        .map_err(|e| KvError::CorruptRow(format!("corrupt timestamptz: {e}")))?,
                )
            }
            tag::INTERVAL => {
                let raw = take_n(&mut cur, 16)?;
                Datum::Interval(
                    pgtypes::datetime::interval_from_binary(raw)
                        .map_err(|e| KvError::CorruptRow(format!("corrupt interval: {e}")))?,
                )
            }
            other => return Err(KvError::CorruptRow(format!("unknown field tag {other}"))),
        };
        cols.push(d);
    }
    Ok(cols)
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (head, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated".into()))?;
    *cur = rest;
    Ok(*head)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated field".into()));
    }
    let (head, rest) = cur.split_at(n);
    *cur = rest;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtypes::Datum;
    use proptest::prelude::*;

    #[test]
    fn roundtrip_all_datum_kinds_including_null() {
        let row = vec![
            Datum::Null,
            Datum::Bool(true),
            Datum::Int4(i32::MIN),
            Datum::Int8(i64::MIN),
            Datum::Text("héllo".into()),
            Datum::Float8(-1.5),
            Datum::Float8(f64::NAN),
            Datum::Float8(-0.0),
            // SP32: numeric round-trips value AND scale (1.50 stays scale 2).
            Datum::Numeric(pgtypes::numeric::parse("1.50").expect("n")),
            Datum::Numeric(pgtypes::numeric::parse("-9999999999999999999.0001").expect("n")),
        ];
        let bytes = encode_row(&row);
        assert_eq!(decode_row(&bytes).expect("decode"), row);
    }

    #[test]
    fn version_byte_is_present() {
        assert_eq!(encode_row(&[Datum::Int4(1)])[0], ROW_VERSION);
    }

    #[test]
    fn truncated_value_errors_not_panics() {
        assert!(decode_row(&[ROW_VERSION, 2, 0, 0]).is_err()); // int4 tag, only 2 payload bytes
    }

    #[test]
    fn numeric_with_an_overflowing_exponent_errors_not_ooms() {
        // Regression (decode_row fuzz OOM): a NUMERIC field whose text parses to an
        // adversarial exponent (`8e88888888`) must be rejected as corrupt, NOT
        // materialized into ~88M digits. tag::NUMERIC = 6, then a u32-BE length and
        // the text bytes.
        let text = b"8e88888888";
        let mut bytes = vec![ROW_VERSION, 6];
        bytes.extend_from_slice(&(text.len() as u32).to_be_bytes());
        bytes.extend_from_slice(text);
        assert!(decode_row(&bytes).is_err());
    }

    #[test]
    fn unknown_version_errors() {
        assert!(decode_row(&[99, 1, 1]).is_err());
    }

    fn arb_datum() -> impl Strategy<Value = Datum> {
        prop_oneof![
            Just(Datum::Null),
            any::<bool>().prop_map(Datum::Bool),
            any::<i32>().prop_map(Datum::Int4),
            any::<i64>().prop_map(Datum::Int8),
            ".*".prop_map(Datum::Text),
            any::<f64>().prop_map(Datum::Float8),
            // SP32: arbitrary numerics (a signed mantissa at a small scale).
            (any::<i64>(), 0u32..6).prop_map(|(m, s)| {
                Datum::Numeric(pgtypes::numeric::parse(&format!("{m}e-{s}")).expect("numeric"))
            }),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_rows(row in prop::collection::vec(arb_datum(), 0..8)) {
            let bytes = encode_row(&row);
            prop_assert_eq!(decode_row(&bytes).expect("decode"), row);
        }
    }

    #[test]
    fn datetime_row_round_trip() {
        use pgtypes::datetime::Interval;
        let row = vec![
            Datum::Date(pgtypes::datetime::parse_date("2024-01-15").expect("d")),
            Datum::Time(pgtypes::datetime::parse_time("13:45:06.5").expect("t")),
            Datum::Timestamp(
                pgtypes::datetime::parse_timestamp("2024-01-15 13:45:06").expect("ts"),
            ),
            Datum::Timestamptz(
                pgtypes::datetime::parse_timestamptz(
                    "2024-01-15 13:45:06+00",
                    &jiff::tz::TimeZone::UTC,
                )
                .expect("tstz"),
            ),
            Datum::Interval(Interval {
                months: 14,
                days: -3,
                micros: 4_500_000,
            }),
        ];
        assert_eq!(decode_row(&encode_row(&row)).expect("decode"), row);
    }
}
