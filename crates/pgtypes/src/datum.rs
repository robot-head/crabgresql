//! The runtime value type and the SQL column types of the SP2 slice.

/// PostgreSQL type OIDs (from pg_type.dat) for the slice's types.
pub mod oids {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    /// SP30: `double precision` (IEEE-754 f64).
    pub const FLOAT8: u32 = 701;
}

/// A SQL column type. SP30 added `Float8` (`double precision`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int4,
    Int8,
    Text,
    /// SP30: PostgreSQL `double precision` (an IEEE-754 `f64`).
    Float8,
}

impl ColumnType {
    pub fn from_sql_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "int4" | "integer" | "int" => Some(ColumnType::Int4),
            "int8" | "bigint" => Some(ColumnType::Int8),
            "text" => Some(ColumnType::Text),
            "bool" | "boolean" => Some(ColumnType::Bool),
            // SP30: `float` (no precision) is `double precision` in PostgreSQL; the
            // two-word `double precision` is normalized to this single string by the
            // parser before it reaches here. `real`/`float4` is a deferred non-goal.
            "float8" | "float" | "double precision" => Some(ColumnType::Float8),
            _ => None,
        }
    }

    pub fn oid(self) -> u32 {
        match self {
            ColumnType::Bool => oids::BOOL,
            ColumnType::Int8 => oids::INT8,
            ColumnType::Int4 => oids::INT4,
            ColumnType::Text => oids::TEXT,
            ColumnType::Float8 => oids::FLOAT8,
        }
    }

    /// PostgreSQL type name (for error messages and FieldDescription debugging).
    pub fn name(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int8 => "bigint",
            ColumnType::Int4 => "integer",
            ColumnType::Text => "text",
            ColumnType::Float8 => "double precision",
        }
    }

    /// pg_type.typlen: fixed sizes, -1 for variable-length text.
    pub fn type_size(self) -> i16 {
        match self {
            ColumnType::Bool => 1,
            ColumnType::Int8 => 8,
            ColumnType::Int4 => 4,
            ColumnType::Text => -1,
            ColumnType::Float8 => 8,
        }
    }
}

/// A runtime value.
///
/// `PartialEq`/`Eq`/`Hash` are **hand-written** (SP30), not derived, because of the
/// `Float8` variant: a raw `f64` is not `Eq`/`Hash` (`NaN != NaN`; `-0.0` and `+0.0`
/// have distinct bit patterns yet compare equal). We instead implement PostgreSQL's
/// *grouping* equality (the `float8` btree equality `GROUP BY`/`DISTINCT` use): all
/// `NaN`s are one value, and `-0.0 == +0.0`. The four non-float variants behave exactly
/// as the old derive did. This keys `GROUP BY` group maps and aggregate `DISTINCT` sets.
#[derive(Debug, Clone)]
pub enum Datum {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
    /// SP30: PostgreSQL `double precision`.
    Float8(f64),
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Bool(a), Datum::Bool(b)) => a == b,
            (Datum::Int4(a), Datum::Int4(b)) => a == b,
            (Datum::Int8(a), Datum::Int8(b)) => a == b,
            (Datum::Text(a), Datum::Text(b)) => a == b,
            // Grouping equality: `NaN == NaN` (Rust's `==` says false, hence the
            // explicit NaN arm) and `-0.0 == +0.0` (Rust's `==` already says true).
            (Datum::Float8(a), Datum::Float8(b)) => a == b || (a.is_nan() && b.is_nan()),
            _ => false,
        }
    }
}

// Sound: the relation above is reflexive (NaN now equals itself), symmetric, and
// transitive (every NaN is interchangeable; -0.0/+0.0 are interchangeable).
impl Eq for Datum {}

impl std::hash::Hash for Datum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // A per-variant discriminant keeps distinct variants from colliding cheaply.
        core::mem::discriminant(self).hash(state);
        match self {
            Datum::Null => {}
            Datum::Bool(b) => b.hash(state),
            Datum::Int4(n) => n.hash(state),
            Datum::Int8(n) => n.hash(state),
            Datum::Text(s) => s.hash(state),
            // Canonicalize so equal floats hash equally (the Hash/Eq contract): every
            // NaN → one bit pattern; `-0.0` → `+0.0` (whose bits are all zero).
            Datum::Float8(f) => {
                let bits = if f.is_nan() {
                    0x7ff8_0000_0000_0000u64 // canonical quiet NaN
                } else if *f == 0.0 {
                    0u64 // both -0.0 and +0.0 map here
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            }
        }
    }
}

impl Datum {
    /// The non-null column type of this value (None for NULL).
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Datum::Null => None,
            Datum::Bool(_) => Some(ColumnType::Bool),
            Datum::Int4(_) => Some(ColumnType::Int4),
            Datum::Int8(_) => Some(ColumnType::Int8),
            Datum::Text(_) => Some(ColumnType::Text),
            Datum::Float8(_) => Some(ColumnType::Float8),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_from_sql_names_and_aliases() {
        assert_eq!(ColumnType::from_sql_name("int4"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("integer"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("INT"), Some(ColumnType::Int4));
        assert_eq!(ColumnType::from_sql_name("int8"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("bigint"), Some(ColumnType::Int8));
        assert_eq!(ColumnType::from_sql_name("text"), Some(ColumnType::Text));
        assert_eq!(ColumnType::from_sql_name("bool"), Some(ColumnType::Bool));
        assert_eq!(ColumnType::from_sql_name("boolean"), Some(ColumnType::Bool));
        // SP30: float8 spellings (the two-word `double precision` is assembled by the
        // parser; `from_sql_name` matches the normalized single string and is
        // case-insensitive).
        assert_eq!(
            ColumnType::from_sql_name("float8"),
            Some(ColumnType::Float8)
        );
        assert_eq!(ColumnType::from_sql_name("float"), Some(ColumnType::Float8));
        assert_eq!(
            ColumnType::from_sql_name("double precision"),
            Some(ColumnType::Float8)
        );
        assert_eq!(
            ColumnType::from_sql_name("DOUBLE PRECISION"),
            Some(ColumnType::Float8)
        );
        // `real`/`float4` is a deferred non-goal — unknown for now.
        assert_eq!(ColumnType::from_sql_name("real"), None);
        assert_eq!(ColumnType::from_sql_name("widget"), None);
    }

    #[test]
    fn float8_oid_name_and_size_match_postgres() {
        assert_eq!(ColumnType::Float8.oid(), 701);
        assert_eq!(ColumnType::Float8.name(), "double precision");
        assert_eq!(ColumnType::Float8.type_size(), 8);
        assert_eq!(Datum::Float8(1.5).column_type(), Some(ColumnType::Float8));
    }

    #[test]
    fn float8_grouping_equality_and_hash_match_postgres() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        fn h(d: &Datum) -> u64 {
            let mut s = DefaultHasher::new();
            d.hash(&mut s);
            s.finish()
        }
        // NaN groups with NaN (unlike raw f64 `==`), and equal values hash equally.
        let nan = Datum::Float8(f64::NAN);
        let nan2 = Datum::Float8(f64::from_bits(0x7ff8_0000_0000_0001)); // a different NaN
        assert_eq!(nan, nan2);
        assert_eq!(h(&nan), h(&nan2));
        // -0.0 and +0.0 group together and hash equally.
        let neg0 = Datum::Float8(-0.0);
        let pos0 = Datum::Float8(0.0);
        assert_eq!(neg0, pos0);
        assert_eq!(h(&neg0), h(&pos0));
        // Distinct finite values are distinct.
        assert_ne!(Datum::Float8(1.5), Datum::Float8(2.5));
        // Cross-variant never equal (and an int and a float never collide as equal).
        assert_ne!(Datum::Float8(1.0), Datum::Int4(1));
    }

    #[test]
    fn column_type_oids_match_postgres() {
        assert_eq!(ColumnType::Bool.oid(), 16);
        assert_eq!(ColumnType::Int8.oid(), 20);
        assert_eq!(ColumnType::Int4.oid(), 23);
        assert_eq!(ColumnType::Text.oid(), 25);
    }

    #[test]
    fn datum_reports_its_column_type() {
        assert_eq!(Datum::Int4(1).column_type(), Some(ColumnType::Int4));
        assert_eq!(Datum::Null.column_type(), None);
    }

    #[test]
    fn column_type_names_match_postgres() {
        assert_eq!(ColumnType::Bool.name(), "boolean");
        assert_eq!(ColumnType::Int4.name(), "integer");
        assert_eq!(ColumnType::Int8.name(), "bigint");
        assert_eq!(ColumnType::Text.name(), "text");
    }

    #[test]
    fn column_type_sizes_match_pg_typlen() {
        assert_eq!(ColumnType::Bool.type_size(), 1);
        assert_eq!(ColumnType::Int4.type_size(), 4);
        assert_eq!(ColumnType::Int8.type_size(), 8);
        assert_eq!(ColumnType::Text.type_size(), -1); // variable-length
    }
}
