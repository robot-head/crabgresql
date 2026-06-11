//! Engine seam: types the wire layer exchanges with the query engine.

/// One column in a RowDescription. Field order matches the wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 = text, 1 = binary.
    pub format: i16,
}
