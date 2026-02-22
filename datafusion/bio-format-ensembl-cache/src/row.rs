use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) enum CellValue {
    Utf8(String),
    Int64(i64),
    Int32(i32),
    Int8(i8),
    Float64(f64),
    Boolean(bool),
    /// Exon list: Vec of (start, end, phase) tuples.
    ExonList(Vec<(i64, i64, i8)>),
}

pub(crate) type Row = HashMap<String, CellValue>;
