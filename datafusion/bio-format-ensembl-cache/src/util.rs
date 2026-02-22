use crate::errors::{Result, exec_err};
use crate::row::{CellValue, Row};
use crate::schema::exon_list_data_type;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int8Builder, Int32Builder, Int64Builder, ListBuilder,
    StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use flate2::read::MultiGzDecoder;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

const IO_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) fn open_text_reader(path: &Path) -> Result<Box<dyn BufRead + Send>> {
    let file = File::open(path)
        .map_err(|e| exec_err(format!("Failed opening {}: {}", path.display(), e)))?;

    if path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        let decoder = MultiGzDecoder::new(BufReader::with_capacity(IO_BUFFER_SIZE, file));
        Ok(Box::new(BufReader::with_capacity(IO_BUFFER_SIZE, decoder)))
    } else {
        Ok(Box::new(BufReader::with_capacity(IO_BUFFER_SIZE, file)))
    }
}

pub(crate) fn read_maybe_gzip_bytes(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path)
        .map_err(|e| exec_err(format!("Failed opening {}: {}", path.display(), e)))?;
    let mut bytes = Vec::new();

    if path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        let mut decoder = MultiGzDecoder::new(BufReader::with_capacity(IO_BUFFER_SIZE, file));
        decoder
            .read_to_end(&mut bytes)
            .map_err(|e| exec_err(format!("Failed decompressing {}: {}", path.display(), e)))?;
    } else {
        let mut reader = BufReader::with_capacity(IO_BUFFER_SIZE, file);
        reader
            .read_to_end(&mut bytes)
            .map_err(|e| exec_err(format!("Failed reading {}: {}", path.display(), e)))?;
    }

    Ok(bytes)
}

pub(crate) fn read_maybe_gzip_prefix(path: &Path, prefix_len: usize) -> Result<Vec<u8>> {
    let file = File::open(path)
        .map_err(|e| exec_err(format!("Failed opening {}: {}", path.display(), e)))?;

    let mut reader: Box<dyn Read + Send> = if path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        Box::new(MultiGzDecoder::new(BufReader::with_capacity(
            IO_BUFFER_SIZE,
            file,
        )))
    } else {
        Box::new(BufReader::with_capacity(IO_BUFFER_SIZE, file))
    };

    let mut bytes = vec![0u8; prefix_len];
    let mut read_total = 0usize;
    while read_total < prefix_len {
        let read_now = reader
            .read(&mut bytes[read_total..])
            .map_err(|e| exec_err(format!("Failed reading {}: {}", path.display(), e)))?;
        if read_now == 0 {
            break;
        }
        read_total += read_now;
    }
    bytes.truncate(read_total);
    Ok(bytes)
}

pub(crate) fn is_storable_binary_payload(path: &Path) -> Result<bool> {
    Ok(read_maybe_gzip_prefix(path, 4)?.as_slice() == b"pst0")
}

pub(crate) fn normalize_nullable(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "." {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn normalize_nullable_ref(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "." {
        None
    } else {
        Some(trimmed)
    }
}

pub(crate) fn parse_i64(raw: Option<&str>) -> Option<i64> {
    raw.and_then(normalize_nullable)
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_i64_ref(raw: Option<&str>) -> Option<i64> {
    raw.and_then(normalize_nullable_ref)
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_i8_ref(raw: Option<&str>) -> Option<i8> {
    raw.and_then(normalize_nullable_ref)
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_f64_ref(raw: Option<&str>) -> Option<f64> {
    raw.and_then(normalize_nullable_ref)
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_bool(raw: Option<&str>) -> Option<bool> {
    raw.and_then(normalize_nullable)
        .and_then(|v| match v.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        })
}

pub(crate) fn normalize_genomic_start(start: i64, coordinate_system_zero_based: bool) -> i64 {
    if coordinate_system_zero_based {
        start.saturating_sub(1)
    } else {
        start
    }
}

pub(crate) fn normalize_genomic_end(end: i64, _coordinate_system_zero_based: bool) -> i64 {
    end
}

pub(crate) fn stable_hash(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub(crate) fn canonical_json_string(value: &Value) -> Result<String> {
    let canonical = canonicalize_json(value);
    serde_json::to_string(&canonical)
        .map_err(|e| exec_err(format!("Failed serializing canonical JSON payload: {}", e)))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (key, val) in map {
                ordered.insert(key.clone(), canonicalize_json(val));
            }
            let mut out = serde_json::Map::new();
            for (key, val) in ordered {
                out.insert(key, val);
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

pub(crate) fn json_str(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => normalize_nullable(s),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Array(items)) => {
            let mut vals = Vec::new();
            for item in items {
                if let Some(v) = json_str(Some(item)) {
                    vals.push(v);
                }
            }
            if vals.is_empty() {
                None
            } else {
                Some(vals.join(","))
            }
        }
        _ => None,
    }
}

pub(crate) fn json_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn json_i32(value: Option<&Value>) -> Option<i32> {
    json_i64(value).and_then(|v| i32::try_from(v).ok())
}

pub(crate) fn json_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn json_bool(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(v)) => Some(*v),
        Some(Value::String(v)) => parse_bool(Some(v.as_str())),
        Some(Value::Number(v)) => v.as_i64().map(|x| x != 0),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ColumnMap – maps column names to builder indices for the projected schema
// ---------------------------------------------------------------------------

pub(crate) struct ColumnMap {
    map: HashMap<String, usize>,
}

impl ColumnMap {
    pub fn from_schema(schema: &SchemaRef) -> Self {
        let mut map = HashMap::with_capacity(schema.fields().len());
        for (idx, field) in schema.fields().iter().enumerate() {
            map.insert(field.name().clone(), idx);
        }
        Self { map }
    }

    #[inline]
    pub fn get(&self, name: &str) -> Option<usize> {
        self.map.get(name).copied()
    }
}

// ---------------------------------------------------------------------------
// ProvenanceWriter – pre-computed indices for constant provenance columns
// ---------------------------------------------------------------------------

use crate::info::CacheInfo;

struct ProvenanceEntry {
    idx: usize,
    value: Option<String>,
}

pub(crate) struct ProvenanceWriter {
    entries: Vec<ProvenanceEntry>,
    source_file_idx: Option<usize>,
}

impl ProvenanceWriter {
    pub fn new(col_map: &ColumnMap, cache_info: &CacheInfo) -> Self {
        let mut entries = Vec::new();
        if let Some(idx) = col_map.get("species") {
            entries.push(ProvenanceEntry {
                idx,
                value: Some(cache_info.species.clone()),
            });
        }
        if let Some(idx) = col_map.get("assembly") {
            entries.push(ProvenanceEntry {
                idx,
                value: Some(cache_info.assembly.clone()),
            });
        }
        if let Some(idx) = col_map.get("cache_version") {
            entries.push(ProvenanceEntry {
                idx,
                value: Some(cache_info.cache_version.clone()),
            });
        }
        for source in &cache_info.source_descriptors {
            if let Some(idx) = col_map.get(&source.source_column) {
                entries.push(ProvenanceEntry {
                    idx,
                    value: Some(source.value.clone()),
                });
            }
        }
        if let Some(idx) = col_map.get("serializer_type") {
            entries.push(ProvenanceEntry {
                idx,
                value: cache_info.serializer_type.clone(),
            });
        }
        if let Some(idx) = col_map.get("source_cache_path") {
            entries.push(ProvenanceEntry {
                idx,
                value: Some(cache_info.source_cache_path.clone()),
            });
        }
        let source_file_idx = col_map.get("source_file");
        Self {
            entries,
            source_file_idx,
        }
    }

    #[inline]
    pub fn write(&self, batch: &mut BatchBuilder, source_file_str: &str) {
        for entry in &self.entries {
            match &entry.value {
                Some(v) => batch.set_utf8(entry.idx, v),
                None => batch.set_null(entry.idx),
            }
        }
        if let Some(idx) = self.source_file_idx {
            batch.set_utf8(idx, source_file_str);
        }
    }
}

// ---------------------------------------------------------------------------
// BatchBuilder – writes directly into Arrow column builders
// ---------------------------------------------------------------------------

pub(crate) struct BatchBuilder {
    builders: Vec<AnyBuilder>,
    schema: SchemaRef,
    row_count: usize,
    written: Vec<bool>,
    written_count: usize,
}

impl BatchBuilder {
    pub fn new(schema: SchemaRef, capacity: usize) -> Result<Self> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| AnyBuilder::for_type(field.data_type(), capacity))
            .collect::<Result<Vec<_>>>()?;
        let num_cols = builders.len();
        Ok(Self {
            builders,
            schema,
            row_count: 0,
            written: vec![false; num_cols],
            written_count: 0,
        })
    }

    #[inline]
    fn mark_written(&mut self, col: usize) {
        if !self.written[col] {
            self.written[col] = true;
            self.written_count += 1;
        }
    }

    #[inline]
    pub fn set_utf8(&mut self, col: usize, value: &str) {
        if let AnyBuilder::Utf8(b) = &mut self.builders[col] {
            b.append_value(value);
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_utf8(&mut self, col: usize, value: Option<&str>) {
        match value {
            Some(v) => self.set_utf8(col, v),
            None => {
                if let AnyBuilder::Utf8(b) = &mut self.builders[col] {
                    b.append_null();
                }
                self.mark_written(col);
            }
        }
    }

    #[inline]
    pub fn set_opt_utf8_owned(&mut self, col: usize, value: Option<&String>) {
        self.set_opt_utf8(col, value.map(String::as_str));
    }

    #[inline]
    pub fn set_i64(&mut self, col: usize, value: i64) {
        if let AnyBuilder::Int64(b) = &mut self.builders[col] {
            b.append_value(value);
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_i64(&mut self, col: usize, value: Option<i64>) {
        if let AnyBuilder::Int64(b) = &mut self.builders[col] {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_i32(&mut self, col: usize, value: Option<i32>) {
        if let AnyBuilder::Int32(b) = &mut self.builders[col] {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_i8(&mut self, col: usize, value: i8) {
        if let AnyBuilder::Int8(b) = &mut self.builders[col] {
            b.append_value(value);
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_i8(&mut self, col: usize, value: Option<i8>) {
        if let AnyBuilder::Int8(b) = &mut self.builders[col] {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_f64(&mut self, col: usize, value: Option<f64>) {
        if let AnyBuilder::Float64(b) = &mut self.builders[col] {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_opt_bool(&mut self, col: usize, value: Option<bool>) {
        if let AnyBuilder::Boolean(b) = &mut self.builders[col] {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        self.mark_written(col);
    }

    /// Append a list of exon (start, end, phase) tuples to a `List<Struct>` column.
    pub fn set_exon_list(&mut self, col: usize, exons: Option<&[(i64, i64, i8)]>) {
        if let AnyBuilder::ExonList(list_builder) = &mut self.builders[col] {
            match exons {
                Some(exon_slice) => {
                    let struct_builder = list_builder.values();
                    for &(start, end, phase) in exon_slice {
                        struct_builder
                            .field_builder::<Int64Builder>(0)
                            .unwrap()
                            .append_value(start);
                        struct_builder
                            .field_builder::<Int64Builder>(1)
                            .unwrap()
                            .append_value(end);
                        struct_builder
                            .field_builder::<Int8Builder>(2)
                            .unwrap()
                            .append_value(phase);
                        struct_builder.append(true);
                    }
                    list_builder.append(true);
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        self.mark_written(col);
    }

    #[inline]
    pub fn set_null(&mut self, col: usize) {
        self.builders[col].append_null();
        self.mark_written(col);
    }

    pub fn finish_row(&mut self) {
        let num_cols = self.written.len();
        if self.written_count == num_cols {
            // Fast path: all columns written, just reset flags
            self.written.fill(false);
        } else {
            // Slow path: null-fill unwritten columns, then reset
            for idx in 0..num_cols {
                if !self.written[idx] {
                    self.builders[idx].append_null();
                }
            }
            self.written.fill(false);
        }
        self.written_count = 0;
        self.row_count += 1;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.row_count
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        if self.schema.fields().is_empty() {
            let count = self.row_count;
            self.row_count = 0;
            let options = RecordBatchOptions::new().with_row_count(Some(count));
            return RecordBatch::try_new_with_options(self.schema.clone(), Vec::new(), &options)
                .map_err(|e| {
                    exec_err(format!(
                        "Failed building zero-column Ensembl cache RecordBatch: {}",
                        e
                    ))
                });
        }

        let capacity = self.row_count.max(64);
        let old_builders = std::mem::replace(
            &mut self.builders,
            self.schema
                .fields()
                .iter()
                .map(|f| AnyBuilder::for_type(f.data_type(), capacity))
                .collect::<Result<Vec<_>>>()?,
        );
        self.row_count = 0;
        self.written.fill(false);
        self.written_count = 0;

        let arrays: Vec<ArrayRef> = old_builders.into_iter().map(AnyBuilder::finish).collect();
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| exec_err(format!("Failed building Ensembl cache RecordBatch: {}", e)))
    }
}

// ---------------------------------------------------------------------------
// AnyBuilder – type-erased Arrow column builder
// ---------------------------------------------------------------------------

enum AnyBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    Int8(Int8Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    ExonList(ListBuilder<StructBuilder>),
}

impl AnyBuilder {
    #[inline]
    fn append_null(&mut self) {
        match self {
            Self::Utf8(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::ExonList(b) => b.append(false),
        }
    }

    fn for_type(data_type: &DataType, capacity: usize) -> Result<Self> {
        match data_type {
            DataType::Utf8 => Ok(Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity * 24,
            ))),
            DataType::Int64 => Ok(Self::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Int32 => Ok(Self::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int8 => Ok(Self::Int8(Int8Builder::with_capacity(capacity))),
            DataType::Float64 => Ok(Self::Float64(Float64Builder::with_capacity(capacity))),
            DataType::Boolean => Ok(Self::Boolean(BooleanBuilder::with_capacity(capacity))),
            dt if *dt == exon_list_data_type() => {
                let fields = vec![
                    Field::new("start", DataType::Int64, false),
                    Field::new("end", DataType::Int64, false),
                    Field::new("phase", DataType::Int8, false),
                ];
                let struct_builder = StructBuilder::new(
                    fields,
                    vec![
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int8Builder::with_capacity(capacity * 8)),
                    ],
                );
                Ok(Self::ExonList(ListBuilder::new(struct_builder)))
            }
            _ => Err(exec_err(format!(
                "Unsupported data type in Ensembl cache schema: {data_type:?}"
            ))),
        }
    }

    fn append(&mut self, value: Option<&CellValue>) {
        match self {
            Self::Utf8(builder) => match value {
                Some(CellValue::Utf8(v)) => builder.append_value(v),
                Some(CellValue::Int64(v)) => builder.append_value(v.to_string()),
                Some(CellValue::Int32(v)) => builder.append_value(v.to_string()),
                Some(CellValue::Int8(v)) => builder.append_value(v.to_string()),
                Some(CellValue::Float64(v)) => builder.append_value(v.to_string()),
                Some(CellValue::Boolean(v)) => builder.append_value(v.to_string()),
                _ => builder.append_null(),
            },
            Self::Int64(builder) => match value {
                Some(CellValue::Int64(v)) => builder.append_value(*v),
                Some(CellValue::Int32(v)) => builder.append_value(*v as i64),
                Some(CellValue::Int8(v)) => builder.append_value(*v as i64),
                Some(CellValue::Utf8(v)) => match v.parse::<i64>() {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                _ => builder.append_null(),
            },
            Self::Int32(builder) => match value {
                Some(CellValue::Int64(v)) => match i32::try_from(*v) {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                Some(CellValue::Int32(v)) => builder.append_value(*v),
                Some(CellValue::Int8(v)) => builder.append_value(*v as i32),
                Some(CellValue::Utf8(v)) => match v.parse::<i32>() {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                _ => builder.append_null(),
            },
            Self::Int8(builder) => match value {
                Some(CellValue::Int64(v)) => match i8::try_from(*v) {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                Some(CellValue::Int32(v)) => match i8::try_from(*v) {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                Some(CellValue::Int8(v)) => builder.append_value(*v),
                Some(CellValue::Utf8(v)) => match v.parse::<i8>() {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                _ => builder.append_null(),
            },
            Self::Float64(builder) => match value {
                Some(CellValue::Float64(v)) => builder.append_value(*v),
                Some(CellValue::Int64(v)) => builder.append_value(*v as f64),
                Some(CellValue::Int32(v)) => builder.append_value(*v as f64),
                Some(CellValue::Int8(v)) => builder.append_value(*v as f64),
                Some(CellValue::Utf8(v)) => match v.parse::<f64>() {
                    Ok(parsed) => builder.append_value(parsed),
                    Err(_) => builder.append_null(),
                },
                _ => builder.append_null(),
            },
            Self::Boolean(builder) => match value {
                Some(CellValue::Boolean(v)) => builder.append_value(*v),
                Some(CellValue::Int64(v)) => builder.append_value(*v != 0),
                Some(CellValue::Int32(v)) => builder.append_value(*v != 0),
                Some(CellValue::Int8(v)) => builder.append_value(*v != 0),
                Some(CellValue::Utf8(v)) => match v.to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" => builder.append_value(true),
                    "false" | "0" | "no" => builder.append_value(false),
                    _ => builder.append_null(),
                },
                _ => builder.append_null(),
            },
            Self::ExonList(list_builder) => match value {
                Some(CellValue::ExonList(exons)) => {
                    let struct_builder = list_builder.values();
                    for &(start, end, phase) in exons {
                        struct_builder
                            .field_builder::<Int64Builder>(0)
                            .unwrap()
                            .append_value(start);
                        struct_builder
                            .field_builder::<Int64Builder>(1)
                            .unwrap()
                            .append_value(end);
                        struct_builder
                            .field_builder::<Int8Builder>(2)
                            .unwrap()
                            .append_value(phase);
                        struct_builder.append(true);
                    }
                    list_builder.append(true);
                }
                _ => list_builder.append(false),
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int8(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::ExonList(mut builder) => Arc::new(builder.finish()),
        }
    }
}

pub(crate) fn rows_to_record_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, Vec::new(), &options).map_err(|e| {
            exec_err(format!(
                "Failed building zero-column Ensembl cache RecordBatch: {}",
                e
            ))
        });
    }

    let mut builders: Vec<AnyBuilder> = schema
        .fields()
        .iter()
        .map(|field| AnyBuilder::for_type(field.data_type(), rows.len()))
        .collect::<Result<Vec<_>>>()?;

    for row in rows {
        for (field, builder) in schema.fields().iter().zip(builders.iter_mut()) {
            builder.append(row.get(field.name().as_str()));
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(AnyBuilder::finish).collect();
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| exec_err(format!("Failed building Ensembl cache RecordBatch: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int8Array, Int64Array, ListArray, StructArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    fn exon_only_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "exons",
            exon_list_data_type(),
            true,
        )]))
    }

    #[test]
    fn set_exon_list_empty() {
        let schema = exon_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        // Empty exon list: Some(&[])
        builder.set_exon_list(0, Some(&[]));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!list.is_null(0)); // not null, just empty
        assert_eq!(list.value(0).len(), 0);
    }

    #[test]
    fn set_exon_list_null() {
        let schema = exon_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        // Null exon list
        builder.set_exon_list(0, None);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn set_exon_list_single_exon() {
        let schema = exon_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_exon_list(0, Some(&[(100, 200, 0)]));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let structs = list
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        assert_eq!(structs.len(), 1);

        let starts = structs
            .column_by_name("start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = structs
            .column_by_name("end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let phases = structs
            .column_by_name("phase")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();

        assert_eq!(starts.value(0), 100);
        assert_eq!(ends.value(0), 200);
        assert_eq!(phases.value(0), 0);
    }

    #[test]
    fn set_exon_list_multi_exon() {
        let schema = exon_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        let exons = vec![(100, 200, 0), (300, 400, 1), (500, 600, 2)];
        builder.set_exon_list(0, Some(&exons));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let structs = list
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        assert_eq!(structs.len(), 3);

        let starts = structs
            .column_by_name("start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(starts.value(0), 100);
        assert_eq!(starts.value(1), 300);
        assert_eq!(starts.value(2), 500);

        let phases = structs
            .column_by_name("phase")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(phases.value(0), 0);
        assert_eq!(phases.value(1), 1);
        assert_eq!(phases.value(2), 2);
    }

    #[test]
    fn set_exon_list_mixed_rows() {
        let schema = exon_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        // Row 0: multi-exon
        builder.set_exon_list(0, Some(&[(10, 20, 0), (30, 40, -1)]));
        builder.finish_row();

        // Row 1: null
        builder.set_exon_list(0, None);
        builder.finish_row();

        // Row 2: empty list
        builder.set_exon_list(0, Some(&[]));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Row 0: 2 exons
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 2);

        // Row 1: null
        assert!(list.is_null(1));

        // Row 2: empty
        assert!(!list.is_null(2));
        assert_eq!(list.value(2).len(), 0);
    }
}
