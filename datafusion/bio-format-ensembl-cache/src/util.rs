use crate::errors::{Result, exec_err};
use crate::row::{CellValue, Row};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int8Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use flate2::read::MultiGzDecoder;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

pub(crate) fn open_text_reader(path: &Path) -> Result<Box<dyn BufRead + Send>> {
    let file = File::open(path)
        .map_err(|e| exec_err(format!("Failed opening {}: {}", path.display(), e)))?;

    if path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        let decoder = MultiGzDecoder::new(file);
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::new(file)))
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
        let mut decoder = MultiGzDecoder::new(file);
        decoder
            .read_to_end(&mut bytes)
            .map_err(|e| exec_err(format!("Failed decompressing {}: {}", path.display(), e)))?;
    } else {
        let mut reader = BufReader::new(file);
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
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(BufReader::new(file))
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

pub(crate) fn parse_i64(raw: Option<&str>) -> Option<i64> {
    raw.and_then(|v| normalize_nullable(v))
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_i8(raw: Option<&str>) -> Option<i8> {
    raw.and_then(|v| normalize_nullable(v))
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw.and_then(|v| normalize_nullable(v))
        .and_then(|v| v.parse().ok())
}

pub(crate) fn parse_bool(raw: Option<&str>) -> Option<bool> {
    raw.and_then(|v| normalize_nullable(v))
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

enum AnyBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    Int8(Int8Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

impl AnyBuilder {
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
                None => builder.append_null(),
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
