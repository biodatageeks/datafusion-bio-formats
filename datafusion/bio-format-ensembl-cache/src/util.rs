use crate::errors::{Result, exec_err};
use crate::schema::{
    cdna_mapper_segment_list_data_type, exon_list_data_type, mirna_region_list_data_type,
    prediction_list_data_type, protein_feature_list_data_type,
};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
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

pub(crate) fn open_binary_reader(path: &Path) -> Result<Box<dyn Read + Send>> {
    let file = File::open(path)
        .map_err(|e| exec_err(format!("Failed opening {}: {}", path.display(), e)))?;

    if path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        Ok(Box::new(MultiGzDecoder::new(BufReader::with_capacity(
            IO_BUFFER_SIZE,
            file,
        ))))
    } else {
        Ok(Box::new(BufReader::with_capacity(IO_BUFFER_SIZE, file)))
    }
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
        .map_err(|e| exec_err(format!("Failed serializing canonical JSON payload: {e}")))
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

    #[cfg(test)]
    pub fn from_map(map: HashMap<String, usize>) -> Self {
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
    pending_error: Option<String>,
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
            pending_error: None,
        })
    }

    #[inline]
    fn set_pending_error_once(&mut self, message: String) {
        if self.pending_error.is_none() {
            self.pending_error = Some(message);
        }
    }

    pub fn take_error(&mut self) -> Option<datafusion::common::DataFusionError> {
        self.pending_error.take().map(exec_err)
    }

    pub fn max_utf8_bytes(&self) -> usize {
        self.builders
            .iter()
            .filter_map(|b| match b {
                AnyBuilder::Utf8(builder) => Some(builder.values_slice().len()),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// Sum of UTF-8 bytes across all string columns.
    ///
    /// Used for memory-aware batch flushing when many string columns are
    /// projected. A batch with 10 columns each at 30 MB uses 300 MB even
    /// though no single column exceeds the per-column threshold.
    pub fn total_utf8_bytes(&self) -> usize {
        self.builders
            .iter()
            .filter_map(|b| match b {
                AnyBuilder::Utf8(builder) => Some(builder.values_slice().len()),
                _ => None,
            })
            .sum()
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
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::Utf8(b) = &mut self.builders[col] {
            let current = b.values_slice().len();
            let next = current.saturating_add(value.len());
            if next > i32::MAX as usize {
                overflow = Some((current, value.len()));
                b.append_null();
            } else {
                b.append_value(value);
            }
        }
        if let Some((current, value_len)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow UTF-8 offset overflow in column '{col_name}' ({current} + {value_len} bytes exceeds i32::MAX). \
                 Reduce batch_size_hint or avoid projecting very large text columns."
            ));
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
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::ExonList(list_builder) = &mut self.builders[col] {
            match exons {
                Some(exon_slice) => {
                    let current_child_len = list_builder.values().len();
                    let next_child_len = current_child_len.saturating_add(exon_slice.len());
                    if next_child_len > i32::MAX as usize {
                        overflow = Some((current_child_len, exon_slice.len()));
                        list_builder.append(false);
                        self.mark_written(col);
                    } else {
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
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        if let Some((current_child_len, added)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow List offset overflow in column '{col_name}' ({current_child_len} + {added} values exceeds i32::MAX). \
                 Reduce batch_size_hint."
            ));
            return;
        }
        self.mark_written(col);
    }

    /// Append a list of mature miRNA genomic regions to a `List<Struct>` column.
    pub fn set_mirna_region_list(&mut self, col: usize, regions: Option<&[(i64, i64)]>) {
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::MirnaRegionList(list_builder) = &mut self.builders[col] {
            match regions {
                Some(region_slice) => {
                    let current_child_len = list_builder.values().len();
                    let next_child_len = current_child_len.saturating_add(region_slice.len());
                    if next_child_len > i32::MAX as usize {
                        overflow = Some((current_child_len, region_slice.len()));
                        list_builder.append(false);
                        self.mark_written(col);
                    } else {
                        let struct_builder = list_builder.values();
                        for &(start, end) in region_slice {
                            struct_builder
                                .field_builder::<Int64Builder>(0)
                                .unwrap()
                                .append_value(start);
                            struct_builder
                                .field_builder::<Int64Builder>(1)
                                .unwrap()
                                .append_value(end);
                            struct_builder.append(true);
                        }
                        list_builder.append(true);
                    }
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        if let Some((current_child_len, added)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow List offset overflow in column '{col_name}' ({current_child_len} + {added} values exceeds i32::MAX). \
                 Reduce batch_size_hint."
            ));
            return;
        }
        self.mark_written(col);
    }

    /// Append a list of cDNA mapper segments to a `List<Struct>` column.
    /// Each segment is `(genomic_start, genomic_end, cdna_start, cdna_end, ori)`.
    #[allow(clippy::type_complexity)]
    pub fn set_cdna_mapper_list(
        &mut self,
        col: usize,
        segments: Option<&[(i64, i64, i64, i64, i8)]>,
    ) {
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::CdnaMapperList(list_builder) = &mut self.builders[col] {
            match segments {
                Some(seg_slice) => {
                    let current_child_len = list_builder.values().len();
                    let next_child_len = current_child_len.saturating_add(seg_slice.len());
                    if next_child_len > i32::MAX as usize {
                        overflow = Some((current_child_len, seg_slice.len()));
                        list_builder.append(false);
                        self.mark_written(col);
                    } else {
                        let struct_builder = list_builder.values();
                        for &(g_start, g_end, c_start, c_end, ori) in seg_slice {
                            struct_builder
                                .field_builder::<Int64Builder>(0)
                                .unwrap()
                                .append_value(g_start);
                            struct_builder
                                .field_builder::<Int64Builder>(1)
                                .unwrap()
                                .append_value(g_end);
                            struct_builder
                                .field_builder::<Int64Builder>(2)
                                .unwrap()
                                .append_value(c_start);
                            struct_builder
                                .field_builder::<Int64Builder>(3)
                                .unwrap()
                                .append_value(c_end);
                            struct_builder
                                .field_builder::<Int8Builder>(4)
                                .unwrap()
                                .append_value(ori);
                            struct_builder.append(true);
                        }
                        list_builder.append(true);
                    }
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        if let Some((current_child_len, added)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow List offset overflow in column '{col_name}' ({current_child_len} + {added} values exceeds i32::MAX). \
                 Reduce batch_size_hint."
            ));
            return;
        }
        self.mark_written(col);
    }

    /// Append a list of protein features to a `List<Struct>` column.
    /// Each feature is `(analysis, hseqname, start, end)`.
    #[allow(clippy::type_complexity)]
    pub fn set_protein_feature_list(
        &mut self,
        col: usize,
        features: Option<&[(Option<String>, Option<String>, Option<i64>, Option<i64>)]>,
    ) {
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::ProteinFeatureList(list_builder) = &mut self.builders[col] {
            match features {
                Some(feat_slice) => {
                    let current_child_len = list_builder.values().len();
                    let next_child_len = current_child_len.saturating_add(feat_slice.len());
                    if next_child_len > i32::MAX as usize {
                        overflow = Some((current_child_len, feat_slice.len()));
                        list_builder.append(false);
                        self.mark_written(col);
                    } else {
                        let struct_builder = list_builder.values();
                        for (analysis, hseqname, start, end) in feat_slice {
                            let analysis_builder =
                                struct_builder.field_builder::<StringBuilder>(0).unwrap();
                            match analysis {
                                Some(v) => analysis_builder.append_value(v),
                                None => analysis_builder.append_null(),
                            }
                            let hseqname_builder =
                                struct_builder.field_builder::<StringBuilder>(1).unwrap();
                            match hseqname {
                                Some(v) => hseqname_builder.append_value(v),
                                None => hseqname_builder.append_null(),
                            }
                            let start_builder =
                                struct_builder.field_builder::<Int64Builder>(2).unwrap();
                            match start {
                                Some(v) => start_builder.append_value(*v),
                                None => start_builder.append_null(),
                            }
                            let end_builder =
                                struct_builder.field_builder::<Int64Builder>(3).unwrap();
                            match end {
                                Some(v) => end_builder.append_value(*v),
                                None => end_builder.append_null(),
                            }
                            struct_builder.append(true);
                        }
                        list_builder.append(true);
                    }
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        if let Some((current_child_len, added)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow List offset overflow in column '{col_name}' ({current_child_len} + {added} values exceeds i32::MAX). \
                 Reduce batch_size_hint."
            ));
            return;
        }
        self.mark_written(col);
    }

    /// Append a list of SIFT/PolyPhen prediction entries to a `List<Struct>` column.
    /// Each entry is `(position, amino_acid, prediction, score)`.
    pub fn set_prediction_list(
        &mut self,
        col: usize,
        predictions: Option<&[(i32, String, String, f32)]>,
    ) {
        let mut overflow: Option<(usize, usize)> = None;
        if let AnyBuilder::PredictionList(list_builder) = &mut self.builders[col] {
            match predictions {
                Some(pred_slice) => {
                    let current_child_len = list_builder.values().len();
                    let next_child_len = current_child_len.saturating_add(pred_slice.len());
                    if next_child_len > i32::MAX as usize {
                        overflow = Some((current_child_len, pred_slice.len()));
                        list_builder.append(false);
                        self.mark_written(col);
                    } else {
                        let struct_builder = list_builder.values();
                        for (position, amino_acid, prediction, score) in pred_slice {
                            struct_builder
                                .field_builder::<Int32Builder>(0)
                                .unwrap()
                                .append_value(*position);
                            struct_builder
                                .field_builder::<StringBuilder>(1)
                                .unwrap()
                                .append_value(amino_acid);
                            struct_builder
                                .field_builder::<StringBuilder>(2)
                                .unwrap()
                                .append_value(prediction);
                            struct_builder
                                .field_builder::<Float32Builder>(3)
                                .unwrap()
                                .append_value(*score);
                            struct_builder.append(true);
                        }
                        list_builder.append(true);
                    }
                }
                None => {
                    list_builder.append(false);
                }
            }
        }
        if let Some((current_child_len, added)) = overflow {
            let col_name = self.schema.field(col).name().clone();
            self.set_pending_error_once(format!(
                "Arrow List offset overflow in column '{col_name}' ({current_child_len} + {added} values exceeds i32::MAX). \
                 Reduce batch_size_hint."
            ));
            return;
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
                        "Failed building zero-column Ensembl cache RecordBatch: {e}"
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
            .map_err(|e| exec_err(format!("Failed building Ensembl cache RecordBatch: {e}")))
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
    MirnaRegionList(ListBuilder<StructBuilder>),
    CdnaMapperList(ListBuilder<StructBuilder>),
    ProteinFeatureList(ListBuilder<StructBuilder>),
    PredictionList(ListBuilder<StructBuilder>),
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
            Self::MirnaRegionList(b) => b.append(false),
            Self::CdnaMapperList(b) => b.append(false),
            Self::ProteinFeatureList(b) => b.append(false),
            Self::PredictionList(b) => b.append(false),
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
            dt if *dt == cdna_mapper_segment_list_data_type() => {
                let fields = vec![
                    Field::new("genomic_start", DataType::Int64, false),
                    Field::new("genomic_end", DataType::Int64, false),
                    Field::new("cdna_start", DataType::Int64, false),
                    Field::new("cdna_end", DataType::Int64, false),
                    Field::new("ori", DataType::Int8, false),
                ];
                let struct_builder = StructBuilder::new(
                    fields,
                    vec![
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int64Builder::with_capacity(capacity * 8)),
                        Box::new(Int8Builder::with_capacity(capacity * 8)),
                    ],
                );
                Ok(Self::CdnaMapperList(ListBuilder::new(struct_builder)))
            }
            dt if *dt == mirna_region_list_data_type() => {
                let fields = vec![
                    Field::new("start", DataType::Int64, false),
                    Field::new("end", DataType::Int64, false),
                ];
                let struct_builder = StructBuilder::new(
                    fields,
                    vec![
                        Box::new(Int64Builder::with_capacity(capacity * 4)),
                        Box::new(Int64Builder::with_capacity(capacity * 4)),
                    ],
                );
                Ok(Self::MirnaRegionList(ListBuilder::new(struct_builder)))
            }
            dt if *dt == protein_feature_list_data_type() => {
                let fields = vec![
                    Field::new("analysis", DataType::Utf8, true),
                    Field::new("hseqname", DataType::Utf8, true),
                    Field::new("start", DataType::Int64, true),
                    Field::new("end", DataType::Int64, true),
                ];
                let struct_builder = StructBuilder::new(
                    fields,
                    vec![
                        Box::new(StringBuilder::with_capacity(capacity * 2, capacity * 16)),
                        Box::new(StringBuilder::with_capacity(capacity * 2, capacity * 16)),
                        Box::new(Int64Builder::with_capacity(capacity * 2)),
                        Box::new(Int64Builder::with_capacity(capacity * 2)),
                    ],
                );
                Ok(Self::ProteinFeatureList(ListBuilder::new(struct_builder)))
            }
            dt if *dt == prediction_list_data_type() => {
                let fields = vec![
                    Field::new("position", DataType::Int32, false),
                    Field::new("amino_acid", DataType::Utf8, false),
                    Field::new("prediction", DataType::Utf8, false),
                    Field::new("score", DataType::Float32, false),
                ];
                let struct_builder = StructBuilder::new(
                    fields,
                    vec![
                        Box::new(Int32Builder::with_capacity(capacity * 4)),
                        Box::new(StringBuilder::with_capacity(capacity * 4, capacity * 4)),
                        Box::new(StringBuilder::with_capacity(capacity * 4, capacity * 32)),
                        Box::new(Float32Builder::with_capacity(capacity * 4)),
                    ],
                );
                Ok(Self::PredictionList(ListBuilder::new(struct_builder)))
            }
            _ => Err(exec_err(format!(
                "Unsupported data type in Ensembl cache schema: {data_type:?}"
            ))),
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
            Self::MirnaRegionList(mut builder) => Arc::new(builder.finish()),
            Self::CdnaMapperList(mut builder) => Arc::new(builder.finish()),
            Self::ProteinFeatureList(mut builder) => Arc::new(builder.finish()),
            Self::PredictionList(mut builder) => Arc::new(builder.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, BooleanArray, Float64Array, Int8Array, Int32Array, Int64Array, ListArray,
        StringArray, StructArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use serde_json::json;

    // -----------------------------------------------------------------------
    // normalize_nullable / normalize_nullable_ref
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_nullable_non_empty() {
        assert_eq!(normalize_nullable("hello"), Some("hello".to_string()));
    }

    #[test]
    fn normalize_nullable_empty() {
        assert_eq!(normalize_nullable(""), None);
    }

    #[test]
    fn normalize_nullable_dot() {
        assert_eq!(normalize_nullable("."), None);
    }

    #[test]
    fn normalize_nullable_whitespace() {
        assert_eq!(normalize_nullable("  "), None);
    }

    #[test]
    fn normalize_nullable_trimmed() {
        assert_eq!(normalize_nullable("  value  "), Some("value".to_string()));
    }

    #[test]
    fn normalize_nullable_ref_non_empty() {
        assert_eq!(normalize_nullable_ref("hello"), Some("hello"));
    }

    #[test]
    fn normalize_nullable_ref_dot() {
        assert_eq!(normalize_nullable_ref("."), None);
    }

    #[test]
    fn normalize_nullable_ref_whitespace_dot() {
        assert_eq!(normalize_nullable_ref(" . "), None);
    }

    // -----------------------------------------------------------------------
    // parse_* helpers
    // -----------------------------------------------------------------------

    #[test]
    fn parse_i64_valid() {
        assert_eq!(parse_i64(Some("42")), Some(42));
    }

    #[test]
    fn parse_i64_negative() {
        assert_eq!(parse_i64(Some("-10")), Some(-10));
    }

    #[test]
    fn parse_i64_dot() {
        assert_eq!(parse_i64(Some(".")), None);
    }

    #[test]
    fn parse_i64_none() {
        assert_eq!(parse_i64(None), None);
    }

    #[test]
    fn parse_i64_invalid() {
        assert_eq!(parse_i64(Some("abc")), None);
    }

    #[test]
    fn parse_i64_ref_valid() {
        assert_eq!(parse_i64_ref(Some("100")), Some(100));
    }

    #[test]
    fn parse_i64_ref_whitespace() {
        assert_eq!(parse_i64_ref(Some(" 100 ")), Some(100));
    }

    #[test]
    fn parse_i8_ref_valid() {
        assert_eq!(parse_i8_ref(Some("1")), Some(1));
    }

    #[test]
    fn parse_i8_ref_negative() {
        assert_eq!(parse_i8_ref(Some("-1")), Some(-1));
    }

    #[test]
    fn parse_i8_ref_overflow() {
        assert_eq!(parse_i8_ref(Some("200")), None);
    }

    #[test]
    fn parse_f64_ref_valid() {
        assert_eq!(parse_f64_ref(Some("0.5")), Some(0.5));
    }

    #[test]
    fn parse_f64_ref_integer() {
        assert_eq!(parse_f64_ref(Some("42")), Some(42.0));
    }

    #[test]
    fn parse_f64_ref_invalid() {
        assert_eq!(parse_f64_ref(Some("abc")), None);
    }

    // -----------------------------------------------------------------------
    // parse_bool
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bool_true_variants() {
        assert_eq!(parse_bool(Some("true")), Some(true));
        assert_eq!(parse_bool(Some("1")), Some(true));
        assert_eq!(parse_bool(Some("yes")), Some(true));
        assert_eq!(parse_bool(Some("TRUE")), Some(true));
    }

    #[test]
    fn parse_bool_false_variants() {
        assert_eq!(parse_bool(Some("false")), Some(false));
        assert_eq!(parse_bool(Some("0")), Some(false));
        assert_eq!(parse_bool(Some("no")), Some(false));
    }

    #[test]
    fn parse_bool_unknown() {
        assert_eq!(parse_bool(Some("maybe")), None);
    }

    #[test]
    fn parse_bool_dot() {
        assert_eq!(parse_bool(Some(".")), None);
    }

    #[test]
    fn parse_bool_none() {
        assert_eq!(parse_bool(None), None);
    }

    // -----------------------------------------------------------------------
    // normalize_genomic_start / normalize_genomic_end
    // -----------------------------------------------------------------------

    #[test]
    fn genomic_start_one_based() {
        assert_eq!(normalize_genomic_start(100, false), 100);
    }

    #[test]
    fn genomic_start_zero_based() {
        assert_eq!(normalize_genomic_start(100, true), 99);
    }

    #[test]
    fn genomic_start_zero_based_at_one() {
        assert_eq!(normalize_genomic_start(1, true), 0);
    }

    #[test]
    fn genomic_start_zero_based_at_zero() {
        // i64::saturating_sub(1) on 0 yields -1 (no unsigned saturation)
        assert_eq!(normalize_genomic_start(0, true), -1);
    }

    #[test]
    fn genomic_end_unchanged() {
        assert_eq!(normalize_genomic_end(200, false), 200);
        assert_eq!(normalize_genomic_end(200, true), 200);
    }

    // -----------------------------------------------------------------------
    // stable_hash
    // -----------------------------------------------------------------------

    #[test]
    fn stable_hash_deterministic() {
        let h1 = stable_hash("hello");
        let h2 = stable_hash("hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn stable_hash_different_inputs() {
        assert_ne!(stable_hash("hello"), stable_hash("world"));
    }

    #[test]
    fn stable_hash_format() {
        let h = stable_hash("test");
        assert_eq!(h.len(), 16);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn stable_hash_empty_string() {
        let h = stable_hash("");
        assert_eq!(h.len(), 16);
    }

    // -----------------------------------------------------------------------
    // canonical_json_string / canonicalize_json
    // -----------------------------------------------------------------------

    #[test]
    fn canonical_json_sorts_keys() {
        let val = json!({"b": 2, "a": 1});
        let s = canonical_json_string(&val).unwrap();
        assert_eq!(s, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn canonical_json_nested() {
        let val = json!({"z": {"b": 2, "a": 1}, "a": 0});
        let s = canonical_json_string(&val).unwrap();
        assert_eq!(s, r#"{"a":0,"z":{"a":1,"b":2}}"#);
    }

    #[test]
    fn canonical_json_array_preserves_order() {
        let val = json!([3, 1, 2]);
        let s = canonical_json_string(&val).unwrap();
        assert_eq!(s, "[3,1,2]");
    }

    #[test]
    fn canonical_json_scalar() {
        let val = json!("hello");
        let s = canonical_json_string(&val).unwrap();
        assert_eq!(s, r#""hello""#);
    }

    // -----------------------------------------------------------------------
    // json_str
    // -----------------------------------------------------------------------

    #[test]
    fn json_str_string() {
        assert_eq!(json_str(Some(&json!("hello"))), Some("hello".to_string()));
    }

    #[test]
    fn json_str_number() {
        assert_eq!(json_str(Some(&json!(42))), Some("42".to_string()));
    }

    #[test]
    fn json_str_bool() {
        assert_eq!(json_str(Some(&json!(true))), Some("true".to_string()));
    }

    #[test]
    fn json_str_null() {
        assert_eq!(json_str(Some(&json!(null))), None);
    }

    #[test]
    fn json_str_none() {
        assert_eq!(json_str(None), None);
    }

    #[test]
    fn json_str_array_of_strings() {
        assert_eq!(
            json_str(Some(&json!(["a", "b", "c"]))),
            Some("a,b,c".to_string())
        );
    }

    #[test]
    fn json_str_empty_array() {
        assert_eq!(json_str(Some(&json!([]))), None);
    }

    #[test]
    fn json_str_dot_string() {
        assert_eq!(json_str(Some(&json!("."))), None);
    }

    #[test]
    fn json_str_empty_string() {
        assert_eq!(json_str(Some(&json!(""))), None);
    }

    // -----------------------------------------------------------------------
    // json_i64
    // -----------------------------------------------------------------------

    #[test]
    fn json_i64_number() {
        assert_eq!(json_i64(Some(&json!(42))), Some(42));
    }

    #[test]
    fn json_i64_negative() {
        assert_eq!(json_i64(Some(&json!(-10))), Some(-10));
    }

    #[test]
    fn json_i64_string() {
        assert_eq!(json_i64(Some(&json!("42"))), Some(42));
    }

    #[test]
    fn json_i64_invalid_string() {
        assert_eq!(json_i64(Some(&json!("abc"))), None);
    }

    #[test]
    fn json_i64_null() {
        assert_eq!(json_i64(Some(&json!(null))), None);
    }

    #[test]
    fn json_i64_none() {
        assert_eq!(json_i64(None), None);
    }

    // -----------------------------------------------------------------------
    // json_i32
    // -----------------------------------------------------------------------

    #[test]
    fn json_i32_valid() {
        assert_eq!(json_i32(Some(&json!(42))), Some(42));
    }

    #[test]
    fn json_i32_overflow() {
        assert_eq!(json_i32(Some(&json!(3_000_000_000i64))), None);
    }

    // -----------------------------------------------------------------------
    // json_f64
    // -----------------------------------------------------------------------

    #[test]
    fn json_f64_number() {
        assert_eq!(json_f64(Some(&json!(3.15))), Some(3.15));
    }

    #[test]
    fn json_f64_string() {
        assert_eq!(json_f64(Some(&json!("3.15"))), Some(3.15));
    }

    #[test]
    fn json_f64_integer() {
        assert_eq!(json_f64(Some(&json!(42))), Some(42.0));
    }

    // -----------------------------------------------------------------------
    // json_bool
    // -----------------------------------------------------------------------

    #[test]
    fn json_bool_true() {
        assert_eq!(json_bool(Some(&json!(true))), Some(true));
    }

    #[test]
    fn json_bool_false() {
        assert_eq!(json_bool(Some(&json!(false))), Some(false));
    }

    #[test]
    fn json_bool_number_1() {
        assert_eq!(json_bool(Some(&json!(1))), Some(true));
    }

    #[test]
    fn json_bool_number_0() {
        assert_eq!(json_bool(Some(&json!(0))), Some(false));
    }

    #[test]
    fn json_bool_string_true() {
        assert_eq!(json_bool(Some(&json!("true"))), Some(true));
    }

    #[test]
    fn json_bool_string_yes() {
        assert_eq!(json_bool(Some(&json!("yes"))), Some(true));
    }

    #[test]
    fn json_bool_null() {
        assert_eq!(json_bool(Some(&json!(null))), None);
    }

    // -----------------------------------------------------------------------
    // ColumnMap
    // -----------------------------------------------------------------------

    #[test]
    fn column_map_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Boolean, true),
        ]));
        let map = ColumnMap::from_schema(&schema);
        assert_eq!(map.get("a"), Some(0));
        assert_eq!(map.get("b"), Some(1));
        assert_eq!(map.get("c"), Some(2));
        assert_eq!(map.get("d"), None);
    }

    // -----------------------------------------------------------------------
    // BatchBuilder basics
    // -----------------------------------------------------------------------

    fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new("flag", DataType::Boolean, true),
        ]))
    }

    #[test]
    fn batch_builder_basic_row() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_utf8(0, "hello");
        builder.set_i64(1, 42);
        builder.set_opt_bool(2, Some(true));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "hello");

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);

        let flags = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.value(0));
    }

    #[test]
    fn batch_builder_unwritten_columns_null_filled() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_utf8(0, "test");
        builder.set_i64(1, 1);
        // Don't set column 2 (flag)
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let flags = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.is_null(0));
    }

    #[test]
    fn batch_builder_multiple_rows() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        for i in 0..5 {
            builder.set_utf8(0, &format!("row{i}"));
            builder.set_i64(1, i as i64);
            builder.set_opt_bool(2, None);
            builder.finish_row();
        }

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn batch_builder_empty_batch() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn batch_builder_all_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_opt_utf8(0, Some("test"));
        builder.set_opt_i64(1, Some(100));
        builder.set_opt_i32(2, Some(42));
        builder.set_opt_i8(3, Some(-1));
        builder.set_opt_f64(4, Some(3.15));
        builder.set_opt_bool(5, Some(false));
        builder.finish_row();

        builder.set_null(0);
        builder.set_null(1);
        builder.set_null(2);
        builder.set_null(3);
        builder.set_null(4);
        builder.set_null(5);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Row 0 has values
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "test");
        assert!(s.is_null(1));

        let i64_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(i64_col.value(0), 100);
        assert!(i64_col.is_null(1));

        let i32_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(i32_col.value(0), 42);

        let i8_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(i8_col.value(0), -1);

        let f64_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((f64_col.value(0) - 3.15).abs() < f64::EPSILON);

        let bool_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!bool_col.value(0));
    }

    #[test]
    fn batch_builder_len_tracks_rows() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();
        assert_eq!(builder.len(), 0);

        builder.set_utf8(0, "a");
        builder.set_i64(1, 1);
        builder.finish_row();
        assert_eq!(builder.len(), 1);

        builder.set_utf8(0, "b");
        builder.set_i64(1, 2);
        builder.finish_row();
        assert_eq!(builder.len(), 2);

        let _ = builder.finish().unwrap();
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn batch_builder_finish_resets() {
        let schema = simple_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_utf8(0, "a");
        builder.set_i64(1, 1);
        builder.finish_row();
        let batch1 = builder.finish().unwrap();

        builder.set_utf8(0, "b");
        builder.set_i64(1, 2);
        builder.finish_row();
        let batch2 = builder.finish().unwrap();

        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(batch2.num_rows(), 1);

        let names = batch2
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "b");
    }

    #[test]
    fn batch_builder_opt_utf8_owned() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        let val = "test".to_string();
        builder.set_opt_utf8_owned(0, Some(&val));
        builder.finish_row();
        builder.set_opt_utf8_owned(0, None);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "test");
        assert!(col.is_null(1));
    }

    #[test]
    fn batch_builder_max_utf8_bytes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_utf8(0, "short");
        builder.set_utf8(1, "much longer string here");
        builder.finish_row();

        assert!(builder.max_utf8_bytes() >= 23);
        assert!(builder.total_utf8_bytes() >= 28);
    }

    #[test]
    fn batch_builder_zero_column_schema() {
        let schema = Arc::new(Schema::empty());
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.finish_row();
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);
    }

    #[test]
    fn batch_builder_unsupported_type_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, None),
            false,
        )]));
        assert!(BatchBuilder::new(schema, 4).is_err());
    }

    // -----------------------------------------------------------------------
    // Exon and miRNA list builders (existing tests below)
    // -----------------------------------------------------------------------

    fn exon_only_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "exons",
            exon_list_data_type(),
            true,
        )]))
    }

    fn mirna_regions_only_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "mature_mirna_regions",
            mirna_region_list_data_type(),
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

    #[test]
    fn set_mirna_region_list_single_region() {
        let schema = mirna_regions_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_mirna_region_list(0, Some(&[(100, 120)]));
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

        assert_eq!(starts.value(0), 100);
        assert_eq!(ends.value(0), 120);
    }

    #[test]
    fn set_mirna_region_list_null_and_empty() {
        let schema = mirna_regions_only_schema();
        let mut builder = BatchBuilder::new(schema.clone(), 4).unwrap();

        builder.set_mirna_region_list(0, None);
        builder.finish_row();
        builder.set_mirna_region_list(0, Some(&[]));
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert!(list.is_null(0));
        assert!(!list.is_null(1));
        assert_eq!(list.value(1).len(), 0);
    }
}
