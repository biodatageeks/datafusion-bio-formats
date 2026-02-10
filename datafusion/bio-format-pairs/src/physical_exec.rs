//! Physical execution plan for Pairs file queries.
//!
//! Implements two execution paths:
//! - Indexed: uses tabix for chr1/pos1 region queries with balanced partitioning
//! - Sequential: full scan for unindexed files

use crate::storage::{IndexedPairsReader, new_local_reader};
use async_stream::try_stream;
use datafusion::arrow::array::{NullArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::{
    genomic_filter::GenomicRegion,
    object_storage::{ObjectStorageOptions, StorageType, get_storage_type},
    partition_balancer::PartitionAssignment,
    record_filter::{RecordFieldAccessor, evaluate_record_filters},
};
use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for Pairs file queries.
#[allow(dead_code)]
pub struct PairsExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    /// Column names from the Pairs header
    pub(crate) columns: Vec<String>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    pub(crate) coordinate_system_zero_based: bool,
    /// Partition assignments for indexed queries (None = full scan)
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    /// Path to the TBI/CSI/px2 index file
    pub(crate) index_path: Option<String>,
    /// Residual filters (chr2/pos2/other) applied after index query
    pub(crate) residual_filters: Vec<Expr>,
}

impl Debug for PairsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairsExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for PairsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let proj_str = match &self.projection {
            Some(_) => {
                let col_names: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                col_names.join(", ")
            }
            None => "*".to_string(),
        };
        write!(f, "PairsExec: projection=[{}]", proj_str)
    }
}

impl ExecutionPlan for PairsExec {
    fn name(&self) -> &str {
        "PairsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let proj_cols = match &self.projection {
            Some(_) => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };
        info!(
            "{}: executing partition={} with projection=[{}]",
            self.name(),
            partition,
            proj_cols
        );
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        // Indexed path
        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
        {
            if partition < assignments.len() {
                let regions = assignments[partition].regions.clone();
                let file_path = self.file_path.clone();
                let index_path = index_path.clone();
                let projection = self.projection.clone();
                let columns = self.columns.clone();
                let coord_zero_based = self.coordinate_system_zero_based;
                let residual_filters = self.residual_filters.clone();

                let fut = get_indexed_pairs_stream(
                    file_path,
                    index_path,
                    regions,
                    schema.clone(),
                    batch_size,
                    projection,
                    columns,
                    coord_zero_based,
                    residual_filters,
                );
                let stream = futures::stream::once(fut).try_flatten();
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
            }
        }

        // Sequential full scan
        let fut = get_local_pairs_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.projection.clone(),
            self.columns.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.limit,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Parsed Pairs record for field access (used in residual filter evaluation).
struct PairsRecordFields {
    fields: Vec<(String, String)>,
}

impl PairsRecordFields {
    fn new(columns: &[String], values: &[&str]) -> Self {
        let fields = columns
            .iter()
            .zip(values.iter())
            .map(|(col, val)| (col.clone(), val.to_string()))
            .collect();
        Self { fields }
    }

    fn get_field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

impl RecordFieldAccessor for PairsRecordFields {
    fn get_string_field(&self, name: &str) -> Option<String> {
        match name {
            "readID" | "chr1" | "chr2" | "strand1" | "strand2" => {
                self.get_field(name).map(|s| s.to_string())
            }
            _ => self.get_field(name).map(|s| s.to_string()),
        }
    }

    fn get_u32_field(&self, name: &str) -> Option<u32> {
        match name {
            "pos1" | "pos2" | "frag1" | "frag2" | "mapq1" | "mapq2" => {
                self.get_field(name).and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    fn get_f32_field(&self, _name: &str) -> Option<f32> {
        None
    }

    fn get_f64_field(&self, _name: &str) -> Option<f64> {
        None
    }
}

/// Build a noodles Region from a GenomicRegion.
fn build_noodles_region(region: &GenomicRegion) -> Result<noodles_core::Region, DataFusionError> {
    let region_str = match (region.start, region.end) {
        (Some(start), Some(end)) => format!("{}:{}-{}", region.chrom, start, end),
        (Some(start), None) => format!("{}:{}", region.chrom, start),
        (None, Some(end)) => format!("{}:1-{}", region.chrom, end),
        (None, None) => region.chrom.clone(),
    };

    region_str
        .parse::<noodles_core::Region>()
        .map_err(|e| DataFusionError::Execution(format!("Invalid region '{}': {}", region_str, e)))
}

enum ColumnValue {
    Str(String),
    UInt32(Option<u32>),
}

/// Build a RecordBatch from accumulated column vectors.
fn build_record_batch(
    schema: SchemaRef,
    column_data: &[Vec<ColumnValue>],
    columns: &[String],
    projection: &Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    if record_count == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Handle empty projection (COUNT(*))
    if let Some(proj) = projection {
        if proj.is_empty() {
            let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> =
                vec![Arc::new(NullArray::new(record_count))];
            let null_field = Field::new("__null", DataType::Null, true);
            let null_schema = Arc::new(Schema::new_with_metadata(
                vec![null_field],
                schema.metadata().clone(),
            ));
            return RecordBatch::try_new(null_schema, arrays)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }

    let projected_indices: Vec<usize> = match projection {
        Some(proj) => proj.clone(),
        None => (0..columns.len()).collect(),
    };

    let mut arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        Vec::with_capacity(projected_indices.len());

    for &col_idx in &projected_indices {
        let col_name = &columns[col_idx];
        let data = &column_data[col_idx];

        match col_name.as_str() {
            "pos1" | "pos2" => {
                // Non-nullable u32
                let values: Vec<u32> = data
                    .iter()
                    .map(|v| match v {
                        ColumnValue::UInt32(Some(val)) => *val,
                        _ => 0,
                    })
                    .collect();
                arrays.push(Arc::new(UInt32Array::from(values)));
            }
            "frag1" | "frag2" | "mapq1" | "mapq2" => {
                // Nullable u32
                let values: Vec<Option<u32>> = data
                    .iter()
                    .map(|v| match v {
                        ColumnValue::UInt32(val) => *val,
                        _ => None,
                    })
                    .collect();
                arrays.push(Arc::new(UInt32Array::from(values)));
            }
            _ => {
                // String columns
                let values: Vec<&str> = data
                    .iter()
                    .map(|v| match v {
                        ColumnValue::Str(s) => s.as_str(),
                        _ => "",
                    })
                    .collect();
                arrays.push(Arc::new(StringArray::from(values)));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Sequential scan for local Pairs files.
#[allow(clippy::too_many_arguments)]
async fn get_local_pairs_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    columns: Vec<String>,
    object_storage_options: Option<ObjectStorageOptions>,
    _coordinate_system_zero_based: bool,
    limit: Option<usize>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let storage_type = get_storage_type(file_path.clone());
    if !matches!(storage_type, StorageType::LOCAL) {
        return Err(DataFusionError::Execution(
            "Remote Pairs file reading not yet supported".to_string(),
        ));
    }

    let schema = schema_ref.clone();

    let stream = try_stream! {
        let opts = object_storage_options.unwrap_or_default();
        let mut reader = new_local_reader(&file_path, opts).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to open Pairs file: {}", e)))?;

        let num_columns = columns.len();
        let mut column_data: Vec<Vec<ColumnValue>> = (0..num_columns).map(|_| Vec::with_capacity(batch_size)).collect();
        let mut total_records = 0usize;
        let mut line = String::new();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line)
                .map_err(|e| DataFusionError::Execution(format!("Read error: {}", e)))?;
            if bytes == 0 {
                break;
            }

            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let fields: Vec<&str> = trimmed.split('\t').collect();
            if fields.len() < num_columns.min(2) {
                continue; // Skip malformed lines
            }

            // Parse and store column values (owned to avoid lifetime issues)
            for (i, col) in columns.iter().enumerate() {
                let raw = if i < fields.len() { fields[i] } else { "" };
                match col.as_str() {
                    "pos1" | "pos2" | "frag1" | "frag2" | "mapq1" | "mapq2" => {
                        column_data[i].push(ColumnValue::UInt32(raw.parse().ok()));
                    }
                    _ => {
                        column_data[i].push(ColumnValue::Str(raw.to_string()));
                    }
                }
            }

            total_records += 1;

            if total_records % batch_size == 0 {
                let batch = build_record_batch(
                    schema.clone(),
                    &column_data,
                    &columns,
                    &projection,
                    batch_size,
                )?;
                yield batch;

                for col in &mut column_data {
                    col.clear();
                }
            }

            if let Some(lim) = limit {
                if total_records >= lim {
                    break;
                }
            }
        }

        // Remaining records
        let remaining = total_records % batch_size;
        if remaining > 0 {
            let batch = build_record_batch(
                schema.clone(),
                &column_data,
                &columns,
                &projection,
                remaining,
            )?;
            yield batch;
        }

        debug!("Pairs sequential scan: {} total records", total_records);
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

/// Indexed scan for Pairs files using tabix.
#[allow(clippy::too_many_arguments)]
async fn get_indexed_pairs_stream(
    file_path: String,
    index_path: String,
    regions: Vec<GenomicRegion>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    columns: Vec<String>,
    _coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    use datafusion::arrow::error::ArrowError;

    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<RecordBatch, ArrowError>>(2);

    std::thread::spawn(move || {
        let mut read_and_send = || -> Result<(), DataFusionError> {
            let mut indexed_reader =
                IndexedPairsReader::new(&file_path, &index_path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open indexed Pairs: {}", e))
                })?;

            let num_columns = columns.len();

            // Determine which columns are needed
            let needs: Vec<bool> = (0..num_columns)
                .map(|i| projection.as_ref().is_none_or(|proj| proj.contains(&i)))
                .collect();

            // Accumulator vectors per column (owned strings)
            let mut string_cols: Vec<Vec<String>> = (0..num_columns)
                .map(|_| Vec::with_capacity(batch_size))
                .collect();
            let mut u32_cols: Vec<Vec<Option<u32>>> = (0..num_columns)
                .map(|_| Vec::with_capacity(batch_size))
                .collect();

            let mut total_records = 0usize;

            for region in &regions {
                if region.unmapped_tail {
                    continue;
                }

                let region_start_1based = region.start.map(|s| s as u32);
                let region_end_1based = region.end.map(|e| e as u32);

                let noodles_region = build_noodles_region(region)?;
                let records = indexed_reader.query(&noodles_region).map_err(|e| {
                    DataFusionError::Execution(format!("Pairs region query failed: {}", e))
                })?;

                for result in records {
                    let raw_record = result.map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Pairs indexed record read error: {}",
                            e
                        ))
                    })?;

                    let line: &str = raw_record.as_ref();
                    if line.starts_with('#') {
                        continue;
                    }
                    let fields: Vec<&str> = line.split('\t').collect();
                    if fields.len() < num_columns.min(2) {
                        continue;
                    }

                    // Get pos1 for sub-region dedup (pos1 is typically column index 2)
                    let pos1_idx = columns.iter().position(|c| c == "pos1");
                    if let Some(idx) = pos1_idx {
                        if idx < fields.len() {
                            if let Ok(pos1_val) = fields[idx].parse::<u32>() {
                                if let Some(rs) = region_start_1based {
                                    if pos1_val < rs {
                                        continue;
                                    }
                                }
                                if let Some(re) = region_end_1based {
                                    if pos1_val > re {
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    // Apply residual filters
                    if !residual_filters.is_empty() {
                        let record_fields = PairsRecordFields::new(&columns, &fields);
                        if !evaluate_record_filters(&record_fields, &residual_filters) {
                            continue;
                        }
                    }

                    // Accumulate values
                    for (i, col) in columns.iter().enumerate() {
                        if !needs[i] {
                            continue;
                        }
                        let raw = if i < fields.len() { fields[i] } else { "" };
                        match col.as_str() {
                            "pos1" | "pos2" | "frag1" | "frag2" | "mapq1" | "mapq2" => {
                                u32_cols[i].push(raw.parse().ok());
                            }
                            _ => {
                                string_cols[i].push(raw.to_string());
                            }
                        }
                    }

                    total_records += 1;

                    if total_records % batch_size == 0 {
                        let batch = build_indexed_batch(
                            &schema,
                            &string_cols,
                            &u32_cols,
                            &columns,
                            &projection,
                            &needs,
                            batch_size,
                        )?;

                        loop {
                            match tx.try_send(Ok(batch.clone())) {
                                Ok(()) => break,
                                Err(e) if e.is_disconnected() => return Ok(()),
                                Err(_) => std::thread::yield_now(),
                            }
                        }

                        for i in 0..num_columns {
                            string_cols[i].clear();
                            u32_cols[i].clear();
                        }
                    }
                }
            }

            // Remaining records
            let remaining = total_records % batch_size;
            if remaining > 0 {
                let batch = build_indexed_batch(
                    &schema,
                    &string_cols,
                    &u32_cols,
                    &columns,
                    &projection,
                    &needs,
                    remaining,
                )?;
                loop {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }

            debug!(
                "Indexed Pairs scan: {} records for {} regions",
                total_records,
                regions.len()
            );
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(ArrowError::ExternalError(Box::new(e))));
        }
    });

    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

/// Build a RecordBatch from indexed scan accumulators.
#[allow(clippy::too_many_arguments)]
fn build_indexed_batch(
    schema: &SchemaRef,
    string_cols: &[Vec<String>],
    u32_cols: &[Vec<Option<u32>>],
    columns: &[String],
    projection: &Option<Vec<usize>>,
    needs: &[bool],
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    if record_count == 0 {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    // Handle empty projection (COUNT(*))
    if let Some(proj) = projection {
        if proj.is_empty() {
            let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> =
                vec![Arc::new(NullArray::new(record_count))];
            let null_field = Field::new("__null", DataType::Null, true);
            let null_schema = Arc::new(Schema::new_with_metadata(
                vec![null_field],
                schema.metadata().clone(),
            ));
            return RecordBatch::try_new(null_schema, arrays)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }

    let projected_indices: Vec<usize> = match projection {
        Some(proj) => proj.clone(),
        None => (0..columns.len()).collect(),
    };

    let mut arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        Vec::with_capacity(projected_indices.len());

    for &col_idx in &projected_indices {
        if !needs[col_idx] {
            continue;
        }
        let col_name = &columns[col_idx];

        match col_name.as_str() {
            "pos1" | "pos2" => {
                let values: Vec<u32> = u32_cols[col_idx].iter().map(|v| v.unwrap_or(0)).collect();
                arrays.push(Arc::new(UInt32Array::from(values)));
            }
            "frag1" | "frag2" | "mapq1" | "mapq2" => {
                arrays.push(Arc::new(UInt32Array::from(u32_cols[col_idx].clone())));
            }
            _ => {
                let values: Vec<&str> = string_cols[col_idx].iter().map(|s| s.as_str()).collect();
                arrays.push(Arc::new(StringArray::from(values)));
            }
        }
    }

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}
