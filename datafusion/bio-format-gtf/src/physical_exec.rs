use crate::storage::{
    GtfLocalReader, GtfRecordTrait, GtfRemoteReader, IndexedGtfReader, parse_gtf_line,
    parse_gtf_pair,
};
use async_stream::try_stream;
use datafusion::arrow::array::{
    Array, Float32Builder, NullArray, RecordBatch, StringArray, UInt32Array, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::{
    genomic_filter::GenomicRegion,
    object_storage::{ObjectStorageOptions, StorageType, get_storage_type},
    partition_balancer::PartitionAssignment,
    table_utils::{Attribute, OptionalField, builders_to_arrays},
};
use futures_util::{StreamExt, TryStreamExt};
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct GtfExec {
    pub(crate) file_path: String,
    pub(crate) attr_fields: Option<Vec<String>>,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) cache: PlanProperties,
    pub(crate) coordinate_system_zero_based: bool,
    /// Optional cloud storage configuration for remote files
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// Partition assignments for index-based queries (None = full scan)
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    /// Path to the TBI/CSI index file
    pub(crate) index_path: Option<String>,
    /// Residual filters to apply after index-based region queries
    pub(crate) residual_filters: Vec<Expr>,
}

impl Debug for GtfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GtfExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for GtfExec {
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
        write!(f, "GtfExec: projection=[{proj_str}]")
    }
}

impl ExecutionPlan for GtfExec {
    fn name(&self) -> &str {
        "GtfExec"
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
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        // Use indexed reading when partition assignments and index are available
        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
            && partition < assignments.len()
        {
            let regions = assignments[partition].regions.clone();
            let file_path = self.file_path.clone();
            let index_path = index_path.clone();
            let projection = self.projection.clone();
            let coord_zero_based = self.coordinate_system_zero_based;
            let attr_fields = self.attr_fields.clone();
            let residual_filters = self.residual_filters.clone();

            let fut = get_indexed_gtf_stream(
                file_path,
                index_path,
                regions,
                schema.clone(),
                batch_size,
                projection,
                coord_zero_based,
                attr_fields,
                residual_filters,
            );
            let stream = futures::stream::once(fut).try_flatten();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }

        // Fallback: full scan (local or remote)
        let file_path = self.file_path.clone();
        let attr_fields = self.attr_fields.clone();
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let object_storage_options = self.object_storage_options.clone();
        let coordinate_system_zero_based = self.coordinate_system_zero_based;

        let stream = futures::stream::once(async move {
            let stream: SendableRecordBatchStream = get_stream(
                file_path,
                attr_fields,
                schema.clone(),
                batch_size,
                projection,
                filters,
                object_storage_options,
                coordinate_system_zero_based,
            )
            .await?;
            Ok::<_, DataFusionError>(stream)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Collects GTF record fields into columnar vectors and builds RecordBatches.
///
/// Encapsulates the projection flags, column vectors, attribute builders, and
/// batch-building logic shared across `get_local_gtf`, `get_remote_gtf_stream`,
/// and `get_indexed_gtf_stream`.
struct GtfBatchCollector {
    // Projection flags
    needs_chrom: bool,
    needs_start: bool,
    needs_end: bool,
    needs_type: bool,
    needs_source: bool,
    needs_score: bool,
    needs_strand: bool,
    needs_phase: bool,

    // Column vectors
    chroms: Vec<String>,
    poss: Vec<u32>,
    pose: Vec<u32>,
    types: Vec<String>,
    sources: Vec<String>,
    scores: Vec<Option<f32>>,
    strands: Vec<String>,
    phases: Vec<Option<u32>>,

    // Attribute handling
    unnest_enable: bool,
    attribute_builders: (Vec<String>, Vec<OptionalField>),
    nested_builder: Vec<OptionalField>,

    // Config
    batch_size: usize,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    record_num: usize,
}

impl GtfBatchCollector {
    fn new(
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<usize>>,
        attr_fields: Option<&[String]>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        let needs_chrom = projection.as_ref().is_none_or(|proj| proj.contains(&0));
        let needs_start = projection.as_ref().is_none_or(|proj| proj.contains(&1));
        let needs_end = projection.as_ref().is_none_or(|proj| proj.contains(&2));
        let needs_type = projection.as_ref().is_none_or(|proj| proj.contains(&3));
        let needs_source = projection.as_ref().is_none_or(|proj| proj.contains(&4));
        let needs_score = projection.as_ref().is_none_or(|proj| proj.contains(&5));
        let needs_strand = projection.as_ref().is_none_or(|proj| proj.contains(&6));
        let needs_phase = projection.as_ref().is_none_or(|proj| proj.contains(&7));

        let cap = |needed: bool| -> usize { if needed { batch_size } else { 0 } };

        let mut attribute_builders: (Vec<String>, Vec<OptionalField>) = (Vec::new(), Vec::new());
        let unnest_enable = if let Some(attrs) = attr_fields {
            set_attribute_builders(batch_size, attrs, &mut attribute_builders);
            true
        } else {
            false
        };

        let nested_builder = if !unnest_enable {
            new_nested_builder(batch_size)?
        } else {
            Vec::new()
        };

        Ok(Self {
            needs_chrom,
            needs_start,
            needs_end,
            needs_type,
            needs_source,
            needs_score,
            needs_strand,
            needs_phase,
            chroms: Vec::with_capacity(cap(needs_chrom)),
            poss: Vec::with_capacity(cap(needs_start)),
            pose: Vec::with_capacity(cap(needs_end)),
            types: Vec::with_capacity(cap(needs_type)),
            sources: Vec::with_capacity(cap(needs_source)),
            scores: Vec::with_capacity(cap(needs_score)),
            strands: Vec::with_capacity(cap(needs_strand)),
            phases: Vec::with_capacity(cap(needs_phase)),
            unnest_enable,
            attribute_builders,
            nested_builder,
            batch_size,
            schema,
            projection,
            coordinate_system_zero_based,
            record_num: 0,
        })
    }

    /// Push a record's fields into the column vectors.
    fn push_record<T: GtfRecordTrait>(
        &mut self,
        record: &T,
    ) -> Result<(), datafusion::arrow::error::ArrowError> {
        if self.needs_chrom {
            self.chroms
                .push(record.reference_sequence_name().to_owned());
        }
        if self.needs_start {
            let start_pos = record.start();
            self.poss.push(if self.coordinate_system_zero_based {
                start_pos.saturating_sub(1)
            } else {
                start_pos
            });
        }
        if self.needs_end {
            self.pose.push(record.end());
        }
        if self.needs_type {
            self.types.push(record.ty().to_owned());
        }
        if self.needs_source {
            self.sources.push(record.source().to_owned());
        }
        if self.needs_score {
            self.scores.push(record.score());
        }
        if self.needs_strand {
            self.strands.push(record.strand().to_owned());
        }
        if self.needs_phase {
            self.phases.push(record.phase().map(|p| p as u32));
        }

        let attributes_str = record.attributes_string();
        if self.unnest_enable {
            load_attributes_unnest_from_string(
                attributes_str,
                &mut self.attribute_builders,
                self.projection.clone(),
            )?;
        } else {
            let attributes = parse_gtf_attributes_to_vec(attributes_str);
            load_attributes_from_vec(attributes, &mut self.nested_builder)?;
        }

        self.record_num += 1;
        Ok(())
    }

    /// Returns true when a full batch has been accumulated.
    fn is_full(&self) -> bool {
        self.record_num > 0 && self.record_num.is_multiple_of(self.batch_size)
    }

    /// Returns true when there are pending records that haven't been flushed.
    fn has_pending(&self) -> bool {
        !self.record_num.is_multiple_of(self.batch_size)
    }

    /// Build a RecordBatch from accumulated data and clear the column vectors.
    fn take_batch(&mut self) -> datafusion::error::Result<RecordBatch> {
        let count = if self.is_full() {
            self.batch_size
        } else {
            self.record_num % self.batch_size
        };

        let attr_arrays = builders_to_arrays(if self.unnest_enable {
            &mut self.attribute_builders.1
        } else {
            &mut self.nested_builder
        });

        let batch = build_record_batch_optimized(
            Arc::clone(&self.schema),
            &self.chroms,
            &self.poss,
            &self.pose,
            &self.types,
            &self.sources,
            &self.scores,
            &self.strands,
            &self.phases,
            Some(&attr_arrays),
            self.projection.clone(),
            count,
        )?;

        self.chroms.clear();
        self.poss.clear();
        self.pose.clear();
        self.types.clear();
        self.sources.clear();
        self.scores.clear();
        self.strands.clear();
        self.phases.clear();

        Ok(batch)
    }
}

fn set_attribute_builders(
    batch_size: usize,
    attr_fields: &[String],
    attribute_builders: &mut (Vec<String>, Vec<OptionalField>),
) {
    for attr_name in attr_fields {
        let field = OptionalField::new(&DataType::Utf8, batch_size).unwrap();
        attribute_builders.0.push(attr_name.clone());
        attribute_builders.1.push(field);
    }
}

/// Parse GTF attributes string into Attribute structs
///
/// GTF format: `key "value"; key "value";`
/// Some values may be unquoted (e.g., `level 2`).
/// Duplicate keys are supported (e.g., multiple `tag` entries).
fn parse_gtf_attributes_to_vec(attributes_str: &str) -> Vec<Attribute> {
    if attributes_str.is_empty() || attributes_str == "." {
        return Vec::new();
    }

    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes = Vec::with_capacity(estimated_pairs);

    for pair in attributes_str.split(';') {
        if let Some((key, value)) = parse_gtf_pair(pair) {
            attributes.push(Attribute {
                tag: key.to_string(),
                value: Some(value.to_string()),
            });
        }
    }

    attributes
}

/// Load GTF attributes for specific attribute columns (unnested)
fn load_attributes_unnest_from_string(
    attributes_str: &str,
    attribute_builders: &mut (Vec<String>, Vec<OptionalField>),
    projection: Option<Vec<usize>>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let mut attributes_map = HashMap::new();

    if !attributes_str.is_empty() && attributes_str != "." {
        let wanted: std::collections::HashSet<&str> =
            attribute_builders.0.iter().map(|s| s.as_str()).collect();
        for pair in attributes_str.split(';') {
            if let Some((key, value)) = parse_gtf_pair(pair)
                && wanted.contains(key)
            {
                // For duplicate keys, concatenate values with commas (consistent with GFF3 multi-value handling)
                attributes_map
                    .entry(key.to_string())
                    .and_modify(|existing: &mut String| {
                        existing.push(',');
                        existing.push_str(value);
                    })
                    .or_insert_with(|| value.to_string());
            }
        }
    }

    let projected_attribute_indices: Option<Vec<usize>> =
        projection.map(|p| p.into_iter().filter(|i| *i >= 8).map(|i| i - 8).collect());

    for i in 0..attribute_builders.1.len() {
        if let Some(indices) = &projected_attribute_indices
            && !indices.contains(&i)
        {
            attribute_builders.1[i].append_null()?;
            continue;
        }

        let name = &attribute_builders.0[i];
        let builder = &mut attribute_builders.1[i];

        if let Some(value) = attributes_map.get(name) {
            builder.append_string(value)?;
        } else {
            builder.append_null()?;
        }
    }
    Ok(())
}

fn load_attributes_from_vec(
    attributes: Vec<Attribute>,
    builder: &mut [OptionalField],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    builder[0].append_array_struct(attributes)?;
    Ok(())
}

fn new_nested_builder(batch_size: usize) -> datafusion::common::Result<Vec<OptionalField>> {
    Ok(vec![OptionalField::new(
        &DataType::List(FieldRef::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("tag", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ))),
        batch_size,
    )?])
}

#[allow(clippy::too_many_arguments)]
async fn get_local_gtf(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let reader = GtfLocalReader::new(&file_path).map_err(|e| {
        DataFusionError::Execution(format!("Failed to open GTF file '{file_path}': {e}"))
    })?;
    let sync_iter = reader.into_sync_iterator();

    let mut collector = GtfBatchCollector::new(
        schema,
        batch_size,
        projection,
        attr_fields.as_deref(),
        coordinate_system_zero_based,
    )?;

    let stream = try_stream! {
        for result in sync_iter {
            let record = result?;

            let attributes_str = record.attributes_string();
            if !crate::filter_utils::evaluate_filters_against_record(&record, &filters, attributes_str, coordinate_system_zero_based) {
                continue;
            }

            collector.push_record(&record)?;

            if collector.is_full() {
                debug!("GTF record number: {}", collector.record_num);
                yield collector.take_batch()?;
            }
        }
        if collector.has_pending() {
            yield collector.take_batch()?;
        }
    };
    Ok(stream)
}

#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_gtf(
                file_path,
                attr_fields,
                schema,
                batch_size,
                projection,
                filters,
                coordinate_system_zero_based,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_gtf_stream(
                file_path,
                attr_fields,
                schema,
                batch_size,
                projection,
                filters,
                object_storage_options,
                coordinate_system_zero_based,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported storage type for GTF: {store_type:?}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_remote_gtf_stream(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader = GtfRemoteReader::new(
        file_path.clone(),
        object_storage_options.ok_or_else(|| {
            DataFusionError::Execution(
                "Object storage options required for remote GTF files".into(),
            )
        })?,
    )
    .await
    .map_err(|e| {
        DataFusionError::Execution(format!("Failed to open remote GTF file '{file_path}': {e}"))
    })?;

    let mut collector = GtfBatchCollector::new(
        schema,
        batch_size,
        projection,
        attr_fields.as_deref(),
        coordinate_system_zero_based,
    )?;

    let stream = try_stream! {
        let mut buf = String::new();
        loop {
            buf.clear();
            let bytes_read = reader.read_line(&mut buf).await?;
            if bytes_read == 0 {
                break;
            }

            let line = buf.trim_end();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            let record = parse_gtf_line(line)?;

            let attributes_str = record.attributes_string();
            if !crate::filter_utils::evaluate_filters_against_record(&record, &filters, attributes_str, coordinate_system_zero_based) {
                continue;
            }

            collector.push_record(&record)?;

            if collector.is_full() {
                debug!("GTF remote record number: {}", collector.record_num);
                yield collector.take_batch()?;
            }
        }
        if collector.has_pending() {
            yield collector.take_batch()?;
        }
    };
    Ok(stream)
}

fn build_noodles_region(region: &GenomicRegion) -> Result<noodles_core::Region, DataFusionError> {
    let region_str = match (region.start, region.end) {
        (Some(start), Some(end)) => format!("{}:{}-{}", region.chrom, start, end),
        (Some(start), None) => format!("{}:{}", region.chrom, start),
        (None, Some(end)) => format!("{}:1-{}", region.chrom, end),
        (None, None) => region.chrom.clone(),
    };

    region_str
        .parse::<noodles_core::Region>()
        .map_err(|e| DataFusionError::Execution(format!("Invalid region '{region_str}': {e}")))
}

/// Get a streaming RecordBatch stream from an indexed GTF file for one or more regions.
///
/// Uses `thread::spawn` + `mpsc::channel(2)` for streaming I/O with backpressure.
#[allow(clippy::too_many_arguments)]
async fn get_indexed_gtf_stream(
    file_path: String,
    index_path: String,
    regions: Vec<GenomicRegion>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    attr_fields: Option<Vec<String>>,
    residual_filters: Vec<Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    use datafusion::arrow::error::ArrowError;

    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<RecordBatch, ArrowError>>(2);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let mut indexed_reader =
                IndexedGtfReader::new(&file_path, &index_path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open indexed GTF: {e}"))
                })?;

            let mut collector = GtfBatchCollector::new(
                schema.clone(),
                batch_size,
                projection,
                attr_fields.as_deref(),
                coordinate_system_zero_based,
            )?;

            for region in &regions {
                if region.unmapped_tail {
                    continue;
                }

                let region_start_1based = region.start.map(|s| s as u32);
                let region_end_1based = region.end.map(|e| e as u32);

                let noodles_region = build_noodles_region(region)?;

                let records = indexed_reader.query(&noodles_region).map_err(|e| {
                    DataFusionError::Execution(format!("GTF region query failed: {e}"))
                })?;

                for result in records {
                    let raw_record = result.map_err(|e| {
                        DataFusionError::Execution(format!("GTF indexed record read error: {e}"))
                    })?;

                    let line: &str = raw_record.as_ref();
                    let record = parse_gtf_line(line)
                        .map_err(|e| DataFusionError::Execution(format!("GTF parse error: {e}")))?;

                    // Skip records outside the sub-region bounds
                    if let Some(rs) = region_start_1based
                        && record.start < rs
                    {
                        continue;
                    }
                    if let Some(re) = region_end_1based
                        && record.start > re
                    {
                        continue;
                    }

                    // Apply residual filters
                    if !residual_filters.is_empty()
                        && !crate::filter_utils::evaluate_filters_against_record(
                            &record,
                            &residual_filters,
                            &record.attributes,
                            coordinate_system_zero_based,
                        )
                    {
                        continue;
                    }

                    collector
                        .push_record(&record)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                    if collector.is_full() {
                        let batch = collector.take_batch()?;
                        loop {
                            match tx.try_send(Ok(batch.clone())) {
                                Ok(()) => break,
                                Err(e) if e.is_disconnected() => return Ok(()),
                                Err(_) => std::thread::yield_now(),
                            }
                        }
                    }
                }
            }

            if collector.has_pending() {
                let batch = collector.take_batch()?;
                loop {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }

            debug!(
                "Indexed GTF scan: {} records for {} regions",
                collector.record_num,
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

#[allow(clippy::too_many_arguments)]
fn build_record_batch_optimized(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ty: &[String],
    source: &[String],
    score: &[Option<f32>],
    strand: &[String],
    phase: &[Option<u32>],
    attributes: Option<&Vec<Arc<dyn Array>>>,
    projection: Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    // Helper closures to build arrays on-demand, avoiding allocation for unprojected columns
    let make_chrom = || Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let make_start = || Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let make_end = || Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>;
    let make_type = || Arc::new(StringArray::from(ty.to_vec())) as Arc<dyn Array>;
    let make_source = || Arc::new(StringArray::from(source.to_vec())) as Arc<dyn Array>;
    let make_score = || {
        let mut builder = Float32Builder::new();
        for s in score {
            builder.append_option(*s);
        }
        Arc::new(builder.finish()) as Arc<dyn Array>
    };
    let make_strand = || Arc::new(StringArray::from(strand.to_vec())) as Arc<dyn Array>;
    let make_phase = || {
        let mut builder = UInt32Builder::new();
        for s in phase {
            builder.append_option(*s);
        }
        Arc::new(builder.finish()) as Arc<dyn Array>
    };

    let build_core_array = |idx: usize| -> Arc<dyn Array> {
        match idx {
            0 => make_chrom(),
            1 => make_start(),
            2 => make_end(),
            3 => make_type(),
            4 => make_source(),
            5 => make_score(),
            6 => make_strand(),
            7 => make_phase(),
            _ => unreachable!(),
        }
    };

    let arrays = match projection {
        None => {
            if schema.fields().is_empty() {
                Vec::new()
            } else {
                let mut arrays: Vec<Arc<dyn Array>> = (0..8).map(&build_core_array).collect();
                if let Some(attr_arrays) = attributes {
                    arrays.append(&mut attr_arrays.clone());
                }
                arrays
            }
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            for i in proj_ids {
                if i < 8 {
                    arrays.push(build_core_array(i));
                } else if let Some(attr_arrays) = attributes {
                    if (i - 8) < attr_arrays.len() {
                        arrays.push(attr_arrays[i - 8].clone());
                    } else {
                        arrays.push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>);
                    }
                } else {
                    arrays.push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>);
                }
            }
            arrays
        }
    };

    if arrays.is_empty() {
        RecordBatch::try_new_with_options(
            schema,
            arrays,
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(record_count)),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Error creating empty GTF batch for COUNT(*): {e:?}"
            ))
        })
    } else {
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating GTF batch: {e:?}")))
    }
}
