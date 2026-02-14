use crate::storage::{
    BamReader, BamRecordFields, IndexedBamReader, SamReader, is_sam_file, open_local_bam_sync,
};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::alignment_utils::{
    CoreBatchBuilders, ProjectionFlags, build_record_batch_from_builders, chrom_name_by_idx,
    format_cigar_ops, format_cigar_ops_unwrap, get_chrom_idx_bam as get_chrom_idx,
    get_chrom_idx_cram as get_chrom_idx_sam,
};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::table_utils::OptionalField;
use datafusion_bio_format_core::tag_registry::get_known_tags;

use futures::SinkExt;
use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};
use noodles_bam as bam;
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::data::field::value::Array as SamArray;
use noodles_sam::alignment::record::data::field::{Tag, Value};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[allow(dead_code)]
pub struct BamExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
    /// Optional list of BAM alignment tags to include as columns
    pub(crate) tag_fields: Option<Vec<String>>,
    /// Partition assignments for index-based reading (None = full scan)
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    /// Path to the index file (BAI/CSI)
    pub(crate) index_path: Option<String>,
    /// Residual filters for record-level evaluation
    pub(crate) residual_filters: Vec<datafusion::logical_expr::Expr>,
}

impl Debug for BamExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BamExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BamExec {
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
        write!(f, "BamExec: projection=[{}]", proj_str)
    }
}

impl ExecutionPlan for BamExec {
    fn name(&self) -> &str {
        "BamExec"
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

        // Use indexed reading when partition assignments and index are available
        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
        {
            if partition < assignments.len() {
                let regions = assignments[partition].regions.clone();
                let file_path = self.file_path.clone();
                let index_path = index_path.clone();
                let projection = self.projection.clone();
                let coord_zero_based = self.coordinate_system_zero_based;
                let tag_fields = self.tag_fields.clone();
                let residual_filters = self.residual_filters.clone();

                let fut = get_indexed_stream(
                    file_path,
                    index_path,
                    regions,
                    schema.clone(),
                    batch_size,
                    projection,
                    coord_zero_based,
                    tag_fields,
                    residual_filters,
                );
                let stream = futures::stream::once(fut).try_flatten();
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
            }
        }

        // Fallback: full scan (original path)
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.tag_fields.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
/// Type alias for tag builders: (tag_names, tag_types, builders, parsed_tags)
type TagBuilders = (Vec<String>, Vec<DataType>, Vec<OptionalField>, Vec<Tag>);

/// Initialize tag builders based on schema fields
fn set_tag_builders(
    batch_size: usize,
    tag_fields: Option<Vec<String>>,
    schema: SchemaRef,
    tag_builders: &mut TagBuilders,
) {
    if let Some(tags) = tag_fields {
        for tag in tags {
            // Find the field in the schema for this tag
            let field = schema.fields().iter().find(|f| f.name() == &tag);

            let arrow_type = if let Some(field) = field {
                field.data_type().clone()
            } else {
                // Fallback to registry if not in schema
                let known_tags = get_known_tags();
                if let Some(tag_def) = known_tags.get(&tag) {
                    tag_def.arrow_type.clone()
                } else {
                    DataType::Utf8
                }
            };

            debug!("Creating builder for tag {}: {:?}", tag, arrow_type);

            if let Ok(builder) = OptionalField::new(&arrow_type, batch_size) {
                // Pre-parse tag to avoid parsing on every record
                let tag_bytes = tag.as_bytes();
                if tag_bytes.len() == 2 {
                    let parsed_tag = Tag::from([tag_bytes[0], tag_bytes[1]]);
                    tag_builders.0.push(tag.clone());
                    tag_builders.1.push(arrow_type);
                    tag_builders.2.push(builder);
                    tag_builders.3.push(parsed_tag);
                }
            } else {
                debug!("Failed to create builder for tag {}: {:?}", tag, arrow_type);
            }
        }
    }
}

/// Dispatch a single tag value to the appropriate builder.
fn append_tag_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    value: Value<'_>,
) -> Result<(), ArrowError> {
    match value {
        Value::Int8(v) => append_int_value(builder, expected_type, v as i32),
        Value::UInt8(v) => append_int_value(builder, expected_type, v as i32),
        Value::Int16(v) => append_int_value(builder, expected_type, v as i32),
        Value::UInt16(v) => append_int_value(builder, expected_type, v as i32),
        Value::Int32(v) => append_int_value(builder, expected_type, v),
        Value::UInt32(v) => append_int_value(builder, expected_type, v as i32),
        Value::Float(f) => {
            if matches!(expected_type, DataType::Utf8) {
                builder.append_string(&f.to_string())
            } else {
                builder.append_float(f)
            }
        }
        Value::String(s) => match std::str::from_utf8(s.as_ref()) {
            Ok(string) => builder.append_string(string),
            Err(_) => builder.append_null(),
        },
        Value::Character(c) => {
            if matches!(expected_type, DataType::Int32) {
                builder.append_int(c as i32)
            } else {
                let ch = char::from(c);
                builder.append_string(&ch.to_string())
            }
        }
        Value::Hex(h) => {
            let bytes: &[u8] = h.as_ref();
            let hex_str = hex::encode(bytes);
            builder.append_string(&hex_str)
        }
        Value::Array(arr) => match arr {
            SamArray::Int8(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().map(|v| v.map(|x| x as i32)).collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::UInt8(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().map(|v| v.map(|x| x as i32)).collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::Int16(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().map(|v| v.map(|x| x as i32)).collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::UInt16(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().map(|v| v.map(|x| x as i32)).collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::Int32(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::UInt32(vals) => {
                let vec: Result<Vec<i32>, _> = vals.iter().map(|v| v.map(|x| x as i32)).collect();
                match vec {
                    Ok(v) => builder.append_array_int(v),
                    Err(_) => builder.append_null(),
                }
            }
            SamArray::Float(vals) => {
                let vec: Result<Vec<f32>, _> = vals.iter().collect();
                match vec {
                    Ok(v) => builder.append_array_float(v),
                    Err(_) => builder.append_null(),
                }
            }
        },
    }
}

/// Extract tag values from a BAM record using single-pass iteration.
///
/// Instead of calling `data.get(tag)` per requested tag (O(N*M) per record),
/// iterates all tags once and dispatches via HashMap lookup (O(M+N) per record).
fn load_tags<R: Record>(
    record: &R,
    tag_builders: &mut TagBuilders,
    tag_to_index: &HashMap<Tag, usize>,
    tag_populated: &mut [bool],
) -> Result<(), ArrowError> {
    let num_tags = tag_builders.0.len();
    if num_tags == 0 {
        return Ok(());
    }

    // Reset populated tracking
    tag_populated.fill(false);

    // Single pass over all tags in the record
    let data = record.data();
    for result in data.iter() {
        match result {
            Ok((tag, value)) => {
                if let Some(&idx) = tag_to_index.get(&tag) {
                    tag_populated[idx] = true;
                    let expected_type = &tag_builders.1[idx];
                    let builder = &mut tag_builders.2[idx];
                    append_tag_value(builder, expected_type, value)?;
                }
            }
            Err(_) => continue, // skip malformed tags
        }
    }

    // Backfill nulls for tags not found in this record
    for (idx, &populated) in tag_populated.iter().enumerate().take(num_tags) {
        if !populated {
            tag_builders.2[idx].append_null()?;
        }
    }

    Ok(())
}

/// Append an integer value to the builder, converting to string if the builder expects Utf8.
///
/// Some BAM files encode character tags (SAM type 'A') as integer types ('c', 'C', etc.)
/// in the binary format. When the schema expects Utf8 (from the tag registry defining the
/// tag as type 'A'), but noodles decodes the value as Int8/UInt8, we convert the byte to
/// its ASCII character representation.
fn append_int_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    value: i32,
) -> Result<(), ArrowError> {
    if matches!(expected_type, DataType::Utf8) {
        // Integer stored as character code - convert to ASCII char
        if let Some(ch) = char::from_u32(value as u32) {
            builder.append_string(&ch.to_string())
        } else {
            builder.append_string(&value.to_string())
        }
    } else {
        builder.append_int(value)
    }
}

/// Convert tag builders to Arrow arrays
fn builders_to_arrays(builders: &mut [OptionalField]) -> Result<Vec<ArrayRef>, ArrowError> {
    builders.iter_mut().map(|b| b.finish()).collect()
}

/// Macro to generate the record processing logic for BAM readers
///
/// This macro contains the shared logic for processing BAM records from any reader source.
/// It accumulates records into batches and yields them as Arrow RecordBatches.
///
/// # Arguments
/// * `$reader` - The BamReader instance (must be mutable)
/// * `$schema` - Arc<Schema> for the output
/// * `$batch_size` - Number of records per batch
/// * `$projection` - Optional projection vector
/// * `$coord_zero_based` - Whether to use 0-based coordinates
/// * `$tag_fields` - Optional list of tag field names
macro_rules! process_bam_records_impl {
    ($reader:expr, $schema:expr, $batch_size:expr, $projection:expr, $coord_zero_based:expr, $tag_fields:expr) => {
        try_stream! {
        let flags = ProjectionFlags::new(&$projection);
        let mut builders = CoreBatchBuilders::new(&flags, $batch_size);

        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_tag_builders($batch_size, $tag_fields, $schema.clone(), &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let tag_to_index: HashMap<Tag, usize> = tag_builders.3.iter().enumerate().map(|(i, t)| (*t, i)).collect();
        let mut tag_populated = vec![false; num_tag_fields];

        let mut cigar_buf = String::new();
        let mut seq_buf = String::new();
        let mut qual_buf = String::new();
        let mut record_num = 0;
        let mut batch_num = 0;

        let ref_sequences = $reader.read_sequences().await;
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
        let mut records = $reader.read_records().await;

        while let Some(result) = records.next().await {
            let record = result?;

            if flags.name {
                match record.name() {
                    Some(read_name) => builders.append_name(Some(&read_name.to_string())),
                    None => builders.append_name(Some("*")),
                };
            }

            if flags.chrom {
                builders.append_chrom(chrom_name_by_idx(get_chrom_idx(
                    record.reference_sequence_id()), &names,
                ));
            }

            if flags.start {
                match record.alignment_start() {
                    Some(start_pos) => {
                        let pos = start_pos?.get() as u32;
                        builders.append_start(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    None => builders.append_start(None),
                }
            }

            if flags.end {
                match record.alignment_end() {
                    Some(end_pos) => builders.append_end(Some(end_pos?.get() as u32)),
                    None => builders.append_end(None),
                };
            }

            if flags.mapq {
                match record.mapping_quality() {
                    Some(mq) => builders.append_mapq(mq.get() as u32),
                    None => builders.append_mapq(255u32),
                };
            }

            if flags.tlen {
                builders.append_tlen(record.template_length());
            }

            if flags.sequence {
                seq_buf.clear();
                seq_buf.extend(record.sequence().iter().map(char::from));
                builders.append_sequence(&seq_buf);
            }

            if flags.quality {
                qual_buf.clear();
                qual_buf.extend(record.quality_scores().iter().map(|p| char::from(p + 33)));
                builders.append_quality(&qual_buf);
            }

            if flags.flags {
                builders.append_flag(record.flags().bits() as u32);
            }
            if flags.cigar {
                format_cigar_ops_unwrap(record.cigar().iter(), &mut cigar_buf);
                builders.append_cigar(&cigar_buf);
            }

            if flags.mate_chrom {
                builders.append_mate_chrom(chrom_name_by_idx(get_chrom_idx(
                    record.mate_reference_sequence_id()), &names,
                ));
            }

            if flags.mate_start {
                match record.mate_alignment_start() {
                    Some(mate_start_pos) => {
                        let pos = mate_start_pos?.get() as u32;
                        builders.append_mate_start(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    _ => builders.append_mate_start(None),
                };
            }

            if flags.any_tag {
                load_tags(&record, &mut tag_builders, &tag_to_index, &mut tag_populated)?;
            }

            record_num += 1;

            if record_num % $batch_size == 0 {
                log::debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch_from_builders(
                    Arc::clone(&$schema),
                    builders.finish(),
                    tag_arrays.as_ref(),
                    &$projection,
                    $batch_size,
                )?;
                batch_num += 1;
                log::debug!("Batch number: {}", batch_num);
                yield batch;
            }
        }

        if record_num > 0 && record_num % $batch_size != 0 {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch_from_builders(
                Arc::clone(&$schema),
                builders.finish(),
                tag_arrays.as_ref(),
                &$projection,
                record_num % $batch_size,
            )?;
            yield batch;
        }
        }
    };
}

async fn get_remote_bam_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader = BamReader::new(file_path.clone(), object_storage_options).await;
    let stream = process_bam_records_impl!(
        reader,
        schema,
        batch_size,
        projection,
        coordinate_system_zero_based,
        tag_fields
    );
    Ok(stream)
}

/// Reads a local BAM file using a synchronous thread with buffer reuse.
///
/// Uses `read_record(&mut record)` to reuse a single record buffer across reads,
/// avoiding per-record clone allocations. Sends batches via a bounded channel for
/// backpressure.
#[allow(clippy::too_many_arguments)]
async fn get_local_bam_sync(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<RecordBatch, ArrowError>>(2);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let (mut reader, header) = open_local_bam_sync(&file_path)
                .map_err(|e| DataFusionError::Execution(format!("Failed to open BAM: {}", e)))?;

            let ref_sequences = header.reference_sequences();
            let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();

            let flags = ProjectionFlags::new(&projection);
            let mut builders = CoreBatchBuilders::new(&flags, batch_size);

            let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_tag_builders(batch_size, tag_fields, schema.clone(), &mut tag_builders);
            let num_tag_fields = tag_builders.0.len();

            let tag_to_index: HashMap<Tag, usize> = tag_builders
                .3
                .iter()
                .enumerate()
                .map(|(i, t)| (*t, i))
                .collect();
            let mut tag_populated = vec![false; num_tag_fields];

            let mut cigar_buf = String::new();
            let mut seq_buf = String::new();
            let mut qual_buf = String::new();
            let mut record = bam::Record::default();
            let mut total_records = 0usize;
            let mut batch_row_count = 0usize;

            loop {
                match reader.read_record(&mut record) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if flags.name {
                            match record.name() {
                                Some(read_name) => {
                                    builders.append_name(Some(&read_name.to_string()))
                                }
                                None => builders.append_name(Some("*")),
                            };
                        }

                        if flags.chrom {
                            builders.append_chrom(chrom_name_by_idx(
                                get_chrom_idx(record.reference_sequence_id()),
                                &names,
                            ));
                        }

                        if flags.start {
                            match record.alignment_start() {
                                Some(start_pos) => {
                                    let pos = start_pos
                                        .map_err(|e| {
                                            DataFusionError::Execution(format!(
                                                "BAM position error: {}",
                                                e
                                            ))
                                        })?
                                        .get() as u32;
                                    builders.append_start(Some(if coordinate_system_zero_based {
                                        pos - 1
                                    } else {
                                        pos
                                    }));
                                }
                                None => builders.append_start(None),
                            }
                        }

                        if flags.end {
                            match record.alignment_end() {
                                Some(end_pos) => {
                                    builders.append_end(Some(
                                        end_pos
                                            .map_err(|e| {
                                                DataFusionError::Execution(format!(
                                                    "BAM end pos error: {}",
                                                    e
                                                ))
                                            })?
                                            .get() as u32,
                                    ));
                                }
                                None => builders.append_end(None),
                            };
                        }

                        if flags.mapq {
                            match record.mapping_quality() {
                                Some(mq) => builders.append_mapq(mq.get() as u32),
                                None => builders.append_mapq(255u32),
                            };
                        }

                        if flags.tlen {
                            builders.append_tlen(record.template_length());
                        }

                        if flags.sequence {
                            seq_buf.clear();
                            seq_buf.extend(record.sequence().iter().map(char::from));
                            builders.append_sequence(&seq_buf);
                        }

                        if flags.quality {
                            qual_buf.clear();
                            qual_buf
                                .extend(record.quality_scores().iter().map(|p| char::from(p + 33)));
                            builders.append_quality(&qual_buf);
                        }

                        if flags.flags {
                            builders.append_flag(record.flags().bits() as u32);
                        }
                        if flags.cigar {
                            format_cigar_ops_unwrap(record.cigar().iter(), &mut cigar_buf);
                            builders.append_cigar(&cigar_buf);
                        }

                        if flags.mate_chrom {
                            builders.append_mate_chrom(chrom_name_by_idx(
                                get_chrom_idx(record.mate_reference_sequence_id()),
                                &names,
                            ));
                        }

                        if flags.mate_start {
                            match record.mate_alignment_start() {
                                Some(mate_start_pos) => {
                                    let pos = mate_start_pos
                                        .map_err(|e| {
                                            DataFusionError::Execution(format!(
                                                "BAM mate pos error: {}",
                                                e
                                            ))
                                        })?
                                        .get() as u32;
                                    builders.append_mate_start(Some(
                                        if coordinate_system_zero_based {
                                            pos - 1
                                        } else {
                                            pos
                                        },
                                    ));
                                }
                                _ => builders.append_mate_start(None),
                            };
                        }

                        if flags.any_tag {
                            load_tags(
                                &record,
                                &mut tag_builders,
                                &tag_to_index,
                                &mut tag_populated,
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }

                        total_records += 1;
                        batch_row_count += 1;

                        if batch_row_count == batch_size {
                            let tag_arrays =
                                if num_tag_fields > 0 {
                                    Some(builders_to_arrays(&mut tag_builders.2).map_err(|e| {
                                        DataFusionError::ArrowError(Box::new(e), None)
                                    })?)
                                } else {
                                    None
                                };
                            let batch = build_record_batch_from_builders(
                                Arc::clone(&schema),
                                builders.finish(),
                                tag_arrays.as_ref(),
                                &projection,
                                batch_row_count,
                            )?;

                            match futures::executor::block_on(tx.send(Ok(batch))) {
                                Ok(()) => {}
                                Err(_) => return Ok(()),
                            }

                            batch_row_count = 0;
                        }
                    }
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!("BAM read error: {}", e)));
                    }
                }
            }

            // Remaining records
            if batch_row_count > 0 {
                let tag_arrays = if num_tag_fields > 0 {
                    Some(
                        builders_to_arrays(&mut tag_builders.2)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                let batch = build_record_batch_from_builders(
                    Arc::clone(&schema),
                    builders.finish(),
                    tag_arrays.as_ref(),
                    &projection,
                    batch_row_count,
                )?;
                let _ = futures::executor::block_on(tx.send(Ok(batch)));
            }

            debug!("Local BAM sync scan: {} records", total_records);
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(ArrowError::ExternalError(Box::new(e))));
        }
    });

    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

/// Macro to generate the record processing logic for SAM readers.
///
/// SAM records use `RecordBuf` (same as CRAM), which differs from BAM's lazy `Record`
/// in how positions, sequences, and quality scores are accessed. Positions are returned
/// directly (no `io::Result` wrapper), sequences and quality scores are accessed via
/// `.as_ref()`, and CIGAR operations are dereferenced rather than unwrapped.
macro_rules! process_sam_records_impl {
    ($reader:expr, $schema:expr, $batch_size:expr, $projection:expr, $coord_zero_based:expr, $tag_fields:expr) => {
        try_stream! {
        let flags = ProjectionFlags::new(&$projection);
        let mut builders = CoreBatchBuilders::new(&flags, $batch_size);

        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_tag_builders($batch_size, $tag_fields, $schema.clone(), &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let tag_to_index: HashMap<Tag, usize> = tag_builders.3.iter().enumerate().map(|(i, t)| (*t, i)).collect();
        let mut tag_populated = vec![false; num_tag_fields];

        let mut cigar_buf = String::new();
        let mut seq_buf = String::new();
        let mut qual_buf = String::new();
        let mut record_num = 0;
        let mut batch_num = 0;

        let ref_sequences = $reader.read_sequences();
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
        let mut records = $reader.read_records();

        while let Some(result) = records.next().await {
            let record = result?;

            if flags.name {
                match record.name() {
                    Some(read_name) => builders.append_name(Some(&read_name.to_string())),
                    None => builders.append_name(Some("*")),
                };
            }

            if flags.chrom {
                builders.append_chrom(chrom_name_by_idx(get_chrom_idx_sam(
                    record.reference_sequence_id()), &names,
                ));
            }

            if flags.start {
                match record.alignment_start() {
                    Some(start_pos) => {
                        let pos = usize::from(start_pos) as u32;
                        builders.append_start(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    None => builders.append_start(None),
                }
            }

            if flags.end {
                match record.alignment_end() {
                    Some(end_pos) => builders.append_end(Some(usize::from(end_pos) as u32)),
                    None => builders.append_end(None),
                };
            }

            if flags.mapq {
                match record.mapping_quality() {
                    Some(mq) => builders.append_mapq(u8::from(mq) as u32),
                    None => builders.append_mapq(255u32),
                };
            }

            if flags.tlen {
                builders.append_tlen(record.template_length());
            }

            if flags.sequence {
                seq_buf.clear();
                seq_buf.extend(record.sequence().as_ref().iter().map(|b| char::from(*b)));
                builders.append_sequence(&seq_buf);
            }

            if flags.quality {
                qual_buf.clear();
                qual_buf.extend(record.quality_scores().as_ref().iter().map(|s| char::from(*s + 33)));
                builders.append_quality(&qual_buf);
            }

            if flags.flags {
                builders.append_flag(record.flags().bits() as u32);
            }
            if flags.cigar {
                format_cigar_ops(record.cigar().as_ref().iter().copied(), &mut cigar_buf);
                builders.append_cigar(&cigar_buf);
            }

            if flags.mate_chrom {
                builders.append_mate_chrom(chrom_name_by_idx(get_chrom_idx_sam(
                    record.mate_reference_sequence_id()), &names,
                ));
            }

            if flags.mate_start {
                match record.mate_alignment_start() {
                    Some(mate_start_pos) => {
                        let pos = usize::from(mate_start_pos) as u32;
                        builders.append_mate_start(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    _ => builders.append_mate_start(None),
                };
            }

            if flags.any_tag {
                load_tags(&record, &mut tag_builders, &tag_to_index, &mut tag_populated)?;
            }

            record_num += 1;

            if record_num % $batch_size == 0 {
                log::debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch_from_builders(
                    Arc::clone(&$schema),
                    builders.finish(),
                    tag_arrays.as_ref(),
                    &$projection,
                    $batch_size,
                )?;
                batch_num += 1;
                log::debug!("Batch number: {}", batch_num);
                yield batch;
            }
        }

        if record_num > 0 && record_num % $batch_size != 0 {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch_from_builders(
                Arc::clone(&$schema),
                builders.finish(),
                tag_arrays.as_ref(),
                &$projection,
                record_num % $batch_size,
            )?;
            yield batch;
        }
        }
    };
}

async fn get_local_sam(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader = SamReader::new(file_path.clone());
    let stream = process_sam_records_impl!(
        reader,
        schema,
        batch_size,
        projection,
        coordinate_system_zero_based,
        tag_fields
    );
    Ok(stream)
}

async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            if is_sam_file(&file_path) {
                let stream = get_local_sam(
                    file_path.clone(),
                    schema.clone(),
                    batch_size,
                    projection,
                    coordinate_system_zero_based,
                    tag_fields,
                )
                .await?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
            } else {
                get_local_bam_sync(
                    file_path.clone(),
                    schema_ref,
                    batch_size,
                    projection,
                    coordinate_system_zero_based,
                    tag_fields,
                )
                .await
            }
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            if is_sam_file(&file_path) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Remote SAM file reading is not supported ({}). Use BAM format for remote storage.",
                    file_path
                )));
            }
            let stream = get_remote_bam_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
                coordinate_system_zero_based,
                tag_fields,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
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

/// Get a streaming RecordBatch stream from an indexed BAM file for one or more regions.
///
/// Uses `thread::spawn` + `mpsc::channel(2)` for streaming I/O with backpressure.
/// Each partition processes its assigned regions sequentially, keeping only ~3 batches
/// in memory at a time (constant memory regardless of data volume).
#[allow(clippy::too_many_arguments)]
async fn get_indexed_stream(
    file_path: String,
    index_path: String,
    regions: Vec<GenomicRegion>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
    residual_filters: Vec<datafusion::logical_expr::Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<RecordBatch, ArrowError>>(2);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let mut indexed_reader =
                IndexedBamReader::new(&file_path, &index_path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open indexed BAM: {}", e))
                })?;

            let names = indexed_reader.reference_names();

            let flags = ProjectionFlags::new(&projection);
            let mut builders = CoreBatchBuilders::new(&flags, batch_size);

            // chrom/start/end/mapq/flags are always decoded for residual filter evaluation
            // and sub-region dedup, but only accumulated when projected
            let has_residual_filters = !residual_filters.is_empty();

            let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_tag_builders(batch_size, tag_fields, schema.clone(), &mut tag_builders);
            let num_tag_fields = tag_builders.0.len();

            let tag_to_index: HashMap<Tag, usize> = tag_builders
                .3
                .iter()
                .enumerate()
                .map(|(i, t)| (*t, i))
                .collect();
            let mut tag_populated = vec![false; num_tag_fields];

            let mut cigar_buf = String::new();
            let mut seq_buf = String::new();
            let mut qual_buf = String::new();
            let mut total_records = 0usize;
            let mut batch_row_count = 0usize;

            /// Helper macro to flush the current batch via the channel.
            macro_rules! flush_batch {
                ($batch_row_count:expr, $disconnect_action:expr) => {{
                    let tag_arrays = if num_tag_fields > 0 {
                        Some(
                            builders_to_arrays(&mut tag_builders.2)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                        )
                    } else {
                        None
                    };
                    let batch = build_record_batch_from_builders(
                        Arc::clone(&schema),
                        builders.finish(),
                        tag_arrays.as_ref(),
                        &projection,
                        $batch_row_count,
                    )?;

                    match futures::executor::block_on(tx.send(Ok(batch))) {
                        Ok(()) => {}
                        Err(_) => $disconnect_action,
                    }
                }};
            }

            /// Helper macro to accumulate fields from a record into builders.
            macro_rules! accumulate_record_fields {
                ($record:expr, $chrom_name:expr, $start_val:expr, $end_val:expr, $mq:expr) => {{
                    if flags.name {
                        match $record.name() {
                            Some(read_name) => builders.append_name(Some(&read_name.to_string())),
                            _ => builders.append_name(Some("*")),
                        };
                    }
                    if flags.chrom {
                        builders.append_chrom($chrom_name);
                    }
                    if flags.start {
                        builders.append_start($start_val);
                    }
                    if flags.end {
                        builders.append_end($end_val);
                    }
                    if flags.mapq {
                        builders.append_mapq($mq);
                    }
                    if flags.tlen {
                        builders.append_tlen($record.template_length());
                    }
                    if flags.sequence {
                        seq_buf.clear();
                        seq_buf.extend($record.sequence().iter().map(char::from));
                        builders.append_sequence(&seq_buf);
                    }
                    if flags.quality {
                        qual_buf.clear();
                        qual_buf
                            .extend($record.quality_scores().iter().map(|p| char::from(p + 33)));
                        builders.append_quality(&qual_buf);
                    }
                    if flags.flags {
                        builders.append_flag($record.flags().bits() as u32);
                    }
                    if flags.cigar {
                        format_cigar_ops_unwrap($record.cigar().iter(), &mut cigar_buf);
                        builders.append_cigar(&cigar_buf);
                    }
                    if flags.mate_chrom {
                        builders.append_mate_chrom(chrom_name_by_idx(
                            get_chrom_idx($record.mate_reference_sequence_id()),
                            &names,
                        ));
                    }
                    if flags.mate_start {
                        match $record.mate_alignment_start() {
                            Some(mate_start_pos) => {
                                let pos = mate_start_pos
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "BAM mate pos error: {}",
                                            e
                                        ))
                                    })?
                                    .get() as u32;
                                builders.append_mate_start(Some(if coordinate_system_zero_based {
                                    pos - 1
                                } else {
                                    pos
                                }));
                            }
                            _ => builders.append_mate_start(None),
                        };
                    }
                    if flags.any_tag {
                        load_tags(
                            &$record,
                            &mut tag_builders,
                            &tag_to_index,
                            &mut tag_populated,
                        )
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    }
                }};
            }

            for region in &regions {
                // Handle unmapped tail regions via direct BGZF seek —
                // stream records one-by-one instead of collecting into Vec.
                if region.unmapped_tail {
                    let unmapped_index = bam::bai::fs::read(&index_path).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to read BAI for unmapped tail: {}",
                            e
                        ))
                    })?;

                    let ref_idx =
                        names
                            .iter()
                            .position(|n| n == &region.chrom)
                            .ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Reference '{}' not found in BAM header",
                                    region.chrom
                                ))
                            })?;

                    let ref_seq = unmapped_index
                        .reference_sequences()
                        .get(ref_idx)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Reference index {} not found in BAI index",
                                ref_idx
                            ))
                        })?;

                    let seek_pos = ref_seq
                        .bins()
                        .values()
                        .flat_map(|bin| bin.chunks())
                        .map(|chunk| chunk.end())
                        .max()
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "No bins found in BAI for reference '{}' — cannot locate unmapped section",
                                region.chrom
                            ))
                        })?;

                    let mut unmapped_reader = bam::io::indexed_reader::Builder::default()
                        .set_index(unmapped_index)
                        .build_from_path(&file_path)
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Failed to open BAM file: {}", e))
                        })?;
                    let _unmapped_header = unmapped_reader.read_header().map_err(|e| {
                        DataFusionError::Execution(format!("Failed to read BAM header: {}", e))
                    })?;
                    unmapped_reader.get_mut().seek(seek_pos).map_err(|e| {
                        DataFusionError::Execution(format!("BGZF seek failed: {}", e))
                    })?;

                    let mut unmapped_buf = bam::Record::default();
                    let mut unmapped_count: usize = 0;

                    loop {
                        match unmapped_reader.read_record(&mut unmapped_buf) {
                            Ok(0) => break, // EOF
                            Ok(_) => {
                                match unmapped_buf.reference_sequence_id() {
                                    Some(Ok(id)) if id == ref_idx => {}
                                    _ => break,
                                }
                                if unmapped_buf.alignment_start().is_some() {
                                    continue; // Skip mapped records near seek point
                                }

                                let chrom_name: Option<&str> = Some(region.chrom.as_str());
                                let start_val: Option<u32> = None;
                                let end_val: Option<u32> = None;
                                let mq = unmapped_buf
                                    .mapping_quality()
                                    .map(|q| q.get() as u32)
                                    .unwrap_or(255u32);

                                if has_residual_filters {
                                    let fields = BamRecordFields {
                                        chrom: chrom_name.map(|s| s.to_string()),
                                        start: start_val,
                                        end: end_val,
                                        mapping_quality: Some(mq),
                                        flags: unmapped_buf.flags().bits() as u32,
                                    };
                                    if !evaluate_record_filters(&fields, &residual_filters) {
                                        continue;
                                    }
                                }

                                accumulate_record_fields!(
                                    unmapped_buf,
                                    chrom_name,
                                    start_val,
                                    end_val,
                                    mq
                                );

                                total_records += 1;
                                batch_row_count += 1;
                                unmapped_count += 1;

                                if batch_row_count == batch_size {
                                    flush_batch!(batch_row_count, return Ok(()));
                                    batch_row_count = 0;
                                }
                            }
                            Err(e) => {
                                return Err(DataFusionError::Execution(format!(
                                    "Error reading unmapped BAM records: {}",
                                    e
                                )));
                            }
                        }
                    }

                    debug!(
                        "Read {} unmapped records for {} via direct seek",
                        unmapped_count, region.chrom
                    );
                    continue; // Done with this unmapped_tail region
                }

                // Sub-region bounds for deduplication (1-based, from partition balancer)
                let region_start_1based = region.start.map(|s| s as u32);
                let region_end_1based = region.end.map(|e| e as u32);

                let noodles_region = build_noodles_region(region)?;
                let records = indexed_reader.query(&noodles_region).map_err(|e| {
                    DataFusionError::Execution(format!("BAM region query failed: {}", e))
                })?;

                for result in records {
                    let record = result.map_err(|e| {
                        DataFusionError::Execution(format!("BAM record read error: {}", e))
                    })?;

                    // Always decode chrom/start for sub-region dedup and residual filters
                    let chrom_name =
                        chrom_name_by_idx(get_chrom_idx(record.reference_sequence_id()), &names);

                    let start_val = match record.alignment_start() {
                        Some(start_pos) => {
                            let pos = start_pos
                                .map_err(|e| {
                                    DataFusionError::Execution(format!("BAM position error: {}", e))
                                })?
                                .get() as u32;
                            Some(if coordinate_system_zero_based {
                                pos - 1
                            } else {
                                pos
                            })
                        }
                        None => None,
                    };

                    // Skip records outside the sub-region bounds (BAI bins may return
                    // overlapping records at sub-region boundaries)
                    if let Some(pos_1based) = start_val.map(|s| {
                        if coordinate_system_zero_based {
                            s + 1
                        } else {
                            s
                        }
                    }) {
                        if let Some(rs) = region_start_1based {
                            if pos_1based < rs {
                                continue;
                            }
                        }
                        if let Some(re) = region_end_1based {
                            if pos_1based > re {
                                continue;
                            }
                        }
                    }

                    let end_val = match record.alignment_end() {
                        Some(end_pos) => Some(
                            end_pos
                                .map_err(|e| {
                                    DataFusionError::Execution(format!("BAM end pos error: {}", e))
                                })?
                                .get() as u32,
                        ),
                        None => None,
                    };

                    let mq = record
                        .mapping_quality()
                        .map(|q| q.get() as u32)
                        .unwrap_or(255u32);

                    // Apply residual filters
                    if has_residual_filters {
                        let fields = BamRecordFields {
                            chrom: chrom_name.map(|s| s.to_string()),
                            start: start_val,
                            end: end_val,
                            mapping_quality: Some(mq),
                            flags: record.flags().bits() as u32,
                        };
                        if !evaluate_record_filters(&fields, &residual_filters) {
                            continue;
                        }
                    }

                    accumulate_record_fields!(record, chrom_name, start_val, end_val, mq);

                    total_records += 1;
                    batch_row_count += 1;

                    if batch_row_count == batch_size {
                        flush_batch!(batch_row_count, return Ok(()));
                        batch_row_count = 0;
                    }
                }
            }

            // Remaining records
            if batch_row_count > 0 {
                flush_batch!(batch_row_count, {});
            }

            debug!(
                "Indexed BAM scan: {} records for {} regions",
                total_records,
                regions.len()
            );
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(ArrowError::ExternalError(Box::new(e))));
        }
    });

    // Stream batches from the channel
    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}
