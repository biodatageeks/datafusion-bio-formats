use crate::storage::{BamReader, BamRecordFields, IndexedBamReader, SamReader, is_sam_file};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::alignment_utils::{
    RecordFields, build_record_batch, format_cigar_ops, format_cigar_ops_unwrap,
    get_chrom_by_seq_id_bam as get_chrom_by_seq_id,
    get_chrom_by_seq_id_cram as get_chrom_by_seq_id_sam,
};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::table_utils::OptionalField;
use datafusion_bio_format_core::tag_registry::get_known_tags;

use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};
use noodles_bam as bam;
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::data::field::value::Array as SamArray;
use noodles_sam::alignment::record::data::field::{Tag, Value};
use std::any::Any;
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
            Some(indices) => {
                let col_names: Vec<&str> = indices
                    .iter()
                    .filter_map(|&i| self.schema.fields().get(i).map(|f| f.name().as_str()))
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
            Some(indices) => indices
                .iter()
                .filter_map(|&i| self.schema.fields().get(i).map(|f| f.name().as_str()))
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

/// Extract tag values from a BAM record and populate builders
fn load_tags<R: Record>(record: &R, tag_builders: &mut TagBuilders) -> Result<(), ArrowError> {
    for i in 0..tag_builders.0.len() {
        let builder = &mut tag_builders.2[i];
        let expected_type = &tag_builders.1[i]; // Expected Arrow type from schema
        let tag = &tag_builders.3[i]; // Use pre-parsed tag

        // Access tag using noodles API: record.data().get(tag)
        let data = record.data();
        let tag_result = data.get(tag);

        match tag_result {
            Some(Ok(value)) => {
                match value {
                    Value::Int8(v) => append_int_value(builder, expected_type, v as i32)?,
                    Value::UInt8(v) => append_int_value(builder, expected_type, v as i32)?,
                    Value::Int16(v) => append_int_value(builder, expected_type, v as i32)?,
                    Value::UInt16(v) => append_int_value(builder, expected_type, v as i32)?,
                    Value::Int32(v) => append_int_value(builder, expected_type, v)?,
                    Value::UInt32(v) => append_int_value(builder, expected_type, v as i32)?,
                    Value::Float(f) => {
                        if matches!(expected_type, DataType::Utf8) {
                            builder.append_string(&f.to_string())?
                        } else {
                            builder.append_float(f)?
                        }
                    }
                    Value::String(s) => {
                        // BStr needs to be converted to str
                        match std::str::from_utf8(s.as_ref()) {
                            Ok(string) => builder.append_string(string)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    Value::Character(c) => {
                        if matches!(expected_type, DataType::Int32) {
                            builder.append_int(c as i32)?
                        } else {
                            // Convert u8 to char, not to its numeric string representation
                            let ch = char::from(c);
                            builder.append_string(&ch.to_string())?
                        }
                    }
                    Value::Hex(h) => {
                        // Convert hex bytes to hex string
                        let bytes: &[u8] = h.as_ref();
                        let hex_str = hex::encode(bytes);
                        builder.append_string(&hex_str)?
                    }
                    Value::Array(arr) => {
                        // Handle array types
                        match arr {
                            SamArray::Int8(vals) => {
                                let vec: Result<Vec<i32>, _> =
                                    vals.iter().map(|v| v.map(|x| x as i32)).collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::UInt8(vals) => {
                                let vec: Result<Vec<i32>, _> =
                                    vals.iter().map(|v| v.map(|x| x as i32)).collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::Int16(vals) => {
                                let vec: Result<Vec<i32>, _> =
                                    vals.iter().map(|v| v.map(|x| x as i32)).collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::UInt16(vals) => {
                                let vec: Result<Vec<i32>, _> =
                                    vals.iter().map(|v| v.map(|x| x as i32)).collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::Int32(vals) => {
                                let vec: Result<Vec<i32>, _> = vals.iter().collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::UInt32(vals) => {
                                let vec: Result<Vec<i32>, _> =
                                    vals.iter().map(|v| v.map(|x| x as i32)).collect();
                                match vec {
                                    Ok(v) => builder.append_array_int(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                            SamArray::Float(vals) => {
                                let vec: Result<Vec<f32>, _> = vals.iter().collect();
                                match vec {
                                    Ok(v) => builder.append_array_float(v)?,
                                    Err(_) => builder.append_null()?,
                                }
                            }
                        }
                    }
                }
            }
            _ => builder.append_null()?,
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
        // Determine which fields are needed based on projection
        let needs_name = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&0));
        let needs_chrom = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&1));
        let needs_start = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&2));
        let needs_end = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&3));
        let needs_flags = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&4));
        let needs_cigar = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&5));
        let needs_mapq = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&6));
        let needs_mate_chrom = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&7));
        let needs_mate_start = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&8));
        let needs_sequence = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&9));
        let needs_quality = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&10));
        let needs_any_tag = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 11));

        // Create vectors for accumulating record data (conditional capacity)
        let mut name: Vec<Option<String>> = if needs_name { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut chrom: Vec<Option<String>> = if needs_chrom { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut start: Vec<Option<u32>> = if needs_start { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut end: Vec<Option<u32>> = if needs_end { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mapping_quality: Vec<Option<u32>> = if needs_mapq { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut flag: Vec<u32> = if needs_flags { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut cigar: Vec<String> = if needs_cigar { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mate_chrom: Vec<Option<String>> = if needs_mate_chrom { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mate_start: Vec<Option<u32>> = if needs_mate_start { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut seq_builder = if needs_sequence { StringBuilder::with_capacity($batch_size, $batch_size * 150) } else { StringBuilder::new() };
        let mut qual_builder = if needs_quality { StringBuilder::with_capacity($batch_size, $batch_size * 150) } else { StringBuilder::new() };

        // Initialize tag builders
        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_tag_builders($batch_size, $tag_fields, $schema.clone(), &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let mut cigar_buf = String::new();
        let mut seq_buf = String::new();
        let mut qual_buf = String::new();
        let mut record_num = 0;
        let mut batch_num = 0;

        // Get reference sequences and record iterator
        let ref_sequences = $reader.read_sequences().await;
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
        let mut records = $reader.read_records().await;

        // Process records one by one
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Extract record name
            if needs_name {
                match record.name() {
                    Some(read_name) => {
                        name.push(Some(read_name.to_string()));
                    },
                    _ => {
                        name.push(None);
                    }
                };
            }

            // Extract chromosome name
            if needs_chrom {
                let chrom_name = get_chrom_by_seq_id(
                    record.reference_sequence_id(),
                    &names,
                );
                chrom.push(chrom_name);
            }

            // Extract alignment start position
            if needs_start {
                match record.alignment_start() {
                    Some(start_pos) => {
                        // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                        let pos = start_pos?.get() as u32;
                        start.push(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    None => {
                        start.push(None);
                    }
                }
            }

            // Extract alignment end position
            if needs_end {
                match record.alignment_end() {
                    Some(end_pos) => {
                        end.push(Some(end_pos?.get() as u32));
                    },
                    None => {
                        end.push(None);
                    }
                };
            }

            // Extract mapping quality
            if needs_mapq {
                match record.mapping_quality() {
                    Some(mapping_quality_value) => {
                        mapping_quality.push(Some(mapping_quality_value.get() as u32));
                    },
                    _ => {
                        mapping_quality.push(None);
                    }
                };
            }

            // Extract sequence
            if needs_sequence {
                seq_buf.clear();
                seq_buf.extend(record.sequence().iter().map(char::from));
                seq_builder.append_value(&seq_buf);
            }

            // Extract quality scores
            if needs_quality {
                qual_buf.clear();
                qual_buf.extend(record.quality_scores().iter().map(|p| char::from(p + 33)));
                qual_builder.append_value(&qual_buf);
            }

            // Extract flags and CIGAR
            if needs_flags {
                flag.push(record.flags().bits() as u32);
            }
            if needs_cigar {
                format_cigar_ops_unwrap(record.cigar().iter(), &mut cigar_buf);
                cigar.push(cigar_buf.clone());
            }

            // Extract mate information
            if needs_mate_chrom {
                let mate_chrom_name = get_chrom_by_seq_id(
                    record.mate_reference_sequence_id(),
                    &names,
                );
                mate_chrom.push(mate_chrom_name);
            }

            if needs_mate_start {
                match record.mate_alignment_start() {
                    Some(mate_start_pos) => {
                        // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                        let pos = mate_start_pos?.get() as u32;
                        mate_start.push(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    _ => mate_start.push(None),
                };
            }

            // Load tag fields
            if needs_any_tag {
                load_tags(&record, &mut tag_builders)?;
            }

            record_num += 1;

            // Once the batch size is reached, build and yield a record batch
            if record_num % $batch_size == 0 {
                log::debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&$schema.clone()),
                    RecordFields {
                        name: &name,
                        chrom: &chrom,
                        start: &start,
                        end: &end,
                        flag: &flag,
                        cigar: &cigar,
                        mapping_quality: &mapping_quality,
                        mate_chrom: &mate_chrom,
                        mate_start: &mate_start,
                        sequence: Arc::new(seq_builder.finish()),
                        quality_scores: Arc::new(qual_builder.finish()),
                    },
                    tag_arrays.as_ref(),
                    $projection.clone(),
                    $batch_size,
                )?;
                batch_num += 1;
                log::debug!("Batch number: {}", batch_num);
                yield batch;

                // Clear vectors for the next batch
                name.clear();
                chrom.clear();
                start.clear();
                end.clear();
                flag.clear();
                cigar.clear();
                mapping_quality.clear();
                mate_chrom.clear();
                mate_start.clear();
            }
        }

        // If there are remaining records that don't fill a complete batch, yield them as well
        if record_num > 0 && record_num % $batch_size != 0 {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch(
                Arc::clone(&$schema.clone()),
                RecordFields {
                    name: &name,
                    chrom: &chrom,
                    start: &start,
                    end: &end,
                    flag: &flag,
                    cigar: &cigar,
                    mapping_quality: &mapping_quality,
                    mate_chrom: &mate_chrom,
                    mate_start: &mate_start,
                    sequence: Arc::new(seq_builder.finish()),
                    quality_scores: Arc::new(qual_builder.finish()),
                },
                tag_arrays.as_ref(),
                $projection.clone(),
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

async fn get_local_bam(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader = BamReader::new(file_path.clone(), None).await;
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

/// Macro to generate the record processing logic for SAM readers.
///
/// SAM records use `RecordBuf` (same as CRAM), which differs from BAM's lazy `Record`
/// in how positions, sequences, and quality scores are accessed. Positions are returned
/// directly (no `io::Result` wrapper), sequences and quality scores are accessed via
/// `.as_ref()`, and CIGAR operations are dereferenced rather than unwrapped.
macro_rules! process_sam_records_impl {
    ($reader:expr, $schema:expr, $batch_size:expr, $projection:expr, $coord_zero_based:expr, $tag_fields:expr) => {
        try_stream! {
        // Determine which fields are needed based on projection
        let needs_name = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&0));
        let needs_chrom = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&1));
        let needs_start = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&2));
        let needs_end = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&3));
        let needs_flags = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&4));
        let needs_cigar = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&5));
        let needs_mapq = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&6));
        let needs_mate_chrom = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&7));
        let needs_mate_start = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&8));
        let needs_sequence = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&9));
        let needs_quality = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.contains(&10));
        let needs_any_tag = $projection.as_ref().is_none_or(|p: &Vec<usize>| p.iter().any(|&i| i >= 11));

        let mut name: Vec<Option<String>> = if needs_name { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut chrom: Vec<Option<String>> = if needs_chrom { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut start: Vec<Option<u32>> = if needs_start { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut end: Vec<Option<u32>> = if needs_end { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mapping_quality: Vec<Option<u32>> = if needs_mapq { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut flag: Vec<u32> = if needs_flags { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut cigar: Vec<String> = if needs_cigar { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mate_chrom: Vec<Option<String>> = if needs_mate_chrom { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut mate_start: Vec<Option<u32>> = if needs_mate_start { Vec::with_capacity($batch_size) } else { Vec::new() };
        let mut seq_builder = if needs_sequence { StringBuilder::with_capacity($batch_size, $batch_size * 150) } else { StringBuilder::new() };
        let mut qual_builder = if needs_quality { StringBuilder::with_capacity($batch_size, $batch_size * 150) } else { StringBuilder::new() };

        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_tag_builders($batch_size, $tag_fields, $schema.clone(), &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

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

            if needs_name {
                match record.name() {
                    Some(read_name) => {
                        name.push(Some(read_name.to_string()));
                    },
                    _ => {
                        name.push(None);
                    }
                };
            }

            if needs_chrom {
                let chrom_name = get_chrom_by_seq_id_sam(
                    record.reference_sequence_id(),
                    &names,
                );
                chrom.push(chrom_name);
            }

            if needs_start {
                match record.alignment_start() {
                    Some(start_pos) => {
                        let pos = usize::from(start_pos) as u32;
                        start.push(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    None => {
                        start.push(None);
                    }
                }
            }

            if needs_end {
                match record.alignment_end() {
                    Some(end_pos) => {
                        let pos = usize::from(end_pos) as u32;
                        end.push(Some(pos));
                    },
                    None => {
                        end.push(None);
                    }
                };
            }

            if needs_mapq {
                match record.mapping_quality() {
                    Some(mapping_quality_value) => {
                        mapping_quality.push(Some(u8::from(mapping_quality_value) as u32));
                    },
                    _ => {
                        mapping_quality.push(None);
                    }
                };
            }

            if needs_sequence {
                seq_buf.clear();
                seq_buf.extend(record.sequence().as_ref().iter().map(|b| char::from(*b)));
                seq_builder.append_value(&seq_buf);
            }

            if needs_quality {
                qual_buf.clear();
                qual_buf.extend(record.quality_scores().as_ref().iter().map(|s| char::from(*s + 33)));
                qual_builder.append_value(&qual_buf);
            }

            if needs_flags {
                flag.push(record.flags().bits() as u32);
            }
            if needs_cigar {
                format_cigar_ops(record.cigar().as_ref().iter().copied(), &mut cigar_buf);
                cigar.push(cigar_buf.clone());
            }

            if needs_mate_chrom {
                let mate_chrom_name = get_chrom_by_seq_id_sam(
                    record.mate_reference_sequence_id(),
                    &names,
                );
                mate_chrom.push(mate_chrom_name);
            }

            if needs_mate_start {
                match record.mate_alignment_start() {
                    Some(mate_start_pos) => {
                        let pos = usize::from(mate_start_pos) as u32;
                        mate_start.push(Some(if $coord_zero_based { pos - 1 } else { pos }));
                    },
                    _ => mate_start.push(None),
                };
            }

            if needs_any_tag {
                load_tags(&record, &mut tag_builders)?;
            }

            record_num += 1;

            if record_num % $batch_size == 0 {
                log::debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&$schema.clone()),
                    RecordFields {
                        name: &name,
                        chrom: &chrom,
                        start: &start,
                        end: &end,
                        flag: &flag,
                        cigar: &cigar,
                        mapping_quality: &mapping_quality,
                        mate_chrom: &mate_chrom,
                        mate_start: &mate_start,
                        sequence: Arc::new(seq_builder.finish()),
                        quality_scores: Arc::new(qual_builder.finish()),
                    },
                    tag_arrays.as_ref(),
                    $projection.clone(),
                    $batch_size,
                )?;
                batch_num += 1;
                log::debug!("Batch number: {}", batch_num);
                yield batch;

                name.clear();
                chrom.clear();
                start.clear();
                end.clear();
                flag.clear();
                cigar.clear();
                mapping_quality.clear();
                mate_chrom.clear();
                mate_start.clear();
            }
        }

        if record_num > 0 && record_num % $batch_size != 0 {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch(
                Arc::clone(&$schema.clone()),
                RecordFields {
                    name: &name,
                    chrom: &chrom,
                    start: &start,
                    end: &end,
                    flag: &flag,
                    cigar: &cigar,
                    mapping_quality: &mapping_quality,
                    mate_chrom: &mate_chrom,
                    mate_start: &mate_start,
                    sequence: Arc::new(seq_builder.finish()),
                    quality_scores: Arc::new(qual_builder.finish()),
                },
                tag_arrays.as_ref(),
                $projection.clone(),
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
                let stream = get_local_bam(
                    file_path.clone(),
                    schema.clone(),
                    batch_size,
                    projection,
                    coordinate_system_zero_based,
                    tag_fields,
                )
                .await?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
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

/// Read unmapped records for a specific reference sequence via direct BGZF seek.
///
/// Computes the end of mapped data by finding the max `chunk.end()` across all BAI bins
/// for this reference, then seeks there and reads forward collecting only unmapped records
/// (those with a matching reference_sequence_id but no alignment_start).
fn read_unmapped_tail(
    file_path: &str,
    index_path: &str,
    chrom: &str,
    reference_names: &[String],
) -> Result<Vec<bam::Record>, DataFusionError> {
    let index = bam::bai::fs::read(index_path).map_err(|e| {
        DataFusionError::Execution(format!("Failed to read BAI for unmapped tail: {}", e))
    })?;

    let ref_idx = reference_names
        .iter()
        .position(|n| n == chrom)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Reference '{}' not found in BAM header", chrom))
        })?;

    let ref_seq = index.reference_sequences().get(ref_idx).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Reference index {} not found in BAI index",
            ref_idx
        ))
    })?;

    // Find the end of mapped data by computing the max chunk.end() across all bins.
    // This is more reliable than metadata().end_position(), which some indexers
    // (e.g., samtools) update for ALL records including unmapped — causing a seek
    // past the unmapped section. Bins only contain mapped records, so max(chunk.end())
    // is guaranteed to be the end of mapped data.
    let seek_pos = ref_seq
        .bins()
        .values()
        .flat_map(|bin| bin.chunks())
        .map(|chunk| chunk.end())
        .max()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "No bins found in BAI for reference '{}' — cannot locate unmapped section",
                chrom
            ))
        })?;

    // Open a separate BGZF reader and seek to the unmapped section
    let mut reader = bam::io::indexed_reader::Builder::default()
        .set_index(index)
        .build_from_path(file_path)
        .map_err(|e| DataFusionError::Execution(format!("Failed to open BAM file: {}", e)))?;
    let _header = reader
        .read_header()
        .map_err(|e| DataFusionError::Execution(format!("Failed to read BAM header: {}", e)))?;

    // Seek to the end of mapped data (max chunk.end() from BAI bins)
    reader
        .get_mut()
        .seek(seek_pos)
        .map_err(|e| DataFusionError::Execution(format!("BGZF seek failed: {}", e)))?;

    let mut record = bam::Record::default();
    let mut unmapped_records = Vec::new();

    loop {
        match reader.read_record(&mut record) {
            Ok(0) => break, // EOF
            Ok(_) => {
                // Check reference_sequence_id matches
                match record.reference_sequence_id() {
                    Some(Ok(id)) if id == ref_idx => {}
                    _ => break, // Left this reference or no reference — done
                }
                // Only collect unmapped records (no alignment position)
                if record.alignment_start().is_none() {
                    unmapped_records.push(record.clone());
                }
                // Skip mapped records that may appear right after the seek point
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
        unmapped_records.len(),
        chrom
    );

    Ok(unmapped_records)
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

            // Initialize accumulators
            let mut name: Vec<Option<String>> = Vec::with_capacity(batch_size);
            let mut chrom: Vec<Option<String>> = Vec::with_capacity(batch_size);
            let mut start: Vec<Option<u32>> = Vec::with_capacity(batch_size);
            let mut end: Vec<Option<u32>> = Vec::with_capacity(batch_size);
            let mut mapping_quality: Vec<Option<u32>> = Vec::with_capacity(batch_size);
            let mut flag: Vec<u32> = Vec::with_capacity(batch_size);
            let mut cigar: Vec<String> = Vec::with_capacity(batch_size);
            let mut mate_chrom: Vec<Option<String>> = Vec::with_capacity(batch_size);
            let mut mate_start: Vec<Option<u32>> = Vec::with_capacity(batch_size);
            let mut seq_builder = StringBuilder::with_capacity(batch_size, batch_size * 150);
            let mut qual_builder = StringBuilder::with_capacity(batch_size, batch_size * 150);

            let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_tag_builders(batch_size, tag_fields, schema.clone(), &mut tag_builders);
            let num_tag_fields = tag_builders.0.len();

            let mut cigar_buf = String::new();
            let mut seq_buf = String::new();
            let mut qual_buf = String::new();
            let mut total_records = 0usize;

            for region in &regions {
                // Handle unmapped tail regions via direct BGZF seek
                if region.unmapped_tail {
                    let unmapped_records =
                        read_unmapped_tail(&file_path, &index_path, &region.chrom, &names)?;

                    for record in unmapped_records {
                        let chrom_name = Some(region.chrom.clone());
                        let start_val: Option<u32> = None;
                        let end_val: Option<u32> = None;
                        let mq = record.mapping_quality().map(|q| q.get() as u32);

                        // Apply residual filters
                        if !residual_filters.is_empty() {
                            let fields = BamRecordFields {
                                chrom: chrom_name.clone(),
                                start: start_val,
                                end: end_val,
                                mapping_quality: mq,
                                flags: record.flags().bits() as u32,
                            };
                            if !evaluate_record_filters(&fields, &residual_filters) {
                                continue;
                            }
                        }

                        match record.name() {
                            Some(read_name) => name.push(Some(read_name.to_string())),
                            _ => name.push(None),
                        };

                        chrom.push(chrom_name);
                        start.push(start_val);
                        end.push(end_val);
                        mapping_quality.push(mq);

                        seq_buf.clear();
                        seq_buf.extend(record.sequence().iter().map(char::from));
                        seq_builder.append_value(&seq_buf);

                        qual_buf.clear();
                        qual_buf.extend(record.quality_scores().iter().map(|p| char::from(p + 33)));
                        qual_builder.append_value(&qual_buf);

                        flag.push(record.flags().bits() as u32);
                        format_cigar_ops_unwrap(record.cigar().iter(), &mut cigar_buf);
                        cigar.push(cigar_buf.clone());

                        let mate_chrom_name =
                            get_chrom_by_seq_id(record.mate_reference_sequence_id(), &names);
                        mate_chrom.push(mate_chrom_name);

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
                                mate_start.push(Some(if coordinate_system_zero_based {
                                    pos - 1
                                } else {
                                    pos
                                }));
                            }
                            _ => mate_start.push(None),
                        };

                        load_tags(&record, &mut tag_builders)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                        total_records += 1;

                        if total_records % batch_size == 0 {
                            let tag_arrays =
                                if num_tag_fields > 0 {
                                    Some(builders_to_arrays(&mut tag_builders.2).map_err(|e| {
                                        DataFusionError::ArrowError(Box::new(e), None)
                                    })?)
                                } else {
                                    None
                                };
                            let batch = build_record_batch(
                                Arc::clone(&schema),
                                RecordFields {
                                    name: &name,
                                    chrom: &chrom,
                                    start: &start,
                                    end: &end,
                                    flag: &flag,
                                    cigar: &cigar,
                                    mapping_quality: &mapping_quality,
                                    mate_chrom: &mate_chrom,
                                    mate_start: &mate_start,
                                    sequence: Arc::new(seq_builder.finish()),
                                    quality_scores: Arc::new(qual_builder.finish()),
                                },
                                tag_arrays.as_ref(),
                                projection.clone(),
                                name.len(),
                            )?;

                            loop {
                                match tx.try_send(Ok(batch.clone())) {
                                    Ok(()) => break,
                                    Err(e) if e.is_disconnected() => return Ok(()),
                                    Err(_) => std::thread::yield_now(),
                                }
                            }

                            name.clear();
                            chrom.clear();
                            start.clear();
                            end.clear();
                            flag.clear();
                            cigar.clear();
                            mapping_quality.clear();
                            mate_chrom.clear();
                            mate_start.clear();
                        }
                    }
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

                    let chrom_name = get_chrom_by_seq_id(record.reference_sequence_id(), &names);

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

                    let mq = record.mapping_quality().map(|q| q.get() as u32);

                    // Apply residual filters
                    if !residual_filters.is_empty() {
                        let fields = BamRecordFields {
                            chrom: chrom_name.clone(),
                            start: start_val,
                            end: end_val,
                            mapping_quality: mq,
                            flags: record.flags().bits() as u32,
                        };
                        if !evaluate_record_filters(&fields, &residual_filters) {
                            continue;
                        }
                    }

                    match record.name() {
                        Some(read_name) => name.push(Some(read_name.to_string())),
                        _ => name.push(None),
                    };

                    chrom.push(chrom_name);
                    start.push(start_val);
                    end.push(end_val);
                    mapping_quality.push(mq);

                    seq_buf.clear();
                    seq_buf.extend(record.sequence().iter().map(char::from));
                    seq_builder.append_value(&seq_buf);

                    qual_buf.clear();
                    qual_buf.extend(record.quality_scores().iter().map(|p| char::from(p + 33)));
                    qual_builder.append_value(&qual_buf);

                    flag.push(record.flags().bits() as u32);
                    format_cigar_ops_unwrap(record.cigar().iter(), &mut cigar_buf);
                    cigar.push(cigar_buf.clone());

                    let mate_chrom_name =
                        get_chrom_by_seq_id(record.mate_reference_sequence_id(), &names);
                    mate_chrom.push(mate_chrom_name);

                    match record.mate_alignment_start() {
                        Some(mate_start_pos) => {
                            let pos = mate_start_pos
                                .map_err(|e| {
                                    DataFusionError::Execution(format!("BAM mate pos error: {}", e))
                                })?
                                .get() as u32;
                            mate_start.push(Some(if coordinate_system_zero_based {
                                pos - 1
                            } else {
                                pos
                            }));
                        }
                        _ => mate_start.push(None),
                    };

                    load_tags(&record, &mut tag_builders)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                    total_records += 1;

                    if total_records % batch_size == 0 {
                        let tag_arrays = if num_tag_fields > 0 {
                            Some(
                                builders_to_arrays(&mut tag_builders.2)
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                            )
                        } else {
                            None
                        };
                        let batch = build_record_batch(
                            Arc::clone(&schema),
                            RecordFields {
                                name: &name,
                                chrom: &chrom,
                                start: &start,
                                end: &end,
                                flag: &flag,
                                cigar: &cigar,
                                mapping_quality: &mapping_quality,
                                mate_chrom: &mate_chrom,
                                mate_start: &mate_start,
                                sequence: Arc::new(seq_builder.finish()),
                                quality_scores: Arc::new(qual_builder.finish()),
                            },
                            tag_arrays.as_ref(),
                            projection.clone(),
                            name.len(),
                        )?;

                        // Send batch with backpressure
                        loop {
                            match tx.try_send(Ok(batch.clone())) {
                                Ok(()) => break,
                                Err(e) if e.is_disconnected() => return Ok(()),
                                Err(_) => std::thread::yield_now(),
                            }
                        }

                        name.clear();
                        chrom.clear();
                        start.clear();
                        end.clear();
                        flag.clear();
                        cigar.clear();
                        mapping_quality.clear();
                        mate_chrom.clear();
                        mate_start.clear();
                    }
                }
            }

            // Remaining records
            if !name.is_empty() {
                let tag_arrays = if num_tag_fields > 0 {
                    Some(
                        builders_to_arrays(&mut tag_builders.2)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&schema),
                    RecordFields {
                        name: &name,
                        chrom: &chrom,
                        start: &start,
                        end: &end,
                        flag: &flag,
                        cigar: &cigar,
                        mapping_quality: &mapping_quality,
                        mate_chrom: &mate_chrom,
                        mate_start: &mate_start,
                        sequence: Arc::new(seq_builder.finish()),
                        quality_scores: Arc::new(qual_builder.finish()),
                    },
                    tag_arrays.as_ref(),
                    projection.clone(),
                    name.len(),
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
