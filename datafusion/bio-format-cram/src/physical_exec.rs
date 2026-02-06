use crate::storage::{CramReader, CramRecordFields, IndexedCramReader};
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
    RecordFields, build_record_batch, cigar_op_to_string,
    get_chrom_by_seq_id_cram as get_chrom_by_seq_id,
};
use datafusion_bio_format_core::calculated_tags::{calculate_md_tag, calculate_nm_tag};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::table_utils::OptionalField;
use datafusion_bio_format_core::tag_registry::get_known_tags;

use futures_util::{StreamExt, TryStreamExt};
use log::debug;
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::data::field::value::Array as SamArray;
use noodles_sam::alignment::record::data::field::{Tag, Value};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[allow(dead_code)]
pub struct CramExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) reference_path: Option<String>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
    /// Optional list of CRAM alignment tags to include as columns
    pub(crate) tag_fields: Option<Vec<String>>,
    /// Genomic regions for index-based reading (None = full scan)
    pub(crate) regions: Option<Vec<GenomicRegion>>,
    /// Path to the index file (CRAI)
    pub(crate) index_path: Option<String>,
    /// Residual filters for record-level evaluation
    pub(crate) residual_filters: Vec<datafusion::logical_expr::Expr>,
}

impl Debug for CramExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for CramExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for CramExec {
    fn name(&self) -> &str {
        "CramExec"
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
        debug!("CramExec::execute partition={}", partition);
        debug!("Projection: {:?}", self.projection);
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        // Use indexed reading when regions and index are available
        if let (Some(regions), Some(index_path)) = (&self.regions, &self.index_path) {
            if partition < regions.len() {
                let region = regions[partition].clone();
                let file_path = self.file_path.clone();
                let index_path = index_path.clone();
                let reference_path = self.reference_path.clone();
                let projection = self.projection.clone();
                let coord_zero_based = self.coordinate_system_zero_based;
                let tag_fields = self.tag_fields.clone();
                let residual_filters = self.residual_filters.clone();

                let fut = get_indexed_stream(
                    file_path,
                    index_path,
                    reference_path,
                    region,
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
            self.reference_path.clone(),
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.tag_fields.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

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
                let tag_bytes = tag.as_bytes();
                if tag_bytes.len() == 2 {
                    let parsed_tag = Tag::from([tag_bytes[0], tag_bytes[1]]);
                    tag_builders.0.push(tag.clone());
                    tag_builders.1.push(arrow_type.clone());
                    tag_builders.2.push(builder);
                    tag_builders.3.push(parsed_tag);
                    debug!("Successfully created builder for tag {}", tag);
                } else {
                    debug!("Invalid tag name length for {}", tag);
                }
            } else {
                debug!("Failed to create builder for tag {}: {:?}", tag, arrow_type);
            }
        }
    }
}

fn load_tags<R: Record>(
    record: &R,
    tag_builders: &mut TagBuilders,
    reference_seq: Option<&[u8]>,
) -> Result<(), ArrowError> {
    for i in 0..tag_builders.0.len() {
        let tag = &tag_builders.3[i];
        let tag_name = &tag_builders.0[i];
        let builder = &mut tag_builders.2[i];
        let data = record.data();
        let tag_result = data.get(tag);

        // Check if this is a calculated tag (MD or NM) that can be computed
        let is_md_tag = tag_name == "MD";
        let is_nm_tag = tag_name == "NM";

        if tag_result.is_none() {
            debug!("Tag {} not found in record data", tag_name);

            // Try to calculate MD or NM tags if not present in data
            if is_md_tag && reference_seq.is_some() {
                debug!("Attempting to calculate MD tag from alignment");
                if let Some(md_value) = calculate_md_tag(record, reference_seq.unwrap()) {
                    debug!("Successfully calculated MD tag: {}", md_value);
                    builder.append_string(&md_value)?;
                    continue;
                } else {
                    debug!("Failed to calculate MD tag");
                }
            } else if is_nm_tag {
                debug!("Attempting to calculate NM tag from alignment");
                if let Some(nm_value) = calculate_nm_tag(record, reference_seq) {
                    debug!("Successfully calculated NM tag: {}", nm_value);
                    builder.append_int(nm_value)?;
                    continue;
                } else {
                    debug!("Failed to calculate NM tag");
                }
            }
        }

        match tag_result {
            Some(Ok(value)) => match value {
                Value::Int8(v) => builder.append_int(v as i32)?,
                Value::UInt8(v) => builder.append_int(v as i32)?,
                Value::Int16(v) => builder.append_int(v as i32)?,
                Value::UInt16(v) => builder.append_int(v as i32)?,
                Value::Int32(v) => builder.append_int(v)?,
                Value::UInt32(v) => builder.append_int(v as i32)?,
                Value::Float(f) => builder.append_float(f)?,
                Value::String(s) => match std::str::from_utf8(s.as_ref()) {
                    Ok(string) => builder.append_string(string)?,
                    Err(_) => builder.append_null()?,
                },
                Value::Character(c) => {
                    // Convert u8 to char, not to its numeric string representation
                    let ch = char::from(c);
                    builder.append_string(&ch.to_string())?
                }
                Value::Hex(h) => {
                    let hex_str = hex::encode::<&[u8]>(h.as_ref());
                    builder.append_string(&hex_str)?
                }
                Value::Array(arr) => match arr {
                    SamArray::Int8(vals) => {
                        let vec: Result<Vec<i32>, _> = vals
                            .as_ref()
                            .iter()
                            .map(|v| v.map(|val| val as i32))
                            .collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::UInt8(vals) => {
                        let vec: Result<Vec<i32>, _> = vals
                            .as_ref()
                            .iter()
                            .map(|v| v.map(|val| val as i32))
                            .collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::Int16(vals) => {
                        let vec: Result<Vec<i32>, _> = vals
                            .as_ref()
                            .iter()
                            .map(|v| v.map(|val| val as i32))
                            .collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::UInt16(vals) => {
                        let vec: Result<Vec<i32>, _> = vals
                            .as_ref()
                            .iter()
                            .map(|v| v.map(|val| val as i32))
                            .collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::Int32(vals) => {
                        let vec: Result<Vec<i32>, _> = vals.as_ref().iter().collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::UInt32(vals) => {
                        let vec: Result<Vec<i32>, _> = vals
                            .as_ref()
                            .iter()
                            .map(|v| v.map(|val| val as i32))
                            .collect();
                        match vec {
                            Ok(v) => builder.append_array_int(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    SamArray::Float(vals) => {
                        let vec: Result<Vec<f32>, _> = vals.as_ref().iter().collect();
                        match vec {
                            Ok(v) => builder.append_array_float(v)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                },
            },
            _ => builder.append_null()?,
        }
    }
    Ok(())
}

fn builders_to_arrays(builders: &mut [OptionalField]) -> Result<Vec<ArrayRef>, ArrowError> {
    builders.iter_mut().map(|b| b.finish()).collect()
}

#[allow(clippy::too_many_arguments)]
async fn get_remote_cram_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader =
        CramReader::new(file_path.clone(), reference_path, object_storage_options).await;

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut name: Vec<Option<String>> = Vec::with_capacity(batch_size);
        let mut chrom: Vec<Option<String>> = Vec::with_capacity(batch_size);
        let mut start: Vec<Option<u32>> = Vec::with_capacity(batch_size);
        let mut end : Vec<Option<u32>> = Vec::with_capacity(batch_size);

        let mut mapping_quality: Vec<Option<u32>> = Vec::with_capacity(batch_size);
        let mut flag : Vec<u32> = Vec::with_capacity(batch_size);
        let mut cigar : Vec<String> = Vec::with_capacity(batch_size);
        let mut mate_chrom : Vec<Option<String>> = Vec::with_capacity(batch_size);
        let mut mate_start : Vec<Option<u32>> = Vec::with_capacity(batch_size);
        let mut quality_scores: Vec<String> = Vec::with_capacity(batch_size);
        let mut sequence : Vec<String> = Vec::with_capacity(batch_size);

        // Initialize tag builders
        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_tag_builders(batch_size, tag_fields, schema.clone(), &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.
        let ref_sequences = reader.get_sequences();
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();

        // Load all reference sequences for MD/NM tag calculation
        let reference_seqs = reader.load_all_references();
        if reference_seqs.is_some() {
            debug!("Loaded {} reference sequences for tag calculation", reference_seqs.as_ref().unwrap().len());
        } else {
            debug!("No external reference available - MD/NM tags will be NULL if not stored");
        }

        let mut records = reader.read_records().await;

        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            match record.name() {
                Some(read_name) => {
                    name.push(Some(read_name.to_string()));
                },
                _ => {
                    name.push(None);
                }
            };
            let chrom_name = get_chrom_by_seq_id(
                record.reference_sequence_id(),
                &names,
            );
            chrom.push(chrom_name);
            // noodles returns 1-based positions; convert to 0-based if needed
            match record.alignment_start() {
                Some(start_pos) => {
                    let pos = usize::from(start_pos) as u32;
                    start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    let pos = usize::from(end_pos) as u32;
                    // End position: noodles returns 1-based inclusive end
                    // For 0-based half-open: keep as-is (1-based inclusive = 0-based exclusive)
                    // For 1-based closed: use as-is
                    end.push(Some(pos));
                },
                None => {
                    end.push(None);
                }
            };
            match record.mapping_quality() {
                Some(mapping_quality_value) => {
                    mapping_quality.push(Some(u8::from(mapping_quality_value) as u32));
                },
                _ => {
                    mapping_quality.push(None);
                }
            };
           let seq_string = record.sequence().as_ref().iter()
               .map(|base| char::from(*base))
               .collect();
            sequence.push(seq_string);
            quality_scores.push(record.quality_scores().as_ref().iter()
                .map(|score| char::from(*score+33))
                .collect::<String>());

            flag.push(record.flags().bits() as u32);
            cigar.push(record.cigar().as_ref().iter()
                .map(|op| cigar_op_to_string(*op))
                .collect::<Vec<String>>().join(""));
            let chrom_name = get_chrom_by_seq_id(
                record.mate_reference_sequence_id(),
                &names,
            );
            mate_chrom.push(chrom_name);
            // mate_alignment_start: same conversion as alignment_start
            match record.mate_alignment_start()  {
                Some(start_val) => {
                    let pos = usize::from(start_val) as u32;
                    mate_start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                _ => mate_start.push(None),
            };

            // Load tag values if present
            // Extract reference sequence for MD/NM tag calculation
            let reference_region = if let Some(ref refs) = reference_seqs {
                // Get reference sequence ID and alignment positions
                if let (Some(seq_id), Some(start_pos), Some(end_pos)) = (
                    record.reference_sequence_id(),
                    record.alignment_start(),
                    record.alignment_end(),
                ) {
                    // Fetch the reference sequence for this chromosome
                    refs.get(seq_id).map(|ref_seq| {
                        let start = (usize::from(start_pos) - 1).min(ref_seq.len()); // Convert to 0-based
                        let end = usize::from(end_pos).min(ref_seq.len());
                        &ref_seq[start..end]
                    })
                } else {
                    None
                }
            } else {
                None
            };

            load_tags(&record, &mut tag_builders, reference_region)?;

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
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
                        sequence: &sequence,
                        quality_scores: &quality_scores,
                    },
                    tag_arrays.as_ref(),
                    projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                chrom.clear();
                start.clear();
                end.clear();
                flag.clear();
                cigar.clear();
                mapping_quality.clear();
                mate_chrom.clear();
                mate_start.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
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
                    sequence: &sequence,
                    quality_scores: &quality_scores,
                },
                tag_arrays.as_ref(),
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

#[allow(clippy::too_many_arguments)]
async fn get_local_cram(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut name: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut chrom: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut start: Vec<Option<u32>> = Vec::with_capacity(batch_size);
    let mut end: Vec<Option<u32>> = Vec::with_capacity(batch_size);

    let mut mapping_quality: Vec<Option<u32>> = Vec::with_capacity(batch_size);
    let mut flag: Vec<u32> = Vec::with_capacity(batch_size);
    let mut cigar: Vec<String> = Vec::with_capacity(batch_size);
    let mut mate_chrom: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut mate_start: Vec<Option<u32>> = Vec::with_capacity(batch_size);
    let mut quality_scores: Vec<String> = Vec::with_capacity(batch_size);
    let mut sequence: Vec<String> = Vec::with_capacity(batch_size);

    // Initialize tag builders
    let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    set_tag_builders(
        batch_size,
        tag_fields.clone(),
        schema.clone(),
        &mut tag_builders,
    );
    let num_tag_fields = tag_builders.0.len();

    let mut batch_num = 0;
    let file_path = file_path.clone();
    let mut reader = CramReader::new(file_path.clone(), reference_path, None).await;
    let mut record_num = 0;

    let stream = try_stream! {
        let ref_sequences = reader.get_sequences();
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();

        // Load all reference sequences for MD/NM tag calculation
        let reference_seqs = reader.load_all_references();
        if reference_seqs.is_some() {
            debug!("Loaded {} reference sequences for tag calculation", reference_seqs.as_ref().unwrap().len());
        } else {
            debug!("No external reference available - MD/NM tags will be NULL if not stored");
        }

        let mut records = reader.read_records().await;

        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            let seq_string = record.sequence().as_ref().iter()
                   .map(|base| char::from(*base))
                   .collect();
            let chrom_name = get_chrom_by_seq_id(
                record.reference_sequence_id(),
                &names,
            );
            chrom.push(chrom_name);
            match record.name() {
                Some(read_name) => {
                    name.push(Some(read_name.to_string()));
                },
                _ => {
                    name.push(None);
                }
            };
            // noodles returns 1-based positions; convert to 0-based if needed
            match record.alignment_start() {
                Some(start_pos) => {
                    let pos = usize::from(start_pos) as u32;
                    start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    let pos = usize::from(end_pos) as u32;
                    // End position: noodles returns 1-based inclusive end
                    // For 0-based half-open: keep as-is (1-based inclusive = 0-based exclusive)
                    // For 1-based closed: use as-is
                    end.push(Some(pos));
                },
                None => {
                    end.push(None);
                }
            };
            sequence.push(seq_string);
            quality_scores.push(record.quality_scores().as_ref().iter()
                .map(|score| char::from(*score+33))
                .collect::<String>());

            match record.mapping_quality() {
                Some(mapping_quality_value) => {
                    mapping_quality.push(Some(u8::from(mapping_quality_value) as u32));
                },
                None => {
                    mapping_quality.push(None);
                }
            };
            flag.push(record.flags().bits() as u32);
            cigar.push(record.cigar().as_ref().iter()
                .map(|op| cigar_op_to_string(*op))
                .collect::<Vec<String>>().join(""));
             let chrom_name = get_chrom_by_seq_id(
                record.mate_reference_sequence_id(),
                &names,
            );
            mate_chrom.push(chrom_name);
            // mate_alignment_start: same conversion as alignment_start
            match record.mate_alignment_start()  {
                Some(start_val) => {
                    let pos = usize::from(start_val) as u32;
                    mate_start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => mate_start.push(None),
            };

            // Load tag values if present
            // Extract reference sequence for MD/NM tag calculation
            let reference_region = if let Some(ref refs) = reference_seqs {
                // Get reference sequence ID and alignment positions
                if let (Some(seq_id), Some(start_pos), Some(end_pos)) = (
                    record.reference_sequence_id(),
                    record.alignment_start(),
                    record.alignment_end(),
                ) {
                    // Fetch the reference sequence for this chromosome
                    refs.get(seq_id).map(|ref_seq| {
                        let start = (usize::from(start_pos) - 1).min(ref_seq.len()); // Convert to 0-based
                        let end = usize::from(end_pos).min(ref_seq.len());
                        &ref_seq[start..end]
                    })
                } else {
                    None
                }
            } else {
                None
            };

            load_tags(&record, &mut tag_builders, reference_region)?;

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let tag_arrays = if num_tag_fields > 0 {
                    Some(builders_to_arrays(&mut tag_builders.2)?)
                } else {
                    None
                };
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
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
                        sequence: &sequence,
                        quality_scores: &quality_scores,
                    },
                    tag_arrays.as_ref(),
                    projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                chrom.clear();
                start.clear();
                end.clear();
                flag.clear();
                cigar.clear();
                mapping_quality.clear();
                mate_chrom.clear();
                mate_start.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() {
            let tag_arrays = if num_tag_fields > 0 {
                Some(builders_to_arrays(&mut tag_builders.2)?)
            } else {
                None
            };
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
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
                    sequence: &sequence,
                    quality_scores: &quality_scores,
                },
                tag_arrays.as_ref(),
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_cram(
                file_path.clone(),
                schema.clone(),
                batch_size,
                reference_path,
                projection,
                coordinate_system_zero_based,
                tag_fields,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_cram_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                reference_path,
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

/// Get a stream of RecordBatches from an indexed CRAM file for a specific region.
///
/// Uses `spawn_blocking` because noodles' CRAM `IndexedReader` uses synchronous I/O
/// and the query iterator is not `Send`. All I/O is done synchronously inside the
/// blocking task, which builds all batches and then streams them out.
#[allow(clippy::too_many_arguments)]
async fn get_indexed_stream(
    file_path: String,
    index_path: String,
    reference_path: Option<String>,
    region: GenomicRegion,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    tag_fields: Option<Vec<String>>,
    residual_filters: Vec<datafusion::logical_expr::Expr>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();

    // Build all batches synchronously in a blocking task
    let batches =
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, DataFusionError> {
            let mut indexed_reader =
                IndexedCramReader::new(&file_path, &index_path, reference_path.as_deref())
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to open indexed CRAM: {}", e))
                    })?;

            let names = indexed_reader.reference_names();
            let noodles_region = build_noodles_region(&region)?;

            let mut all_batches = Vec::new();

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
            let mut quality_scores: Vec<String> = Vec::with_capacity(batch_size);
            let mut sequence: Vec<String> = Vec::with_capacity(batch_size);

            let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            set_tag_builders(batch_size, tag_fields, schema.clone(), &mut tag_builders);
            let num_tag_fields = tag_builders.0.len();

            let mut record_num = 0;

            let records = indexed_reader.query(&noodles_region).map_err(|e| {
                DataFusionError::Execution(format!("CRAM region query failed: {}", e))
            })?;

            for result in records {
                let record = result.map_err(|e| {
                    DataFusionError::Execution(format!("CRAM record read error: {}", e))
                })?;

                let chrom_name = get_chrom_by_seq_id(record.reference_sequence_id(), &names);

                // CRAM RecordBuf: positions are returned directly (no io::Result wrapper)
                let start_val = match record.alignment_start() {
                    Some(start_pos) => {
                        let pos = usize::from(start_pos) as u32;
                        Some(if coordinate_system_zero_based {
                            pos - 1
                        } else {
                            pos
                        })
                    }
                    None => None,
                };

                let end_val = record
                    .alignment_end()
                    .map(|end_pos| usize::from(end_pos) as u32);

                let mq = record.mapping_quality().map(|q| u8::from(q) as u32);

                // Apply residual filters
                if !residual_filters.is_empty() {
                    let fields = CramRecordFields {
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

                let seq_string: String = record
                    .sequence()
                    .as_ref()
                    .iter()
                    .map(|base| char::from(*base))
                    .collect();
                sequence.push(seq_string);

                quality_scores.push(
                    record
                        .quality_scores()
                        .as_ref()
                        .iter()
                        .map(|score| char::from(*score + 33))
                        .collect::<String>(),
                );

                flag.push(record.flags().bits() as u32);
                cigar.push(
                    record
                        .cigar()
                        .as_ref()
                        .iter()
                        .map(|op| cigar_op_to_string(*op))
                        .collect::<Vec<String>>()
                        .join(""),
                );

                let mate_chrom_name =
                    get_chrom_by_seq_id(record.mate_reference_sequence_id(), &names);
                mate_chrom.push(mate_chrom_name);

                match record.mate_alignment_start() {
                    Some(mate_start_pos) => {
                        let pos = usize::from(mate_start_pos) as u32;
                        mate_start.push(Some(if coordinate_system_zero_based {
                            pos - 1
                        } else {
                            pos
                        }));
                    }
                    _ => mate_start.push(None),
                };

                // Load tag values (no reference-based tag calculation in indexed path for now)
                load_tags(&record, &mut tag_builders, None)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                record_num += 1;

                if record_num % batch_size == 0 {
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
                            sequence: &sequence,
                            quality_scores: &quality_scores,
                        },
                        tag_arrays.as_ref(),
                        projection.clone(),
                    )?;
                    all_batches.push(batch);

                    name.clear();
                    chrom.clear();
                    start.clear();
                    end.clear();
                    flag.clear();
                    cigar.clear();
                    mapping_quality.clear();
                    mate_chrom.clear();
                    mate_start.clear();
                    sequence.clear();
                    quality_scores.clear();
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
                        sequence: &sequence,
                        quality_scores: &quality_scores,
                    },
                    tag_arrays.as_ref(),
                    projection.clone(),
                )?;
                all_batches.push(batch);
            }

            debug!(
                "Indexed CRAM scan: {} records for region {:?}",
                record_num, region
            );
            Ok(all_batches)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Indexed CRAM task join error: {}", e))
        })??;

    // Stream out the pre-built batches
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}
