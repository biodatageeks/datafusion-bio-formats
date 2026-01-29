use crate::storage::BamReader;
use crate::tag_registry::get_known_tags;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, ArrayRef, NullArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::table_utils::OptionalField;

use futures_util::{StreamExt, TryStreamExt};
use log::debug;
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::cigar::Op;
use noodles_sam::alignment::record::cigar::op::Kind as OpKind;
use noodles_sam::alignment::record::data::field::value::Array as SamArray;
use noodles_sam::alignment::record::data::field::{Tag, Value};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;

#[allow(dead_code)]
pub struct BamExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
    /// Optional list of BAM alignment tags to include as columns
    pub(crate) tag_fields: Option<Vec<String>>,
}

impl Debug for BamExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for BamExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        debug!("BamExec::execute");
        debug!("Projection: {:?}", self.projection);
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.thread_num,
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.tag_fields.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
/// Type alias for tag builders: (tag_names, tag_types, builders)
type TagBuilders = (Vec<String>, Vec<DataType>, Vec<OptionalField>);

/// Initialize tag builders based on requested tag fields
fn set_tag_builders(
    batch_size: usize,
    tag_fields: Option<Vec<String>>,
    tag_builders: &mut TagBuilders,
) {
    if let Some(tags) = tag_fields {
        let known_tags = get_known_tags();
        for tag in tags {
            if let Some(tag_def) = known_tags.get(&tag) {
                if let Ok(builder) = OptionalField::new(&tag_def.arrow_type, batch_size) {
                    tag_builders.0.push(tag.clone());
                    tag_builders.1.push(tag_def.arrow_type.clone());
                    tag_builders.2.push(builder);
                }
            }
        }
    }
}

/// Extract tag values from a BAM record and populate builders
fn load_tags<R: Record>(record: &R, tag_builders: &mut TagBuilders) -> Result<(), ArrowError> {
    for i in 0..tag_builders.0.len() {
        let tag_name = &tag_builders.0[i];
        let builder = &mut tag_builders.2[i];

        // Parse tag from string (e.g., "NM" -> Tag)
        // Tags must be exactly 2 bytes
        let tag_bytes = tag_name.as_bytes();
        if tag_bytes.len() != 2 {
            builder.append_null()?;
            continue;
        }
        let tag = Tag::from([tag_bytes[0], tag_bytes[1]]);

        // Access tag using noodles API: record.data().get(tag)
        let data = record.data();
        let tag_result = data.get(&tag);

        match tag_result {
            Some(Ok(value)) => {
                match value {
                    Value::Int8(v) => builder.append_int(v as i32)?,
                    Value::UInt8(v) => builder.append_int(v as i32)?,
                    Value::Int16(v) => builder.append_int(v as i32)?,
                    Value::UInt16(v) => builder.append_int(v as i32)?,
                    Value::Int32(v) => builder.append_int(v)?,
                    Value::UInt32(v) => builder.append_int(v as i32)?,
                    Value::Float(f) => builder.append_float(f)?,
                    Value::String(s) => {
                        // BStr needs to be converted to str
                        match std::str::from_utf8(s.as_ref()) {
                            Ok(string) => builder.append_string(string)?,
                            Err(_) => builder.append_null()?,
                        }
                    }
                    Value::Character(c) => builder.append_string(&c.to_string())?,
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

/// Convert tag builders to Arrow arrays
fn builders_to_arrays(builders: &mut [OptionalField]) -> Result<Vec<ArrayRef>, ArrowError> {
    builders.iter_mut().map(|b| b.finish()).collect()
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
    let mut reader = BamReader::new(file_path.clone(), None, object_storage_options).await;

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
        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new());
        set_tag_builders(batch_size, tag_fields, &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.

        let ref_sequences = reader.read_sequences().await;
        let mut records = reader.read_records().await;
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
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
            match record.alignment_start() {
                Some(start_pos) => {
                    // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                    let pos = start_pos?.get() as u32;
                    start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    end.push(Some(end_pos?.get() as u32));
                },
                None => {
                    end.push(None);
                }
            };
            match record.mapping_quality() {
                Some(mapping_quality_value) => {
                    mapping_quality.push(Some(mapping_quality_value.get() as u32));
                },
                _ => {
                    mapping_quality.push(None);
                }
            };
           let seq_string = record.sequence().iter()
               .map(char::from)
               .collect();
            sequence.push(seq_string);
            quality_scores.push(record.quality_scores().iter()
                .map(|p| char::from(p+33))
                .collect::<String>());

            flag.push(record.flags().bits() as u32);
            cigar.push(record.cigar().iter().map(|p| p.unwrap())
                .map(cigar_op_to_string)
                .collect::<Vec<String>>().join(""));
            let chrom_name = get_chrom_by_seq_id(
                record.mate_reference_sequence_id(),
                &names,
            );
            mate_chrom.push(chrom_name);
            match record.mate_alignment_start()  {
                Some(mate_start_pos) => {
                    // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                    let pos = mate_start_pos?.get() as u32;
                    mate_start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                _ => mate_start.push(None),
            };

            // Load tag fields
            load_tags(&record, &mut tag_builders)?;

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
                    &name,
                    &chrom,
                    &start,
                    &end,
                    &flag,
                    &cigar,
                    &mapping_quality,
                    &mate_chrom,
                    &mate_start,
                    &sequence,
                    &quality_scores,
                    tag_arrays.as_ref(),
                    num_tag_fields,
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
                &name,
                &chrom,
                &start,
                &end,
                &flag,
                &cigar,
                &mapping_quality,
                &mate_chrom,
                &mate_start,
                &sequence,
                &quality_scores,
                tag_arrays.as_ref(),
                num_tag_fields,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_bam(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
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

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let file_path = file_path.clone();
    let mut reader = BamReader::new(file_path.clone(), thread_num, None).await;
    let mut record_num = 0;

    let stream = try_stream! {
        // Initialize tag builders
        let mut tag_builders: TagBuilders = (Vec::new(), Vec::new(), Vec::new());
        set_tag_builders(batch_size, tag_fields, &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let ref_sequences = reader.read_sequences().await;
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
        let mut records = reader.read_records().await;
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            let seq_string = record.sequence().iter()
                   .map(char::from)
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
            match record.alignment_start() {
                Some(start_pos) => {
                    // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                    let pos = start_pos?.get() as u32;
                    start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    end.push(Some(end_pos?.get() as u32));
                },
                None => {
                    end.push(None);
                }
            };
            sequence.push(seq_string);
            quality_scores.push(record.quality_scores().iter()
                .map(|p| char::from(p+33))
                .collect::<String>());

            match record.mapping_quality() {
                Some(mapping_quality_value) => {
                    mapping_quality.push(Some(mapping_quality_value.get() as u32));
                },
                None => {
                    mapping_quality.push(None);
                }
            };
            flag.push(record.flags().bits() as u32);
            cigar.push(record.cigar().iter().map(|p| p.unwrap())
                .map(cigar_op_to_string)
                .collect::<Vec<String>>().join(""));
             let chrom_name = get_chrom_by_seq_id(
                record.mate_reference_sequence_id(),
                &names,
            );
            mate_chrom.push(chrom_name);
            match record.mate_alignment_start()  {
                Some(mate_start_pos) => {
                    // noodles normalizes all positions to 1-based; subtract 1 for 0-based output
                    let pos = mate_start_pos?.get() as u32;
                    mate_start.push(Some(if coordinate_system_zero_based { pos - 1 } else { pos }));
                },
                None => mate_start.push(None),
            };

            // Load tag fields
            load_tags(&record, &mut tag_builders)?;

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
                   &name,
                    &chrom,
                    &start,
                    &end,
                    &flag,
                    &cigar,
                    &mapping_quality,
                    &mate_chrom,
                    &mate_start,
                    &sequence,
                    &quality_scores,
                    tag_arrays.as_ref(),
                    num_tag_fields,
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
               &name,
                &chrom,
                &start,
                &end,
                &flag,
                &cigar,
                &mapping_quality,
                &mate_chrom,
                &mate_start,
                &sequence,
                &quality_scores,
                tag_arrays.as_ref(),
                num_tag_fields,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

#[allow(clippy::too_many_arguments)]
fn build_record_batch(
    schema: SchemaRef,
    name: &[Option<String>],
    chrom: &[Option<String>],
    start: &[Option<u32>],
    end: &[Option<u32>],
    flag: &[u32],
    cigar: &[String],
    mapping_quality: &[Option<u32>],
    mate_chrom: &[Option<String>],
    mate_start: &[Option<u32>],
    sequence: &[String],
    quality_scores: &[String],
    tag_arrays: Option<&Vec<ArrayRef>>,
    _num_tag_fields: usize,
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<RecordBatch> {
    let name_array = Arc::new(StringArray::from(name.to_vec())) as Arc<dyn Array>;
    let chrom_array = Arc::new(StringArray::from(chrom.to_vec())) as Arc<dyn Array>;
    let start_array = Arc::new(UInt32Array::from(start.to_vec())) as Arc<dyn Array>;
    let end_array = Arc::new(UInt32Array::from(end.to_vec())) as Arc<dyn Array>;
    let flag_array = Arc::new(UInt32Array::from(flag.to_vec())) as Arc<dyn Array>;
    let cigar_array = Arc::new(StringArray::from(cigar.to_vec())) as Arc<dyn Array>;
    let mapping_quality_array =
        Arc::new(UInt32Array::from(mapping_quality.to_vec())) as Arc<dyn Array>;
    let mate_chrom_array = Arc::new(StringArray::from(mate_chrom.to_vec())) as Arc<dyn Array>;
    let mate_start_array = Arc::new(UInt32Array::from(mate_start.to_vec())) as Arc<dyn Array>;
    let sequence_array = Arc::new(StringArray::from(sequence.to_vec())) as Arc<dyn Array>;
    let quality_scores_array =
        Arc::new(StringArray::from(quality_scores.to_vec())) as Arc<dyn Array>;

    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                name_array,
                chrom_array,
                start_array,
                end_array,
                flag_array,
                cigar_array,
                mapping_quality_array,
                mate_chrom_array,
                mate_start_array,
                sequence_array,
                quality_scores_array,
            ];
            // Add tag arrays if present
            if let Some(tags) = tag_arrays {
                arrays.extend_from_slice(tags);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(name.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(name_array.len())) as Arc<dyn Array>);
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(name_array.clone()),
                        1 => arrays.push(chrom_array.clone()),
                        2 => arrays.push(start_array.clone()),
                        3 => arrays.push(end_array.clone()),
                        4 => arrays.push(flag_array.clone()),
                        5 => arrays.push(cigar_array.clone()),
                        6 => arrays.push(mapping_quality_array.clone()),
                        7 => arrays.push(mate_chrom_array.clone()),
                        8 => arrays.push(mate_start_array.clone()),
                        9 => arrays.push(sequence_array.clone()),
                        10 => arrays.push(quality_scores_array.clone()),
                        _ => {
                            // Tag fields start at index 11
                            let tag_idx = i - 11;
                            if let Some(tags) = tag_arrays {
                                if tag_idx < tags.len() {
                                    arrays.push(tags[tag_idx].clone());
                                } else {
                                    arrays.push(Arc::new(NullArray::new(name_array.len()))
                                        as Arc<dyn Array>);
                                }
                            } else {
                                arrays
                                    .push(Arc::new(NullArray::new(name_array.len()))
                                        as Arc<dyn Array>);
                            }
                        }
                    }
                }
            }
            arrays
        }
    };
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}

async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
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
            let stream = get_local_bam(
                file_path.clone(),
                schema.clone(),
                batch_size,
                thread_num,
                projection,
                coordinate_system_zero_based,
                tag_fields,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
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

fn cigar_op_to_string(op: Op) -> String {
    let kind = match op.kind() {
        OpKind::Match => 'M',
        OpKind::Insertion => 'I',
        OpKind::Deletion => 'D',
        OpKind::Skip => 'N',
        OpKind::SoftClip => 'S',
        OpKind::HardClip => 'H',
        OpKind::Pad => 'P',
        OpKind::SequenceMatch => '=',
        OpKind::SequenceMismatch => 'X',
    };
    format!("{}{}", op.len(), kind)
}

fn get_chrom_by_seq_id(rid: Option<io::Result<usize>>, names: &[String]) -> Option<String> {
    match rid {
        Some(rid) => {
            let idx = rid.unwrap();
            let chrom_name = names
                .get(idx)
                .expect("reference_sequence_id() should be in bounds");
            let mut chrom_name = chrom_name.to_string().to_lowercase();
            if !chrom_name.starts_with("chr") {
                chrom_name = format!("chr{}", chrom_name);
            }
            Some(chrom_name)
        }
        _ => None,
    }
}
