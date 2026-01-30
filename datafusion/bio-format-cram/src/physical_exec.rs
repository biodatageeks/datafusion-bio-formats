use crate::storage::CramReader;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::alignment_utils::{
    RecordFields, build_record_batch, cigar_op_to_string,
    get_chrom_by_seq_id_cram as get_chrom_by_seq_id,
};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        debug!("CramExec::execute");
        debug!("Projection: {:?}", self.projection);
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
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
                    let tag_bytes = tag.as_bytes();
                    if tag_bytes.len() == 2 {
                        let parsed_tag = Tag::from([tag_bytes[0], tag_bytes[1]]);
                        tag_builders.0.push(tag.clone());
                        tag_builders.1.push(tag_def.arrow_type.clone());
                        tag_builders.2.push(builder);
                        tag_builders.3.push(parsed_tag);
                    }
                }
            }
        }
    }
}

fn load_tags<R: Record>(record: &R, tag_builders: &mut TagBuilders) -> Result<(), ArrowError> {
    for i in 0..tag_builders.0.len() {
        let tag = &tag_builders.3[i];
        let builder = &mut tag_builders.2[i];
        let data = record.data();
        let tag_result = data.get(tag);

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
                Value::Character(c) => builder.append_string(&c.to_string())?,
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
        set_tag_builders(batch_size, tag_fields, &mut tag_builders);
        let num_tag_fields = tag_builders.0.len();

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.
        let ref_sequences = reader.get_sequences();
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
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
                // Reset tag builders
                if num_tag_fields > 0 {
                    set_tag_builders(batch_size, Some(tag_builders.0.clone()), &mut tag_builders);
                }
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
    set_tag_builders(batch_size, tag_fields, &mut tag_builders);
    let num_tag_fields = tag_builders.0.len();

    let mut batch_num = 0;
    let file_path = file_path.clone();
    let mut reader = CramReader::new(file_path.clone(), reference_path, None).await;
    let mut record_num = 0;

    let stream = try_stream! {
        let ref_sequences = reader.get_sequences();
        let names: Vec<_> = ref_sequences.keys().map(|k| k.to_string()).collect();
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
                // Reset tag builders
                if num_tag_fields > 0 {
                    set_tag_builders(batch_size, Some(tag_builders.0.clone()), &mut tag_builders);
                }
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
