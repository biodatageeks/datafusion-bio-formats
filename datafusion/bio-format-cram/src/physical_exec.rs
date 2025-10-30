use crate::storage::CramReader;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, NullArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};

use futures_util::{StreamExt, TryStreamExt};
use log::debug;
use noodles_sam::alignment::record::cigar::Op;
use noodles_sam::alignment::record::cigar::op::Kind as OpKind;
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
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn get_remote_cram_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
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
            match record.alignment_start() {
                Some(start_pos) => {
                    start.push(Some(usize::from(start_pos) as u32));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    end.push(Some(usize::from(end_pos) as u32));
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
                .map(|score| char::from(u8::from(*score)+33))
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
            match record.mate_alignment_start()  {
                Some(start) => mate_start.push(Some(usize::from(start) as u32)),
                _ => mate_start.push(None),
            };

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
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
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_cram(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
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
            match record.alignment_start() {
                Some(start_pos) => {
                    start.push(Some(usize::from(start_pos) as u32));
                },
                None => {
                    start.push(None);
                }
            }
            match record.alignment_end() {
                Some(end_pos) => {
                    end.push(Some(usize::from(end_pos) as u32));
                },
                None => {
                    end.push(None);
                }
            };
            sequence.push(seq_string);
            quality_scores.push(record.quality_scores().as_ref().iter()
                .map(|score| char::from(u8::from(*score)+33))
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
            match record.mate_alignment_start()  {
                Some(start) => mate_start.push(Some(usize::from(start) as u32)),
                None => mate_start.push(None),
            };

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
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
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

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
            let arrays: Vec<Arc<dyn Array>> = vec![
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
                        _ => arrays
                            .push(Arc::new(NullArray::new(name_array.len())) as Arc<dyn Array>),
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
    reference_path: Option<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
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

fn get_chrom_by_seq_id(rid: Option<usize>, names: &Vec<String>) -> Option<String> {
    match rid {
        Some(rid) => {
            let chrom_name = names
                .get(rid)
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
