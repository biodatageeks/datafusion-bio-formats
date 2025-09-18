use crate::needletail_storage::{NeedletailLocalReader, NeedletailRemoteReader};
use crate::parser::FastqParser;
use crate::storage::{FastqLocalReader, FastqRemoteReader};
use async_stream::try_stream;
use datafusion::arrow::array::{Array, NullArray, RecordBatch, StringArray, StringBuilder};
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
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[allow(dead_code)]
pub struct FastqExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    pub(crate) parser: FastqParser,
}

impl Debug for FastqExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for FastqExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for FastqExec {
    fn name(&self) -> &str {
        "FastqExec"
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
        debug!("FastqExec::execute");
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
            self.parser,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn get_remote_fastq_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    parser: FastqParser,
) -> datafusion::error::Result<
    std::pin::Pin<Box<dyn futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send>>,
> {
    log::info!(
        "Starting remote FASTQ stream processing with {} parser",
        parser
    );
    match parser {
        FastqParser::Noodles => {
            let stream = get_remote_fastq_stream_noodles(
                file_path,
                schema,
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(stream))
        }
        FastqParser::Needletail => {
            let stream = get_remote_fastq_stream_needletail(
                file_path,
                schema,
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(stream))
        }
    }
}

async fn get_remote_fastq_stream_noodles(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader =
        FastqRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    // Determine which fields we need to parse based on projection
    let needs_name = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_description = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_sequence = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_quality_scores = projection.as_ref().map_or(true, |proj| proj.contains(&3));

    let stream = try_stream! {
        // Create vectors for accumulating record data only for needed fields.
        let mut name: Vec<String> = if needs_name { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut description: Vec<Option<String>> = if needs_description { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut sequence: Vec<String> = if needs_sequence { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut quality_scores: Vec<String> = if needs_quality_scores { Vec::with_capacity(batch_size) } else { Vec::new() };

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.
        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_name {
                name.push(record.name().to_string());
            }
            if needs_description {
                description.push(if record.description().is_empty() {
                    None
                } else {
                    Some(record.description().to_string())
                });
            }
            if needs_sequence {
                sequence.push(std::str::from_utf8(record.sequence()).unwrap().to_string());
            }
            if needs_quality_scores {
                quality_scores.push(std::str::from_utf8(record.quality_scores()).unwrap().to_string());
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    &quality_scores,
                    projection.clone(),
                    needs_name,
                    needs_description,
                    needs_sequence,
                    needs_quality_scores,
                    batch_size,
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() || !description.is_empty() || !sequence.is_empty() || !quality_scores.is_empty() || record_num > 0 {
            let actual_size = if needs_name { name.len() } else if needs_description { description.len() } else if needs_sequence { sequence.len() } else if needs_quality_scores { quality_scores.len() } else { record_num % batch_size };
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                &quality_scores,
                projection.clone(),
                needs_name,
                needs_description,
                needs_sequence,
                needs_quality_scores,
                actual_size,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_remote_fastq_stream_needletail(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader =
        NeedletailRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    // Determine which fields we need to parse based on projection
    let needs_name = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_description = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_sequence = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_quality_scores = projection.as_ref().map_or(true, |proj| proj.contains(&3));

    let stream = try_stream! {
        // Create vectors for accumulating record data only for needed fields.
        let mut name: Vec<String> = if needs_name { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut description: Vec<Option<String>> = if needs_description { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut sequence: Vec<String> = if needs_sequence { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut quality_scores: Vec<String> = if needs_quality_scores { Vec::with_capacity(batch_size) } else { Vec::new() };

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.
        let mut records = reader.read_records();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_name {
                name.push(record.name);
            }
            if needs_description {
                description.push(record.description);
            }
            if needs_sequence {
                sequence.push(record.sequence);
            }
            if needs_quality_scores {
                quality_scores.push(record.quality_scores);
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    &quality_scores,
                    projection.clone(),
                    needs_name,
                    needs_description,
                    needs_sequence,
                    needs_quality_scores,
                    batch_size,
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() || !description.is_empty() || !sequence.is_empty() || !quality_scores.is_empty() || record_num > 0 {
            let actual_size = if needs_name { name.len() } else if needs_description { description.len() } else if needs_sequence { sequence.len() } else if needs_quality_scores { quality_scores.len() } else { record_num % batch_size };
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                &quality_scores,
                projection.clone(),
                needs_name,
                needs_description,
                needs_sequence,
                needs_quality_scores,
                actual_size,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_fastq(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    parser: FastqParser,
) -> datafusion::error::Result<
    std::pin::Pin<Box<dyn futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send>>,
> {
    log::info!(
        "Starting local FASTQ stream processing with {} parser",
        parser
    );
    match parser {
        FastqParser::Noodles => {
            let stream = get_local_fastq_noodles(
                file_path,
                schema,
                batch_size,
                thread_num,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(stream))
        }
        FastqParser::Needletail => {
            let stream = get_local_fastq_needletail(
                file_path,
                schema,
                batch_size,
                thread_num,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(stream))
        }
    }
}

async fn get_local_fastq_noodles(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    // Determine which fields we need to parse based on projection
    let needs_name = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_description = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_sequence = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_quality_scores = projection.as_ref().map_or(true, |proj| proj.contains(&3));

    let mut name: Vec<String> = if needs_name {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut description: Vec<Option<String>> = if needs_description {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut sequence: Vec<String> = if needs_sequence {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut quality_scores: Vec<String> = if needs_quality_scores {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };

    let mut batch_num = 0;
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = FastqLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await?;
    let mut record_num = 0;

    let stream = try_stream! {

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_name {
                name.push(record.name().to_string());
            }
            if needs_description {
                description.push(if record.description().is_empty() {
                    None
                } else {
                    Some(record.description().to_string())
                });
            }
            if needs_sequence {
                sequence.push(std::str::from_utf8(record.sequence()).unwrap().to_string());
            }
            if needs_quality_scores {
                quality_scores.push(std::str::from_utf8(record.quality_scores()).unwrap().to_string());
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    &quality_scores,
                    projection.clone(),
                    needs_name,
                    needs_description,
                    needs_sequence,
                    needs_quality_scores,
                    batch_size,
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() || !description.is_empty() || !sequence.is_empty() || !quality_scores.is_empty() || record_num > 0 {
            let actual_size = if needs_name { name.len() } else if needs_description { description.len() } else if needs_sequence { sequence.len() } else if needs_quality_scores { quality_scores.len() } else { record_num % batch_size };
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                &quality_scores,
                projection.clone(),
                needs_name,
                needs_description,
                needs_sequence,
                needs_quality_scores,
                actual_size,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_fastq_needletail(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    // Determine which fields we need to parse based on projection
    let needs_name = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_description = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_sequence = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_quality_scores = projection.as_ref().map_or(true, |proj| proj.contains(&3));

    let mut name: Vec<String> = if needs_name {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut description: Vec<Option<String>> = if needs_description {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut sequence: Vec<String> = if needs_sequence {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut quality_scores: Vec<String> = if needs_quality_scores {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };

    let mut batch_num = 0;
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = NeedletailLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await?;
    let mut record_num = 0;

    let stream = try_stream! {

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_name {
                name.push(record.name);
            }
            if needs_description {
                description.push(record.description);
            }
            if needs_sequence {
                sequence.push(record.sequence);
            }
            if needs_quality_scores {
                quality_scores.push(record.quality_scores);
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    &quality_scores,
                    projection.clone(),
                    needs_name,
                    needs_description,
                    needs_sequence,
                    needs_quality_scores,
                    batch_size,
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();
                quality_scores.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() || !description.is_empty() || !sequence.is_empty() || !quality_scores.is_empty() || record_num > 0 {
            let actual_size = if needs_name { name.len() } else if needs_description { description.len() } else if needs_sequence { sequence.len() } else if needs_quality_scores { quality_scores.len() } else { record_num % batch_size };
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                &quality_scores,
                projection.clone(),
                needs_name,
                needs_description,
                needs_sequence,
                needs_quality_scores,
                actual_size,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

fn build_record_batch_optimized(
    schema: SchemaRef,
    name: &[String],
    description: &[Option<String>],
    sequence: &[String],
    quality_scores: &[String],
    projection: Option<Vec<usize>>,
    needs_name: bool,
    needs_description: bool,
    needs_sequence: bool,
    needs_quality_scores: bool,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    // Only create arrays for fields that are actually needed
    let name_array = if needs_name {
        Arc::new(StringArray::from(name.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let description_array = if needs_description {
        Arc::new({
            let mut builder = StringBuilder::new();
            for s in description {
                builder.append_option(s.clone());
            }
            builder.finish()
        }) as Arc<dyn Array>
    } else {
        Arc::new({
            let mut builder = StringBuilder::new();
            for _ in 0..record_count {
                builder.append_null();
            }
            builder.finish()
        }) as Arc<dyn Array>
    };

    let sequence_array = if needs_sequence {
        Arc::new(StringArray::from(sequence.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let quality_scores_array = if needs_quality_scores {
        Arc::new(StringArray::from(quality_scores.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let arrays = match projection {
        None => {
            vec![
                name_array,
                description_array,
                sequence_array,
                quality_scores_array,
            ]
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>);
            } else {
                for i in proj_ids {
                    match i {
                        0 => arrays.push(name_array.clone()),
                        1 => arrays.push(description_array.clone()),
                        2 => arrays.push(sequence_array.clone()),
                        3 => arrays.push(quality_scores_array.clone()),
                        _ => arrays.push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>),
                    }
                }
            }
            arrays
        }
    };

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating optimized batch: {:?}", e)))
}

async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    parser: FastqParser,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_fastq(
                file_path.clone(),
                schema.clone(),
                batch_size,
                thread_num,
                projection,
                object_storage_options,
                parser,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_fastq_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
                parser,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}
