use crate::storage::{FastaLocalReader, FastaRemoteReader, open_local_fasta_sync};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, NullArray, RecordBatch, StringArray, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_storage_type,
};

use futures::Future;
use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for reading FASTA files.
///
/// This struct implements DataFusion's `ExecutionPlan` trait and is responsible for executing
/// queries against FASTA files. It orchestrates the reading of FASTA records, optionally filtering
/// columns via projection, and limits the number of records processed.
///
/// # Fields
///
/// - `file_path`: Path to the FASTA file (local or cloud storage URI)
/// - `schema`: Arrow schema describing the FASTA record structure
/// - `projection`: Optional column indices to project/filter the output columns
/// - `cache`: Cached plan properties for performance
/// - `limit`: Optional maximum number of records to read
/// - `thread_num`: Number of threads to use for parallel BGZF decompression
/// - `object_storage_options`: Configuration for cloud storage access
#[allow(dead_code)]
pub struct FastaExec {
    /// File path for the FASTA file.
    pub(crate) file_path: String,
    /// Arrow schema for the FASTA records.
    pub(crate) schema: SchemaRef,
    /// Optional column projection indices.
    pub(crate) projection: Option<Vec<usize>>,
    /// Cached plan properties.
    pub(crate) cache: PlanProperties,
    /// Optional record limit.
    pub(crate) limit: Option<usize>,
    /// Number of threads for decompression.
    pub(crate) thread_num: Option<usize>,
    /// Cloud storage configuration options.
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
}

impl Debug for FastaExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastaExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for FastaExec {
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
        write!(f, "FastaExec: projection=[{}]", proj_str)
    }
}

impl ExecutionPlan for FastaExec {
    fn name(&self) -> &str {
        "FastaExec"
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
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.thread_num,
            self.projection.clone(),
            self.object_storage_options.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn get_remote_fasta_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader =
        FastaRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut name: Vec<String> = Vec::with_capacity(batch_size);
        let mut description: Vec<Option<String>> = Vec::with_capacity(batch_size);
        let mut sequence: Vec<String>  = Vec::with_capacity(batch_size);


        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            name.push(std::str::from_utf8(record.name()).unwrap().to_string());
            description.push(
                record.description().map(|s| std::str::from_utf8(s).unwrap().to_string())
            );
            sequence.push(std::str::from_utf8(record.sequence().as_ref()).unwrap().to_string());

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();

            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() {
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_fasta(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut name: Vec<String> = Vec::with_capacity(batch_size);
    let mut description: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut sequence: Vec<String> = Vec::with_capacity(batch_size);

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = FastaLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await?;
    let mut record_num = 0;

    let stream = try_stream! {

        let mut records = reader.read_records().await;
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
             name.push(std::str::from_utf8(record.name()).unwrap().to_string());
            description.push(
                record.description().map(|s| std::str::from_utf8(s).unwrap().to_string())
            );
            sequence.push(std::str::from_utf8(record.sequence().as_ref()).unwrap().to_string());

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &name,
                    &description,
                    &sequence,
                    projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                name.clear();
                description.clear();
                sequence.clear();
            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !name.is_empty() {
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
                &name,
                &description,
                &sequence,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

/// Reads a local FASTA file using a synchronous thread with buffer reuse.
///
/// Uses `read_record(&mut record)` to reuse a single record buffer across reads,
/// avoiding per-record clone allocations. Sends batches via a bounded channel for
/// backpressure. Falls back to the async `get_local_fasta` for GZIP compression.
async fn get_local_fasta_sync(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    compression_type: CompressionType,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<
        Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    >(2);
    let thread_count = thread_num.unwrap_or(1);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let mut reader = open_local_fasta_sync(&file_path, compression_type, thread_count)
                .map_err(|e| DataFusionError::Execution(format!("Failed to open FASTA: {}", e)))?;

            let mut name: Vec<String> = Vec::with_capacity(batch_size);
            let mut description: Vec<Option<String>> = Vec::with_capacity(batch_size);
            let mut sequence: Vec<String> = Vec::with_capacity(batch_size);

            let mut def_buf = String::new();
            let mut seq_buf = Vec::new();
            let mut record_num = 0usize;

            loop {
                def_buf.clear();
                match reader.read_definition(&mut def_buf) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        seq_buf.clear();
                        reader.read_sequence(&mut seq_buf).map_err(|e| {
                            DataFusionError::Execution(format!("FASTA sequence read error: {}", e))
                        })?;

                        let definition: noodles_fasta::record::Definition =
                            def_buf.parse().map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "FASTA definition parse error: {}",
                                    e
                                ))
                            })?;

                        name.push(
                            std::str::from_utf8(definition.name())
                                .unwrap_or("")
                                .to_string(),
                        );
                        description.push(
                            definition
                                .description()
                                .map(|s| std::str::from_utf8(s).unwrap_or("").to_string()),
                        );
                        sequence.push(std::str::from_utf8(&seq_buf).unwrap_or("").to_string());

                        record_num += 1;

                        if record_num % batch_size == 0 {
                            debug!("Record number: {}", record_num);
                            let batch = build_record_batch(
                                Arc::clone(&schema),
                                &name,
                                &description,
                                &sequence,
                                projection.clone(),
                            )?;

                            loop {
                                match tx.try_send(Ok(batch.clone())) {
                                    Ok(()) => break,
                                    Err(e) if e.is_disconnected() => return Ok(()),
                                    Err(_) => std::thread::yield_now(),
                                }
                            }

                            name.clear();
                            description.clear();
                            sequence.clear();
                        }
                    }
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!(
                            "FASTA read error: {}",
                            e
                        )));
                    }
                }
            }

            // Remaining records
            if !name.is_empty() {
                let batch = build_record_batch(
                    Arc::clone(&schema),
                    &name,
                    &description,
                    &sequence,
                    projection.clone(),
                )?;
                loop {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }

            debug!("Local FASTA sync scan: {} records", record_num);
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(e),
            )));
        }
    });

    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

fn build_record_batch(
    schema: SchemaRef,
    name: &[String],
    description: &[Option<String>],
    sequence: &[String],
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<RecordBatch> {
    let name_array = Arc::new(StringArray::from(name.to_vec())) as Arc<dyn Array>;
    let sequence_array = Arc::new(StringArray::from(sequence.to_vec())) as Arc<dyn Array>;
    let description_array = Arc::new({
        let mut builder = StringBuilder::new();
        for s in description {
            builder.append_option(s.clone());
        }
        builder.finish()
    }) as Arc<dyn Array>;
    let arrays = match projection {
        None => {
            let arrays: Vec<Arc<dyn Array>> = vec![name_array, description_array, sequence_array];
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
                        1 => arrays.push(description_array.clone()),
                        2 => arrays.push(sequence_array.clone()),
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
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let opts = object_storage_options.clone().unwrap_or_default();
            let compression_type = get_compression_type(
                file_path.clone(),
                opts.compression_type.clone(),
                opts.clone(),
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to detect compression: {}", e))
            })?;

            if matches!(compression_type, CompressionType::GZIP) {
                // GZIP: fall back to the async stream-based reader
                let stream = get_local_fasta(
                    file_path.clone(),
                    schema.clone(),
                    batch_size,
                    thread_num,
                    projection,
                    object_storage_options,
                )
                .await?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
            } else {
                // BGZF / PLAIN: sync thread with buffer reuse
                get_local_fasta_sync(
                    file_path.clone(),
                    schema_ref,
                    batch_size,
                    thread_num,
                    projection,
                    compression_type,
                )
                .await
            }
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_fasta_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}
