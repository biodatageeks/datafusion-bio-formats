use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use futures_util::TryStreamExt;
use log::debug;
use tokio::io::AsyncBufRead;
use tokio_stream::Stream;
use tokio_util::io::StreamReader;

use arrow::array::{ArrayRef, RecordBatchOptions};
use oxbow::alignment::model::BatchBuilder;
use oxbow::alignment::model::Push as _;

#[allow(dead_code)]
pub struct BamExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) tag_defs: Option<Vec<(String, String)>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: usize,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
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
        debug!("Fields: {:?}", self.fields);
        debug!("Tag defs: {:?}", self.tag_defs);
        let batch_size = context.session_config().batch_size();
        let thread_num = self.thread_num;
        let storage_type = get_storage_type(self.file_path.clone());
        let fut = get_stream(
            self.file_path.clone(),
            self.fields.clone(),
            self.tag_defs.clone(),
            self.schema.clone(),
            storage_type,
            batch_size,
            self.limit,
            thread_num,
            self.object_storage_options.clone(),
        );
        let schema = self.schema.clone();
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    fields: Option<Vec<String>>,
    tag_defs: Option<Vec<(String, String)>>,
    schema: SchemaRef,
    storage_type: StorageType,
    batch_size: usize,
    limit: Option<usize>,
    thread_num: usize,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    match storage_type {
        StorageType::LOCAL => {
            let mut reader = std::fs::File::open(file_path)
                .map(|f| {
                    noodles_bgzf::MultithreadedReader::with_worker_count(
                        std::num::NonZero::new(thread_num).unwrap(),
                        f,
                    )
                })
                .map(noodles::bam::io::Reader::from)?;
            let header = reader.read_header()?;
            if schema.fields().is_empty() {
                let stream = columnless_batch_stream(reader, batch_size, limit);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            } else {
                let builder = BatchBuilder::new(header, fields, tag_defs, batch_size)?;
                let stream = local_batch_stream(reader, builder, batch_size, limit);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
        StorageType::AZBLOB | StorageType::GCS | StorageType::S3 => {
            let object_storage_options = object_storage_options
                .expect("Object storage options must be provided for remote BAM files");
            let bytes_stream = get_remote_stream(file_path.clone(), object_storage_options, None)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to get remote bytes stream: {e}"))
                })?;
            let mut reader = noodles::bam::r#async::io::Reader::from(
                noodles::bgzf::AsyncReader::new(StreamReader::new(bytes_stream)),
            );
            let header = reader.read_header().await?;
            if schema.fields().is_empty() {
                let stream = remote_columnless_batch_stream(reader, batch_size, limit);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            } else {
                let builder = BatchBuilder::new(header, fields, tag_defs, batch_size)?;
                let stream = remote_batch_stream(reader, builder, batch_size, limit);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
        _ => panic!("Unsupported storage type for BAM file: {storage_type:?}"),
    }
}

pub fn local_batch_stream<R>(
    mut reader: noodles::bam::io::Reader<R>,
    mut builder: BatchBuilder,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
where
    R: std::io::Read,
{
    stream! {
        let mut record = noodles::bam::Record::default();
        let mut total_count = 0;
        let limit = limit.unwrap_or(usize::MAX);

        loop {
            let mut count = 0;

            while count < batch_size && total_count < limit {
                match reader.read_record(&mut record) {
                    Ok(0) => break, // EOF
                    Ok(_) => match builder.push(&record) {
                        Ok(()) => {
                            total_count += 1;
                            count += 1;
                        }
                        Err(e) => {
                            yield Err(e.into());
                            return;
                        }
                    },
                    Err(e) => {
                        yield Err(DataFusionError::IoError(e));
                        return;
                    }
                }
            }

            if count == 0 {
                break;
            }

            yield builder.finish().map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }
}

pub fn remote_batch_stream<R>(
    mut reader: noodles::bam::r#async::io::Reader<R>,
    mut builder: BatchBuilder,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
where
    R: AsyncBufRead + Unpin,
{
    stream! {
        let mut record = noodles::bam::Record::default();
        let mut total_count = 0;
        let limit = limit.unwrap_or(usize::MAX);

        loop {
            let mut count = 0;

            while count < batch_size && total_count < limit {
                match reader.read_record(&mut record).await {
                    Ok(0) => break, // EOF
                    Ok(_) => match builder.push(&record) {
                        Ok(()) => {
                            total_count += 1;
                            count += 1;
                        }
                        Err(e) => {
                            yield Err(e.into());
                            return;
                        }
                    },
                    Err(e) => {
                        yield Err(DataFusionError::IoError(e));
                        return;
                    }
                }
            }

            if count == 0 {
                break;
            }

            yield builder.finish().map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }
}

pub fn columnless_batch_stream<R>(
    mut reader: noodles::bam::io::Reader<R>,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
where
    R: std::io::Read,
{
    stream! {
        let mut record = noodles::bam::Record::default();
        let mut total_count = 0;
        let limit = limit.unwrap_or(usize::MAX);

        loop {
            let mut count = 0;

            while count < batch_size && total_count < limit {
                match reader.read_record(&mut record) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        count += 1;
                        total_count += 1;
                    },
                    Err(e) => {
                        yield Err(DataFusionError::IoError(e));
                        return;
                    }
                }
            }

            if count == 0 {
                break;
            }

            let columns: Vec<ArrayRef> = vec![];
            let schema = arrow::datatypes::Schema::new(vec![] as Vec<arrow::datatypes::Field>);
            let options = RecordBatchOptions::new().with_row_count(Some(count));

            yield RecordBatch::try_new_with_options(Arc::new(schema), columns, &options)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }
}

pub fn remote_columnless_batch_stream<R>(
    mut reader: noodles::bam::r#async::io::Reader<R>,
    batch_size: usize,
    limit: Option<usize>,
) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
where
    R: AsyncBufRead + Unpin,
{
    stream! {
        let mut record = noodles::bam::Record::default();
        let mut total_count = 0;
        let limit = limit.unwrap_or(usize::MAX);

        loop {
            let mut count = 0;

            while count < batch_size && total_count < limit {
                match reader.read_record(&mut record).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        count += 1;
                        total_count += 1;
                    },
                    Err(e) => {
                        yield Err(DataFusionError::IoError(e));
                        return;
                    }
                }
            }

            if count == 0 {
                break;
            }

            let columns: Vec<ArrayRef> = vec![];
            let schema = arrow::datatypes::Schema::new(vec![] as Vec<arrow::datatypes::Field>);
            let options = RecordBatchOptions::new().with_row_count(Some(count));

            yield RecordBatch::try_new_with_options(Arc::new(schema), columns, &options)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }
}
