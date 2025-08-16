use crate::storage::FastaReader;
use datafusion::arrow::array::{Array, NullArray, RecordBatch, StringArray, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use futures::stream;
use log::debug;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::thread;

#[allow(dead_code)]
pub struct FastaExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
}

impl Debug for FastaExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for FastaExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        debug!("FastaExec::execute");
        debug!("Projection: {:?}", self.projection);
        let _batch_size = context.session_config().batch_size();
        let is_remote = matches!(
            get_storage_type(self.file_path.clone()),
            StorageType::HTTP | StorageType::GCS | StorageType::S3 | StorageType::AZBLOB
        );
        let file_path = self.file_path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();

        let handle = thread::spawn(move || {
            let mut reader = FastaReader::new(file_path, is_remote).unwrap();
            reader.records().collect::<Vec<_>>()
        });

        let records = handle.join().unwrap();

        let stream = stream::iter(records.into_iter().map(move |result| {
            let record = result?;
            let rec_name = std::str::from_utf8(record.name()).unwrap().to_string();
            let rec_desc = record.description().map(|s| s.to_string());
            let sequence = std::str::from_utf8(record.sequence().as_ref())
                .unwrap()
                .to_string();
            build_record_batch(
                schema.clone(),
                &[rec_name],
                &[rec_desc],
                &[sequence],
                projection.clone(),
            )
        }));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
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
