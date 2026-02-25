use crate::physical_exec::{FastqExec, FastqPartitionStrategy, detect_local_strategy};
use crate::write_exec::FastqWriteExec;
use crate::writer::FastqCompressionType;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Constraints;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, dml::InsertOp};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use log::debug;
use std::any::Any;
use std::sync::Arc;

fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];
    let schema = Schema::new(fields);
    debug!("Schema: {schema:?}");
    Ok(Arc::new(schema))
}

/// A DataFusion table provider for FASTQ files
///
/// This provider enables SQL queries on FASTQ files (both compressed and uncompressed).
/// It supports local files and cloud storage (S3, GCS, Azure) via object storage options.
///
/// For local files, the provider automatically detects the best read strategy:
/// - BGZF-compressed files with a GZI index are read in parallel partitions
/// - Uncompressed files are split into byte-range partitions for parallel reads
/// - GZIP-compressed files are read sequentially (single partition)
///
/// Parallelism is controlled by DataFusion's `SessionConfig::target_partitions()`.
#[derive(Clone, Debug)]
pub struct FastqTableProvider {
    file_path: String,
    schema: SchemaRef,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl FastqTableProvider {
    /// Creates a new FASTQ table provider
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the FASTQ file (local or cloud storage URL)
    /// * `object_storage_options` - Optional configuration for cloud storage access
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be determined
    pub fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
            schema,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for FastqTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("FastqTableProvider::scan");

        // Determine partition strategy based on file type and location
        let store_type = get_storage_type(self.file_path.clone());
        let target_partitions = state.config().target_partitions();

        let strategy = if matches!(store_type, StorageType::LOCAL) {
            detect_local_strategy(&self.file_path, target_partitions).map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Failed to detect FASTQ partition strategy for '{}': {}",
                    self.file_path, e
                ))
            })?
        } else {
            FastqPartitionStrategy::Sequential
        };

        // For empty projections (e.g. COUNT(*)), the thread+channel strategies (Bgzf/ByteRange)
        // produce zero-column batches with row count, so they need a zero-field schema.
        // The Sequential strategy uses the stream-based path which needs a dummy Null column.
        let uses_threaded_path = !matches!(strategy, FastqPartitionStrategy::Sequential);

        fn project_schema(
            schema: &SchemaRef,
            projection: Option<&Vec<usize>>,
            uses_threaded_path: bool,
        ) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    if uses_threaded_path {
                        Arc::new(Schema::empty())
                    } else {
                        Arc::new(Schema::new(vec![Field::new("dummy", DataType::Null, true)]))
                    }
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new(projected_fields))
                }
                None => schema.clone(),
            }
        }

        let schema = project_schema(&self.schema, projection, uses_threaded_path);

        let num_partitions = match &strategy {
            FastqPartitionStrategy::Bgzf { partitions, .. } => partitions.len(),
            FastqPartitionStrategy::ByteRange { partitions } => partitions.len(),
            FastqPartitionStrategy::Sequential => 1,
        };

        Ok(Arc::new(FastqExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(num_partitions),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            strategy,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("FastqTableProvider::insert_into");

        // Only OVERWRITE mode is supported (file will be created/replaced)
        if insert_op != InsertOp::Overwrite {
            return Err(datafusion::common::DataFusionError::NotImplemented(
                "FASTQ write only supports OVERWRITE mode (INSERT OVERWRITE). \
                 APPEND mode is not supported."
                    .to_string(),
            ));
        }

        // Validate input schema matches FASTQ schema
        let input_schema = input.schema();
        if input_schema.fields().len() < 4 {
            return Err(datafusion::common::DataFusionError::Plan(
                "Input schema must have at least 4 columns: name, description, sequence, quality_scores"
                    .to_string(),
            ));
        }

        // Determine compression from file path
        let compression = FastqCompressionType::from_path(&self.file_path);

        Ok(Arc::new(FastqWriteExec::new(
            input,
            self.file_path.clone(),
            Some(compression),
        )))
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }
}
