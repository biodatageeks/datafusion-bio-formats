use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use log::debug;
use tokio_util::io::StreamReader;

use crate::physical_exec::BamExec;

/// A DataFusion table provider for BAM (Binary Alignment Map) files.
///
/// This struct implements the DataFusion TableProvider trait, allowing BAM files
/// to be queried using SQL via DataFusion. It supports both local and remote
/// (cloud storage) files with configurable threading for decompression.
#[derive(Clone, Debug)]
pub struct BamTableProvider {
    /// Path to the BAM file (local or remote)
    file_path: String,
    // Fields and tag defs define the initial table schema
    fields: Option<Vec<String>>,             // None means default fields
    tag_defs: Option<Vec<(String, String)>>, // None means no tags column
    /// Schema of the BAM file as an Arrow SchemaRef
    schema: SchemaRef,
    /// Number of threads to use for BGZF decompression
    thread_num: usize,
    /// Configuration for cloud storage access
    object_storage_options: Option<ObjectStorageOptions>,
}

impl BamTableProvider {
    /// Creates a new BAM table provider.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BAM file (local or remote URL)
    /// * `thread_num` - Optional number of threads for BGZF decompression
    /// * `object_storage_options` - Optional configuration for cloud storage access
    ///
    /// # Returns
    ///
    /// A new BamTableProvider or a DataFusion error if schema determination fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use datafusion_bio_format_bam::table_provider::BamTableProvider;
    ///
    /// # async fn example() -> datafusion::common::Result<()> {
    /// let provider = BamTableProvider::try_new(
    ///     "data/alignments.bam".to_string(),
    ///     None,     // No field filter
    ///     None,     // No tag definitions
    ///     Some(4),  // Use 4 threads
    ///     None,     // No cloud storage
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_new(
        file_path: String,
        fields: Option<Vec<String>>,
        tag_defs: Option<Vec<(String, String)>>,
        // tag_scan_rows: Option<usize>,  // TODO: optional tag discovery
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let thread_num = thread_num.unwrap_or(1);
        let header = get_header(&file_path, thread_num, &object_storage_options).await?;
        let builder = oxbow::alignment::model::BatchBuilder::new(
            header.clone(),
            fields.clone(),
            tag_defs.clone(),
            0,
        )?;
        let schema = Arc::new(builder.get_arrow_schema());
        Ok(Self {
            file_path,
            fields,
            tag_defs,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for BamTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("BamTableProvider::scan");

        let (fields, tag_defs, projected_schema) = match projection {
            None => (
                self.fields.clone(),
                self.tag_defs.clone(),
                self.schema.clone(),
            ),
            Some(indices) => {
                let projected_schema: SchemaRef = self.schema.project(indices)?.into();

                let fields: Option<Vec<String>> = Some(
                    indices
                        .iter()
                        .map(|i| self.schema.field(*i).name().to_string())
                        .filter(|name| name != "tags")
                        .collect(),
                );

                let tag_defs: Option<Vec<(String, String)>> = Some(
                    indices
                        .iter()
                        .filter_map(|&i| {
                            if *self.schema.field(i).name() == "tags" {
                                self.tag_defs.clone()
                            } else {
                                None
                            }
                        })
                        .flatten()
                        .collect(),
                );

                (fields, tag_defs, projected_schema)
            }
        };

        Ok(Arc::new(BamExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            fields,
            tag_defs,
            schema: projected_schema,
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}

async fn get_header(
    file_path: &str,
    thread_num: usize,
    object_storage_options: &Option<ObjectStorageOptions>,
) -> datafusion::common::Result<noodles::sam::Header> {
    let storage_type = get_storage_type(file_path.to_owned());
    let header = match storage_type {
        StorageType::LOCAL => {
            // For local files, use spawn_blocking to avoid blocking the async runtime
            let file_path = file_path.to_owned();
            tokio::task::spawn_blocking(move || {
                let mut reader = std::fs::File::open(&file_path)
                    .map(|f| {
                        noodles_bgzf::MultithreadedReader::with_worker_count(
                            std::num::NonZero::new(thread_num).unwrap(),
                            f,
                        )
                    })
                    .map(noodles::bam::io::Reader::from)?;
                reader.read_header()
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("Join error: {e}")))?
        }
        StorageType::AZBLOB | StorageType::GCS | StorageType::S3 => {
            let object_storage_options = object_storage_options
                .as_ref()
                .expect("Object storage options must be provided for remote BAM files");
            let bytes_stream =
                get_remote_stream(file_path.to_owned(), object_storage_options.clone(), None)
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to get remote bytes stream: {e}"
                        ))
                    })?;
            let mut reader = noodles::bam::r#async::io::Reader::from(
                noodles::bgzf::AsyncReader::new(StreamReader::new(bytes_stream)),
            );
            reader.read_header().await
        }
        _ => panic!("Unsupported storage type for BAM file: {storage_type:?}"),
    }?;
    Ok(header)
}
