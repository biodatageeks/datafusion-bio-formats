//! Whole-source partitions with fresh cursors per execution and conservative filter pushdown.
use crate::{batch_builder, model::NormalizedEntry, options::StructureOptions, schema};
use async_trait::async_trait;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::Result,
    datasource::TableType,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use std::{any::Any, fmt, sync::Arc};
/// A bounded source of whole entries. Implementations must reopen resources on every call.
#[async_trait]
pub trait EntrySource: fmt::Debug + Send + Sync {
    async fn load(&self, options: &StructureOptions) -> Result<Vec<NormalizedEntry>>;
}
#[derive(Debug, Clone)]
pub struct StructureTableProvider {
    sources: Vec<Arc<dyn EntrySource>>,
    options: StructureOptions,
    schema: SchemaRef,
}
impl StructureTableProvider {
    pub fn from_sources(
        sources: Vec<Arc<dyn EntrySource>>,
        options: StructureOptions,
    ) -> Result<Self> {
        options.validate()?;
        let schema = schema::schema(&options);
        Ok(Self {
            sources,
            options,
            schema,
        })
    }
    #[cfg(feature = "text-formats")]
    pub fn new(
        paths: Vec<String>,
        format: Option<crate::manifest::TextFormat>,
        options: StructureOptions,
        storage: Option<datafusion_bio_format_core::object_storage::ObjectStorageOptions>,
    ) -> Result<Self> {
        let sources = crate::manifest::expand(paths, format)?
            .into_iter()
            .map(|source| {
                Arc::new(TextSource {
                    source,
                    storage: storage.clone(),
                }) as Arc<dyn EntrySource>
            })
            .collect();
        Self::from_sources(sources, options)
    }
}
#[cfg(feature = "text-formats")]
#[derive(Debug)]
struct TextSource {
    source: crate::manifest::Source,
    storage: Option<datafusion_bio_format_core::object_storage::ObjectStorageOptions>,
}
#[cfg(feature = "text-formats")]
#[async_trait]
impl EntrySource for TextSource {
    async fn load(&self, options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
        let result = async {
            let (data, encoded_bytes) =
                crate::storage::read(&self.source.path, options, self.storage.clone()).await?;
            let (mut entries, format) = match self.source.format {
                crate::manifest::TextFormat::Pdb => (
                    crate::pdb::parse(
                        std::str::from_utf8(&data).map_err(|e| crate::error(e.to_string()))?,
                        options,
                    )?,
                    "pdb",
                ),
                crate::manifest::TextFormat::Mmcif => {
                    (crate::mmcif::parse(&data, options)?, "mmcif")
                }
            };
            if let Some(first) = entries.first_mut() {
                first.encoded_bytes = encoded_bytes;
            }
            for e in &mut entries {
                e.source_path = self.source.path.clone();
                e.source_index = self.source.source_index;
                e.source_format = format.into();
            }
            Ok(entries)
        }
        .await;
        result.map_err(|e: datafusion::common::DataFusionError| {
            crate::error(format!("{}: {e}", self.source.path))
        })
    }
}
#[async_trait]
impl TableProvider for StructureTableProvider {
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&projection)?);
        let partitions = state
            .config()
            .target_partitions()
            .min(self.sources.len())
            .max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(StructureExec {
            sources: self.sources.clone(),
            options: self.options.clone(),
            schema,
            projection,
            partitions,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}
#[derive(Debug)]
struct StructureExec {
    sources: Vec<Arc<dyn EntrySource>>,
    options: StructureOptions,
    schema: SchemaRef,
    projection: Vec<usize>,
    partitions: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
impl DisplayAs for StructureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StructureExec: level={:?}, sources={}, partitions={}",
            self.options.level,
            self.sources.len(),
            self.partitions
        )
    }
}
impl ExecutionPlan for StructureExec {
    fn name(&self) -> &str {
        "StructureExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(crate::error("StructureExec has no children"));
        }
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.partitions {
            return Err(crate::error("invalid structure partition"));
        }
        let sources: Vec<_> = self
            .sources
            .iter()
            .skip(partition)
            .step_by(self.partitions)
            .cloned()
            .collect();
        let options = self.options.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let batch_size = context.session_config().batch_size().max(1);
        let output_schema = schema.clone();
        let opened = MetricBuilder::new(&self.metrics).counter("sources_opened", partition);
        let decoded = MetricBuilder::new(&self.metrics).counter("entries_decoded", partition);
        let bytes = MetricBuilder::new(&self.metrics).counter("encoded_bytes_read", partition);
        let rows = MetricBuilder::new(&self.metrics).counter("output_rows", partition);
        let stream = async_stream::try_stream! {
            for source in sources {
                opened.add(1);
                for entry in source.load(&options).await? {
                    decoded.add(1);
                    bytes.add(entry.encoded_bytes);
                    let batch = batch_builder::build(&entry, &options, schema.clone(), &projection)?;
                    drop(entry);
                    for start in (0..batch.num_rows()).step_by(batch_size) {
                        let length = batch_size.min(batch.num_rows() - start);
                        rows.add(length);
                        yield batch.slice(start, length);
                    }
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}
