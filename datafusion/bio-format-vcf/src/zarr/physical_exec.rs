use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use zarrs::array::CodecOptions;

use super::arrays::read_projected_arrays;
use super::metadata::VcfZarrMetadata;
use super::planning::{
    DeferredPositionPruning, PartitionRowSelection, PartitioningMode, ProjectionPlan,
    PruningMethod, RowSelection, zarr_read_options,
};
use super::pruning::apply_position_array_pruning;
use super::record_batch::build_record_batch;
use super::samples::SampleSelection;
use super::table_provider::VcfZarrReadOptions;

pub(crate) struct VcfZarrExec {
    schema: SchemaRef,
    metadata: VcfZarrMetadata,
    options: VcfZarrReadOptions,
    sample_selection: SampleSelection,
    projection_plan: ProjectionPlan,
    partition_selections: Vec<PartitionRowSelection>,
    pruning_method: PruningMethod,
    deferred_pruning: Option<DeferredPositionPruning>,
    codec_options: CodecOptions,
    cache: PlanProperties,
}

pub(crate) struct VcfZarrExecConfig {
    pub schema: SchemaRef,
    pub metadata: VcfZarrMetadata,
    pub options: VcfZarrReadOptions,
    pub sample_selection: SampleSelection,
    pub projection_plan: ProjectionPlan,
    pub partition_selections: Vec<PartitionRowSelection>,
    pub pruning_method: PruningMethod,
    pub deferred_pruning: Option<DeferredPositionPruning>,
}

impl VcfZarrExec {
    pub(crate) fn new(config: VcfZarrExecConfig) -> Self {
        let VcfZarrExecConfig {
            schema,
            metadata,
            options,
            sample_selection,
            projection_plan,
            partition_selections,
            pruning_method,
            deferred_pruning,
        } = config;
        let partition_count = partition_selections.len();
        let codec_options = zarr_read_options();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            metadata,
            options,
            sample_selection,
            projection_plan,
            partition_selections,
            pruning_method,
            deferred_pruning,
            codec_options,
            cache,
        }
    }

    fn display_row_ranges(&self) -> String {
        self.partition_selections
            .iter()
            .map(|partition| partition.selection.display_ranges())
            .filter(|ranges| !ranges.is_empty())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn display_partition_ranges(&self) -> String {
        self.partition_selections
            .iter()
            .enumerate()
            .map(|(index, partition)| format!("{index}:[{}]", partition.selection.display_ranges()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn row_count(&self) -> usize {
        self.partition_selections
            .iter()
            .map(|partition| partition.selection.row_count())
            .sum()
    }
}

impl Debug for VcfZarrExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VcfZarrExec").finish()
    }
}

impl DisplayAs for VcfZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let raw_arrays = self
            .projection_plan
            .raw_arrays
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "VcfZarrExec: projection=[{columns}], raw_arrays=[{raw_arrays}], pruning={}, partition_count={}, zarr_concurrency={}, partition_ranges=[{}], row_ranges=[{}], rows={}",
            self.pruning_method.as_str(),
            self.partition_selections.len(),
            self.codec_options.concurrent_target(),
            self.display_partition_ranges(),
            self.display_row_ranges(),
            self.row_count()
        )
    }
}

impl ExecutionPlan for VcfZarrExec {
    fn name(&self) -> &str {
        "VcfZarrExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(partition_selection) = self.partition_selections.get(partition) else {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr partition {partition} is out of range; partition count is {}",
                self.partition_selections.len()
            )));
        };

        let batch_size = context.session_config().batch_size().max(1);
        let partition_selection = partition_selection.clone();
        let partition_count = self.partition_selections.len();
        let metadata = self.metadata.clone();
        let options = self.options.clone();
        let sample_selection = self.sample_selection.clone();
        let schema = self.schema.clone();
        let stream_schema = self.schema.clone();
        let codec_options = self.codec_options;
        let deferred_pruning = self.deferred_pruning.clone();

        let stream = async_stream::try_stream! {
            let pruning_metadata = metadata.clone();
            let pruning_options = options.clone();
            let pruning_codec_options = codec_options;
            let batch_selections = tokio::task::spawn_blocking(move || -> Result<Vec<RowSelection>> {
                let row_selection =
                    if let (Some(deferred), PartitioningMode::ChunkCandidates) =
                        (&deferred_pruning, partition_selection.mode)
                    {
                        let limit = if partition_count == 1 {
                            deferred.limit
                        } else {
                            // A partition-local limit would be incorrect here because each
                            // partition owns only chunk candidates, not globally ordered
                            // exact matches. DataFusion's outer LIMIT applies after all
                            // partitions emit their pruned rows.
                            None
                        };
                        apply_position_array_pruning(
                            &pruning_metadata,
                            &pruning_options,
                            &partition_selection.selection,
                            &deferred.constraints,
                            limit,
                            &pruning_codec_options,
                        )?
                    } else {
                        partition_selection.selection.clone()
                    };

                Ok(row_selection.split_by_row_count(batch_size))
            })
            .await
            .map_err(join_blocking_error)??;

            for row_selection in batch_selections {
                let batch_metadata = metadata.clone();
                let batch_options = options.clone();
                let batch_sample_selection = sample_selection.clone();
                let batch_schema = schema.clone();
                let batch_codec_options = codec_options;
                let batch = tokio::task::spawn_blocking(move || -> Result<RecordBatch> {
                    let arrays = read_projected_arrays(
                        &batch_metadata,
                        &batch_schema,
                        &batch_options,
                        &batch_sample_selection,
                        &row_selection,
                        &batch_codec_options,
                    )?;
                    build_record_batch(batch_schema, arrays)
                })
                .await
                .map_err(join_blocking_error)??;

                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

fn join_blocking_error(error: tokio::task::JoinError) -> DataFusionError {
    DataFusionError::Execution(format!("VCF Zarr blocking read task failed: {error}"))
}
