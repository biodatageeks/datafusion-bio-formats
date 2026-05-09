use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::stream;

use super::arrays::read_projected_arrays;
use super::metadata::VcfZarrMetadata;
use super::planning::{
    DeferredPositionPruning, PartitionRowSelection, PartitioningMode, ProjectionPlan, PruningMethod,
};
use super::pruning::apply_position_array_pruning;
use super::record_batch::build_record_batch;
use super::table_provider::VcfZarrReadOptions;

pub(crate) struct VcfZarrExec {
    schema: SchemaRef,
    metadata: VcfZarrMetadata,
    options: VcfZarrReadOptions,
    projection_plan: ProjectionPlan,
    partition_selections: Vec<PartitionRowSelection>,
    pruning_method: PruningMethod,
    deferred_pruning: Option<DeferredPositionPruning>,
    cache: PlanProperties,
}

impl VcfZarrExec {
    pub(crate) fn new(
        schema: SchemaRef,
        metadata: VcfZarrMetadata,
        options: VcfZarrReadOptions,
        projection_plan: ProjectionPlan,
        partition_selections: Vec<PartitionRowSelection>,
        pruning_method: PruningMethod,
        deferred_pruning: Option<DeferredPositionPruning>,
    ) -> Self {
        let partition_count = partition_selections.len();
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
            projection_plan,
            partition_selections,
            pruning_method,
            deferred_pruning,
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
            "VcfZarrExec: projection=[{columns}], raw_arrays=[{raw_arrays}], pruning={}, partition_count={}, partition_ranges=[{}], row_ranges=[{}], rows={}",
            self.pruning_method.as_str(),
            self.partition_selections.len(),
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(partition_selection) = self.partition_selections.get(partition) else {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr partition {partition} is out of range; partition count is {}",
                self.partition_selections.len()
            )));
        };
        let row_selection = if let (Some(deferred), PartitioningMode::ChunkCandidates) =
            (&self.deferred_pruning, partition_selection.mode)
        {
            apply_position_array_pruning(
                &self.metadata,
                &self.options,
                &partition_selection.selection,
                &deferred.constraints,
                deferred.limit,
            )?
        } else {
            partition_selection.selection.clone()
        };
        let arrays =
            read_projected_arrays(&self.metadata, &self.schema, &self.options, &row_selection)?;
        let batch = build_record_batch(self.schema.clone(), arrays)?;
        let stream = stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
