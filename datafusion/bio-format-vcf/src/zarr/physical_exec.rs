use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::stream;

use super::arrays::read_projected_arrays;
use super::metadata::VcfZarrMetadata;
use super::record_batch::build_record_batch;
use super::table_provider::VcfZarrReadOptions;

pub(crate) struct VcfZarrExec {
    schema: SchemaRef,
    metadata: VcfZarrMetadata,
    options: VcfZarrReadOptions,
    limit: Option<usize>,
    cache: PlanProperties,
}

impl VcfZarrExec {
    pub(crate) fn new(
        schema: SchemaRef,
        metadata: VcfZarrMetadata,
        options: VcfZarrReadOptions,
        limit: Option<usize>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            metadata,
            options,
            limit,
            cache,
        }
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
        write!(f, "VcfZarrExec: projection=[{columns}]")
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let arrays =
            read_projected_arrays(&self.metadata, &self.schema, &self.options, self.limit)?;
        let batch = build_record_batch(self.schema.clone(), arrays)?;
        let stream = stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
