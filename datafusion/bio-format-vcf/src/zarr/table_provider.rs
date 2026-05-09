use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use super::metadata::VcfZarrMetadata;
use super::physical_exec::VcfZarrExec;
use super::planning::{ProjectionPlan, PruningMethod};
use super::pruning::build_row_pruning;
use super::schema::build_logical_schema;

/// Options controlling how VCF Zarr data is exposed through DataFusion.
#[derive(Clone, Debug, Default)]
pub struct VcfZarrReadOptions {
    /// Optional list of INFO fields to include. `None` does not infer fields yet.
    pub info_fields: Option<Vec<String>>,
    /// Optional list of FORMAT fields to include. `None` does not infer fields yet.
    pub format_fields: Option<Vec<String>>,
    /// Optional list of sample names to include once sample discovery/subsetting is implemented.
    pub samples: Option<Vec<String>>,
    /// If true, expose positions as zero-based coordinates.
    pub coordinate_system_zero_based: bool,
}

/// A DataFusion table provider for local VCF Zarr stores.
#[derive(Debug)]
pub struct VcfZarrTableProvider {
    options: VcfZarrReadOptions,
    metadata: VcfZarrMetadata,
    schema: SchemaRef,
}

impl VcfZarrTableProvider {
    /// Creates a new VCF Zarr table provider from a local store path.
    pub fn new(path: String, options: VcfZarrReadOptions) -> Result<Self> {
        let metadata = VcfZarrMetadata::open_local(&path)?;
        let schema = build_logical_schema(&metadata, &options)?;

        Ok(Self {
            options,
            metadata,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for VcfZarrTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut projection_plan =
            ProjectionPlan::from_projection_and_filters(&self.schema, projection, filters);
        let row_pruning = build_row_pruning(&self.metadata, &self.options, filters, limit)?;
        if row_pruning.method == PruningMethod::RegionIndex {
            projection_plan
                .raw_arrays
                .insert("region_index".to_string());
        }
        let schema = project_schema(&self.schema, projection);

        Ok(Arc::new(VcfZarrExec::new(
            schema,
            self.metadata.clone(),
            self.options.clone(),
            projection_plan,
            row_pruning.selection,
            row_pruning.method,
        )))
    }
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) if indices.is_empty() => Arc::new(Schema::new_with_metadata(
            Vec::<Field>::new(),
            schema.metadata().clone(),
        )),
        Some(indices) => Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|index| schema.field(*index).clone())
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        )),
        None => schema.clone(),
    }
}
