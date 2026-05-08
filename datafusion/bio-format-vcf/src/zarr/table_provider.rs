use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use super::metadata::VcfZarrMetadata;

/// Options controlling how VCF Zarr data is exposed through DataFusion.
#[derive(Clone, Debug, Default)]
pub struct VcfZarrReadOptions {
    /// Optional list of INFO fields to include. `None` means include all fields.
    pub info_fields: Option<Vec<String>>,
    /// Optional list of FORMAT fields to include. `None` means include all fields.
    pub format_fields: Option<Vec<String>>,
    /// Optional list of sample names to include. `None` means include all samples.
    pub samples: Option<Vec<String>>,
    /// If true, expose positions as zero-based coordinates.
    pub coordinate_system_zero_based: bool,
}

/// A DataFusion table provider for local VCF Zarr stores.
#[derive(Debug)]
pub struct VcfZarrTableProvider {
    path: String,
    options: VcfZarrReadOptions,
    metadata: VcfZarrMetadata,
    schema: SchemaRef,
}

impl VcfZarrTableProvider {
    /// Creates a new VCF Zarr table provider from a local store path.
    pub fn new(path: String, options: VcfZarrReadOptions) -> Result<Self> {
        let metadata = VcfZarrMetadata::open_local(&path)?;
        let schema = Arc::new(Schema::empty());

        Ok(Self {
            path,
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (&self.path, &self.options, &self.metadata);

        Err(DataFusionError::NotImplemented(
            "VCF Zarr execution plan is not implemented yet".to_string(),
        ))
    }
}
