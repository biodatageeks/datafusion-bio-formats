//! Local standalone FCZ and indexed Foldcomp database table providers.
//! Selectors are resolved from metadata before payload decoding; empty means zero rows.
pub mod codec;
mod manifest;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::Result,
    datasource::TableType,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use datafusion_bio_format_structure::{
    EntrySource, StructureOptions, StructureTableProvider, error,
};
use std::{any::Any, sync::Arc};
#[derive(Debug, Clone, Default)]
pub struct FoldcompOptions {
    pub structure: StructureOptions,
    pub ids: Option<Vec<String>>,
    pub entry_keys: Option<Vec<u64>>,
}
#[derive(Debug, Clone)]
pub struct FoldcompTableProvider {
    inner: StructureTableProvider,
}
impl FoldcompTableProvider {
    pub fn new(path: String, options: FoldcompOptions) -> Result<Self> {
        if options.ids.is_some() && options.entry_keys.is_some() {
            return Err(error("ids and entry_keys are mutually exclusive"));
        }
        options.structure.validate()?;
        let sources = manifest::select(&path, &options)
            .map_err(|e| error(format!("{path}: {e}")))?
            .into_iter()
            .map(|s| Arc::new(s) as Arc<dyn EntrySource>)
            .collect();
        Ok(Self {
            inner: StructureTableProvider::from_sources(sources, options.structure)?,
        })
    }
}
#[async_trait]
impl TableProvider for FoldcompTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}
