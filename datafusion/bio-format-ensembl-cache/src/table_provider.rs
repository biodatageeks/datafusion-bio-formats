use crate::discovery::{
    discover_regulatory_files, discover_transcript_files, discover_variation_files,
};
use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::filter::{extract_simple_predicate, is_pushdown_supported};
use crate::info::CacheInfo;
use crate::physical_exec::EnsemblCacheExec;
use crate::schema::{
    motif_feature_schema, regulatory_feature_schema, transcript_schema, variation_schema,
};
use crate::variation::detect_region_size;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::path::Path;
use std::sync::Arc;

/// Configuration for Ensembl cache table providers.
#[derive(Debug, Clone)]
pub struct EnsemblCacheOptions {
    /// Root directory of a single Ensembl VEP cache.
    pub cache_root: String,
    /// Coordinate-system metadata marker exposed in Arrow schema.
    pub coordinate_system_zero_based: bool,
    /// Optional batch-size override used by the execution plan.
    pub batch_size_hint: Option<usize>,
}

impl EnsemblCacheOptions {
    /// Creates new options with defaults.
    ///
    /// Defaults:
    /// - `coordinate_system_zero_based = false` (VEP cache semantics)
    /// - `batch_size_hint = None` (use DataFusion session batch size)
    pub fn new(cache_root: impl Into<String>) -> Self {
        Self {
            cache_root: cache_root.into(),
            coordinate_system_zero_based: false,
            batch_size_hint: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ProviderInner {
    kind: EnsemblEntityKind,
    options: EnsemblCacheOptions,
    cache_info: CacheInfo,
    schema: SchemaRef,
    files: Vec<std::path::PathBuf>,
    variation_region_size: Option<i64>,
}

impl ProviderInner {
    fn new(kind: EnsemblEntityKind, options: EnsemblCacheOptions) -> Result<Self> {
        let cache_root = Path::new(&options.cache_root);
        let cache_info = CacheInfo::from_root(cache_root)?;

        let (schema, files, variation_region_size) = match kind {
            EnsemblEntityKind::Variation => {
                let schema = variation_schema(&cache_info, options.coordinate_system_zero_based)?;
                // var_type is part of VEP cache contract. v1 discovery already prefers all_vars
                // when present, which matches tabix caches.
                let _is_tabix_mode = cache_info.var_type.as_deref() == Some("tabix");
                let files = discover_variation_files(cache_root)?;
                let region_size = Some(detect_region_size(&cache_info, &files));
                (schema, files, region_size)
            }
            EnsemblEntityKind::Transcript => {
                validate_serializer(&cache_info)?;
                let schema = transcript_schema(&cache_info, options.coordinate_system_zero_based);
                let files = discover_transcript_files(cache_root)?;
                (schema, files, None)
            }
            EnsemblEntityKind::RegulatoryFeature => {
                validate_serializer(&cache_info)?;
                let schema =
                    regulatory_feature_schema(&cache_info, options.coordinate_system_zero_based);
                let files = discover_regulatory_files(cache_root)?;
                (schema, files, None)
            }
            EnsemblEntityKind::MotifFeature => {
                validate_serializer(&cache_info)?;
                let schema =
                    motif_feature_schema(&cache_info, options.coordinate_system_zero_based);
                let files = discover_regulatory_files(cache_root)?;
                (schema, files, None)
            }
        };

        if files.is_empty() {
            return Err(exec_err(format!(
                "No source files discovered for {:?} under {}",
                kind,
                cache_root.display()
            )));
        }

        Ok(Self {
            kind,
            options,
            cache_info,
            schema,
            files,
            variation_region_size,
        })
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if is_pushdown_supported(expr) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection);
        let predicate = extract_simple_predicate(filters);

        Ok(Arc::new(EnsemblCacheExec::new(
            self.kind,
            self.cache_info.clone(),
            self.files.clone(),
            projected_schema,
            predicate,
            limit,
            self.variation_region_size,
            self.options.batch_size_hint,
        )))
    }
}

fn validate_serializer(cache_info: &CacheInfo) -> Result<()> {
    let Some(serializer) = cache_info.serializer_type.as_deref() else {
        return Err(exec_err(
            "Missing serialiser_type in info.txt for transcript/regulatory entities",
        ));
    };

    if serializer != "storable" && serializer != "sereal" {
        return Err(exec_err(format!(
            "Unknown serializer type in info.txt: {}",
            serializer
        )));
    }
    Ok(())
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) if indices.is_empty() => Arc::new(Schema::new_with_metadata(
            Vec::<Field>::new(),
            schema.metadata().clone(),
        )),
        Some(indices) => {
            let fields = indices
                .iter()
                .map(|idx| schema.field(*idx).clone())
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        }
        None => schema.clone(),
    }
}

/// Helper factory for creating typed Ensembl cache table providers.
#[derive(Debug, Clone)]
pub struct EnsemblCacheTableProvider;

impl EnsemblCacheTableProvider {
    /// Creates a provider for the requested entity kind.
    pub fn for_entity(
        kind: EnsemblEntityKind,
        options: EnsemblCacheOptions,
    ) -> Result<Arc<dyn TableProvider>> {
        match kind {
            EnsemblEntityKind::Variation => Ok(Arc::new(VariationTableProvider::new(options)?)),
            EnsemblEntityKind::Transcript => Ok(Arc::new(TranscriptTableProvider::new(options)?)),
            EnsemblEntityKind::RegulatoryFeature => {
                Ok(Arc::new(RegulatoryFeatureTableProvider::new(options)?))
            }
            EnsemblEntityKind::MotifFeature => {
                Ok(Arc::new(MotifFeatureTableProvider::new(options)?))
            }
        }
    }
}

/// DataFusion table provider for VEP variation cache entity.
#[derive(Debug, Clone)]
pub struct VariationTableProvider {
    inner: ProviderInner,
}

impl VariationTableProvider {
    /// Creates a variation provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::Variation, options)?,
        })
    }
}

/// DataFusion table provider for VEP transcript cache entity.
#[derive(Debug, Clone)]
pub struct TranscriptTableProvider {
    inner: ProviderInner,
}

impl TranscriptTableProvider {
    /// Creates a transcript provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::Transcript, options)?,
        })
    }
}

/// DataFusion table provider for VEP regulatory feature cache entity.
#[derive(Debug, Clone)]
pub struct RegulatoryFeatureTableProvider {
    inner: ProviderInner,
}

impl RegulatoryFeatureTableProvider {
    /// Creates a regulatory-feature provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::RegulatoryFeature, options)?,
        })
    }
}

/// DataFusion table provider for VEP motif feature cache entity.
#[derive(Debug, Clone)]
pub struct MotifFeatureTableProvider {
    inner: ProviderInner,
}

impl MotifFeatureTableProvider {
    /// Creates a motif-feature provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::MotifFeature, options)?,
        })
    }
}

macro_rules! impl_table_provider {
    ($provider:ty) => {
        #[async_trait]
        impl TableProvider for $provider {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn schema(&self) -> SchemaRef {
                self.inner.schema()
            }

            fn table_type(&self) -> TableType {
                TableType::Base
            }

            fn supports_filters_pushdown(
                &self,
                filters: &[&Expr],
            ) -> Result<Vec<TableProviderFilterPushDown>> {
                self.inner.supports_filters_pushdown(filters)
            }

            async fn scan(
                &self,
                _state: &dyn Session,
                projection: Option<&Vec<usize>>,
                filters: &[Expr],
                limit: Option<usize>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                self.inner.scan(projection, filters, limit).await
            }
        }
    };
}

impl_table_provider!(VariationTableProvider);
impl_table_provider!(TranscriptTableProvider);
impl_table_provider!(RegulatoryFeatureTableProvider);
impl_table_provider!(MotifFeatureTableProvider);
