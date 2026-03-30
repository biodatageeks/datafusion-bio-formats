use crate::discovery::{
    discover_regulatory_files, discover_transcript_files, discover_variation_files,
    extract_distinct_chroms,
};
use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::filter::{extract_simple_predicate, is_pushdown_supported};
use crate::info::CacheInfo;
use crate::physical_exec::{
    EnsemblCacheExec, EnsemblCacheExecConfig, file_matches_chrom_predicate,
    variation_file_matches_predicate,
};
use crate::schema::{
    exon_schema, motif_feature_schema, regulatory_feature_schema, transcript_schema,
    translation_schema, variation_schema,
};
use crate::variation::detect_region_size;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use std::any::Any;
use std::path::Path;
use std::sync::Arc;

/// Configuration for Ensembl cache table providers.
#[derive(Debug, Clone)]
pub struct EnsemblCacheOptions {
    /// Root directory of a single Ensembl VEP cache.
    pub cache_root: String,
    /// If true, expose genomic coordinates as 0-based half-open.
    /// If false, expose genomic coordinates as 1-based closed (VEP native).
    ///
    /// Value is also exposed in Arrow schema metadata under
    /// `bio.coordinate_system_zero_based`.
    pub coordinate_system_zero_based: bool,
    /// Optional target partition count for parallel file scanning.
    ///
    /// If not set, the provider uses DataFusion session target partitions.
    pub target_partitions: Option<usize>,
    /// Optional batch-size override used by the execution plan.
    pub batch_size_hint: Option<usize>,
    /// Maximum partition parallelism for Storable-format entities (transcript,
    /// regulatory). Each partition holds a full streaming parser, so memory
    /// scales linearly with the number of concurrent partitions. Defaults to 4
    /// when unset.
    pub max_storable_partitions: Option<usize>,
    /// When true, variation files are assigned to partitions in genomic order
    /// (consecutive chunks) and per-partition output ordering `(chrom, start)`
    /// is declared in the execution plan.  This allows DataFusion to replace a
    /// full SortExec with a SortPreservingMergeExec when the caller adds
    /// `ORDER BY chrom, start`, avoiding a redundant re-sort.
    ///
    /// Defaults to true for variation entities.
    pub preserve_sort_order: Option<bool>,
}

impl EnsemblCacheOptions {
    /// Creates new options with defaults.
    ///
    /// Defaults:
    /// - `coordinate_system_zero_based = false` (VEP cache semantics)
    /// - `target_partitions = None` (use DataFusion session target partitions)
    /// - `batch_size_hint = None` (use DataFusion session batch size)
    pub fn new(cache_root: impl Into<String>) -> Self {
        Self {
            cache_root: cache_root.into(),
            coordinate_system_zero_based: false,
            target_partitions: None,
            batch_size_hint: None,
            max_storable_partitions: None,
            preserve_sort_order: None,
        }
    }
}

/// Schema metadata key for the JSON-encoded list of chromosomes discovered
/// from VEP cache file paths (e.g. `["1","2","22","X"]`).
///
/// The value is `None`/absent when chromosome names could not be reliably
/// determined from file paths alone (e.g. tabix caches with a single
/// `all_vars.gz` in a non-chromosome directory).
pub const VEP_CHROMOSOMES_METADATA_KEY: &str = "bio.vep.chromosomes";

#[derive(Debug, Clone)]
struct ProviderInner {
    kind: EnsemblEntityKind,
    options: EnsemblCacheOptions,
    cache_info: CacheInfo,
    schema: SchemaRef,
    files: Vec<std::path::PathBuf>,
    variation_region_size: Option<i64>,
    /// Distinct chromosomes extracted from file paths, if available.
    chroms: Option<Vec<String>>,
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
            EnsemblEntityKind::Exon => {
                validate_serializer(&cache_info)?;
                let schema = exon_schema(&cache_info, options.coordinate_system_zero_based);
                let files = discover_transcript_files(cache_root)?;
                (schema, files, None)
            }
            EnsemblEntityKind::Translation => {
                validate_serializer(&cache_info)?;
                let schema = translation_schema(&cache_info, options.coordinate_system_zero_based);
                let files = discover_transcript_files(cache_root)?;
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

        // Extract distinct chromosomes from file paths (best-effort).
        let chroms = extract_distinct_chroms(&files, kind);

        // If chromosomes were resolved, store them in schema metadata so that
        // downstream consumers (parquet writers, SQL information_schema, etc.)
        // can access them without a full scan.
        let schema = if let Some(ref chroms) = chroms {
            let mut metadata = schema.metadata().clone();
            if let Ok(json) = serde_json::to_string(chroms) {
                metadata.insert(VEP_CHROMOSOMES_METADATA_KEY.to_string(), json);
            }
            Arc::new(Schema::new_with_metadata(
                schema.fields().to_vec(),
                metadata,
            ))
        } else {
            schema
        };

        Ok(Self {
            kind,
            options,
            cache_info,
            schema,
            files,
            variation_region_size,
            chroms,
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

    /// Returns the distinct chromosomes derived from file paths, if available.
    ///
    /// Returns `None` when chromosome names could not be reliably extracted
    /// from the directory structure (e.g. tabix caches with a single
    /// `all_vars.gz` in a non-chromosome directory). In that case a full
    /// scan is needed to discover chromosomes.
    fn chromosomes(&self) -> Option<&[String]> {
        self.chroms.as_deref()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection);
        let predicate = extract_simple_predicate(filters);

        // Prune files by predicate BEFORE partition assignment so that
        // per-chromosome queries distribute matching files across all
        // partitions instead of concentrating them in one.
        let files: Vec<std::path::PathBuf> = self
            .files
            .iter()
            .filter(|path| {
                if self.kind == EnsemblEntityKind::Variation {
                    variation_file_matches_predicate(path, &predicate)
                } else {
                    file_matches_chrom_predicate(path, &predicate, self.kind)
                }
            })
            .cloned()
            .collect();

        let base_partitions = match self.options.target_partitions {
            Some(target) => target,
            None => state.config().target_partitions(),
        };
        // Storable entities stream-parse entire files with significant per-
        // partition memory (alias refs, SValue trees).  Cap concurrency to
        // prevent OOM regardless of how target_partitions was set.
        let use_storable = self.kind != EnsemblEntityKind::Variation
            && self.cache_info.serializer_type.as_deref() == Some("storable");
        let requested_partitions = if use_storable {
            let cap = self.options.max_storable_partitions.unwrap_or(4);
            base_partitions.min(cap)
        } else {
            base_partitions
        }
        .max(1);
        let num_partitions = requested_partitions.min(files.len().max(1));

        // Default: preserve sort order for variation (already sorted in
        // genomic order by discovery), disable for other entities.
        let preserve_sort_order = self
            .options
            .preserve_sort_order
            .unwrap_or(self.kind == EnsemblEntityKind::Variation);

        let single_chrom_filter = predicate.chrom.clone();

        // For tabix variation caches with few files (typically 1 per chrom),
        // split the bgzf file into byte-range partitions for intra-file
        // parallelism.  This is the key optimization for VEP 110+ caches.
        //
        // When a chrom predicate is present AND a .tbi index exists, use
        // the index to restrict partitioning to only the target chromosome's
        // byte range.  This avoids decompressing data for other chromosomes.
        let is_tabix = self.kind == EnsemblEntityKind::Variation
            && self.cache_info.var_type.as_deref() == Some("tabix");
        let bgzf_partitions = if is_tabix && files.len() == 1 && requested_partitions > 1 {
            let data_file = &files[0];
            let tbi_path = data_file.with_extension("gz.tbi");
            let csi_path = data_file.with_extension("gz.csi");
            let index_path = if tbi_path.exists() {
                Some(tbi_path)
            } else if csi_path.exists() {
                Some(csi_path)
            } else {
                None
            };

            // Try tabix-index-aware partitioning first (chrom filter + index exists)
            let parts = if let Some(ref chrom) = single_chrom_filter
                && let Some(ref idx_path) = index_path
            {
                match crate::tabix_reader::tabix_chrom_byte_range(idx_path, chrom) {
                    Ok(Some((start, end))) => {
                        crate::tabix_reader::compute_bgzf_partitions_in_range(
                            data_file,
                            start,
                            end,
                            requested_partitions,
                        )
                        .ok()
                    }
                    _ => None,
                }
            } else {
                None
            };

            // Fall back to whole-file partitioning
            let parts = parts.unwrap_or_else(|| {
                crate::tabix_reader::compute_bgzf_partitions(data_file, requested_partitions)
                    .unwrap_or_default()
            });

            if parts.len() > 1 {
                Some(
                    parts
                        .into_iter()
                        .map(|p| (data_file.clone(), p))
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        } else {
            None
        };

        let effective_partitions = if let Some(ref bp) = bgzf_partitions {
            bp.len()
        } else {
            num_partitions
        };

        let exec = Arc::new(EnsemblCacheExec::new(EnsemblCacheExecConfig {
            kind: self.kind,
            cache_info: self.cache_info.clone(),
            files,
            schema: projected_schema.clone(),
            predicate,
            limit,
            variation_region_size: self.variation_region_size,
            batch_size_hint: self.options.batch_size_hint,
            coordinate_system_zero_based: self.options.coordinate_system_zero_based,
            num_partitions: effective_partitions,
            preserve_sort_order,
            single_chrom_filter: single_chrom_filter.clone(),
            bgzf_partitions,
        }));

        // When sort order is preserved, a single chrom is filtered, and
        // there are multiple partitions, wrap with SortPreservingMergeExec
        // to merge the pre-sorted partition streams by (start ASC).
        // This guarantees parallel execution instead of relying on the
        // optimizer to choose merge over a full single-threaded SortExec.
        if preserve_sort_order
            && single_chrom_filter.is_some()
            && effective_partitions > 1
            && let Ok(si) = projected_schema.index_of("start")
        {
            let sort_exprs = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("start", si)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )])
            .expect("sort expressions should not be empty");

            return Ok(Arc::new(SortPreservingMergeExec::new(sort_exprs, exec)));
        }

        Ok(exec)
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
            "Unknown serializer type in info.txt: {serializer}"
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
        Some(indices) => Arc::new(schema.project(indices).expect("valid projection indices")),
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
            EnsemblEntityKind::Exon => Ok(Arc::new(ExonTableProvider::new(options)?)),
            EnsemblEntityKind::Translation => Ok(Arc::new(TranslationTableProvider::new(options)?)),
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

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
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

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
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

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
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

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
    }
}

/// DataFusion table provider for individual exon objects from VEP transcript cache.
#[derive(Debug, Clone)]
pub struct ExonTableProvider {
    inner: ProviderInner,
}

impl ExonTableProvider {
    /// Creates an exon provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::Exon, options)?,
        })
    }

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
    }
}

/// DataFusion table provider for translation objects from VEP transcript cache.
#[derive(Debug, Clone)]
pub struct TranslationTableProvider {
    inner: ProviderInner,
}

impl TranslationTableProvider {
    /// Creates a translation provider from cache options.
    pub fn new(options: EnsemblCacheOptions) -> Result<Self> {
        Ok(Self {
            inner: ProviderInner::new(EnsemblEntityKind::Translation, options)?,
        })
    }

    /// Returns the distinct chromosomes derived from cache file paths, or
    /// `None` if they could not be determined without a full scan.
    pub fn chromosomes(&self) -> Option<&[String]> {
        self.inner.chromosomes()
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
                state: &dyn Session,
                projection: Option<&Vec<usize>>,
                filters: &[Expr],
                limit: Option<usize>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                self.inner.scan(state, projection, filters, limit).await
            }
        }
    };
}

impl_table_provider!(VariationTableProvider);
impl_table_provider!(TranscriptTableProvider);
impl_table_provider!(RegulatoryFeatureTableProvider);
impl_table_provider!(MotifFeatureTableProvider);
impl_table_provider!(ExonTableProvider);
impl_table_provider!(TranslationTableProvider);
