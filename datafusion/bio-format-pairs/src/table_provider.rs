//! Apache DataFusion table provider for Pairs (Hi-C) files.
//!
//! Implements the TableProvider trait to enable SQL queries on Pairs files.
//! Supports filter pushdown for chr1/pos1 (index-based) and chr2/pos2 (residual).

use crate::filter_utils::{extract_pairs_genomic_regions, is_pairs_index_filter};
use crate::header::{PairsHeader, columns_to_arrow_fields};
use crate::physical_exec::PairsExec;
use crate::storage::IndexedPairsReader;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::genomic_filter::build_full_scan_regions;
use datafusion_bio_format_core::index_utils::discover_pairs_index;
use datafusion_bio_format_core::metadata::{
    PAIRS_CHROMSIZES_KEY, PAIRS_COLUMNS_KEY, PAIRS_FORMAT_VERSION_KEY, PAIRS_GENOME_ASSEMBLY_KEY,
    PAIRS_SHAPE_KEY, PAIRS_SORTED_KEY,
};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::balance_partitions;
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Build the Arrow schema from a parsed Pairs header.
fn determine_schema(
    header: &PairsHeader,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
    let fields: Vec<Field> = columns_to_arrow_fields(&header.columns);

    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    metadata.insert(
        PAIRS_FORMAT_VERSION_KEY.to_string(),
        header.format_version.clone(),
    );
    metadata.insert(
        PAIRS_COLUMNS_KEY.to_string(),
        serde_json::to_string(&header.columns).unwrap_or_default(),
    );

    if !header.chromsizes.is_empty() {
        metadata.insert(
            PAIRS_CHROMSIZES_KEY.to_string(),
            serde_json::to_string(&header.chromsizes).unwrap_or_default(),
        );
    }
    if let Some(ref sorted) = header.sorted {
        metadata.insert(PAIRS_SORTED_KEY.to_string(), sorted.clone());
    }
    if let Some(ref shape) = header.shape {
        metadata.insert(PAIRS_SHAPE_KEY.to_string(), shape.clone());
    }
    if let Some(ref genome) = header.genome_assembly {
        metadata.insert(PAIRS_GENOME_ASSEMBLY_KEY.to_string(), genome.clone());
    }

    let schema = Schema::new_with_metadata(fields, metadata);
    Ok(Arc::new(schema))
}

/// Apache DataFusion table provider for Pairs (Hi-C) files.
///
/// Features:
/// - Tabix index support for chr1/pos1 region queries
/// - Pairix (.px2) index detection
/// - Filter pushdown (index for chr1/pos1, residual for chr2/pos2)
/// - Projection pushdown
/// - BGZF and plain text file support
#[derive(Clone, Debug)]
pub struct PairsTableProvider {
    file_path: String,
    schema: SchemaRef,
    header: PairsHeader,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    /// Path to the TBI/CSI/px2 index file, if discovered
    index_path: Option<String>,
    /// Contig names extracted from the index header
    contig_names: Vec<String>,
}

impl PairsTableProvider {
    /// Creates a new Pairs table provider.
    ///
    /// Reads the file header to determine the schema and auto-discovers
    /// any companion index file (TBI, CSI, px2).
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the Pairs file
    /// * `object_storage_options` - Optional cloud storage configuration
    /// * `coordinate_system_zero_based` - If true, positions are 0-based; if false, 1-based
    pub fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        let storage_type = get_storage_type(file_path.clone());
        if !matches!(storage_type, StorageType::LOCAL) {
            return Err(datafusion::common::DataFusionError::NotImplemented(
                format!(
                    "Remote Pairs file reading is not yet supported: {}",
                    file_path
                ),
            ));
        }

        let storage_opts = object_storage_options.clone().unwrap_or_default();

        // Read header from local file
        let header =
            crate::storage::get_local_pairs_header(&file_path, &storage_opts).map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Failed to read Pairs header from {}: {}",
                    file_path, e
                ))
            })?;

        let schema = determine_schema(&header, coordinate_system_zero_based)?;

        // Auto-discover index file
        let (index_path, contig_names) =
            if let Some((idx_path, idx_fmt)) = discover_pairs_index(&file_path) {
                debug!(
                    "Discovered Pairs index: {} (format: {:?})",
                    idx_path, idx_fmt
                );
                let names = match IndexedPairsReader::new(&file_path, &idx_path) {
                    Ok(reader) => reader.contig_names().to_vec(),
                    Err(e) => {
                        debug!("Failed to read Pairs index contig names: {}", e);
                        Vec::new()
                    }
                };
                (Some(idx_path), names)
            } else {
                (None, Vec::new())
            };

        debug!(
            "PairsTableProvider::new - {} columns, index={}, contigs={}",
            header.columns.len(),
            index_path.is_some(),
            contig_names.len()
        );

        Ok(Self {
            file_path,
            schema,
            header,
            object_storage_options,
            coordinate_system_zero_based,
            index_path,
            contig_names,
        })
    }
}

#[async_trait]
impl TableProvider for PairsTableProvider {
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
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let pushdown_support = filters
            .iter()
            .map(|expr| {
                if self.index_path.is_some() && is_pairs_index_filter(expr) {
                    debug!("Pairs filter can be pushed down (indexed): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else if can_push_down_record_filter(expr, &self.schema) {
                    debug!("Pairs filter can be pushed down (record-level): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("Pairs filter cannot be pushed down: {:?}", expr);
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(pushdown_support)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "PairsTableProvider::scan - {} filters, index={}, contigs={:?}",
            filters.len(),
            self.index_path.is_some(),
            self.contig_names
        );

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    let empty_fields: Vec<Field> = vec![];
                    Arc::new(Schema::new_with_metadata(
                        empty_fields,
                        schema.metadata().clone(),
                    ))
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new_with_metadata(
                        projected_fields,
                        schema.metadata().clone(),
                    ))
                }
                None => schema.clone(),
            }
        }

        let projected_schema = project_schema(&self.schema, projection);

        // Indexed path
        if let Some(ref index_path) = self.index_path {
            let analysis =
                extract_pairs_genomic_regions(filters, self.coordinate_system_zero_based);

            let regions = if !analysis.regions.is_empty() {
                debug!(
                    "Pairs scan: using {} filter-derived region(s)",
                    analysis.regions.len()
                );
                analysis.regions
            } else if !self.contig_names.is_empty() {
                debug!(
                    "Pairs scan: no genomic filters, full-scan on {} contig(s)",
                    self.contig_names.len()
                );
                build_full_scan_regions(&self.contig_names)
            } else {
                debug!("Pairs scan: no index regions, falling back to sequential");
                Vec::new()
            };

            if !regions.is_empty() {
                let target_partitions = state.config().target_partitions();
                let contig_lengths: Vec<u64> = self
                    .contig_names
                    .iter()
                    .map(|name| {
                        self.header
                            .chromsizes
                            .iter()
                            .find(|cs| cs.name == *name)
                            .map_or(0, |cs| cs.length)
                    })
                    .collect();
                let estimates = crate::storage::estimate_sizes_from_tbi(
                    index_path,
                    &regions,
                    &self.contig_names,
                    &contig_lengths,
                );
                let assignments = balance_partitions(estimates, target_partitions);
                let num_partitions = assignments.len();

                // Collect all residual filters (chr2/pos2 + record-level)
                let residual_filters: Vec<Expr> = analysis.residual_filters;

                debug!(
                    "Pairs indexed scan: {} partitions, {} residual filters",
                    num_partitions,
                    residual_filters.len()
                );

                return Ok(Arc::new(PairsExec {
                    cache: PlanProperties::new(
                        EquivalenceProperties::new(projected_schema.clone()),
                        Partitioning::UnknownPartitioning(num_partitions),
                        EmissionType::Final,
                        Boundedness::Bounded,
                    ),
                    file_path: self.file_path.clone(),
                    schema: projected_schema,
                    columns: self.header.columns.clone(),
                    projection: projection.cloned(),
                    limit,
                    object_storage_options: self.object_storage_options.clone(),
                    coordinate_system_zero_based: self.coordinate_system_zero_based,
                    partition_assignments: Some(assignments),
                    index_path: Some(index_path.clone()),
                    residual_filters,
                }));
            }
        }

        // Fallback: sequential full scan
        Ok(Arc::new(PairsExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: projected_schema,
            columns: self.header.columns.clone(),
            projection: projection.cloned(),
            limit,
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            partition_assignments: None,
            index_path: None,
            residual_filters: Vec::new(),
        }))
    }
}
