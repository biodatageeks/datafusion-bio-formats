use crate::filter_utils::can_push_down_filter;
use crate::physical_exec::GtfExec;
use crate::storage::IndexedGtfReader;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::genomic_filter::{
    build_full_scan_regions, extract_genomic_regions, is_genomic_coordinate_filter,
};
use datafusion_bio_format_core::index_utils::discover_gff_index;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::balance_partitions;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Constructs schema on-demand based on requested attribute fields.
///
/// When `coordinate_system_zero_based` is true, `start` is converted from 1-based to
/// 0-based (via `saturating_sub(1)`) but `end` stays unchanged, following the BED
/// half-open `[start, end)` convention used across the project's format crates.
fn determine_schema_on_demand(
    attr_fields: Option<Vec<String>>,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::UInt32, true),
    ];

    match attr_fields {
        None => {
            fields.push(Field::new(
                "attributes",
                DataType::List(FieldRef::from(Box::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    true,
                )))),
                true,
            ));
            debug!(
                "GTF Schema Mode 1 (Default): 8 static fields + nested attributes = 9 columns total"
            );
        }
        Some(attrs) => {
            for attr_name in &attrs {
                fields.push(Field::new(attr_name, DataType::Utf8, true));
            }
            debug!(
                "GTF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
    }

    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    Ok(Arc::new(schema))
}

/// Apache DataFusion table provider for GTF bioinformatics files
///
/// Implements the TableProvider trait to enable seamless SQL queries on GTF files.
/// Features include:
/// - Projection-driven schema construction for attribute columns
/// - Filter pushdown support for efficient predicate evaluation
/// - BGZF compression and tabix index support for region-based queries
/// - Compressed and uncompressed file handling
#[derive(Clone, Debug)]
pub struct GtfTableProvider {
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    /// Optional cloud storage configuration for remote files (S3, GCS, Azure)
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
    /// Path to the TBI/CSI index file, if discovered
    index_path: Option<String>,
    /// Contig names extracted from the index header
    contig_names: Vec<String>,
}

impl GtfTableProvider {
    /// Creates a new GTF table provider with projection-driven schema construction.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the GTF file
    /// * `attr_fields` - Optional list of attribute fields to include
    /// * `object_storage_options` - Optional cloud storage configuration (S3, GCS, Azure)
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    pub fn new(
        file_path: String,
        attr_fields: Option<Vec<String>>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema_on_demand(attr_fields.clone(), coordinate_system_zero_based)?;

        // Auto-discover index file for local BGZF-compressed GTF files
        // Reuses GFF index discovery since GTF uses the same tabix format
        let storage_type = get_storage_type(file_path.clone());
        let (index_path, contig_names) = if matches!(storage_type, StorageType::LOCAL) {
            if let Some((idx_path, idx_fmt)) = discover_gff_index(&file_path) {
                debug!("Discovered GTF index: {idx_path} (format: {idx_fmt:?})");
                match IndexedGtfReader::new(&file_path, &idx_path) {
                    Ok(reader) => (Some(idx_path), reader.contig_names().to_vec()),
                    Err(e) => {
                        use datafusion_bio_format_core::index_utils::IndexFormat;
                        if matches!(idx_fmt, IndexFormat::CSI) {
                            log::warn!(
                                "CSI index found at {idx_path} but only TBI is currently supported; \
                                 falling back to sequential scan"
                            );
                        } else {
                            log::warn!(
                                "Failed to open indexed GTF reader for {idx_path}, \
                                 falling back to sequential scan: {e}"
                            );
                        }
                        (None, Vec::new())
                    }
                }
            } else {
                (None, Vec::new())
            }
        } else {
            (None, Vec::new())
        };

        debug!("GtfTableProvider::new - constructed schema for file: {file_path}");

        Ok(Self {
            file_path,
            attr_fields,
            schema,
            object_storage_options,
            coordinate_system_zero_based,
            index_path,
            contig_names,
        })
    }
}

#[async_trait]
impl TableProvider for GtfTableProvider {
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
                if self.index_path.is_some() && is_genomic_coordinate_filter(expr) {
                    debug!("GTF filter can be pushed down (indexed): {expr:?}");
                    TableProviderFilterPushDown::Inexact
                } else if can_push_down_filter(expr, &self.schema) {
                    debug!("GTF filter can be pushed down (record-level): {expr:?}");
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("GTF filter cannot be pushed down: {expr:?}");
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
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
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

        let pushable_filters: Vec<Expr> = filters
            .iter()
            .filter(|expr| can_push_down_filter(expr, &self.schema))
            .cloned()
            .collect();

        // Indexed path: use TBI/CSI index for region-based queries
        if let Some(ref index_path) = self.index_path {
            let analysis = extract_genomic_regions(filters, self.coordinate_system_zero_based);

            let regions = if !analysis.regions.is_empty() {
                debug!(
                    "GTF scan: using {} filter-derived region(s)",
                    analysis.regions.len()
                );
                analysis.regions
            } else if !self.contig_names.is_empty() {
                debug!(
                    "GTF scan: no genomic filters pushed down, using full-scan on {} contig(s)",
                    self.contig_names.len()
                );
                build_full_scan_regions(&self.contig_names)
            } else {
                Vec::new()
            };

            if !regions.is_empty() {
                let target_partitions = state.config().target_partitions();
                let estimates = crate::storage::estimate_sizes_from_tbi(
                    index_path,
                    &regions,
                    &self.contig_names,
                    &[],
                );
                let assignments = balance_partitions(estimates, target_partitions);
                let num_partitions = assignments.len();

                let residual_filters: Vec<Expr> = filters
                    .iter()
                    .filter(|expr| can_push_down_filter(expr, &self.schema))
                    .cloned()
                    .collect();

                debug!(
                    "GTF indexed scan: {} partitions (from {} regions, target {}), {} residual filters",
                    num_partitions,
                    assignments.iter().map(|a| a.regions.len()).sum::<usize>(),
                    target_partitions,
                    residual_filters.len()
                );

                return Ok(Arc::new(GtfExec {
                    cache: PlanProperties::new(
                        EquivalenceProperties::new(projected_schema.clone()),
                        Partitioning::UnknownPartitioning(num_partitions),
                        EmissionType::Final,
                        Boundedness::Bounded,
                    ),
                    file_path: self.file_path.clone(),
                    attr_fields: self.attr_fields.clone(),
                    schema: projected_schema,
                    projection: projection.cloned(),
                    filters: pushable_filters,
                    coordinate_system_zero_based: self.coordinate_system_zero_based,
                    object_storage_options: self.object_storage_options.clone(),
                    partition_assignments: Some(assignments),
                    index_path: Some(index_path.clone()),
                    residual_filters,
                }));
            }
        }

        // Fallback: sequential full scan (no index or no regions)
        Ok(Arc::new(GtfExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            attr_fields: self.attr_fields.clone(),
            schema: projected_schema,
            projection: projection.cloned(),
            filters: pushable_filters,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            object_storage_options: self.object_storage_options.clone(),
            partition_assignments: None,
            index_path: None,
            residual_filters: Vec::new(),
        }))
    }
}
