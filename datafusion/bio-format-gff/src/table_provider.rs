use crate::filter_utils::can_push_down_filter;
use crate::physical_exec::GffExec;
use crate::storage::IndexedGffReader;
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
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Constructs schema on-demand based on requested attribute fields from Python layer
/// This eliminates the need for file scanning and supports two distinct modes:
///
/// Mode 1 (Default): No specific attributes requested -> nested attributes structure
/// Mode 2 (Projection): Specific attributes requested -> flattened individual columns
fn determine_schema_on_demand(
    attr_fields: Option<Vec<String>>,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
    // Always include 8 static GFF fields
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
            // Mode 1: Default - return nested attributes for maximum flexibility
            fields.push(Field::new(
                "attributes",
                DataType::List(FieldRef::from(Box::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false), // tag must be non-null
                        Field::new("value", DataType::Utf8, true), // value may be null
                    ])),
                    true,
                )))),
                true,
            ));
            debug!(
                "GFF Schema Mode 1 (Default): 8 static fields + nested attributes = 9 columns total"
            );
        }
        Some(attrs) => {
            // Mode 2: Projection - add requested attributes as individual columns
            // All GFF attributes are treated as nullable strings for simplicity
            for attr_name in &attrs {
                fields.push(Field::new(
                    attr_name,
                    DataType::Utf8, // All GFF attributes are strings
                    true,           // Attributes can be null
                ));
            }
            debug!(
                "GFF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
    }

    // Add coordinate system metadata to schema
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    Ok(Arc::new(schema))
}

/// Apache DataFusion table provider for GFF/GFF3 bioinformatics files
///
/// Implements the TableProvider trait to enable seamless SQL queries on GFF files.
/// Features include:
/// - Projection-driven schema construction for attribute columns
/// - Filter pushdown support for efficient predicate evaluation
/// - Support for local and cloud storage (S3, GCS, Azure)
/// - Compressed and uncompressed file handling
#[derive(Clone, Debug)]
pub struct GffTableProvider {
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    thread_num: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
    /// Path to the TBI/CSI index file, if discovered
    index_path: Option<String>,
    /// Contig names extracted from the index header
    contig_names: Vec<String>,
}

impl GffTableProvider {
    /// Creates a new GFF table provider with projection-driven schema construction.
    ///
    /// The schema is built immediately based on the attr_fields parameter from Python:
    /// - None: Creates default schema with nested attributes (9 columns)
    /// - Some(attrs): Creates projection schema with flattened attributes (8 + N columns)
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the GFF file
    /// * `attr_fields` - Optional list of attribute fields to include
    /// * `thread_num` - Optional number of threads for parallel reading
    /// * `object_storage_options` - Optional cloud storage configuration
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    pub fn new(
        file_path: String,
        attr_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        // Schema construction based on Python-provided attribute fields
        let schema = determine_schema_on_demand(attr_fields.clone(), coordinate_system_zero_based)?;

        // Auto-discover index file for local BGZF-compressed files
        let storage_type = get_storage_type(file_path.clone());
        let (index_path, contig_names) = if matches!(storage_type, StorageType::LOCAL) {
            if let Some((idx_path, idx_fmt)) = discover_gff_index(&file_path) {
                debug!("Discovered GFF index: {} (format: {:?})", idx_path, idx_fmt);
                // Read contig names from the index
                let names = match IndexedGffReader::new(&file_path, &idx_path) {
                    Ok(reader) => reader.contig_names().to_vec(),
                    Err(e) => {
                        debug!("Failed to read GFF index contig names: {}", e);
                        Vec::new()
                    }
                };
                (Some(idx_path), names)
            } else {
                (None, Vec::new())
            }
        } else {
            (None, Vec::new())
        };

        debug!(
            "GffTableProvider::new - constructed schema for file: {}",
            file_path
        );

        Ok(Self {
            file_path,
            attr_fields, // Store original Python-provided fields for execution
            schema,      // Pre-constructed based on request
            thread_num,
            object_storage_options,
            coordinate_system_zero_based,
            index_path,
            contig_names,
        })
    }
}

#[async_trait]
impl TableProvider for GffTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        debug!(
            "GffTableProvider::supports_filters_pushdown with {} filters",
            filters.len()
        );

        let pushdown_support = filters
            .iter()
            .map(|expr| {
                if self.index_path.is_some() && is_genomic_coordinate_filter(expr) {
                    debug!("GFF filter can be pushed down (indexed): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else if can_push_down_filter(expr, &self.schema) {
                    debug!("GFF filter can be pushed down (record-level): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("GFF filter cannot be pushed down: {:?}", expr);
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(pushdown_support)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "GffTableProvider::scan with projection: {:?}, filters: {:?}",
            projection, filters
        );

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    // For empty projections (COUNT(*)), return an empty schema with preserved metadata
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

        // Schema is already constructed correctly in constructor based on Python categorization
        // Just apply DataFusion's standard projection logic
        let projected_schema = project_schema(&self.schema, projection);
        debug!(
            "GFF projected schema has {} columns",
            projected_schema.fields().len()
        );

        // Filter the provided filters to only include those that can be pushed down
        let pushable_filters: Vec<Expr> = filters
            .iter()
            .filter(|expr| can_push_down_filter(expr, &self.schema))
            .cloned()
            .collect();

        debug!(
            "GffTableProvider::scan - {} filters can be pushed down out of {} total",
            pushable_filters.len(),
            filters.len()
        );

        // Indexed path: use TBI/CSI index for region-based queries
        if let Some(ref index_path) = self.index_path {
            let analysis = extract_genomic_regions(filters, self.coordinate_system_zero_based);

            let regions = if !analysis.regions.is_empty() {
                analysis.regions
            } else if !self.contig_names.is_empty() {
                build_full_scan_regions(&self.contig_names)
            } else {
                Vec::new()
            };

            if !regions.is_empty() {
                let num_partitions = regions.len();

                // Collect residual filters for record-level evaluation
                let residual_filters: Vec<Expr> = filters
                    .iter()
                    .filter(|expr| can_push_down_filter(expr, &self.schema))
                    .cloned()
                    .collect();

                debug!(
                    "GFF indexed scan: {} regions, {} residual filters",
                    num_partitions,
                    residual_filters.len()
                );

                return Ok(Arc::new(GffExec {
                    cache: PlanProperties::new(
                        EquivalenceProperties::new(projected_schema.clone()),
                        Partitioning::UnknownPartitioning(num_partitions),
                        EmissionType::Final,
                        Boundedness::Bounded,
                    ),
                    file_path: self.file_path.clone(),
                    attr_fields: self.attr_fields.clone(),
                    schema: projected_schema.clone(),
                    projection: projection.cloned(),
                    filters: pushable_filters,
                    limit,
                    thread_num: self.thread_num,
                    object_storage_options: self.object_storage_options.clone(),
                    coordinate_system_zero_based: self.coordinate_system_zero_based,
                    regions: Some(regions),
                    index_path: Some(index_path.clone()),
                    residual_filters,
                }));
            }
        }

        // Fallback: sequential full scan (no index or no regions)
        Ok(Arc::new(GffExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            attr_fields: self.attr_fields.clone(),
            schema: projected_schema.clone(),
            projection: projection.cloned(),
            filters: pushable_filters,
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            regions: None,
            index_path: None,
            residual_filters: Vec::new(),
        }))
    }
}
