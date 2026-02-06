use crate::physical_exec::CramExec;
use crate::write_exec::CramWriteExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::genomic_filter::{
    build_full_scan_regions, extract_genomic_regions, is_genomic_coordinate_filter,
};
use datafusion_bio_format_core::index_utils::discover_cram_index;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;
use datafusion_bio_format_core::tag_registry::get_known_tags;
use datafusion_bio_format_core::{
    BAM_SORT_ORDER_KEY, BAM_TAG_DESCRIPTION_KEY, BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY,
    COORDINATE_SYSTEM_METADATA_KEY, extract_header_metadata,
};
use log::{debug, warn};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

fn determine_schema(
    tag_fields: &Option<Vec<String>>,
    coordinate_system_zero_based: bool,
    header_metadata: Option<HashMap<String, String>>,
) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("chrom", DataType::Utf8, true),
        Field::new("start", DataType::UInt32, true),
        Field::new("end", DataType::UInt32, true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, true),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];

    // Add tag fields if specified
    if let Some(tags) = tag_fields {
        let known_tags = get_known_tags();
        for tag in tags {
            let mut field_metadata = HashMap::new();
            field_metadata.insert(BAM_TAG_TAG_KEY.to_string(), tag.clone());

            // Use known tag definition if available, otherwise use default (String/Utf8)
            let (sam_type, arrow_type, description) = if let Some(tag_def) = known_tags.get(tag) {
                (
                    tag_def.sam_type,
                    tag_def.arrow_type.clone(),
                    tag_def.description.clone(),
                )
            } else {
                // Default for unknown tags: treat as string (most flexible)
                ('Z', DataType::Utf8, "Unknown tag".to_string())
            };

            field_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), sam_type.to_string());
            field_metadata.insert(BAM_TAG_DESCRIPTION_KEY.to_string(), description);

            fields.push(Field::new(tag.clone(), arrow_type, true).with_metadata(field_metadata));
        }
    }

    // Add coordinate system metadata to schema
    let mut metadata = header_metadata.unwrap_or_default();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    debug!("CRAM Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

/// Helper function to infer Arrow DataType from CRAM value
fn infer_type_from_cram_value(
    value: &noodles_sam::alignment::record_buf::data::field::Value,
) -> (char, DataType) {
    use noodles_sam::alignment::record_buf::data::field::Value as CramValue;

    match value {
        CramValue::Character(_) => ('A', DataType::Utf8),
        CramValue::Int8(_)
        | CramValue::UInt8(_)
        | CramValue::Int16(_)
        | CramValue::UInt16(_)
        | CramValue::Int32(_)
        | CramValue::UInt32(_) => ('i', DataType::Int32),
        CramValue::Float(_) => ('f', DataType::Float32),
        CramValue::String(_) => ('Z', DataType::Utf8),
        CramValue::Hex(_) => ('H', DataType::Utf8),
        CramValue::Array(_) => (
            'B',
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ),
    }
}

/// DataFusion table provider for CRAM files.
///
/// Allows registering CRAM files as queryable tables in DataFusion.
/// Supports both local and cloud-based CRAM files with optional
/// external reference sequences.
#[derive(Clone, Debug)]
pub struct CramTableProvider {
    file_path: String,
    schema: SchemaRef,
    reference_path: Option<String>,
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
    /// Optional list of CRAM alignment tags to include as columns
    tag_fields: Option<Vec<String>>,
    /// Whether to sort records by coordinate (chrom ASC, start ASC) on write
    sort_on_write: bool,
    /// Path to an index file (CRAI). Auto-discovered if not provided.
    index_path: Option<String>,
    /// Reference sequence names from the file header (for partitioning full scans by chromosome)
    reference_names: Vec<String>,
}

impl CramTableProvider {
    /// Creates a new CRAM table provider.
    ///
    /// # Arguments
    /// * `file_path` - Path to CRAM file (local or cloud storage URL)
    /// * `reference_path` - Optional path to FASTA reference file
    /// * `object_storage_options` - Optional cloud storage configuration
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    /// * `tag_fields` - Optional list of BAM/SAM tags to extract as columns (e.g., "NM", "MD")
    ///
    /// # Returns
    /// * `Ok(provider)` - Successfully created provider
    /// * `Err` - Failed to determine schema
    pub async fn new(
        file_path: String,
        reference_path: Option<String>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
        tag_fields: Option<Vec<String>>,
    ) -> datafusion::common::Result<Self> {
        use datafusion_bio_format_core::object_storage::{StorageType, get_storage_type};

        // Best-effort header reading: extract metadata if possible, fall back to empty
        let storage_type = get_storage_type(file_path.clone());
        let header_metadata = if matches!(storage_type, StorageType::LOCAL) {
            // For local CRAM files, read header synchronously
            use noodles_cram as cram;
            use noodles_fasta as fasta;

            match cram::io::reader::Builder::default()
                .set_reference_sequence_repository(fasta::Repository::default())
                .build_from_path(&file_path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .and_then(|mut reader| reader.read_header())
            {
                Ok(header) => extract_header_metadata(&header),
                Err(e) => {
                    warn!(
                        "Failed to read CRAM header from {}: {}, using empty metadata",
                        file_path, e
                    );
                    HashMap::new()
                }
            }
        } else {
            // For remote CRAM files, use async reader
            use crate::storage::CramReader;
            let reader = CramReader::new(
                file_path.clone(),
                reference_path.clone(),
                object_storage_options.clone(),
            )
            .await;
            extract_header_metadata(reader.get_header())
        };

        // Extract reference sequence names for partitioning
        let reference_names: Vec<String> = header_metadata
            .get(datafusion_bio_format_core::BAM_REFERENCE_SEQUENCES_KEY)
            .and_then(|json| {
                serde_json::from_str::<Vec<datafusion_bio_format_core::ReferenceSequenceMetadata>>(
                    json,
                )
                .ok()
            })
            .map(|refs| refs.into_iter().map(|r| r.name).collect())
            .unwrap_or_default();

        let schema = determine_schema(
            &tag_fields,
            coordinate_system_zero_based,
            Some(header_metadata),
        )?;

        // Auto-discover index file for local files
        let index_path = if matches!(storage_type, StorageType::LOCAL) {
            discover_cram_index(&file_path).map(|(path, fmt)| {
                debug!("Discovered CRAM index: {} (format: {:?})", path, fmt);
                path
            })
        } else {
            None
        };

        Ok(Self {
            file_path,
            schema,
            reference_path,
            object_storage_options,
            coordinate_system_zero_based,
            tag_fields,
            sort_on_write: false,
            index_path,
            reference_names,
        })
    }

    /// Creates a new CRAM table provider with automatic tag discovery.
    ///
    /// This method samples the CRAM file to discover available tags and
    /// automatically includes them in the schema. Tags found in sampled
    /// records will be readable in queries.
    ///
    /// For calculable tags (MD/NM) that may not be stored, they will be
    /// automatically included in the schema if a reference is available.
    ///
    /// # Arguments
    /// * `file_path` - Path to CRAM file (local or cloud storage URL)
    /// * `reference_path` - Optional path to FASTA reference file
    /// * `object_storage_options` - Optional cloud storage configuration
    /// * `coordinate_system_zero_based` - If true, output 0-based coordinates; if false, 1-based
    /// * `sample_size` - Number of records to sample for tag discovery (default: 100)
    ///
    /// # Returns
    /// * `Ok(provider)` - Successfully created provider with inferred schema
    /// * `Err` - Failed to read file or determine schema
    pub async fn try_new_with_inferred_schema(
        file_path: String,
        reference_path: Option<String>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
        sample_size: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        use crate::storage::CramReader;
        use futures_util::StreamExt;

        // Sample the file to discover tags
        let mut reader = CramReader::new(
            file_path.clone(),
            reference_path.clone(),
            object_storage_options.clone(),
        )
        .await;

        // Extract header metadata before borrowing reader for records
        let header_metadata = extract_header_metadata(reader.get_header());

        let mut discovered_tags: HashMap<String, (char, DataType)> = HashMap::new();
        let mut records = reader.read_records().await;
        let sample_size = sample_size.unwrap_or(100);
        let mut count = 0;

        while let Some(result) = records.next().await {
            if count >= sample_size {
                break;
            }

            match result {
                Ok(record) => {
                    let data = record.data();
                    for (tag, value) in data.iter() {
                        let tag_str =
                            format!("{}{}", tag.as_ref()[0] as char, tag.as_ref()[1] as char);
                        discovered_tags
                            .entry(tag_str)
                            .or_insert_with(|| infer_type_from_cram_value(value));
                    }
                    count += 1;
                }
                Err(_) => continue,
            }
        }

        // Always include MD/NM tags if reference is available
        // These can be calculated even if not stored
        if reference_path.is_some() {
            if !discovered_tags.contains_key("MD") {
                discovered_tags.insert("MD".to_string(), ('Z', DataType::Utf8));
            }
            if !discovered_tags.contains_key("NM") {
                discovered_tags.insert("NM".to_string(), ('i', DataType::Int32));
            }
        } else {
            // NM can be partially calculated without reference
            if !discovered_tags.contains_key("NM") {
                discovered_tags.insert("NM".to_string(), ('i', DataType::Int32));
            }
        }

        // Convert discovered tags to tag_fields format
        let tag_fields: Vec<String> = discovered_tags.keys().cloned().collect();

        // Build schema with header metadata included
        let schema = determine_schema(
            &Some(tag_fields.clone()),
            coordinate_system_zero_based,
            Some(header_metadata.clone()),
        )?;

        // Extract reference names from header metadata
        let reference_names: Vec<String> = header_metadata
            .get(datafusion_bio_format_core::BAM_REFERENCE_SEQUENCES_KEY)
            .and_then(|json| {
                serde_json::from_str::<Vec<datafusion_bio_format_core::ReferenceSequenceMetadata>>(
                    json,
                )
                .ok()
            })
            .map(|refs| refs.into_iter().map(|r| r.name).collect())
            .unwrap_or_default();

        // Auto-discover index file
        use datafusion_bio_format_core::object_storage::{StorageType, get_storage_type};
        let storage_type = get_storage_type(file_path.clone());
        let index_path = if matches!(storage_type, StorageType::LOCAL) {
            discover_cram_index(&file_path).map(|(path, _)| path)
        } else {
            None
        };

        Ok(Self {
            file_path,
            schema,
            reference_path,
            object_storage_options,
            coordinate_system_zero_based,
            tag_fields: Some(tag_fields),
            sort_on_write: false,
            index_path,
            reference_names,
        })
    }

    /// Creates a new CRAM table provider for write operations.
    ///
    /// # Arguments
    /// * `output_path` - Path to output CRAM file
    /// * `schema` - Schema defining the structure of data to write
    /// * `reference_path` - Optional path to FASTA reference file
    /// * `tag_fields` - Optional list of BAM/SAM tags to write
    /// * `coordinate_system_zero_based` - If true, input is 0-based; if false, 1-based
    /// * `sort_on_write` - If true, sort records by coordinate (chrom ASC, start ASC)
    ///   before writing and set SO:coordinate in the header. If false, write records
    ///   as-is and set SO:unsorted.
    pub fn new_for_write(
        output_path: String,
        schema: SchemaRef,
        reference_path: Option<String>,
        tag_fields: Option<Vec<String>>,
        coordinate_system_zero_based: bool,
        sort_on_write: bool,
    ) -> Self {
        Self {
            file_path: output_path,
            schema,
            reference_path,
            object_storage_options: None,
            coordinate_system_zero_based,
            tag_fields,
            sort_on_write,
            index_path: None,
            reference_names: Vec::new(),
        }
    }

    /// Discover available columns by sampling records from the CRAM file.
    ///
    /// Returns a DataFrame describing all columns including:
    /// - Core alignment fields (name, chrom, start, end, etc.)
    /// - Discovered tag fields with their types and descriptions
    ///
    /// # Arguments
    /// * `ctx` - DataFusion session context
    /// * `sample_size` - Number of records to sample (default: 100)
    pub async fn describe(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        sample_size: Option<usize>,
    ) -> Result<datafusion::prelude::DataFrame, datafusion::common::DataFusionError> {
        use crate::storage::CramReader;
        use datafusion::arrow::array::{BooleanBuilder, RecordBatch, StringBuilder};
        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::MemTable;
        use futures_util::StreamExt;

        let sample_size = sample_size.unwrap_or(100);

        // Open CRAM reader
        let mut reader = CramReader::new(
            self.file_path.clone(),
            self.reference_path.clone(),
            self.object_storage_options.clone(),
        )
        .await;

        // Discover tags by sampling records
        let mut discovered_tags: HashMap<String, (char, DataType)> = HashMap::new();
        let mut records = reader.read_records().await;
        let mut count = 0;

        while let Some(result) = records.next().await {
            if count >= sample_size {
                break;
            }

            match result {
                Ok(record) => {
                    let data = record.data();
                    for (tag, value) in data.iter() {
                        let tag_str =
                            format!("{}{}", tag.as_ref()[0] as char, tag.as_ref()[1] as char);
                        discovered_tags
                            .entry(tag_str)
                            .or_insert_with(|| infer_type_from_cram_value(value));
                    }
                    count += 1;
                }
                Err(_) => continue,
            }
        }

        // Build schema description DataFrame
        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut nullable = BooleanBuilder::new();
        let mut category = StringBuilder::new();
        let mut sam_types = StringBuilder::new();
        let mut descriptions = StringBuilder::new();

        // Core fields
        let core_fields = vec![
            ("name", "Utf8", true, "Read name"),
            ("chrom", "Utf8", true, "Chromosome/reference sequence name"),
            ("start", "UInt32", true, "Alignment start position"),
            ("end", "UInt32", true, "Alignment end position"),
            ("flags", "UInt32", false, "SAM flags"),
            ("cigar", "Utf8", false, "CIGAR string"),
            ("mapping_quality", "UInt32", true, "Mapping quality"),
            ("mate_chrom", "Utf8", true, "Mate chromosome"),
            ("mate_start", "UInt32", true, "Mate start position"),
            ("sequence", "Utf8", false, "Read sequence"),
            ("quality_scores", "Utf8", false, "Quality scores"),
        ];

        for (name, dtype, is_null, desc) in core_fields {
            column_names.append_value(name);
            data_types.append_value(dtype);
            nullable.append_value(is_null);
            category.append_value("core");
            sam_types.append_value("");
            descriptions.append_value(desc);
        }

        // Tag fields
        let known_tags = get_known_tags();
        for (tag_name, (sam_type, arrow_type)) in discovered_tags {
            column_names.append_value(&tag_name);
            data_types.append_value(format!("{:?}", arrow_type));
            nullable.append_value(true);
            category.append_value("tag");
            sam_types.append_value(sam_type.to_string());

            let desc = known_tags
                .get(&tag_name)
                .map(|t| t.description.clone())
                .unwrap_or_else(|| "Unknown tag".to_string());
            descriptions.append_value(desc);
        }

        // Create RecordBatch
        let schema = Arc::new(Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("column_name", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("data_type", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("nullable", DataType::Boolean, false),
            datafusion::arrow::datatypes::Field::new("category", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("sam_type", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("description", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(column_names.finish()),
                Arc::new(data_types.finish()),
                Arc::new(nullable.finish()),
                Arc::new(category.finish()),
                Arc::new(sam_types.finish()),
                Arc::new(descriptions.finish()),
            ],
        )?;

        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.read_table(Arc::new(table))
    }
}

#[async_trait]
impl TableProvider for CramTableProvider {
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
                // Genomic coordinate filters get Inexact when index is available
                if self.index_path.is_some() && is_genomic_coordinate_filter(expr) {
                    debug!("CRAM filter can be pushed down (indexed): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else if can_push_down_record_filter(expr, &self.schema) {
                    debug!("CRAM filter can be pushed down (record-level): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("CRAM filter cannot be pushed down: {:?}", expr);
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
        debug!("CramTableProvider::scan");

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    // For empty projections (COUNT(*)), use a dummy field with preserved metadata
                    Arc::new(Schema::new_with_metadata(
                        vec![Field::new("dummy", DataType::Null, true)],
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

        let schema = project_schema(&self.schema, projection);

        // Determine regions and partitioning when index is available
        if let Some(ref index_path) = self.index_path {
            let analysis = extract_genomic_regions(filters, self.coordinate_system_zero_based);

            let regions = if !analysis.regions.is_empty() {
                // Use extracted regions from filters
                analysis.regions
            } else if !self.reference_names.is_empty() {
                // Full scan: partition by chromosome for parallel reading
                build_full_scan_regions(&self.reference_names)
            } else {
                Vec::new()
            };

            if !regions.is_empty() {
                let num_partitions = regions.len();

                // Collect filters for record-level evaluation
                let record_filters: Vec<Expr> = filters
                    .iter()
                    .filter(|expr| can_push_down_record_filter(expr, &self.schema))
                    .cloned()
                    .collect();

                debug!(
                    "CRAM indexed scan: {} regions, {} record-level filters",
                    num_partitions,
                    record_filters.len()
                );

                return Ok(Arc::new(CramExec {
                    cache: PlanProperties::new(
                        EquivalenceProperties::new(schema.clone()),
                        Partitioning::UnknownPartitioning(num_partitions),
                        EmissionType::Final,
                        Boundedness::Bounded,
                    ),
                    file_path: self.file_path.clone(),
                    schema: schema.clone(),
                    projection: projection.cloned(),
                    limit,
                    reference_path: self.reference_path.clone(),
                    object_storage_options: self.object_storage_options.clone(),
                    coordinate_system_zero_based: self.coordinate_system_zero_based,
                    tag_fields: self.tag_fields.clone(),
                    regions: Some(regions),
                    index_path: Some(index_path.clone()),
                    residual_filters: record_filters,
                }));
            }
        }

        // Fallback: sequential full scan (no index or no regions)
        Ok(Arc::new(CramExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            limit,
            reference_path: self.reference_path.clone(),
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            tag_fields: self.tag_fields.clone(),
            regions: None,
            index_path: None,
            residual_filters: Vec::new(),
        }))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::DataFusionError;

        if insert_op != InsertOp::Overwrite {
            return Err(DataFusionError::NotImplemented(
                "CRAM insert_into only supports OVERWRITE mode (INSERT OVERWRITE)".to_string(),
            ));
        }

        // Extract tag fields from schema metadata
        let mut tag_fields: Vec<String> = self
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                field
                    .metadata()
                    .get(BAM_TAG_TAG_KEY)
                    .map(|tag| tag.to_string())
            })
            .collect();

        // Merge with explicitly provided tag_fields (if any)
        // This ensures tags passed to new_for_write are honored even without metadata
        if let Some(explicit_tags) = &self.tag_fields {
            for tag in explicit_tags {
                if !tag_fields.contains(tag) {
                    tag_fields.push(tag.clone());
                }
            }
        }

        // Build metadata overrides for sort order
        let mut schema_metadata_overrides = HashMap::new();
        if self.sort_on_write {
            schema_metadata_overrides
                .insert(BAM_SORT_ORDER_KEY.to_string(), "coordinate".to_string());
        } else {
            schema_metadata_overrides
                .insert(BAM_SORT_ORDER_KEY.to_string(), "unsorted".to_string());
        }

        // Create write execution plan (SortExec wrapping happens at execution time
        // inside CramWriteExec::execute to avoid DataFusion's optimizer stripping it)
        Ok(Arc::new(CramWriteExec::new(
            input,
            self.file_path.clone(),
            self.reference_path.clone(),
            tag_fields,
            self.coordinate_system_zero_based,
            schema_metadata_overrides,
            self.sort_on_write,
        )))
    }
}
