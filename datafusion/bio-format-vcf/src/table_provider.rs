use crate::physical_exec::VcfExec;
use crate::storage::get_header;
use crate::write_exec::VcfWriteExec;
use crate::writer::VcfCompressionType;
use async_trait::async_trait;
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::genomic_filter::{
    build_full_scan_regions, extract_genomic_regions, is_genomic_coordinate_filter,
};
use datafusion_bio_format_core::index_utils::discover_vcf_index;
use datafusion_bio_format_core::metadata::{
    AltAlleleMetadata, ContigMetadata, FilterMetadata, VCF_ALTERNATIVE_ALLELES_KEY,
    VCF_CONTIGS_KEY, VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY,
    VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY, VCF_FILE_FORMAT_KEY, VCF_FILTERS_KEY,
    VCF_SAMPLE_NAMES_KEY, to_json_string,
};
use datafusion_bio_format_core::partition_balancer::balance_partitions;
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;
use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Constraints;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures::executor::block_on;
use log::debug;
use noodles_vcf::header::Formats;
use noodles_vcf::header::Infos;
use noodles_vcf::header::record::value::map::format::{Number as FormatNumber, Type as FormatType};
use noodles_vcf::header::record::value::map::info::{Number, Type as InfoType};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Determines the Arrow schema for a VCF file by reading its header.
///
/// This function extracts VCF header information and stores it in Arrow schema metadata
/// for round-trip preservation:
/// - Schema-level metadata: file format, filters, contigs, ALT alleles, sample names (as JSON)
/// - Field-level metadata: INFO/FORMAT field descriptions, types, and numbers using `bio.vcf.field.*` keys
/// - Coordinate system metadata: `bio.coordinate_system_zero_based`
///
/// # Arguments
///
/// * `file_path` - Path to the VCF file
/// * `info_fields` - Optional list of INFO fields to include (if None, all are included)
/// * `format_fields` - Optional list of FORMAT fields to include (if None, all are included)
/// * `object_storage_options` - Configuration for cloud storage access
/// * `coordinate_system_zero_based` - If true, coordinates are 0-based half-open; if false, 1-based closed
///
/// # Returns
///
/// A tuple of (Arrow SchemaRef, sample names) representing the VCF table structure
/// and the sample names from the header.
///
/// # Errors
///
/// Returns an error if the file cannot be read or the header is invalid
async fn determine_schema_from_header(
    file_path: &str,
    info_fields: &Option<Vec<String>>,
    format_fields: &Option<Vec<String>>,
    object_storage_options: &Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<(SchemaRef, Vec<String>)> {
    let header = get_header(file_path.to_string(), object_storage_options.clone()).await?;
    let header_infos = header.infos();
    let header_formats = header.formats();
    let sample_names: Vec<String> = header
        .sample_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Extract header metadata for schema storage
    let file_format_obj = header.file_format();
    let file_format = format!(
        "VCFv{}.{}",
        file_format_obj.major(),
        file_format_obj.minor()
    );

    let filters: Vec<FilterMetadata> = header
        .filters()
        .iter()
        .map(|(id, filter)| FilterMetadata {
            id: id.to_string(),
            description: filter.description().to_string(),
        })
        .collect();

    let contigs: Vec<ContigMetadata> = header
        .contigs()
        .iter()
        .map(|(id, contig)| ContigMetadata {
            id: id.to_string(),
            length: contig.length().map(|l| l as u64),
            metadata: HashMap::new(), // Extract additional metadata if available
        })
        .collect();

    let alt_alleles: Vec<AltAlleleMetadata> = header
        .alternative_alleles()
        .iter()
        .map(|(id, alt)| AltAlleleMetadata {
            id: id.to_string(),
            description: alt.description().to_string(),
        })
        .collect();

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    if let Some(infos) = info_fields {
        for tag in infos {
            let dtype = info_to_arrow_type(header_infos, tag);
            let info = header_infos.get(tag.as_str()).unwrap();
            let nullable = is_nullable(&info.ty());
            // Store VCF header metadata in field metadata for round-trip preservation
            let mut field_metadata = HashMap::new();
            field_metadata.insert(
                VCF_FIELD_DESCRIPTION_KEY.to_string(),
                info.description().to_string(),
            );
            field_metadata.insert(
                VCF_FIELD_TYPE_KEY.to_string(),
                info_type_to_string(&info.ty()),
            );
            field_metadata.insert(
                VCF_FIELD_NUMBER_KEY.to_string(),
                info_number_to_string(info.number()),
            );
            field_metadata.insert(VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string());
            // Preserve case sensitivity for INFO fields to avoid conflicts
            let field = Field::new(tag.clone(), dtype, nullable).with_metadata(field_metadata);
            fields.push(field);
        }
    }

    // Generate per-sample FORMAT columns
    // Naming convention: {format_field} for single sample, {sample_name}_{format_field} for multiple
    // If format_fields is None, include all FORMAT fields from header
    let format_tags: Vec<String> = match format_fields {
        Some(tags) => tags.clone(),
        None => header_formats.keys().map(|k| k.to_string()).collect(),
    };
    if !format_tags.is_empty() && !sample_names.is_empty() {
        let single_sample = sample_names.len() == 1;
        for sample_name in &sample_names {
            for tag in &format_tags {
                let dtype = format_to_arrow_type(header_formats, tag);
                // Skip sample prefix for single-sample VCFs
                let field_name = if single_sample {
                    tag.clone()
                } else {
                    format!("{}_{}", sample_name, tag)
                };
                // Store VCF header metadata in field metadata for round-trip preservation
                let mut field_metadata = HashMap::new();
                if let Some(format_info) = header_formats.get(tag.as_str()) {
                    field_metadata.insert(
                        VCF_FIELD_DESCRIPTION_KEY.to_string(),
                        format_info.description().to_string(),
                    );
                    field_metadata.insert(
                        VCF_FIELD_TYPE_KEY.to_string(),
                        format_type_to_string(&format_info.ty()),
                    );
                    field_metadata.insert(
                        VCF_FIELD_NUMBER_KEY.to_string(),
                        format_number_to_string(format_info.number()),
                    );
                }
                field_metadata.insert(VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string());
                field_metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), tag.clone());
                let field = Field::new(field_name, dtype, true).with_metadata(field_metadata);
                fields.push(field);
            }
        }
    }

    // Add coordinate system metadata to schema
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    metadata.insert(VCF_FILE_FORMAT_KEY.to_string(), file_format);

    // Serialize complex structures as JSON using shared utilities
    metadata.insert(VCF_FILTERS_KEY.to_string(), to_json_string(&filters));
    metadata.insert(VCF_CONTIGS_KEY.to_string(), to_json_string(&contigs));
    metadata.insert(
        VCF_ALTERNATIVE_ALLELES_KEY.to_string(),
        to_json_string(&alt_alleles),
    );
    metadata.insert(
        VCF_SAMPLE_NAMES_KEY.to_string(),
        to_json_string(&sample_names),
    );

    let schema = Schema::new_with_metadata(fields, metadata);
    // println!("Schema: {:?}", schema);
    Ok((Arc::new(schema), sample_names))
}

/// Determines if a VCF INFO field type is nullable.
///
/// FLAG type fields are not nullable (always present as true/false), while other
/// types can be absent for specific variants.
///
/// # Arguments
///
/// * `ty` - The VCF INFO field type
///
/// # Returns
///
/// `true` if the field can be null/missing, `false` if it's always present
pub fn is_nullable(ty: &InfoType) -> bool {
    !matches!(ty, InfoType::Flag)
}

/// Converts a VCF FORMAT field type to an Arrow DataType.
///
/// Handles scalar types (Integer, Float, String, Character) and array types
/// based on the Number field of the FORMAT definition. GT (genotype) fields
/// are always treated as Utf8 strings.
///
/// # Arguments
///
/// * `formats` - The VCF header FORMAT definitions
/// * `field` - The FORMAT field name
///
/// # Returns
///
/// The corresponding Arrow DataType, defaulting to Utf8 if field is not found
pub fn format_to_arrow_type(formats: &Formats, field: &str) -> DataType {
    // GT (genotype) is always represented as a string (e.g., "0/1", "1|0", "./.")
    if field == "GT" {
        return DataType::Utf8;
    }

    match formats.get(field) {
        Some(format) => {
            let inner = match format.ty() {
                FormatType::Integer => DataType::Int32,
                FormatType::Float => DataType::Float32,
                FormatType::Character => DataType::Utf8,
                FormatType::String => DataType::Utf8,
            };

            match format.number() {
                FormatNumber::Count(0) | FormatNumber::Count(1) => inner,
                // All other Number variants indicate variable-length arrays
                _ => DataType::List(Arc::new(Field::new("item", inner, true))),
            }
        }
        None => {
            log::warn!(
                "VCF FORMAT tag '{}' not found in header; defaulting to Utf8",
                field
            );
            DataType::Utf8
        }
    }
}

/// A DataFusion table provider for reading VCF files.
///
/// This provider enables SQL queries on VCF files by implementing the DataFusion
/// TableProvider interface. It supports local and remote files, multiple compression formats,
/// and projection pushdown optimization.
#[derive(Clone, Debug)]
pub struct VcfTableProvider {
    /// Path to the VCF file (local path or cloud URI)
    file_path: String,
    /// Optional list of INFO fields to include (if None, all are included)
    info_fields: Option<Vec<String>>,
    /// Optional list of FORMAT fields to include (if None, all are included)
    format_fields: Option<Vec<String>>,
    /// Arrow schema representing the VCF table structure
    schema: SchemaRef,
    /// Optional number of worker threads for BGZF decompression
    thread_num: Option<usize>,
    /// Configuration for cloud storage access
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
    /// Sample names from the VCF header (used for FORMAT column naming)
    sample_names: Vec<String>,
    /// Path to an index file (TBI/CSI). Auto-discovered if not provided.
    index_path: Option<String>,
    /// Contig names from the file header (for partitioning full scans by chromosome)
    contig_names: Vec<String>,
    /// Contig lengths from the file header (for balanced partitioning)
    contig_lengths: Vec<u64>,
}

impl VcfTableProvider {
    /// Creates a new VCF table provider.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the VCF file
    /// * `info_fields` - Optional list of INFO fields to include
    /// * `format_fields` - Optional list of FORMAT fields to include
    /// * `thread_num` - Optional number of worker threads for BGZF decompression
    /// * `object_storage_options` - Configuration for cloud storage access
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    ///
    /// # Returns
    ///
    /// A new `VcfTableProvider` instance with schema determined from the VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the header is invalid
    pub fn new(
        file_path: String,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
    ) -> datafusion::common::Result<Self> {
        use datafusion_bio_format_core::object_storage::{StorageType, get_storage_type};

        let (schema, sample_names) = block_on(determine_schema_from_header(
            &file_path,
            &info_fields,
            &format_fields,
            &object_storage_options,
            coordinate_system_zero_based,
        ))?;

        // Extract contig names and lengths from schema metadata
        let contig_metadata: Vec<ContigMetadata> = schema
            .metadata()
            .get(VCF_CONTIGS_KEY)
            .and_then(|json| serde_json::from_str::<Vec<ContigMetadata>>(json).ok())
            .unwrap_or_default();
        let contig_names: Vec<String> = contig_metadata.iter().map(|c| c.id.clone()).collect();
        let contig_lengths: Vec<u64> = contig_metadata
            .iter()
            .map(|c| c.length.unwrap_or(0))
            .collect();

        // Auto-discover index file for local BGZF-compressed files
        let storage_type = get_storage_type(file_path.clone());
        let index_path = if matches!(storage_type, StorageType::LOCAL) {
            discover_vcf_index(&file_path).map(|(path, fmt)| {
                debug!("Discovered VCF index: {} (format: {:?})", path, fmt);
                path
            })
        } else {
            None
        };

        Ok(Self {
            file_path,
            info_fields,
            format_fields,
            schema,
            thread_num,
            object_storage_options,
            coordinate_system_zero_based,
            sample_names,
            index_path,
            contig_names,
            contig_lengths,
        })
    }

    /// Creates a new VCF table provider for write operations.
    ///
    /// This constructor is used when the output file does not exist yet. It accepts
    /// the schema directly instead of reading it from the file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the output VCF file
    /// * `schema` - Arrow schema for the output
    /// * `info_fields` - List of INFO fields to write
    /// * `format_fields` - List of FORMAT fields to write
    /// * `sample_names` - Sample names for the VCF header
    /// * `coordinate_system_zero_based` - If true, input coordinates are 0-based half-open;
    ///   if false, input coordinates are 1-based closed
    ///
    /// # Returns
    ///
    /// A new `VcfTableProvider` instance configured for writing
    pub fn new_for_write(
        file_path: String,
        schema: SchemaRef,
        info_fields: Vec<String>,
        format_fields: Vec<String>,
        sample_names: Vec<String>,
        coordinate_system_zero_based: bool,
    ) -> Self {
        Self {
            file_path,
            info_fields: if info_fields.is_empty() {
                None
            } else {
                Some(info_fields)
            },
            format_fields: if format_fields.is_empty() {
                None
            } else {
                Some(format_fields)
            },
            schema,
            thread_num: None,
            object_storage_options: None,
            coordinate_system_zero_based,
            sample_names,
            index_path: None,
            contig_names: Vec::new(),
            contig_lengths: Vec::new(),
        }
    }
}

#[async_trait]
impl TableProvider for VcfTableProvider {
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
        let pushdown_support = filters
            .iter()
            .map(|expr| {
                if self.index_path.is_some() && is_genomic_coordinate_filter(expr) {
                    debug!("VCF filter can be pushed down (indexed): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else if can_push_down_record_filter(expr, &self.schema) {
                    debug!("VCF filter can be pushed down (record-level): {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("VCF filter cannot be pushed down: {:?}", expr);
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
        debug!("VcfTableProvider::scan");

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

        let schema = project_schema(&self.schema, projection);

        // Determine regions and partitioning when index is available
        if let Some(ref index_path) = self.index_path {
            let analysis = extract_genomic_regions(filters, self.coordinate_system_zero_based);

            let regions = if !analysis.regions.is_empty() {
                analysis.regions
            } else if !self.contig_names.is_empty() {
                // Full scan: partition by chromosome for parallel reading
                build_full_scan_regions(&self.contig_names)
            } else {
                Vec::new()
            };

            if !regions.is_empty() {
                // Use balanced partitioning with index size estimates
                let target_partitions = state.config().target_partitions();
                let estimates = crate::storage::estimate_sizes_from_tbi(
                    index_path,
                    &regions,
                    &self.contig_names,
                    &self.contig_lengths,
                );
                let assignments = balance_partitions(estimates, target_partitions);
                let num_partitions = assignments.len();

                // Collect filters for record-level evaluation
                let record_filters: Vec<Expr> = filters
                    .iter()
                    .filter(|expr| can_push_down_record_filter(expr, &self.schema))
                    .cloned()
                    .collect();

                debug!(
                    "VCF indexed scan: {} partitions (from {} regions, target {}), {} record-level filters",
                    num_partitions,
                    assignments.iter().map(|a| a.regions.len()).sum::<usize>(),
                    target_partitions,
                    record_filters.len()
                );

                return Ok(Arc::new(VcfExec {
                    cache: PlanProperties::new(
                        EquivalenceProperties::new(schema.clone()),
                        Partitioning::UnknownPartitioning(num_partitions),
                        EmissionType::Final,
                        Boundedness::Bounded,
                    ),
                    file_path: self.file_path.clone(),
                    schema: schema.clone(),
                    info_fields: self.info_fields.clone(),
                    format_fields: self.format_fields.clone(),
                    sample_names: self.sample_names.clone(),
                    projection: projection.cloned(),
                    limit,
                    thread_num: self.thread_num,
                    object_storage_options: self.object_storage_options.clone(),
                    coordinate_system_zero_based: self.coordinate_system_zero_based,
                    partition_assignments: Some(assignments),
                    index_path: Some(index_path.clone()),
                    residual_filters: record_filters,
                }));
            }
        }

        // Fallback: sequential full scan (no index or no regions)
        Ok(Arc::new(VcfExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            info_fields: self.info_fields.clone(),
            format_fields: self.format_fields.clone(),
            sample_names: self.sample_names.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            partition_assignments: None,
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
        debug!("VcfTableProvider::insert_into");

        // Only OVERWRITE mode is supported (file will be created/replaced)
        if insert_op != InsertOp::Overwrite {
            return Err(datafusion::common::DataFusionError::NotImplemented(
                "VCF write only supports OVERWRITE mode (INSERT OVERWRITE). \
                 APPEND mode is not supported."
                    .to_string(),
            ));
        }

        // Validate input schema has the required core columns
        let input_schema = input.schema();
        if input_schema.fields().len() < 8 {
            return Err(datafusion::common::DataFusionError::Plan(
                "Input schema must have at least 8 columns: chrom, start, end, id, ref, alt, qual, filter"
                    .to_string(),
            ));
        }

        // Determine compression from file path
        let compression = VcfCompressionType::from_path(&self.file_path);

        // Get info and format field names
        let info_fields = self.info_fields.clone().unwrap_or_default();
        let format_fields = self.format_fields.clone().unwrap_or_default();

        Ok(Arc::new(VcfWriteExec::new(
            input,
            self.file_path.clone(),
            Some(compression),
            info_fields,
            format_fields,
            self.sample_names.clone(),
            self.coordinate_system_zero_based,
        )))
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }
}

/// Converts a VCF INFO field type to an Arrow DataType.
///
/// Handles scalar types (Integer, Float, String, Character, Flag) and array types
/// based on the Number field of the INFO definition.
///
/// # Arguments
///
/// * `infos` - The VCF header INFO definitions
/// * `field` - The INFO field name
///
/// # Returns
///
/// The corresponding Arrow DataType, defaulting to Utf8 if field is not found
pub fn info_to_arrow_type(infos: &Infos, field: &str) -> DataType {
    match infos.get(field) {
        Some(t) => {
            let inner = match t.ty() {
                InfoType::Integer => DataType::Int32,
                InfoType::String | InfoType::Character => DataType::Utf8,
                InfoType::Float => DataType::Float32,
                InfoType::Flag => DataType::Boolean,
            };

            match t.number() {
                Number::Count(0) | Number::Count(1) => inner,
                Number::Count(_)
                | Number::Unknown
                | Number::AlternateBases
                | Number::ReferenceAlternateBases
                | Number::Samples => DataType::List(Arc::new(Field::new("item", inner, true))),
            }
        }
        None => {
            log::warn!(
                "VCF tag '{}' not found in header; defaulting to Utf8",
                field
            );
            DataType::Utf8
        }
    }
}

/// Converts INFO Number enum to VCF string representation
fn info_number_to_string(number: Number) -> String {
    match number {
        Number::Count(n) => n.to_string(),
        Number::AlternateBases => "A".to_string(),
        Number::ReferenceAlternateBases => "R".to_string(),
        Number::Samples => "G".to_string(),
        Number::Unknown => ".".to_string(),
    }
}

/// Converts INFO Type enum to VCF string representation
fn info_type_to_string(ty: &InfoType) -> String {
    match ty {
        InfoType::Integer => "Integer".to_string(),
        InfoType::Float => "Float".to_string(),
        InfoType::Flag => "Flag".to_string(),
        InfoType::Character => "Character".to_string(),
        InfoType::String => "String".to_string(),
    }
}

/// Converts FORMAT Number enum to VCF string representation
fn format_number_to_string(number: FormatNumber) -> String {
    match number {
        FormatNumber::Count(n) => n.to_string(),
        FormatNumber::AlternateBases => "A".to_string(),
        FormatNumber::ReferenceAlternateBases => "R".to_string(),
        FormatNumber::Samples => "G".to_string(),
        FormatNumber::Unknown => ".".to_string(),
        FormatNumber::LocalAlternateBases => "LA".to_string(),
        FormatNumber::LocalReferenceAlternateBases => "LR".to_string(),
        FormatNumber::LocalSamples => "LG".to_string(),
        FormatNumber::Ploidy => "P".to_string(),
        FormatNumber::BaseModifications => "M".to_string(),
    }
}

/// Converts FORMAT Type enum to VCF string representation
fn format_type_to_string(ty: &FormatType) -> String {
    match ty {
        FormatType::Integer => "Integer".to_string(),
        FormatType::Float => "Float".to_string(),
        FormatType::Character => "Character".to_string(),
        FormatType::String => "String".to_string(),
    }
}
