use crate::physical_exec::BamExec;
use crate::write_exec::BamWriteExec;
use async_trait::async_trait;
use datafusion::arrow::array::{BooleanBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::{Expr, dml::InsertOp};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::DataFrame;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::tag_registry::get_known_tags;
use datafusion_bio_format_core::{
    BAM_TAG_DESCRIPTION_KEY, BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY, COORDINATE_SYSTEM_METADATA_KEY,
};
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

fn determine_schema(
    tag_fields: &Option<Vec<String>>,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("chrom", DataType::Utf8, true),
        Field::new("start", DataType::UInt32, true),
        Field::new("end", DataType::UInt32, true),
        Field::new("flags", DataType::UInt32, false), //FIXME:: optimize storage
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
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
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

/// Determines schema by scanning actual BAM file records to infer tag types
///
/// This is more accurate than using only the static registry, as it discovers
/// the actual types used in the file.
async fn determine_schema_from_file(
    file_path: String,
    thread_num: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
    tag_fields: &Option<Vec<String>>,
    coordinate_system_zero_based: bool,
    sample_size: usize,
) -> datafusion::common::Result<SchemaRef> {
    use crate::storage::BamReader;
    use datafusion_bio_format_core::tag_registry::infer_type_from_noodles_value;
    use futures_util::StreamExt;

    let mut fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("chrom", DataType::Utf8, true),
        Field::new("start", DataType::UInt32, true),
        Field::new("end", DataType::UInt32, true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];

    // If tag fields are specified, discover their actual types from the file
    if let Some(tags) = tag_fields {
        let known_tags = get_known_tags();

        // Create BAM reader to sample records
        let mut reader = BamReader::new(file_path, thread_num, object_storage_options).await;

        // Read header (required before reading records)
        let _ref_sequences = reader.read_sequences().await;

        // Discover actual tag types by sampling records
        let mut discovered_tags: HashMap<String, (char, DataType)> = HashMap::new();
        let mut records = reader.read_records().await;
        let mut count = 0;

        debug!(
            "Starting schema inference by sampling {} records",
            sample_size
        );
        debug!("Looking for tags: {:?}", tags);

        while let Some(result) = records.next().await {
            if count >= sample_size {
                break;
            }

            match result {
                Ok(record) => {
                    let data = record.data();

                    // Look for the requested tags in this record
                    for tag_name in tags {
                        if discovered_tags.contains_key(tag_name) {
                            continue; // Already discovered
                        }

                        // Parse tag to noodles format
                        let tag_bytes = tag_name.as_bytes();
                        if tag_bytes.len() == 2 {
                            let tag = noodles_sam::alignment::record::data::field::Tag::from([
                                tag_bytes[0],
                                tag_bytes[1],
                            ]);

                            if let Some(Ok(value)) = data.get(&tag) {
                                let (sam_type, arrow_type) = infer_type_from_noodles_value(&value);
                                debug!(
                                    "Found tag {} in record {}: sam_type={}, arrow_type={:?}",
                                    tag_name, count, sam_type, arrow_type
                                );
                                discovered_tags.insert(tag_name.clone(), (sam_type, arrow_type));
                            }
                        }
                    }
                    count += 1;
                }
                Err(e) => {
                    debug!("Error reading record during schema inference: {:?}", e);
                    continue;
                }
            }
        }

        debug!("Schema inference completed after {} records", count);
        debug!(
            "Discovered tags: {:?}",
            discovered_tags.keys().collect::<Vec<_>>()
        );

        // Build fields for all requested tags
        for tag in tags {
            let mut field_metadata = HashMap::new();
            field_metadata.insert(BAM_TAG_TAG_KEY.to_string(), tag.clone());

            // Use discovered type if found, otherwise fall back to registry, then default
            let (sam_type, arrow_type, description) =
                if let Some((sam_t, arrow_t)) = discovered_tags.get(tag) {
                    // Use actual discovered type from file
                    let desc = known_tags
                        .get(tag)
                        .map(|t| t.description.clone())
                        .unwrap_or_else(|| format!("Tag type discovered from file ({})", sam_t));
                    debug!(
                        "Using discovered type for {}: {} -> {:?}",
                        tag, sam_t, arrow_t
                    );
                    (*sam_t, arrow_t.clone(), desc)
                } else if let Some(tag_def) = known_tags.get(tag) {
                    // Fall back to registry definition
                    debug!(
                        "Using registry type for {}: {} -> {:?}",
                        tag, tag_def.sam_type, tag_def.arrow_type
                    );
                    (
                        tag_def.sam_type,
                        tag_def.arrow_type.clone(),
                        tag_def.description.clone(),
                    )
                } else {
                    // Default for unknown tags
                    debug!("Using default type for {}: Z -> Utf8", tag);
                    ('Z', DataType::Utf8, "Unknown tag".to_string())
                };

            field_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), sam_type.to_string());
            field_metadata.insert(BAM_TAG_DESCRIPTION_KEY.to_string(), description);

            fields.push(
                Field::new(tag.clone(), arrow_type.clone(), true).with_metadata(field_metadata),
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
    debug!("Schema (from file): {:?}", schema);
    Ok(Arc::new(schema))
}

/// A DataFusion table provider for BAM (Binary Alignment Map) files.
///
/// This struct implements the DataFusion TableProvider trait, allowing BAM files
/// to be queried using SQL via DataFusion. It supports both local and remote
/// (cloud storage) files with configurable threading for decompression.
#[derive(Clone, Debug)]
pub struct BamTableProvider {
    /// Path to the BAM file (local or remote)
    file_path: String,
    /// Arrow schema for the BAM records
    schema: SchemaRef,
    /// Number of threads to use for BGZF decompression
    thread_num: Option<usize>,
    /// Configuration for cloud storage access
    object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    coordinate_system_zero_based: bool,
    /// Optional list of BAM alignment tags to include as columns
    tag_fields: Option<Vec<String>>,
}

impl BamTableProvider {
    /// Creates a new BAM table provider.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BAM file (local or remote URL)
    /// * `thread_num` - Optional number of threads for BGZF decompression
    /// * `object_storage_options` - Optional configuration for cloud storage access
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    /// * `tag_fields` - Optional list of BAM alignment tag names to include as columns.
    ///   `None` = no tags included (only 11 core fields),
    ///   `Some(vec!["NM", "MD", "AS"])` = include specified tags as columns
    ///
    /// # Returns
    ///
    /// A new BamTableProvider or a DataFusion error if schema determination fails
    ///
    /// # Note
    ///
    /// This method uses the static tag registry for schema determination. If your BAM file
    /// contains tags with non-standard types (e.g., XS as float instead of int), consider
    /// using `try_new_with_inferred_schema()` instead, which samples the file to discover
    /// actual tag types.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use datafusion_bio_format_bam::table_provider::BamTableProvider;
    ///
    /// # async fn example() -> datafusion::common::Result<()> {
    /// // Basic usage without tags
    /// let provider = BamTableProvider::new(
    ///     "data/alignments.bam".to_string(),
    ///     Some(4),  // Use 4 threads
    ///     None,     // No cloud storage
    ///     true,     // Use 0-based coordinates (default)
    ///     None,     // No tag fields
    /// )?;
    ///
    /// // With alignment tags
    /// let provider_with_tags = BamTableProvider::new(
    ///     "data/alignments.bam".to_string(),
    ///     Some(4),
    ///     None,
    ///     true,
    ///     Some(vec!["NM".to_string(), "MD".to_string(), "AS".to_string()]),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
        tag_fields: Option<Vec<String>>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema(&tag_fields, coordinate_system_zero_based)?;
        Ok(Self {
            file_path,
            schema,
            thread_num,
            object_storage_options,
            coordinate_system_zero_based,
            tag_fields,
        })
    }

    /// Creates a new BAM table provider with schema inferred from the file.
    ///
    /// This method samples records from the BAM file to discover the actual types
    /// of the requested tags, rather than relying solely on the static registry.
    /// This is more accurate for files that may use non-standard tag types.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BAM file (local or remote URL)
    /// * `thread_num` - Optional number of threads for BGZF decompression
    /// * `object_storage_options` - Optional configuration for cloud storage access
    /// * `coordinate_system_zero_based` - If true (default), output 0-based half-open coordinates;
    ///   if false, output 1-based closed coordinates
    /// * `tag_fields` - Optional list of BAM alignment tag names to include as columns
    /// * `sample_size` - Number of records to sample for type inference (default: 100)
    ///
    /// # Returns
    ///
    /// A new BamTableProvider or a DataFusion error if schema inference fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use datafusion_bio_format_bam::table_provider::BamTableProvider;
    ///
    /// # async fn example() -> datafusion::common::Result<()> {
    /// // Create provider with inferred schema
    /// let provider = BamTableProvider::try_new_with_inferred_schema(
    ///     "data/alignments.bam".to_string(),
    ///     Some(4),
    ///     None,
    ///     true,
    ///     Some(vec!["XS".to_string(), "XT".to_string()]),
    ///     Some(100),  // Sample 100 records
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_new_with_inferred_schema(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
        coordinate_system_zero_based: bool,
        tag_fields: Option<Vec<String>>,
        sample_size: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let sample_size = sample_size.unwrap_or(100);
        let schema = determine_schema_from_file(
            file_path.clone(),
            thread_num,
            object_storage_options.clone(),
            &tag_fields,
            coordinate_system_zero_based,
            sample_size,
        )
        .await?;
        Ok(Self {
            file_path,
            schema,
            thread_num,
            object_storage_options,
            coordinate_system_zero_based,
            tag_fields,
        })
    }

    /// Creates a new BAM table provider for write operations.
    ///
    /// This constructor is used when creating a table provider for writing
    /// query results to a BAM/SAM file.
    ///
    /// # Arguments
    ///
    /// * `output_path` - Path to the output BAM/SAM file
    /// * `schema` - Arrow schema with metadata for header construction
    /// * `tag_fields` - Optional list of alignment tag names to write
    /// * `coordinate_system_zero_based` - If true, input uses 0-based coordinates
    ///
    /// # Returns
    ///
    /// A new BamTableProvider configured for writing
    pub fn new_for_write(
        output_path: String,
        schema: SchemaRef,
        tag_fields: Option<Vec<String>>,
        coordinate_system_zero_based: bool,
    ) -> Self {
        Self {
            file_path: output_path,
            schema,
            thread_num: None,
            object_storage_options: None,
            coordinate_system_zero_based,
            tag_fields,
        }
    }

    /// Discovers and describes all columns in the BAM file by sampling records.
    ///
    /// This method reads a sample of records from the BAM file and discovers which
    /// alignment tags are actually present in the data, along with their inferred
    /// data types.
    ///
    /// # Arguments
    ///
    /// * `ctx` - SessionContext for creating the output DataFrame
    /// * `sample_size` - Number of records to sample for discovery (default: 100)
    ///
    /// # Returns
    ///
    /// A DataFrame with columns:
    /// - `column_name`: Name of the field
    /// - `data_type`: Arrow data type as string
    /// - `nullable`: Whether the field accepts null values
    /// - `category`: Either "core" or "tag"
    /// - `sam_type`: SAM type character (i, Z, f, A, H, B)
    /// - `description`: Human-readable description
    ///
    /// # Example
    ///
    /// ```no_run
    /// use datafusion::prelude::*;
    /// use datafusion_bio_format_bam::table_provider::BamTableProvider;
    ///
    /// # async fn example() -> datafusion::error::Result<()> {
    /// let ctx = SessionContext::new();
    /// let provider = BamTableProvider::new(
    ///     "data/alignments.bam".to_string(),
    ///     Some(4),
    ///     None,
    ///     true,
    ///     None,
    /// )?;
    ///
    /// let schema_df = provider.describe(&ctx, Some(100)).await?;
    /// schema_df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        sample_size: Option<usize>,
    ) -> Result<DataFrame, DataFusionError> {
        use crate::storage::BamReader;
        use datafusion_bio_format_core::tag_registry::infer_type_from_noodles_value;
        use futures_util::StreamExt;

        let sample_size = sample_size.unwrap_or(100);

        // Create BAM reader
        let mut reader = BamReader::new(
            self.file_path.clone(),
            self.thread_num,
            self.object_storage_options.clone(),
        )
        .await;

        // Read header first (required before reading records)
        let _ref_sequences = reader.read_sequences().await;

        // Discover tags by reading sample records
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

                    // Iterate through all tags in this record
                    for (tag, value) in data.iter().filter_map(Result::ok) {
                        let tag_str =
                            format!("{}{}", tag.as_ref()[0] as char, tag.as_ref()[1] as char);

                        // Only add if not already discovered
                        discovered_tags
                            .entry(tag_str)
                            .or_insert_with(|| infer_type_from_noodles_value(&value));
                    }
                    count += 1;
                }
                Err(_) => continue,
            }
        }

        // Build output RecordBatch
        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut nullables = BooleanBuilder::new();
        let mut categories = StringBuilder::new();
        let mut sam_types = StringBuilder::new();
        let mut descriptions = StringBuilder::new();

        // Helper to convert DataType to string
        fn datatype_to_string(dtype: &DataType) -> String {
            match dtype {
                DataType::Utf8 => "Utf8".to_string(),
                DataType::Int32 => "Int32".to_string(),
                DataType::UInt32 => "UInt32".to_string(),
                DataType::Float32 => "Float32".to_string(),
                DataType::Boolean => "Boolean".to_string(),
                DataType::List(f) => format!("List<{}>", datatype_to_string(f.data_type())),
                _ => format!("{:?}", dtype),
            }
        }

        // Add 11 core fields
        let core_fields = vec![
            (
                "name",
                DataType::Utf8,
                true,
                "Read name/query template name",
            ),
            ("chrom", DataType::Utf8, true, "Reference sequence name"),
            ("start", DataType::UInt32, true, "Leftmost mapping position"),
            ("end", DataType::UInt32, true, "Rightmost mapping position"),
            ("flags", DataType::UInt32, false, "Bitwise flags"),
            ("cigar", DataType::Utf8, false, "CIGAR string"),
            (
                "mapping_quality",
                DataType::UInt32,
                false,
                "Mapping quality (0-255)",
            ),
            (
                "mate_chrom",
                DataType::Utf8,
                true,
                "Mate reference sequence name",
            ),
            ("mate_start", DataType::UInt32, true, "Mate position"),
            ("sequence", DataType::Utf8, false, "Segment sequence"),
            (
                "quality_scores",
                DataType::Utf8,
                false,
                "Base quality scores",
            ),
        ];

        for (name, dtype, nullable, desc) in core_fields {
            column_names.append_value(name);
            data_types.append_value(datatype_to_string(&dtype));
            nullables.append_value(nullable);
            categories.append_value("core");
            sam_types.append_null();

            let full_desc = if name == "start" {
                format!(
                    "{} ({})",
                    desc,
                    if self.coordinate_system_zero_based {
                        "0-based"
                    } else {
                        "1-based"
                    }
                )
            } else {
                desc.to_string()
            };
            descriptions.append_value(full_desc);
        }

        // Add discovered tags (sorted alphabetically)
        let mut tag_names: Vec<_> = discovered_tags.keys().cloned().collect();
        tag_names.sort();

        let known_tags = get_known_tags();

        for tag_name in tag_names {
            let (sam_type, arrow_type) = &discovered_tags[&tag_name];

            column_names.append_value(&tag_name);
            data_types.append_value(datatype_to_string(arrow_type));
            nullables.append_value(true); // All tags are nullable
            categories.append_value("tag");
            sam_types.append_value(sam_type.to_string());

            // Use registry description if available, otherwise generic
            let desc = if let Some(tag_def) = known_tags.get(&tag_name) {
                tag_def.description.clone()
            } else {
                format!("Custom/unknown tag ({})", sam_type)
            };
            descriptions.append_value(desc);
        }

        // Build arrays
        let column_names_array = Arc::new(column_names.finish());
        let data_types_array = Arc::new(data_types.finish());
        let nullables_array = Arc::new(nullables.finish());
        let categories_array = Arc::new(categories.finish());
        let sam_types_array = Arc::new(sam_types.finish());
        let descriptions_array = Arc::new(descriptions.finish());

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("sam_type", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, false),
        ]));

        // Build RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                column_names_array,
                data_types_array,
                nullables_array,
                categories_array,
                sam_types_array,
                descriptions_array,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Create MemTable and return DataFrame
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.read_table(Arc::new(provider))
    }
}

#[async_trait]
impl TableProvider for BamTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("BamTableProvider::scan");

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

        Ok(Arc::new(BamExec {
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
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            tag_fields: self.tag_fields.clone(),
        }))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Overwrite {
            return Err(DataFusionError::NotImplemented(
                "BAM insert_into only supports OVERWRITE mode".to_string(),
            ));
        }

        debug!("BamTableProvider::insert_into path={}", self.file_path);

        // Extract metadata from schema
        let schema_metadata = self.schema.metadata();
        let coordinate_system_zero_based = schema_metadata
            .get(COORDINATE_SYSTEM_METADATA_KEY)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true);

        // Extract tag fields from schema (fields with BAM_TAG_TAG_KEY metadata)
        let mut tag_fields = Vec::new();
        for field in self.schema.fields() {
            if field.metadata().contains_key(BAM_TAG_TAG_KEY) {
                tag_fields.push(field.name().clone());
            }
        }

        // Merge with explicitly provided tag_fields (if any)
        // This ensures tags passed to new_for_write are honored even without metadata
        if let Some(explicit_tags) = &self.tag_fields {
            for tag in explicit_tags {
                if !tag_fields.contains(tag) {
                    tag_fields.push(tag.clone());
                }
            }
        }

        // Create write execution plan
        Ok(Arc::new(BamWriteExec::new(
            input,
            self.file_path.clone(),
            None, // Auto-detect compression from file extension
            tag_fields,
            coordinate_system_zero_based,
        )))
    }
}
