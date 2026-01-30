use crate::physical_exec::BamExec;
use crate::tag_registry::get_known_tags;
use async_trait::async_trait;
use datafusion::arrow::array::{BooleanBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::DataFrame;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
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
            let tag_def = known_tags.get(tag).ok_or_else(|| {
                let available_tags: Vec<String> = known_tags.keys().cloned().collect();
                DataFusionError::Configuration(format!(
                    "Unknown BAM tag '{}'. Available tags: {}",
                    tag,
                    available_tags.join(", ")
                ))
            })?;

            let mut field_metadata = HashMap::new();
            field_metadata.insert(BAM_TAG_TAG_KEY.to_string(), tag.clone());
            field_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), tag_def.sam_type.to_string());
            field_metadata.insert(
                BAM_TAG_DESCRIPTION_KEY.to_string(),
                tag_def.description.clone(),
            );

            fields.push(
                Field::new(tag.clone(), tag_def.arrow_type.clone(), true)
                    .with_metadata(field_metadata),
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
    debug!("Schema: {:?}", schema);
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
        use crate::tag_registry::infer_type_from_noodles_value;
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
                true,
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
                format!("Discovered in data. {}", tag_def.description)
            } else {
                format!("Discovered in data. Custom/unknown tag ({})", sam_type)
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
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
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
}
