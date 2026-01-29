use crate::physical_exec::BamExec;
use crate::tag_registry::get_known_tags;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
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
