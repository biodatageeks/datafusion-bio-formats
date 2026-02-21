//! Write execution plan for BAM/SAM files
//!
//! This module provides the physical execution plan for writing DataFusion
//! query results to BAM/SAM files.

use crate::header_builder::build_bam_header;
use crate::serializer::batch_to_bam_records;
use crate::writer::{BamCompressionType, BamLocalWriter};
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::StreamExt;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for writing BAM/SAM files
///
/// This execution plan consumes input RecordBatches, converts them to BAM records,
/// and writes them to a local file with optional compression.
///
/// The plan returns a single-row RecordBatch with a `count` column containing
/// the total number of records written.
pub struct BamWriteExec {
    /// The input execution plan providing data to write
    input: Arc<dyn ExecutionPlan>,
    /// Path to the output BAM/SAM file
    output_path: String,
    /// Compression type for the output file
    compression: BamCompressionType,
    /// Alignment tag field names
    tag_fields: Vec<String>,
    /// Whether the input coordinates are 0-based (need +1 for BAM POS output)
    coordinate_system_zero_based: bool,
    /// Metadata overrides to merge into the input schema before building the header
    schema_metadata_overrides: HashMap<String, String>,
    /// Whether to sort records by coordinate before writing
    sort_on_write: bool,
    /// Cached plan properties
    cache: PlanProperties,
}

impl BamWriteExec {
    /// Creates a new BAM write execution plan
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan providing RecordBatches to write
    /// * `output_path` - Path to the output BAM/SAM file
    /// * `compression` - Compression type (defaults to auto-detection from path)
    /// * `tag_fields` - List of alignment tag field names
    /// * `coordinate_system_zero_based` - Whether input uses 0-based coordinates
    ///
    /// # Returns
    ///
    /// A new `BamWriteExec` instance
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        compression: Option<BamCompressionType>,
        tag_fields: Vec<String>,
        coordinate_system_zero_based: bool,
        schema_metadata_overrides: HashMap<String, String>,
        sort_on_write: bool,
    ) -> Self {
        let compression =
            compression.unwrap_or_else(|| BamCompressionType::from_path(&output_path));

        // Output schema is a single count column
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let cache = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            output_path,
            compression,
            tag_fields,
            coordinate_system_zero_based,
            schema_metadata_overrides,
            sort_on_write,
            cache,
        }
    }

    /// Returns the output file path
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    /// Returns the compression type
    pub fn compression(&self) -> BamCompressionType {
        self.compression
    }

    /// Returns the output schema
    fn schema(&self) -> SchemaRef {
        self.cache.eq_properties.schema().clone()
    }
}

impl Debug for BamWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BamWriteExec")
            .field("output_path", &self.output_path)
            .field("compression", &self.compression)
            .field("tag_fields", &self.tag_fields)
            .finish()
    }
}

impl DisplayAs for BamWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "BamWriteExec: path={}, compression={:?}, tag_fields={}",
            self.output_path,
            self.compression,
            self.tag_fields.len()
        )
    }
}

impl ExecutionPlan for BamWriteExec {
    fn name(&self) -> &str {
        "BamWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.sort_on_write {
            // Preserve partitions for parallel sort + merge
            vec![Distribution::UnspecifiedDistribution]
        } else {
            // Coalesce all partitions so execute() reads all data
            vec![Distribution::SinglePartition]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "BamWriteExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(BamWriteExec::new(
            children[0].clone(),
            self.output_path.clone(),
            Some(self.compression),
            self.tag_fields.clone(),
            self.coordinate_system_zero_based,
            self.schema_metadata_overrides.clone(),
            self.sort_on_write,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "BamWriteExec::execute partition={}, path={}",
            partition, self.output_path
        );

        let output_path = self.output_path.clone();
        let compression = self.compression;
        let tag_fields = self.tag_fields.clone();
        let coordinate_system_zero_based = self.coordinate_system_zero_based;
        let schema_metadata_overrides = self.schema_metadata_overrides.clone();

        // If sort_on_write, wrap the input plan in a SortExec at execution time
        // (cannot rely on plan-level wrapping since DataFusion's optimizer may strip it)
        let input_schema = self.input.schema();
        let input = if self.sort_on_write {
            let chrom_idx = input_schema.index_of("chrom").map_err(|_| {
                DataFusionError::Plan("Column 'chrom' not found for sort_on_write".to_string())
            })?;
            let start_idx = input_schema.index_of("start").map_err(|_| {
                DataFusionError::Plan("Column 'start' not found for sort_on_write".to_string())
            })?;

            let sort_exprs = LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    Arc::new(Column::new("chrom", chrom_idx)),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                ),
                PhysicalSortExpr::new(
                    Arc::new(Column::new("start", start_idx)),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                ),
            ])
            .expect("sort expressions should not be empty");

            let sort_exec = Arc::new(
                SortExec::new(sort_exprs.clone(), self.input.clone())
                    .with_preserve_partitioning(true),
            );
            let merge_exec = SortPreservingMergeExec::new(sort_exprs, sort_exec);
            merge_exec.execute(0, context)?
        } else {
            self.input.execute(0, context)?
        };
        let output_schema = self.schema();

        let stream = futures::stream::once(write_bam_stream(
            input,
            input_schema,
            output_path,
            compression,
            tag_fields,
            coordinate_system_zero_based,
            schema_metadata_overrides,
            output_schema,
        ));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Writes a stream of RecordBatches to a BAM/SAM file
///
/// This is the main write function that:
/// 1. Creates a BAM/SAM writer
/// 2. Builds and writes the header from schema metadata
/// 3. Converts each RecordBatch to BAM records
/// 4. Writes all records to the file
/// 5. Returns a count of written records
#[allow(clippy::too_many_arguments)]
async fn write_bam_stream(
    mut input: SendableRecordBatchStream,
    input_schema: SchemaRef,
    output_path: String,
    compression: BamCompressionType,
    tag_fields: Vec<String>,
    coordinate_system_zero_based: bool,
    schema_metadata_overrides: HashMap<String, String>,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    debug!("Starting BAM write to: {output_path}");

    // Create writer
    let mut writer = BamLocalWriter::with_compression(&output_path, compression)?;

    // Merge metadata overrides into input schema before building header
    let effective_schema = if schema_metadata_overrides.is_empty() {
        input_schema
    } else {
        let mut metadata = input_schema.metadata().clone();
        metadata.extend(schema_metadata_overrides);
        Arc::new(Schema::new_with_metadata(
            input_schema.fields().iter().cloned().collect::<Vec<_>>(),
            metadata,
        ))
    };

    // Build header from schema metadata
    let header = build_bam_header(&effective_schema, &tag_fields)?;

    // Write header
    writer.write_header(&header)?;

    // Process and write batches
    let mut total_count = 0u64;

    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        debug!("Writing batch with {num_rows} rows");

        // Convert batch to BAM records
        let records =
            batch_to_bam_records(&batch, &header, &tag_fields, coordinate_system_zero_based)?;

        // Write records
        writer.write_records(&header, &records)?;
        total_count += num_rows as u64;
    }

    // Finish writing
    writer.finish(&header)?;

    debug!("Wrote {total_count} records to {output_path}");

    // Return count as RecordBatch
    let count_array = Arc::new(UInt64Array::from(vec![total_count]));
    let result_batch = RecordBatch::try_new(output_schema, vec![count_array])
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(result_batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::prelude::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_bam_write_exec_basic() -> Result<()> {
        // Create a minimal schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("chrom", DataType::Utf8, true),
            Field::new("start", DataType::UInt32, true),
            Field::new("flags", DataType::UInt32, false),
            Field::new("cigar", DataType::Utf8, false),
            Field::new("mapping_quality", DataType::UInt32, true),
            Field::new("mate_chrom", DataType::Utf8, true),
            Field::new("mate_start", DataType::UInt32, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
            Field::new("template_length", DataType::Int32, false),
        ]));

        // Create test data
        let names = StringArray::from(vec![Some("read1"), Some("read2")]);
        let chroms = StringArray::from(vec![Some("chr1"), Some("chr1")]);
        let starts = UInt32Array::from(vec![Some(100u32), Some(200u32)]);
        let flags = UInt32Array::from(vec![0u32, 16u32]);
        let cigars = StringArray::from(vec!["10M", "10M"]);
        let mapping_qualities = UInt32Array::from(vec![60u32, 60u32]);
        let mate_chroms = StringArray::from(vec![Option::<&str>::None, Option::<&str>::None]);
        let mate_starts = UInt32Array::from(vec![Option::<u32>::None, Option::<u32>::None]);
        let sequences = StringArray::from(vec!["ACGTACGTAC", "TGCATGCATG"]);
        let quality_scores = StringArray::from(vec!["!!!!!!!!!!", "!!!!!!!!!!"]);
        let template_lengths = Int32Array::from(vec![0i32, 0i32]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(names),
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(flags),
                Arc::new(cigars),
                Arc::new(mapping_qualities),
                Arc::new(mate_chroms),
                Arc::new(mate_starts),
                Arc::new(sequences),
                Arc::new(quality_scores),
                Arc::new(template_lengths),
            ],
        )
        .unwrap();

        // Create a MemTable as input
        let provider = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        // Create temporary output file
        let temp_file = NamedTempFile::with_suffix(".sam").unwrap();
        let output_path = temp_file.path().to_string_lossy().to_string();

        // Create write exec
        let input_plan = provider
            .scan(&SessionContext::new().state(), None, &[], None)
            .await?;
        let write_exec = BamWriteExec::new(
            input_plan,
            output_path.clone(),
            None,
            vec![],
            true,
            HashMap::new(),
            false,
        );

        // Execute the plan
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = write_exec.execute(0, task_ctx)?;

        // Get result
        let result = stream.next().await.unwrap()?;
        let count_array = result
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(count_array.value(0), 2);

        Ok(())
    }
}
