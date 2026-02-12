//! Write execution plan for CRAM files
//!
//! This module provides the physical execution plan for writing DataFusion
//! query results to CRAM files with reference sequence support.

use crate::header_builder::build_cram_header;
use crate::serializer::batch_to_cram_records;
use crate::writer::CramLocalWriter;
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

/// Physical execution plan for writing CRAM files
///
/// This execution plan consumes input RecordBatches, converts them to CRAM records,
/// and writes them to a local file with reference sequence support.
///
/// The plan returns a single-row RecordBatch with a `count` column containing
/// the total number of records written.
pub struct CramWriteExec {
    /// The input execution plan providing data to write
    input: Arc<dyn ExecutionPlan>,
    /// Path to the output CRAM file
    output_path: String,
    /// Path to reference FASTA file (optional)
    reference_path: Option<String>,
    /// Alignment tag field names
    tag_fields: Vec<String>,
    /// Whether the input coordinates are 0-based (need +1 for CRAM POS output)
    coordinate_system_zero_based: bool,
    /// Metadata overrides to merge into the input schema before building the header
    schema_metadata_overrides: HashMap<String, String>,
    /// Whether to sort records by coordinate before writing
    sort_on_write: bool,
    /// Cached plan properties
    cache: PlanProperties,
}

impl CramWriteExec {
    /// Creates a new CRAM write execution plan
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan providing RecordBatches to write
    /// * `output_path` - Path to the output CRAM file
    /// * `reference_path` - Optional path to reference FASTA file
    /// * `tag_fields` - List of alignment tag field names
    /// * `coordinate_system_zero_based` - Whether input uses 0-based coordinates
    ///
    /// # Returns
    ///
    /// A new `CramWriteExec` instance
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        reference_path: Option<String>,
        tag_fields: Vec<String>,
        coordinate_system_zero_based: bool,
        schema_metadata_overrides: HashMap<String, String>,
        sort_on_write: bool,
    ) -> Self {
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
            reference_path,
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

    /// Returns the reference file path
    pub fn reference_path(&self) -> Option<&str> {
        self.reference_path.as_deref()
    }

    /// Returns the output schema
    fn schema(&self) -> SchemaRef {
        self.cache.eq_properties.schema().clone()
    }
}

impl Debug for CramWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CramWriteExec")
            .field("output_path", &self.output_path)
            .field("reference_path", &self.reference_path)
            .field("tag_fields", &self.tag_fields)
            .finish()
    }
}

impl DisplayAs for CramWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "CramWriteExec: path={}, reference={:?}, tag_fields={}",
            self.output_path,
            self.reference_path,
            self.tag_fields.len()
        )
    }
}

impl ExecutionPlan for CramWriteExec {
    fn name(&self) -> &str {
        "CramWriteExec"
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
                "CramWriteExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(CramWriteExec::new(
            children[0].clone(),
            self.output_path.clone(),
            self.reference_path.clone(),
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
            "CramWriteExec::execute partition={}, path={}",
            partition, self.output_path
        );

        let output_path = self.output_path.clone();
        let reference_path = self.reference_path.clone();
        let tag_fields = self.tag_fields.clone();
        let coordinate_system_zero_based = self.coordinate_system_zero_based;
        let schema_metadata_overrides = self.schema_metadata_overrides.clone();

        // If sort_on_write, wrap the input plan in a SortExec at execution time
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

        let stream = futures::stream::once(write_cram_stream(
            input,
            input_schema,
            output_path,
            reference_path,
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

/// Writes a stream of RecordBatches to a CRAM file
///
/// This is the main write function that:
/// 1. Creates a CRAM writer with reference sequence
/// 2. Builds and writes the header from schema metadata
/// 3. Converts each RecordBatch to CRAM records
/// 4. Writes all records to the file
/// 5. Returns a count of written records
#[allow(clippy::too_many_arguments)]
async fn write_cram_stream(
    mut input: SendableRecordBatchStream,
    input_schema: SchemaRef,
    output_path: String,
    reference_path: Option<String>,
    tag_fields: Vec<String>,
    coordinate_system_zero_based: bool,
    schema_metadata_overrides: HashMap<String, String>,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    debug!("Starting CRAM write to: {}", output_path);

    // Create writer with reference
    let reference_path_ref = reference_path.as_ref();
    let mut writer = CramLocalWriter::new(&output_path, reference_path_ref)?;

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
    let header = build_cram_header(&effective_schema, &tag_fields)?;

    // Error if writing mapped reads without a reference
    if reference_path.is_none() && !header.reference_sequences().is_empty() {
        return Err(DataFusionError::Execution(
            "Writing CRAM with mapped reads requires a reference FASTA file. \
             Please provide reference_path (with .fai index). \
             Without a reference, only unmapped reads can be written to CRAM."
                .to_string(),
        ));
    }

    // Write header
    writer.write_header(&header)?;

    // Process and write batches
    let mut total_count = 0u64;

    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        debug!("Writing batch with {} rows", num_rows);

        // Convert batch to CRAM records
        let records =
            batch_to_cram_records(&batch, &header, &tag_fields, coordinate_system_zero_based)?;

        // Write records
        writer.write_records(&header, &records)?;
        total_count += num_rows as u64;
    }

    // Finish writing
    writer.finish(&header)?;

    debug!("Wrote {} records to {}", total_count, output_path);

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
    async fn test_cram_write_exec_basic() -> Result<()> {
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

        // Create test data with unmapped reads (no reference needed)
        let names = StringArray::from(vec![Some("read1"), Some("read2")]);
        let chroms = StringArray::from(vec![Option::<&str>::None, Option::<&str>::None]);
        let starts = UInt32Array::from(vec![Option::<u32>::None, Option::<u32>::None]);
        let flags = UInt32Array::from(vec![4u32, 4u32]); // 0x4 = unmapped
        let cigars = StringArray::from(vec!["*", "*"]);
        let mapping_qualities = UInt32Array::from(vec![0u32, 0u32]);
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
        let temp_file = NamedTempFile::with_suffix(".cram").unwrap();
        let output_path = temp_file.path().to_string_lossy().to_string();

        // Create write exec (without reference for this test)
        let input_plan = provider
            .scan(&SessionContext::new().state(), None, &[], None)
            .await?;
        let write_exec = CramWriteExec::new(
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
