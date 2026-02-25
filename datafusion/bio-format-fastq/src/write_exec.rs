//! Write execution plan for FASTQ files
//!
//! This module provides the physical execution plan for writing DataFusion
//! query results to FASTQ files.

use crate::serializer::batch_to_fastq_records;
use crate::writer::{FastqCompressionType, FastqLocalWriter};
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::StreamExt;
use log::debug;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for writing FASTQ files
///
/// This execution plan consumes input RecordBatches, converts them to FASTQ records,
/// and writes them to a local file with optional compression.
///
/// The plan returns a single-row RecordBatch with a `count` column containing
/// the total number of records written.
pub struct FastqWriteExec {
    /// The input execution plan providing data to write
    input: Arc<dyn ExecutionPlan>,
    /// Path to the output FASTQ file
    output_path: String,
    /// Compression type for the output file
    compression: FastqCompressionType,
    /// Cached plan properties
    cache: PlanProperties,
}

impl FastqWriteExec {
    /// Creates a new FASTQ write execution plan
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan providing RecordBatches to write
    /// * `output_path` - Path to the output FASTQ file
    /// * `compression` - Compression type (defaults to auto-detection from path)
    ///
    /// # Returns
    ///
    /// A new `FastqWriteExec` instance
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        compression: Option<FastqCompressionType>,
    ) -> Self {
        let compression =
            compression.unwrap_or_else(|| FastqCompressionType::from_path(&output_path));

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
            cache,
        }
    }

    /// Returns the output file path
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    /// Returns the compression type
    pub fn compression(&self) -> FastqCompressionType {
        self.compression
    }

    /// Returns the output schema
    fn schema(&self) -> SchemaRef {
        self.cache.eq_properties.schema().clone()
    }
}

impl Debug for FastqWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastqWriteExec")
            .field("output_path", &self.output_path)
            .field("compression", &self.compression)
            .finish()
    }
}

impl DisplayAs for FastqWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FastqWriteExec: path={}, compression={:?}",
            self.output_path, self.compression
        )
    }
}

impl ExecutionPlan for FastqWriteExec {
    fn name(&self) -> &str {
        "FastqWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
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
                "FastqWriteExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(FastqWriteExec::new(
            children[0].clone(),
            self.output_path.clone(),
            Some(self.compression),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "FastqWriteExec::execute partition={}, path={}",
            partition, self.output_path
        );

        let output_path = self.output_path.clone();
        let compression = self.compression;
        let input = self.input.execute(0, context)?;
        let output_schema = self.schema();

        let stream = futures::stream::once(async move {
            write_fastq_stream(input, output_path, compression, output_schema).await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Writes records from a stream to a FASTQ file
async fn write_fastq_stream(
    mut input: SendableRecordBatchStream,
    output_path: String,
    compression: FastqCompressionType,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut writer = FastqLocalWriter::with_compression(&output_path, compression)?;
    let mut total_count: u64 = 0;

    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;

        if batch.num_rows() == 0 {
            continue;
        }

        let records = batch_to_fastq_records(&batch)?;
        total_count += records.len() as u64;
        writer.write_records(&records)?;
    }

    writer.finish()?;

    debug!("FastqWriteExec: wrote {total_count} records to {output_path}");

    // Return a batch with the count
    let count_array = Arc::new(UInt64Array::from(vec![total_count]));
    RecordBatch::try_new(output_schema, vec![count_array])
        .map_err(|e| DataFusionError::Execution(format!("Failed to create result batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fastq_write_exec_properties() {
        // Create a mock input - for actual testing, the serializer and writer tests cover the functionality
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ]));

        let output_path = "/tmp/test.fastq".to_string();
        let write_exec = FastqWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema)),
            output_path.clone(),
            None,
        );

        assert_eq!(write_exec.output_path(), output_path);
        assert_eq!(write_exec.compression(), FastqCompressionType::Plain);
        assert_eq!(write_exec.name(), "FastqWriteExec");
    }

    #[test]
    fn test_compression_detection_in_write_exec() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        // Test BGZF detection
        let bgzf_exec = FastqWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.fastq.bgz".to_string(),
            None,
        );
        assert_eq!(bgzf_exec.compression(), FastqCompressionType::Bgzf);

        // Test GZIP detection
        let gzip_exec = FastqWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.fastq.gz".to_string(),
            None,
        );
        assert_eq!(gzip_exec.compression(), FastqCompressionType::Gzip);
    }
}
