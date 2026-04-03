//! Write execution plan for FASTA files
//!
//! This module provides the physical execution plan for writing DataFusion
//! query results to FASTA files.

use crate::serializer::batch_to_fasta_records;
use crate::writer::{FastaCompressionType, FastaLocalWriter};
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

/// Physical execution plan for writing FASTA files
///
/// This execution plan consumes input RecordBatches, converts them to FASTA records,
/// and writes them to a local file with optional compression.
///
/// The plan returns a single-row RecordBatch with a `count` column containing
/// the total number of records written.
pub struct FastaWriteExec {
    input: Arc<dyn ExecutionPlan>,
    output_path: String,
    compression: FastaCompressionType,
    cache: PlanProperties,
}

impl FastaWriteExec {
    /// Creates a new FASTA write execution plan
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        compression: Option<FastaCompressionType>,
    ) -> Self {
        let compression =
            compression.unwrap_or_else(|| FastaCompressionType::from_path(&output_path));

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
    pub fn compression(&self) -> FastaCompressionType {
        self.compression
    }

    fn schema(&self) -> SchemaRef {
        self.cache.eq_properties.schema().clone()
    }
}

impl Debug for FastaWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastaWriteExec")
            .field("output_path", &self.output_path)
            .field("compression", &self.compression)
            .finish()
    }
}

impl DisplayAs for FastaWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FastaWriteExec: path={}, compression={:?}",
            self.output_path, self.compression
        )
    }
}

impl ExecutionPlan for FastaWriteExec {
    fn name(&self) -> &str {
        "FastaWriteExec"
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
                "FastaWriteExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(FastaWriteExec::new(
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
            "FastaWriteExec::execute partition={}, path={}",
            partition, self.output_path
        );

        let output_path = self.output_path.clone();
        let compression = self.compression;
        let input = self.input.execute(0, context)?;
        let output_schema = self.schema();

        let stream = futures::stream::once(async move {
            write_fasta_stream(input, output_path, compression, output_schema).await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Writes records from a stream to a FASTA file
async fn write_fasta_stream(
    mut input: SendableRecordBatchStream,
    output_path: String,
    compression: FastaCompressionType,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut writer = FastaLocalWriter::with_compression(&output_path, compression)?;
    let mut total_count: u64 = 0;

    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;

        if batch.num_rows() == 0 {
            continue;
        }

        let records = batch_to_fasta_records(&batch)?;
        total_count += records.len() as u64;
        writer.write_records(&records)?;
    }

    writer.finish()?;

    debug!("FastaWriteExec: wrote {total_count} records to {output_path}");

    let count_array = Arc::new(UInt64Array::from(vec![total_count]));
    RecordBatch::try_new(output_schema, vec![count_array])
        .map_err(|e| DataFusionError::Execution(format!("Failed to create result batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fasta_write_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]));

        let output_path = "/tmp/test.fasta".to_string();
        let write_exec = FastaWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema)),
            output_path.clone(),
            None,
        );

        assert_eq!(write_exec.output_path(), output_path);
        assert_eq!(write_exec.compression(), FastaCompressionType::Plain);
        assert_eq!(write_exec.name(), "FastaWriteExec");
    }

    #[test]
    fn test_compression_detection_in_write_exec() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let bgzf_exec = FastaWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.fasta.bgz".to_string(),
            None,
        );
        assert_eq!(bgzf_exec.compression(), FastaCompressionType::Bgzf);

        let gzip_exec = FastaWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.fasta.gz".to_string(),
            None,
        );
        assert_eq!(gzip_exec.compression(), FastaCompressionType::Gzip);
    }
}
