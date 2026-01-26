//! Serializer for converting Arrow RecordBatches to FASTQ records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to FASTQ format for writing to files.

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::common::{DataFusionError, Result};
use noodles_fastq as fastq;
use noodles_fastq::record::Definition;

/// Converts an Arrow RecordBatch to a vector of FASTQ records.
///
/// The RecordBatch must have the following schema:
/// - Column 0: `name` (Utf8, required) - Sequence identifier
/// - Column 1: `description` (Utf8, nullable) - Sequence description
/// - Column 2: `sequence` (Utf8, required) - DNA/RNA sequence
/// - Column 3: `quality_scores` (Utf8, required) - Quality scores (Phred+33 encoded)
///
/// # Arguments
///
/// * `batch` - The Arrow RecordBatch to convert
///
/// # Returns
///
/// A vector of FASTQ records that can be written to a file
///
/// # Errors
///
/// Returns an error if:
/// - The batch schema does not match the expected FASTQ schema
/// - Any required column contains null values
/// - Column types are not Utf8
pub fn batch_to_fastq_records(batch: &RecordBatch) -> Result<Vec<fastq::Record>> {
    let num_columns = batch.num_columns();

    // Validate schema - we need at least 4 columns (name, description, sequence, quality_scores)
    if num_columns < 4 {
        return Err(DataFusionError::Execution(format!(
            "FASTQ batch must have at least 4 columns, got {}",
            num_columns
        )));
    }

    // Get columns by position (matching the schema defined in table_provider.rs)
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Column 0 (name) must be Utf8 type".to_string())
        })?;

    let descriptions = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Column 1 (description) must be Utf8 type".to_string())
        })?;

    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Column 2 (sequence) must be Utf8 type".to_string())
        })?;

    let quality_scores = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Column 3 (quality_scores) must be Utf8 type".to_string())
        })?;

    let mut records = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        // Name is required
        let name = names.value(i);
        if name.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {}: name cannot be empty",
                i
            )));
        }

        // Description is optional (can be null or empty)
        let description = if descriptions.is_null(i) {
            ""
        } else {
            descriptions.value(i)
        };

        // Sequence is required
        let sequence = sequences.value(i);
        if sequence.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {}: sequence cannot be empty",
                i
            )));
        }

        // Quality scores are required
        let quality = quality_scores.value(i);
        if quality.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {}: quality_scores cannot be empty",
                i
            )));
        }

        // Validate sequence and quality lengths match
        if sequence.len() != quality.len() {
            return Err(DataFusionError::Execution(format!(
                "Row {}: sequence length ({}) must match quality_scores length ({})",
                i,
                sequence.len(),
                quality.len()
            )));
        }

        // Build the FASTQ record
        // Definition contains name and optional description
        let definition = if description.is_empty() {
            Definition::new(name, "")
        } else {
            Definition::new(name, description)
        };

        let record = fastq::Record::new(
            definition,
            sequence.as_bytes().to_vec(),
            quality.as_bytes().to_vec(),
        );

        records.push(record);
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_batch_to_fastq_records_basic() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1", "seq2"]);
        let descriptions = StringArray::from(vec![Some("desc1"), None]);
        let sequences = StringArray::from(vec!["ACGT", "TGCA"]);
        let quality_scores = StringArray::from(vec!["IIII", "HHHH"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(names),
                Arc::new(descriptions),
                Arc::new(sequences),
                Arc::new(quality_scores),
            ],
        )
        .unwrap();

        let records = batch_to_fastq_records(&batch).unwrap();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].name(), "seq1");
        assert_eq!(records[0].description(), "desc1");
        assert_eq!(records[0].sequence(), b"ACGT");
        assert_eq!(records[0].quality_scores(), b"IIII");

        assert_eq!(records[1].name(), "seq2");
        assert_eq!(records[1].description(), "");
        assert_eq!(records[1].sequence(), b"TGCA");
        assert_eq!(records[1].quality_scores(), b"HHHH");
    }

    #[test]
    fn test_batch_to_fastq_records_empty_description() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1"]);
        let descriptions = StringArray::from(vec![Some("")]);
        let sequences = StringArray::from(vec!["ACGT"]);
        let quality_scores = StringArray::from(vec!["IIII"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(names),
                Arc::new(descriptions),
                Arc::new(sequences),
                Arc::new(quality_scores),
            ],
        )
        .unwrap();

        let records = batch_to_fastq_records(&batch).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].description(), "");
    }

    #[test]
    fn test_batch_to_fastq_records_length_mismatch() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1"]);
        let descriptions = StringArray::from(vec![Some("")]);
        let sequences = StringArray::from(vec!["ACGT"]);
        let quality_scores = StringArray::from(vec!["III"]); // Length mismatch

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(names),
                Arc::new(descriptions),
                Arc::new(sequences),
                Arc::new(quality_scores),
            ],
        )
        .unwrap();

        let result = batch_to_fastq_records(&batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length"));
    }
}
