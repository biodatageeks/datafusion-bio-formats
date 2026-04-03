//! Serializer for converting Arrow RecordBatches to FASTA records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to FASTA format for writing to files.

use bstr::BString;
use datafusion::arrow::array::{Array, LargeStringArray, RecordBatch, StringArray};
use datafusion::common::{DataFusionError, Result};
use noodles_fasta as fasta;
use noodles_fasta::record::{Definition, Sequence};

/// Enum to hold either StringArray or LargeStringArray reference
/// This allows handling both standard Arrow Utf8 and Polars LargeUtf8 types
enum StringColumnRef<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl StringColumnRef<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            StringColumnRef::Small(arr) => arr.value(i),
            StringColumnRef::Large(arr) => arr.value(i),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            StringColumnRef::Small(arr) => Array::is_null(*arr, i),
            StringColumnRef::Large(arr) => Array::is_null(*arr, i),
        }
    }
}

/// Gets a string column from the batch (supports both Utf8 and LargeUtf8)
fn get_string_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<StringColumnRef<'a>> {
    let column = batch.column(index);

    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(StringColumnRef::Small(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(StringColumnRef::Large(arr));
    }

    Err(DataFusionError::Execution(format!(
        "Column {index} ({name}) must be Utf8 or LargeUtf8 type"
    )))
}

/// Converts an Arrow RecordBatch to a vector of FASTA records.
///
/// The RecordBatch must have the following schema:
/// - Column 0: `name` (Utf8, required) - Sequence identifier
/// - Column 1: `description` (Utf8, nullable) - Sequence description
/// - Column 2: `sequence` (Utf8, required) - The sequence data
pub fn batch_to_fasta_records(batch: &RecordBatch) -> Result<Vec<fasta::Record>> {
    let num_columns = batch.num_columns();

    if num_columns < 3 {
        return Err(DataFusionError::Execution(format!(
            "FASTA batch must have at least 3 columns, got {num_columns}"
        )));
    }

    let names = get_string_column(batch, 0, "name")?;
    let descriptions = get_string_column(batch, 1, "description")?;
    let sequences = get_string_column(batch, 2, "sequence")?;

    let mut records = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let name = names.value(i);
        if name.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: name cannot be empty"
            )));
        }

        let description = if descriptions.is_null(i) {
            None
        } else {
            let desc = descriptions.value(i);
            if desc.is_empty() { None } else { Some(desc) }
        };

        let sequence_str = sequences.value(i);
        if sequence_str.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: sequence cannot be empty"
            )));
        }

        let definition = Definition::new(name, description.map(BString::from));
        let sequence = Sequence::from(sequence_str.as_bytes().to_vec());
        let record = fasta::Record::new(definition, sequence);

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
        ]))
    }

    #[test]
    fn test_batch_to_fasta_records_basic() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1", "seq2"]);
        let descriptions = StringArray::from(vec![Some("protein A"), None]);
        let sequences = StringArray::from(vec!["ACGT", "TGCA"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap();

        let records = batch_to_fasta_records(&batch).unwrap();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].name(), b"seq1");
        assert_eq!(records[0].description(), Some(bstr::BStr::new("protein A")));
        assert_eq!(records[0].sequence().as_ref(), b"ACGT");

        assert_eq!(records[1].name(), b"seq2");
        assert_eq!(records[1].description(), None);
        assert_eq!(records[1].sequence().as_ref(), b"TGCA");
    }

    #[test]
    fn test_batch_to_fasta_records_empty_description() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1"]);
        let descriptions = StringArray::from(vec![Some("")]);
        let sequences = StringArray::from(vec!["ACGT"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap();

        let records = batch_to_fasta_records(&batch).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].description(), None);
    }

    #[test]
    fn test_batch_to_fasta_records_empty_name_error() {
        let schema = create_test_schema();

        let names = StringArray::from(vec![""]);
        let descriptions = StringArray::from(vec![Some("")]);
        let sequences = StringArray::from(vec!["ACGT"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap();

        let result = batch_to_fasta_records(&batch);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("name cannot be empty")
        );
    }

    #[test]
    fn test_batch_to_fasta_records_empty_sequence_error() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1"]);
        let descriptions = StringArray::from(vec![Some("")]);
        let sequences = StringArray::from(vec![""]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap();

        let result = batch_to_fasta_records(&batch);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("sequence cannot be empty")
        );
    }

    #[test]
    fn test_batch_to_fasta_records_too_few_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("sequence", DataType::Utf8, false),
        ]));

        let names = StringArray::from(vec!["seq1"]);
        let sequences = StringArray::from(vec!["ACGT"]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(names), Arc::new(sequences)]).unwrap();

        let result = batch_to_fasta_records(&batch);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least 3 columns")
        );
    }

    #[test]
    fn test_batch_to_fasta_records_large_string_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, false),
            Field::new("description", DataType::LargeUtf8, true),
            Field::new("sequence", DataType::LargeUtf8, false),
        ]));

        let names = LargeStringArray::from(vec!["seq_polars"]);
        let descriptions = LargeStringArray::from(vec![Some("from polars")]);
        let sequences = LargeStringArray::from(vec!["ACGTACGT"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap();

        let records = batch_to_fasta_records(&batch).unwrap();
        assert_eq!(records.len(), 1);

        assert_eq!(records[0].name(), b"seq_polars");
        assert_eq!(
            records[0].description(),
            Some(bstr::BStr::new("from polars"))
        );
        assert_eq!(records[0].sequence().as_ref(), b"ACGTACGT");
    }
}
