//! Serializer for converting Arrow RecordBatches to FASTA records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to FASTA format for writing to files.

use bstr::BString;
use datafusion::arrow::array::{
    Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use datafusion::common::{DataFusionError, Result};
use noodles_fasta as fasta;
use noodles_fasta::record::{Definition, Sequence};

/// Enum to hold either StringArray or LargeStringArray reference
/// This allows handling both standard Arrow Utf8 and Polars LargeUtf8 types
enum StringColumnRef<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl StringColumnRef<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            StringColumnRef::Small(arr) => arr.value(i),
            StringColumnRef::Large(arr) => arr.value(i),
            StringColumnRef::View(arr) => arr.value(i),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            StringColumnRef::Small(arr) => arr.is_null(i),
            StringColumnRef::Large(arr) => arr.is_null(i),
            StringColumnRef::View(arr) => arr.is_null(i),
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
    if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
        return Ok(StringColumnRef::View(arr));
    }

    Err(DataFusionError::Execution(format!(
        "Column {index} ({name}) must be Utf8, LargeUtf8 or Utf8View type"
    )))
}

/// Converts an Arrow RecordBatch to a vector of FASTA records.
///
/// The RecordBatch must have the following schema:
/// - Column 0: `name` (Utf8, required) - Sequence identifier
/// - Column 1: `description` (Utf8, nullable) - Sequence description
/// - Column 2: `sequence` (LargeUtf8, required) - The sequence data
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

/// Bases per sequence line, as written by noodles' `fasta::io::Writer` by default.
pub const FASTA_LINE_WIDTH: usize = 80;

/// Appends the FASTA text of every row in `batch` to `out`, and returns the
/// number of records written.
///
/// Produces the same bytes as building `fasta::Record`s with
/// [`batch_to_fasta_records`] and writing them with noodles'
/// `fasta::io::Writer` (`>name[ description]\n`, then the sequence in lines of
/// [`FASTA_LINE_WIDTH`] bases), and applies the same checks, but without
/// allocating a record per row.
///
/// On error `out` may hold the rows formatted before the failing one; the
/// caller discards it.
pub fn append_fasta_batch(batch: &RecordBatch, out: &mut Vec<u8>) -> Result<usize> {
    let num_columns = batch.num_columns();
    if num_columns < 3 {
        return Err(DataFusionError::Execution(format!(
            "FASTA batch must have at least 3 columns, got {num_columns}"
        )));
    }

    let names = get_string_column(batch, 0, "name")?;
    let descriptions = get_string_column(batch, 1, "description")?;
    let sequences = get_string_column(batch, 2, "sequence")?;

    for i in 0..batch.num_rows() {
        let name = names.value(i);
        if name.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: name cannot be empty"
            )));
        }
        let description = if descriptions.is_null(i) {
            ""
        } else {
            descriptions.value(i)
        };
        let sequence = sequences.value(i).as_bytes();
        if sequence.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: sequence cannot be empty"
            )));
        }

        let lines = sequence.len().div_ceil(FASTA_LINE_WIDTH);
        out.reserve(name.len() + description.len() + sequence.len() + lines + 3);
        out.push(b'>');
        out.extend_from_slice(name.as_bytes());
        if !description.is_empty() {
            out.push(b' ');
            out.extend_from_slice(description.as_bytes());
        }
        out.push(b'\n');
        for line in sequence.chunks(FASTA_LINE_WIDTH) {
            out.extend_from_slice(line);
            out.push(b'\n');
        }
    }

    Ok(batch.num_rows())
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
            Field::new("sequence", DataType::LargeUtf8, false),
        ]))
    }

    #[test]
    fn test_batch_to_fasta_records_basic() {
        let schema = create_test_schema();

        let names = StringArray::from(vec!["seq1", "seq2"]);
        let descriptions = StringArray::from(vec![Some("protein A"), None]);
        let sequences = LargeStringArray::from(vec!["ACGT", "TGCA"]);

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
        let sequences = LargeStringArray::from(vec!["ACGT"]);

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
        let sequences = LargeStringArray::from(vec!["ACGT"]);

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
        let sequences = LargeStringArray::from(vec![""]);

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
            Field::new("sequence", DataType::LargeUtf8, false),
        ]));

        let names = StringArray::from(vec!["seq1"]);
        let sequences = LargeStringArray::from(vec!["ACGT"]);

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

    #[test]
    fn test_append_fasta_batch_matches_noodles_writer() {
        use datafusion::arrow::array::StringViewArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use noodles_fasta::io::Writer;
        use std::sync::Arc;

        let long = "ACGT".repeat(50); // 200 bases: two full lines and a partial one
        let exact = "N".repeat(160); // exactly two lines
        let names = ["s1", "s2", "s3", "s4"];
        let descriptions = [Some("chr1 desc"), None, Some(""), Some("x")];
        let sequences = ["MKV*", long.as_str(), exact.as_str(), "A"];

        let utf8_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]));
        let utf8 = RecordBatch::try_new(
            utf8_schema,
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(descriptions.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
            ],
        )
        .unwrap();
        let view_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, false),
            Field::new("description", DataType::Utf8View, true),
            Field::new("sequence", DataType::Utf8View, false),
        ]));
        let view = RecordBatch::try_new(
            view_schema,
            vec![
                Arc::new(StringViewArray::from(names.to_vec())),
                Arc::new(StringViewArray::from(descriptions.to_vec())),
                Arc::new(StringViewArray::from(sequences.to_vec())),
            ],
        )
        .unwrap();

        let mut expected = Writer::new(Vec::new());
        for record in batch_to_fasta_records(&utf8).unwrap() {
            expected.write_record(&record).unwrap();
        }
        let expected = expected.into_inner();

        for batch in [&utf8, &view] {
            let mut out = Vec::new();
            assert_eq!(append_fasta_batch(batch, &mut out).unwrap(), 4);
            assert_eq!(out, expected);
        }
    }
}
