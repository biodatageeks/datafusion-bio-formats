//! Serializer for converting Arrow RecordBatches to FASTQ records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to FASTQ format for writing to files.

use datafusion::arrow::array::{
    Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use datafusion::common::{DataFusionError, Result};
use noodles_fastq as fastq;
use noodles_fastq::record::Definition;

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
            StringColumnRef::Small(arr) => Array::is_null(*arr, i),
            StringColumnRef::Large(arr) => Array::is_null(*arr, i),
            StringColumnRef::View(arr) => Array::is_null(*arr, i),
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

    // Try StringArray first, then LargeStringArray
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
            "FASTQ batch must have at least 4 columns, got {num_columns}"
        )));
    }

    // Get columns by position (matching the schema defined in table_provider.rs)
    // Supports both Utf8 (StringArray) and LargeUtf8 (LargeStringArray) for Polars compatibility
    let names = get_string_column(batch, 0, "name")?;
    let descriptions = get_string_column(batch, 1, "description")?;
    let sequences = get_string_column(batch, 2, "sequence")?;
    let quality_scores = get_string_column(batch, 3, "quality_scores")?;

    let mut records = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        // Name is required
        let name = names.value(i);
        if name.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: name cannot be empty"
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
                "Row {i}: sequence cannot be empty"
            )));
        }

        // Quality scores are required
        let quality = quality_scores.value(i);
        if quality.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: quality_scores cannot be empty"
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

/// Appends the FASTQ text of every row in `batch` to `out`, and returns the
/// number of records written.
///
/// Produces the same bytes as building `fastq::Record`s with
/// [`batch_to_fastq_records`] and writing them with noodles'
/// `fastq::io::Writer` (`@name[ description]\nSEQUENCE\n+\nQUALITY\n`), and
/// applies the same checks, but without allocating a record per row. `out` is
/// reused across batches by the caller, so a batch costs one `write_all`.
///
/// On error `out` may hold the rows formatted before the failing one; the
/// caller discards it.
pub fn append_fastq_batch(batch: &RecordBatch, out: &mut Vec<u8>) -> Result<usize> {
    let num_columns = batch.num_columns();
    if num_columns < 4 {
        return Err(DataFusionError::Execution(format!(
            "FASTQ batch must have at least 4 columns, got {num_columns}"
        )));
    }

    let names = get_string_column(batch, 0, "name")?;
    let descriptions = get_string_column(batch, 1, "description")?;
    let sequences = get_string_column(batch, 2, "sequence")?;
    let quality_scores = get_string_column(batch, 3, "quality_scores")?;

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
        let sequence = sequences.value(i);
        if sequence.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: sequence cannot be empty"
            )));
        }
        let quality = quality_scores.value(i);
        if quality.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Row {i}: quality_scores cannot be empty"
            )));
        }
        if sequence.len() != quality.len() {
            return Err(DataFusionError::Execution(format!(
                "Row {}: sequence length ({}) must match quality_scores length ({})",
                i,
                sequence.len(),
                quality.len()
            )));
        }

        out.reserve(name.len() + description.len() + 2 * sequence.len() + 7);
        out.push(b'@');
        out.extend_from_slice(name.as_bytes());
        if !description.is_empty() {
            out.push(b' ');
            out.extend_from_slice(description.as_bytes());
        }
        out.push(b'\n');
        out.extend_from_slice(sequence.as_bytes());
        out.extend_from_slice(b"\n+\n");
        out.extend_from_slice(quality.as_bytes());
        out.push(b'\n');
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

    #[test]
    fn test_batch_to_fastq_records_large_string_array() {
        // Test with LargeUtf8 (LargeStringArray) - Polars default string type
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, false),
            Field::new("description", DataType::LargeUtf8, true),
            Field::new("sequence", DataType::LargeUtf8, false),
            Field::new("quality_scores", DataType::LargeUtf8, false),
        ]));

        let names = LargeStringArray::from(vec!["seq_polars"]);
        let descriptions = LargeStringArray::from(vec![Some("from polars")]);
        let sequences = LargeStringArray::from(vec!["ACGTACGT"]);
        let quality_scores = LargeStringArray::from(vec!["IIIIIIII"]);

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

        assert_eq!(records[0].name(), "seq_polars");
        assert_eq!(records[0].description(), "from polars");
        assert_eq!(records[0].sequence(), b"ACGTACGT");
        assert_eq!(records[0].quality_scores(), b"IIIIIIII");
    }

    #[test]
    fn test_append_fastq_batch_matches_noodles_writer() {
        use datafusion::arrow::array::StringViewArray;
        use noodles_fastq::io::Writer;

        let names = ["r1", "r2", "r3"];
        let descriptions = [Some("lane=1 x"), None, Some("")];
        let sequences = ["ACGTN", "T", "GGCCAATT"];
        let qualities = ["IIII#", "!", "ABCDEFGH"];

        let utf8 = RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(descriptions.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
                Arc::new(StringArray::from(qualities.to_vec())),
            ],
        )
        .unwrap();
        let view_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, false),
            Field::new("description", DataType::Utf8View, true),
            Field::new("sequence", DataType::Utf8View, false),
            Field::new("quality_scores", DataType::Utf8View, false),
        ]));
        let view = RecordBatch::try_new(
            view_schema,
            vec![
                Arc::new(StringViewArray::from(names.to_vec())),
                Arc::new(StringViewArray::from(descriptions.to_vec())),
                Arc::new(StringViewArray::from(sequences.to_vec())),
                Arc::new(StringViewArray::from(qualities.to_vec())),
            ],
        )
        .unwrap();

        let mut expected = Writer::new(Vec::new());
        for record in batch_to_fastq_records(&utf8).unwrap() {
            expected.write_record(&record).unwrap();
        }
        let expected = expected.into_inner();

        for batch in [&utf8, &view] {
            let mut out = Vec::new();
            assert_eq!(append_fastq_batch(batch, &mut out).unwrap(), 3);
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn test_append_fastq_batch_rejects_length_mismatch() {
        let batch = RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(StringArray::from(vec!["r1"])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["ACGT"])),
                Arc::new(StringArray::from(vec!["II"])),
            ],
        )
        .unwrap();
        let err = append_fastq_batch(&batch, &mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("must match quality_scores length"));
    }
}
