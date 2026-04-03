//! Integration tests for FASTA write functionality
//!
//! Tests verify that FASTA files can be written correctly through the SQL
//! INSERT OVERWRITE path and that data round-trips properly through
//! read -> write -> read cycles across all compression formats.

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fasta::FastaTableProvider;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: generate an uncompressed FASTA file with `num_records` records.
fn generate_test_fasta(path: &str, num_records: usize) {
    let mut file = std::fs::File::create(path).expect("Failed to create test file");
    for i in 0..num_records {
        writeln!(file, ">seq_{i} sample description {i}").unwrap();
        writeln!(file, "ACGTACGTACGTACGT").unwrap();
    }
}

// ---- SQL round-trip tests ----

#[tokio::test]
async fn test_write_plain_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta");

    let num_records = 50;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider = FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .expect("Failed to execute INSERT")
        .collect()
        .await
        .expect("Failed to collect results");

    assert_eq!(result.len(), 1);
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, num_records as u64);

    // Read back the output file and verify data integrity
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT * FROM written_fasta ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, num_records,
        "Expected {num_records} rows, got {total_rows}"
    );

    // Verify all names are unique
    let mut names = HashSet::new();
    for batch in &batches {
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            names.insert(name_array.value(i).to_string());
        }
    }
    assert_eq!(names.len(), num_records);
}

#[tokio::test]
async fn test_write_gzip_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta.gz");

    let num_records = 50;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider = FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, num_records as u64);

    // Read back the gzip file (GZIP reader path requires ObjectStorageOptions)
    let ctx2 = SessionContext::new();
    let read_provider = FastaTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        Some(ObjectStorageOptions::default()),
    )
    .unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

#[tokio::test]
async fn test_write_bgzf_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta.bgz");

    let num_records = 50;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider = FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, num_records as u64);

    // Read back the bgzf file
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

// ---- Data integrity tests ----

#[tokio::test]
async fn test_write_preserves_all_fields() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta");

    // Create a file with specific known content
    {
        let mut file = std::fs::File::create(&input_path).unwrap();
        writeln!(file, ">seq_alpha description text here").unwrap();
        writeln!(file, "ACGTACGT").unwrap();
        writeln!(file, ">seq_beta").unwrap();
        writeln!(file, "TTTTGGGG").unwrap();
    }

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify each field
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT name, description, sequence FROM written_fasta ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let descriptions = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Row 0: seq_alpha (sorted first)
    assert_eq!(names.value(0), "seq_alpha");
    assert_eq!(descriptions.value(0), "description text here");
    assert_eq!(sequences.value(0), "ACGTACGT");

    // Row 1: seq_beta
    assert_eq!(names.value(1), "seq_beta");
    assert_eq!(sequences.value(1), "TTTTGGGG");
}

#[tokio::test]
async fn test_write_with_sql_filter() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("filtered.fasta");

    let num_records = 100;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    // Write only records whose name starts with 'seq_1' (seq_1, seq_10..seq_19)
    ctx.sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta WHERE name LIKE 'seq_1%'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("filtered_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM filtered_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // seq_1, seq_10, seq_11, ..., seq_19 = 11 records
    assert_eq!(
        total_rows, 11,
        "Expected 11 filtered rows, got {total_rows}"
    );

    // Verify all names match the filter
    for batch in &batches {
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                name_array.value(i).starts_with("seq_1"),
                "Name {} should start with 'seq_1'",
                name_array.value(i)
            );
        }
    }
}

// ---- Cross-format round-trip tests ----

#[tokio::test]
async fn test_write_plain_to_bgzf_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta.bgz");

    let num_records = 30;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back the BGZF file and verify all records
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT * FROM written_fasta ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);

    // Verify data integrity
    let mut names = HashSet::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 3);
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seq_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            names.insert(name_array.value(i).to_string());
            assert_eq!(seq_array.value(i), "ACGTACGTACGTACGT");
        }
    }
    assert_eq!(names.len(), num_records);
}

#[tokio::test]
async fn test_write_bgzf_to_plain_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let bgzf_path = tmp_dir.path().join("intermediate.fasta.bgz");
    let output_path = tmp_dir.path().join("output.fasta");

    let num_records = 30;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    // First write plain -> bgzf
    let ctx = SessionContext::new();
    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let bgzf_provider =
        FastaTableProvider::new(bgzf_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("bgzf_fasta", Arc::new(bgzf_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE bgzf_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Then write bgzf -> plain
    let ctx2 = SessionContext::new();
    let bgzf_read_provider =
        FastaTableProvider::new(bgzf_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("bgzf_fasta", Arc::new(bgzf_read_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx2
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM bgzf_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, num_records as u64);

    // Read back the plain file
    let ctx3 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx3.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx3.sql("SELECT name FROM written_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

// ---- Edge case tests ----

#[tokio::test]
async fn test_write_single_record() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("single.fasta");
    let output_path = tmp_dir.path().join("single_out.fasta");

    generate_test_fasta(input_path.to_str().unwrap(), 1);

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);

    // Read back
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT * FROM written_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn test_write_large_record_count() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("large.fasta");
    let output_path = tmp_dir.path().join("large_out.fasta.bgz");

    let num_records = 5000;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, num_records as u64);

    // Read back the BGZF file
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fasta").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

#[tokio::test]
async fn test_write_append_not_supported() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta");

    generate_test_fasta(input_path.to_str().unwrap(), 5);

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    // INSERT INTO (append) should fail
    let result = async {
        let df = ctx
            .sql("INSERT INTO output_fasta SELECT * FROM input_fasta")
            .await?;
        df.collect().await
    }
    .await;

    assert!(
        result.is_err(),
        "INSERT INTO (append) should not be supported"
    );
}

#[tokio::test]
async fn test_write_preserves_long_sequences() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta");

    // Create a file with sequences longer than the 80-char line wrap
    {
        let mut file = std::fs::File::create(&input_path).unwrap();
        let long_seq = "A".repeat(200);
        writeln!(file, ">long_seq protein sequence").unwrap();
        writeln!(file, "{long_seq}").unwrap();
    }

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify the full sequence is preserved
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT name, sequence FROM written_fasta")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let sequences = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sequences.value(0).len(), 200);
    assert_eq!(sequences.value(0), "A".repeat(200));
}

#[tokio::test]
async fn test_write_with_no_description() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");
    let output_path = tmp_dir.path().join("output.fasta");

    {
        let mut file = std::fs::File::create(&input_path).unwrap();
        writeln!(file, ">seq_no_desc").unwrap();
        writeln!(file, "MKTLLIFLAG").unwrap();
    }

    let ctx = SessionContext::new();

    let input_provider =
        FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fasta", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fasta SELECT * FROM input_fasta")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify
    let ctx2 = SessionContext::new();
    let read_provider =
        FastaTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fasta", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT name, description, sequence FROM written_fasta")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "seq_no_desc");
    assert_eq!(sequences.value(0), "MKTLLIFLAG");
}
