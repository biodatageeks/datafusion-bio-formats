//! Integration tests for FASTQ write functionality
//!
//! Tests verify that FASTQ files can be written correctly through the SQL
//! INSERT OVERWRITE path and that data round-trips properly through
//! read -> write -> read cycles across all compression formats.

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: generate an uncompressed FASTQ file with `num_records` records.
fn generate_test_fastq(path: &str, num_records: usize) {
    let mut file = std::fs::File::create(path).expect("Failed to create test file");
    for i in 0..num_records {
        writeln!(file, "@read_{} sample description {}", i, i).unwrap();
        writeln!(file, "ACGTACGTACGTACGT").unwrap();
        writeln!(file, "+").unwrap();
        writeln!(file, "IIIIIIIIIIIIIIII").unwrap();
    }
}

// ---- SQL round-trip tests ----

#[tokio::test]
async fn test_write_plain_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq");

    let num_records = 50;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    // Register input table
    let input_provider = FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    // Register output table
    let output_provider = FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    // Write via SQL
    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
        .await
        .expect("Failed to execute INSERT")
        .collect()
        .await
        .expect("Failed to collect results");

    // Verify the write returned a count
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
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT * FROM written_fastq ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, num_records,
        "Expected {} rows, got {}",
        num_records, total_rows
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
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq.gz");

    let num_records = 50;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider = FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
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

    // Read back the gzip file
    let ctx2 = SessionContext::new();
    let read_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

#[tokio::test]
async fn test_write_bgzf_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq.bgz");

    let num_records = 50;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider = FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create output provider");
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
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
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

// ---- Data integrity tests ----

#[tokio::test]
async fn test_write_preserves_all_fields() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq");

    // Create a file with specific known content
    {
        let mut file = std::fs::File::create(&input_path).unwrap();
        writeln!(file, "@seq_alpha description text here").unwrap();
        writeln!(file, "ACGTACGT").unwrap();
        writeln!(file, "+").unwrap();
        writeln!(file, "IIIHHHHH").unwrap();
        writeln!(file, "@seq_beta").unwrap();
        writeln!(file, "TTTTGGGG").unwrap();
        writeln!(file, "+").unwrap();
        writeln!(file, "AAAABBBB").unwrap();
    }

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify each field
    let ctx2 = SessionContext::new();
    let read_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT name, description, sequence, quality_scores FROM written_fastq ORDER BY name")
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
    let quality_scores = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Row 0: seq_alpha (sorted first)
    assert_eq!(names.value(0), "seq_alpha");
    assert_eq!(descriptions.value(0), "description text here");
    assert_eq!(sequences.value(0), "ACGTACGT");
    assert_eq!(quality_scores.value(0), "IIIHHHHH");

    // Row 1: seq_beta
    assert_eq!(names.value(1), "seq_beta");
    assert_eq!(sequences.value(1), "TTTTGGGG");
    assert_eq!(quality_scores.value(1), "AAAABBBB");
}

#[tokio::test]
async fn test_write_with_sql_filter() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("filtered.fastq");

    let num_records = 100;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    // Write only records whose name starts with 'read_1' (read_1, read_10..read_19)
    ctx.sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq WHERE name LIKE 'read_1%'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back and verify
    let ctx2 = SessionContext::new();
    let read_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("filtered_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM filtered_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // read_1, read_10, read_11, ..., read_19 = 11 records
    assert_eq!(
        total_rows, 11,
        "Expected 11 filtered rows, got {}",
        total_rows
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
                name_array.value(i).starts_with("read_1"),
                "Name {} should start with 'read_1'",
                name_array.value(i)
            );
        }
    }
}

// ---- Cross-format round-trip tests ----

#[tokio::test]
async fn test_write_plain_to_bgzf_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq.bgz");

    let num_records = 30;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    ctx.sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back the BGZF file and verify all records
    let ctx2 = SessionContext::new();
    let read_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2
        .sql("SELECT * FROM written_fastq ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);

    // Verify data integrity
    let mut names = HashSet::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 4);
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
        let qual_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            names.insert(name_array.value(i).to_string());
            assert_eq!(seq_array.value(i), "ACGTACGTACGTACGT");
            assert_eq!(qual_array.value(i), "IIIIIIIIIIIIIIII");
        }
    }
    assert_eq!(names.len(), num_records);
}

#[tokio::test]
async fn test_write_bgzf_to_plain_round_trip() {
    let tmp_dir = TempDir::new().unwrap();
    let output_path = tmp_dir.path().join("output.fastq");

    // Read from the test BGZF file shipped with the crate
    let input_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));

    let ctx = SessionContext::new();

    let input_provider = FastqTableProvider::new(input_path.clone(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
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
    assert_eq!(count, 2000);

    // Read back the plain file
    let ctx2 = SessionContext::new();
    let read_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2000);
}

// ---- Edge case tests ----

#[tokio::test]
async fn test_write_single_record() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("single.fastq");
    let output_path = tmp_dir.path().join("single_out.fastq");

    generate_test_fastq(input_path.to_str().unwrap(), 1);

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
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
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT * FROM written_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn test_write_large_record_count() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("large.fastq");
    let output_path = tmp_dir.path().join("large_out.fastq.bgz");

    let num_records = 5000;
    generate_test_fastq(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    let result = ctx
        .sql("INSERT OVERWRITE output_fastq SELECT * FROM input_fastq")
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
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx2.register_table("written_fastq", Arc::new(read_provider))
        .unwrap();

    let df = ctx2.sql("SELECT name FROM written_fastq").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_records);
}

#[tokio::test]
async fn test_write_append_not_supported() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fastq");
    let output_path = tmp_dir.path().join("output.fastq");

    generate_test_fastq(input_path.to_str().unwrap(), 5);

    let ctx = SessionContext::new();

    let input_provider =
        FastqTableProvider::new(input_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("input_fastq", Arc::new(input_provider))
        .unwrap();

    let output_provider =
        FastqTableProvider::new(output_path.to_str().unwrap().to_string(), None).unwrap();
    ctx.register_table("output_fastq", Arc::new(output_provider))
        .unwrap();

    // INSERT INTO (append) should fail — the error may surface at plan or execution time
    let result = async {
        let df = ctx
            .sql("INSERT INTO output_fastq SELECT * FROM input_fastq")
            .await?;
        df.collect().await
    }
    .await;

    assert!(
        result.is_err(),
        "INSERT INTO (append) should not be supported"
    );
}
