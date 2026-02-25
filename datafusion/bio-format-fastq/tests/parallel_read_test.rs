use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

/// Generate an uncompressed FASTQ file with `num_records` records.
fn generate_test_fastq(path: &str, num_records: usize) {
    let mut file = std::fs::File::create(path).expect("Failed to create test file");
    for i in 0..num_records {
        writeln!(file, "@read_{i}").unwrap();
        writeln!(file, "ACGTACGTACGTACGT").unwrap();
        writeln!(file, "+").unwrap();
        writeln!(file, "IIIIIIIIIIIIIIII").unwrap();
    }
}

// ---- BGZF parallel read tests ----

#[tokio::test]
async fn test_bgzf_parallel_row_count() {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));
    for partitions in 1..=8 {
        let config = SessionConfig::new().with_target_partitions(partitions);
        let ctx = SessionContext::new_with_config(config);

        let provider =
            FastqTableProvider::new(file_path.clone(), None).expect("Failed to create provider");
        ctx.register_table("fastq", Arc::new(provider))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT name FROM fastq")
            .await
            .expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2000,
            "Row count mismatch for {partitions} target partitions: got {total_rows}"
        );
    }
}

#[tokio::test]
async fn test_bgzf_parallel_data_integrity() {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider =
        FastqTableProvider::new(file_path.clone(), None).expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM fastq ORDER BY name")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2000);

    // Check no duplicates by collecting all names
    let mut names = HashSet::new();
    for batch in &batches {
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let name = name_array.value(i);
            assert!(!name.is_empty(), "Name should not be empty");
            names.insert(name.to_string());
        }
        // Verify all columns present
        assert_eq!(batch.num_columns(), 4);

        // Verify sequence and quality_scores are non-empty
        let seq_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let qual_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!seq_array.value(i).is_empty());
            assert!(!qual_array.value(i).is_empty());
        }
    }
    assert_eq!(
        names.len(),
        2000,
        "Expected 2000 unique names, got {}",
        names.len()
    );
}

#[tokio::test]
async fn test_bgzf_parallel_with_projection() {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider =
        FastqTableProvider::new(file_path.clone(), None).expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT sequence FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2000);

    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "sequence");
    }
}

#[tokio::test]
async fn test_bgzf_parallel_with_limit() {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider =
        FastqTableProvider::new(file_path.clone(), None).expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM fastq LIMIT 10")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows <= 10,
        "Expected at most 10 rows, got {total_rows}"
    );
}

#[tokio::test]
async fn test_bgzf_no_gzi_falls_back_to_sequential() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test_no_gzi.fastq.bgz");

    // Copy the .bgz file to a temp location without the .gzi
    let src = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));
    std::fs::copy(&src, &tmp_path).expect("Failed to copy test file");

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT name FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2000,
        "Sequential fallback should still read all 2000 rows"
    );
}

// ---- Uncompressed parallel read tests ----

#[tokio::test]
async fn test_uncompressed_parallel_row_count() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 500);

    for partitions in 1..=8 {
        let config = SessionConfig::new().with_target_partitions(partitions);
        let ctx = SessionContext::new_with_config(config);

        let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
            .expect("Failed to create provider");
        ctx.register_table("fastq", Arc::new(provider))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT name FROM fastq")
            .await
            .expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 500,
            "Row count mismatch for {partitions} target partitions: got {total_rows}"
        );
    }
}

#[tokio::test]
async fn test_uncompressed_parallel_data_integrity() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 500);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM fastq ORDER BY name")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 500);

    let mut names = HashSet::new();
    for batch in &batches {
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let name = name_array.value(i);
            assert!(!name.is_empty());
            names.insert(name.to_string());
        }
    }
    assert_eq!(
        names.len(),
        500,
        "Expected 500 unique names, got {}",
        names.len()
    );
}

#[tokio::test]
async fn test_uncompressed_parallel_with_projection() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 500);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT sequence FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 500);

    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "sequence");
    }
}

#[tokio::test]
async fn test_uncompressed_parallel_with_limit() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 500);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM fastq LIMIT 10")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows <= 10,
        "Expected at most 10 rows, got {total_rows}"
    );
}

#[tokio::test]
async fn test_uncompressed_parallel_small_file() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 3);

    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT name FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Expected 3 rows for small file, got {total_rows}"
    );
}

#[tokio::test]
async fn test_uncompressed_parallel_single_record() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq");
    generate_test_fastq(tmp_path.to_str().unwrap(), 1);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected exactly 1 row, got {total_rows}");
}

// ---- GZIP sequential fallback test ----

#[tokio::test]
async fn test_gzip_reads_sequentially() {
    use datafusion_bio_format_fastq::FastqLocalWriter;
    use noodles_fastq::record::Definition;

    let tmp_dir = TempDir::new().unwrap();
    let tmp_path = tmp_dir.path().join("test.fastq.gz");
    let num_records = 50;

    {
        let mut writer = FastqLocalWriter::with_compression(
            &tmp_path,
            datafusion_bio_format_fastq::FastqCompressionType::Gzip,
        )
        .expect("Failed to create writer");
        for i in 0..num_records {
            let record = noodles_fastq::Record::new(
                Definition::new(format!("read_{i}"), ""),
                b"ACGTACGT".to_vec(),
                b"IIIIIIII".to_vec(),
            );
            writer.write_record(&record).expect("Failed to write");
        }
        writer.finish().expect("Failed to finish");
    }

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    let provider = FastqTableProvider::new(tmp_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT name FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, num_records,
        "Expected {num_records} rows from gzip file, got {total_rows}"
    );
}
