//! Integration tests for FASTA read functionality
//!
//! Tests verify that FASTA files can be read correctly even for large files

use datafusion::prelude::*;
use datafusion_bio_format_fasta::FastaTableProvider;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: generate an uncompressed FASTA file with `num_records` records.
fn generate_test_fasta(path: &str, num_records: usize) {
    let mut file =
        std::io::BufWriter::new(std::fs::File::create(path).expect("Failed to create test file"));
    // Each line has 24 bytes, 12_500_000 lines per record means
    // ten records will give ~ 3 GB of data
    let num_lines = 12_500_000;
    //let body = b"ACGTACGTACGTACGTACGT".repeat(num_lines);
    for i in 0..num_records {
        writeln!(file, ">seq_{i} sample description {i}").unwrap();
        for _j in 0..num_lines {
            writeln!(file, "ACGTACGTACGTACGTACGT").unwrap();
        }
    }
    file.flush().unwrap();
}

#[tokio::test]
async fn test_read_3gb_file() {
    let tmp_dir = TempDir::new().unwrap();
    let input_path = tmp_dir.path().join("input.fasta");

    let num_records = 10;
    generate_test_fasta(input_path.to_str().unwrap(), num_records);

    let ctx = SessionContext::new();

    let input_provider = FastaTableProvider::new(input_path.to_str().unwrap().to_string(), None)
        .expect("Failed to create input provider");
    ctx.register_table("input_fasta", Arc::new(input_provider))
        .unwrap();

    let result = ctx
        .sql("SELECT * FROM input_fasta")
        .await
        .expect("Failed to execute SELECT")
        .collect()
        .await
        .expect("Failed to collect results");

    assert_eq!(result.len(), 10);
}
