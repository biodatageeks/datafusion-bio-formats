use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::{FastqParser, table_provider::FastqTableProvider};
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

// Sample FASTQ data for testing
const SAMPLE_FASTQ: &str = r#"@SRR123456.1 Illumina sequencing read 1
ACGTACGTACGTACGT
+
IIIIIIIIIIIIIIII
@SRR123456.2 Illumina sequencing read 2
TGCATGCATGCATGCA
+
JJJJJJJJJJJJJJJJ
@SRR123456.3
AAATTTCCCGGGATCG
+
KKKKKKKKKKKKKKKK
"#;

async fn create_test_fastq_file() -> tempfile::NamedTempFile {
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    temp_file
        .write_all(SAMPLE_FASTQ.as_bytes())
        .expect("Failed to write to temp file");
    temp_file
}

#[tokio::test]
async fn test_noodles_vs_needletail_same_results() {
    let temp_file = create_test_fastq_file().await;
    let file_path = temp_file.path().to_str().unwrap().to_string();

    let storage_options = ObjectStorageOptions::default();

    // Create table providers with different parsers
    let noodles_provider = FastqTableProvider::new_with_parser(
        file_path.clone(),
        None,
        Some(storage_options.clone()),
        FastqParser::Noodles,
    )
    .expect("Failed to create noodles provider");

    let needletail_provider = FastqTableProvider::new_with_parser(
        file_path,
        None,
        Some(storage_options),
        FastqParser::Needletail,
    )
    .expect("Failed to create needletail provider");

    // Create DataFusion context
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await
        .expect("Failed to set DataFusion configuration");

    // Register both table providers
    ctx.register_table("fastq_noodles", Arc::new(noodles_provider))
        .expect("Failed to register noodles table");
    ctx.register_table("fastq_needletail", Arc::new(needletail_provider))
        .expect("Failed to register needletail table");

    // Query both tables
    let noodles_df = ctx
        .sql("SELECT * FROM fastq_noodles ORDER BY name")
        .await
        .expect("Failed to query noodles table");
    let needletail_df = ctx
        .sql("SELECT * FROM fastq_needletail ORDER BY name")
        .await
        .expect("Failed to query needletail table");

    // Collect results
    let noodles_batches = noodles_df
        .collect()
        .await
        .expect("Failed to collect noodles results");
    let needletail_batches = needletail_df
        .collect()
        .await
        .expect("Failed to collect needletail results");

    // Compare results
    assert_eq!(
        noodles_batches.len(),
        needletail_batches.len(),
        "Different number of batches"
    );

    for (noodles_batch, needletail_batch) in noodles_batches.iter().zip(needletail_batches.iter()) {
        assert_eq!(
            noodles_batch.num_rows(),
            needletail_batch.num_rows(),
            "Different number of rows"
        );
        assert_eq!(
            noodles_batch.num_columns(),
            needletail_batch.num_columns(),
            "Different number of columns"
        );

        // Compare each column
        for col_idx in 0..noodles_batch.num_columns() {
            let noodles_col = noodles_batch.column(col_idx);
            let needletail_col = needletail_batch.column(col_idx);

            assert_eq!(
                noodles_col.data_type(),
                needletail_col.data_type(),
                "Different data types for column {}",
                col_idx
            );
            assert_eq!(
                noodles_col.len(),
                needletail_col.len(),
                "Different lengths for column {}",
                col_idx
            );
        }
    }
}

#[tokio::test]
async fn test_noodles_vs_needletail_record_count() {
    let temp_file = create_test_fastq_file().await;
    let file_path = temp_file.path().to_str().unwrap().to_string();

    let storage_options = ObjectStorageOptions::default();

    // Create table providers with different parsers
    let noodles_provider = FastqTableProvider::new_with_parser(
        file_path.clone(),
        None,
        Some(storage_options.clone()),
        FastqParser::Noodles,
    )
    .expect("Failed to create noodles provider");

    let needletail_provider = FastqTableProvider::new_with_parser(
        file_path,
        None,
        Some(storage_options),
        FastqParser::Needletail,
    )
    .expect("Failed to create needletail provider");

    // Create DataFusion context
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await
        .expect("Failed to set DataFusion configuration");

    // Register both table providers
    ctx.register_table("fastq_noodles", Arc::new(noodles_provider))
        .expect("Failed to register noodles table");
    ctx.register_table("fastq_needletail", Arc::new(needletail_provider))
        .expect("Failed to register needletail table");

    // Count records in both tables
    let noodles_count = ctx
        .sql("SELECT COUNT(*) as count FROM fastq_noodles")
        .await
        .expect("Failed to query noodles count")
        .collect()
        .await
        .expect("Failed to collect noodles count");

    let needletail_count = ctx
        .sql("SELECT COUNT(*) as count FROM fastq_needletail")
        .await
        .expect("Failed to query needletail count")
        .collect()
        .await
        .expect("Failed to collect needletail count");

    // Both should have 3 records
    assert_eq!(noodles_count.len(), 1);
    assert_eq!(needletail_count.len(), 1);

    // Extract counts and compare
    let noodles_count_value = noodles_count[0].column(0);
    let needletail_count_value = needletail_count[0].column(0);

    assert_eq!(
        noodles_count_value, needletail_count_value,
        "Different record counts between parsers"
    );
}

#[tokio::test]
async fn test_noodles_vs_needletail_projection() {
    let temp_file = create_test_fastq_file().await;
    let file_path = temp_file.path().to_str().unwrap().to_string();

    let storage_options = ObjectStorageOptions::default();

    // Create table providers with different parsers
    let noodles_provider = FastqTableProvider::new_with_parser(
        file_path.clone(),
        None,
        Some(storage_options.clone()),
        FastqParser::Noodles,
    )
    .expect("Failed to create noodles provider");

    let needletail_provider = FastqTableProvider::new_with_parser(
        file_path,
        None,
        Some(storage_options),
        FastqParser::Needletail,
    )
    .expect("Failed to create needletail provider");

    // Create DataFusion context
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await
        .expect("Failed to set DataFusion configuration");

    // Register both table providers
    ctx.register_table("fastq_noodles", Arc::new(noodles_provider))
        .expect("Failed to register noodles table");
    ctx.register_table("fastq_needletail", Arc::new(needletail_provider))
        .expect("Failed to register needletail table");

    // Test projection - select only name and sequence
    let noodles_df = ctx
        .sql("SELECT name, sequence FROM fastq_noodles ORDER BY name")
        .await
        .expect("Failed to query noodles table");
    let needletail_df = ctx
        .sql("SELECT name, sequence FROM fastq_needletail ORDER BY name")
        .await
        .expect("Failed to query needletail table");

    // Collect results
    let noodles_batches = noodles_df
        .collect()
        .await
        .expect("Failed to collect noodles results");
    let needletail_batches = needletail_df
        .collect()
        .await
        .expect("Failed to collect needletail results");

    // Compare results
    assert_eq!(
        noodles_batches.len(),
        needletail_batches.len(),
        "Different number of batches"
    );

    for (noodles_batch, needletail_batch) in noodles_batches.iter().zip(needletail_batches.iter()) {
        assert_eq!(
            noodles_batch.num_rows(),
            needletail_batch.num_rows(),
            "Different number of rows"
        );
        assert_eq!(
            noodles_batch.num_columns(),
            2,
            "Expected 2 columns for noodles"
        );
        assert_eq!(
            needletail_batch.num_columns(),
            2,
            "Expected 2 columns for needletail"
        );
    }
}

#[tokio::test]
async fn test_noodles_vs_needletail_description_parsing() {
    let temp_file = create_test_fastq_file().await;
    let file_path = temp_file.path().to_str().unwrap().to_string();

    let storage_options = ObjectStorageOptions::default();

    // Create table providers with different parsers
    let noodles_provider = FastqTableProvider::new_with_parser(
        file_path.clone(),
        None,
        Some(storage_options.clone()),
        FastqParser::Noodles,
    )
    .expect("Failed to create noodles provider");

    let needletail_provider = FastqTableProvider::new_with_parser(
        file_path,
        None,
        Some(storage_options),
        FastqParser::Needletail,
    )
    .expect("Failed to create needletail provider");

    // Create DataFusion context
    let ctx = SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await
        .expect("Failed to set DataFusion configuration");

    // Register both table providers
    ctx.register_table("fastq_noodles", Arc::new(noodles_provider))
        .expect("Failed to register noodles table");
    ctx.register_table("fastq_needletail", Arc::new(needletail_provider))
        .expect("Failed to register needletail table");

    // Test name and description parsing
    let noodles_df = ctx
        .sql("SELECT name, description FROM fastq_noodles ORDER BY name")
        .await
        .expect("Failed to query noodles table");
    let needletail_df = ctx
        .sql("SELECT name, description FROM fastq_needletail ORDER BY name")
        .await
        .expect("Failed to query needletail table");

    // Collect results
    let noodles_batches = noodles_df
        .collect()
        .await
        .expect("Failed to collect noodles results");
    let needletail_batches = needletail_df
        .collect()
        .await
        .expect("Failed to collect needletail results");

    // Compare results - both should parse names and descriptions identically
    assert_eq!(noodles_batches.len(), needletail_batches.len());

    for (noodles_batch, needletail_batch) in noodles_batches.iter().zip(needletail_batches.iter()) {
        assert_eq!(noodles_batch.num_rows(), needletail_batch.num_rows());
        assert_eq!(noodles_batch.num_columns(), 2); // name and description
        assert_eq!(needletail_batch.num_columns(), 2);
    }

    println!("Description parsing test completed successfully!");
}
