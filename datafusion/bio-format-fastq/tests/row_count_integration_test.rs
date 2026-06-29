use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;
use std::sync::Arc;

#[tokio::test]
async fn test_bgzf_fastq_table_provider_row_count() {
    for i in 1..=4 {
        let config = SessionConfig::new().with_target_partitions(i);
        let ctx = SessionContext::new_with_config(config);

        // Construct the path to the data file relative to the crate's manifest directory.
        let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));

        let provider = FastqTableProvider::new(file_path, None).expect("Failed to create provider");
        ctx.register_table("fastq", Arc::new(provider))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT count(*) FROM fastq")
            .await
            .expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        assert_eq!(batches.len(), 1);
        let batch = batches.first().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(count, 2000, "Row count mismatch for {i} target partitions");
    }
}

/// Regression test for the COUNT(*) failure on plain-GZIP FASTQ files.
///
/// Plain (non-BGZF) gzip files take the `Sequential` strategy, whose stream-based
/// scan path produced a zero-column batch while `scan()` advertised a one-field
/// dummy schema, triggering the Arrow error:
/// "number of columns(0) must match number of fields(1) in schema".
#[tokio::test]
async fn test_gzip_fastq_count_star() {
    for i in 1..=4 {
        let config = SessionConfig::new().with_target_partitions(i);
        let ctx = SessionContext::new_with_config(config);

        // Plain gzip (single member) -> Sequential strategy.
        let file_path = format!("{}/data/count_star.fastq.gz", env!("CARGO_MANIFEST_DIR"));

        let provider = FastqTableProvider::new(file_path, None).expect("Failed to create provider");
        ctx.register_table("fastq", Arc::new(provider))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT count(*) FROM fastq")
            .await
            .expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        assert_eq!(batches.len(), 1);
        let batch = batches.first().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(count, 5, "Row count mismatch for {i} target partitions");
    }
}
