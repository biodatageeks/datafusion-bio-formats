use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

/// Helper: execute a SQL query and return total row count across all batches.
async fn count_rows(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Test reading CRAM with tags — verifies schema has extra tag columns and values are populated.
///
/// Uses multi_chrom.cram (421 reads, 9 tags per record: MQ, PG, UQ, NM, OQ, E2, ZQ, MD, RG).
#[tokio::test]
async fn test_cram_read_with_tags() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None, // no_ref mode
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "MQ".to_string(),
            "RG".to_string(),
        ]),
        false,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    // Verify schema: 12 core + 4 tag fields = 16
    let df = ctx.table("cram").await.unwrap();
    let schema = df.schema();
    assert_eq!(schema.fields().len(), 16);

    // Query all records with tags
    let total = count_rows(
        &ctx,
        "SELECT chrom, start, \"NM\", \"MD\", \"MQ\", \"RG\" FROM cram",
    )
    .await;
    assert_eq!(total, 421, "Should have 421 reads");

    // Verify NM values are small non-negative integers
    use datafusion::arrow::array::Int32Array;
    let df = ctx
        .sql("SELECT \"NM\" FROM cram WHERE \"NM\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let nm_count: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(
        nm_count > 400,
        "NM should be present in most reads, got {}",
        nm_count
    );

    for batch in &results {
        let nm_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..nm_col.len() {
            if !nm_col.is_null(i) {
                let nm = nm_col.value(i);
                assert!(
                    (0..100).contains(&nm),
                    "NM={} should be a small non-negative integer",
                    nm
                );
            }
        }
    }

    // Verify MD values are non-empty strings
    use datafusion::arrow::array::StringArray;
    let df = ctx
        .sql("SELECT \"MD\" FROM cram WHERE \"MD\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let md_count: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(
        md_count > 400,
        "MD should be present in most reads, got {}",
        md_count
    );

    for batch in &results {
        let md_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..md_col.len() {
            if !md_col.is_null(i) {
                let md = md_col.value(i);
                assert!(!md.is_empty(), "MD value should not be empty");
            }
        }
    }

    // Verify RG is present in all reads
    let rg_count = count_rows(&ctx, "SELECT \"RG\" FROM cram WHERE \"RG\" IS NOT NULL").await;
    assert_eq!(rg_count, 421, "RG should be present in all reads");

    // Verify MQ is present in most reads
    let mq_count = count_rows(&ctx, "SELECT \"MQ\" FROM cram WHERE \"MQ\" IS NOT NULL").await;
    assert!(
        mq_count > 400,
        "MQ should be present in most reads, got {}",
        mq_count
    );
}

/// Test nullable tags with mixed presence across records.
#[tokio::test]
async fn test_cram_nullable_tags_with_mixed_presence() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MQ".to_string(), "E2".to_string()]),
        false,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    assert_eq!(total, 421);

    // Count non-null values for each tag
    let nm_count = count_rows(&ctx, "SELECT \"NM\" FROM cram WHERE \"NM\" IS NOT NULL").await;
    let mq_count = count_rows(&ctx, "SELECT \"MQ\" FROM cram WHERE \"MQ\" IS NOT NULL").await;
    let e2_count = count_rows(&ctx, "SELECT \"E2\" FROM cram WHERE \"E2\" IS NOT NULL").await;

    // NM and MQ should be present in most reads
    assert!(
        nm_count > 400,
        "NM should be present in most reads, got {}",
        nm_count
    );
    assert!(
        mq_count > 400,
        "MQ should be present in most reads, got {}",
        mq_count
    );

    // E2 may be present in some or all — just verify the query runs and count is reasonable
    assert!(e2_count <= 421, "E2 count should not exceed total reads");
}

/// Test that projection pushdown excludes tags when only core fields are selected.
#[tokio::test]
async fn test_cram_tag_projection_pushdown() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "MQ".to_string(),
            "RG".to_string(),
        ]),
        false,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    // Query only core fields — tags should not be in output
    let df = ctx.sql("SELECT chrom, start FROM cram").await.unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);

    // Should have 2 columns only (tags not projected)
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 2);
    }
}

/// Test tag reading with a chromosome filter.
#[tokio::test]
async fn test_cram_tag_with_filter() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string()]),
        false,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT chrom, \"NM\" FROM cram WHERE chrom = 'chr1'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 160, "chr1 should have 160 reads");

    // Verify result has 2 columns
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 2);
    }
}
