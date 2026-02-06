//! Integration tests for index-based predicate pushdown and parallel reading.
//!
//! Tests verify that BAM files with BAI indexes correctly:
//! - Partition reads by genomic region
//! - Push down chromosome and position filters via index
//! - Apply record-level filters (e.g., mapping quality)
//! - Produce correct results compared to full-scan queries

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::collections::HashSet;
use std::sync::Arc;

/// Helper: execute a SQL query and return total row count across all batches.
async fn count_rows(ctx: &SessionContext, sql: &str) -> u64 {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

/// Helper: collect distinct chrom values from a query result.
async fn collect_distinct_chroms(ctx: &SessionContext, sql: &str) -> HashSet<String> {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    let mut chroms = HashSet::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray for chrom column");
        for i in 0..col.len() {
            if !col.is_null(i) {
                chroms.insert(col.value(i).to_string());
            }
        }
    }
    chroms
}

/// Create a session context with the multi_chrom.bam test file registered.
async fn setup_bam_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = BamTableProvider::new(
        "tests/multi_chrom.bam".to_string(),
        None, // thread_num
        None, // object_storage_options
        true, // zero-based coordinates
        None, // tag_fields
    )
    .await?;
    ctx.register_table("bam", Arc::new(provider))?;
    Ok(ctx)
}

/// Test: single chromosome filter via index pushdown.
/// WHERE chrom = 'chr1' should use the BAI index to read only chr1 reads.
#[tokio::test]
async fn test_bam_single_region_query() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM bam WHERE chrom = 'chr1'").await;

    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    assert!(count > 0, "Expected reads on chr1");

    Ok(())
}

/// Test: multi-chromosome filter via index pushdown.
/// WHERE chrom IN ('chr1', 'chr2') should create two partitions.
#[tokio::test]
async fn test_bam_multi_chromosome_query() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM bam WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;

    assert!(
        chroms.contains("chr1"),
        "Expected chr1 in results: {:?}",
        chroms
    );
    assert!(
        chroms.contains("chr2"),
        "Expected chr2 in results: {:?}",
        chroms
    );

    Ok(())
}

/// Test: full scan without chromosome filter.
/// When no chrom filter is given, all chromosomes should be read.
/// Total count should equal sum of per-chromosome counts.
#[tokio::test]
async fn test_bam_full_scan_total_count() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM bam").await;
    assert!(total > 0, "Expected some reads in full scan");

    let chr1 = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    let chr2 = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr2'").await;
    let chrx = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chrX'").await;

    assert_eq!(
        total,
        chr1 + chr2 + chrx,
        "Full scan total ({}) should equal sum of per-chrom counts (chr1={}, chr2={}, chrX={})",
        total,
        chr1,
        chr2,
        chrx
    );

    Ok(())
}

/// Test: record-level filter (mapping quality).
/// mapping_quality >= 30 is a non-genomic filter applied after index lookup.
#[tokio::test]
async fn test_bam_record_level_filter() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM bam").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM bam WHERE mapping_quality >= 30").await;

    assert!(
        filtered > 0,
        "Expected some reads with mapping_quality >= 30"
    );
    assert!(
        filtered <= total,
        "Filtered count ({}) should be <= total ({})",
        filtered,
        total
    );

    Ok(())
}

/// Test: combined genomic region + record-level filter.
/// WHERE chrom = 'chr1' AND mapping_quality >= 30 should use index for chr1
/// and then apply record-level filter.
#[tokio::test]
async fn test_bam_combined_genomic_and_record_filter() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM bam WHERE chrom = 'chr1' AND mapping_quality >= 30",
    )
    .await;

    assert!(combined > 0, "Expected some reads matching combined filter");
    assert!(
        combined <= chr1_total,
        "Combined filter count ({}) should be <= chr1 total ({})",
        combined,
        chr1_total
    );

    Ok(())
}

/// Test: genomic region with start/end position bounds.
/// Tests that position-based filters work with the index.
#[tokio::test]
async fn test_bam_region_with_start_end() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;

    // Query a subregion of chr1 (positions are 0-based)
    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM bam WHERE chrom = 'chr1' AND start >= 100002700 AND \"end\" <= 100003500",
    )
    .await;

    assert!(region_count > 0, "Expected reads in the specified region");
    assert!(
        region_count <= chr1_total,
        "Region count ({}) should be <= chr1 total ({})",
        region_count,
        chr1_total
    );

    Ok(())
}

/// Test: correctness of indexed vs full scan results.
/// The set of reads returned by an indexed chr1 query should match
/// what you get from a full scan filtered to chr1.
#[tokio::test]
async fn test_bam_indexed_vs_full_scan_correctness() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    // Indexed path: chrom filter pushes down to BAI index
    let indexed_count = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;

    // Full scan: read all and count chr1 manually
    let df = ctx.sql("SELECT chrom FROM bam").await?;
    let batches = df.collect().await?;
    let mut manual_count: u64 = 0;
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) && col.value(i) == "chr1" {
                manual_count += 1;
            }
        }
    }

    assert_eq!(
        indexed_count, manual_count,
        "Indexed chr1 count ({}) should equal manual count from full scan ({})",
        indexed_count, manual_count
    );

    Ok(())
}
