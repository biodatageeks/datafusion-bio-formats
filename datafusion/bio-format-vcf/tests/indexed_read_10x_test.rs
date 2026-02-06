//! Integration tests for index-based predicate pushdown with 10x larger VCF data.
//!
//! Tests verify that VCF files with TBI indexes correctly:
//! - Partition variants by genomic region (contig)
//! - Push down chromosome and position filters via TBI index
//! - Apply post-filters (e.g., quality score)
//! - Produce correct results compared to full-scan queries
//!
//! The test VCF uses bare chromosome names (21, 22) — no "chr" prefix.
//! Uses multi_chrom_10x.vcf.gz: 1000 variants across chr 21(500), chr 22(500).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
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

/// Create a session context with the multi_chrom_10x.vcf.gz test file registered.
async fn setup_vcf_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = VcfTableProvider::new(
        "tests/multi_chrom_10x.vcf.gz".to_string(),
        None, // info_fields
        None, // format_fields
        None, // thread_num
        None, // object_storage_options
        true, // zero-based coordinates
    )?;
    ctx.register_table("vcf", Arc::new(provider))?;
    Ok(ctx)
}

/// Test: single chromosome filter via TBI index pushdown.
/// WHERE chrom = '21' should use the TBI index.
#[tokio::test]
async fn test_vcf_single_region_query_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM vcf WHERE chrom = '21'").await;

    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("21"));

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    assert_eq!(count, 500, "Expected 500 variants on chr 21");

    Ok(())
}

/// Test: multi-chromosome filter via index pushdown.
#[tokio::test]
async fn test_vcf_multi_chromosome_query_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM vcf WHERE chrom IN ('21', '22')",
    )
    .await;

    assert!(
        chroms.contains("21"),
        "Expected chr 21 in results: {:?}",
        chroms
    );
    assert!(
        chroms.contains("22"),
        "Expected chr 22 in results: {:?}",
        chroms
    );

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom IN ('21', '22')").await;
    assert_eq!(
        count,
        500 + 500,
        "Expected 1000 variants on chr 21 + chr 22"
    );

    Ok(())
}

/// Test: full scan without chromosome filter.
#[tokio::test]
async fn test_vcf_full_scan_total_count_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    assert_eq!(total, 1000, "Expected 1000 total variants in full scan");

    let chr21 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let chr22 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '22'").await;

    assert_eq!(
        total,
        chr21 + chr22,
        "Full scan total ({}) should equal sum of per-chrom counts (21={}, 22={})",
        total,
        chr21,
        chr22
    );

    Ok(())
}

/// Test: post-filter on quality score.
/// qual is Float64; this filter is applied by DataFusion after index lookup.
#[tokio::test]
async fn test_vcf_record_level_filter_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM vcf WHERE qual >= 30.0").await;

    assert!(filtered > 0, "Expected some variants with qual >= 30");
    assert!(
        filtered <= total,
        "Filtered count ({}) should be <= total ({})",
        filtered,
        total
    );

    Ok(())
}

/// Test: combined genomic region + quality filter.
#[tokio::test]
async fn test_vcf_combined_filters_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND qual >= 30.0",
    )
    .await;

    assert!(
        combined > 0,
        "Expected some variants matching combined filter"
    );
    assert!(
        combined <= chr21_total,
        "Combined filter count ({}) should be <= chr21 total ({})",
        combined,
        chr21_total
    );

    Ok(())
}

/// Test: genomic region with position bounds.
#[tokio::test]
async fn test_vcf_region_with_positions_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;

    // Query a subregion of chr21 (0-based positions)
    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND start >= 5000999 AND \"end\" <= 5025999",
    )
    .await;

    assert!(
        region_count > 0,
        "Expected variants in the specified region"
    );
    assert!(
        region_count <= chr21_total,
        "Region count ({}) should be <= chr21 total ({})",
        region_count,
        chr21_total
    );

    Ok(())
}

/// Test: correctness of indexed vs full scan results.
#[tokio::test]
async fn test_vcf_indexed_correctness_10x() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let indexed_count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;

    let df = ctx.sql("SELECT chrom FROM vcf").await?;
    let batches = df.collect().await?;
    let mut manual_count: u64 = 0;
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) && col.value(i) == "21" {
                manual_count += 1;
            }
        }
    }

    assert_eq!(
        indexed_count, manual_count,
        "Indexed chr21 count ({}) should equal manual count from full scan ({})",
        indexed_count, manual_count
    );

    Ok(())
}
