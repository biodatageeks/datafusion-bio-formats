//! Integration tests for index-based predicate pushdown and parallel reading for GFF.
//!
//! Tests verify that BGZF-compressed GFF files with TBI indexes correctly:
//! - Partition features by genomic region (contig)
//! - Push down chromosome and position filters via TBI index
//! - Apply post-filters (e.g., feature type)
//! - Produce correct results compared to full-scan queries
//!
//! Uses multi_chrom.gff3.gz: 448 features across chr1(150), chr2(208), chrX(90).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
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

/// Create a session context with the multi_chrom.gff3.gz test file registered.
async fn setup_gff_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = GffTableProvider::new(
        "tests/multi_chrom.gff3.gz".to_string(),
        None, // attr_fields
        None, // object_storage_options
        true, // zero-based coordinates
    )?;
    ctx.register_table("gff", Arc::new(provider))?;
    Ok(ctx)
}

/// Test: single chromosome filter via TBI index pushdown.
/// WHERE chrom = 'chr1' should use the TBI index.
#[tokio::test]
async fn test_gff_single_region_query() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM gff WHERE chrom = 'chr1'").await;

    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    assert_eq!(count, 150, "Expected 150 features on chr1");

    Ok(())
}

/// Test: multi-chromosome filter via index pushdown.
#[tokio::test]
async fn test_gff_multi_chromosome_query() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM gff WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;

    assert!(
        chroms.contains("chr1"),
        "Expected chr1 in results: {chroms:?}"
    );
    assert!(
        chroms.contains("chr2"),
        "Expected chr2 in results: {chroms:?}"
    );

    let count = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert_eq!(count, 150 + 208, "Expected 358 features on chr1 + chr2");

    Ok(())
}

/// Test: full scan without chromosome filter.
#[tokio::test]
async fn test_gff_full_scan_total_count() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM gff").await;
    assert_eq!(total, 448, "Expected 448 total features in full scan");

    let chr1 = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    let chr2 = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr2'").await;
    let chrx = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chrX'").await;

    assert_eq!(
        total,
        chr1 + chr2 + chrx,
        "Full scan total ({total}) should equal sum of per-chrom counts (chr1={chr1}, chr2={chr2}, chrX={chrx})"
    );

    Ok(())
}

/// Test: record-level filter on feature type.
/// type is a string field; this filter is applied post-index.
#[tokio::test]
async fn test_gff_record_level_filter() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM gff").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM gff WHERE \"type\" = 'gene'").await;

    assert!(filtered > 0, "Expected some gene features");
    assert!(
        filtered < total,
        "Filtered count ({filtered}) should be < total ({total}) since not all features are genes"
    );

    Ok(())
}

/// Test: combined genomic region + record-level filter.
#[tokio::test]
async fn test_gff_combined_filters() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom = 'chr1' AND \"type\" = 'gene'",
    )
    .await;

    assert!(combined > 0, "Expected some gene features on chr1");
    assert!(
        combined <= chr1_total,
        "Combined filter count ({combined}) should be <= chr1 total ({chr1_total})"
    );

    Ok(())
}

/// Test: genomic region with position bounds.
#[tokio::test]
async fn test_gff_region_with_positions() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;

    // Query a subregion of chr1 (0-based coordinates since we use zero_based=true)
    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom = 'chr1' AND start >= 999 AND \"end\" <= 50000",
    )
    .await;

    assert!(
        region_count > 0,
        "Expected features in the specified region"
    );
    assert!(
        region_count <= chr1_total,
        "Region count ({region_count}) should be <= chr1 total ({chr1_total})"
    );

    Ok(())
}

/// Test: correctness of indexed vs full scan results.
#[tokio::test]
async fn test_gff_indexed_correctness() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let indexed_count = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;

    let df = ctx.sql("SELECT chrom FROM gff").await?;
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
        "Indexed chr1 count ({indexed_count}) should equal manual count from full scan ({manual_count})"
    );

    Ok(())
}
