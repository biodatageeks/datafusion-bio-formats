//! Integration tests for index-based predicate pushdown with CRAM data.
//!
//! Tests verify that CRAM files with CRAI indexes correctly:
//! - Partition reads by genomic region
//! - Push down chromosome and position filters via index
//! - Apply record-level filters (e.g., mapping quality)
//! - Produce correct results compared to full-scan queries
//!
//! The test CRAM file was generated with `no_ref` mode (reference-free),
//! so no external reference path is needed. The noodles fork includes a fix
//! for multi-ref slice handling in no_ref CRAMs (biodatageeks/noodles@da905ff).
//!
//! Uses multi_chrom.cram: 421 reads across chr1(160), chr2(159), chrX(102).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
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

/// Create a session context with the multi_chrom.cram test file registered.
async fn setup_cram_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,  // reference_path: None for no_ref CRAM
        None,  // object_storage_options
        true,  // zero-based coordinates
        None,  // tag_fields
        false, // binary_cigar
    )
    .await?;
    ctx.register_table("cram", Arc::new(provider))?;
    Ok(ctx)
}

/// Test: single chromosome filter via index pushdown.
#[tokio::test]
async fn test_cram_single_region_query() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM cram WHERE chrom = 'chr1'").await;

    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;
    assert_eq!(count, 160, "Expected 160 reads on chr1");

    Ok(())
}

/// Test: multi-chromosome filter via index pushdown.
#[tokio::test]
async fn test_cram_multi_chromosome_query() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM cram WHERE chrom IN ('chr1', 'chr2')",
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
#[tokio::test]
async fn test_cram_full_scan_total_count() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    assert_eq!(total, 421, "Expected 421 total reads in full scan");

    let chr1 = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;
    let chr2 = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr2'").await;
    let chrx = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chrX'").await;

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
#[tokio::test]
async fn test_cram_record_level_filter() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM cram WHERE mapping_quality >= 30").await;

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
#[tokio::test]
async fn test_cram_combined_genomic_and_record_filter() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM cram WHERE chrom = 'chr1' AND mapping_quality >= 30",
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
#[tokio::test]
async fn test_cram_region_with_start_end() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM cram WHERE chrom = 'chr1' AND start >= 55004999 AND \"end\" <= 55015000",
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
#[tokio::test]
async fn test_cram_indexed_vs_full_scan_correctness() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let indexed_count = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;

    let df = ctx.sql("SELECT chrom FROM cram").await?;
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
