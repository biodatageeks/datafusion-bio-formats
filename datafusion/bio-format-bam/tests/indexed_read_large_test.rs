//! Large integration tests for index-based predicate pushdown with BAM data.
//!
//! Uses multi_chrom_large.bam: 4277 reads across chr1(1662), chr2(1694), chrX(921).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::collections::HashSet;
use std::sync::Arc;

async fn count_rows(ctx: &SessionContext, sql: &str) -> u64 {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

async fn collect_distinct_chroms(ctx: &SessionContext, sql: &str) -> HashSet<String> {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    let mut chroms = HashSet::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("expected StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) {
                chroms.insert(col.value(i).to_string());
            }
        }
    }
    chroms
}

async fn setup_bam_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = BamTableProvider::new(
        "tests/multi_chrom_large.bam".to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    ctx.register_table("bam", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::test]
async fn test_bam_single_region_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM bam WHERE chrom = 'chr1'").await;
    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    assert_eq!(count, 1662, "Expected 1662 reads on chr1");

    Ok(())
}

#[tokio::test]
async fn test_bam_multi_chromosome_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM bam WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert!(chroms.contains("chr1"));
    assert!(chroms.contains("chr2"));

    let count = count_rows(
        &ctx,
        "SELECT chrom FROM bam WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert_eq!(count, 1662 + 1694, "Expected 3356 reads on chr1 + chr2");

    Ok(())
}

#[tokio::test]
async fn test_bam_full_scan_total_count_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM bam").await;
    assert_eq!(total, 4277, "Expected 4277 total reads");

    let chr1 = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    let chr2 = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr2'").await;
    let chrx = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chrX'").await;

    assert_eq!(total, chr1 + chr2 + chrx);

    Ok(())
}

#[tokio::test]
async fn test_bam_record_level_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM bam").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM bam WHERE mapping_quality >= 30").await;

    assert!(filtered > 0);
    assert!(filtered <= total);

    Ok(())
}

#[tokio::test]
async fn test_bam_combined_genomic_and_record_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM bam WHERE chrom = 'chr1' AND mapping_quality >= 30",
    )
    .await;

    assert!(combined > 0);
    assert!(combined <= chr1_total);

    Ok(())
}

#[tokio::test]
async fn test_bam_region_with_start_end_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM bam WHERE chrom = 'chr1' AND start >= 55004999 AND \"end\" <= 55015000",
    )
    .await;

    assert!(region_count > 0);
    assert!(region_count < chr1_total);

    Ok(())
}

#[tokio::test]
async fn test_bam_indexed_vs_full_scan_correctness_large() -> datafusion::error::Result<()> {
    let ctx = setup_bam_ctx().await?;

    let indexed_count = count_rows(&ctx, "SELECT chrom FROM bam WHERE chrom = 'chr1'").await;

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

    assert_eq!(indexed_count, manual_count);

    Ok(())
}
