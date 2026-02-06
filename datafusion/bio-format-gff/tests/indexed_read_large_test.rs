//! Large integration tests for index-based predicate pushdown with GFF data.
//!
//! Uses multi_chrom_large.gff3.gz: 4426 features across chr1(1470), chr2(2056), chrX(900).

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
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

async fn setup_gff_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = GffTableProvider::new(
        "tests/multi_chrom_large.gff3.gz".to_string(),
        None,
        None,
        None,
        true,
    )?;
    ctx.register_table("gff", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::test]
async fn test_gff_single_region_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM gff WHERE chrom = 'chr1'").await;
    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    assert_eq!(count, 1470, "Expected 1470 features on chr1");

    Ok(())
}

#[tokio::test]
async fn test_gff_multi_chromosome_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM gff WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert!(chroms.contains("chr1"));
    assert!(chroms.contains("chr2"));

    let count = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert_eq!(count, 1470 + 2056, "Expected 3526 features on chr1 + chr2");

    Ok(())
}

#[tokio::test]
async fn test_gff_full_scan_total_count_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM gff").await;
    assert_eq!(total, 4426, "Expected 4426 total features");

    let chr1 = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    let chr2 = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr2'").await;
    let chrx = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chrX'").await;

    assert_eq!(total, chr1 + chr2 + chrx);

    Ok(())
}

#[tokio::test]
async fn test_gff_record_level_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM gff").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM gff WHERE \"type\" = 'gene'").await;

    assert!(filtered > 0);
    assert!(filtered < total);

    Ok(())
}

#[tokio::test]
async fn test_gff_combined_filters_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom = 'chr1' AND \"type\" = 'gene'",
    )
    .await;

    assert!(combined > 0);
    assert!(combined <= chr1_total);

    Ok(())
}

#[tokio::test]
async fn test_gff_region_with_positions_large() -> datafusion::error::Result<()> {
    let ctx = setup_gff_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM gff WHERE chrom = 'chr1'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM gff WHERE chrom = 'chr1' AND start >= 999 AND \"end\" <= 50000",
    )
    .await;

    assert!(region_count > 0);
    assert!(region_count <= chr1_total);

    Ok(())
}

#[tokio::test]
async fn test_gff_indexed_correctness_large() -> datafusion::error::Result<()> {
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

    assert_eq!(indexed_count, manual_count);

    Ok(())
}
