//! Large integration tests for index-based predicate pushdown with CRAM data.
//!
//! Uses multi_chrom_large.cram: 4277 reads across chr1(1662), chr2(1694), chrX(921).
//! All tests are ignored because noodles' indexed CRAM reader panics on no_ref CRAMs.

use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
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

async fn setup_cram_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = CramTableProvider::new(
        "tests/multi_chrom_large.cram".to_string(),
        None,
        None,
        true,
        None,
    )
    .await?;
    ctx.register_table("cram", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with region queries"]
async fn test_cram_single_region_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chroms =
        collect_distinct_chroms(&ctx, "SELECT DISTINCT chrom FROM cram WHERE chrom = 'chr1'").await;
    assert_eq!(chroms.len(), 1);
    assert!(chroms.contains("chr1"));

    let count = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;
    assert_eq!(count, 1662, "Expected 1662 reads on chr1");

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with multiple partitions"]
async fn test_cram_multi_chromosome_query_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM cram WHERE chrom IN ('chr1', 'chr2')",
    )
    .await;
    assert!(chroms.contains("chr1"));
    assert!(chroms.contains("chr2"));

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with multiple partitions"]
async fn test_cram_full_scan_total_count_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    assert_eq!(total, 4277, "Expected 4277 total reads");

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with multiple partitions"]
async fn test_cram_record_level_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM cram WHERE mapping_quality >= 30").await;

    assert!(filtered > 0);
    assert!(filtered <= total);

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with multiple partitions"]
async fn test_cram_combined_genomic_and_record_filter_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM cram WHERE chrom = 'chr1' AND mapping_quality >= 30",
    )
    .await;

    assert!(combined > 0);
    assert!(combined <= chr1_total);

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with region queries"]
async fn test_cram_region_with_start_end_large() -> datafusion::error::Result<()> {
    let ctx = setup_cram_ctx().await?;

    let chr1_total = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom = 'chr1'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM cram WHERE chrom = 'chr1' AND start >= 55004999 AND \"end\" <= 55015000",
    )
    .await;

    assert!(region_count > 0);
    assert!(region_count <= chr1_total);

    Ok(())
}

#[tokio::test]
#[ignore = "noodles indexed CRAM reader panics on no_ref CRAMs with region queries"]
async fn test_cram_indexed_vs_full_scan_correctness_large() -> datafusion::error::Result<()> {
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

    assert_eq!(indexed_count, manual_count);

    Ok(())
}
