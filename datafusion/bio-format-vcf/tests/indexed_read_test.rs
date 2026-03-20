//! Integration tests for index-based predicate pushdown with VCF data.
//!
//! Tests verify that VCF files with TBI indexes correctly:
//! - Partition variants by genomic region (contig)
//! - Push down chromosome and position filters via TBI index
//! - Apply record-level filters (e.g., quality score)
//! - Produce correct results compared to full-scan queries
//!
//! Uses multi_chrom.vcf.gz: 1000 variants across 21(500), 22(500).

use datafusion::arrow::array::Array;
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::VCF_CONTIGS_INDEXED_KEY;
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

/// Create a session context with the multi_chrom.vcf.gz test file registered.
async fn setup_vcf_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = VcfTableProvider::new(
        "tests/multi_chrom.vcf.gz".to_string(),
        None, // info_fields
        None, // format_fields
        None, // object_storage_options
        true, // zero-based coordinates
    )?;
    ctx.register_table("vcf", Arc::new(provider))?;
    Ok(ctx)
}

/// Test: single chromosome filter via TBI index pushdown.
#[tokio::test]
async fn test_vcf_single_region_query() -> datafusion::error::Result<()> {
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
async fn test_vcf_multi_chromosome_query() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chroms = collect_distinct_chroms(
        &ctx,
        "SELECT DISTINCT chrom FROM vcf WHERE chrom IN ('21', '22')",
    )
    .await;

    assert!(chroms.contains("21"), "Expected 21 in results: {chroms:?}");
    assert!(chroms.contains("22"), "Expected 22 in results: {chroms:?}");

    let count = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom IN ('21', '22')").await;
    assert_eq!(count, 500 + 500, "Expected 1000 variants on chr 21 + 22");

    Ok(())
}

/// Test: full scan without chromosome filter.
#[tokio::test]
async fn test_vcf_full_scan_total_count() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    assert_eq!(total, 1000, "Expected 1000 total variants in full scan");

    let chr21 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let chr22 = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '22'").await;

    assert_eq!(
        total,
        chr21 + chr22,
        "Full scan total ({total}) should equal sum of per-chrom counts (21={chr21}, 22={chr22})"
    );

    Ok(())
}

/// Test: record-level filter (quality score).
#[tokio::test]
async fn test_vcf_record_level_filter() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let total = count_rows(&ctx, "SELECT chrom FROM vcf").await;
    let filtered = count_rows(&ctx, "SELECT chrom FROM vcf WHERE qual >= 50").await;

    assert!(filtered > 0, "Expected some variants with quality >= 50");
    assert!(
        filtered < total,
        "Filtered count ({filtered}) should be < total ({total})"
    );

    Ok(())
}

/// Test: combined genomic region + record-level filter.
#[tokio::test]
async fn test_vcf_combined_filters() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;
    let combined = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND qual >= 50",
    )
    .await;

    assert!(
        combined > 0,
        "Expected some variants matching combined filter"
    );
    assert!(
        combined <= chr21_total,
        "Combined filter count ({combined}) should be <= chr21 total ({chr21_total})"
    );

    Ok(())
}

/// Test: genomic region with position bounds.
#[tokio::test]
async fn test_vcf_region_with_positions() -> datafusion::error::Result<()> {
    let ctx = setup_vcf_ctx().await?;

    let chr21_total = count_rows(&ctx, "SELECT chrom FROM vcf WHERE chrom = '21'").await;

    let region_count = count_rows(
        &ctx,
        "SELECT chrom FROM vcf WHERE chrom = '21' AND start >= 5009999 AND start <= 5029999",
    )
    .await;

    assert!(
        region_count > 0,
        "Expected variants in the specified region"
    );
    assert!(
        region_count < chr21_total,
        "Region count ({region_count}) should be < chr21 total ({chr21_total})"
    );

    Ok(())
}

/// Test: correctness of indexed vs full scan results.
#[tokio::test]
async fn test_vcf_indexed_correctness() -> datafusion::error::Result<()> {
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
        "Indexed chr21 count ({indexed_count}) should equal manual count from full scan ({manual_count})"
    );

    Ok(())
}

/// Test: schema metadata contains bio.vcf.contigs.indexed with TBI-derived contigs.
#[tokio::test]
async fn test_vcf_contigs_indexed_metadata() -> datafusion::error::Result<()> {
    let provider = VcfTableProvider::new(
        "tests/multi_chrom.vcf.gz".to_string(),
        None,
        None,
        None,
        true,
    )?;

    let schema = provider.schema();
    let metadata = schema.metadata();

    // The indexed contigs key must be present when a TBI index exists
    let indexed_json = metadata
        .get(VCF_CONTIGS_INDEXED_KEY)
        .expect("bio.vcf.contigs.indexed should be present when TBI index exists");

    let indexed_contigs: Vec<String> =
        serde_json::from_str(indexed_json).expect("should be a valid JSON array of strings");

    // multi_chrom.vcf.gz has data on chromosomes 21 and 22
    assert_eq!(
        indexed_contigs.len(),
        2,
        "Expected 2 indexed contigs, got: {indexed_contigs:?}"
    );
    assert!(
        indexed_contigs.contains(&"21".to_string()),
        "Expected '21' in indexed contigs: {indexed_contigs:?}"
    );
    assert!(
        indexed_contigs.contains(&"22".to_string()),
        "Expected '22' in indexed contigs: {indexed_contigs:?}"
    );

    Ok(())
}

/// Test: bio.vcf.contigs.indexed is absent when no TBI index is available.
#[tokio::test]
async fn test_vcf_contigs_indexed_absent_without_index() -> datafusion::error::Result<()> {
    // Use a plain (uncompressed) VCF that has no TBI index
    let provider = VcfTableProvider::new(
        "tests/head_106667_tail_6.vcf".to_string(),
        None,
        None,
        None,
        true,
    )?;

    let schema = provider.schema();
    let metadata = schema.metadata();

    assert!(
        !metadata.contains_key(VCF_CONTIGS_INDEXED_KEY),
        "bio.vcf.contigs.indexed should NOT be present without an index"
    );

    Ok(())
}

/// Test: bio.vcf.contigs.indexed is populated from a CSI index.
#[tokio::test]
async fn test_vcf_contigs_indexed_metadata_csi() -> datafusion::error::Result<()> {
    // multi_chrom_csi.vcf.gz has only a .csi index (no .tbi)
    let provider = VcfTableProvider::new(
        "tests/multi_chrom_csi.vcf.gz".to_string(),
        None,
        None,
        None,
        true,
    )?;

    let schema = provider.schema();
    let metadata = schema.metadata();

    let indexed_json = metadata
        .get(VCF_CONTIGS_INDEXED_KEY)
        .expect("bio.vcf.contigs.indexed should be present when CSI index exists");

    let indexed_contigs: Vec<String> =
        serde_json::from_str(indexed_json).expect("should be a valid JSON array of strings");

    assert_eq!(
        indexed_contigs.len(),
        2,
        "Expected 2 indexed contigs from CSI, got: {indexed_contigs:?}"
    );
    assert!(
        indexed_contigs.contains(&"21".to_string()),
        "Expected '21' in CSI-indexed contigs: {indexed_contigs:?}"
    );
    assert!(
        indexed_contigs.contains(&"22".to_string()),
        "Expected '22' in CSI-indexed contigs: {indexed_contigs:?}"
    );

    Ok(())
}
