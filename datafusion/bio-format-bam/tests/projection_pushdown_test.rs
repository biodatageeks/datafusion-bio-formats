//! Projection pushdown tests for BAM and SAM formats.
//!
//! Tests verify both execution plan analysis (no ProjectionExec wrapper, correct
//! leaf exec schema) and data correctness when projecting subsets of columns.

use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_core::test_utils::{assert_plan_projection, find_leaf_exec};
use std::sync::Arc;

const BAM_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/multi_chrom.bam");

async fn setup_bam_ctx(table_name: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let table = BamTableProvider::new(BAM_PATH.to_string(), None, true, None).await?;
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(ctx)
}

// ── Plan analysis tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_bam_plan_single_column_projection() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT name FROM t").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "BamExec", &["name"]);
    Ok(())
}

#[tokio::test]
async fn test_bam_plan_multi_column_projection() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT chrom, start, \"end\" FROM t").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "BamExec", &["chrom", "start", "end"]);
    Ok(())
}

#[tokio::test]
async fn test_bam_plan_no_projection_select_star() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT * FROM t").await?;
    let plan = df.create_physical_plan().await?;
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "BamExec");
    // SELECT * should have all 12 core columns
    assert_eq!(leaf.schema().fields().len(), 12);
    Ok(())
}

// ── Data correctness tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_bam_projection_single_column() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT name FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421); // multi_chrom.bam has 421 reads
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "name");
    }
    Ok(())
}

#[tokio::test]
async fn test_bam_projection_position_columns() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT chrom, start, \"end\" FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);

    // Verify first batch has correct column types
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 3);
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some()
    );
    assert!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .is_some()
    );
    assert!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn test_bam_projection_reordered_columns() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx
        .sql("SELECT mapping_quality, chrom, name FROM t")
        .await?;
    let batches = df.collect().await?;
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).name(), "mapping_quality");
    assert_eq!(batch.schema().field(1).name(), "chrom");
    assert_eq!(batch.schema().field(2).name(), "name");
    Ok(())
}

#[tokio::test]
async fn test_bam_projection_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT chrom, start FROM t LIMIT 5").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn test_bam_projection_with_count() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT COUNT(chrom) FROM t").await?;
    let batches = df.collect().await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 421);
    Ok(())
}

#[tokio::test]
async fn test_bam_count_star() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT COUNT(*) FROM t").await?;
    let batches = df.collect().await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 421);
    Ok(())
}

// ── Partial batch / no-name projection tests ────────────────────────────────

/// Regression test: selecting columns that exclude `name` must not drop the
/// final partial batch.  `multi_chrom.bam` has 421 reads — with the default
/// batch size (8192) the entire file is one partial batch.
#[tokio::test]
async fn test_bam_projection_no_name_returns_all_rows() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx.sql("SELECT chrom FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "chrom");
    }
    Ok(())
}

/// Regression test: filtered projection without `name` — verify row counts per
/// chromosome are correct even through the indexed path.
#[tokio::test]
async fn test_bam_projection_no_name_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_bam_ctx("t").await?;
    let df = ctx
        .sql("SELECT chrom, start FROM t WHERE chrom = 'chr1'")
        .await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 160);
    Ok(())
}

// ── SAM tests ───────────────────────────────────────────────────────────────

/// Helper: write a small SAM file, return its path in a temp dir that must be kept alive.
async fn write_sam_file() -> Result<(tempfile::TempDir, String), Box<dyn std::error::Error>> {
    let tmp = tempfile::TempDir::new()?;
    let sam_path = tmp.path().join("test.sam");
    let content = "@HD\tVN:1.6\tSO:coordinate\n\
                    @SQ\tSN:chr1\tLN:249250621\n\
                    read1\t0\tchr1\t100\t60\t10M\t*\t0\t0\tACGTACGTAC\tIIIIIIIIII\n\
                    read2\t0\tchr1\t200\t50\t10M\t*\t0\t0\tGCTAGCTAGC\tJJJJJJJJJJ\n\
                    read3\t16\tchr1\t300\t40\t10M\t*\t0\t0\tTTTTAAAACC\tKKKKKKKKKK\n";
    tokio::fs::write(&sam_path, content).await?;
    Ok((tmp, sam_path.to_string_lossy().to_string()))
}

#[tokio::test]
async fn test_sam_plan_projection() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, sam_path) = write_sam_file().await?;
    let table = BamTableProvider::new(sam_path, None, true, None).await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;
    let df = ctx.sql("SELECT name, chrom FROM t").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "BamExec", &["name", "chrom"]);
    Ok(())
}

#[tokio::test]
async fn test_sam_projection_single_column() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, sam_path) = write_sam_file().await?;
    let table = BamTableProvider::new(sam_path, None, true, None).await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;
    let df = ctx.sql("SELECT name FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "read1");
    assert_eq!(arr.value(1), "read2");
    assert_eq!(arr.value(2), "read3");
    Ok(())
}

#[tokio::test]
async fn test_sam_projection_reordered() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, sam_path) = write_sam_file().await?;
    let table = BamTableProvider::new(sam_path, None, true, None).await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;
    let df = ctx.sql("SELECT mapping_quality, name FROM t").await?;
    let batches = df.collect().await?;
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).name(), "mapping_quality");
    assert_eq!(batch.schema().field(1).name(), "name");
    Ok(())
}
