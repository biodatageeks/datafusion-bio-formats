//! Projection pushdown tests for CRAM format.
//!
//! Tests verify both execution plan analysis (no ProjectionExec wrapper, correct
//! leaf exec schema) and data correctness when projecting subsets of columns.

use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::prelude::*;
use datafusion_bio_format_core::test_utils::{assert_plan_projection, find_leaf_exec};
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

const CRAM_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/multi_chrom.cram");

async fn setup_cram_ctx(table_name: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let table = CramTableProvider::new(
        CRAM_PATH.to_string(),
        None, // no reference (no_ref mode)
        None,
        true,
        None,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(ctx)
}

// ── Plan analysis tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_cram_plan_single_column_projection() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
    let df = ctx.sql("SELECT name FROM t").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "CramExec", &["name"]);
    Ok(())
}

#[tokio::test]
async fn test_cram_plan_multi_column_projection() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
    let df = ctx.sql("SELECT chrom, start, \"end\" FROM t").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "CramExec", &["chrom", "start", "end"]);
    Ok(())
}

#[tokio::test]
async fn test_cram_plan_no_projection_select_star() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
    let df = ctx.sql("SELECT * FROM t").await?;
    let plan = df.create_physical_plan().await?;
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "CramExec");
    assert_eq!(leaf.schema().fields().len(), 11);
    Ok(())
}

// ── Data correctness tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_cram_projection_single_column() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
    let df = ctx.sql("SELECT name FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "name");
    }
    Ok(())
}

#[tokio::test]
async fn test_cram_projection_position_columns() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
    let df = ctx.sql("SELECT chrom, start, \"end\" FROM t").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);

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
async fn test_cram_projection_reordered_columns() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
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
async fn test_cram_projection_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
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
async fn test_cram_count_star() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_cram_ctx("t").await?;
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
