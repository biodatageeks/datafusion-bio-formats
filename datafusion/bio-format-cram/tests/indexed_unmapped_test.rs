//! An indexed CRAM full scan must return the unplaced, unmapped records.
//!
//! Region queries only cover placed reads, so an indexed full scan used to return
//! fewer rows than a sequential scan of the same file — silently, with no error.
//! A CRAI does describe the unmapped slice (its reference sequence ID is -1), and
//! noodles exposes `Reader::query_unmapped` to seek to it, so the reader now emits
//! a dedicated partition for those records.
//!
//! `unmapped_indexed.cram` holds 500 reads — 150 mapped to chr1, 150 to chr2 and
//! 200 unplaced and unmapped — and ships with its `.crai`, whose third entry is
//! the unmapped slice:
//!
//! ```text
//! 0   1  1550  327   196  503
//! 1   1  1550  1047  196  505
//! -1  0  1     1770  185  494
//! ```
//!
//! The same file without an index is covered by `huffman_byte_encoding_test.rs`.

use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

const INDEXED_CRAM: &str = "tests/unmapped_indexed.cram";

const TOTAL_READS: u64 = 500;
const UNMAPPED_READS: u64 = 200;
const MAPPED_PER_CHROM: u64 = 150;

async fn setup_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = CramTableProvider::new(
        INDEXED_CRAM.to_string(),
        None,  // reference_path: None for no_ref CRAM
        None,  // object_storage_options
        true,  // zero-based coordinates
        None,  // tag_fields
        false, // binary_cigar
        true,
        100,
        None,
    )
    .await?;
    ctx.register_table("cram", Arc::new(provider))?;
    Ok(ctx)
}

async fn count_rows(ctx: &SessionContext, sql: &str) -> u64 {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

/// The headline behaviour: an indexed full scan returns every record.
#[tokio::test]
async fn test_indexed_full_scan_includes_unmapped_reads() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    let count = count_rows(&ctx, "SELECT * FROM cram").await;
    assert_eq!(
        count, TOTAL_READS,
        "an indexed full scan must return the unmapped tail as well as placed reads"
    );

    Ok(())
}

/// The unmapped records are the ones with no reference sequence.
#[tokio::test]
async fn test_unmapped_reads_have_no_chrom() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    let count = count_rows(&ctx, "SELECT chrom FROM cram WHERE chrom IS NULL").await;
    assert_eq!(count, UNMAPPED_READS);

    Ok(())
}

/// Reading the unmapped tail must not duplicate the placed reads, which would be
/// the obvious way for a naive implementation to reach the right total.
#[tokio::test]
async fn test_no_records_are_duplicated() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    let distinct = count_rows(&ctx, "SELECT DISTINCT name FROM cram").await;
    assert_eq!(
        distinct, TOTAL_READS,
        "every read name should appear exactly once"
    );

    Ok(())
}

/// A filtered scan asks for placed reads, so it must not pick up the unmapped tail.
#[tokio::test]
async fn test_region_filter_excludes_unmapped_reads() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    for chrom in ["chr1", "chr2"] {
        let count = count_rows(
            &ctx,
            &format!("SELECT chrom FROM cram WHERE chrom = '{chrom}'"),
        )
        .await;
        assert_eq!(
            count, MAPPED_PER_CHROM,
            "a filter on {chrom} should return only its placed reads"
        );
    }

    Ok(())
}
