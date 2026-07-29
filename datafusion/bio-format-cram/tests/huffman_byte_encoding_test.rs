//! Regression tests for CRAM files whose bases series uses Huffman coding.
//!
//! Reading such a file used to abort the query with
//! `Partition task failed: task N panicked with message "not yet implemented"`,
//! raised from `noodles_cram`'s `Byte::decode_take`, whose `Huffman` arm was a
//! `todo!()`. Only the single-byte `Byte::decode` path handled Huffman; the bulk
//! `decode_take` path used for the bases (BA) series did not.
//! See biodatageeks/polars-bio#429 and zaeleus/noodles#393.
//!
//! # Why the fixture looks the way it does
//!
//! `tests/huffman_byte_encoding.cram` is written by samtools 1.21 in `no_ref`
//! mode, so no external reference is needed. It holds 500 reads: 150 mapped to
//! chr1, 150 mapped to chr2, and 200 unmapped. Every read is poly-N, which makes
//! htslib encode the bases series as Huffman over a single-symbol alphabet —
//! `Huffman { alphabet: [78], bit_lens: [0] }`, verified by instrumenting the
//! decoder. This is the shape that arises in practice, in the poly-N unmapped
//! tail of a WGS file.
//!
//! Two properties of the fixture are load-bearing, so please preserve them when
//! regenerating it:
//!
//! 1. **The unmapped reads are required.** In noodles the bases series is read
//!    only by `read_unmapped_read`; mapped records reconstruct their sequence
//!    from features instead. A fixture of mapped reads alone never reaches
//!    `decode_take` and so cannot catch this regression.
//! 2. **The fixture ships without a `.crai`**, which keeps these tests on the
//!    sequential read path. The indexed path over the same data is covered by
//!    `indexed_unmapped_test.rs`, which uses `unmapped_indexed.cram`.
//!
//! The first property was confirmed against a pre-fix baseline, where an
//! unindexed scan panics with `not yet implemented`.

use datafusion::arrow::array::{Array, StringArray};
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

const EXPECTED_TOTAL: u64 = 500;
const EXPECTED_MAPPED_PER_CHROM: u64 = 150;
const READ_LENGTH: usize = 60;

/// Registers the Huffman-encoded fixture as table `cram`.
async fn setup_ctx() -> datafusion::error::Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = CramTableProvider::new(
        "tests/huffman_byte_encoding.cram".to_string(),
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

/// A full scan must decode every record, including the Huffman-coded unmapped
/// block, rather than panicking in `decode_take`.
#[tokio::test]
async fn test_huffman_cram_full_scan_does_not_panic() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    let count = count_rows(&ctx, "SELECT * FROM cram").await;
    assert_eq!(
        count, EXPECTED_TOTAL,
        "expected all {EXPECTED_TOTAL} reads (300 mapped + 200 unmapped) to decode"
    );

    Ok(())
}

/// The Huffman-coded bases series must decode to the correct sequence, not just
/// avoid panicking. Every read in the fixture is poly-N.
#[tokio::test]
async fn test_huffman_cram_sequences_decode_correctly() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    let df = ctx.sql("SELECT sequence FROM cram").await?;
    let batches = df.collect().await?;

    let expected = "N".repeat(READ_LENGTH);
    let mut checked = 0u64;

    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray for sequence column");

        for i in 0..col.len() {
            assert!(!col.is_null(i), "sequence should not be null");
            assert_eq!(
                col.value(i),
                expected,
                "Huffman-decoded sequence differs from what samtools wrote"
            );
            checked += 1;
        }
    }

    assert_eq!(
        checked, EXPECTED_TOTAL,
        "expected to check every read's sequence"
    );

    Ok(())
}

/// Decoding the Huffman block must not disturb the mapped records: both
/// references should still report their full complement of reads.
#[tokio::test]
async fn test_huffman_cram_mapped_reads_by_chrom() -> datafusion::error::Result<()> {
    let ctx = setup_ctx().await?;

    for chrom in ["chr1", "chr2"] {
        let count = count_rows(
            &ctx,
            &format!("SELECT chrom FROM cram WHERE chrom = '{chrom}'"),
        )
        .await;
        assert_eq!(
            count, EXPECTED_MAPPED_PER_CHROM,
            "expected {EXPECTED_MAPPED_PER_CHROM} reads on {chrom}"
        );
    }

    Ok(())
}
