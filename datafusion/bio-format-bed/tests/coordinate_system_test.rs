// Regression tests for BED coordinate-system conversion (issue #413).
//
// A BED file stores intervals as 0-based half-open `[start, end)`. The reader
// must therefore emit, for an on-disk record `chrom  start  end`:
//   * zero_based = true  -> (start, end)         unchanged: 0-based half-open
//   * zero_based = false -> (start + 1, end)      1-based closed
//
// Before the fix the zero_based path decremented `end` by one, producing
// 0-based *closed* intervals (`end - 1`), contradicting the documented
// 0-based half-open semantics. The end coordinate is identical in both
// systems because a 1-based inclusive end equals a 0-based exclusive end.
use datafusion::arrow::array::UInt32Array;
use datafusion::prelude::*;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use std::sync::Arc;

/// Read the fixture and return the (start, end) columns in file order.
async fn read_coords(zero_based: bool) -> (Vec<u32>, Vec<u32>) {
    let path = format!("{}/tests/coords.bed", env!("CARGO_MANIFEST_DIR"));
    let provider = BedTableProvider::new(path, BEDFields::BED4, None, zero_based).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("bed", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT start, \"end\" FROM bed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for b in &batches {
        let s = b
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("start is UInt32");
        let e = b
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("end is UInt32");
        for i in 0..b.num_rows() {
            starts.push(s.value(i));
            ends.push(e.value(i));
        }
    }
    (starts, ends)
}

// On-disk fixture (tests/coords.bed):
//   chr1  1000  2000
//   chr2  1500  2500
//   chrX  5000  8000
const FILE_STARTS: [u32; 3] = [1000, 1500, 5000];
const FILE_ENDS: [u32; 3] = [2000, 2500, 8000];

#[tokio::test]
async fn zero_based_outputs_0based_half_open() {
    let (starts, ends) = read_coords(true).await;
    // start unchanged, end unchanged -> identical to the on-disk record.
    assert_eq!(
        starts, FILE_STARTS,
        "0-based start must equal the file start"
    );
    assert_eq!(
        ends, FILE_ENDS,
        "0-based half-open end must equal the file end (not end - 1)"
    );
}

#[tokio::test]
async fn one_based_outputs_1based_closed() {
    let (starts, ends) = read_coords(false).await;
    let expected_starts: Vec<u32> = FILE_STARTS.iter().map(|s| s + 1).collect();
    // start + 1, end unchanged.
    assert_eq!(
        starts, expected_starts,
        "1-based start must be file start + 1"
    );
    assert_eq!(
        ends.as_slice(),
        FILE_ENDS,
        "1-based closed end must equal the file end"
    );
}

#[tokio::test]
async fn both_systems_preserve_interval_length() {
    // The number of covered bases must be identical regardless of coordinate
    // system: half-open length = end - start; closed length = end - (start) + 1
    // ... but here the lengths are computed against their own start, so both
    // must equal the original on-disk span (end - start).
    let (z_starts, z_ends) = read_coords(true).await;
    let (o_starts, o_ends) = read_coords(false).await;
    for i in 0..FILE_STARTS.len() {
        let half_open_len = z_ends[i] - z_starts[i];
        let closed_len = o_ends[i] - o_starts[i] + 1;
        let file_span = FILE_ENDS[i] - FILE_STARTS[i];
        assert_eq!(half_open_len, file_span, "half-open length mismatch");
        assert_eq!(closed_len, file_span, "closed length mismatch");
    }
}
