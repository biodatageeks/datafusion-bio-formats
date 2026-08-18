//! Splits a BGEN scan into the two costs it is made of: decompressing the
//! probability blocks, and turning them into output.
//!
//! The decompression phase walks the variant records itself and calls
//! libdeflate on each payload, which is the same work any reader of this file
//! must do, in the same library the C readers use. Whatever the full scan costs
//! beyond that is this provider's own.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-bgen --example bgen_decode_profile \
//!     -- <path.bgen> [partitions...]

use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_bgen::{BgenOutputMode, BgenReadOptions, BgenTableProvider};
use futures::StreamExt;

fn u16le(b: &[u8], o: usize) -> usize {
    u16::from_le_bytes([b[o], b[o + 1]]) as usize
}
fn u32le(b: &[u8], o: usize) -> usize {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]) as usize
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: bgen_decode_profile <path.bgen> [partitions...]");

    let bytes = std::fs::read(&path).expect("read bgen");
    let start_of_data = u32le(&bytes, 0) + 4;
    let header_len = u32le(&bytes, 4);
    let variant_count = u32le(&bytes, 8);
    let sample_count = u32le(&bytes, 12);
    let flags = u32le(&bytes, 4 + header_len - 4);
    let compression = flags & 3;
    let layout = (flags >> 2) & 0xf;
    println!(
        "{variant_count} variants x {sample_count} samples, layout {layout}, compression {compression}, {} MB",
        bytes.len() / 1_000_000
    );
    assert_eq!(layout, 2, "this probe only walks layout 2");
    // Layout 2 defines three compression modes and the reader supports all of
    // them; this probe only inflates. Dispatching on the flag lands one commit
    // later in the stack, and until then a clear refusal beats handing a zstd
    // or uncompressed block to libdeflate and reporting its complaint.
    assert_eq!(
        compression, 1,
        "this probe only handles zlib (flag 1); see perf/bgen-projectable-ploidy \
         for full dispatch"
    );

    // --- Phase A: walk the records and decompress every payload ---
    let mut decompressor = libdeflater::Decompressor::new();
    let mut out = vec![0_u8; 64 << 20];
    let mut cursor = start_of_data;
    let mut walked = 0_usize;
    let mut compressed_total = 0_usize;
    let mut decompressed_total = 0_usize;
    let mut payload_spans: Vec<(usize, usize, usize)> = Vec::with_capacity(variant_count);

    let walk_started = Instant::now();
    for _ in 0..variant_count {
        for _ in 0..3 {
            let n = u16le(&bytes, cursor);
            cursor += 2 + n;
        }
        cursor += 4; // position
        let alleles = u16le(&bytes, cursor);
        cursor += 2;
        for _ in 0..alleles {
            let n = u32le(&bytes, cursor);
            cursor += 4 + n;
        }
        let stored = u32le(&bytes, cursor);
        cursor += 4;
        let (data_start, data_len, expanded) = if compression == 0 {
            (cursor, stored, stored)
        } else {
            (cursor + 4, stored - 4, u32le(&bytes, cursor))
        };
        payload_spans.push((data_start, data_len, expanded));
        compressed_total += data_len;
        decompressed_total += expanded;
        cursor += stored;
        walked += 1;
    }
    let walk = walk_started.elapsed();
    assert_eq!(walked, variant_count);

    let inflate_started = Instant::now();
    let mut checksum = 0_u64;
    for &(data_start, data_len, expanded) in &payload_spans {
        if out.len() < expanded {
            out.resize(expanded, 0);
        }
        let written = decompressor
            .zlib_decompress(
                &bytes[data_start..data_start + data_len],
                &mut out[..expanded],
            )
            .expect("inflate");
        // Touch the output so the decompression cannot be optimized away.
        checksum += written as u64 + out[written - 1] as u64;
    }
    let inflate = inflate_started.elapsed();

    println!(
        "\nrecord walk      {:>8.3} s   ({variant_count} records)",
        walk.as_secs_f64()
    );
    println!(
        "zlib inflate     {:>8.3} s   {:.2} GB out from {:.2} GB in, {:.2} GB/s produced  [checksum {checksum}]",
        inflate.as_secs_f64(),
        decompressed_total as f64 / 1e9,
        compressed_total as f64 / 1e9,
        decompressed_total as f64 / 1e9 / inflate.as_secs_f64()
    );
    println!(
        "walk + inflate   {:>8.3} s   <- the floor any reader of this file pays",
        (walk + inflate).as_secs_f64()
    );

    // --- Phase B: the provider's own scan, no Python, batches dropped ---
    let partitions: Vec<usize> = std::env::args()
        .skip(2)
        .map(|value| value.parse().expect("partition count"))
        .collect();
    let partitions = if partitions.is_empty() {
        vec![1]
    } else {
        partitions
    };

    println!();
    for target in partitions {
        let provider = BgenTableProvider::try_new(
            path.clone(),
            BgenReadOptions {
                output_mode: BgenOutputMode::Dosage,
                ..Default::default()
            },
        )
        .await
        .expect("open bgen");
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(target));
        context.register_table("b", Arc::new(provider)).unwrap();

        let started = Instant::now();
        let mut stream = context
            .sql("SELECT genotypes FROM b")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let mut rows = 0_usize;
        let mut batches = 0_usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            rows += batch.num_rows();
            batches += 1;
        }
        let scan = started.elapsed();
        // The floor above was measured on one thread, so subtracting it only
        // says anything about a one-partition scan; a parallel scan divides the
        // decompression too and the difference would be meaningless.
        if target == 1 {
            println!(
                "scan t={target:<2}         {:>8.3} s   rows={rows} batches={batches}   \
                 inflate floor {:>6.3} s, everything else {:>6.3} s",
                scan.as_secs_f64(),
                inflate.as_secs_f64(),
                scan.as_secs_f64() - inflate.as_secs_f64()
            );
        } else {
            println!(
                "scan t={target:<2}         {:>8.3} s   rows={rows} batches={batches}",
                scan.as_secs_f64()
            );
        }
    }
}
