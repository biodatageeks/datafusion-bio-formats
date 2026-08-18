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
//!
//! The floor phase reads the whole file into memory to walk its records, which
//! is fine for a chromosome — chr22 is 160 MB — and is not for a whole-genome
//! BGEN. Point it at one chromosome at a time.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Array;
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
    // Layout 2 defines three compression modes and this probe handles all of
    // them, so only an out-of-range flag is refused. The zlib-only assertion the
    // previous commit carried is superseded by the dispatch below.
    assert!(
        compression <= 2,
        "unknown BGEN compression flag {compression}"
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
        let payload = &bytes[data_start..data_start + data_len];
        let written = match compression {
            // Nothing to decompress; copying the block is what a reader still
            // does to hand it to the decoder.
            0 => {
                out[..expanded].copy_from_slice(payload);
                expanded
            }
            1 => decompressor
                .zlib_decompress(payload, &mut out[..expanded])
                .expect("zlib inflate"),
            _ => zstd::bulk::decompress_to_buffer(payload, &mut out[..expanded])
                .expect("zstd decompress"),
        };
        // Touch the output so the decompression cannot be optimized away. A
        // zero-length block is pathological but representable, and indexing
        // `written - 1` would panic on it.
        checksum += written as u64 + out[..written].last().copied().unwrap_or(0) as u64;
    }
    let inflate = inflate_started.elapsed();

    let codec = match compression {
        0 => "copy (none)",
        1 => "zlib inflate",
        _ => "zstd decode",
    };
    println!(
        "\nrecord walk      {:>8.3} s   ({variant_count} records)",
        walk.as_secs_f64()
    );
    println!(
        "{codec:<16} {:>8.3} s   {:.2} GB out from {:.2} GB in, {:.2} GB/s produced  [checksum {checksum}]",
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
                // `BGEN_PLOIDY=1` keeps the PLOIDY child, to compare what a
                // scan that emits it costs against one that does not.
                genotype_fields: if std::env::var("BGEN_PLOIDY").is_ok() {
                    None
                } else {
                    Some(vec!["DS".to_string()])
                },
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
        let digest_enabled = std::env::var("BGEN_DIGEST").is_ok();
        let mut digest = 0xcbf29ce484222325_u64;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            rows += batch.num_rows();
            batches += 1;
            // FNV over every emitted dosage bit pattern and every ploidy byte,
            // so two builds can be compared cell for cell rather than by total.
            if !digest_enabled {
                continue;
            }
            let genotypes = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()
                .expect("genotypes struct");
            // However many children the projection kept: this defaults to DS
            // alone, so a fixed [0, 1] would index past the struct.
            for child in 0..genotypes.num_columns() {
                let column = genotypes.column(child);
                let list = column
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::ListArray>()
                    .expect("list child");
                let values = list.values();
                if let Some(floats) = values
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float32Array>()
                {
                    for index in 0..floats.len() {
                        let bits = if floats.is_null(index) {
                            0xffff_ffff_u32
                        } else {
                            floats.value(index).to_bits()
                        };
                        digest = (digest ^ bits as u64).wrapping_mul(0x100000001b3);
                    }
                } else if let Some(bytes) = values
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt8Array>()
                {
                    for index in 0..bytes.len() {
                        digest = (digest ^ bytes.value(index) as u64).wrapping_mul(0x100000001b3);
                    }
                }
            }
        }
        let scan = started.elapsed();
        // The floor above was measured on one thread, so subtracting it only
        // says anything about a one-partition scan; a parallel scan divides the
        // decompression too and the difference would be meaningless.
        if target == 1 {
            println!(
                "scan t={target:<2}         {:>8.3} s   rows={rows} batches={batches}   \
                 inflate floor {:>6.3} s, everything else {:>6.3} s, digest {}",
                scan.as_secs_f64(),
                inflate.as_secs_f64(),
                scan.as_secs_f64() - inflate.as_secs_f64(),
                // Printing the untouched seed would read as a real digest.
                if digest_enabled {
                    format!("{digest:016x}")
                } else {
                    "disabled".to_string()
                }
            );
        } else {
            println!(
                "scan t={target:<2}         {:>8.3} s   rows={rows} batches={batches}",
                scan.as_secs_f64()
            );
        }
    }
}
