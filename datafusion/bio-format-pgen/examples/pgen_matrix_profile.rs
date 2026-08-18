//! Times decoding straight into a caller matrix, against the Arrow scan.
//!
//! The scan builds Arrow batches that something else must copy into the
//! destination; this writes the values at their final address. The gap between
//! the two is the copy that copy-elimination removes.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-pgen --example pgen_matrix_profile \
//!     -- <path.pgen> [field] [threads...]

use std::time::Instant;

use datafusion_bio_format_pgen::PgenReadOptions;
use datafusion_bio_format_pgen::matrix::{MatrixData, genotype_matrix_shape, read_genotype_matrix};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: pgen_matrix_profile <path.pgen> [field] [threads...]");
    let field = args.next().unwrap_or_else(|| "DS".to_string());
    let thread_counts: Vec<usize> = {
        let rest: Vec<usize> = args.map(|v| v.parse().expect("thread count")).collect();
        if rest.is_empty() {
            vec![1, 2, 4, 8]
        } else {
            rest
        }
    };

    let options = PgenReadOptions::default();
    let shape = genotype_matrix_shape(path.clone(), &options)
        .await
        .expect("shape");
    let cells = shape.variants * shape.samples;
    println!(
        "{} variants x {} samples = {cells} cells, field {field}",
        shape.variants, shape.samples
    );
    println!(
        "{:>8} {:>9} {:>9} {:>12}",
        "threads", "seconds", "speedup", "checksum"
    );

    let mut base = None;
    for threads in thread_counts {
        // The checksum is deliberately outside the timer: it reads the whole
        // matrix back, which on the dosage workload is another 10 GB and would
        // roughly double the figure being reported.
        let elapsed;
        let checksum;
        if field == "ALT_COUNT" {
            let mut values = vec![0_i8; cells];
            let started = Instant::now();
            read_genotype_matrix(
                path.clone(),
                &options,
                MatrixData::AltCount {
                    values: &mut values,
                    missing: -9,
                },
                threads,
            )
            .await
            .expect("read");
            elapsed = started.elapsed().as_secs_f64();
            checksum = values.iter().map(|&v| i64::from(v)).sum::<i64>();
        } else {
            let mut values = vec![0.0_f32; cells];
            let started = Instant::now();
            read_genotype_matrix(
                path.clone(),
                &options,
                MatrixData::Dosage {
                    values: &mut values,
                    missing: f32::NAN,
                },
                threads,
            )
            .await
            .expect("read");
            elapsed = started.elapsed().as_secs_f64();
            checksum = values
                .iter()
                .map(|&v| if v.is_nan() { 0 } else { v as i64 })
                .sum::<i64>();
        }
        let speedup = base.map_or(1.0, |b: f64| b / elapsed);
        if base.is_none() {
            base = Some(elapsed);
        }
        println!("{threads:>8} {elapsed:>9.3} {speedup:>8.2}x {checksum:>12}");
    }
}
