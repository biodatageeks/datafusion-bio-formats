//! Times the matrix path against the Arrow scan it replaces.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-bgen --example bgen_matrix_profile \
//!     -- <path.bgen> [threads...]

use std::time::Instant;

use datafusion_bio_format_bgen::matrix::{genotype_matrix_shape, read_genotype_matrix};
use datafusion_bio_format_bgen::{BgenOutputMode, BgenReadOptions};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: bgen_matrix_profile <path.bgen> [threads...]");
    let threads: Vec<usize> = {
        let rest: Vec<usize> = std::env::args()
            .skip(2)
            .map(|value| value.parse().expect("thread count"))
            .collect();
        if rest.is_empty() {
            vec![1, 2, 4, 8]
        } else {
            rest
        }
    };
    let options = BgenReadOptions {
        output_mode: BgenOutputMode::Dosage,
        genotype_fields: Some(vec!["DS".to_string()]),
        ..Default::default()
    };
    let shape = genotype_matrix_shape(path.clone(), options.clone())
        .await
        .expect("shape");
    println!(
        "{} variants x {} samples = {} cells",
        shape.variants,
        shape.samples,
        shape.variants * shape.samples
    );
    println!(
        "{:>8} {:>10} {:>9} {:>14}",
        "threads", "seconds", "speedup", "checksum"
    );
    let mut base = None;
    for count in threads {
        let mut values = vec![0.0_f32; shape.variants * shape.samples];
        let started = Instant::now();
        read_genotype_matrix(path.clone(), options.clone(), &mut values, f32::NAN, count)
            .await
            .expect("matrix");
        let elapsed = started.elapsed().as_secs_f64();
        // Outside the timer: reading 10 GB back would dominate the figure.
        let checksum: f64 = values.iter().step_by(9_973).map(|v| *v as f64).sum();
        let first = *base.get_or_insert(elapsed);
        println!(
            "{count:>8} {elapsed:>10.3} {:>8.2}x {checksum:>14.1}",
            first / elapsed
        );
    }
}
