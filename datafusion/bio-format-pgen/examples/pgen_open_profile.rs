//! Times opening a PGEN fileset, which is dominated by parsing the `.pvar`.
//!
//! This is the scan's serial prologue: it happens once, before any partition
//! runs, so it does not shrink when partitions are added and is a fixed floor
//! under every multi-partition read.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-pgen --example pgen_open_profile \
//!     -- <path.pgen> [repeats]

use std::time::Instant;

use datafusion::catalog::TableProvider;
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: pgen_open_profile <path.pgen> [repeats]");
    let repeats: usize = args
        .next()
        .map(|value| value.parse().expect("repeats must be a number"))
        .unwrap_or(5);

    let mut best = f64::MAX;
    for round in 1..=repeats {
        let options = PgenReadOptions {
            genotype_fields: Some(vec!["DS".to_string()]),
            ..Default::default()
        };
        let started = Instant::now();
        let provider = PgenTableProvider::try_new(path.clone(), options)
            .await
            .expect("open fileset");
        let elapsed = started.elapsed().as_secs_f64();
        best = best.min(elapsed);
        let variants = TableProvider::schema(&provider)
            .metadata()
            .get("bio.pgen.variant_count")
            .cloned()
            .unwrap_or_default();
        println!("round {round}: {elapsed:.4}s  variants={variants}");
    }
    println!("best: {best:.4}s");
}
