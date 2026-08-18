//! Times a single-partition genotype scan of a real fileset, with no Python and
//! no materialization into a contiguous array, so the decode path can be profiled
//! and compared against pgenlib directly: `DS` against `read_dosages_list`,
//! `ALT_COUNT` against `read_list`.
//!
//! Usage:
//!   cargo run --release --example pgen_ds_profile -- <path.pgen> [repeats] [field]

use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: pgen_ds_profile <path.pgen> [repeats] [field]");
    let repeats: usize = args
        .next()
        .map(|value| value.parse().expect("repeats must be a number"))
        .unwrap_or(3);
    let field = args.next().unwrap_or_else(|| "DS".to_string());

    for round in 1..=repeats {
        let options = PgenReadOptions {
            genotype_fields: Some(vec![field.clone()]),
            ..Default::default()
        };
        let provider = PgenTableProvider::try_new(path.clone(), options)
            .await
            .expect("open fileset");
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1 << 20);
        let context = SessionContext::new_with_config(config);
        context
            .register_table("pgen", Arc::new(provider))
            .expect("register");

        let started = Instant::now();
        let batches = context
            .sql("SELECT genotypes FROM pgen")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let elapsed = started.elapsed();

        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        println!(
            "round {round}: {field} {:.3}s  rows={rows}  batches={}",
            elapsed.as_secs_f64(),
            batches.len()
        );
    }
}
