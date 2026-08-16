//! Reproducible steady-state PGEN `GT` benchmark used by the release gate.
//!
//! Usage:
//! `cargo run --release -p datafusion-bio-format-pgen --example pgen_parity -- <PGEN> [iterations] [partitions]`

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, FixedSizeListArray, ListArray, StructArray, UInt16Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};

async fn execute(context: &SessionContext) -> (usize, usize) {
    let batches = context
        .sql("SELECT genotypes FROM pgen")
        .await
        .expect("plan GT scan")
        .collect()
        .await
        .expect("execute GT scan");
    (
        batches.iter().map(|batch| batch.num_rows()).sum(),
        batches
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum(),
    )
}

async fn oracle_digest(context: &SessionContext) -> GenotypeDigest {
    let batches = context
        .sql("SELECT genotypes FROM pgen")
        .await
        .expect("plan GT checksum scan")
        .collect()
        .await
        .expect("execute GT checksum scan");
    let mut digest = GenotypeDigest::default();
    for batch in &batches {
        digest.update(batch);
    }
    digest
}

#[derive(Default)]
struct GenotypeDigest {
    sample_index: u64,
    valid_count: u64,
    left_sum: u64,
    right_sum: u64,
    weighted_sum: u64,
}

impl GenotypeDigest {
    fn update(&mut self, batch: &RecordBatch) {
        let genotypes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("genotypes struct");
        let gt = genotypes
            .column_by_name("GT")
            .expect("GT child")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("GT outer list");
        let samples = gt
            .values()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("GT fixed-size sample pairs");
        let alleles = samples
            .values()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("GT UInt16 alleles");
        for sample in 0..samples.len() {
            self.sample_index += 1;
            if samples.is_null(sample) {
                continue;
            }
            let left = u64::from(alleles.value(sample * 2));
            let right = u64::from(alleles.value(sample * 2 + 1));
            self.valid_count += 1;
            self.left_sum += left;
            self.right_sum += right;
            self.weighted_sum = self.weighted_sum.wrapping_add(
                self.sample_index
                    .wrapping_mul(left.wrapping_mul(3).wrapping_add(right.wrapping_mul(5))),
            );
        }
    }

    fn display(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.valid_count, self.left_sum, self.right_sum, self.weighted_sum
        )
    }
}

async fn open_context(path: &str, partitions: usize) -> SessionContext {
    let provider = PgenTableProvider::try_new(
        path,
        PgenReadOptions {
            genotype_fields: Some(vec!["GT".to_string()]),
            ..Default::default()
        },
    )
    .await
    .expect("open PGEN fileset");
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(8192),
    );
    context
        .register_table("pgen", Arc::new(provider))
        .expect("register PGEN table");
    context
}

fn median(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

async fn run(path: &str, iterations: usize, partitions: usize) {
    assert!(
        iterations >= 10,
        "release parity requires at least 10 iterations"
    );
    assert!(partitions > 0, "partitions must be positive");

    let open_started = Instant::now();
    let context = open_context(path, partitions).await;
    let provider_open = open_started.elapsed();
    let expected = execute(&context).await;
    let digest = oracle_digest(&context).await;

    let mut scan_times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        assert_eq!(execute(&context).await, expected);
        scan_times.push(started.elapsed());
    }

    let mut end_to_end_times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let fresh = open_context(path, partitions).await;
        assert_eq!(execute(&fresh).await, expected);
        end_to_end_times.push(started.elapsed());
    }

    println!("rows={}", expected.0);
    println!("arrow_bytes={}", expected.1);
    println!("digest={}", digest.display());
    println!("iterations={iterations}");
    println!("partitions={partitions}");
    println!("provider_open_ns={}", provider_open.as_nanos());
    println!("scan_median_ns={}", median(&mut scan_times).as_nanos());
    println!(
        "end_to_end_median_ns={}",
        median(&mut end_to_end_times).as_nanos()
    );
}

fn main() {
    let path = std::env::args().nth(1).expect("PGEN path argument");
    let iterations = std::env::args()
        .nth(2)
        .map(|value| value.parse::<usize>().expect("integer iteration count"))
        .unwrap_or(11);
    let partitions = std::env::args()
        .nth(3)
        .map(|value| value.parse::<usize>().expect("integer partition count"))
        .unwrap_or(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .enable_all()
        .build()
        .expect("build Tokio runtime")
        .block_on(run(&path, iterations, partitions));
}
