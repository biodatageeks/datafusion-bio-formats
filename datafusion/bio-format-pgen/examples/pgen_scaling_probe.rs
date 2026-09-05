//! Measures how a PGEN scan scales with partition count, with no Python in the
//! way, and reports the scan metrics alongside the wall clock.
//!
//! Poor scaling has two candidate explanations that look identical from the
//! outside: the work does not divide (a serial stage dominates), or it divides
//! but each partition redoes some of it. `DependencyRecords` separates them —
//! it counts records a partition must decode to reconstruct an LD chain but
//! never emits, which is duplicated work that grows with the partition count.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-pgen --example pgen_scaling_probe \
//!     -- <path.pgen> [field] [partitions...]

use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::genotype::GenotypeMetric;
use datafusion_bio_format_pgen::{PgenExec, PgenReadOptions, PgenTableProvider};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: pgen_scaling_probe <path.pgen> [field] [partitions...]");
    let field = args.next().unwrap_or_else(|| "DS".to_string());
    let partition_counts: Vec<usize> = {
        let rest: Vec<usize> = args.map(|v| v.parse().expect("partition count")).collect();
        if rest.is_empty() {
            vec![1, 2, 4, 8, 16]
        } else {
            rest
        }
    };

    println!(
        "{:>5} {:>9} {:>8} {:>12} {:>12} {:>10} {:>12}",
        "parts", "seconds", "speedup", "emitted", "dependency", "ranges", "bytes read"
    );
    let mut baseline = None;

    for partitions in partition_counts {
        let options = PgenReadOptions {
            genotype_fields: Some(vec![field.clone()]),
            ..Default::default()
        };
        let provider = PgenTableProvider::try_new(path.clone(), options)
            .await
            .expect("open fileset");
        let config = SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(1 << 20);
        let context = SessionContext::new_with_config(config);
        context
            .register_table("pgen", Arc::new(provider))
            .expect("register");

        // Execute the plan we hold, not a fresh one: `DataFrame::collect` builds
        // its own physical plan, and reading metrics off a different instance
        // reports an unexecuted scan.
        let plan = context
            .sql("SELECT genotypes FROM pgen")
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");

        let started = Instant::now();
        let batches = datafusion::physical_plan::collect(plan.clone(), context.task_ctx())
            .await
            .expect("collect");
        let elapsed = started.elapsed().as_secs_f64();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let exec = find_pgen_exec(&plan).expect("PgenExec in plan");
        let snapshot = exec.metrics_snapshot();
        let get = |metric: GenotypeMetric| {
            snapshot
                .iter()
                .find(|(m, _)| *m == metric)
                .map(|(_, v)| *v)
                .unwrap_or(0)
        };

        let speedup = baseline.map_or(1.0, |b: f64| b / elapsed);
        if baseline.is_none() {
            baseline = Some(elapsed);
        }
        println!(
            "{:>5} {:>9.3} {:>7.2}x {:>12} {:>12} {:>10} {:>12}",
            partitions,
            elapsed,
            speedup,
            rows,
            get(GenotypeMetric::DependencyRecords),
            get(GenotypeMetric::RangeRequests),
            get(GenotypeMetric::PrimaryBytesRead),
        );
    }
}

/// The collected plan is wrapped by coalesce/repartition nodes, so walk to it.
fn find_pgen_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&PgenExec> {
    if let Some(exec) = plan.downcast_ref::<PgenExec>() {
        return Some(exec);
    }
    for child in plan.children() {
        if let Some(found) = find_pgen_exec(child) {
            return Some(found);
        }
    }
    None
}
