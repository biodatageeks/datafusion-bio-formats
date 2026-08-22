//! Profile source-partition balance for a local BigWig or BigBed file.
//!
//! Usage: `cargo run -p datafusion-bio-format-bbi --example partition_profile
//! --release -- <bigwig|bigbed> <path> <partitions> [count|decode|aggregate]`

use std::env;
use std::sync::Arc;
use std::time::Instant;

use datafusion::catalog::TableProvider;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion_bio_format_bbi::bigbed::{BigBedSchemaMode, BigBedTableProvider};
use datafusion_bio_format_bbi::bigwig::BigWigTableProvider;
use futures_util::StreamExt;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = env::args().skip(1);
    let format = args.next().ok_or("missing format")?;
    let path = args.next().ok_or("missing path")?;
    let target_partitions = args
        .next()
        .ok_or("missing partition count")?
        .parse::<usize>()?;
    let workload = args.next().unwrap_or_else(|| "count".to_string());
    if args.next().is_some() {
        return Err("unexpected extra argument".into());
    }

    let provider: Arc<dyn TableProvider> = match format.as_str() {
        "bigwig" => Arc::new(BigWigTableProvider::new(path, true)?),
        "bigbed" => Arc::new(BigBedTableProvider::new(
            path,
            true,
            BigBedSchemaMode::Rest,
        )?),
        _ => return Err(format!("unsupported format: {format}").into()),
    };
    let context = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    if workload == "aggregate" {
        context.register_table("bbi", provider)?;
        let dataframe = context.sql("SELECT count(*) AS rows FROM bbi").await?;
        let plan = dataframe.create_physical_plan().await?;
        println!("plan={plan:?}");
        let started = Instant::now();
        let batches = datafusion::physical_plan::collect(plan, context.task_ctx()).await?;
        println!(
            "aggregate_output_batches={} aggregate_output_rows={} elapsed_seconds={:.6}",
            batches.len(),
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            started.elapsed().as_secs_f64()
        );
        return Ok(());
    }
    let projection = match workload.as_str() {
        "count" => Some(Vec::new()),
        "decode" => None,
        _ => return Err(format!("unsupported workload: {workload}").into()),
    };
    let plan = provider
        .scan(&context.state(), projection.as_ref(), &[], None)
        .await?;
    println!("plan={plan:?}");

    let overall_started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for partition in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan.execute(partition, context.task_ctx())?;
        tasks.spawn(async move {
            let started = Instant::now();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch?.num_rows();
            }
            Ok::<_, datafusion::error::DataFusionError>((partition, rows, started.elapsed()))
        });
    }

    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result??);
    }
    results.sort_unstable_by_key(|(partition, _, _)| *partition);
    for (partition, rows, elapsed) in results {
        println!(
            "partition={partition} rows={rows} elapsed_seconds={:.6}",
            elapsed.as_secs_f64()
        );
    }
    println!(
        "overall_elapsed_seconds={:.6}",
        overall_started.elapsed().as_secs_f64()
    );
    Ok(())
}
