use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    CacheSourceType, EnsemblCacheOptions, MotifFeatureTableProvider, RegulatoryFeatureTableProvider,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

fn cache_source_type_from_env() -> datafusion::common::Result<CacheSourceType> {
    let value = std::env::var("VEP_CACHE_SOURCE_TYPE").map_err(|_| {
        datafusion::error::DataFusionError::Execution(
            "Set VEP_CACHE_SOURCE_TYPE to ensembl, merged, or refseq".to_string(),
        )
    })?;
    value.parse().map_err(|err| {
        datafusion::error::DataFusionError::Execution(format!(
            "invalid VEP_CACHE_SOURCE_TYPE {value:?}: {err}"
        ))
    })
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let cache_root = std::env::args().nth(1).unwrap_or_else(|| {
        "/Users/mwiewior/research/data/vep/homo_sapiens_merged/115_GRCh38".into()
    });
    let target = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "regulatory".to_string());
    let partitions: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let query = std::env::args().nth(4).unwrap_or_else(|| {
        if target == "motif" {
            "SELECT chrom, start, \"end\" AS end_pos, stable_id, binding_matrix \
             FROM t \
             WHERE chrom = '17' AND start >= 43000001 AND \"end\" <= 44000000"
                .to_string()
        } else {
            "SELECT chrom, start, \"end\" AS end_pos, stable_id, feature_type \
             FROM t \
             WHERE chrom = '17' AND start >= 43000001 AND \"end\" <= 44000000"
                .to_string()
        }
    });

    let cache_source_type = cache_source_type_from_env()?;

    println!("Cache: {cache_root}");
    println!("Source type: {cache_source_type}");
    println!("Target: {target}");
    println!("Target partitions: {partitions}");
    println!("Query: {query}");

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let mut options =
        EnsemblCacheOptions::new(&cache_root).with_cache_source_type(cache_source_type);
    options.target_partitions = Some(partitions);

    match target.as_str() {
        "regulatory" => {
            let provider = RegulatoryFeatureTableProvider::new(options)?;
            ctx.register_table("t", Arc::new(provider))?;
        }
        "motif" => {
            let provider = MotifFeatureTableProvider::new(options)?;
            ctx.register_table("t", Arc::new(provider))?;
        }
        other => {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "Unknown target '{other}', expected 'regulatory' or 'motif'"
            )));
        }
    }

    let start = Instant::now();
    let df = ctx.sql(&query).await?;
    let mut stream = df.execute_stream().await?;

    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        total_batches += 1;
    }
    let elapsed = start.elapsed();

    println!("Rows: {total_rows}");
    println!("Batches: {total_batches}");
    println!("Elapsed: {elapsed:.2?}");
    if elapsed.as_secs_f64() > 0.0 {
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
        println!("Throughput: {rows_per_sec:.0} rows/sec");
    }

    Ok(())
}
