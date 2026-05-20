use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    CacheSourceType, EnsemblCacheOptions, VariationTableProvider,
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
    let partitions: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);

    let cache_source_type = cache_source_type_from_env()?;

    println!("Cache: {cache_root}");
    println!("Source type: {cache_source_type}");
    println!("Target partitions: {partitions}");

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let options = EnsemblCacheOptions::new(&cache_root).with_cache_source_type(cache_source_type);
    let provider = VariationTableProvider::new(options)?;
    ctx.register_table("vep_variation", Arc::new(provider))?;

    let start = Instant::now();
    let df = ctx.sql("SELECT * FROM vep_variation").await?;
    let mut stream = df.execute_stream().await?;

    let mut total_rows: usize = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    println!("Rows: {total_rows}");
    println!("Elapsed: {elapsed:.2?}");
    println!("Throughput: {rows_per_sec:.0} rows/sec");

    Ok(())
}
