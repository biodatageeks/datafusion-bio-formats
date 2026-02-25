use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{EnsemblCacheOptions, TranscriptTableProvider};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let cache_root = std::env::args().nth(1).unwrap_or_else(|| {
        "/Users/mwiewior/research/data/vep/homo_sapiens_merged/115_GRCh38".into()
    });
    let partitions: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let query = std::env::args().nth(3).unwrap_or_else(|| {
        "SELECT chrom, start, end, stable_id, source_file \
         FROM tx \
         WHERE chrom = '17' AND start >= 43000001 AND end <= 44000000"
            .to_string()
    });

    println!("Cache: {cache_root}");
    println!("Target partitions: {partitions}");
    println!("Query: {query}");

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let mut options = EnsemblCacheOptions::new(&cache_root);
    options.target_partitions = Some(partitions);
    let provider = TranscriptTableProvider::new(options)?;
    ctx.register_table("tx", Arc::new(provider))?;

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
