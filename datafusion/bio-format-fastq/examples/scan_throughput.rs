//! Read a FASTQ file end to end and report records and wall time.
//!
//! Decodes every column (the query sums the length of each), so it measures
//! decompression + parsing + Arrow building, the cost every scan pays.
//!
//! ```bash
//! cargo run --release --example scan_throughput -- reads.fastq.gz [PARTITIONS] [REPEATS]
//! ```

use std::env;
use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::common::Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = args
        .get(1)
        .expect("usage: scan_throughput FILE [PARTITIONS] [REPEATS]");
    let partitions: usize = args.get(2).map_or(1, |v| v.parse().unwrap());
    let repeats: usize = args.get(3).map_or(3, |v| v.parse().unwrap());

    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
    ctx.register_table("t", Arc::new(FastqTableProvider::new(path.clone(), None)?))?;
    let sql = "SELECT count(*), sum(length(name)), sum(length(sequence)), sum(length(quality_scores)) FROM t";
    for _ in 0..repeats {
        let start = Instant::now();
        let batches = ctx.sql(sql).await?.collect().await?;
        let elapsed = start.elapsed().as_secs_f64();
        let row = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
        let values: Vec<&str> = row
            .lines()
            .nth(3)
            .unwrap_or("")
            .split('|')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        println!("partitions={partitions} wall={elapsed:.3}s values={values:?}");
    }
    Ok(())
}
