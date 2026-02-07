use datafusion::execution::context::SessionContext;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;
use std::time::Instant;

const BAM_PATH: &str = "/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.chr1.bam";
const NUM_RUNS: usize = 3;

async fn count_rows(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(usize, std::time::Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((total_rows, elapsed))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = BamTableProvider::new(BAM_PATH.to_string(), Some(4), None, true, None).await?;

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(table))?;

    let queries = vec![
        ("SELECT chrom, start FROM bam", "chrom+start (minimal)"),
        ("SELECT * FROM bam", "SELECT * (all fields)"),
    ];

    for (sql, label) in &queries {
        println!("\n--- {} ---", label);
        println!("SQL: {}", sql);

        let mut timings = Vec::with_capacity(NUM_RUNS);
        let mut rows = 0;

        for run in 1..=NUM_RUNS {
            let (r, elapsed) = count_rows(&ctx, sql).await?;
            rows = r;
            let secs = elapsed.as_secs_f64();
            let rows_per_sec = rows as f64 / secs;
            println!(
                "  Run {}: {:.3}s  ({} rows, {:.0} rows/sec)",
                run, secs, rows, rows_per_sec
            );
            timings.push(elapsed);
        }

        timings.sort();
        let median = timings[NUM_RUNS / 2];
        let median_secs = median.as_secs_f64();
        println!(
            "  Median: {:.3}s  ({} rows, {:.0} rows/sec)",
            median_secs,
            rows,
            rows as f64 / median_secs
        );
    }

    Ok(())
}
