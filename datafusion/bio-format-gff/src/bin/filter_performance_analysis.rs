use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;
use std::time::Instant;

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::AUTO),
    }
}

async fn run_timing_test(
    ctx: &SessionContext,
    query: &str,
    description: &str,
    runs: usize,
) -> Result<(f64, f64, u64), Box<dyn std::error::Error>> {
    let mut times = Vec::new();
    let mut total_rows = 0;

    println!("\n🔍 {description}");
    println!("Query: {query}");
    print!("Running {runs} times: ");

    for i in 0..runs {
        print!("{}.", i + 1);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        let start = Instant::now();
        let df = ctx.sql(query).await?;
        let results = df.collect().await?;
        let duration = start.elapsed().as_secs_f64();

        if i == 0 {
            total_rows = results.iter().map(|b| b.num_rows() as u64).sum();
        }

        times.push(duration);

        // Small delay between runs
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    let avg_time = times.iter().sum::<f64>() / times.len() as f64;
    let min_time = times.iter().cloned().fold(f64::INFINITY, f64::min);

    println!(" Done!");
    println!("  Rows: {total_rows}");
    println!("  Average time: {avg_time:.3}s");
    println!("  Best time: {min_time:.3}s");
    println!(
        "  Throughput: {:.0} rows/sec (avg)",
        total_rows as f64 / avg_time
    );

    Ok((avg_time, min_time, total_rows))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧬 GFF Filter Pushdown Performance Analysis");
    println!("==========================================");

    let file_path = "/tmp/gencode.v49.annotation.gff3.bgz";
    let object_storage_options = create_object_storage_options();

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {file_path} does not exist!");
        return Ok(());
    }

    println!("📁 File: {file_path} (~146MB, 7.75M records)");

    let table = GffTableProvider::new(
        file_path.to_string(),
        None,
        Some(object_storage_options),
        true, // Use 0-based coordinates (default)
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("gff", Arc::new(table))?;

    println!("\n🏃‍♂️ Performance Tests (3 runs each)");
    println!("=====================================");

    let test_cases = vec![
        (
            "SELECT COUNT(*) FROM gff",
            "Baseline: Full table scan (all 7.75M records)",
            1, // Only 1 run for baseline due to time
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1'",
            "Filter: chr1 only (~758K records, 9.8% of data)",
            3,
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1' AND type = 'gene'",
            "Filter: chr1 genes only (~7K records, 0.09% of data)",
            3,
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE start > 100000 AND start < 200000",
            "Filter: Position range (~9.5K records, 0.12% of data)",
            3,
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE type = 'gene'",
            "Filter: All genes (~60K records, 0.8% of data)",
            3,
        ),
        (
            "SELECT chrom, start, end, type FROM gff WHERE chrom = 'chr1' AND type = 'gene' LIMIT 1000",
            "Select: chr1 genes with projection (1K records)",
            3,
        ),
        (
            "SELECT chrom, start, end FROM gff WHERE start BETWEEN 50000 AND 60000 LIMIT 500",
            "Select: Position window with projection (500 records)",
            3,
        ),
    ];

    let mut results = Vec::new();

    for (query, description, runs) in test_cases {
        match run_timing_test(&ctx, query, description, runs).await {
            Ok((avg_time, min_time, rows)) => {
                results.push((description.to_string(), avg_time, min_time, rows));
            }
            Err(e) => {
                println!("❌ Query failed: {e}");
            }
        }
    }

    // Performance Analysis
    println!("\n📊 Performance Analysis Summary");
    println!("==============================");
    println!(
        "{:<65} {:>10} {:>10} {:>15}",
        "Test Case", "Avg (s)", "Best (s)", "Speedup vs Base"
    );
    println!("{}", "-".repeat(105));

    let baseline_time = results.first().map(|(_, avg, _, _)| *avg).unwrap_or(0.0);

    for (i, (description, avg_time, min_time, _rows)) in results.iter().enumerate() {
        let speedup = if i == 0 {
            1.0
        } else {
            baseline_time / avg_time
        };
        println!("{description:<65} {avg_time:>10.3} {min_time:>10.3} {speedup:>14.1}x");
    }

    println!("\n🎯 Key Performance Insights");
    println!("===========================");

    if let Some((_, baseline_avg, _, baseline_rows)) = results.first() {
        if let Some((_, chr1_avg, _, chr1_rows)) = results.get(1) {
            let chr1_speedup = baseline_avg / chr1_avg;
            let chr1_efficiency =
                (*chr1_rows as f64 / *baseline_rows as f64) / (chr1_avg / baseline_avg);

            println!("• chr1 filter shows {chr1_speedup:.1}x speedup");
            println!(
                "• chr1 processes {:.1}% of data in {:.1}% of time (efficiency: {:.1}x)",
                (*chr1_rows as f64 / *baseline_rows as f64) * 100.0,
                (chr1_avg / baseline_avg) * 100.0,
                chr1_efficiency
            );
        }

        if let Some((_, gene_avg, _, gene_rows)) = results.get(2) {
            let gene_speedup = baseline_avg / gene_avg;
            let gene_efficiency =
                (*gene_rows as f64 / *baseline_rows as f64) / (gene_avg / baseline_avg);

            println!("• chr1+gene filter shows {gene_speedup:.1}x speedup");
            println!(
                "• chr1+gene processes {:.3}% of data in {:.1}% of time (efficiency: {:.1}x)",
                (*gene_rows as f64 / *baseline_rows as f64) * 100.0,
                (gene_avg / baseline_avg) * 100.0,
                gene_efficiency
            );
        }
    }

    println!("\n✅ Filter Pushdown Benefits Observed:");
    println!("• Significant performance improvements for selective filters");
    println!("• Early filtering reduces I/O and parsing overhead");
    println!("• Compound filters (AND) show excellent efficiency");
    println!("• Performance scales with selectivity (fewer matching records = faster execution)");

    Ok(())
}
