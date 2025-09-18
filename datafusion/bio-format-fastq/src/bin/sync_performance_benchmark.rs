use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::{FastqParser, table_provider::FastqTableProvider};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test file - use the larger performance test file
    let file_path = "/tmp/ERR194146.fastq.bgz".to_string();

    println!("=== FASTQ Parser Performance Benchmark ===");
    println!("Testing file: {}", file_path);
    println!("Target partitions: 1 (single-threaded)");
    println!();

    // Run noodles benchmark
    println!("🧬 Testing Noodles parser...");
    let noodles_time = benchmark_parser(&file_path, FastqParser::Noodles).await?;

    // Run needletail benchmark
    println!("🪡 Testing Needletail parser...");
    let needletail_time = benchmark_parser(&file_path, FastqParser::Needletail).await?;

    // Compare results
    println!();
    println!("=== RESULTS ===");
    println!("Noodles time:    {:?}", noodles_time);
    println!("Needletail time: {:?}", needletail_time);

    if needletail_time < noodles_time {
        let improvement =
            (noodles_time.as_secs_f64() / needletail_time.as_secs_f64() - 1.0) * 100.0;
        println!("🎉 Needletail is {:.1}% faster than Noodles!", improvement);
    } else {
        let degradation =
            (needletail_time.as_secs_f64() / noodles_time.as_secs_f64() - 1.0) * 100.0;
        println!("⚠️  Needletail is {:.1}% slower than Noodles", degradation);
    }

    Ok(())
}

async fn benchmark_parser(
    file_path: &str,
    parser: FastqParser,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let storage_options = ObjectStorageOptions::default();
    let provider = FastqTableProvider::new_with_parser(
        file_path.to_string(),
        None,
        Some(storage_options),
        parser,
    )?;

    ctx.register_table("fastq", Arc::new(provider))?;

    let start = Instant::now();

    let df = ctx.sql("SELECT * FROM fastq").await?;
    let batches = df.collect().await?;

    let duration = start.elapsed();

    // Extract and print the count
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    println!("  Rows: {}, Time: {:?}", total_rows, duration);

    Ok(duration)
}
