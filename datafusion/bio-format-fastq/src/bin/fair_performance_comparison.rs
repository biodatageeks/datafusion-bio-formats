use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::{FastqParser, table_provider::FastqTableProvider};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/ERR194146.fastq.bgz".to_string();

    println!("=== Fair FASTQ Parser Performance Comparison ===");
    println!("Testing file: {}", file_path);
    println!();

    // Test 1: Single-threaded comparison (fair)
    println!("📊 SINGLE-THREADED COMPARISON (fair):");
    println!();

    println!("🧬 Testing Noodles parser (1 thread)...");
    let noodles_single = benchmark_parser(&file_path, FastqParser::Noodles, 1).await?;

    println!("🪡 Testing Needletail parser (1 thread)...");
    let needletail_single = benchmark_parser(&file_path, FastqParser::Needletail, 1).await?;

    // Test 2: Multi-threaded comparison
    println!();
    println!("🚀 MULTI-THREADED COMPARISON:");
    println!();

    let thread_count = std::thread::available_parallelism().unwrap().get();
    println!("Using {} threads", thread_count);

    println!("🧬 Testing Noodles parser ({} threads)...", thread_count);
    let noodles_multi = benchmark_parser(&file_path, FastqParser::Noodles, thread_count).await?;

    println!("🪡 Testing Needletail parser ({} threads)...", thread_count);
    let needletail_multi =
        benchmark_parser(&file_path, FastqParser::Needletail, thread_count).await?;

    // Results
    println!();
    println!("=== RESULTS ===");
    println!();
    println!("Single-threaded:");
    println!("  Noodles:    {:?}", noodles_single);
    println!("  Needletail: {:?}", needletail_single);

    if needletail_single < noodles_single {
        let improvement =
            (noodles_single.as_secs_f64() / needletail_single.as_secs_f64() - 1.0) * 100.0;
        println!("  🎉 Needletail is {:.1}% faster!", improvement);
    } else {
        let degradation =
            (needletail_single.as_secs_f64() / noodles_single.as_secs_f64() - 1.0) * 100.0;
        println!("  ⚠️  Needletail is {:.1}% slower", degradation);
    }

    println!();
    println!("Multi-threaded:");
    println!("  Noodles:    {:?}", noodles_multi);
    println!("  Needletail: {:?}", needletail_multi);

    if needletail_multi < noodles_multi {
        let improvement =
            (noodles_multi.as_secs_f64() / needletail_multi.as_secs_f64() - 1.0) * 100.0;
        println!("  🎉 Needletail is {:.1}% faster!", improvement);
    } else {
        let degradation =
            (needletail_multi.as_secs_f64() / noodles_multi.as_secs_f64() - 1.0) * 100.0;
        println!("  ⚠️  Needletail is {:.1}% slower", degradation);
    }

    println!();
    println!("Threading efficiency:");
    let noodles_speedup = noodles_single.as_secs_f64() / noodles_multi.as_secs_f64();
    let needletail_speedup = needletail_single.as_secs_f64() / needletail_multi.as_secs_f64();
    println!("  Noodles speedup:    {:.1}x", noodles_speedup);
    println!("  Needletail speedup: {:.1}x", needletail_speedup);

    Ok(())
}

async fn benchmark_parser(
    file_path: &str,
    parser: FastqParser,
    thread_count: usize,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let config = SessionConfig::new().with_target_partitions(thread_count);
    let ctx = SessionContext::new_with_config(config);

    let storage_options = ObjectStorageOptions::default();
    let provider = FastqTableProvider::new_with_parser(
        file_path.to_string(),
        Some(thread_count),
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
