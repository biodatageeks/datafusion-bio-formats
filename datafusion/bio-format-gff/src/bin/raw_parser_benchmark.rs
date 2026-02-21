use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType};
use std::time::Instant;

async fn benchmark_raw_parser(
    file_path: &str,
    parser_type: GffParserType,
    parser_name: &str,
) -> Result<(usize, std::time::Duration), Box<dyn std::error::Error>> {
    println!("Starting raw benchmark for {parser_name} parser...");

    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        ObjectStorageOptions::default(),
        parser_type,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    // Just count records without any processing
    for result in iterator {
        let _record = result?; // Only parse the record, don't process fields
        record_count += 1;

        if record_count % 100_000 == 0 {
            println!("{parser_name} parser: processed {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("{parser_name} parser completed: {record_count} records in {duration:?}");

    Ok((record_count, duration))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🚀 Raw Parser Performance Benchmark (no field processing)");
    println!("File: {file_path}");
    println!("=========================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("Error: File {file_path} not found");
        std::process::exit(1);
    }

    // Benchmark Standard parser
    let (std_count, std_time) =
        benchmark_raw_parser(file_path, GffParserType::Standard, "Standard").await?;

    println!();

    // Benchmark Fast parser
    let (fast_count, fast_time) =
        benchmark_raw_parser(file_path, GffParserType::Fast, "Fast").await?;

    println!();

    // Benchmark SIMD parser
    let (simd_count, simd_time) =
        benchmark_raw_parser(file_path, GffParserType::Simd, "SIMD").await?;

    println!();
    println!("RAW PARSER BENCHMARK RESULTS");
    println!("=============================");
    println!("File: {file_path}");
    println!("Records processed: {std_count}");
    println!();

    let std_records_per_sec = std_count as f64 / std_time.as_secs_f64();
    let fast_records_per_sec = fast_count as f64 / fast_time.as_secs_f64();
    let simd_records_per_sec = simd_count as f64 / simd_time.as_secs_f64();

    println!("Standard parser: {std_time:?} ({std_records_per_sec:.0} records/sec)");
    println!("Fast parser:     {fast_time:?} ({fast_records_per_sec:.0} records/sec)");
    println!("SIMD parser:     {simd_time:?} ({simd_records_per_sec:.0} records/sec)");
    println!();

    if std_time > fast_time {
        let speedup = std_time.as_secs_f64() / fast_time.as_secs_f64();
        println!("🔥 Fast parser is {speedup:.2}x faster than Standard");
    }

    if std_time > simd_time {
        let speedup = std_time.as_secs_f64() / simd_time.as_secs_f64();
        println!("⚡ SIMD parser is {speedup:.2}x faster than Standard");
    }

    println!("\n🎯 Expected performance:");
    println!(
        "Fast parser should be ~3x faster: {:.0} records/sec expected",
        std_records_per_sec * 3.0
    );
    println!(
        "SIMD parser should be ~3.3x faster: {:.0} records/sec expected",
        std_records_per_sec * 3.3
    );

    Ok(())
}
