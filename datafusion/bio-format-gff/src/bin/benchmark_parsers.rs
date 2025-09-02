use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType};
use std::time::Instant;

async fn benchmark_parser(
    file_path: &str,
    parser_type: GffParserType,
    parser_name: &str,
) -> Result<(usize, std::time::Duration), Box<dyn std::error::Error>> {
    println!("Starting benchmark for {} parser...", parser_name);

    let start = Instant::now();

    // Create reader (still needs to be async to set up sync readers internally)
    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        4, // thread count for BGZF
        ObjectStorageOptions::default(),
        parser_type,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    // Use the sync iterator directly
    for result in iterator {
        let _record = result?;
        record_count += 1;

        // Print progress every 100k records
        if record_count % 100_000 == 0 {
            println!("{} parser: processed {} records", parser_name, record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "{} parser completed: {} records in {:?}",
        parser_name, record_count, duration
    );

    Ok((record_count, duration))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("Benchmarking GFF parsers with file: {}", file_path);
    println!("=========================================");

    // Check if file exists
    if !std::path::Path::new(file_path).exists() {
        eprintln!("Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Benchmark Standard parser
    let (std_count, std_time) =
        benchmark_parser(file_path, GffParserType::Standard, "Standard").await?;

    println!();

    // Benchmark Fast parser
    let (fast_count, fast_time) = benchmark_parser(file_path, GffParserType::Fast, "Fast").await?;

    println!();

    // Benchmark SIMD parser
    let (simd_count, simd_time) = benchmark_parser(file_path, GffParserType::Simd, "SIMD").await?;

    println!();
    println!("BENCHMARK RESULTS");
    println!("=================");
    println!("File: {}", file_path);
    println!("Records processed: {}", std_count);
    println!();
    println!(
        "Standard parser: {:?} ({:.2} ns/record)",
        std_time,
        std_time.as_nanos() as f64 / std_count as f64
    );
    println!(
        "Fast parser:     {:?} ({:.2} ns/record)",
        fast_time,
        fast_time.as_nanos() as f64 / fast_count as f64
    );
    println!(
        "SIMD parser:     {:?} ({:.2} ns/record)",
        simd_time,
        simd_time.as_nanos() as f64 / simd_count as f64
    );
    println!();

    if std_time > fast_time {
        let speedup = std_time.as_nanos() as f64 / fast_time.as_nanos() as f64;
        println!("Fast parser is {:.2}x faster than Standard", speedup);
    }

    if std_time > simd_time {
        let speedup = std_time.as_nanos() as f64 / simd_time.as_nanos() as f64;
        println!("SIMD parser is {:.2}x faster than Standard", speedup);
    }

    if fast_time > simd_time {
        let speedup = fast_time.as_nanos() as f64 / simd_time.as_nanos() as f64;
        println!("SIMD parser is {:.2}x faster than Fast", speedup);
    } else if simd_time > fast_time {
        let speedup = simd_time.as_nanos() as f64 / fast_time.as_nanos() as f64;
        println!("Fast parser is {:.2}x faster than SIMD", speedup);
    }

    Ok(())
}
