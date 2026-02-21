use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::time::Instant;

async fn benchmark_with_attributes(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🔥 Benchmarking WITH attribute processing...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let record = result?;

        // Simulate our current expensive attribute processing
        let attributes_str = record.attributes_string(); // String allocation
        let _attributes_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new(); // HashMap creation

        // Simulate the parsing we do in parse_gff_attributes
        for _pair in attributes_str.split(';') {
            // Expensive parsing operations
        }

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Processed {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("✅ WITH attributes: {record_count} records in {duration:?}");
    Ok(duration)
}

async fn benchmark_without_attributes(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 Benchmarking WITHOUT attribute processing...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let _record = result?;
        // NO attribute processing at all

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Processed {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("✅ WITHOUT attributes: {record_count} records in {duration:?}");
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("📊 GFF Attribute Processing Performance Analysis");
    println!("File: {file_path}");
    println!("============================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {file_path} not found");
        std::process::exit(1);
    }

    // Test without attribute processing
    let no_attr_time = benchmark_without_attributes(file_path).await?;

    println!();

    // Test with attribute processing
    let with_attr_time = benchmark_with_attributes(file_path).await?;

    println!();
    println!("📈 PERFORMANCE ANALYSIS");
    println!("========================");

    let no_attr_records_per_sec = 3_148_136.0 / no_attr_time.as_secs_f64();
    let with_attr_records_per_sec = 3_148_136.0 / with_attr_time.as_secs_f64();

    println!("🚀 WITHOUT attributes: {no_attr_time:?} ({no_attr_records_per_sec:.0} records/sec)");
    println!(
        "🐌 WITH attributes:    {with_attr_time:?} ({with_attr_records_per_sec:.0} records/sec)"
    );

    let slowdown = with_attr_time.as_secs_f64() / no_attr_time.as_secs_f64();
    println!();
    println!("🎯 VERDICT:");
    println!("Attribute processing is {slowdown:.1}x SLOWER");

    if slowdown > 5.0 {
        println!("❌ MAJOR BOTTLENECK: Attribute processing is the primary performance killer!");
        println!("💡 Solution: Optimize attribute parsing or make it optional");
    } else if slowdown > 2.0 {
        println!("⚠️  MODERATE BOTTLENECK: Attribute processing adds significant overhead");
    } else {
        println!("✅ Attribute processing overhead is acceptable");
    }

    println!();
    println!("📊 Expected performance with optimized attributes:");
    println!(
        "   Should be ~{:.1}s (target 2x slowdown max)",
        no_attr_time.as_secs_f64() * 2.0
    );

    Ok(())
}
