use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// Our current SLOW custom implementation
fn parse_gff_attributes_custom(attributes_str: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    if attributes_str.trim().is_empty() || attributes_str == "." {
        return attributes;
    }

    for pair in attributes_str.split(';') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        if let Some(eq_pos) = pair.find('=') {
            let key = pair[..eq_pos].trim().to_string();
            let value = pair[eq_pos + 1..].trim();
            let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                value[1..value.len() - 1].to_string()
            } else {
                value
                    .replace("%3B", ";")
                    .replace("%3D", "=")
                    .replace("%26", "&")
                    .replace("%2C", ",")
                    .replace("%09", "\t")
            };
            attributes.insert(key, decoded_value);
        }
    }
    attributes
}

// NEW optimized implementation - try to avoid string parsing if possible
fn parse_gff_attributes_optimized(attributes_str: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    if attributes_str.trim().is_empty() || attributes_str == "." {
        return attributes;
    }

    // Try to optimize by avoiding multiple string operations
    attributes_str.split(';').for_each(|pair| {
        let pair = pair.trim();
        if !pair.is_empty() {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];

                // Only do expensive operations if needed
                let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                    value[1..value.len() - 1].to_string()
                } else if value.contains('%') {
                    value
                        .replace("%3B", ";")
                        .replace("%3D", "=")
                        .replace("%26", "&")
                        .replace("%2C", ",")
                        .replace("%09", "\t")
                } else {
                    value.to_string()
                };

                attributes.insert(key.to_string(), decoded_value);
            }
        }
    });

    attributes
}

async fn benchmark_custom_parsing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 Testing CURRENT custom parsing...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        4,
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let record = result?;

        let attributes_str = record.attributes_string();
        let _attributes_map = parse_gff_attributes_custom(&attributes_str);

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Custom: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ CUSTOM parsing: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_optimized_parsing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("⚡ Testing OPTIMIZED parsing...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        4,
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let record = result?;

        let attributes_str = record.attributes_string();
        let _attributes_map = parse_gff_attributes_optimized(&attributes_str);

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Optimized: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ OPTIMIZED parsing: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_minimal_parsing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 Testing MINIMAL (no parsing)...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        4,
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let record = result?;

        let _attributes_str = record.attributes_string();
        // No parsing at all

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Minimal: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!("✅ MINIMAL: {} records in {:?}", record_count, duration);
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🔬 ATTRIBUTE PARSING OPTIMIZATION Benchmark");
    println!("File: {}", file_path);
    println!("===========================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test minimal (baseline)
    let minimal_time = benchmark_minimal_parsing(file_path).await?;
    println!();

    // Test current custom implementation
    let custom_time = benchmark_custom_parsing(file_path).await?;
    println!();

    // Test optimized implementation
    let optimized_time = benchmark_optimized_parsing(file_path).await?;

    println!();
    println!("📊 PARSING OPTIMIZATION RESULTS");
    println!("================================");

    let minimal_rps = 3_148_136.0 / minimal_time.as_secs_f64();
    let custom_rps = 3_148_136.0 / custom_time.as_secs_f64();
    let optimized_rps = 3_148_136.0 / optimized_time.as_secs_f64();

    println!(
        "🚀 MINIMAL:    {:?} ({:.0} records/sec)",
        minimal_time, minimal_rps
    );
    println!(
        "🐌 CUSTOM:     {:?} ({:.0} records/sec)",
        custom_time, custom_rps
    );
    println!(
        "⚡ OPTIMIZED:  {:?} ({:.0} records/sec)",
        optimized_time, optimized_rps
    );

    let custom_slowdown = custom_time.as_secs_f64() / minimal_time.as_secs_f64();
    let optimized_slowdown = optimized_time.as_secs_f64() / minimal_time.as_secs_f64();
    let improvement = custom_time.as_secs_f64() / optimized_time.as_secs_f64();

    println!();
    println!("🎯 ANALYSIS:");
    println!("• Custom parsing slowdown:    {:.1}x", custom_slowdown);
    println!("• Optimized parsing slowdown: {:.1}x", optimized_slowdown);
    println!("• Improvement factor:         {:.1}x faster", improvement);

    if improvement > 2.0 {
        println!("✅ SIGNIFICANT IMPROVEMENT! Optimization is working.");
    } else if improvement > 1.3 {
        println!("✅ Moderate improvement, but more optimization needed.");
    } else {
        println!("❌ Minimal improvement, need to investigate noodles-gff native API.");
    }

    println!();
    println!("🔍 NEXT STEPS:");
    if improvement < 2.0 {
        println!("• Investigate using noodles-gff RecordBuf.attributes() directly");
        println!("• Check if fast_records() already have parsed attributes");
        println!("• Consider lazy/optional attribute parsing");
    }

    Ok(())
}
