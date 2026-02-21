use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD slow implementation
fn parse_gff_attributes_old(attributes_str: &str) -> HashMap<String, String> {
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

// NEW optimized implementation
fn parse_gff_attributes_optimized(attributes_str: &str) -> HashMap<String, String> {
    if attributes_str.is_empty() || attributes_str == "." {
        return HashMap::new();
    }

    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes = HashMap::with_capacity(estimated_pairs);

    for pair in attributes_str.split(';') {
        if pair.is_empty() {
            continue;
        }

        if let Some(eq_pos) = pair.find('=') {
            let key = &pair[..eq_pos];
            let value = &pair[eq_pos + 1..];

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

    attributes
}

async fn benchmark_old_parsing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 OLD parsing (with trim and always replace)...");
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

        let attributes_str = record.attributes_string();
        let _attributes_map = parse_gff_attributes_old(&attributes_str);

        record_count += 1;
        if record_count % 200_000 == 0 {
            println!("  OLD: {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("✅ OLD parsing: {record_count} records in {duration:?}");
    Ok(duration)
}

async fn benchmark_optimized_parsing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("⚡ OPTIMIZED parsing...");
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

        let attributes_str = record.attributes_string();
        let _attributes_map = parse_gff_attributes_optimized(&attributes_str);

        record_count += 1;
        if record_count % 200_000 == 0 {
            println!("  OPTIMIZED: {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("✅ OPTIMIZED parsing: {record_count} records in {duration:?}");
    Ok(duration)
}

async fn benchmark_baseline(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 BASELINE (no parsing)...");
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
        let _attributes_str = record.attributes_string(); // Just get the string

        record_count += 1;
        if record_count % 200_000 == 0 {
            println!("  BASELINE: {record_count} records");
        }
    }

    let duration = start.elapsed();
    println!("✅ BASELINE: {record_count} records in {duration:?}");
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("⚡ OPTIMIZED ATTRIBUTE PARSING Benchmark");
    println!("File: {file_path}");
    println!("========================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {file_path} not found");
        std::process::exit(1);
    }

    // Test baseline
    let baseline_time = benchmark_baseline(file_path).await?;
    println!();

    // Test old implementation
    let old_time = benchmark_old_parsing(file_path).await?;
    println!();

    // Test optimized implementation
    let optimized_time = benchmark_optimized_parsing(file_path).await?;

    println!();
    println!("🎯 OPTIMIZATION RESULTS");
    println!("========================");

    let baseline_rps = 3_148_136.0 / baseline_time.as_secs_f64();
    let old_rps = 3_148_136.0 / old_time.as_secs_f64();
    let optimized_rps = 3_148_136.0 / optimized_time.as_secs_f64();

    println!("🚀 BASELINE:   {baseline_time:?} ({baseline_rps:.0} records/sec)");
    println!("🐌 OLD:        {old_time:?} ({old_rps:.0} records/sec)");
    println!("⚡ OPTIMIZED:  {optimized_time:?} ({optimized_rps:.0} records/sec)");

    let old_slowdown = old_time.as_secs_f64() / baseline_time.as_secs_f64();
    let optimized_slowdown = optimized_time.as_secs_f64() / baseline_time.as_secs_f64();
    let improvement = old_time.as_secs_f64() / optimized_time.as_secs_f64();

    println!();
    println!("📊 PERFORMANCE ANALYSIS:");
    println!("• Old parsing slowdown:       {old_slowdown:.1}x");
    println!("• Optimized parsing slowdown: {optimized_slowdown:.1}x");
    println!("• Improvement factor:         {improvement:.1}x faster");

    if improvement > 3.0 {
        println!("🎉 EXCELLENT! Major optimization achieved!");
    } else if improvement > 2.0 {
        println!("✅ GOOD! Significant improvement");
    } else if improvement > 1.5 {
        println!("✅ Moderate improvement");
    } else {
        println!("❌ Minimal improvement - need different approach");
    }

    let target_slowdown = 2.0; // Target max 2x slowdown
    if optimized_slowdown <= target_slowdown {
        println!("🎯 SUCCESS! Optimized version meets performance target (<= 2x slowdown)");
    } else {
        println!(
            "⚠️  Still {:.1}x slower than target. Need more optimization.",
            optimized_slowdown / target_slowdown
        );
    }

    Ok(())
}
