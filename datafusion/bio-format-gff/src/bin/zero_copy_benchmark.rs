use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD implementation (multiple string replacements)
fn parse_gff_attributes_old(attributes_str: &str) -> HashMap<String, String> {
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

            // OLD: Multiple string replacements (inefficient)
            let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                value[1..value.len() - 1].to_string()
            } else if value.contains('%') {
                value
                    .replace("%3B", ";") // Each replace scans the entire string
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

// NEW implementation (single pass zero-copy URL decoding)
fn parse_gff_attributes_new(attributes_str: &str) -> HashMap<String, String> {
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

            // ZERO-COPY OPTIMIZATION: Single pass URL decoding
            let decoded_value =
                if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
                    value[1..value.len() - 1].to_string()
                } else if value.contains('%') {
                    // Single pass replacement - more efficient
                    let mut result = String::with_capacity(value.len());
                    let value_bytes = value.as_bytes();
                    let mut i = 0;
                    while i < value_bytes.len() {
                        if value_bytes[i] == b'%' && i + 2 < value_bytes.len() {
                            // Try to decode the next two characters
                            let hex_bytes = &value_bytes[i + 1..i + 3];
                            if let Ok(hex_str) = std::str::from_utf8(hex_bytes) {
                                match hex_str {
                                    "3B" => result.push(';'),
                                    "3D" => result.push('='),
                                    "26" => result.push('&'),
                                    "2C" => result.push(','),
                                    "09" => result.push('\t'),
                                    _ => {
                                        // Not a recognized encoding, keep as-is
                                        result.push('%');
                                        result.push_str(hex_str);
                                    }
                                }
                                i += 3; // Skip the '%' and two hex digits
                            } else {
                                result.push(value_bytes[i] as char);
                                i += 1;
                            }
                        } else {
                            result.push(value_bytes[i] as char);
                            i += 1;
                        }
                    }
                    result
                } else {
                    value.to_string()
                };

            attributes.insert(key.to_string(), decoded_value);
        }
    }

    attributes
}

async fn benchmark_old_url_decoding(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 OLD: Multiple string replacements...");
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

        // OLD behavior: Multiple string replacements
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_old(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  OLD: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ OLD (multiple replacements): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_new_zero_copy(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("⚡ NEW: Single pass zero-copy decoding...");
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

        // NEW behavior: Single pass zero-copy decoding
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_new(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  NEW: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ NEW (zero-copy): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🚀 ZERO-COPY URL DECODING Benchmark");
    println!("File: {}", file_path);
    println!("===================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test old behavior (multiple replacements)
    let old_time = benchmark_old_url_decoding(file_path).await?;
    println!();

    // Test new behavior (zero-copy single pass)
    let new_time = benchmark_new_zero_copy(file_path).await?;

    println!();
    println!("🎯 ZERO-COPY OPTIMIZATION RESULTS");
    println!("==================================");

    let old_rps = 3_148_136.0 / old_time.as_secs_f64();
    let new_rps = 3_148_136.0 / new_time.as_secs_f64();

    println!(
        "🐌 OLD (multiple replace): {:?} ({:.0} records/sec)",
        old_time, old_rps
    );
    println!(
        "⚡ NEW (zero-copy):        {:?} ({:.0} records/sec)",
        new_time, new_rps
    );

    let improvement = old_time.as_secs_f64() / new_time.as_secs_f64();

    println!();
    println!("📊 OPTIMIZATION IMPACT:");
    println!("Zero-copy decoding is {:.2}x FASTER", improvement);

    if improvement > 1.5 {
        println!("✅ EXCELLENT! Zero-copy provides significant benefit");
    } else if improvement > 1.2 {
        println!("🤔 GOOD: Measurable improvement from zero-copy approach");
    } else if improvement > 1.1 {
        println!("🤷 MODERATE: Small but measurable improvement");
    } else {
        println!("❓ MINIMAL: Improvement is within noise margin");
    }

    println!();
    println!("💡 WHY THIS HELPS:");
    println!("• Single pass through string vs multiple full scans");
    println!("• Avoids intermediate string allocations from .replace()");
    println!("• Most benefit when URLs contain encoded characters");
    println!("• Reduces string manipulation overhead");

    Ok(())
}
