use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// Simulate the expensive parse_gff_attributes function
fn parse_gff_attributes(attributes_str: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    if attributes_str.trim().is_empty() || attributes_str == "." {
        return attributes;
    }

    // Split by semicolon and parse key=value pairs
    for pair in attributes_str.split(';') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        if let Some(eq_pos) = pair.find('=') {
            let key = pair[..eq_pos].trim().to_string();
            let value = pair[eq_pos + 1..].trim();

            // URL decode if needed and handle quoted values
            let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                value[1..value.len() - 1].to_string()
            } else {
                // Simple URL decoding for common cases
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

async fn benchmark_full_attribute_processing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🔥 Full attribute processing (like physical_exec.rs)...");
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

        // FULL processing like we do in physical_exec.rs
        let attributes_str = record.attributes_string(); // String allocation
        let attributes_map = parse_gff_attributes(&attributes_str); // Full parsing with HashMap creation

        // Simulate load_attributes_from_map processing
        let mut _vec_attributes: Vec<(String, Option<String>)> =
            Vec::with_capacity(attributes_map.len());
        for (tag, value) in attributes_map.iter() {
            _vec_attributes.push((tag.clone(), Some(value.clone()))); // More string cloning
        }

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Processed {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ FULL processing: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_minimal_processing(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 Minimal processing (no attributes)...");
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
        // Just parse and access basic fields

        record_count += 1;
        if record_count % 500_000 == 0 {
            println!("  Processed {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ MINIMAL processing: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🔬 GFF FULL PIPELINE Performance Analysis");
    println!("File: {}", file_path);
    println!("=========================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test minimal processing
    let minimal_time = benchmark_minimal_processing(file_path).await?;

    println!();

    // Test full attribute processing
    let full_time = benchmark_full_attribute_processing(file_path).await?;

    println!();
    println!("🎯 FULL PIPELINE ANALYSIS");
    println!("==========================");

    let minimal_records_per_sec = 3_148_136.0 / minimal_time.as_secs_f64();
    let full_records_per_sec = 3_148_136.0 / full_time.as_secs_f64();

    println!(
        "🚀 MINIMAL:     {:?} ({:.0} records/sec)",
        minimal_time, minimal_records_per_sec
    );
    println!(
        "🐌 FULL ATTRS:  {:?} ({:.0} records/sec)",
        full_time, full_records_per_sec
    );

    let slowdown = full_time.as_secs_f64() / minimal_time.as_secs_f64();
    println!();
    println!("📊 ATTRIBUTE PROCESSING IMPACT:");
    println!("Full attribute processing is {:.1}x SLOWER", slowdown);

    if slowdown > 8.0 {
        println!("🚨 CRITICAL: This matches your 10x slowdown observation!");
        println!("💡 The bottleneck is definitely our attribute parsing implementation");
    } else if slowdown > 5.0 {
        println!("❌ MAJOR BOTTLENECK: Attribute processing is the primary performance killer!");
    } else if slowdown > 2.0 {
        println!("⚠️  MODERATE BOTTLENECK: Attribute processing adds significant overhead");
    } else {
        println!("✅ Attribute processing overhead is acceptable");
    }

    println!();
    println!("🔍 ROOT CAUSE ANALYSIS:");
    println!("• String allocations: attributes_string() for 3.1M records");
    println!("• HashMap creation: 3.1M HashMap::new() calls");
    println!("• String parsing: split(';'), find('='), replace() operations");
    println!("• More string cloning: tag.clone(), value.clone() in load_attributes_*");
    println!("• Vector allocations: Vec::new() for each record's attributes");

    Ok(())
}
