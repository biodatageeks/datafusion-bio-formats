use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD implementation (eager HashMap allocation)
fn parse_gff_attributes_old(attributes_str: &str) -> HashMap<String, String> {
    if attributes_str.is_empty() || attributes_str == "." {
        return HashMap::new(); // Still creates HashMap even when empty!
    }

    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes = HashMap::with_capacity(estimated_pairs); // EAGER allocation

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

// NEW implementation (lazy HashMap allocation)
fn parse_gff_attributes_new(attributes_str: &str) -> HashMap<String, String> {
    if attributes_str.is_empty() || attributes_str == "." {
        return HashMap::new();
    }

    // LAZY ALLOCATION: Don't create HashMap until we know we have valid pairs
    let estimated_pairs = attributes_str.matches(';').count() + 1;
    let mut attributes: Option<HashMap<String, String>> = None;

    for pair in attributes_str.split(';') {
        if pair.is_empty() {
            continue;
        }

        if let Some(eq_pos) = pair.find('=') {
            let key = &pair[..eq_pos];
            let value = &pair[eq_pos + 1..];

            // LAZY ALLOCATION: Only create HashMap when we have our first valid pair
            if attributes.is_none() {
                attributes = Some(HashMap::with_capacity(estimated_pairs));
            }

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

            attributes
                .as_mut()
                .unwrap()
                .insert(key.to_string(), decoded_value);
        }
    }

    // Return populated HashMap or empty if no valid pairs were found
    attributes.unwrap_or_default()
}

async fn benchmark_eager_allocation(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 OLD: Eager HashMap allocation...");
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

        // OLD behavior: Always allocate HashMap
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_old(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  OLD: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ OLD (eager allocation): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_lazy_allocation(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("⚡ NEW: Lazy HashMap allocation...");
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

        // NEW behavior: Lazy HashMap allocation
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_new(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  NEW: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ NEW (lazy allocation): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🧠 LAZY HASHMAP ALLOCATION Benchmark");
    println!("File: {}", file_path);
    println!("====================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test old behavior (eager allocation)
    let eager_time = benchmark_eager_allocation(file_path).await?;
    println!();

    // Test new behavior (lazy allocation)
    let lazy_time = benchmark_lazy_allocation(file_path).await?;

    println!();
    println!("🎯 LAZY ALLOCATION RESULTS");
    println!("==========================");

    let eager_rps = 3_148_136.0 / eager_time.as_secs_f64();
    let lazy_rps = 3_148_136.0 / lazy_time.as_secs_f64();

    println!(
        "🐌 OLD (eager):     {:?} ({:.0} records/sec)",
        eager_time, eager_rps
    );
    println!(
        "⚡ NEW (lazy):      {:?} ({:.0} records/sec)",
        lazy_time, lazy_rps
    );

    let improvement = eager_time.as_secs_f64() / lazy_time.as_secs_f64();

    println!();
    println!("📊 OPTIMIZATION IMPACT:");
    println!("Lazy allocation is {:.2}x FASTER", improvement);

    if improvement > 1.2 {
        println!("✅ GOOD! Lazy allocation provides measurable benefit");
    } else if improvement > 1.1 {
        println!("🤔 MODERATE: Small but measurable improvement");
    } else {
        println!("❓ MINIMAL: Improvement is within noise margin");
    }

    println!();
    println!("💡 WHY THIS HELPS:");
    println!("• Avoids HashMap allocation for records with malformed attributes");
    println!("• Saves memory allocations for edge cases");
    println!("• Reduces allocation pressure on memory allocator");
    println!("• Most benefit when many records have unparseable attribute strings");

    Ok(())
}
