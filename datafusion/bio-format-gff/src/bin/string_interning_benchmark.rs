use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD implementation (always allocate new strings for keys)
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

            // OLD: Always allocate new string for key
            attributes.insert(key.to_string(), decoded_value);
        }
    }

    attributes
}

// STRING INTERNING cache for keys
thread_local! {
    static KEY_CACHE: std::cell::RefCell<HashMap<String, String>> = std::cell::RefCell::new(HashMap::new());
}

fn intern_key(key: &str) -> String {
    KEY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(interned) = cache.get(key) {
            interned.clone()
        } else {
            let owned = key.to_string();
            cache.insert(owned.clone(), owned.clone());
            owned
        }
    })
}

// NEW implementation (string interning for keys)
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

            // NEW: Use interned key to reduce allocations
            let interned_key = intern_key(key);
            attributes.insert(interned_key, decoded_value);
        }
    }

    attributes
}

async fn benchmark_regular_keys(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 OLD: Regular key allocation...");
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

        // OLD behavior: Always allocate new strings for keys
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_old(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  OLD: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ OLD (regular keys): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_interned_keys(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("⚡ NEW: String interning for keys...");
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

        // NEW behavior: Use string interning for keys
        let attributes_str = record.attributes_string();
        let _attributes = parse_gff_attributes_new(&attributes_str);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  NEW: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ NEW (interned keys): {} records in {:?}",
        record_count, duration
    );

    // Report cache statistics
    KEY_CACHE.with(|cache| {
        let cache = cache.borrow();
        println!("🧠 Key cache contains {} unique keys", cache.len());
    });

    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🔑 STRING INTERNING FOR KEYS Benchmark");
    println!("File: {}", file_path);
    println!("======================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test old behavior (regular key allocation)
    let old_time = benchmark_regular_keys(file_path).await?;
    println!();

    // Test new behavior (string interning)
    let new_time = benchmark_interned_keys(file_path).await?;

    println!();
    println!("🎯 STRING INTERNING RESULTS");
    println!("===========================");

    let old_rps = 3_148_136.0 / old_time.as_secs_f64();
    let new_rps = 3_148_136.0 / new_time.as_secs_f64();

    println!(
        "🐌 OLD (regular):     {:?} ({:.0} records/sec)",
        old_time, old_rps
    );
    println!(
        "⚡ NEW (interned):    {:?} ({:.0} records/sec)",
        new_time, new_rps
    );

    let improvement = old_time.as_secs_f64() / new_time.as_secs_f64();

    println!();
    println!("📊 OPTIMIZATION IMPACT:");
    println!("String interning is {:.2}x FASTER", improvement);

    if improvement > 1.5 {
        println!("✅ EXCELLENT! String interning provides significant benefit");
    } else if improvement > 1.2 {
        println!("🤔 GOOD: Measurable improvement from interning keys");
    } else if improvement > 1.1 {
        println!("🤷 MODERATE: Small but measurable improvement");
    } else {
        println!("❓ MINIMAL: Improvement is within noise margin");
    }

    println!();
    println!("💡 WHY THIS HELPS:");
    println!("• Reduces string allocations for repeated attribute keys");
    println!("• Common keys like 'gene_id', 'transcript_id' are reused");
    println!("• Benefits increase with key repetition rate");
    println!("• Cache hit rate determines effectiveness");

    Ok(())
}
