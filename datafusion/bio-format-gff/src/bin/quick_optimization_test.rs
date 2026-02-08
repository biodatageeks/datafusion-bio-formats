use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD implementation
fn parse_old(attributes_str: &str) -> HashMap<String, String> {
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
            let decoded_value = value
                .replace("%3B", ";")
                .replace("%3D", "=")
                .replace("%26", "&")
                .replace("%2C", ",")
                .replace("%09", "\t");
            attributes.insert(key, decoded_value);
        }
    }
    attributes
}

// OPTIMIZED implementation
fn parse_optimized(attributes_str: &str) -> HashMap<String, String> {
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
            let decoded_value = if value.contains('%') {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("⚡ QUICK Optimization Test (first 100k records)");
    println!("===============================================");

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    let iterator = reader.into_sync_iterator();
    let mut test_data = Vec::new();

    // Collect first 100k attribute strings
    println!("📊 Collecting test data...");
    for (i, result) in iterator.enumerate() {
        if i >= 100_000 {
            break;
        }
        let record = result?;
        test_data.push(record.attributes_string());
    }

    println!("✅ Collected {} attribute strings", test_data.len());

    // Test OLD implementation
    println!("\n🐌 Testing OLD implementation...");
    let start = Instant::now();
    for attr_str in &test_data {
        let _result = parse_old(attr_str);
    }
    let old_time = start.elapsed();

    // Test OPTIMIZED implementation
    println!("⚡ Testing OPTIMIZED implementation...");
    let start = Instant::now();
    for attr_str in &test_data {
        let _result = parse_optimized(attr_str);
    }
    let optimized_time = start.elapsed();

    println!("\n🎯 QUICK TEST RESULTS");
    println!("======================");
    println!("Records tested: {}", test_data.len());
    println!("OLD:        {:?}", old_time);
    println!("OPTIMIZED:  {:?}", optimized_time);

    let improvement = old_time.as_secs_f64() / optimized_time.as_secs_f64();
    println!("Improvement: {:.1}x faster", improvement);

    if improvement > 2.0 {
        println!("✅ GOOD! Optimization working");
    } else if improvement > 1.3 {
        println!("✅ Some improvement");
    } else {
        println!("❌ Minimal improvement");
    }

    // Estimate full file performance
    let estimated_old_full = (old_time.as_secs_f64() / 100_000.0) * 3_148_136.0;
    let estimated_optimized_full = (optimized_time.as_secs_f64() / 100_000.0) * 3_148_136.0;

    println!("\n📈 ESTIMATED FULL FILE PERFORMANCE:");
    println!("OLD:        {:.1}s", estimated_old_full);
    println!("OPTIMIZED:  {:.1}s", estimated_optimized_full);

    if estimated_optimized_full < 20.0 {
        println!("🎉 EXCELLENT! Under 20 seconds");
    } else if estimated_optimized_full < 30.0 {
        println!("✅ GOOD! Under 30 seconds");
    } else {
        println!("⚠️  Still too slow - need more optimization");
    }

    Ok(())
}
