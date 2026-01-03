use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::table_utils::{Attribute, OptionalField};
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// Simulate the expensive parse_gff_attributes function
fn parse_gff_attributes(attributes_str: &str) -> HashMap<String, String> {
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

// Simulate load_attributes_from_map - the ARROW BUILDING part
fn load_attributes_from_map(
    attributes_map: &HashMap<String, String>,
    builder: &mut [OptionalField],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut vec_attributes: Vec<Attribute> = Vec::with_capacity(attributes_map.len());

    for (tag, value) in attributes_map.iter() {
        vec_attributes.push(Attribute {
            tag: tag.clone(),
            value: Some(value.clone()),
        });
    }

    builder[0].append_array_struct(vec_attributes)?;
    Ok(())
}

async fn benchmark_with_arrow_building(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🔥 WITH Arrow batch building (full pipeline)...");
    let start = Instant::now();

    let reader = GffLocalReader::new_with_parser(
        file_path.to_string(),
        4,
        ObjectStorageOptions::default(),
        GffParserType::Fast,
    )
    .await?;

    // Create the Arrow builder (like in physical_exec.rs)
    let batch_size = 8192;
    let mut builder = vec![OptionalField::new(
        &DataType::List(FieldRef::new(Field::new(
            "attribute",
            DataType::Struct(Fields::from(vec![
                Field::new("tag", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ))),
        batch_size,
    )?];

    let mut record_count = 0;
    let iterator = reader.into_sync_iterator();

    for result in iterator {
        let record = result?;

        // Full processing including ARROW BUILDING
        let attributes_str = record.attributes_string();
        let attributes_map = parse_gff_attributes(&attributes_str);
        load_attributes_from_map(&attributes_map, &mut builder)?; // ARROW BUILDING!

        record_count += 1;
        if record_count % 100_000 == 0 {
            println!("  Processed {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ WITH Arrow building: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_without_arrow_building(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 WITHOUT Arrow building (parsing only)...");
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

        // Parsing only, NO arrow building
        let attributes_str = record.attributes_string();
        let _attributes_map = parse_gff_attributes(&attributes_str);
        // No load_attributes_from_map call!

        record_count += 1;
        if record_count % 100_000 == 0 {
            println!("  Processed {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ WITHOUT Arrow building: {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("⚡ ARROW BATCH BUILDING Performance Analysis");
    println!("File: {}", file_path);
    println!("============================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test without Arrow building (parsing only)
    let no_arrow_time = benchmark_without_arrow_building(file_path).await?;

    println!();

    // Test with full Arrow building
    let with_arrow_time = benchmark_with_arrow_building(file_path).await?;

    println!();
    println!("🎯 ARROW BUILDING IMPACT ANALYSIS");
    println!("==================================");

    let no_arrow_records_per_sec = 3_148_136.0 / no_arrow_time.as_secs_f64();
    let with_arrow_records_per_sec = 3_148_136.0 / with_arrow_time.as_secs_f64();

    println!(
        "🚀 PARSING ONLY: {:?} ({:.0} records/sec)",
        no_arrow_time, no_arrow_records_per_sec
    );
    println!(
        "🐌 WITH ARROW:   {:?} ({:.0} records/sec)",
        with_arrow_time, with_arrow_records_per_sec
    );

    let arrow_slowdown = with_arrow_time.as_secs_f64() / no_arrow_time.as_secs_f64();
    println!();
    println!("📊 ARROW BUILDING IMPACT:");
    println!("Arrow building adds {:.1}x slowdown", arrow_slowdown);

    if arrow_slowdown > 5.0 {
        println!("🚨 CRITICAL: Arrow building is a major bottleneck!");
        println!("💡 OptionalField.append_array_struct() is expensive for 3.1M calls");
    } else if arrow_slowdown > 2.0 {
        println!("⚠️ SIGNIFICANT: Arrow building adds notable overhead");
    } else {
        println!("✅ Arrow building overhead is reasonable");
    }

    println!();
    println!("🔍 BOTTLENECK BREAKDOWN:");
    println!("• OptionalField::append_array_struct() called 3.1M times");
    println!("• Each call creates Arrow struct arrays");
    println!("• Memory allocations for Attribute structs");
    println!("• Arrow's internal validation and building overhead");

    Ok(())
}
