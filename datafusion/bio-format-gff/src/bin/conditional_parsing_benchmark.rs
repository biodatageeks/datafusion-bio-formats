use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::storage::{GffLocalReader, GffParserType, GffRecordTrait};
use std::collections::HashMap;
use std::time::Instant;

// OLD implementation (always parse attributes)
fn simulate_old_behavior(record: &impl GffRecordTrait) -> HashMap<String, String> {
    let attributes_str = record.attributes_string();
    parse_gff_attributes(&attributes_str)
}

// NEW implementation (conditional parsing)
fn simulate_new_behavior(
    record: &impl GffRecordTrait,
    attributes_needed: bool,
) -> Option<HashMap<String, String>> {
    if attributes_needed {
        let attributes_str = record.attributes_string();
        Some(parse_gff_attributes(&attributes_str))
    } else {
        None // Skip parsing entirely!
    }
}

// Optimized parsing function (from our previous optimization)
fn parse_gff_attributes(attributes_str: &str) -> HashMap<String, String> {
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

async fn benchmark_always_parse(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🐌 OLD: Always parse attributes...");
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

        // OLD behavior: Always parse attributes
        let _attributes = simulate_old_behavior(&record);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  OLD: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ OLD (always parse): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_conditional_parse(
    file_path: &str,
    parse_percent: f32,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!(
        "⚡ NEW: Conditional parsing ({}% of records)...",
        (parse_percent * 100.0) as i32
    );
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

        // NEW behavior: Conditional parsing
        let attributes_needed = (record_count as f32 / 3_148_136.0) < parse_percent; // Parse only X% of records
        let _attributes = simulate_new_behavior(&record, attributes_needed);

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  NEW: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ NEW (conditional): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

async fn benchmark_never_parse(
    file_path: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    println!("🚀 BASELINE: Never parse attributes...");
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

        // BASELINE: Never parse attributes
        let _attributes_str = record.attributes_string(); // Just get string

        record_count += 1;
        if record_count % 300_000 == 0 {
            println!("  BASELINE: {} records", record_count);
        }
    }

    let duration = start.elapsed();
    println!(
        "✅ BASELINE (never parse): {} records in {:?}",
        record_count, duration
    );
    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🎯 CONDITIONAL ATTRIBUTE PARSING Benchmark");
    println!("File: {}", file_path);
    println!("===========================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        std::process::exit(1);
    }

    // Test baseline (no parsing)
    let baseline_time = benchmark_never_parse(file_path).await?;
    println!();

    // Test old behavior (always parse)
    let always_time = benchmark_always_parse(file_path).await?;
    println!();

    // Test new behavior - conditional parsing (0% = skip all)
    let conditional_0_time = benchmark_conditional_parse(file_path, 0.0).await?;
    println!();

    // Test new behavior - conditional parsing (50% = parse half)
    let conditional_50_time = benchmark_conditional_parse(file_path, 0.5).await?;

    println!();
    println!("🎯 CONDITIONAL PARSING RESULTS");
    println!("===============================");

    let baseline_rps = 3_148_136.0 / baseline_time.as_secs_f64();
    let always_rps = 3_148_136.0 / always_time.as_secs_f64();
    let conditional_0_rps = 3_148_136.0 / conditional_0_time.as_secs_f64();
    let conditional_50_rps = 3_148_136.0 / conditional_50_time.as_secs_f64();

    println!(
        "🚀 BASELINE (no parsing):    {:?} ({:.0} records/sec)",
        baseline_time, baseline_rps
    );
    println!(
        "🐌 OLD (always parse):       {:?} ({:.0} records/sec)",
        always_time, always_rps
    );
    println!(
        "⚡ NEW (conditional 0%):     {:?} ({:.0} records/sec)",
        conditional_0_time, conditional_0_rps
    );
    println!(
        "⚡ NEW (conditional 50%):    {:?} ({:.0} records/sec)",
        conditional_50_time, conditional_50_rps
    );

    let always_slowdown = always_time.as_secs_f64() / baseline_time.as_secs_f64();
    let conditional_0_slowdown = conditional_0_time.as_secs_f64() / baseline_time.as_secs_f64();
    let conditional_50_slowdown = conditional_50_time.as_secs_f64() / baseline_time.as_secs_f64();

    let optimization_0 = always_time.as_secs_f64() / conditional_0_time.as_secs_f64();
    let optimization_50 = always_time.as_secs_f64() / conditional_50_time.as_secs_f64();

    println!();
    println!("📊 PERFORMANCE ANALYSIS:");
    println!(
        "• Always parsing slowdown:     {:.1}x vs baseline",
        always_slowdown
    );
    println!(
        "• Conditional 0% slowdown:     {:.1}x vs baseline",
        conditional_0_slowdown
    );
    println!(
        "• Conditional 50% slowdown:    {:.1}x vs baseline",
        conditional_50_slowdown
    );
    println!();
    println!("🎉 OPTIMIZATION IMPACT:");
    println!(
        "• Skip all attributes:   {:.1}x FASTER than always parsing",
        optimization_0
    );
    println!(
        "• Parse 50% attributes:  {:.1}x FASTER than always parsing",
        optimization_50
    );

    if optimization_0 > 2.0 {
        println!("✅ EXCELLENT! Major speedup when attributes not needed");
    } else {
        println!("⚠️  Less improvement than expected");
    }

    println!();
    println!("💡 CONCLUSION:");
    println!("This optimization will help most when:");
    println!("• Queries don't need attribute columns");
    println!("• Only core GFF fields (chrom, start, end, etc.) are selected");
    println!("• COUNT(*) queries or simple filtering");

    Ok(())
}
