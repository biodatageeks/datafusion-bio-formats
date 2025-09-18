use needletail::parse_fastx_file;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));

    println!("=== Raw Needletail Performance Test ===");
    println!("Testing file: {}", file_path);
    println!();

    // Test raw needletail performance
    println!("🧬 Testing raw needletail parsing...");
    let start = Instant::now();

    let mut reader = parse_fastx_file(&file_path)?;
    let mut count = 0;

    while let Some(record_result) = reader.next() {
        match record_result {
            Ok(_record) => {
                count += 1;
                // Just count, don't process
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }

    let raw_duration = start.elapsed();
    println!("  Raw needletail: {} records in {:?}", count, raw_duration);

    // Test with our record conversion
    println!("🔄 Testing with record conversion...");
    let start = Instant::now();

    let mut reader = parse_fastx_file(&file_path)?;
    let mut count = 0;

    while let Some(record_result) = reader.next() {
        match record_result {
            Ok(record) => {
                // Convert to our record format
                let _name = unsafe { std::str::from_utf8_unchecked(record.id()) };
                let seq_bytes = record.seq();
                let _sequence = unsafe { std::str::from_utf8_unchecked(&seq_bytes) };
                let _quality = record
                    .qual()
                    .map(|q| unsafe { std::str::from_utf8_unchecked(q) })
                    .unwrap_or("");
                count += 1;
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }

    let conversion_duration = start.elapsed();
    println!(
        "  With conversion: {} records in {:?}",
        count, conversion_duration
    );

    // Simple comparison - just show the overhead
    println!(
        "🧪 Conversion overhead: {:.1}%",
        (conversion_duration.as_secs_f64() / raw_duration.as_secs_f64() - 1.0) * 100.0
    );

    Ok(())
}
