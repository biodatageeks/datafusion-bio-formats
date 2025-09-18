use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"));

    println!("=== Direct Parser Comparison ===");
    println!("Testing file: {}", file_path);
    println!();

    // Test needletail directly
    println!("🪡 Raw Needletail:");
    let start = Instant::now();
    let mut reader = needletail::parse_fastx_file(&file_path)?;
    let mut needletail_count = 0;

    while let Some(record_result) = reader.next() {
        match record_result {
            Ok(_) => needletail_count += 1,
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }
    let needletail_time = start.elapsed();
    println!("  {} records in {:?}", needletail_count, needletail_time);

    // Test noodles directly
    println!("🧬 Raw Noodles:");
    let start = Instant::now();

    use datafusion_bio_format_core::object_storage::{ObjectStorageOptions, get_remote_stream};
    use futures_util::StreamExt;
    use noodles_fastq as fastq;
    use tokio_util::io::StreamReader;

    let stream = get_remote_stream(file_path, ObjectStorageOptions::default(), None).await?;
    let stream_reader = StreamReader::new(stream);
    let mut reader = fastq::r#async::io::Reader::new(stream_reader);

    let mut noodles_count = 0;
    let mut record = fastq::Record::default();

    while reader.read_record(&mut record).await? != 0 {
        noodles_count += 1;
    }

    let noodles_time = start.elapsed();
    println!("  {} records in {:?}", noodles_count, noodles_time);

    println!();
    println!("=== Results ===");
    if needletail_time < noodles_time {
        let improvement =
            (noodles_time.as_secs_f64() / needletail_time.as_secs_f64() - 1.0) * 100.0;
        println!("🎉 Needletail is {:.1}% faster than Noodles!", improvement);
    } else {
        let degradation =
            (needletail_time.as_secs_f64() / noodles_time.as_secs_f64() - 1.0) * 100.0;
        println!("⚠️ Needletail is {:.1}% slower than Noodles", degradation);
    }

    Ok(())
}
