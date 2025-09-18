use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::{FastqParser, table_provider::FastqTableProvider};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <fastq_file_path> [parser]", args[0]);
        eprintln!("  parser: noodles (default) or needletail");
        std::process::exit(1);
    }

    let file_path = args[1].clone();
    let parser = if args.len() > 2 {
        match args[2].as_str() {
            "noodles" => FastqParser::Noodles,
            "needletail" => FastqParser::Needletail,
            other => {
                eprintln!(
                    "Unknown parser: {}. Valid options: noodles, needletail",
                    other
                );
                std::process::exit(1);
            }
        }
    } else {
        FastqParser::Noodles
    };

    println!("Testing FASTQ file: {}", file_path);
    println!("Using parser: {}", parser);

    let object_storage_options = ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),        // 16 MB
        concurrent_fetches: Some(8), // Number of concurrent requests
        compression_type: None,
    };

    let start_time = Instant::now();

    let table = FastqTableProvider::new_with_parser(
        file_path,
        Some(1),
        Some(object_storage_options),
        parser,
    )?;

    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await?;

    ctx.register_table("fastq_data", Arc::new(table))?;

    println!("Counting records...");
    let df = ctx
        .sql("SELECT COUNT(*) as record_count FROM fastq_data")
        .await?;
    let results = df.collect().await?;

    let elapsed = start_time.elapsed();

    if let Some(batch) = results.first() {
        if let Some(count_array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            let count = count_array.value(0);
            println!("Total records: {}", count);
        }
    }

    println!("Time elapsed: {:?}", elapsed);

    // Show first few records
    println!("\nFirst 5 records:");
    let df_sample = ctx
        .sql("SELECT name, LENGTH(sequence) as seq_length FROM fastq_data LIMIT 5")
        .await?;
    let sample_results = df_sample.collect().await?;

    for batch in sample_results {
        for row in 0..batch.num_rows() {
            let name = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap()
                .value(row);
            let seq_len = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .value(row);
            println!("  {}: {} bp", name, seq_len);
        }
    }

    Ok(())
}
