use datafusion::execution::context::SessionContext;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::env;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let file_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/tmp/gencode.v38.annotation.gff3.gz".to_string());

    // Optional: threads via env or default 4
    let threads: usize = env::var("GFF_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    let object_storage_options = ObjectStorageOptions::default();

    // Register table
    let table = GffTableProvider::new(
        file_path.clone(),
        None, // SELECT * mode (nested attributes)
        Some(threads),
        Some(object_storage_options),
    )?;

    let ctx = SessionContext::new();

    // Run SELECT * and time it
    let start = Instant::now();
    ctx.register_table("gff_table", Arc::new(table))?;
    let df = ctx.sql("SELECT * FROM gff_table").await?;
    let batches = df.collect().await?;
    let elapsed = start.elapsed();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let batches_count = batches.len();

    println!("File: {}", file_path);
    println!("Threads: {}", threads);
    println!("Batches: {}", batches_count);
    println!("Rows: {}", rows);
    println!("Elapsed: {:?}", elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        println!(
            "Throughput: {:.0} rows/sec",
            rows as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}
