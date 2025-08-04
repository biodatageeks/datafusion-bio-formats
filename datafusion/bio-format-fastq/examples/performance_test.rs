//! performance_test.rs - A performance testing example for BgzfFastqTableProvider.
//!
//! This example demonstrates how to use the `BgzfFastqTableProvider` to run a
//! parallelized query on a BGZF-compressed FASTQ file and measures the time
//! it takes to complete.
//!
//! ## Usage
//!
//! To run this example, you first need a BGZF-compressed FASTQ file and its
//! corresponding GZI index. You can create these using the `bgzip` and `indexer`
//! command-line tools.
//!
//! ```bash
//! # Compress a FASTQ file
//! bgzip -c your_file.fastq > your_file.fastq.bgz
//!
//! # Create the GZI index
//! indexer -f your_file.fastq.bgz
//! ```
//!
//! Once you have the necessary files, you can run the example with:
//!
//! ```bash
//! cargo run --release --example performance_test -- /path/to/your_file.fastq.bgz
//! ```

use std::env;
use std::time::Instant;

use datafusion::prelude::*;
use datafusion_bio_format_fastq::BgzfFastqTableProvider;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!(
            "Usage: cargo run --release --example performance_test -- /path/to/your_file.fastq.bgz"
        );
        return Ok(());
    }
    let file_path = &args[1];

    println!("Using file: {}", file_path);

    // Create a SessionContext
    let config = SessionConfig::new().with_target_partitions(16);
    let ctx = SessionContext::new_with_config(config);

    // Create and register the table provider
    let provider = BgzfFastqTableProvider::try_new(file_path)?;
    ctx.register_table("fastq", std::sync::Arc::new(provider))?;

    // Execute a query and measure the time
    let start_time = Instant::now();
    let df = ctx.sql("SELECT count(*) FROM fastq").await?;
    let batches = df.collect().await?;
    let elapsed = start_time.elapsed();

    // Print the results
    if let Some(batch) = batches.first() {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            if !array.is_empty() {
                println!("Record count: {}", array.value(0));
            }
        }
    }

    println!("Query executed in: {:?}", elapsed);

    Ok(())
}
