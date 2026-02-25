//! performance_test.rs - A performance testing example for FastqTableProvider.
//!
//! This example demonstrates how to use the `FastqTableProvider` to run a
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
use datafusion_bio_format_fastq::FastqTableProvider;

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

    println!("Using file: {file_path}");

    // Create a SessionContext with 16 target partitions for parallel reading
    let config = SessionConfig::new().with_target_partitions(16);
    let ctx = SessionContext::new_with_config(config);

    // Create and register the table provider
    let provider = FastqTableProvider::new(file_path.to_string(), None)?;
    ctx.register_table("fastq", std::sync::Arc::new(provider))?;

    // Execute a query and measure the time
    let start_time = Instant::now();
    let df = ctx.sql("SELECT * FROM fastq").await?;
    let batches = df.collect().await?;
    println!("{}", batches.len());
    let elapsed = start_time.elapsed();

    println!("Query executed in: {elapsed:?}");

    Ok(())
}
