use std::path::Path;
use std::time::Instant;

use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_gff::bgzf_parallel_reader::BgzfGffTableProvider;
use datafusion_bio_format_gff::table_provider::GffTableProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default file unless passed as first CLI arg
    let default_path = "/tmp/gencode.v38.annotation.gff3.gz".to_string();
    let file_path = std::env::args().nth(1).unwrap_or(default_path);

    eprintln!("BGZF GFF SELECT * benchmark");
    eprintln!("File: {}", file_path);

    if !Path::new(&file_path).exists() {
        eprintln!("Error: file not found: {}", file_path);
        std::process::exit(2);
    }
    let gzi = format!("{}.gzi", file_path);
    if !Path::new(&gzi).exists() {
        eprintln!("Error: GZI index not found: {}", gzi);
        std::process::exit(3);
    }

    let thread_counts = [1usize, 2, 4, 6];
    let mut rows: Vec<(usize, f64, usize, f64)> = Vec::new();

    for &threads in &thread_counts {
        let config = SessionConfig::new().with_target_partitions(threads);
        let ctx = SessionContext::new_with_config(config);

        let provider = BgzfGffTableProvider::try_new(&file_path, None)?;
        ctx.register_table("gff", std::sync::Arc::new(provider))?;

        let start = Instant::now();
        let df = ctx.sql("SELECT * FROM gff").await?;
        let batches = df.collect().await?;
        let elapsed = start.elapsed().as_secs_f64();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let rps = if elapsed > 0.0 {
            total_rows as f64 / elapsed
        } else {
            0.0
        };

        eprintln!(
            "threads={:>2}  time={:>8.2}s  rows={:>12}  rec/s={:>12.0}",
            threads, elapsed, total_rows, rps
        );
        rows.push((threads, elapsed, total_rows, rps));
    }

    // Compare against single-thread provider (GffTableProvider) for 1-thread correctness
    {
        let ctx = SessionContext::new();
        let provider = GffTableProvider::new(
            file_path.clone(),
            None,
            Some(1),
            Some(ObjectStorageOptions::default()),
            true, // Use 0-based coordinates (default)
        )?;
        ctx.register_table("gff_single", std::sync::Arc::new(provider))?;
        let start = Instant::now();
        let df = ctx.sql("SELECT * FROM gff_single").await?;
        let batches = df.collect().await?;
        let elapsed = start.elapsed().as_secs_f64();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let rps = if elapsed > 0.0 {
            total_rows as f64 / elapsed
        } else {
            0.0
        };
        eprintln!(
            "single-provider time={:>8.2}s  rows={:>12}  rec/s={:>12.0}",
            elapsed, total_rows, rps
        );
        // Emit a CSV row with threads=1_provider to distinguish
        rows.push((1usize, elapsed, total_rows, rps));
    }

    // Print summary as CSV to stdout so it can be captured easily
    println!("threads,seconds,rows,rows_per_sec");
    for (t, sec, r, rps) in rows {
        println!("{},{:.4},{},{}", t, sec, r, rps.round());
    }

    Ok(())
}
