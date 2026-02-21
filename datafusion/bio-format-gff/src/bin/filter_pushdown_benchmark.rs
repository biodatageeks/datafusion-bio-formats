use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;
use std::time::Instant;

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::AUTO), // Auto-detect BGZF compression
    }
}

async fn run_benchmark_query(
    ctx: &SessionContext,
    query: &str,
    description: &str,
) -> Result<(u64, f64), Box<dyn std::error::Error>> {
    println!("\n🔍 Running: {description}");
    println!("Query: {query}");

    let start_time = Instant::now();
    let df = ctx.sql(query).await?;
    let results = df.collect().await?;
    let duration = start_time.elapsed();

    let total_rows = results.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64;
    let duration_secs = duration.as_secs_f64();

    println!(
        "✅ Results: {} rows in {:.3}s ({:.0} rows/sec)",
        total_rows,
        duration_secs,
        total_rows as f64 / duration_secs
    );

    Ok((total_rows, duration_secs))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧬 GFF Filter Pushdown Performance Benchmark");
    println!("============================================");

    let file_path = "/tmp/gencode.v49.annotation.gff3.bgz";
    let object_storage_options = create_object_storage_options();

    // Check if file exists
    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {file_path} does not exist!");
        eprintln!("Please ensure the file is available for benchmarking.");
        return Ok(());
    }

    println!("📁 File: {file_path}");

    // Create table provider
    let table = GffTableProvider::new(
        file_path.to_string(),
        None, // No specific attribute fields for core benchmarks
        Some(object_storage_options),
        true, // Use 0-based coordinates (default)
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("gff", Arc::new(table))?;

    println!("\n📊 Testing Filter Pushdown Performance");
    println!("=====================================");

    let benchmarks = vec![
        // COUNT queries (measure filtering efficiency)
        (
            "SELECT COUNT(*) FROM gff",
            "Baseline: Count all records (no filters)",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1'",
            "Filter: Count chr1 records",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1' AND type = 'gene'",
            "Filter: Count chr1 genes",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE start > 100000 AND start < 200000",
            "Filter: Count records in position range",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom IN ('chr1', 'chr2', 'chr3')",
            "Filter: Count records from multiple chromosomes",
        ),
        // SELECT queries (measure data transfer efficiency)
        (
            "SELECT chrom, start, end, type FROM gff WHERE chrom = 'chr1' LIMIT 1000",
            "Select: chr1 records with projection + limit",
        ),
        (
            "SELECT chrom, start, end FROM gff WHERE start BETWEEN 50000 AND 60000 LIMIT 1000",
            "Select: Records in position window",
        ),
        (
            "SELECT chrom, type, source FROM gff WHERE type = 'exon' AND chrom = 'chr1' LIMIT 1000",
            "Select: chr1 exons with multiple filters",
        ),
        (
            "SELECT start, end FROM gff WHERE chrom = 'chrX' AND strand = '+' LIMIT 1000",
            "Select: chrX positive strand features",
        ),
        // Complex queries
        (
            "SELECT chrom, COUNT(*) as count FROM gff WHERE type IN ('gene', 'exon', 'CDS') GROUP BY chrom ORDER BY count DESC LIMIT 10",
            "Complex: Feature counts by chromosome",
        ),
    ];

    let mut results = Vec::new();

    for (query, description) in benchmarks {
        match run_benchmark_query(&ctx, query, description).await {
            Ok((rows, duration)) => {
                results.push((description.to_string(), rows, duration));
            }
            Err(e) => {
                println!("❌ Query failed: {e}");
            }
        }

        // Small delay between queries
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Summary
    println!("\n📈 Benchmark Summary");
    println!("===================");
    println!(
        "{:<60} {:>10} {:>10} {:>15}",
        "Query", "Rows", "Time (s)", "Throughput (r/s)"
    );
    println!("{}", "-".repeat(100));

    for (description, rows, duration) in results {
        let throughput = if duration > 0.0 {
            rows as f64 / duration
        } else {
            0.0
        };
        println!("{description:<60} {rows:>10} {duration:>10.3} {throughput:>15.0}");
    }

    println!("\n🎯 Performance Analysis");
    println!("======================");
    println!("• Filter pushdown should show improved performance for filtered queries");
    println!("• Look for faster execution times on queries with WHERE clauses");
    println!("• COUNT queries benefit most from early filtering");
    println!("• Complex filters (AND, IN) should still perform well");

    // Test filter support detection
    println!("\n🔧 Filter Support Detection Test");
    println!("===============================");

    let filter_tests = vec![
        ("chrom = 'chr1'", "String equality"),
        ("start > 100000", "Numeric comparison"),
        ("start BETWEEN 10000 AND 20000", "BETWEEN expression"),
        ("chrom IN ('chr1', 'chr2')", "IN list"),
        ("chrom = 'chr1' AND type = 'gene'", "AND expression"),
        ("score IS NOT NULL", "Unsupported expression"),
    ];

    for (filter_expr, description) in filter_tests {
        let query = format!("SELECT COUNT(*) FROM gff WHERE {filter_expr}");
        println!("Testing: {description} ({filter_expr})");

        match ctx.sql(&query).await {
            Ok(df) => {
                let start = Instant::now();
                match df.collect().await {
                    Ok(results) => {
                        let duration = start.elapsed();
                        let rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
                        println!("  ✅ Success: {rows} rows in {duration:?}");
                    }
                    Err(e) => println!("  ❌ Execution failed: {e}"),
                }
            }
            Err(e) => println!("  ❌ Parse failed: {e}"),
        }
    }

    println!("\n🏁 Benchmark Complete!");
    Ok(())
}
