use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vcf_file = "/tmp/homo_sapiens-chr1.vcf.bgz";

    println!("🧬 VCF Performance Comparison");
    println!("📁 File: {}", vcf_file);

    // Test 1: Regular VcfTableProvider (uses MultithreadedReader)
    println!("\n1. Testing regular VcfTableProvider...");
    let start = Instant::now();

    let ctx = SessionContext::new();
    let table_provider = VcfTableProvider::new(
        vcf_file.to_string(),
        None,    // No specific info fields
        None,    // No specific format fields
        Some(4), // Use 4 threads
        Some(ObjectStorageOptions::default()),
        true, // Use 0-based coordinates (default)
    )?;

    ctx.register_table("vcf_regular", Arc::new(table_provider))?;

    let df = ctx
        .sql("SELECT COUNT(*) FROM vcf_regular LIMIT 10000")
        .await?;
    let results = df.collect().await?;
    let regular_time = start.elapsed();

    let regular_count = if let Some(batch) = results.first() {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            array.value(0)
        } else {
            0
        }
    } else {
        0
    };

    println!(
        "✅ Regular VCF: {} records in {:?}",
        regular_count, regular_time
    );

    // Test 2: Parallel BGZF VcfTableProvider
    println!("\n2. Testing BgzfVcfTableProvider...");
    let start = Instant::now();

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);

    println!("   Creating table provider...");
    let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
    println!("   Registering table...");
    ctx.register_table("vcf_parallel", Arc::new(table_provider))?;

    println!("   Running query (this may take a while)...");
    let query_start = Instant::now();
    let df = ctx
        .sql("SELECT COUNT(*) FROM vcf_parallel LIMIT 10000")
        .await?;
    let results = df.collect().await?;
    let parallel_time = start.elapsed();
    let query_time = query_start.elapsed();

    let parallel_count = if let Some(batch) = results.first() {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            array.value(0)
        } else {
            0
        }
    } else {
        0
    };

    println!(
        "✅ Parallel BGZF: {} records in {:?} (query: {:?})",
        parallel_count, parallel_time, query_time
    );

    // Comparison
    println!("\n📊 COMPARISON RESULTS");
    println!("=====================");
    println!(
        "Regular VCF:     {} records in {:?}",
        regular_count, regular_time
    );
    println!(
        "Parallel BGZF:   {} records in {:?}",
        parallel_count, parallel_time
    );

    if regular_count == parallel_count {
        println!("✅ Row counts match!");
    } else {
        println!(
            "❌ Row count mismatch: {} vs {}",
            regular_count, parallel_count
        );
    }

    if parallel_time < regular_time {
        let speedup = regular_time.as_secs_f64() / parallel_time.as_secs_f64();
        println!("🎯 Parallel is {:.2}x faster!", speedup);
    } else {
        let slowdown = parallel_time.as_secs_f64() / regular_time.as_secs_f64();
        println!("⚠️  Parallel is {:.2}x slower", slowdown);
    }

    Ok(())
}
