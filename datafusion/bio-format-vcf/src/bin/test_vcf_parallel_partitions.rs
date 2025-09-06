use datafusion::prelude::*;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Test files that might exist
    let test_files = vec![
        "gnomad.exomes.v4.1.sites.chr21.vcf.bgz",
        "test.vcf.bgz",
        "sample.vcf.gz",
    ];

    let mut test_file = None;
    for file in &test_files {
        if std::path::Path::new(file).exists() {
            // Check if corresponding GZI index exists
            let gzi_file = format!("{}.gzi", file);
            if std::path::Path::new(&gzi_file).exists() {
                test_file = Some(file);
                break;
            }
        }
    }

    let file_path = match test_file {
        Some(f) => f.to_string(),
        None => {
            println!("No BGZF VCF files with GZI index found for parallel testing.");
            println!("To test parallel partition-based processing, you need:");
            for file in &test_files {
                println!("  - {} (with corresponding .gzi index)", file);
            }
            println!("\nTesting basic parallel table provider functionality...");

            // Test creating the parallel table provider even without a file
            match BgzfVcfTableProvider::try_new("nonexistent.vcf.bgz") {
                Ok(_) => unreachable!(), // This should fail for nonexistent file
                Err(_) => println!("✓ BgzfVcfTableProvider correctly handles nonexistent files"),
            }

            return Ok(());
        }
    };

    println!(
        "Testing parallel partition-based VCF processing with: {}",
        file_path
    );

    // Test 1: Different partition counts
    let partition_counts = vec![1, 2, 4, 8];

    for &target_partitions in &partition_counts {
        println!(
            "\n=== Test with {} target partitions ===",
            target_partitions
        );
        let start_time = Instant::now();

        // Create session with specific target partitions
        let config = SessionConfig::new().with_target_partitions(target_partitions);
        let ctx = SessionContext::new_with_config(config);

        // Create parallel BGZF VCF table provider
        let table_provider = BgzfVcfTableProvider::try_new(file_path.clone())?;

        // Register table
        ctx.register_table("vcf_parallel", Arc::new(table_provider))?;

        // Run COUNT query to test parallel partition processing
        let df = ctx
            .sql("SELECT COUNT(*) as record_count FROM vcf_parallel")
            .await?;

        let results = df.collect().await?;
        let duration = start_time.elapsed();

        if let Some(batch) = results.first() {
            if let Some(column) = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
            {
                println!("Records processed: {}", column.value(0));
            }
        }
        println!("Time with {} partitions: {:?}", target_partitions, duration);
    }

    // Test 2: Projection with parallel partitions
    println!("\n=== Test projection with parallel partitions ===");
    let start_time = Instant::now();

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let table_provider = BgzfVcfTableProvider::try_new(file_path.clone())?;

    ctx.register_table("vcf_parallel", Arc::new(table_provider))?;

    let df = ctx
        .sql("SELECT chrom, start, end FROM vcf_parallel LIMIT 1000")
        .await?;

    let results = df.collect().await?;
    let duration = start_time.elapsed();

    println!(
        "Projection query (4 partitions) completed in: {:?}",
        duration
    );
    println!("Batches returned: {}", results.len());
    if let Some(batch) = results.first() {
        println!("First batch rows: {}", batch.num_rows());
    }

    // Test 3: Performance comparison
    println!("\n=== Performance comparison: 1 vs 4 partitions ===");

    // Single partition
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    let table_provider = BgzfVcfTableProvider::try_new(file_path.clone())?;
    ctx.register_table("vcf_parallel", Arc::new(table_provider))?;

    let start_time = Instant::now();
    let df = ctx
        .sql("SELECT COUNT(*) FROM vcf_parallel LIMIT 10000")
        .await?;
    let _results = df.collect().await?;
    let single_partition_time = start_time.elapsed();

    // Four partitions
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let table_provider = BgzfVcfTableProvider::try_new(file_path.clone())?;
    ctx.register_table("vcf_parallel", Arc::new(table_provider))?;

    let start_time = Instant::now();
    let df = ctx
        .sql("SELECT COUNT(*) FROM vcf_parallel LIMIT 10000")
        .await?;
    let _results = df.collect().await?;
    let multi_partition_time = start_time.elapsed();

    println!("Single partition time: {:?}", single_partition_time);
    println!("Multi partition time: {:?}", multi_partition_time);

    if multi_partition_time < single_partition_time {
        println!("✓ Parallel processing shows performance improvement!");
    } else {
        println!("ℹ Parallel processing overhead may exceed benefits for this dataset size");
    }

    println!("\n🎉 Parallel partition-based VCF processing tests completed successfully!");
    Ok(())
}
