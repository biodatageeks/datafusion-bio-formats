use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Test if we have a VCF file to work with
    let test_files = vec![
        "gnomad.exomes.v4.1.sites.chr21.vcf.bgz",
        "test.vcf.bgz",
        "sample.vcf.gz",
    ];

    let mut test_file = None;
    for file in &test_files {
        if std::path::Path::new(file).exists() {
            test_file = Some(file);
            break;
        }
    }

    let file_path = match test_file {
        Some(f) => f.to_string(),
        None => {
            println!(
                "No test VCF files found. Creating a small test to demonstrate functionality."
            );
            println!("For full testing, please ensure you have a BGZF compressed VCF file:");
            for file in &test_files {
                println!("  - {}", file);
            }
            println!("\nTesting basic VCF table provider functionality...");

            // Just test table creation without file operations
            let ctx = SessionContext::new();
            match VcfTableProvider::new(
                "nonexistent.vcf.bgz".to_string(),
                None,
                None,
                Some(4),
                Some(ObjectStorageOptions::default()),
            ) {
                Ok(_) => println!("✓ VcfTableProvider created successfully with 4 threads"),
                Err(e) => println!("✗ Error creating VcfTableProvider: {}", e),
            }

            return Ok(());
        }
    };

    println!("Testing parallel BGZF reading with file: {}", file_path);

    // Test 1: Basic functionality with auto thread detection
    println!("\n=== Test 1: Auto thread detection ===");
    let start_time = Instant::now();

    let ctx = SessionContext::new();
    let table_provider = VcfTableProvider::new(
        file_path.clone(),
        None, // No specific info fields
        None, // No specific format fields
        None, // Auto-detect threads
        Some(ObjectStorageOptions::default()),
    )?;

    ctx.register_table("vcf_table", Arc::new(table_provider))?;

    let df = ctx
        .sql("SELECT COUNT(*) as record_count FROM vcf_table")
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
    println!("Time with auto threads: {:?}", duration);

    // Test 2: Explicit thread counts
    let thread_counts = vec![1, 2, 4];
    for thread_count in thread_counts {
        println!("\n=== Test with {} threads ===", thread_count);
        let start_time = Instant::now();

        let ctx = SessionContext::new();
        let table_provider = VcfTableProvider::new(
            file_path.clone(),
            None,
            None,
            Some(thread_count),
            Some(ObjectStorageOptions::default()),
        )?;

        ctx.register_table("vcf_table", Arc::new(table_provider))?;

        let df = ctx
            .sql("SELECT COUNT(*) as record_count FROM vcf_table LIMIT 100")
            .await?;

        let _results = df.collect().await?;
        let duration = start_time.elapsed();

        println!("Time with {} threads: {:?}", thread_count, duration);
    }

    println!("\nParallel BGZF VCF reading tests completed successfully!");
    Ok(())
}
