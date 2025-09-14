///! Example demonstrating parallel BGZF VCF processing with DataFusion
///!
///! This example shows how to use the BgzfVcfTableProvider for partition-based
///! parallel processing of BGZF-compressed VCF files. The parallelism is controlled
///! by DataFusion's `target_partitions` configuration.
use datafusion::prelude::*;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up logging to see debug information about partitioning
    env_logger::init();

    // For this example, you'll need:
    // 1. A BGZF-compressed VCF file (*.vcf.bgz)
    // 2. A corresponding GZI index file (*.vcf.bgz.gzi)
    //
    // You can create a GZI index using: bgzip -r your_file.vcf.gz
    let vcf_file = "example.vcf.bgz";

    if !std::path::Path::new(vcf_file).exists() {
        eprintln!("VCF file not found: {}", vcf_file);
        eprintln!("Please provide a BGZF-compressed VCF file with .gzi index");
        return Ok(());
    }

    let gzi_file = format!("{}.gzi", vcf_file);
    if !std::path::Path::new(&gzi_file).exists() {
        eprintln!("GZI index file not found: {}", gzi_file);
        eprintln!("Create it with: bgzip -r {}", vcf_file);
        return Ok(());
    }

    println!("🧬 VCF Parallel BGZF Processing Example");
    println!("File: {}", vcf_file);
    println!("Index: {}", gzi_file);

    // Example 1: Single partition (sequential processing)
    println!("\n📊 Example 1: Single Partition Processing");
    {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        let start = std::time::Instant::now();
        let df = ctx
            .sql("SELECT COUNT(*) as total_variants FROM vcf_data")
            .await?;
        let result = df.collect().await?;
        let duration = start.elapsed();

        if let Some(batch) = result.first() {
            if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
            {
                println!("Total variants: {}", array.value(0));
            }
        }
        println!("Sequential processing time: {:?}", duration);
    }

    // Example 2: Multi-partition processing (parallel)
    println!("\n⚡ Example 2: Multi-Partition Processing");
    {
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        let start = std::time::Instant::now();
        let df = ctx
            .sql("SELECT COUNT(*) as total_variants FROM vcf_data")
            .await?;
        let result = df.collect().await?;
        let duration = start.elapsed();

        if let Some(batch) = result.first() {
            if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
            {
                println!("Total variants: {}", array.value(0));
            }
        }
        println!("Parallel processing time (4 partitions): {:?}", duration);
    }

    // Example 3: Complex query with projection and filtering
    println!("\n🔍 Example 3: Complex Query with Projection");
    {
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        let start = std::time::Instant::now();
        let df = ctx
            .sql(
                "SELECT chrom, start, end, ref, alt 
             FROM vcf_data 
             WHERE chrom = '1' 
             LIMIT 1000",
            )
            .await?;
        let result = df.collect().await?;
        let duration = start.elapsed();

        println!("Query returned {} batches", result.len());
        if let Some(batch) = result.first() {
            println!("First batch has {} rows", batch.num_rows());
        }
        println!("Complex query time: {:?}", duration);
    }

    // Example 4: Demonstrate scalability with different partition counts
    println!("\n📈 Example 4: Scalability Test");
    for &partitions in &[1, 2, 4, 8] {
        let config = SessionConfig::new().with_target_partitions(partitions);
        let ctx = SessionContext::new_with_config(config);

        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        let start = std::time::Instant::now();
        let df = ctx
            .sql("SELECT chrom, COUNT(*) as count FROM vcf_data GROUP BY chrom LIMIT 25")
            .await?;
        let _result = df.collect().await?;
        let duration = start.elapsed();

        println!("{} partitions: {:?}", partitions, duration);
    }

    println!("\n✅ All examples completed successfully!");
    println!("The BgzfVcfTableProvider automatically:");
    println!("  📁 Reads BGZF block boundaries from the GZI index");
    println!("  📊 Distributes work across partitions based on target_partitions");
    println!("  🔄 Processes each partition in parallel threads");
    println!("  🎯 Maintains record boundaries and data integrity");

    Ok(())
}
