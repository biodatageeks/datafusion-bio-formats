use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vcf_file = "/tmp/homo_sapiens-chr1.vcf.bgz";

    println!("🧬 Minimal VCF Test");
    println!("📁 File: {}", vcf_file);

    // Test 1: Just create table provider and schema
    println!("\n1. Creating table provider...");
    let start = Instant::now();
    let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
    println!("✅ Created in {:?}", start.elapsed());
    println!(
        "   Schema fields: {}",
        table_provider.schema().fields().len()
    );

    // Test 2: Register with DataFusion
    println!("\n2. Registering with DataFusion...");
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("vcf_data", Arc::new(table_provider))?;
    println!("✅ Registered successfully");

    // Test 3: Very simple query with limit
    println!("\n3. Running LIMIT 10 query...");
    let start = Instant::now();
    let df = ctx
        .sql("SELECT chrom, start FROM vcf_data LIMIT 10")
        .await?;
    let results = df.collect().await?;
    let duration = start.elapsed();

    println!("✅ Query completed in {:?}", duration);
    println!("   Returned {} batches", results.len());

    if let Some(batch) = results.first() {
        println!("   First batch: {} rows", batch.num_rows());
        if batch.num_rows() > 0 {
            // Print first few values
            let chrom_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let start_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                .unwrap();

            for i in 0..std::cmp::min(3, batch.num_rows()) {
                println!(
                    "     Row {}: {} {}",
                    i,
                    chrom_array.value(i),
                    start_array.value(i)
                );
            }
        }
    }

    // Test 4: Count query with small limit
    println!("\n4. Running COUNT with LIMIT 1000...");
    let start = Instant::now();
    let df = ctx.sql("SELECT COUNT(*) FROM vcf_data LIMIT 1000").await?;
    let results = df.collect().await?;
    let duration = start.elapsed();

    if let Some(batch) = results.first() {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            println!("✅ Count: {} in {:?}", array.value(0), duration);
        }
    }

    println!("\n🎉 Minimal test completed!");

    Ok(())
}
