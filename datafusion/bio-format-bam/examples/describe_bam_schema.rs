use datafusion::functions_aggregate::expr_fn::count;
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("BAM Schema Discovery Example\n");
    println!("=============================\n");

    let ctx = SessionContext::new();

    // Example 1: Discover schema from first 100 records
    println!("Discovering schema from first 100 records...\n");
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true, // 0-based coordinates
        None, // No pre-specified tags
    )
    .await?;

    let schema_df = provider.describe(&ctx, Some(100)).await?;

    println!("Discovered columns:");
    schema_df.clone().show().await?;

    // Example 2: Query the schema DataFrame to find tags
    println!("\n\nDiscovered tags only:");
    println!("---------------------");
    let tags_only = schema_df
        .clone()
        .filter(col("category").eq(lit("tag")))?
        .select(vec![col("column_name"), col("data_type"), col("sam_type")])?;
    tags_only.show().await?;

    // Example 3: Count by category
    println!("\n\nColumn counts by category:");
    println!("--------------------------");
    let summary = schema_df
        .aggregate(
            vec![col("category")],
            vec![count(col("column_name")).alias("count")],
        )?
        .sort(vec![col("category").sort(true, false)])?;
    summary.show().await?;

    // Example 4: Different sample sizes
    println!("\n\nComparing different sample sizes:");
    println!("----------------------------------");

    for sample_size in [10, 50, 200] {
        let schema_df = provider.describe(&ctx, Some(sample_size)).await?;
        let count = schema_df.count().await?;
        println!(
            "Sample size {}: {} total columns discovered",
            sample_size, count
        );
    }

    Ok(())
}
