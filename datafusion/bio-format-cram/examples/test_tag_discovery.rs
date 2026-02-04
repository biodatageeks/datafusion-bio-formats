use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_file = "/Users/mwiewior/research/git/polars-bio/tests/data/io/cram/test.cram";

    println!("Testing CRAM tag discovery with file: {}", test_file);
    println!("{}", "=".repeat(80));

    // Test 1: Using try_new_with_inferred_schema (should discover tags)
    println!("\n[Test 1] Using try_new_with_inferred_schema()");
    let provider = CramTableProvider::try_new_with_inferred_schema(
        test_file.to_string(),
        None, // No reference needed for stored tags
        None,
        true,
        Some(10),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("cram_data", Arc::new(provider))?;

    // Check the schema
    let table = ctx.table("cram_data").await?;
    let schema = table.schema();

    println!("Schema fields:");
    for field in schema.fields() {
        println!("  - {} (type: {:?})", field.name(), field.data_type());
    }

    // Try to query NM tag (found in schema) - use quotes for case-sensitive names
    println!("\nQuerying first 3 records with NM tag:");
    let df = ctx
        .sql(r#"SELECT name, chrom, start, "NM", "RG", "MQ" FROM cram_data LIMIT 3"#)
        .await?;
    df.show().await?;

    // Test 2: Using new() without tag_fields (should have NO tags)
    println!("\n{}", "=".repeat(80));
    println!("\n[Test 2] Using new() without tag_fields (for comparison)");
    let provider_no_tags = CramTableProvider::new(
        test_file.to_string(),
        None,
        None,
        true,
        None, // No tags specified
    )
    .await?;

    let ctx2 = SessionContext::new();
    ctx2.register_table("cram_data_no_tags", Arc::new(provider_no_tags))?;

    let table2 = ctx2.table("cram_data_no_tags").await?;
    let schema2 = table2.schema();

    println!("Schema fields:");
    for field in schema2.fields() {
        println!("  - {} (type: {:?})", field.name(), field.data_type());
    }

    // Try to query - should fail since tags not in schema
    println!("\nAttempting to query NM (should fail - not in schema):");
    match ctx2
        .sql(r#"SELECT name, chrom, start, "NM" FROM cram_data_no_tags LIMIT 3"#)
        .await
    {
        Ok(df) => {
            println!("Query succeeded (tags might be NULL):");
            df.show().await?;
        }
        Err(e) => {
            println!("Query failed as expected: {}", e);
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("\n✓ Test completed successfully!");

    Ok(())
}
