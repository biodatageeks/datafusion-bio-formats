use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_file = "/Users/mwiewior/research/git/polars-bio/tests/data/io/cram/test.cram";

    println!("Testing explicit MD tag specification");
    println!("{}", "=".repeat(80));

    // Test 1: Explicitly request MD tag
    println!("\n[Test 1] Explicitly requesting MD tag in schema");
    let provider = CramTableProvider::new(
        test_file.to_string(),
        None,
        None,
        true,
        Some(vec!["MD".to_string(), "NM".to_string()]),
        false,
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("cram_data", Arc::new(provider))?;

    let table = ctx.table("cram_data").await?;
    let schema = table.schema();

    println!("Schema fields:");
    for field in schema.fields() {
        println!("  - {} (type: {:?})", field.name(), field.data_type());
    }

    // Try to query MD and NM tags
    println!("\nQuerying first 3 records with explicit MD and NM:");
    let df = ctx
        .sql(r#"SELECT name, chrom, start, "MD", "NM" FROM cram_data LIMIT 3"#)
        .await?;
    df.show().await?;

    // Test 2: Try with inferred schema
    println!("\n{}", "=".repeat(80));
    println!(
        "\n[Test 2] Using try_new_with_inferred_schema (with reference for MD/NM calculation)"
    );

    let provider2 = CramTableProvider::try_new_with_inferred_schema(
        test_file.to_string(),
        None, // Try without reference first
        None,
        true,
        Some(10),
        false,
    )
    .await?;

    let ctx2 = SessionContext::new();
    ctx2.register_table("cram_inferred", Arc::new(provider2))?;

    let table2 = ctx2.table("cram_inferred").await?;
    let schema2 = table2.schema();
    println!("Discovered tags:");
    for field in schema2.fields() {
        if field.name().len() == 2 && field.name().chars().all(|c| c.is_uppercase()) {
            println!("  - {}", field.name());
        }
    }

    println!("\n✓ Test completed");

    Ok(())
}
