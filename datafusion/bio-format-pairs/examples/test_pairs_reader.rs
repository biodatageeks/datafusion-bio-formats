use datafusion::prelude::*;
use datafusion_bio_format_pairs::table_provider::PairsTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let test_file = format!("{}/tests/test_small.pairs", env!("CARGO_MANIFEST_DIR"));

    let table = PairsTableProvider::new(test_file, None, true)?;
    ctx.register_table("pairs", Arc::new(table))?;

    println!("=== SELECT * FROM pairs LIMIT 5 ===");
    let df = ctx.sql("SELECT * FROM pairs LIMIT 5").await?;
    df.show().await?;

    println!("\n=== SELECT chr1, pos1, chr2, pos2 FROM pairs ===");
    let df = ctx.sql("SELECT chr1, pos1, chr2, pos2 FROM pairs").await?;
    df.show().await?;

    println!("\n=== SELECT * FROM pairs WHERE chr1 = 'chr1' ===");
    let df = ctx.sql("SELECT * FROM pairs WHERE chr1 = 'chr1'").await?;
    df.show().await?;

    println!("\n=== SELECT * FROM pairs WHERE chr1 = 'chr1' AND chr2 = 'chr2' ===");
    let df = ctx
        .sql("SELECT * FROM pairs WHERE chr1 = 'chr1' AND chr2 = 'chr2'")
        .await?;
    df.show().await?;

    Ok(())
}
