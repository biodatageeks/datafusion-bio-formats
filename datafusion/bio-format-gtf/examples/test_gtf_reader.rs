use datafusion_bio_format_gtf::table_provider::GtfTableProvider;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "datafusion/bio-format-gtf/tests/test.gtf".to_string());

    let table = GtfTableProvider::new(
        file_path.clone(),
        Some(vec![
            "gene_id".to_string(),
            "gene_name".to_string(),
            "gene_type".to_string(),
        ]),
        None,
        true,
    )
    .unwrap();

    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.register_table("gtf_table", Arc::new(table)).unwrap();

    println!("=== All records ===");
    let df = ctx
        .sql("SELECT chrom, type, start, \"end\", gene_id, gene_name FROM gtf_table")
        .await?;
    df.show().await?;

    println!("\n=== Feature type counts ===");
    let df = ctx
        .sql("SELECT type, COUNT(*) as cnt FROM gtf_table GROUP BY type ORDER BY cnt DESC")
        .await?;
    df.show().await?;

    println!("\n=== Exons only ===");
    let df = ctx
        .sql("SELECT chrom, start, \"end\", gene_name FROM gtf_table WHERE type = 'exon'")
        .await?;
    df.show().await?;

    Ok(())
}
