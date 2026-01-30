use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create BAM table with NM, MD, AS tags
    let table = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        Some(4), // 4 threads
        None,
        true, // 0-based coordinates
        Some(vec!["NM".to_string(), "MD".to_string(), "AS".to_string()]),
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("alignments", Arc::new(table))?;

    println!("===========================================");
    println!("BAM Optional Tag Support Example");
    println!("===========================================\n");

    // Query 1: Basic tag projection
    println!("Query 1: Alignments with tag fields (NM, MD, AS)");
    println!("--------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT name, chrom, start, NM, MD, AS
        FROM alignments
        LIMIT 10
    ",
        )
        .await?;
    df.show().await?;

    // Query 2: Filter by tag values (edit distance)
    println!("\nQuery 2: Alignments with low edit distance (NM <= 2)");
    println!("------------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT name, chrom, start, end, NM
        FROM alignments
        WHERE NM IS NOT NULL AND NM <= 2
        LIMIT 10
    ",
        )
        .await?;
    df.show().await?;

    // Query 3: Tag aggregations
    println!("\nQuery 3: Average edit distance per chromosome");
    println!("------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT chrom,
               AVG(NM) as avg_edit_distance,
               COUNT(*) as read_count
        FROM alignments
        WHERE NM IS NOT NULL
        GROUP BY chrom
        ORDER BY chrom
    ",
        )
        .await?;
    df.show().await?;

    // Query 4: Complex filtering
    println!("\nQuery 4: High-quality alignments (with tag filtering)");
    println!("--------------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT name, chrom, start, NM, AS, mapping_quality
        FROM alignments
        WHERE NM IS NOT NULL
          AND NM <= 1
          AND mapping_quality >= 30
        LIMIT 10
    ",
        )
        .await?;
    df.show().await?;

    // Query 5: Count alignments with missing tags
    println!("\nQuery 5: Count of alignments with/without NM tag");
    println!("---------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT
            CASE WHEN NM IS NULL THEN 'Missing' ELSE 'Present' END as nm_status,
            COUNT(*) as count
        FROM alignments
        GROUP BY nm_status
    ",
        )
        .await?;
    df.show().await?;

    // Query 6: Core fields only (tags not parsed - lazy evaluation demo)
    println!("\nQuery 6: Core fields only (no tags - lazy evaluation)");
    println!("--------------------------------------------------------");
    let df = ctx
        .sql(
            "
        SELECT chrom, COUNT(*) as count
        FROM alignments
        GROUP BY chrom
        ORDER BY chrom
    ",
        )
        .await?;
    df.show().await?;

    println!("\n===========================================");
    println!("Example completed successfully!");
    println!("===========================================");

    Ok(())
}
