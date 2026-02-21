//! Example demonstrating BAM file write support
//!
//! This example shows how to:
//! 1. Read data from a BAM file
//! 2. Apply SQL transformations
//! 3. Write results to a new BAM file

use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register input BAM file (replace with actual file path)
    let input_path = "test_data/sample.bam";
    let input_table = BamTableProvider::new(
        input_path.to_string(),
        None,  // object storage options
        true,  // 0-based coordinates
        None,  // tag fields
        false, // String CIGAR (default)
    )
    .await?;
    ctx.register_table("input_bam", Arc::new(input_table))?;

    // Example 1: Filter and write high-quality alignments
    println!("Example 1: Writing filtered BAM file");
    let output_path_1 = "output/high_quality.bam";

    // Create output table provider for write
    let df = ctx
        .sql("SELECT * FROM input_bam WHERE mapping_quality >= 30")
        .await?;

    let output_table_1 = BamTableProvider::new_for_write(
        output_path_1.to_string(),
        df.schema().inner().clone(),
        None,  // tag fields (extracted from schema)
        true,  // 0-based coordinates
        false, // sort_on_write
    );
    ctx.register_table("output_1", Arc::new(output_table_1))?;

    ctx.sql("INSERT OVERWRITE output_1 SELECT * FROM input_bam WHERE mapping_quality >= 30")
        .await?
        .show()
        .await?;

    // Example 2: Write SAM format (uncompressed)
    println!("\nExample 2: Writing SAM file (uncompressed)");
    let output_path_2 = "output/filtered.sam";

    let output_table_2 = BamTableProvider::new_for_write(
        output_path_2.to_string(),
        df.schema().inner().clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_2", Arc::new(output_table_2))?;

    ctx.sql("INSERT OVERWRITE output_2 SELECT * FROM input_bam WHERE chrom = 'chr1' LIMIT 100")
        .await?
        .show()
        .await?;

    // Example 3: Convert SAM to BAM
    println!("\nExample 3: Converting SAM to BAM");

    // Register SAM input
    let sam_input = BamTableProvider::new(
        "input.sam".to_string(),
        None,  // object storage options
        true,  // 0-based coordinates
        None,  // tag fields
        false, // String CIGAR (default)
    )
    .await?;
    ctx.register_table("input_sam", Arc::new(sam_input))?;

    // Write as BAM
    let bam_output = BamTableProvider::new_for_write(
        "output/converted.bam".to_string(),
        ctx.table("input_sam").await?.schema().inner().clone(),
        None,
        true,
        false,
    );
    ctx.register_table("bam_output", Arc::new(bam_output))?;

    ctx.sql("INSERT OVERWRITE bam_output SELECT * FROM input_sam")
        .await?
        .show()
        .await?;

    println!("\n✓ BAM/SAM write examples completed successfully");
    println!("Output files:");
    println!("  - {output_path_1}");
    println!("  - {output_path_2}");
    println!("  - output/converted.bam");

    Ok(())
}
