//! Example demonstrating CRAM file write support
//!
//! This example shows how to:
//! 1. Read data from a CRAM file
//! 2. Apply SQL transformations
//! 3. Write results to a new CRAM file with reference sequence support

use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Path to reference genome (required for CRAM)
    let reference_path = Some("reference/genome.fasta".to_string());

    // Register input CRAM file with automatic tag discovery (replace with actual file path)
    let input_path = "test_data/sample.cram";
    let input_table = CramTableProvider::try_new_with_inferred_schema(
        input_path.to_string(),
        reference_path.clone(),
        None,      // object storage options
        true,      // 0-based coordinates
        Some(100), // Sample 100 records to discover tags
        false,     // binary_cigar
    )
    .await?;
    ctx.register_table("input_cram", Arc::new(input_table))?;

    // Example 1: Filter and write high-quality alignments
    println!("Example 1: Writing filtered CRAM file with reference");
    let output_path_1 = "output/high_quality.cram";

    // Create output table provider for write
    let df = ctx
        .sql("SELECT * FROM input_cram WHERE mapping_quality >= 30")
        .await?;

    let output_table_1 = CramTableProvider::new_for_write(
        output_path_1.to_string(),
        df.schema().inner().clone(),
        reference_path.clone(),
        None,  // tag fields (extracted from schema)
        true,  // 0-based coordinates
        false, // sort_on_write
    );
    ctx.register_table("output_1", Arc::new(output_table_1))?;

    ctx.sql("INSERT OVERWRITE output_1 SELECT * FROM input_cram WHERE mapping_quality >= 30")
        .await?
        .show()
        .await?;

    // Example 2: Extract specific chromosome
    println!("\nExample 2: Extracting chr1 to CRAM");
    let output_path_2 = "output/chr1_only.cram";

    let output_table_2 = CramTableProvider::new_for_write(
        output_path_2.to_string(),
        df.schema().inner().clone(),
        reference_path.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_2", Arc::new(output_table_2))?;

    ctx.sql("INSERT OVERWRITE output_2 SELECT * FROM input_cram WHERE chrom = 'chr1'")
        .await?
        .show()
        .await?;

    // Example 3: Write CRAM without reference (less efficient)
    println!("\nExample 3: Writing CRAM without reference (not recommended)");
    let output_path_3 = "output/no_ref.cram";

    let output_table_3 = CramTableProvider::new_for_write(
        output_path_3.to_string(),
        df.schema().inner().clone(),
        None, // No reference - will store full sequences
        None,
        true,
        false,
    );
    ctx.register_table("output_3", Arc::new(output_table_3))?;

    ctx.sql("INSERT OVERWRITE output_3 SELECT * FROM input_cram LIMIT 1000")
        .await?
        .show()
        .await?;

    // Example 4: Convert BAM to CRAM
    println!("\nExample 4: Converting BAM to CRAM");
    use datafusion_bio_format_bam::table_provider::BamTableProvider;

    let bam_input = BamTableProvider::try_new_with_inferred_schema(
        "input.bam".to_string(),
        None,      // object storage options
        true,      // 0-based coordinates
        None,      // tag_fields (None to discover all)
        Some(100), // Sample 100 records to discover tags
        false,     // String CIGAR (default)
    )
    .await?;
    ctx.register_table("input_bam", Arc::new(bam_input))?;

    let cram_output = CramTableProvider::new_for_write(
        "output/converted.cram".to_string(),
        ctx.table("input_bam").await?.schema().inner().clone(),
        reference_path,
        None,
        true,
        false,
    );
    ctx.register_table("cram_output", Arc::new(cram_output))?;

    ctx.sql("INSERT OVERWRITE cram_output SELECT * FROM input_bam")
        .await?
        .show()
        .await?;

    println!("\n✓ CRAM write examples completed successfully");
    println!("Output files:");
    println!("  - {}", output_path_1);
    println!("  - {}", output_path_2);
    println!("  - {}", output_path_3);
    println!("  - output/converted.cram");
    println!("\nNote: Create .fai index for reference with: samtools faidx reference/genome.fasta");

    Ok(())
}
