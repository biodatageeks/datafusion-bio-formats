# datafusion-bio-format-cram

CRAM file format support for Apache DataFusion, enabling SQL queries on compressed sequence alignment data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading CRAM files, a highly compressed columnar file format for storing biological sequences aligned to a reference genome.

## Features

- **Read** CRAM files directly into DataFusion tables
- **Write** CRAM files from DataFusion query results
- High compression ratios with reference-based encoding
- Reference FASTA file support (required for optimal compression)
- Cloud storage support (GCS, S3, Azure Blob Storage) for reads
- Memory-efficient streaming for large alignment files
- Support for CRAM indexes (CRAI)
- Round-trip metadata preservation (headers, reference sequences, read groups)

## Installation

```toml
[dependencies]
datafusion-bio-format-cram = { path = "../bio-format-cram" }
datafusion = "50.3.0"
```

## Schema

CRAM files are read into tables with alignment information similar to BAM:

| Column | Type | Description |
|--------|------|-------------|
| `qname` | String | Query template name |
| `flag` | Int32 | Bitwise flags |
| `rname` | String | Reference sequence name |
| `pos` | Int64 | 1-based leftmost mapping position |
| `mapq` | Int32 | Mapping quality |
| `cigar` | String | CIGAR string |
| `rnext` | String | Reference name of mate/next read |
| `pnext` | Int64 | Position of mate/next read |
| `tlen` | Int64 | Observed template length |
| `seq` | String | Segment sequence |
| `qual` | String | ASCII Phred-scaled base qualities |

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_cram::CramTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a CRAM file as a table
    let table = CramTableProvider::try_new("data/alignments.cram").await?;
    ctx.register_table("alignments", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT qname, rname, pos, mapq
        FROM alignments
        WHERE mapq >= 30 AND rname = 'chr1'
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

### Writing CRAM Files

Write DataFusion query results to CRAM files with reference sequence support:

```rust
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to reference genome (REQUIRED for optimal CRAM compression)
    let reference_path = Some("reference/genome.fasta".to_string());

    // Register input CRAM file
    let input_table = CramTableProvider::new(
        "input.cram".to_string(),
        reference_path.clone(),
        None,  // cloud storage options
        true,  // 0-based coordinates
        None,  // no tags
    )?;
    ctx.register_table("input", Arc::new(input_table))?;

    // Create output table for write
    let output_table = CramTableProvider::new_for_write(
        "output.cram".to_string(),
        ctx.table("input").await?.schema().into(),
        reference_path,  // Use same reference
        None,  // tag fields extracted from schema
        true,  // 0-based coordinates
    );
    ctx.register_table("output", Arc::new(output_table))?;

    // Write filtered results
    ctx.sql("
        INSERT OVERWRITE output
        SELECT * FROM input
        WHERE mapping_quality >= 30
    ").await?.show().await?;

    Ok(())
}
```

**Important Notes:**
- **Reference Required**: CRAM files need a reference FASTA for optimal compression
- Create reference index with: `samtools faidx reference/genome.fasta`
- Writing without a reference is possible but results in larger files
- Reference path is preserved in metadata for round-trip compatibility

**Example: BAM to CRAM Conversion**
```sql
-- Convert BAM to CRAM (requires reference)
INSERT OVERWRITE cram_output SELECT * FROM bam_input;

-- Filter and convert
INSERT OVERWRITE output SELECT * FROM input WHERE chrom = 'chr1' AND mapping_quality >= 30;
```

**Example: Writing Without Reference (Not Recommended)**
```rust
// Creates larger CRAM files without reference-based compression
let output_table = CramTableProvider::new_for_write(
    "output.cram".to_string(),
    schema,
    None,  // No reference - stores full sequences
    None,
    true,
);
```

See `examples/write_cram.rs` for complete write examples.

## Supported File Types

- CRAM files (`.cram`)
- Indexed CRAM files with CRAI indexes
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## Important Notes

- **Git Dependency**: This crate uses a forked version of noodles from biodatageeks/noodles
- The fork provides enhanced CRAM support required by this implementation
- Users will need git access to build this crate
- CRAM files require a reference genome for full sequence reconstruction

## License

Licensed under Apache-2.0
