# datafusion-bio-format-bam

BAM (Binary Alignment Map) file format support for Apache DataFusion, enabling SQL queries on sequence alignment data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading BAM files, the binary compressed version of SAM (Sequence Alignment/Map) format used to store sequence alignments.

## Features

- Read BAM files directly into DataFusion tables
- BGZF compression support (native to BAM format)
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Memory-efficient streaming for large alignment files
- Support for indexed BAM files (BAI)
- **Optional alignment tag support** with lazy parsing - include tags like NM, MD, AS as queryable columns

## Installation

```toml
[dependencies]
datafusion-bio-format-bam = { path = "../bio-format-bam" }
datafusion = "50.3.0"
```

## Schema

BAM files are read into tables with the following core alignment columns:

| Column | Type | Description |
|--------|------|-------------|
| `name` | String | Query template name |
| `chrom` | String | Reference sequence name (chromosome) |
| `start` | UInt32 | Leftmost mapping position (0-based or 1-based) |
| `end` | UInt32 | Rightmost mapping position |
| `flags` | UInt32 | Bitwise alignment flags |
| `cigar` | String | CIGAR string |
| `mapping_quality` | UInt32 | Mapping quality |
| `mate_chrom` | String | Reference name of mate/next read |
| `mate_start` | UInt32 | Position of mate/next read |
| `sequence` | String | Read sequence |
| `quality_scores` | String | ASCII Phred-scaled base qualities |

### Optional Alignment Tags

You can include additional BAM alignment tags as columns by specifying them when creating the table provider:

```rust
let table = BamTableProvider::new(
    "data/alignments.bam".to_string(),
    Some(4),  // threads
    None,     // storage options
    true,     // 0-based coordinates
    Some(vec!["NM".to_string(), "MD".to_string(), "AS".to_string()]),  // tags
)?;
```

Common supported tags include:
- **Alignment scoring**: NM (edit distance), MD (mismatch string), AS (alignment score), XS (suboptimal score)
- **Read groups**: RG (read group), LB (library), PU (platform unit)
- **Single-cell**: CB (cell barcode), UB (UMI barcode)
- **Quality**: BQ (base quality), OQ (original quality)
- And ~40 more standard tags

Tags are only parsed when explicitly requested in queries (lazy evaluation), ensuring minimal performance overhead.

## Usage Examples

### Basic Usage (Core Fields Only)

```rust
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a BAM file as a table (no tags)
    let table = BamTableProvider::new(
        "data/alignments.bam".to_string(),
        Some(4),  // 4 threads for decompression
        None,     // No cloud storage options
        true,     // Use 0-based coordinates
        None,     // No optional tags
    )?;
    ctx.register_table("alignments", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT name, chrom, start, mapping_quality
        FROM alignments
        WHERE mapping_quality >= 30 AND chrom = 'chr1'
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

### With Optional Alignment Tags

```rust
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a BAM file with alignment tags
    let table = BamTableProvider::new(
        "data/alignments.bam".to_string(),
        Some(4),
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string(), "AS".to_string()]),
    )?;
    ctx.register_table("alignments", Arc::new(table))?;

    // Query using tag fields
    let df = ctx.sql("
        SELECT name, chrom, start, NM, AS
        FROM alignments
        WHERE NM <= 2 AND AS >= 100
        LIMIT 10
    ").await?;

    df.show().await?;

    // Aggregate by tag values
    let df = ctx.sql("
        SELECT chrom, AVG(NM) as avg_edit_distance
        FROM alignments
        WHERE NM IS NOT NULL
        GROUP BY chrom
    ").await?;

    df.show().await?;
    Ok(())
}
```

See `examples/test_bam_with_tags.rs` for more comprehensive examples.

## Supported File Types

- BAM files (`.bam`)
- Indexed BAM files with BAI indexes
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## License

Licensed under Apache-2.0
