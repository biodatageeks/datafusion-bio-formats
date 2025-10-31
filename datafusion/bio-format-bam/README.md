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

## Installation

```toml
[dependencies]
datafusion-bio-format-bam = { path = "../bio-format-bam" }
datafusion = "50.3.0"
```

## Schema

BAM files are read into tables with alignment information:

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
use datafusion_bio_format_bam::BamTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a BAM file as a table
    let table = BamTableProvider::try_new("data/alignments.bam").await?;
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

## Supported File Types

- BAM files (`.bam`)
- Indexed BAM files with BAI indexes
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## License

Licensed under Apache-2.0
