# datafusion-bio-format-cram

CRAM file format support for Apache DataFusion, enabling SQL queries on compressed sequence alignment data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading CRAM files, a highly compressed columnar file format for storing biological sequences aligned to a reference genome.

## Features

- Read CRAM files directly into DataFusion tables
- High compression ratios with reference-based encoding
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Memory-efficient streaming for large alignment files
- Support for CRAM indexes (CRAI)

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
