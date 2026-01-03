# datafusion-bio-format-bed

BED (Browser Extensible Data) file format support for Apache DataFusion, enabling SQL queries on genomic intervals and annotations.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading BED files, a simple tab-delimited format for representing genomic regions and annotations.

## Features

- Read BED files directly into DataFusion tables
- Support for BED3, BED6, BED12, and custom BED formats
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Efficient querying of genomic intervals

## Installation

```toml
[dependencies]
datafusion-bio-format-bed = { path = "../bio-format-bed" }
datafusion = "50.3.0"
```

## Schema

BED files support multiple formats with varying numbers of columns:

**BED3 (minimum):**
- `chrom`: Chromosome name
- `chrom_start`: Start position (0-based)
- `chrom_end`: End position (exclusive)

**BED6:**
- Adds: `name`, `score`, `strand`

**BED12:**
- Adds: `thick_start`, `thick_end`, `item_rgb`, `block_count`, `block_sizes`, `block_starts`

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_bed::BedTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a BED file as a table
    let table = BedTableProvider::try_new("data/genes.bed").await?;
    ctx.register_table("genes", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT chrom, chrom_start, chrom_end, name
        FROM genes
        WHERE chrom = 'chr1' AND (chrom_end - chrom_start) > 1000
        ORDER BY chrom_start
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Supported File Types

- Uncompressed BED (`.bed`)
- GZIP-compressed BED (`.bed.gz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## License

Licensed under Apache-2.0
