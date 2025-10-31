# datafusion-bio-format-gff

GFF (General Feature Format) file format support for Apache DataFusion, enabling SQL queries on genome annotations.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading GFF/GFF3 files, a standard tab-delimited format for genomic features and annotations.

## Features

- Read GFF/GFF3 files directly into DataFusion tables
- Parse and query GFF attributes
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Support for compressed files (GZIP)
- Efficient annotation queries

## Installation

```toml
[dependencies]
datafusion-bio-format-gff = { path = "../bio-format-gff" }
datafusion = "50.3.0"
```

## Schema

GFF files are read into tables with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `seqid` | String | Sequence/chromosome name |
| `source` | String | Source of the annotation |
| `type` | String | Feature type (gene, exon, CDS, etc.) |
| `start` | Int64 | Start position (1-based) |
| `end` | Int64 | End position (inclusive) |
| `score` | Float64 | Score value (optional) |
| `strand` | String | Strand (+, -, ., ?) |
| `phase` | String | Phase for CDS features (0, 1, 2, .) |
| `attributes` | String | Semicolon-separated attributes |

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_gff::GffTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a GFF file as a table
    let table = GffTableProvider::try_new("data/annotations.gff3").await?;
    ctx.register_table("annotations", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT seqid, type, start, end
        FROM annotations
        WHERE type = 'gene' AND seqid = 'chr1'
        ORDER BY start
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Supported File Types

- Uncompressed GFF (`.gff`, `.gff3`)
- GZIP-compressed GFF (`.gff.gz`, `.gff3.gz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## Attributes

GFF attributes are stored as a semicolon-separated string. You can parse specific attributes using SQL string functions or post-process them in your application.

## License

Licensed under Apache-2.0
