# datafusion-bio-format-fastq

FASTQ file format support for Apache DataFusion, enabling SQL queries on DNA sequencing data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading FASTQ files, a standard text-based format for storing nucleotide sequences and their corresponding quality scores.

## Features

- Read FASTQ files directly into DataFusion tables
- Support for compressed files (GZIP, BGZF)
- Parallel reading of BGZF-compressed files for improved performance
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Streaming for memory-efficient processing of large files

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-bio-format-fastq = { path = "../bio-format-fastq" }
datafusion = "50.3.0"
```

## Schema

FASTQ files are read into tables with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `name` | String | Sequence identifier |
| `description` | String (optional) | Sequence description |
| `sequence` | String | Nucleotide sequence (DNA/RNA) |
| `quality_scores` | String | Quality scores (Phred+33 encoded) |

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a FASTQ file as a table
    let table = FastqTableProvider::try_new("data/sample.fastq.gz").await?;
    ctx.register_table("sequences", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT name, sequence, LENGTH(sequence) as seq_length
        FROM sequences
        WHERE LENGTH(sequence) > 100
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Supported File Types

- Uncompressed FASTQ (`.fastq`, `.fq`)
- GZIP-compressed FASTQ (`.fastq.gz`, `.fq.gz`)
- BGZF-compressed FASTQ (`.fastq.bgz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## Performance

For optimal performance with large FASTQ files:

- Use BGZF compression to enable parallel decompression
- Files from cloud storage are automatically streamed
- Projection pushdown reduces memory usage by selecting only needed columns

## License

Licensed under Apache-2.0
