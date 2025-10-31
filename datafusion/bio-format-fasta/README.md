# datafusion-bio-format-fasta

FASTA file format support for Apache DataFusion, enabling SQL queries on biological sequences.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading FASTA files, a simple text-based format for representing nucleotide or protein sequences.

## Features

- Read FASTA files directly into DataFusion tables
- Support for compressed files (GZIP)
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Efficient sequence queries

## Installation

```toml
[dependencies]
datafusion-bio-format-fasta = { path = "../bio-format-fasta" }
datafusion = "50.3.0"
```

## Schema

FASTA files are read into tables with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `name` | String | Sequence identifier (after >) |
| `description` | String | Optional description text |
| `sequence` | String | Nucleotide or protein sequence |

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_fasta::FastaTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a FASTA file as a table
    let table = FastaTableProvider::try_new("data/sequences.fasta").await?;
    ctx.register_table("sequences", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT name, LENGTH(sequence) as seq_length
        FROM sequences
        WHERE LENGTH(sequence) > 1000
        ORDER BY seq_length DESC
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Supported File Types

- Uncompressed FASTA (`.fasta`, `.fa`, `.fna`)
- GZIP-compressed FASTA (`.fasta.gz`, `.fa.gz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## Important Notes

- **Git Dependency**: This crate uses a forked version of noodles from biodatageeks/noodles
- The fork provides enhanced FASTA support required by this implementation
- Users will need git access to build this crate

## License

Licensed under Apache-2.0
