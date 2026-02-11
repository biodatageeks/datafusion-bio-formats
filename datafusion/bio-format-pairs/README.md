# datafusion-bio-format-pairs

Pairs (Hi-C) file format support for Apache DataFusion, enabling SQL queries on chromosome conformation capture contact data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading Pairs files, the standard tab-delimited format for Hi-C contact data from the [4D Nucleome (4DN) project](https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md).

## Features

- Read Pairs files directly into DataFusion tables
- Support for plain text and BGZF-compressed files (`.pairs`, `.pairs.gz`)
- Tabix (`.tbi`) and Pairix (`.px2`) index support for efficient region queries
- Two-level filter pushdown: chr1/pos1 via index, chr2/pos2/strand as residual filters
- Configurable coordinate system (0-based or 1-based output)
- Projection pushdown for efficient querying
- Cloud storage support (GCS, S3, Azure Blob Storage)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-bio-format-pairs = { path = "../bio-format-pairs" }
datafusion = "50.3.0"
```

## Schema

Pairs files are read into tables with columns defined by the file header. The standard 7-column format:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `readID` | String | No | Read/fragment identifier |
| `chr1` | String | No | Chromosome of the first contact |
| `pos1` | UInt32 | No | Position of the first contact |
| `chr2` | String | No | Chromosome of the second contact |
| `pos2` | UInt32 | No | Position of the second contact |
| `strand1` | String | No | Strand of the first contact (+/-) |
| `strand2` | String | No | Strand of the second contact (+/-) |

Extended columns (e.g., `frag1`, `frag2`, `mapq1`, `mapq2`) are automatically detected from the `#columns:` header line.

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_pairs::table_provider::PairsTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a Pairs file as a table (1-based coordinates, matching the Pairs spec)
    let table = PairsTableProvider::new(
        "data/contacts.pairs.gz".to_string(),
        None,   // auto-detect index
        false,  // coordinate_system_zero_based
    )?;
    ctx.register_table("contacts", Arc::new(table))?;

    // Query with SQL — index pushdown on chr1/pos1
    let df = ctx.sql("
        SELECT chr1, pos1, chr2, pos2, strand1, strand2
        FROM contacts
        WHERE chr1 = 'chr1' AND pos1 >= 50000 AND pos1 <= 150000
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Index Support

The provider automatically discovers tabix (`.tbi`) and Pairix (`.px2`) index files:

- `file.pairs.gz.tbi` — standard tabix index
- `file.pairs.gz.px2` — Pairix index (treated as tabix-compatible)

When an index is present, queries filtering on `chr1` and/or `pos1` use indexed region lookups. Filters on `chr2`, `pos2`, `strand1`, `strand2`, and other columns are applied as residual filters after the index query.

## Coordinate System

Pairs files use 1-based positions per the 4DN specification. The `coordinate_system_zero_based` flag controls output:

- `false` (default): Positions are returned as-is from the file (1-based)
- `true`: Positions are converted to 0-based (subtract 1) for consistency with BAM/BED conventions

## Supported File Types

- Uncompressed Pairs (`.pairs`)
- BGZF-compressed Pairs (`.pairs.gz`)
- GZIP-compressed Pairs (`.pairs.gz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## License

Licensed under Apache-2.0
