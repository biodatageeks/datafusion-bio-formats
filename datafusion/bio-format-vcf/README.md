# datafusion-bio-format-vcf

VCF (Variant Call Format) file format support for Apache DataFusion, enabling SQL queries on genetic variation data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading VCF files, the standard format for storing genetic variants discovered through DNA sequencing.

## Features

- Read VCF files directly into DataFusion tables
- Support for compressed files (GZIP, BGZF)
- Parallel reading of BGZF-compressed files
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Preserves case sensitivity for INFO and FORMAT fields
- Projection pushdown for efficient querying

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-bio-format-vcf = { path = "../bio-format-vcf" }
datafusion = "50.3.0"
```

## Schema

VCF files are read into tables with core variant columns plus dynamically generated INFO and FORMAT columns:

**Core Columns:**
- `chrom`: Chromosome name (String)
- `pos`: Position (Int64)
- `id`: Variant identifier (String)
- `ref`: Reference allele (String)
- `alt`: Alternate alleles (String)
- `qual`: Quality score (Float64)
- `filter`: Filter status (String)

**INFO Columns:** Dynamically created based on VCF header (e.g., `info_af`, `info_dp`)

**FORMAT/Sample Columns:** Created per sample (e.g., `sample1_gt`, `sample1_dp`)

## Usage Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_vcf::VcfTableProvider;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a VCF file as a table
    let table = VcfTableProvider::try_new("data/variants.vcf.gz").await?;
    ctx.register_table("variants", Arc::new(table))?;

    // Query the data with SQL
    let df = ctx.sql("
        SELECT chrom, pos, ref, alt, info_af
        FROM variants
        WHERE chrom = 'chr1' AND info_af > 0.01
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

## Important Notes

- **Case Sensitivity**: INFO and FORMAT field names are case-sensitive as per VCF specification
- **Git Dependency**: This crate uses a forked version of noodles for enhanced VCF support

## Supported File Types

- Uncompressed VCF (`.vcf`)
- GZIP-compressed VCF (`.vcf.gz`)
- BGZF-compressed VCF (`.vcf.bgz`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## License

Licensed under Apache-2.0
