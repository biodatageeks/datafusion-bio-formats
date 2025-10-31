# datafusion-bio-formats

[![CI](https://github.com/biodatageeks/datafusion-bio-formats/actions/workflows/ci.yml/badge.svg)](https://github.com/biodatageeks/datafusion-bio-formats/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Apache DataFusion table providers for bioinformatics file formats, enabling SQL queries on genomic data.

## Overview

This workspace provides a collection of Rust crates that implement DataFusion `TableProvider` interfaces for various bioinformatics file formats. Query your genomic data using SQL through Apache DataFusion's powerful query engine.

## Crates

| Crate | Description | Predicate Pushdown | Projection Pushdown | Multi-threaded | Status |
|-------|-------------|-------------------|---------------------|----------------|--------|
| **[datafusion-bio-format-core](datafusion/bio-format-core)** | Core utilities and object storage support | N/A | N/A | N/A | ✅ |
| **[datafusion-bio-format-fastq](datafusion/bio-format-fastq)** | FASTQ sequencing reads | ❌ | ✅ | ✅ (BGZF) | ✅ |
| **[datafusion-bio-format-vcf](datafusion/bio-format-vcf)** | VCF genetic variants | ❌ | ✅ | ✅ (BGZF) | ✅ |
| **[datafusion-bio-format-bam](datafusion/bio-format-bam)** | BAM sequence alignments | ❌ | ✅ | ❌ | ✅ |
| **[datafusion-bio-format-bed](datafusion/bio-format-bed)** | BED genomic intervals | ❌ | ✅ | ❌ | ✅ |
| **[datafusion-bio-format-gff](datafusion/bio-format-gff)** | GFF genome annotations | ✅ | ✅ | ✅ (BGZF) | ✅ |
| **[datafusion-bio-format-fasta](datafusion/bio-format-fasta)** | FASTA biological sequences | ❌ | ✅ | ❌ | ✅ |
| **[datafusion-bio-format-cram](datafusion/bio-format-cram)** | CRAM compressed alignments | ❌ | ✅ | ❌ | ✅ |

## Features

- 🚀 **High Performance**: Parallel reading of BGZF-compressed files
- ☁️ **Cloud Native**: Built-in support for GCS, S3, and Azure Blob Storage
- 📊 **SQL Interface**: Query genomic data using familiar SQL syntax
- 💾 **Memory Efficient**: Streaming architecture for large files
- 🔧 **DataFusion Integration**: Seamless integration with Apache DataFusion ecosystem

## Installation

Add the crates you need to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "50.3.0"

# Choose the formats you need:
datafusion-bio-format-fastq = { git = "https://github.com/biodatageeks/datafusion-bio-formats" }
datafusion-bio-format-vcf = { git = "https://github.com/biodatageeks/datafusion-bio-formats" }
datafusion-bio-format-bam = { git = "https://github.com/biodatageeks/datafusion-bio-formats" }
# ... etc
```

## Quick Start

### Query a FASTQ file

```rust
use datafusion::prelude::*;
use datafusion_bio_format_fastq::BgzfFastqTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a FASTQ file as a table
    let table = BgzfFastqTableProvider::try_new("data/sample.fastq.gz", None).await?;
    ctx.register_table("sequences", Arc::new(table))?;

    // Query with SQL
    let df = ctx.sql("
        SELECT name, sequence, quality_scores
        FROM sequences
        WHERE LENGTH(sequence) > 100
        LIMIT 10
    ").await?;

    df.show().await?;
    Ok(())
}
```

### Query a VCF file

```rust
use datafusion::prelude::*;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a VCF file
    let table = BgzfVcfTableProvider::try_new("data/variants.vcf.gz", None).await?;
    ctx.register_table("variants", Arc::new(table))?;

    // Query variants
    let df = ctx.sql("
        SELECT chrom, pos, ref, alt, info_af
        FROM variants
        WHERE chrom = 'chr1' AND info_af > 0.01
    ").await?;

    df.show().await?;
    Ok(())
}
```

### Query from cloud storage

```rust
// Works with GCS, S3, Azure
let table = BgzfFastqTableProvider::try_new(
    "gs://my-bucket/sample.fastq.gz",
    None
).await?;
```

## Development

### Build

```bash
cargo build --all
```

### Test

```bash
cargo test --all
```

### Format

```bash
cargo fmt --all
```

### Lint

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Documentation

```bash
cargo doc --no-deps --all-features --open
```

## Requirements

- Rust 1.86.0 or later (specified in `rust-toolchain.toml`)
- Git (for building crates with git dependencies)

## Dependencies

This project uses a forked version of [noodles](https://github.com/zaeleus/noodles) from [biodatageeks/noodles](https://github.com/biodatageeks/noodles) for enhanced support of certain file formats (CRAM, FASTA, VCF, GFF).

## Related Projects

- [polars-bio](https://github.com/biodatageeks/polars-bio) - Polars extensions for bioinformatics

## License

Licensed under Apache-2.0. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Acknowledgments

This project builds upon:
- [Apache DataFusion](https://datafusion.apache.org/) - Fast, extensible query engine
- [noodles](https://github.com/zaeleus/noodles) - Bioinformatics I/O library
- [OpenDAL](https://opendal.apache.org/) - Unified data access layer
