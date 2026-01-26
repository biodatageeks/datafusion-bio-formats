# datafusion-bio-formats

[![CI](https://github.com/biodatageeks/datafusion-bio-formats/actions/workflows/ci.yml/badge.svg)](https://github.com/biodatageeks/datafusion-bio-formats/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Apache DataFusion table providers for bioinformatics file formats, enabling SQL queries on genomic data.

## Overview

This workspace provides a collection of Rust crates that implement DataFusion `TableProvider` interfaces for various bioinformatics file formats. Query your genomic data using SQL through Apache DataFusion's powerful query engine.

## Crates

| Crate | Description | Predicate Pushdown | Projection Pushdown | Multi-threaded | Write Support | Status |
|-------|-------------|-------------------|---------------------|----------------|---------------|--------|
| **[datafusion-bio-format-core](datafusion/bio-format-core)** | Core utilities and object storage support | N/A | N/A | N/A | N/A | ✅ |
| **[datafusion-bio-format-fastq](datafusion/bio-format-fastq)** | FASTQ sequencing reads | ❌ | ❌ |✅ (BGZF) | ✅ | ✅ |
| **[datafusion-bio-format-vcf](datafusion/bio-format-vcf)** | VCF genetic variants | ❌ | ❌ | ❌ | ✅ | ✅ |
| **[datafusion-bio-format-bam](datafusion/bio-format-bam)** | BAM sequence alignments | ❌ | ❌ | ❌ | ❌ | ✅ |
| **[datafusion-bio-format-bed](datafusion/bio-format-bed)** | BED genomic intervals | ❌ | ❌  | ❌ | ❌ | ✅ |
| **[datafusion-bio-format-gff](datafusion/bio-format-gff)** | GFF genome annotations | ✅ | ✅ | ✅ (BGZF) | ❌ | ✅ |
| **[datafusion-bio-format-fasta](datafusion/bio-format-fasta)** | FASTA biological sequences | ❌ | ❌  | ❌ | ❌ | ✅ |
| **[datafusion-bio-format-cram](datafusion/bio-format-cram)** | CRAM compressed alignments | ❌ | ❌  | ❌ | ❌ | ✅ |

## Features

- 🚀 **High Performance**: Parallel reading of BGZF-compressed files
- ☁️ **Cloud Native**: Built-in support for GCS, S3, and Azure Blob Storage
- 📊 **SQL Interface**: Query genomic data using familiar SQL syntax
- 💾 **Memory Efficient**: Streaming architecture for large files
- 🔧 **DataFusion Integration**: Seamless integration with Apache DataFusion ecosystem
- ✍️ **Write Support**: Export query results to FASTQ and VCF files with compression

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

## Write Support

FASTQ and VCF formats support writing DataFusion query results back to files using the `INSERT INTO` SQL syntax or the `insert_into()` API.

### Design

The write implementation follows DataFusion's standard patterns:

```
TableProvider.insert_into()
    └── WriteExec (ExecutionPlan consuming RecordBatches)
        ├── Serializer (Arrow → format conversion)
        └── LocalWriter (compression handling)
```

**Key features:**
- **Compression**: Auto-detected from file extension (`.bgz`/`.bgzf` → BGZF, `.gz` → GZIP, else plain)
- **BGZF default**: Block-gzipped format recommended for bioinformatics (allows random access)
- **Coordinate conversion**: Automatic 0-based ↔ 1-based conversion for VCF files
- **Mode**: OVERWRITE only (creates/replaces file)

### Write a FASTQ file

```rust
use datafusion::prelude::*;
use datafusion_bio_format_fastq::BgzfFastqTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register source FASTQ
    let source = BgzfFastqTableProvider::try_new("input.fastq.gz", None).await?;
    ctx.register_table("source", Arc::new(source))?;

    // Register destination FASTQ (compression auto-detected from extension)
    let dest = BgzfFastqTableProvider::try_new("output.fastq.bgz", None).await?;
    ctx.register_table("dest", Arc::new(dest))?;

    // Filter and write to new file
    ctx.sql("
        INSERT INTO dest
        SELECT name, description, sequence, quality_scores
        FROM source
        WHERE LENGTH(sequence) >= 100
    ").await?.collect().await?;

    Ok(())
}
```

### Write a VCF file

```rust
use datafusion::prelude::*;
use datafusion_bio_format_vcf::VcfTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register source VCF with INFO fields
    let source = VcfTableProvider::try_new(
        "input.vcf.gz",
        Some(vec!["AF".to_string(), "DP".to_string()]),  // INFO fields
        None,  // FORMAT fields
        true,  // coordinate_system_zero_based
    ).await?;
    ctx.register_table("variants", Arc::new(source))?;

    // Register destination VCF
    let dest = VcfTableProvider::try_new(
        "filtered.vcf.bgz",
        Some(vec!["AF".to_string(), "DP".to_string()]),
        None,
        true,
    ).await?;
    ctx.register_table("output", Arc::new(dest))?;

    // Filter variants and write
    ctx.sql("
        INSERT INTO output
        SELECT chrom, start, end, id, ref, alt, qual, filter, AF, DP
        FROM variants
        WHERE AF > 0.01 AND DP >= 10
    ").await?.collect().await?;

    Ok(())
}
```

### Coordinate System Handling

VCF files use 1-based coordinates, but the table provider can expose them as 0-based half-open intervals (common in bioinformatics tools like BED):

| Setting | Read Behavior | Write Behavior |
|---------|---------------|----------------|
| `coordinate_system_zero_based=true` | VCF POS 100 → DataFrame start=99 | DataFrame start=99 → VCF POS 100 |
| `coordinate_system_zero_based=false` | VCF POS 100 → DataFrame start=100 | DataFrame start=100 → VCF POS 100 |

The same setting controls both reading and writing, ensuring round-trip consistency.

### Compression Options

| Extension | Compression | Notes |
|-----------|-------------|-------|
| `.vcf`, `.fastq` | Plain | Uncompressed |
| `.vcf.gz`, `.fastq.gz` | GZIP | Standard compression |
| `.vcf.bgz`, `.fastq.bgz` | BGZF | Block-gzipped (recommended) |
| `.vcf.bgzf`, `.fastq.bgzf` | BGZF | Block-gzipped (recommended) |

## Performance Benchmarks

This project includes a comprehensive benchmark framework to track performance across releases and validate optimizations.

📊 **[View Benchmark Results](https://biodatageeks.org/datafusion-bio-formats/benchmark-comparison/)**

### Run Benchmarks Locally

```bash
# Build the benchmark runner
cargo build --release --package datafusion-bio-benchmarks-runner

# Run GFF benchmarks
./target/release/benchmark-runner benchmarks/configs/gff.yml
```

See [benchmarks/README.md](benchmarks/README.md) for detailed documentation on running benchmarks and adding new formats.

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
