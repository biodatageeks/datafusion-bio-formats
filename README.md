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
| **[datafusion-bio-format-vcf](datafusion/bio-format-vcf)** | VCF genetic variants | ✅ (TBI/CSI) | ❌ | ✅ (indexed) | ✅ | ✅ |
| **[datafusion-bio-format-bam](datafusion/bio-format-bam)** | BAM sequence alignments | ✅ (BAI/CSI) | ❌ | ✅ (indexed) | ✅ | ✅ |
| **[datafusion-bio-format-bed](datafusion/bio-format-bed)** | BED genomic intervals | ❌ | ❌  | ❌ | ❌ | ✅ |
| **[datafusion-bio-format-gff](datafusion/bio-format-gff)** | GFF genome annotations | ✅ | ✅ | ✅ (BGZF) | ❌ | ✅ |
| **[datafusion-bio-format-fasta](datafusion/bio-format-fasta)** | FASTA biological sequences | ❌ | ❌  | ❌ | ❌ | ✅ |
| **[datafusion-bio-format-cram](datafusion/bio-format-cram)** | CRAM compressed alignments | ✅ (CRAI) | ❌  | ✅ (indexed) | ✅ | ✅ |

## Features

- 🚀 **High Performance**: Index-based random access and parallel reading across chromosomes
- 🔍 **Predicate Pushdown**: SQL `WHERE` clauses on genomic coordinates use BAI/CRAI/TBI indexes to skip irrelevant data
- ☁️ **Cloud Native**: Built-in support for GCS, S3, and Azure Blob Storage
- 📊 **SQL Interface**: Query genomic data using familiar SQL syntax
- 💾 **Memory Efficient**: Streaming architecture for large files
- 🔧 **DataFusion Integration**: Seamless integration with Apache DataFusion ecosystem
- ✍️ **Write Support**: Export query results to BAM/SAM, CRAM, FASTQ, and VCF files with compression

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

### VCF Header Metadata Preservation

When reading VCF files, header metadata (field descriptions, types, and numbers) is stored in Arrow field metadata. This enables round-trip read/write operations to preserve the original VCF header information:

```rust
// Reading preserves metadata in schema
let provider = VcfTableProvider::new("input.vcf", ...)?;
let schema = provider.schema();

// Field metadata contains original VCF definitions
let dp_field = schema.field_with_name("DP")?;
let metadata = dp_field.metadata();
// metadata["vcf_description"] = "Total read depth"
// metadata["vcf_type"] = "Integer"
// metadata["vcf_number"] = "1"
```

The writer uses this metadata to reconstruct proper VCF header lines:
```
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
```

For write-only operations (new output files), use `VcfTableProvider::new_for_write()` which accepts the schema directly without reading from file.

## Index-Based Range Queries

BAM, CRAM, and VCF table providers support **index-based predicate pushdown** for efficient genomic region queries. When an index file is present alongside the data file, SQL filters on `chrom`, `start`, and `end` columns are translated into indexed random access, skipping irrelevant data entirely.

### Supported Index Formats

| Data Format | Index Formats | Naming Convention |
|-------------|---------------|-------------------|
| BAM | BAI, CSI | `sample.bam.bai` or `sample.bai`, `sample.bam.csi` |
| CRAM | CRAI | `sample.cram.crai` |
| VCF (bgzf) | TBI, CSI | `sample.vcf.gz.tbi`, `sample.vcf.gz.csi` |

Index files are **auto-discovered** — place them alongside the data file and the table provider will find them automatically. No configuration needed.

### SQL Query Patterns

All standard genomic filter patterns are supported:

```sql
-- Single region query
SELECT * FROM alignments
WHERE chrom = 'chr1' AND start >= 1000000 AND end <= 2000000;

-- Multi-chromosome query (parallel across chromosomes)
SELECT * FROM alignments
WHERE chrom IN ('chr1', 'chr2', 'chr3');

-- Range with BETWEEN
SELECT * FROM alignments
WHERE chrom = 'chr1' AND start BETWEEN 1000000 AND 2000000;

-- Combine genomic region with record-level filters
SELECT * FROM alignments
WHERE chrom = 'chr1' AND start >= 1000000 AND mapping_quality >= 30;
```

### How It Works

```
┌────────────────────────────────────────┐
│  SQL: WHERE chrom = 'chr1'             │
│        AND start >= 1000000            │
│        AND mapping_quality >= 30       │
└──────────┬─────────────────────────────┘
           │
    ┌──────▼─────────────────────┐
    │  1. Extract genomic regions │  chrom/start/end → index query regions
    │  2. Separate residual       │  mapping_quality → post-read filter
    └──────┬─────────────────────┘
           │
    ┌──────▼─────────────────────┐
    │  3. Partition by region     │  Each region = 1 DataFusion partition
    │     (parallel execution)    │  Executed concurrently by DataFusion
    └──────┬─────────────────────┘
           │
    ┌──────▼─────────────────────┐
    │  4. Per-partition:          │
    │     IndexedReader.query()   │  Seek directly via BAI/CRAI/TBI
    │     → apply residual filter │  mapping_quality >= 30
    │     → build RecordBatch     │
    └────────────────────────────┘
```

**Partitioning behavior:**

| Index Available? | SQL Filters | Partitions |
|-----------------|-------------|------------|
| Yes | `chrom = 'chr1' AND start >= 1000` | 1 (the specified region) |
| Yes | `chrom IN ('chr1', 'chr2')` | 2 (one per chromosome) |
| Yes | `mapping_quality >= 30` (no genomic filter) | N (one per chromosome in file) |
| Yes | None (full scan) | N (one per chromosome — parallel full scan) |
| No | Any | 1 (sequential full scan) |

When an index exists but no genomic filters are specified, the query is automatically parallelized across all chromosomes in the file.

### Record-Level Filter Pushdown

Beyond index-based region queries, all formats support record-level predicate evaluation. Filters on columns like `mapping_quality`, `flag`, `score`, or `strand` are evaluated as each record is read, filtering early before Arrow `RecordBatch` construction.

This works **with or without** an index file:

```sql
-- No index needed — filters applied per-record during sequential scan
SELECT * FROM alignments WHERE mapping_quality >= 30 AND flag & 4 = 0;
```

### Index File Generation

Create index files using standard bioinformatics tools:

```bash
# BAM: sort and index
samtools sort input.bam -o sorted.bam
samtools index sorted.bam                # creates sorted.bam.bai

# CRAM: sort and index
samtools sort input.cram -o sorted.cram --reference ref.fa
samtools index sorted.cram               # creates sorted.cram.crai

# VCF: sort, compress, and index
bcftools sort input.vcf -Oz -o sorted.vcf.gz
bcftools index -t sorted.vcf.gz          # creates sorted.vcf.gz.tbi
```

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
