# Add Performance Benchmark Framework

## Why

The project needs a comprehensive performance benchmarking system to:
- Track performance improvements and regressions across releases
- Compare performance optimizations in pull requests against baseline versions
- Validate key optimizations: BGZF parallelism, predicate pushdown, and projection pushdown
- Provide visibility into performance characteristics across different platforms (Linux, macOS)

Currently, there is no automated way to systematically measure and track performance across different file formats, making it difficult to quantify optimization gains or detect regressions.

## What Changes

- Add complete benchmark infrastructure modeled after polars-bio's benchmark system with configuration-driven approach
- Implement **generic benchmark runner** that works with any file format through YAML configuration
- Implement three benchmark categories for each file format:
  1. **Parallelism benchmarks** - Testing BGZF parallel decompression performance with configurable thread counts
  2. **Predicate pushdown benchmarks** - Testing filter optimization efficiency with configurable SQL queries
  3. **Projection pushdown benchmarks** - Testing column pruning optimization with configurable SQL queries
- **YAML configuration files** for each format specifying:
  - Test data files on Google Drive (URLs, checksums)
  - SQL queries for each benchmark category
  - Repetition counts and thread configurations
  - Format-specific table registration parameters
- Create GitHub Actions workflow for automated benchmark execution on Linux and macOS
- Generate interactive HTML comparison reports with dropdown switches for baseline/target and OS selection
- Store benchmark history for tagged releases in GitHub Pages
- Initial configuration for GFF3 format using gencode.49 test data from Google Drive
- **Zero-code extensibility**: Adding new formats requires only adding a YAML configuration file
- Publish results to https://biodatageeks.github.io/datafusion-bio-formats/benchmark/

## Impact

### Affected Specs
- **NEW**: `benchmark-framework` - Complete benchmark system specification
- **MODIFIED**: `ci-cd` - New benchmark workflow addition

### Affected Code
- `benchmarks/` - Already contains common infrastructure; will add:
  - `benchmarks/runner/` - Generic benchmark runner binary
  - `benchmarks/configs/` - YAML configuration files for each format
    - `benchmarks/configs/gff.yml` - GFF3 benchmark configuration
    - (Future: vcf.yml, fastq.yml, bam.yml, etc.)
  - `benchmarks/python/` - HTML report generation scripts
  - GitHub workflow: `.github/workflows/benchmark.yml`
- Infrastructure already partially exists:
  - `benchmarks/common/` - Harness and data downloader (already implemented)
  - Benchmark categories enum already defined (Parallelism, PredicatePushdown, ProjectionPushdown)

### Breaking Changes
None - This is a purely additive change

### Dependencies
- Python 3.x for report generation scripts
- Additional Python packages: plotly, pandas, jinja2
- YAML parsing: serde_yaml (Rust crate)
- GitHub Pages enabled for result publishing
