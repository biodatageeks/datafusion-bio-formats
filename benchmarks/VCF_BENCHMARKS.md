# VCF Benchmarks - Local Setup Guide

VCF benchmarks are configured for **local use only** due to large file sizes. They are not included in CI/CD workflows.

## Quick Start

### 1. Download VCF Test Files

Run the download script to get real VCF files from 1000 Genomes project:

```bash
./benchmarks/scripts/download_vcf_benchmark_files.sh [output_dir]
# Default output: /tmp
```

This downloads 4 different sizes:
- **SMALL** (~50KB): Y chromosome trio - quick tests
- **SMALL-MEDIUM** (~800KB): CEU trio sites
- **MEDIUM** (~2-3MB): CEU trio with genotypes
- **LARGE** (~200-300MB): Chromosome 22, full Phase 3

### 2. Update Configuration

Edit `benchmarks/configs/vcf_local.yml` and update the file paths:

```yaml
test_data:
  - filename: /tmp/medium_ceu_genotypes.vcf.gz  # Update this path
    drive_url: https://local-file  # Ignored for local files
    checksum: null
```

### 3. Run Benchmarks

```bash
# Build the benchmark runner
cargo build --release --package datafusion-bio-benchmarks-runner

# Run VCF benchmarks
./target/release/benchmark-runner benchmarks/configs/vcf_local.yml --output-dir results
```

## Benchmark Categories

### Parallelism Tests
Measures BGZF parallel decompression speedup with different thread counts (1, 2, 4, 8).

### Predicate Pushdown Tests
Tests filter optimization efficiency:
- Chromosome filtering
- Position range filtering
- Quality score filtering

### Projection Pushdown Tests
Tests column pruning optimization:
- Full schema (all columns)
- Core fields only
- Single column

## File Size Recommendations

- **Quick tests**: Use `small_trio_ychr.vcf.gz` (~50KB)
- **Standard benchmarks**: Use `medium_ceu_genotypes.vcf.gz` (~2-3MB)
- **Comprehensive benchmarks**: Use `large_chr22_phase3.vcf.gz` (~200-300MB)

## Notes

- VCF benchmarks are excluded from CI/CD due to file size constraints
- Local benchmarks provide meaningful performance comparisons
- Results can be compared manually between different code versions
- For CI/CD, use GFF benchmarks which have smaller test files

