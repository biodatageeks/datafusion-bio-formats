# DataFusion Bio-Formats Benchmark Framework

A configuration-driven benchmark framework for measuring performance across different bioinformatics file formats.

## Overview

This benchmark framework provides:

- **Generic Runner**: Single binary that works with any file format via YAML configuration
- **Three Benchmark Categories**:
  - **Parallelism**: Measures BGZF parallel decompression speedup
  - **Predicate Pushdown**: Measures filter optimization efficiency
  - **Projection Pushdown**: Measures column pruning benefits
- **Zero-Code Extensibility**: Add new formats by creating YAML configuration files only
- **Automated CI/CD**: GitHub Actions workflow for continuous benchmarking
- **Interactive Reports**: HTML comparison reports with Plotly charts

## Quick Start

### Run Benchmarks Locally

```bash
# Build the benchmark runner
cargo build --release --package datafusion-bio-benchmarks-runner

# Run GFF benchmarks
./target/release/benchmark-runner benchmarks/configs/gff.yml

# Specify output directory
./target/release/benchmark-runner benchmarks/configs/gff.yml --output-dir my_results
```

### View Results

Results are saved as JSON files in the output directory:

```
benchmark_results/
└── gff/
    ├── gff_parallelism_1threads_20250103_143052.json
    ├── gff_parallelism_2threads_20250103_143055.json
    ├── gff_predicate_chromosome_filter_20250103_143100.json
    └── ...
```

## Adding a New File Format

Adding benchmarks for a new format requires only creating a YAML configuration file:

### 1. Copy the Template

```bash
cp benchmarks/configs/TEMPLATE.yml benchmarks/configs/vcf.yml
```

### 2. Configure the Format

Edit `vcf.yml`:

```yaml
format: vcf
table_name: variants

test_data:
  - filename: homo_sapiens.vcf.gz
    drive_url: https://drive.google.com/file/d/YOUR_FILE_ID/view
    checksum: null  # Optional SHA-256

parallelism_tests:
  thread_counts: [1, 2, 4, 8, max]
  repetitions: 3
  query: "SELECT COUNT(*) FROM {table_name}"

predicate_pushdown_tests:
  repetitions: 3
  tests:
    - name: chromosome_filter
      query: "SELECT * FROM {table_name} WHERE chrom = '1'"
    - name: quality_filter
      query: "SELECT * FROM {table_name} WHERE qual > 30"

projection_pushdown_tests:
  repetitions: 3
  tests:
    - name: full_schema
      query: "SELECT * FROM {table_name} LIMIT 100000"
    - name: positions_only
      query: "SELECT chrom, pos FROM {table_name} LIMIT 100000"
```

### 3. Run the Benchmarks

```bash
./target/release/benchmark-runner benchmarks/configs/vcf.yml
```

That's it! No code changes required.

## Configuration Reference

### Top-Level Fields

- `format` (string): Format name (gff, vcf, fastq, bam, bed, fasta, cram)
- `table_name` (string): Name to use when registering the table in DataFusion
- `test_data` (array): List of test data files
- `parallelism_tests` (object): Parallelism benchmark configuration
- `predicate_pushdown_tests` (object): Predicate pushdown configuration
- `projection_pushdown_tests` (object): Projection pushdown configuration

### Test Data Configuration

```yaml
test_data:
  - filename: local_cache_name.gz
    drive_url: https://drive.google.com/file/d/FILE_ID/view
    checksum: sha256_hash  # Optional
```

Files are downloaded from Google Drive and cached locally. Include checksums for validation.

### Parallelism Tests

```yaml
parallelism_tests:
  thread_counts: [1, 2, 4, 8, max]  # "max" uses all CPU cores
  repetitions: 3
  query: "SELECT COUNT(*) FROM {table_name}"
```

Tests the query with different thread counts to measure parallel speedup.

### Predicate Pushdown Tests

```yaml
predicate_pushdown_tests:
  repetitions: 3
  tests:
    - name: test_name
      query: "SELECT * FROM {table_name} WHERE condition"
```

Each test measures how efficiently filters are pushed down to reduce data scanning.

### Projection Pushdown Tests

```yaml
projection_pushdown_tests:
  repetitions: 3
  tests:
    - name: test_name
      query: "SELECT columns FROM {table_name} LIMIT N"
```

Each test measures I/O and parse time reduction from column pruning.

### Placeholders

Use `{table_name}` in queries, which will be replaced with the configured table name.

## GitHub Actions Workflow

The benchmark system uses **two separate workflows** following polars-bio's architecture:

### 1. Benchmark Workflow (`benchmark.yml`)

**Purpose**: Execute benchmarks and store raw JSON results

**Triggers**:
- Manual: Actions → Benchmark → Run workflow
- Automatic: On release tags (e.g., `v0.1.2`)

**What it does**:
1. Runs benchmarks for baseline (latest tag) and target (PR/branch)
2. Stores raw JSON results in `gh-pages` branch under `benchmark-data/`
3. No report generation (separation of concerns)

**Options**:
- **Runner**: `all`, `linux`, or `macos`
- **Suite**: `fast` (3 reps) or `full` (10 reps)
- **Baseline**: Tag to compare against (defaults to latest)
- **Target**: Branch to benchmark (defaults to current)

### 2. Pages Workflow (`pages.yml`)

**Purpose**: Generate HTML reports from stored benchmark data

**Triggers**:
- Automatic: When benchmark data is pushed to `gh-pages`
- Manual: workflow_dispatch

**What it does**:
1. Scans `benchmark-data/` for all available results
2. Generates interactive comparison HTML
3. Deploys to GitHub Pages

### View Results

**Landing Page**: https://biodatageeks.org/datafusion-bio-formats/benchmark-comparison/

**Interactive Comparison**: https://biodatageeks.org/datafusion-bio-formats/benchmark-comparison/index.html

**Raw Data**: https://biodatageeks.org/datafusion-bio-formats/benchmark-data/

## Directory Structure

### Source Code (main branch)

```
benchmarks/
├── common/                # Shared benchmark infrastructure
│   ├── src/
│   │   ├── harness.rs    # Result recording and metrics
│   │   └── data_downloader.rs  # Google Drive download
│   └── Cargo.toml
├── runner/                # Generic benchmark runner
│   ├── src/
│   │   └── main.rs       # Main runner logic
│   └── Cargo.toml
├── configs/               # YAML configurations
│   ├── TEMPLATE.yml      # Template for new formats
│   └── gff.yml           # GFF3 configuration
├── python/                # Report generation scripts
│   ├── generate_interactive_comparison.py
│   └── requirements.txt
└── README.md
```

### GitHub Pages (gh-pages branch)

```
benchmark-data/            # Raw benchmark results
├── index.json            # Master index of all datasets
├── tags/
│   └── v0.1.0/
│       ├── benchmark-info.json  # Run metadata
│       ├── linux/
│       │   ├── baseline/results/*.json
│       │   ├── target/results/*.json
│       │   └── linux.json       # Platform metadata
│       └── macos/
│           ├── baseline/results/*.json
│           ├── target/results/*.json
│           └── macos.json
└── commits/
    └── {short_sha}/
        └── {platform}/...

benchmark-comparison/      # Generated HTML reports
├── landing.html          # Dashboard
├── index.html            # Interactive comparison tool
└── {branch}/             # Per-branch reports (future)
```

## Result JSON Schema

Each benchmark produces a JSON result file:

```json
{
  "benchmark_name": "gff_parallelism_4threads",
  "format": "gff",
  "category": "parallelism",
  "timestamp": "2025-01-03T14:30:52Z",
  "system_info": {
    "os": "Linux 5.15.0",
    "cpu_model": "Intel Xeon",
    "cpu_cores": 8,
    "total_memory_gb": 32.0
  },
  "configuration": {
    "threads": 4,
    "repetitions": 3
  },
  "metrics": {
    "throughput_records_per_sec": 125000.0,
    "elapsed_seconds": 45.2,
    "total_records": 5650000,
    "speedup_vs_baseline": 3.8,
    "peak_memory_mb": null
  }
}
```

## Calculating Checksums

To calculate checksums for test files:

```bash
# macOS
shasum -a 256 file.gz

# Linux
sha256sum file.gz
```

Add the checksum to your YAML configuration for validation.

## Troubleshooting

### Google Drive Download Issues

If downloads fail:

1. Verify the file ID is correct (from the sharing URL)
2. Ensure the file is publicly accessible or shared appropriately
3. Check for "virus scan warning" on large files (handled automatically)

### Table Registration Errors

Ensure the format name matches one of the supported formats:
- gff, vcf, fastq, bam, bed, fasta, cram

Format names are case-insensitive.

### Out of Memory

For large datasets:
- Reduce `LIMIT` values in projection tests
- Use smaller test files
- Increase available memory

## Contributing

To add support for a new file format:

1. Create YAML configuration in `benchmarks/configs/`
2. Identify appropriate test data (preferably on Google Drive)
3. Define meaningful test queries for your format
4. Test locally
5. Submit PR with the configuration

No Rust code changes needed!

## Example: Complete VCF Configuration

```yaml
format: vcf
table_name: variants

test_data:
  - filename: homo_sapiens_chr1.vcf.gz
    drive_url: https://drive.google.com/file/d/1A2B3C4D5E6F7G8H/view
    checksum: abcdef1234567890...
  - filename: homo_sapiens_chr1.vcf.gz.tbi
    drive_url: https://drive.google.com/file/d/9H8G7F6E5D4C3B2A/view
    checksum: 0987654321fedcba...

parallelism_tests:
  thread_counts: [1, 2, 4, 8, max]
  repetitions: 3
  query: "SELECT COUNT(*) FROM {table_name}"

predicate_pushdown_tests:
  repetitions: 3
  tests:
    - name: chrom_filter
      query: "SELECT * FROM {table_name} WHERE chrom = '1'"
    - name: position_range
      query: "SELECT * FROM {table_name} WHERE pos >= 1000000 AND pos <= 2000000"
    - name: quality_threshold
      query: "SELECT * FROM {table_name} WHERE qual > 30"
    - name: combined_filter
      query: "SELECT * FROM {table_name} WHERE chrom = '1' AND qual > 30"

projection_pushdown_tests:
  repetitions: 3
  tests:
    - name: full_schema
      query: "SELECT * FROM {table_name} LIMIT 100000"
    - name: core_fields
      query: "SELECT chrom, pos, ref, alt FROM {table_name} LIMIT 100000"
    - name: positions_only
      query: "SELECT chrom, pos FROM {table_name} LIMIT 100000"
    - name: single_column
      query: "SELECT chrom FROM {table_name} LIMIT 100000"
```

## License

Same as datafusion-bio-formats project.
