# Benchmark Framework Design

## Context

The datafusion-bio-formats project needs systematic performance tracking to ensure optimizations deliver measurable improvements and prevent regressions. This design is inspired by the polars-bio benchmark system, which successfully provides interactive performance comparisons across releases and platforms.

Key stakeholders:
- Contributors need to validate optimization PRs against baseline performance
- Users need visibility into performance characteristics and improvements
- Maintainers need to prevent performance regressions across releases

Constraints:
- Must work with large genomic test files (multi-GB) stored on Google Drive
- Must support cross-platform comparison (Linux, macOS, potentially Windows)
- Must provide historical tracking without bloating the main repository
- Must be extensible to all supported formats (GFF, VCF, FASTQ, BAM, BED, FASTA, CRAM)

## Goals / Non-Goals

### Goals
- Automated benchmark execution on PRs and releases via GitHub Actions
- Interactive HTML reports comparing baseline vs target performance
- Support for three optimization categories: parallelism, predicate pushdown, projection pushdown
- Cross-platform results (Linux and macOS runners)
- Historical benchmark data storage in GitHub Pages
- Easy extensibility to new file formats
- Reusable benchmark harness and data management utilities

### Non-Goals
- Real-time performance monitoring or profiling
- Micro-benchmarks of individual functions (use Criterion for that)
- Benchmarking compression algorithms themselves (focus on DataFusion integration)
- Windows support in initial implementation (can be added later)
- Automatic performance regression blocking (alerts only, human review required)

## Decisions

### Architecture: Rust Benchmark Binaries + Python Reporting

**Decision**: Use Rust binaries for benchmark execution and Python for report generation.

**Rationale**:
- Rust binaries ensure accurate performance measurement without interpreter overhead
- Python ecosystem excels at data visualization (Plotly) and HTML generation
- Matches polars-bio's proven architecture
- Separates concerns: performance measurement vs. result presentation

**Alternatives considered**:
- Pure Rust with charting crates (plotters, polars): Less mature interactive charting, harder HTML generation
- Pure Python with subprocess calls: Adds Python overhead to measurements, less accurate
- JavaScript-based reporting: Requires Node.js dependency, more complex build

### Configuration-Driven Architecture: YAML Configuration Files

**Decision**: Use a single generic benchmark runner with YAML configuration files for each format, instead of format-specific binaries.

**Rationale**:
- **Zero-code extensibility**: Adding a new format requires only creating a YAML config file
- **Consistency**: All formats follow the same test patterns and structure
- **Maintainability**: Single codebase for the runner, easier to fix bugs and add features
- **Declarative**: YAML makes it easy to see what's being tested without reading code
- **Flexibility**: Non-developers can add new test queries by editing YAML
- **Reduces duplication**: Common logic (table registration, query execution, result recording) is shared

**Configuration Structure**:
Each format has a YAML file (`benchmarks/configs/{format}.yml`) specifying:
```yaml
format: gff
table_name: gencode_annotations
test_data:
  - filename: gencode.v49.annotation.gff3.gz
    drive_url: https://drive.google.com/file/d/1PsHqKG-gyRJy5-sNzuH3xRntw4Er--Si/view
    checksum: <sha256>
  - filename: gencode.v49.annotation.gff3.gz.tbi
    drive_url: https://drive.google.com/file/d/173RT5Afi2jAh64uCJwNRGHF4ozYU-xzX/view
    checksum: <sha256>

parallelism_tests:
  thread_counts: [1, 2, 4, 8, max]
  repetitions: 3
  query: "SELECT COUNT(*) FROM {table_name}"

predicate_pushdown_tests:
  repetitions: 3
  tests:
    - name: chromosome_filter
      query: "SELECT COUNT(*) FROM {table_name} WHERE seqid = 'chr1'"
    - name: range_filter
      query: "SELECT * FROM {table_name} WHERE start > 1000000 AND end < 2000000"
    - name: type_filter
      query: "SELECT * FROM {table_name} WHERE type = 'gene'"

projection_pushdown_tests:
  repetitions: 3
  tests:
    - name: full_schema
      query: "SELECT * FROM {table_name} LIMIT 100000"
    - name: core_fields
      query: "SELECT seqid, start, end, type FROM {table_name} LIMIT 100000"
    - name: single_column
      query: "SELECT type FROM {table_name} LIMIT 100000"
```

**Generic Runner Flow**:
1. Load YAML configuration for specified format
2. Download and cache test data files from Google Drive
3. Register table using format-specific DataFusion table provider
4. Execute parallelism tests with configured thread counts
5. Execute predicate pushdown tests with configured queries
6. Execute projection pushdown tests with configured queries
7. Record results in standardized JSON format

**Alternatives considered**:
- Format-specific binaries (e.g., `benchmarks/gff/`, `benchmarks/vcf/`): More code duplication, harder to maintain, requires Rust knowledge to add formats
- JSON configuration: Less human-readable than YAML, more verbose
- TOML configuration: Good alternative, but YAML is more common for CI/CD configs
- Embedded configuration in code: Harder to modify, requires recompilation

### Test Data: Google Drive with Local Caching

**Decision**: Store large test files on Google Drive, download and cache locally during benchmarks.

**Rationale**:
- Keeps repository size minimal (no multi-GB files in Git)
- Google Drive provides reliable hosting with good download speeds
- Local caching prevents redundant downloads
- SHA-256 checksums ensure data integrity
- Already implemented in `benchmarks/common/data_downloader.rs`

**Test Data for GFF3**:
- File: gencode.49 (compressed GFF + index)
- GFF URL: https://drive.google.com/file/d/1PsHqKG-gyRJy5-sNzuH3xRntw4Er--Si/view?usp=drive_link
- Index URL: https://drive.google.com/file/d/173RT5Afi2jAh64uCJwNRGHF4ozYU-xzX/view?usp=drive_link

### Benchmark Categories: Three Core Optimizations

**Decision**: Implement three benchmark categories per format:

1. **Parallelism**: Measure speedup from BGZF parallel decompression
   - Test with varying thread counts (1, 2, 4, 8, max)
   - Compare against single-threaded baseline
   - Measure throughput (records/sec) and speedup factor

2. **Predicate Pushdown**: Measure filter optimization efficiency
   - Test common query patterns (range filters, equality filters)
   - Compare full scan vs. pushdown-optimized queries
   - Measure rows scanned vs. rows returned ratio

3. **Projection Pushdown**: Measure column pruning efficiency
   - Test queries selecting different column subsets
   - Compare full schema read vs. projected reads
   - Measure I/O reduction and parse time savings

**Rationale**:
- These are the three primary optimization vectors in datafusion-bio-formats
- Matches the actual optimization work done in the codebase
- Provides actionable metrics for contributors
- Easy to explain and understand

### GitHub Actions Workflow: Matrix Strategy

**Decision**: Use job matrix for parallel benchmark execution across platforms.

**Workflow structure**:
```yaml
jobs:
  prepare:
    - Determine baseline tag (from input or latest)
    - Determine target ref (PR branch or master)
    - Build runner matrix (linux, macos)

  benchmark:
    - Matrix: [linux, macos]
    - Run baseline benchmarks (from crates.io or tagged release)
    - Run target benchmarks (from current branch)
    - Upload JSON results as artifacts

  aggregate:
    - Download all artifacts
    - Generate comparison HTML reports
    - Publish to GitHub Pages
    - Comment on PR with results link
```

**Rationale**:
- Parallel execution reduces total workflow time
- Matrix strategy easily extends to additional platforms
- Artifact-based communication decouples execution from reporting
- Follows GitHub Actions best practices

**Alternatives considered**:
- Sequential execution: Too slow for multiple platforms
- Separate workflows per platform: Harder to coordinate and aggregate
- Single-platform only: Doesn't catch platform-specific regressions

### Result Storage: GitHub Pages with Structured Layout

**Decision**: Store benchmark results in GitHub Pages with structured directory layout.

**Layout**:
```
gh-pages/
  benchmark/
    index.html                    # Latest results and navigation
    comparison.html               # Interactive comparison tool
    data/
      index.json                  # Master index of all datasets
      tags/
        v0.1.0/
          linux.json              # Benchmark results
          macos.json
        v0.1.1/
          linux.json
          macos.json
      commits/
        {sha}/
          linux.json
          macos.json
```

**Rationale**:
- Structured paths enable easy historical queries
- JSON format supports programmatic access
- Separate tags from commits prevents clutter
- Master index enables efficient lookups
- Matches polars-bio proven structure

### Report Generation: Python Script with Plotly

**Decision**: Generate interactive HTML with Python using Plotly and embedded JSON data.

**Implementation based on polars-bio's `generate_interactive_comparison.py`**:
- Load master index to populate dropdown menus
- Embed all benchmark data as JSON in HTML
- Use Plotly.js for interactive charts
- Support dynamic baseline/target switching
- Support platform switching (Linux/macOS tabs)

**Chart types**:
- Grouped bar charts for total runtime comparison
- Per-test-case breakdown bars
- Speedup ratio displays
- Color-coded baseline vs. target

**Rationale**:
- Plotly provides professional, interactive visualizations
- Embedded JSON eliminates need for separate data fetching
- Single-file HTML is easy to host and share
- Dropdown switches provide flexible comparison options

### Extensibility: YAML Configuration Files

**Decision**: Add new file formats by creating YAML configuration files only, no code changes required.

**Pattern for adding new format**:
1. Create `benchmarks/configs/{format}.yml`
2. Specify test data sources (Google Drive URLs)
3. Define SQL queries for each benchmark category
4. Run: `cargo run --bin benchmark-runner -- --config configs/{format}.yml`

**Example for adding VCF format** (`benchmarks/configs/vcf.yml`):
```yaml
format: vcf
table_name: variants
test_data:
  - filename: homo_sapiens.vcf.gz
    drive_url: https://drive.google.com/file/d/XXXXX/view
    checksum: abc123...
  - filename: homo_sapiens.vcf.gz.tbi
    drive_url: https://drive.google.com/file/d/YYYYY/view
    checksum: def456...

parallelism_tests:
  thread_counts: [1, 2, 4, 8, max]
  repetitions: 3
  query: "SELECT COUNT(*) FROM {table_name}"

predicate_pushdown_tests:
  repetitions: 3
  tests:
    - name: chromosome_filter
      query: "SELECT COUNT(*) FROM {table_name} WHERE chrom = '1'"
    - name: quality_filter
      query: "SELECT * FROM {table_name} WHERE qual > 30"

projection_pushdown_tests:
  repetitions: 3
  tests:
    - name: full_schema
      query: "SELECT * FROM {table_name} LIMIT 100000"
    - name: position_only
      query: "SELECT chrom, pos FROM {table_name} LIMIT 100000"
```

**Rationale**:
- **Zero code changes**: Adding VCF, FASTQ, BAM, etc. requires only YAML file
- **Non-developer friendly**: SQL and YAML don't require Rust knowledge
- **Version controlled**: Configuration changes tracked in Git
- **Easy testing**: Can test new queries locally by editing YAML
- **Reduces maintenance**: Bug fixes in runner benefit all formats
- **Consistency**: All formats use identical benchmark structure

## Risks / Trade-offs

### Risk: Google Drive Download Reliability
**Mitigation**:
- Implement retry logic with exponential backoff
- Support fallback to direct HTTP URLs if provided
- Cache downloads to minimize re-download frequency
- Add checksum validation to detect corruption

### Risk: Platform-Specific Performance Variance
**Impact**: Results may vary significantly between GitHub Actions runners
**Mitigation**:
- Always compare within same platform (Linux vs Linux, macOS vs macOS)
- Include system info (CPU, memory) in results metadata
- Use consistent runner types (ubuntu-22.04, macos-latest)
- Document expected variance ranges

### Risk: Long Benchmark Execution Times
**Impact**: Slow CI feedback on PRs
**Mitigation**:
- Implement "fast" and "full" benchmark modes
- Default to fast mode on PRs (subset of test cases)
- Run full benchmarks only on release tags
- Use workflow_dispatch for on-demand full runs

### Risk: GitHub Pages Size Growth
**Impact**: Historical data accumulates over time
**Mitigation**:
- Store only summary statistics, not raw data
- Implement data retention policy (keep last N versions)
- Use compressed JSON format
- Provide cleanup script for old data

### Trade-off: Accuracy vs Speed
- Running more iterations increases accuracy but slows benchmarks
- Decision: Use 3 iterations for PRs, 10 for releases
- Document variance expectations in results

### Trade-off: Baseline Selection
- Latest tag vs. specific version vs. master
- Decision: Default to latest tag, allow manual override
- Enables comparing against stable releases by default

## Migration Plan

### Phase 1: GFF3 Implementation (Initial Release)
1. Implement GFF3 benchmarks in `benchmarks/gff/`
2. Create Python report generation script
3. Set up GitHub Actions workflow
4. Configure GitHub Pages
5. Publish initial benchmark results

### Phase 2: Additional Formats (Incremental)
1. Add VCF configuration (`benchmarks/configs/vcf.yml`)
2. Add FASTQ configuration (`benchmarks/configs/fastq.yml`)
3. Add BAM configuration (`benchmarks/configs/bam.yml`)
4. Add remaining formats (BED, FASTA, CRAM) as YAML configs

### Rollback Plan
- Benchmark infrastructure is additive only
- Can disable workflow by commenting out workflow file
- Can delete gh-pages branch to remove published results
- No impact on main codebase functionality

## Open Questions

### Q1: Benchmark Frequency
**Question**: How often should benchmarks run automatically?
**Options**:
- On every PR commit (expensive, slow feedback)
- On PR ready-for-review (good balance)
- Only on release tags (minimal cost, less visibility)
**Recommendation**: On workflow_dispatch (manual trigger) and release tags, with option for PR authors to manually trigger

### Q2: Performance Regression Thresholds
**Question**: What performance degradation should trigger alerts?
**Options**:
- Fixed threshold (e.g., 10% slower)
- Statistical analysis (e.g., 2 standard deviations)
- Manual review only (no automatic alerts)
**Recommendation**: Start with manual review, add configurable threshold alerts in Phase 2

### Q3: Benchmark Data Versioning
**Question**: How to handle test data updates?
**Options**:
- Fixed dataset forever (ensures comparability)
- Allow dataset updates (tests realistic scenarios)
- Version datasets separately (complex but flexible)
**Recommendation**: Start with fixed gencode.49, version separately if needed later

### Q4: Comparison Granularity
**Question**: Should benchmarks compare individual operations or aggregated metrics?
**Options**:
- Per-operation detail (detailed but noisy)
- Aggregated categories (cleaner but less insight)
- Both (best of both worlds, more complex)
**Recommendation**: Both - aggregate view by default, drill-down available

## Implementation Notes

### Generic Benchmark Runner Structure
Single binary in `benchmarks/runner/src/main.rs` that loads YAML configs:
```rust
use datafusion_bio_benchmarks_common::*;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct BenchmarkConfig {
    format: String,
    table_name: String,
    test_data: Vec<TestDataConfig>,
    parallelism_tests: ParallelismConfig,
    predicate_pushdown_tests: PredicateConfig,
    projection_pushdown_tests: ProjectionConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args().nth(1)
        .expect("Usage: benchmark-runner <config.yml>");

    // Load YAML configuration
    let config: BenchmarkConfig = serde_yaml::from_str(
        &std::fs::read_to_string(config_path)?
    )?;

    // Download test data
    let downloader = DataDownloader::new()?;
    for data_file in &config.test_data {
        downloader.download(&data_file.into(), false)?;
    }

    // Register table using format-specific provider
    let ctx = SessionContext::new();
    register_table(&ctx, &config.format, &config.table_name, &config.test_data).await?;

    // Run benchmark categories using queries from config
    run_parallelism_benchmarks(&ctx, &config.parallelism_tests).await?;
    run_predicate_benchmarks(&ctx, &config.predicate_pushdown_tests).await?;
    run_projection_benchmarks(&ctx, &config.projection_pushdown_tests).await?;

    Ok(())
}
```

### Python Report Script Requirements
- Input: Multiple JSON result files from different runners/platforms
- Output: Single HTML file with embedded data and Plotly charts
- Features:
  - Dropdown menus for baseline/target selection
  - Platform tabs for Linux/macOS switching
  - Grouped bar charts with hover tooltips
  - Speedup/regression indicators
  - Direct comparison mode

### GitHub Actions Workflow Configuration
```yaml
name: Benchmark
on:
  workflow_dispatch:
    inputs:
      runner:
        type: choice
        options: [all, linux, macos]
      benchmark_suite:
        type: choice
        options: [fast, full]
      baseline_tag:
        type: string
        description: 'Baseline tag (leave empty for latest)'
```

### Result JSON Schema
```json
{
  "benchmark_name": "gff_parallelism_8threads",
  "format": "gff",
  "category": "parallelism",
  "timestamp": "2025-11-03T10:30:00Z",
  "system_info": {
    "os": "Linux 5.15.0",
    "cpu_model": "Intel Xeon",
    "cpu_cores": 8,
    "total_memory_gb": 32.0
  },
  "configuration": {
    "threads": 8,
    "test_file": "gencode.v49.annotation.gff3.gz"
  },
  "metrics": {
    "throughput_records_per_sec": 125000.0,
    "elapsed_seconds": 45.2,
    "total_records": 5650000,
    "speedup_vs_baseline": 6.8,
    "peak_memory_mb": 512
  }
}
```
