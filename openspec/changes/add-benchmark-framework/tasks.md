# Implementation Tasks

## 1. Generic Benchmark Runner Implementation

### 1.1 Create Benchmark Runner Binary
- [ ] 1.1.1 Create `benchmarks/runner/Cargo.toml` with dependencies:
  - datafusion-bio-benchmarks-common
  - datafusion (with all format table providers)
  - serde, serde_yaml
  - tokio, anyhow
- [ ] 1.1.2 Create `benchmarks/runner/src/main.rs` with CLI argument parsing
- [ ] 1.1.3 Implement YAML configuration loading with serde_yaml
- [ ] 1.1.4 Define configuration structs matching YAML schema
- [ ] 1.1.5 Add configuration validation (required fields, positive numbers, etc.)

### 1.2 Implement Configuration Structures
- [ ] 1.2.1 Create `BenchmarkConfig` struct with format, table_name, test_data
- [ ] 1.2.2 Create `TestDataConfig` struct with filename, drive_url, checksum
- [ ] 1.2.3 Create `ParallelismConfig` struct with thread_counts, repetitions, query
- [ ] 1.2.4 Create `PredicateConfig` struct with repetitions and list of test cases
- [ ] 1.2.5 Create `ProjectionConfig` struct with repetitions and list of test cases
- [ ] 1.2.6 Implement Deserialize traits for all config structs

### 1.3 Implement Generic Table Registration
- [ ] 1.3.1 Create `register_table()` function that accepts format name
- [ ] 1.3.2 Match on format name to determine table provider type
- [ ] 1.3.3 Support format names: gff, vcf, fastq, bam, bed, fasta, cram
- [ ] 1.3.4 Register table in DataFusion SessionContext with configured name
- [ ] 1.3.5 Handle errors for unsupported formats with clear messages

### 1.4 Implement Generic Parallelism Benchmarks
- [ ] 1.4.1 Create `run_parallelism_benchmarks()` accepting SessionContext and config
- [ ] 1.4.2 Iterate through configured thread counts (handle "max" special value)
- [ ] 1.4.3 Set tokio runtime thread count for each configuration
- [ ] 1.4.4 Execute configured SQL query (replace {table_name} placeholder)
- [ ] 1.4.5 Measure throughput and elapsed time for configured repetitions
- [ ] 1.4.6 Calculate speedup ratios vs single-threaded baseline
- [ ] 1.4.7 Record results using `BenchmarkResultBuilder`

### 1.5 Implement Generic Predicate Pushdown Benchmarks
- [ ] 1.5.1 Create `run_predicate_benchmarks()` accepting SessionContext and config
- [ ] 1.5.2 Iterate through configured test cases
- [ ] 1.5.3 Execute each SQL query (replace {table_name} placeholder)
- [ ] 1.5.4 Measure execution time for configured repetitions
- [ ] 1.5.5 Extract rows scanned vs rows returned metrics from DataFusion
- [ ] 1.5.6 Record results for each named test case

### 1.6 Implement Generic Projection Pushdown Benchmarks
- [ ] 1.6.1 Create `run_projection_benchmarks()` accepting SessionContext and config
- [ ] 1.6.2 Iterate through configured test cases
- [ ] 1.6.3 Execute each SQL query (replace {table_name} placeholder)
- [ ] 1.6.4 Measure parse time and I/O for configured repetitions
- [ ] 1.6.5 Calculate I/O reduction percentages between projections
- [ ] 1.6.6 Record results for each named test case

### 1.7 Create GFF3 YAML Configuration
- [ ] 1.7.1 Create `benchmarks/configs/gff.yml`
- [ ] 1.7.2 Configure format: gff, table_name: gencode_annotations
- [ ] 1.7.3 Configure test data with Google Drive URLs:
  - GFF: https://drive.google.com/file/d/1PsHqKG-gyRJy5-sNzuH3xRntw4Er--Si/view
  - Index: https://drive.google.com/file/d/173RT5Afi2jAh64uCJwNRGHF4ozYU-xzX/view
- [ ] 1.7.4 Calculate and add SHA-256 checksums for both files
- [ ] 1.7.5 Configure parallelism tests with thread_counts [1, 2, 4, 8, max]
- [ ] 1.7.6 Configure predicate tests with queries:
  - chromosome_filter: `WHERE seqid = 'chr1'`
  - range_filter: `WHERE start > 1000000 AND end < 2000000`
  - type_filter: `WHERE type = 'gene'`
- [ ] 1.7.7 Configure projection tests with queries:
  - full_schema: `SELECT * FROM {table_name} LIMIT 100000`
  - core_fields: `SELECT seqid, start, end, type FROM {table_name} LIMIT 100000`
  - single_column: `SELECT type FROM {table_name} LIMIT 100000`

### 1.8 Test Benchmark Runner Locally
- [ ] 1.8.1 Build runner: `cargo build --release --package datafusion-bio-benchmarks-runner`
- [ ] 1.8.2 Run with GFF config: `./target/release/benchmark-runner benchmarks/configs/gff.yml`
- [ ] 1.8.3 Verify test data downloads correctly from Google Drive
- [ ] 1.8.4 Verify all three benchmark categories execute successfully
- [ ] 1.8.5 Inspect generated JSON result files for correctness
- [ ] 1.8.6 Validate JSON schema compliance
- [ ] 1.8.7 Test with invalid YAML to verify error handling

## 2. Python Report Generation

### 2.1 Create Report Generation Script
- [ ] 2.1.1 Create `benchmarks/python/generate_interactive_comparison.py`
- [ ] 2.1.2 Add dependencies to `benchmarks/python/requirements.txt`:
  - plotly
  - pandas
  - jinja2 (if needed for templating)
- [ ] 2.1.3 Implement `load_index()` to read master index JSON
- [ ] 2.1.4 Implement `parse_json_results()` to load benchmark JSON files
- [ ] 2.1.5 Implement `extract_operation_info()` for categorizing results

### 2.2 Implement Chart Generation
- [ ] 2.2.1 Create `generate_comparison_charts()` function
- [ ] 2.2.2 Generate grouped bar charts for baseline vs target
- [ ] 2.2.3 Create per-category breakdown charts (parallelism, predicate, projection)
- [ ] 2.2.4 Add color coding (green for improvement, red for regression)
- [ ] 2.2.5 Configure hover tooltips with detailed metrics
- [ ] 2.2.6 Support responsive chart sizing

### 2.3 Implement Interactive HTML Generation
- [ ] 2.3.1 Create `generate_html_template()` function
- [ ] 2.3.2 Embed JSON data directly in HTML
- [ ] 2.3.3 Add dropdown menus for baseline/target selection
- [ ] 2.3.4 Add platform tabs (Linux/macOS switching)
- [ ] 2.3.5 Add Plotly.js for client-side interactivity
- [ ] 2.3.6 Add validation for valid comparison pairs
- [ ] 2.3.7 Generate single standalone HTML file

### 2.4 Test Report Generation Locally
- [ ] 2.4.1 Create sample benchmark JSON results for testing
- [ ] 2.4.2 Create sample master index JSON
- [ ] 2.4.3 Run script: `python generate_interactive_comparison.py`
- [ ] 2.4.4 Verify HTML report opens in browser
- [ ] 2.4.5 Test dropdown functionality for baseline/target switching
- [ ] 2.4.6 Test platform tab switching
- [ ] 2.4.7 Verify charts render correctly with sample data

## 3. GitHub Actions Workflow

### 3.1 Create Benchmark Workflow File
- [ ] 3.1.1 Create `.github/workflows/benchmark.yml`
- [ ] 3.1.2 Configure workflow triggers:
  - `workflow_dispatch` with inputs (runner, suite, baseline_tag)
  - `push` with tag filter (tags matching `v*.*.*`)
- [ ] 3.1.3 Define workflow permissions for GitHub Pages deployment

### 3.2 Implement Prepare Job
- [ ] 3.2.1 Create `prepare` job to determine configuration
- [ ] 3.2.2 Determine baseline tag (from input or latest tag)
- [ ] 3.2.3 Determine target ref (current branch/tag)
- [ ] 3.2.4 Build runner matrix based on input (linux, macos, or both)
- [ ] 3.2.5 Select benchmark mode (fast or full)
- [ ] 3.2.6 Output configuration as job outputs for downstream jobs

### 3.3 Implement Benchmark Job
- [ ] 3.3.1 Create `benchmark` job with matrix strategy
- [ ] 3.3.2 Configure matrix: `platform: [ubuntu-22.04, macos-latest]`
- [ ] 3.3.3 Checkout repository with full history
- [ ] 3.3.4 Set up Rust toolchain (1.85.0)
- [ ] 3.3.5 Set up Python for potential baseline installation
- [ ] 3.3.6 Cache Cargo registry, Git dependencies, and target/
- [ ] 3.3.7 Implement baseline benchmark execution:
  - Checkout baseline tag/ref
  - Build benchmarks with `--release`
  - Run benchmark binaries
  - Save results to `results/baseline/`
- [ ] 3.3.8 Implement target benchmark execution:
  - Checkout target ref
  - Build benchmarks with `--release`
  - Run benchmark binaries
  - Save results to `results/target/`
- [ ] 3.3.9 Upload results as artifacts (named by platform)
- [ ] 3.3.10 Generate runner metadata JSON

### 3.4 Implement Aggregate Job
- [ ] 3.4.1 Create `aggregate` job depending on benchmark job completion
- [ ] 3.4.2 Download all benchmark artifacts
- [ ] 3.4.3 Set up Python environment
- [ ] 3.4.4 Install Python dependencies (plotly, pandas)
- [ ] 3.4.5 Clone or create `gh-pages` branch
- [ ] 3.4.6 Create directory structure:
  - `benchmark/data/tags/{version}/` for releases
  - `benchmark/data/commits/{sha}/` for PRs
- [ ] 3.4.7 Copy JSON results to appropriate directories
- [ ] 3.4.8 Update master index (`benchmark/data/index.json`)
- [ ] 3.4.9 Run Python script to generate comparison HTML
- [ ] 3.4.10 Commit and push to gh-pages branch
- [ ] 3.4.11 Add PR comment with results link (if triggered from PR)

### 3.5 Test Workflow Locally (Act)
- [ ] 3.5.1 Install `act` for local GitHub Actions testing
- [ ] 3.5.2 Run workflow with `act workflow_dispatch`
- [ ] 3.5.3 Verify prepare job outputs correct configuration
- [ ] 3.5.4 Verify benchmark job builds and runs successfully
- [ ] 3.5.5 Verify artifacts are created correctly
- [ ] 3.5.6 Fix any issues found during local testing

## 4. GitHub Pages Configuration

### 4.1 Configure Repository Settings
- [ ] 4.1.1 Enable GitHub Pages in repository settings
- [ ] 4.1.2 Set source to `gh-pages` branch
- [ ] 4.1.3 Configure custom domain (if applicable): biodatageeks.github.io/datafusion-bio-formats
- [ ] 4.1.4 Verify GitHub Pages URL: https://biodatageeks.github.io/datafusion-bio-formats/benchmark/

### 4.2 Create Initial gh-pages Structure
- [ ] 4.2.1 Create and checkout `gh-pages` branch
- [ ] 4.2.2 Create directory structure:
  ```
  benchmark/
    index.html
    data/
      index.json
      tags/
      commits/
  ```
- [ ] 4.2.3 Create initial `index.html` with navigation
- [ ] 4.2.4 Create initial `index.json` with empty dataset list
- [ ] 4.2.5 Add `.nojekyll` file to disable Jekyll processing
- [ ] 4.2.6 Commit and push gh-pages branch

### 4.3 Test GitHub Pages Deployment
- [ ] 4.3.1 Manually trigger benchmark workflow
- [ ] 4.3.2 Wait for workflow completion
- [ ] 4.3.3 Verify results published to gh-pages
- [ ] 4.3.4 Navigate to https://biodatageeks.github.io/datafusion-bio-formats/benchmark/
- [ ] 4.3.5 Verify HTML report renders correctly
- [ ] 4.3.6 Test interactive features (dropdowns, charts)

## 5. Documentation

### 5.1 Create Benchmark Documentation
- [ ] 5.1.1 Add `benchmarks/README.md` with:
  - Overview of benchmark framework
  - How to run benchmarks locally
  - How to add benchmarks for new formats
  - Explanation of benchmark categories
- [ ] 5.1.2 Document test data sources and checksums
- [ ] 5.1.3 Document benchmark result JSON schema
- [ ] 5.1.4 Provide example benchmark implementations

### 5.2 Update Main README
- [ ] 5.2.1 Add "Performance Benchmarks" section to main README.md
- [ ] 5.2.2 Link to benchmark results: https://biodatageeks.github.io/datafusion-bio-formats/benchmark/
- [ ] 5.2.3 Add badge showing latest benchmark results (if applicable)
- [ ] 5.2.4 Document how to trigger benchmarks on PRs

### 5.3 Update CLAUDE.md
- [ ] 5.3.1 Add benchmark framework to project overview
- [ ] 5.3.2 Document benchmark commands in "Common Development Commands"
- [ ] 5.3.3 Add benchmark workflow to development environment section

## 6. Testing and Validation

### 6.1 End-to-End Testing
- [ ] 6.1.1 Trigger benchmark workflow manually on a test branch
- [ ] 6.1.2 Verify all jobs complete successfully
- [ ] 6.1.3 Verify JSON results contain correct data
- [ ] 6.1.4 Verify HTML report generates correctly
- [ ] 6.1.5 Verify GitHub Pages deployment succeeds
- [ ] 6.1.6 Verify PR comment appears with results link

### 6.2 Cross-Platform Validation
- [ ] 6.2.1 Verify benchmarks run on Linux (ubuntu-22.04)
- [ ] 6.2.2 Verify benchmarks run on macOS (macos-latest)
- [ ] 6.2.3 Compare results between platforms for sanity
- [ ] 6.2.4 Verify platform tabs work in HTML report

### 6.3 Baseline Comparison Testing
- [ ] 6.3.1 Create a release tag (e.g., v0.1.2-benchmark-test)
- [ ] 6.3.2 Trigger benchmark workflow
- [ ] 6.3.3 Make a test optimization in a branch
- [ ] 6.3.4 Run benchmarks comparing branch to release tag
- [ ] 6.3.5 Verify comparison report shows performance difference
- [ ] 6.3.6 Verify speedup/regression calculations are correct

### 6.4 Performance Validation
- [ ] 6.4.1 Verify parallelism benchmarks show expected speedup
- [ ] 6.4.2 Verify predicate pushdown reduces rows scanned
- [ ] 6.4.3 Verify projection pushdown reduces parse time
- [ ] 6.4.4 Document baseline performance metrics

## 7. Extensibility Preparation

### 7.1 Document Format Extension Process
- [ ] 7.1.1 Create `benchmarks/configs/TEMPLATE.yml` with annotated example
- [ ] 7.1.2 Document steps to add new format in benchmarks/README.md:
  - Copy TEMPLATE.yml to {format}.yml
  - Update format name and table name
  - Add test data Google Drive URLs and checksums
  - Define format-specific SQL queries
  - Test locally with benchmark runner
- [ ] 7.1.3 Provide checklist for new format validation
- [ ] 7.1.4 Document how to calculate checksums for test files

### 7.2 Prepare for Future Formats
- [ ] 7.2.1 Identify test data sources for VCF format and document in README
- [ ] 7.2.2 Identify test data sources for FASTQ format and document in README
- [ ] 7.2.3 Identify test data sources for BAM format and document in README
- [ ] 7.2.4 Create example YAML snippets for each format's common queries

## 8. Cleanup and Polish

### 8.1 Code Quality
- [ ] 8.1.1 Run `cargo fmt` on all benchmark code
- [ ] 8.1.2 Run `cargo clippy` and fix warnings
- [ ] 8.1.3 Add comprehensive code comments
- [ ] 8.1.4 Run `cargo test` to ensure no regressions

### 8.2 Python Code Quality
- [ ] 8.2.1 Format Python code with `black`
- [ ] 8.2.2 Add type hints where appropriate
- [ ] 8.2.3 Add docstrings to functions
- [ ] 8.2.4 Test with sample data

### 8.3 Final Review
- [ ] 8.3.1 Review all documentation for accuracy
- [ ] 8.3.2 Verify all links work correctly
- [ ] 8.3.3 Test benchmark workflow one final time
- [ ] 8.3.4 Create PR with all changes
- [ ] 8.3.5 Request review from maintainers
