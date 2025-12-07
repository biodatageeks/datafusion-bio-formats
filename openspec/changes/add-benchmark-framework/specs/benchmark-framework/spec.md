# Benchmark Framework Specification

## ADDED Requirements

### Requirement: Benchmark Execution Infrastructure
The system SHALL provide a benchmark execution framework that measures performance across three optimization categories: parallelism, predicate pushdown, and projection pushdown.

#### Scenario: Execute parallelism benchmark
- **WHEN** a parallelism benchmark is executed for a file format
- **THEN** the system measures throughput with varying thread counts (1, 2, 4, 8, max cores)
- **AND** calculates speedup ratios compared to single-threaded baseline
- **AND** records elapsed time, throughput (records/sec), and total records processed

#### Scenario: Execute predicate pushdown benchmark
- **WHEN** a predicate pushdown benchmark is executed
- **THEN** the system runs queries with and without filter optimizations
- **AND** measures the ratio of rows scanned to rows returned
- **AND** records query execution time and I/O statistics

#### Scenario: Execute projection pushdown benchmark
- **WHEN** a projection pushdown benchmark is executed
- **THEN** the system runs queries selecting different column subsets
- **AND** compares full schema reads against projected reads
- **AND** measures I/O reduction and parse time savings

### Requirement: Test Data Management
The system SHALL download and cache large test files from Google Drive with integrity verification.

#### Scenario: Download test file from Google Drive
- **WHEN** a benchmark requires test data stored on Google Drive
- **THEN** the system extracts the file ID from Google Drive URLs
- **AND** downloads the file with progress indication
- **AND** caches the file locally in the system cache directory
- **AND** verifies file integrity using SHA-256 checksums if provided

#### Scenario: Use cached test file
- **WHEN** a previously downloaded test file exists in the cache
- **THEN** the system reuses the cached file without re-downloading
- **AND** validates the checksum matches the expected value
- **AND** re-downloads if checksum verification fails

#### Scenario: Handle Google Drive download confirmation
- **WHEN** a direct download fails due to Google Drive's confirmation requirement
- **THEN** the system automatically retries with the confirmation URL
- **AND** successfully downloads large files requiring virus scan acknowledgment

### Requirement: Benchmark Result Recording
The system SHALL record benchmark results in structured JSON format with comprehensive metadata.

#### Scenario: Record benchmark result
- **WHEN** a benchmark completes execution
- **THEN** the system creates a JSON result file containing:
  - Benchmark name and file format
  - Category (parallelism, predicate_pushdown, projection_pushdown)
  - Timestamp in ISO 8601 format
  - System information (OS, CPU model, cores, memory)
  - Configuration parameters (thread count, query filters, projected columns)
  - Performance metrics (throughput, elapsed time, speedup ratios)
- **AND** writes the result to the specified output directory

#### Scenario: Calculate performance metrics
- **WHEN** recording benchmark results
- **THEN** the system calculates throughput as total_records / elapsed_seconds
- **AND** calculates speedup as baseline_time / target_time
- **AND** includes peak memory usage if available

### Requirement: Multi-Platform Benchmark Execution
The system SHALL execute benchmarks on multiple platforms via GitHub Actions workflow.

#### Scenario: Execute benchmark workflow on PR
- **WHEN** a benchmark workflow is manually triggered on a pull request
- **THEN** the system determines the baseline version (latest tag or specified tag)
- **AND** determines the target version (current PR branch)
- **AND** executes benchmarks on Linux and macOS runners in parallel
- **AND** uploads JSON results as workflow artifacts

#### Scenario: Execute benchmarks on release
- **WHEN** a new release tag is created
- **THEN** the system automatically executes the full benchmark suite
- **AND** runs on both Linux and macOS platforms
- **AND** stores results in GitHub Pages for historical tracking

#### Scenario: Support fast and full benchmark modes
- **WHEN** benchmarks are triggered via workflow_dispatch
- **THEN** the user can select "fast" mode with a subset of test cases
- **OR** select "full" mode with comprehensive test coverage
- **AND** the workflow adjusts iteration counts accordingly (3 for fast, 10 for full)

### Requirement: Interactive Benchmark Comparison Reports
The system SHALL generate interactive HTML reports comparing baseline and target benchmark results across platforms.

#### Scenario: Generate comparison report
- **WHEN** all benchmark artifacts are collected after workflow completion
- **THEN** the system aggregates results from all runners (Linux, macOS)
- **AND** generates an HTML report with embedded JSON data
- **AND** includes Plotly.js interactive charts
- **AND** provides dropdown menus for selecting baseline and target datasets
- **AND** provides platform tabs for switching between Linux and macOS results

#### Scenario: Display performance comparison charts
- **WHEN** a user views the benchmark comparison report
- **THEN** the report displays grouped bar charts comparing baseline vs target
- **AND** shows per-category breakdowns (parallelism, predicate pushdown, projection pushdown)
- **AND** displays speedup/regression indicators with color coding (green for improvement, red for regression)
- **AND** supports hover tooltips with detailed metrics

#### Scenario: Switch between comparison configurations
- **WHEN** a user selects different baseline and target versions from dropdowns
- **THEN** the charts update dynamically without page reload
- **AND** the system validates that both versions have results for the selected platform
- **AND** displays an error message if comparison is not possible

### Requirement: GitHub Pages Result Publishing
The system SHALL publish benchmark results to GitHub Pages with structured organization and historical tracking.

#### Scenario: Publish release benchmark results
- **WHEN** benchmarks complete for a tagged release (e.g., v0.1.1)
- **THEN** the system creates directory structure `gh-pages/benchmark/data/tags/v0.1.1/`
- **AND** stores `linux.json` and `macos.json` with benchmark results
- **AND** updates the master index at `gh-pages/benchmark/data/index.json`
- **AND** regenerates the comparison HTML report
- **AND** deploys to https://biodatageeks.github.io/datafusion-bio-formats/benchmark/

#### Scenario: Publish PR benchmark results
- **WHEN** benchmarks complete for a pull request commit
- **THEN** the system creates directory structure `gh-pages/benchmark/data/commits/{sha}/`
- **AND** stores platform-specific results
- **AND** adds a comment to the PR with a link to the comparison report
- **AND** includes summary statistics in the comment

#### Scenario: Maintain master index
- **WHEN** new benchmark results are published
- **THEN** the system updates `data/index.json` with the new dataset entry
- **AND** includes metadata: version/tag, commit SHA, timestamp, available platforms
- **AND** maintains chronological ordering for easy navigation

### Requirement: YAML Configuration-Driven Benchmarks
The system SHALL use YAML configuration files to define benchmarks for each file format, enabling zero-code extensibility.

#### Scenario: Load benchmark configuration from YAML
- **WHEN** the benchmark runner is executed with a configuration file
- **THEN** the system parses the YAML file using serde_yaml
- **AND** validates the configuration structure and required fields
- **AND** extracts format name, table name, and test data specifications
- **AND** extracts test configurations for parallelism, predicate pushdown, and projection pushdown

#### Scenario: Configure test data in YAML
- **WHEN** a YAML configuration specifies test data
- **THEN** each test data entry includes:
  - filename (local cache name)
  - drive_url (Google Drive sharing URL)
  - checksum (SHA-256 hash for validation)
- **AND** the system downloads files using the data downloader
- **AND** validates checksums after download

#### Scenario: Configure parallelism tests in YAML
- **WHEN** a YAML configuration defines parallelism tests
- **THEN** the configuration specifies thread_counts as a list (e.g., [1, 2, 4, 8, max])
- **AND** specifies repetitions count for statistical accuracy
- **AND** specifies a SQL query template with {table_name} placeholder
- **AND** the runner executes the query with each thread count configuration

#### Scenario: Configure predicate pushdown tests in YAML
- **WHEN** a YAML configuration defines predicate pushdown tests
- **THEN** the configuration includes a list of named test cases
- **AND** each test case has a name and SQL query
- **AND** queries use {table_name} placeholder for table reference
- **AND** the runner executes each query the specified number of repetitions

#### Scenario: Configure projection pushdown tests in YAML
- **WHEN** a YAML configuration defines projection pushdown tests
- **THEN** the configuration includes a list of named test cases
- **AND** each test case specifies different column projections (full schema, subset, single column)
- **AND** queries use {table_name} placeholder for table reference
- **AND** the runner executes each query the specified number of repetitions

#### Scenario: Register table from configuration
- **WHEN** the benchmark runner loads a configuration
- **THEN** the system determines the appropriate table provider based on format name
- **AND** registers the table in DataFusion SessionContext with the configured table_name
- **AND** uses the downloaded test data file paths
- **AND** supports all implemented formats (gff, vcf, fastq, bam, bed, fasta, cram)

#### Scenario: Add new format with only YAML configuration
- **WHEN** adding benchmarks for a new file format (e.g., VCF, FASTQ)
- **THEN** contributors create `benchmarks/configs/{format}.yml`
- **AND** specify test data Google Drive URLs and checksums
- **AND** define SQL queries for parallelism tests
- **AND** define SQL queries for predicate pushdown tests
- **AND** define SQL queries for projection pushdown tests
- **AND** run benchmarks without any code changes to the runner
- **AND** results automatically integrate into comparison reports

#### Scenario: Validate YAML configuration
- **WHEN** the benchmark runner loads a YAML configuration
- **THEN** the system validates required fields are present (format, table_name, test_data)
- **AND** validates each test category has at least one test defined
- **AND** validates SQL queries contain {table_name} placeholder
- **AND** validates thread_counts and repetitions are positive integers
- **AND** reports clear error messages for invalid configurations

### Requirement: Benchmark Result Validation
The system SHALL validate benchmark results for consistency and detect anomalies.

#### Scenario: Validate result completeness
- **WHEN** benchmark results are collected
- **THEN** the system verifies all required fields are present
- **AND** validates JSON schema compliance
- **AND** ensures metrics are within reasonable ranges (e.g., positive throughput)
- **AND** flags missing or invalid results for review

#### Scenario: Detect performance anomalies
- **WHEN** comparing benchmark results
- **THEN** the system calculates percentage change from baseline
- **AND** highlights regressions exceeding configurable threshold (default 10%)
- **AND** highlights improvements exceeding threshold
- **AND** includes anomaly indicators in the HTML report

### Requirement: Extensible Configuration
The system SHALL support configuration for benchmark behavior and thresholds.

#### Scenario: Configure benchmark parameters
- **WHEN** running benchmarks
- **THEN** users can specify:
  - Thread counts for parallelism tests
  - Iteration counts for statistical accuracy
  - Test data sources and checksums
  - Output directories for results
- **AND** configuration is validated before execution

#### Scenario: Configure reporting thresholds
- **WHEN** generating comparison reports
- **THEN** users can configure:
  - Performance regression alert threshold (e.g., 10%)
  - Performance improvement highlight threshold
  - Chart styling and color schemes
- **AND** thresholds are documented in the report
