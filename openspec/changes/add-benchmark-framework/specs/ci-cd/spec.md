# CI/CD Specification Delta

## ADDED Requirements

### Requirement: Automated Performance Benchmarking
The project SHALL provide automated performance benchmarking workflows to track performance improvements and detect regressions.

#### Scenario: Manual benchmark trigger on PRs
- **WHEN** a contributor wants to benchmark a pull request
- **THEN** they can manually trigger the benchmark workflow via workflow_dispatch
- **AND** select runner platforms (Linux, macOS, or both)
- **AND** select benchmark suite mode (fast or full)
- **AND** optionally specify a baseline tag for comparison

#### Scenario: Automatic benchmark on releases
- **WHEN** a new release tag is created (matching pattern v*.*.*)
- **THEN** the benchmark workflow automatically executes
- **AND** runs the full benchmark suite on both Linux and macOS
- **AND** publishes results to GitHub Pages
- **AND** stores historical data for future comparisons

#### Scenario: Matrix-based parallel execution
- **WHEN** the benchmark workflow executes
- **THEN** it uses a job matrix to run benchmarks in parallel
- **AND** the prepare job determines baseline and target references
- **AND** the benchmark job runs on each platform (ubuntu-22.04, macos-latest)
- **AND** the aggregate job collects results and generates reports

#### Scenario: Benchmark artifact management
- **WHEN** benchmarks complete on a runner platform
- **THEN** the system uploads JSON result files as workflow artifacts
- **AND** artifacts are named with platform identifier (linux, macos)
- **AND** artifacts are retained for the standard GitHub retention period
- **AND** the aggregate job downloads all artifacts for processing

#### Scenario: GitHub Pages deployment
- **WHEN** the aggregate job completes
- **THEN** it clones or creates the gh-pages branch
- **AND** stores benchmark results in structured directories (tags/, commits/)
- **AND** updates the master index (data/index.json)
- **AND** generates interactive comparison HTML reports
- **AND** publishes to https://biodatageeks.github.io/datafusion-bio-formats/benchmark/

#### Scenario: PR comment with results
- **WHEN** benchmarks complete for a pull request
- **THEN** the workflow posts a comment on the PR
- **AND** includes a link to the comparison report
- **AND** provides summary statistics (speedup/regression percentages)
- **AND** highlights any significant performance changes

#### Scenario: Benchmark workflow caching
- **WHEN** the benchmark workflow runs
- **THEN** it caches the Cargo registry and Git dependencies
- **AND** caches compiled targets to speed up builds
- **AND** caches downloaded test data files
- **AND** uses appropriate cache keys based on Cargo.lock and data checksums
