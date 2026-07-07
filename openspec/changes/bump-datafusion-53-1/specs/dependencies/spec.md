## MODIFIED Requirements

### Requirement: DataFusion Dependency Version
The project MUST use Apache DataFusion version 53.1.0 for SQL query execution and table provider functionality. The project MUST use a Rust toolchain that satisfies DataFusion 53.1.0's Rust 1.88 minimum requirement. All dependencies MUST be specified in a manner compatible with crates.io publishing requirements.

#### Scenario: Workspace dependency configuration
- **WHEN** building the workspace
- **THEN** Cargo.toml [workspace.dependencies] specifies datafusion = "53.1.0"
- **AND** datafusion-execution = "53.1.0"
- **AND** DataFusion's default `compression` feature is disabled to avoid native LZMA link conflicts with CRAM dependencies
- **AND** all workspace member crates can compile successfully

#### Scenario: Rust toolchain requirement
- **WHEN** compiling the project
- **THEN** rust-toolchain.toml specifies a Rust toolchain version greater than or equal to 1.88.0
- **AND** the rustfmt component is available
- **AND** the project compiles without errors

#### Scenario: API compatibility
- **WHEN** using DataFusion APIs in table providers and physical execution plans
- **THEN** the code uses DataFusion 53.x compatible API calls
- **AND** custom ExecutionPlan implementations return &Arc<PlanProperties> from properties()
- **AND** custom ExecutionPlan statistics are exposed through partition_statistics()
- **AND** table provider scan behavior remains compatible with DataFusion 53 scan and scan_with_args flows

#### Scenario: Test compatibility
- **WHEN** running the test suite
- **THEN** all existing tests pass without modification or with focused adjustments for DataFusion 53.x behavior changes
- **AND** no functional regressions are introduced
- **AND** example programs continue to work correctly

#### Scenario: Performance preservation
- **WHEN** executing queries with DataFusion 53.1.0
- **THEN** the system benefits from performance improvements in DataFusion 53.x
- **AND** no material performance regressions occur in existing functionality
- **AND** memory usage remains reasonable for large files

#### Scenario: Publishing compatibility
- **WHEN** preparing crates for publication to crates.io
- **THEN** all dependencies MUST use registry versions, path dependencies with version constraints, or documented git dependencies
- **AND** git dependencies are acceptable and will be resolved by Cargo at build time
- **AND** wildcard versions ("*") MUST NOT be used as they are rejected by crates.io
