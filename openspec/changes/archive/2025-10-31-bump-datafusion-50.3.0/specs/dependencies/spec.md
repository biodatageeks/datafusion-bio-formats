# Dependencies Specification

## ADDED Requirements

### Requirement: DataFusion Dependency Version
The project MUST use Apache DataFusion version 50.3.0 for SQL query execution and table provider functionality. The project MUST use Rust toolchain version 1.86.0 to support DataFusion 50.3.0 requirements.

#### Scenario: Workspace dependency configuration
- **WHEN** building the workspace
- **THEN** Cargo.toml [workspace.dependencies] specifies datafusion = "50.3.0"
- **AND** datafusion-execution = "50.3.0"
- **AND** all workspace member crates can compile successfully

#### Scenario: Rust toolchain requirement
- **WHEN** compiling the project
- **THEN** rust-toolchain.toml specifies Rust 1.86.0
- **AND** the rustfmt component is available
- **AND** the project compiles without errors

#### Scenario: API compatibility
- **WHEN** using DataFusion APIs in table providers and physical execution plans
- **THEN** the code uses DataFusion 50.x compatible API calls
- **AND** handles ConfigOptions as &Arc<ConfigOptions> where required
- **AND** uses ProjectionExpr struct fields (.expr, .alias) instead of tuples
- **AND** accommodates Hive partition auto-detection behavior

#### Scenario: Test compatibility
- **WHEN** running the test suite
- **THEN** all existing tests pass without modification or with minimal adjustments for DataFusion 50.x behavior changes
- **AND** no functional regressions are introduced
- **AND** example programs continue to work correctly

#### Scenario: Performance preservation
- **WHEN** executing queries with DataFusion 50.3.0
- **THEN** the system benefits from performance improvements in DataFusion 50.x
- **AND** no performance regressions occur in existing functionality
- **AND** memory usage remains reasonable for large files
