# dependencies Specification

## Purpose
TBD - created by archiving change bump-datafusion-50.3.0. Update Purpose after archive.
## Requirements
### Requirement: DataFusion Dependency Version
The project MUST use Apache DataFusion version 50.3.0 for SQL query execution and table provider functionality. The project MUST use Rust toolchain version 1.86.0 to support DataFusion 50.3.0 requirements. All dependencies MUST be specified in a manner compatible with crates.io publishing requirements.

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

#### Scenario: Publishing compatibility
- **WHEN** preparing crates for publication to crates.io
- **THEN** all dependencies MUST use registry versions, path dependencies with version constraints, or documented git dependencies
- **AND** git dependencies are acceptable and will be resolved by Cargo at build time
- **AND** wildcard versions ("*") MUST NOT be used as they are rejected by crates.io

### Requirement: Git Dependency Documentation
The project MUST document all git-based dependencies for transparency and user guidance.

#### Scenario: Noodles fork dependencies
- **WHEN** examining workspace dependencies
- **THEN** noodles-cram, noodles-sam, and noodles-fasta use git = "https://github.com/biodatageeks/noodles.git"
- **AND** the rationale for using the fork MUST be documented in affected crate READMEs
- **AND** the documentation MUST explain what functionality the fork provides
- **AND** the documentation SHOULD mention the specific git revision being used

#### Scenario: User transparency
- **WHEN** users install crates with git dependencies
- **THEN** Cargo will automatically fetch the git dependencies during build
- **AND** users MUST have git installed and network access
- **AND** this is standard behavior in the Rust ecosystem
- **AND** the README MUST note this requirement

### Requirement: Dependency Version Constraints
All workspace dependencies MUST use appropriate version constraints that balance compatibility and stability.

#### Scenario: Exact version constraints
- **WHEN** specifying major external dependencies
- **THEN** use exact versions for critical dependencies (e.g., datafusion = "50.3.0")
- **AND** this ensures consistent builds and predictable behavior

#### Scenario: Caret requirements
- **WHEN** specifying library dependencies following SemVer
- **THEN** caret requirements MAY be used (e.g., "^0.3.1" allows 0.3.x updates)
- **AND** this enables automatic compatible updates

#### Scenario: Workspace member dependencies
- **WHEN** a crate depends on another workspace crate
- **THEN** it MUST specify both path and version: `bio-format-core = { version = "0.1.0", path = "../bio-format-core" }`
- **AND** the version MUST match the crate's actual version
- **AND** Cargo will use the path during local development and the version when published

### Requirement: Optional Features
Crates MUST support optional features to reduce compilation time and dependency bloat for users who don't need all functionality.

#### Scenario: Cloud storage features
- **WHEN** defining features for object storage support
- **THEN** bio-format-core MAY define optional features like ["gcs", "s3", "azure"]
- **AND** default features SHOULD include the most commonly used functionality
- **AND** feature flags SHOULD be documented in the crate's README

#### Scenario: Feature documentation
- **WHEN** a crate defines optional features
- **THEN** the Cargo.toml MUST document each feature's purpose
- **AND** the README SHOULD explain how to enable features
- **AND** examples SHOULD show common feature combinations

