## MODIFIED Requirements

### Requirement: Git Dependency Documentation

The project MUST document all git-based dependencies for transparency and user guidance.

#### Scenario: Noodles fork dependencies

- **WHEN** examining workspace dependencies
- **THEN** noodles-cram, noodles-sam, and noodles-fasta use git = "https://github.com/biodatageeks/noodles.git"
- **AND** the rationale for using the fork MUST be documented in affected crate READMEs
- **AND** the documentation MUST explain what functionality the fork provides
- **AND** the documentation SHOULD mention the specific git revision being used

#### Scenario: BigTools fork dependency

- **WHEN** examining the `datafusion-bio-format-bbi` dependencies
- **THEN** BigTools uses git = "https://github.com/biodatageeks/bigtools.git"
- **AND** the BBI crate README MUST document the block-layout and bounded-query functionality supplied by the fork
- **AND** the documentation MUST identify the pinned revision and the upstream migration path

#### Scenario: User transparency

- **WHEN** users install crates with git dependencies
- **THEN** Cargo will automatically fetch the git dependencies during build
- **AND** users MUST have git installed and network access
- **AND** this is standard behavior in the Rust ecosystem
- **AND** the README MUST note this requirement
