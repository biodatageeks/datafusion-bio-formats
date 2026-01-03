# package-metadata Specification

## Purpose
TBD - created by archiving change improve-repository-quality. Update Purpose after archive.
## Requirements
### Requirement: Essential Package Metadata
All workspace crates MUST include essential metadata fields in their Cargo.toml manifest for proper project documentation and organization.

#### Scenario: License information present
- **WHEN** examining any crate's Cargo.toml
- **THEN** it MUST contain either a `license` field with an SPDX identifier (e.g., "MIT", "Apache-2.0", "MIT OR Apache-2.0")
- **OR** a `license-file` field pointing to a LICENSE file

#### Scenario: Description field present
- **WHEN** examining any crate's Cargo.toml
- **THEN** it MUST contain a `description` field with a concise explanation of the crate's purpose
- **AND** the description MUST accurately reflect the crate's functionality

### Requirement: Recommended Metadata for Documentation
All workspace crates MUST include recommended metadata fields to improve project documentation and organization.

#### Scenario: Repository information
- **WHEN** examining any crate's Cargo.toml
- **THEN** it SHOULD contain a `repository` field pointing to the GitHub repository URL
- **AND** it SHOULD contain a `homepage` field (can be same as repository for this project)

#### Scenario: README reference
- **WHEN** examining any crate's Cargo.toml
- **THEN** it SHOULD contain a `readme` field pointing to the crate's README.md
- **AND** the README file MUST exist at the specified path

#### Scenario: Authors field
- **WHEN** examining any crate's Cargo.toml
- **THEN** it SHOULD contain an `authors` field listing the project maintainers
- **AND** authors SHOULD be in the format "Name <email>" or just "Name"

### Requirement: Workspace Metadata Inheritance
The workspace MUST leverage Cargo's workspace inheritance to reduce duplication and ensure consistency across crates.

#### Scenario: Common fields inherited
- **WHEN** examining workspace member crates
- **THEN** common fields like `license`, `repository`, `homepage`, `authors`, and `edition` SHOULD be inherited from [workspace.package]
- **AND** the root Cargo.toml MUST define these fields under [workspace.package]

#### Scenario: Workspace inheritance syntax
- **WHEN** a crate inherits a workspace field
- **THEN** it MUST use the syntax `field.workspace = true` in its Cargo.toml
- **AND** the field MUST be defined in the workspace's [workspace.package] section

