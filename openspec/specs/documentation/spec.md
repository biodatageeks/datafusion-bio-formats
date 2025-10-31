# documentation Specification

## Purpose
TBD - created by archiving change improve-repository-quality. Update Purpose after archive.
## Requirements
### Requirement: Crate-Level Documentation
All workspace crates MUST include comprehensive crate-level documentation in their lib.rs file.

#### Scenario: Crate documentation structure
- **WHEN** examining any crate's src/lib.rs
- **THEN** it MUST contain a crate-level doc comment (//! or /*! ... */) before any items
- **AND** the documentation MUST include a brief description of the crate's purpose
- **AND** the documentation SHOULD include usage examples with code blocks
- **AND** the documentation SHOULD reference the README for additional details

#### Scenario: Example code in documentation
- **WHEN** crate documentation includes code examples
- **THEN** examples MUST use ``` rust code fences
- **AND** examples SHOULD be tested with `cargo test --doc`
- **AND** examples SHOULD demonstrate the most common use cases

#### Scenario: Documentation completeness
- **WHEN** generating documentation with `cargo doc`
- **THEN** all public items (functions, structs, traits, enums, modules) MUST have documentation comments
- **AND** documentation SHOULD include parameter descriptions, return values, and examples where applicable
- **AND** missing documentation SHOULD be caught by clippy with #![warn(missing_docs)]

### Requirement: Per-Crate README Files
Each workspace crate MUST have its own README.md file with usage instructions and examples.

#### Scenario: README structure
- **WHEN** examining a crate's README.md
- **THEN** it MUST include a title matching the crate name
- **AND** it MUST include a brief description (matching Cargo.toml description)
- **AND** it SHOULD include installation instructions via Cargo
- **AND** it SHOULD include at least one usage example with code
- **AND** it SHOULD link to docs.rs for full API documentation
- **AND** it MAY include badges (crates.io version, docs.rs, CI status)

#### Scenario: README referenced in Cargo.toml
- **WHEN** a crate has a README.md file
- **THEN** Cargo.toml MUST include `readme = "README.md"`
- **AND** the README MUST be included in the published crate package
- **AND** the README MUST be displayed on the crate's crates.io page

### Requirement: Root README Enhancement
The workspace root README.md MUST be enhanced with badges, installation instructions, and links to published crates.

#### Scenario: Badges display
- **WHEN** examining the root README.md
- **THEN** it SHOULD include a CI status badge from GitHub Actions
- **AND** it SHOULD include crates.io version badges for all published crates
- **AND** it SHOULD include docs.rs badges for all published crates
- **AND** it SHOULD include a license badge

#### Scenario: Installation instructions
- **WHEN** a user reads the root README.md
- **THEN** it MUST include instructions for adding crates as dependencies
- **AND** it SHOULD show Cargo.toml dependency syntax for each crate
- **AND** it SHOULD mention feature flags if applicable

#### Scenario: Crate listing
- **WHEN** the root README describes the workspace
- **THEN** it MUST list all published crates with their purpose
- **AND** each crate listing SHOULD link to its docs.rs page
- **AND** each crate listing SHOULD link to its crates.io page

### Requirement: API Documentation Quality
Public APIs MUST be documented with sufficient detail for users to understand usage without reading source code.

#### Scenario: Function documentation
- **WHEN** documenting a public function
- **THEN** it MUST include a summary of what the function does
- **AND** it SHOULD document each parameter with # Arguments or inline descriptions
- **AND** it SHOULD document return values with # Returns
- **AND** it SHOULD document possible errors with # Errors
- **AND** it SHOULD include # Example code blocks when usage is not obvious

#### Scenario: Type documentation
- **WHEN** documenting a public struct, enum, or trait
- **THEN** it MUST include a description of the type's purpose
- **AND** public fields SHOULD be documented
- **AND** it SHOULD include usage examples if the type is commonly constructed by users

#### Scenario: Module documentation
- **WHEN** a module contains public items
- **THEN** the module SHOULD have a doc comment explaining its purpose
- **AND** the module doc SHOULD provide an overview of the contained items

### Requirement: Docs.rs Compatibility
Documentation MUST build successfully on docs.rs for all published crates.

#### Scenario: Local documentation build
- **WHEN** running `cargo doc --no-deps` in any crate
- **THEN** it MUST build without errors
- **AND** it SHOULD build without warnings
- **AND** intra-doc links SHOULD be valid

#### Scenario: Documentation metadata
- **WHEN** a crate needs special docs.rs configuration
- **THEN** it MAY include a [package.metadata.docs.rs] section in Cargo.toml
- **AND** it MAY specify features to enable with `features = ["full"]`
- **AND** it MAY specify rustdoc flags with `rustdoc-args = ["--cfg", "docsrs"]`

### Requirement: Code Examples
Each crate MUST include working examples in its examples/ directory.

#### Scenario: Example programs
- **WHEN** examining a crate's examples/ directory
- **THEN** it SHOULD contain at least one example demonstrating basic usage
- **AND** examples SHOULD be runnable with `cargo run --example <name>`
- **AND** examples SHOULD include comments explaining key concepts
- **AND** examples MAY be referenced in documentation

#### Scenario: Example metadata
- **WHEN** an example requires special configuration
- **THEN** it MAY be explicitly declared in Cargo.toml with [[example]]
- **AND** it MUST specify the path with `path = "examples/<name>.rs"`

