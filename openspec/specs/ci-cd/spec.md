# ci-cd Specification

## Purpose
TBD - created by archiving change improve-repository-quality. Update Purpose after archive.
## Requirements
### Requirement: Enhanced CI Workflow
The project MUST include comprehensive CI checks that validate code quality, tests, and documentation before merging.

#### Scenario: Existing CI enhancement
- **WHEN** the CI workflow runs on pull requests or pushes
- **THEN** it MUST run on ubuntu-latest (or ubuntu-22.04)
- **AND** it MUST check code formatting with `cargo fmt --all -- --check`
- **AND** it MUST run all tests with `cargo test --all`
- **AND** it SHOULD run clippy lints with `cargo clippy --all-targets --all-features -- -D warnings`
- **AND** it SHOULD build documentation with `cargo doc --no-deps --all-features`

#### Scenario: Documentation build validation
- **WHEN** the CI workflow validates documentation
- **THEN** it MUST run `cargo doc --no-deps` to ensure docs build without errors
- **AND** it SHOULD fail if documentation contains broken intra-doc links
- **AND** it MAY use `RUSTDOCFLAGS="-D warnings"` to treat doc warnings as errors

#### Scenario: Multiple Rust versions
- **WHEN** the project needs to support multiple Rust versions
- **THEN** CI MAY test on both the MSRV (1.86.0) and stable toolchain
- **AND** the rust-toolchain.toml MUST specify the primary version

### Requirement: Dependency Update Checks
The project MUST include automated checks for outdated dependencies.

#### Scenario: Dependabot integration
- **WHEN** the project uses Dependabot
- **THEN** a .github/dependabot.yml file SHOULD be configured
- **AND** it SHOULD check for Cargo dependency updates
- **AND** it SHOULD check for GitHub Actions updates
- **AND** it SHOULD run at least monthly

#### Scenario: Security audits
- **WHEN** the CI workflow runs
- **THEN** it MAY run `cargo audit` to check for known security vulnerabilities
- **AND** it SHOULD fail if high-severity vulnerabilities are found
- **AND** it SHOULD use the actions-rs/audit-check action or equivalent

### Requirement: Badge Generation
The project MUST configure CI to generate status badges for display in documentation.

#### Scenario: CI status badge
- **WHEN** the main CI workflow is configured
- **THEN** it MUST have a consistent name (e.g., "CI") for badge generation
- **AND** the badge URL MUST be `https://github.com/{org}/{repo}/actions/workflows/{workflow}.yml/badge.svg`
- **AND** the badge SHOULD link to the Actions tab

#### Scenario: Documentation status
- **WHEN** crates are published to crates.io
- **THEN** docs.rs will automatically build and host documentation
- **AND** the docs.rs badge MUST be `https://docs.rs/{crate}/badge.svg`
- **AND** the badge SHOULD link to `https://docs.rs/{crate}`

#### Scenario: Crates.io version badge
- **WHEN** crates are published
- **THEN** the version badge MUST be `https://img.shields.io/crates/v/{crate}.svg`
- **AND** the badge SHOULD link to `https://crates.io/crates/{crate}`

### Requirement: Cache Optimization
CI workflows MUST cache dependencies to improve build times and reduce resource usage.

#### Scenario: Cargo cache
- **WHEN** the CI workflow sets up caching
- **THEN** it MUST cache ~/.cargo/registry
- **AND** it MUST cache ~/.cargo/git
- **AND** it MUST cache target/ directory
- **AND** cache keys MUST include the Cargo.lock hash for proper invalidation
- **AND** it SHOULD use actions/cache@v3 or actions/cache@v4

### Requirement: Branch Protection
The project MUST configure branch protection rules to enforce CI checks.

#### Scenario: Main branch protection
- **WHEN** configuring the main/master branch
- **THEN** it SHOULD require CI checks to pass before merging
- **AND** it SHOULD require at least one approval for pull requests
- **AND** it SHOULD require branches to be up to date before merging
- **AND** administrators MAY bypass protections for emergency fixes

