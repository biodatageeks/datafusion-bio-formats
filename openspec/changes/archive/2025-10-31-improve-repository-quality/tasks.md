# Implementation Tasks

## 1. Configure Licensing and Workspace Metadata
- [ ] 1.1 Add LICENSE-MIT and LICENSE-APACHE files to repository root
- [ ] 1.2 Verify license files comply with OSI standards
- [ ] 1.3 Add [workspace.package] section to root Cargo.toml with:
  - [ ] license = "MIT OR Apache-2.0"
  - [ ] authors = ["BiodataGeeks Team"]
  - [ ] repository = "https://github.com/biodatageeks/datafusion-bio-formats"
  - [ ] homepage = "https://github.com/biodatageeks/datafusion-bio-formats"
  - [ ] edition = "2024"

## 2. Update bio-format-core Crate
- [ ] 2.1 Update Cargo.toml with:
  - [ ] Inherit workspace fields: license.workspace = true, authors.workspace = true, etc.
  - [ ] Add description = "Core utilities for DataFusion bioinformatics table providers"
  - [ ] Add readme = "README.md"
- [ ] 2.2 Create README.md with usage examples and purpose
- [ ] 2.3 Add comprehensive crate documentation to src/lib.rs
- [ ] 2.4 Add #![warn(missing_docs)] and document all public APIs with examples
- [ ] 2.5 Verify documentation builds with `cargo doc --no-deps`

## 3. Update bio-format-fastq Crate
- [ ] 3.1 Update Cargo.toml (following pattern from 2.1)
  - [ ] description = "FASTQ file format support for Apache DataFusion"
- [ ] 3.2 Create README.md with FASTQ usage examples
- [ ] 3.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 3.4 Document all public APIs with examples

## 4. Update bio-format-vcf Crate
- [ ] 4.1 Update Cargo.toml with description = "VCF file format support for Apache DataFusion"
- [ ] 4.2 Create README.md with VCF usage examples
- [ ] 4.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 4.4 Document all public APIs with examples

## 5. Update bio-format-bam Crate
- [ ] 5.1 Update Cargo.toml with description = "BAM file format support for Apache DataFusion"
- [ ] 5.2 Create README.md with BAM usage examples
- [ ] 5.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 5.4 Document all public APIs with examples

## 6. Update bio-format-bed Crate
- [ ] 6.1 Update Cargo.toml with description = "BED file format support for Apache DataFusion"
- [ ] 6.2 Create README.md with BED usage examples
- [ ] 6.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 6.4 Document all public APIs with examples

## 7. Update bio-format-gff Crate
- [ ] 7.1 Update Cargo.toml with description = "GFF file format support for Apache DataFusion"
- [ ] 7.2 Create README.md with GFF usage examples
- [ ] 7.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 7.4 Document all public APIs with examples

## 8. Update bio-format-fasta Crate
- [ ] 8.1 Update Cargo.toml with description = "FASTA file format support for Apache DataFusion"
- [ ] 8.2 Create README.md with FASTA usage examples (document noodles fork dependency)
- [ ] 8.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 8.4 Document all public APIs with examples

## 9. Update bio-format-cram Crate
- [ ] 9.1 Update Cargo.toml with description = "CRAM file format support for Apache DataFusion"
- [ ] 9.2 Create README.md with CRAM usage examples (document noodles fork dependency)
- [ ] 9.3 Add crate documentation to src/lib.rs with #![warn(missing_docs)]
- [ ] 9.4 Document all public APIs with examples

## 10. Enhance Root README
- [ ] 10.1 Add badges section:
  - [ ] CI status badge
  - [ ] License badge
- [ ] 10.2 Add "Crates" section listing all workspace crates with descriptions
- [ ] 10.3 Add "Installation" section with instructions for adding as dependencies
- [ ] 10.4 Add "Development" section with build and test instructions
- [ ] 10.5 Ensure existing content is current and accurate
- [ ] 10.6 Add usage examples for common scenarios

## 11. Enhance CI Workflow
- [ ] 11.1 Update .github/workflows/ci.yml to add:
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo doc --no-deps --all-features` with RUSTDOCFLAGS="-D warnings"
  - [ ] Ensure workflow has a clear name "CI" for badge generation
- [ ] 11.2 Verify CI passes on a test branch
- [ ] 11.3 Update cache configuration to use latest actions/cache version

## 12. Setup Dependabot
- [ ] 12.1 Create .github/dependabot.yml configured for:
  - [ ] Cargo dependency updates (monthly)
  - [ ] GitHub Actions updates (monthly)
- [ ] 12.2 Verify Dependabot is enabled in repository settings

## 13. Create CHANGELOG
- [ ] 13.1 Create CHANGELOG.md in root directory
- [ ] 13.2 Document format following Keep a Changelog conventions
- [ ] 13.3 Add section headers: Added, Changed, Deprecated, Removed, Fixed, Security
- [ ] 13.4 Add initial content noting major features

## 14. Verification and Testing
- [ ] 14.1 Verify all crates build with `cargo build --all`
- [ ] 14.2 Verify all tests pass with `cargo test --all`
- [ ] 14.3 Verify documentation builds with `cargo doc --no-deps --all-features`
- [ ] 14.4 Test examples run successfully
- [ ] 14.5 Verify enhanced CI workflow passes
- [ ] 14.6 Check badge URLs are correctly formatted in README

## 15. Documentation Review
- [ ] 15.1 Review all README files for clarity and completeness
- [ ] 15.2 Review all crate-level documentation
- [ ] 15.3 Verify code examples compile and run
- [ ] 15.4 Check for broken links in documentation
- [ ] 15.5 Ensure consistent tone and formatting across all docs
- [ ] 15.6 Verify git dependencies are properly documented in affected crate READMEs

## 16. Final Quality Checks
- [ ] 16.1 Verify all Cargo.toml files have correct metadata
- [ ] 16.2 Verify license files are present and properly referenced
- [ ] 16.3 Run `cargo clippy --all-targets --all-features` with no warnings
- [ ] 16.4 Run `cargo fmt --all -- --check` to ensure formatting
- [ ] 16.5 Verify all public APIs have documentation
- [ ] 16.6 Ensure CHANGELOG.md is complete and accurate
