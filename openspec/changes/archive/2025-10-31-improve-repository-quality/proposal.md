# Improve Repository Quality

## Why

The datafusion-bio-formats workspace needs to establish professional standards for an open-source project. Currently, it lacks comprehensive documentation, proper licensing, quality badges, and enhanced CI/CD workflows. Improving these aspects will increase visibility, enable easier adoption by the Rust bioinformatics community, and establish best practices for the project's continued development.

## What Changes

- **Package metadata**: Add descriptive Cargo.toml fields for all workspace crates (license, description, homepage, repository, keywords, authors, readme)
- **Licensing**: Establish clear open-source licensing with MIT OR Apache-2.0 dual licensing
- **Documentation**: Add comprehensive crate-level documentation, API docs with examples, and per-crate README files
- **CI/CD enhancements**: Improve GitHub Actions with clippy lints, documentation builds, and dependency updates
- **Quality badges**: Add CI status and license badges to documentation
- **Dependency documentation**: Document the rationale for git dependencies where they exist
- **Workspace optimization**: Use workspace inheritance for common fields to reduce duplication

**BREAKING**: None - this is additive work that prepares the crates for publication but doesn't change existing APIs or behavior.

## Impact

### Affected Specs
- `dependencies` - Modified to document git dependency requirements
- `package-metadata` - **NEW** - Requirements for package metadata and licensing
- `documentation` - **NEW** - Documentation standards for the project
- `ci-cd` - **NEW** - Continuous integration workflow enhancements

### Affected Code
- Root `Cargo.toml` - workspace metadata inheritance
- `LICENSE-MIT` and `LICENSE-APACHE` - new license files
- All `datafusion/bio-format-*/Cargo.toml` - package metadata
- All `datafusion/bio-format-*/src/lib.rs` - crate-level documentation
- All `datafusion/bio-format-*/README.md` - per-crate documentation (new files)
- `.github/workflows/ci.yml` - enhanced CI workflow
- `.github/dependabot.yml` - new dependency update automation
- Root `README.md` - badges and installation instructions
- `CHANGELOG.md` - new changelog file

### Dependencies Note
- `noodles-cram`, `noodles-sam`, `noodles-fasta` - currently use git fork from biodatageeks/noodles
- Git dependencies are acceptable for publication; crates.io will reference them as-is
- The rationale for the fork should be documented in relevant crate READMEs

## Success Criteria

- License files are present and properly referenced in Cargo.toml
- Documentation builds successfully with `cargo doc --no-deps`
- Enhanced CI workflows pass (including clippy and doc checks)
- Each crate has comprehensive README with usage examples
- Badges are properly displayed in root README
- All public APIs have documentation with examples
- Code follows clippy recommendations with no warnings
