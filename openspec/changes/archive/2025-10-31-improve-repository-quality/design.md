# Design: Prepare Crates for Publication

## Context

The datafusion-bio-formats project is a Rust workspace providing bioinformatics file format support for Apache DataFusion. The workspace currently contains 8 crates (core + 7 format crates) that are not yet published to crates.io. Publishing these crates will:

1. Increase visibility in the Rust ecosystem
2. Enable easier adoption by other projects (including polars-bio)
3. Establish professional standards for documentation and release management
4. Provide reliable, versioned dependencies for downstream users

**Current State:**
- Crates lack required metadata (license, description, etc.)
- No semantic versioning policy
- Minimal or missing per-crate documentation
- CI/CD lacks publishing workflows
- Git dependencies on noodles fork need resolution

**Stakeholders:**
- Project maintainers (biodatageeks team)
- Downstream consumers (polars-bio, other bioinformatics tools)
- Rust bioinformatics community

## Goals / Non-Goals

### Goals
1. All workspace crates pass `cargo publish --dry-run` validation
2. Comprehensive documentation enables users to adopt crates without reading source
3. Automated CI/CD reduces manual effort for releases
4. Clear versioning policy guides future development
5. Professional presentation increases community trust

### Non-Goals
1. Changing existing APIs or behavior (this is purely additive)
2. Performance optimization or refactoring
3. Adding new features or file format support
4. Migrating to different dependencies (beyond resolving git deps)
5. Publishing to registries other than crates.io

## Decisions

### Decision 1: Dual MIT/Apache-2.0 Licensing
**Choice:** Use standard Rust dual licensing (MIT OR Apache-2.0)

**Rationale:**
- Industry standard for Rust projects
- Maximizes compatibility with other projects
- Aligns with Apache DataFusion's licensing
- Provides flexibility for both permissive and patent protection concerns

**Alternatives Considered:**
- Single MIT license: Simpler but lacks patent protection
- Single Apache-2.0: Less familiar to some users
- GPL/AGPL: Would limit adoption in commercial settings

### Decision 2: Initial Version 0.1.0
**Choice:** Start all crates at version 0.1.0

**Rationale:**
- Follows SemVer convention for pre-1.0 projects
- Signals API stability is not yet guaranteed
- Allows breaking changes in 0.x releases (via MINOR version bumps)
- Gives time to gather user feedback before committing to 1.0 stability

**Alternatives Considered:**
- Start at 1.0.0: Premature commitment to API stability
- Use 0.0.x: Implies extreme instability

### Decision 3: Workspace Metadata Inheritance
**Choice:** Use [workspace.package] to share common metadata

**Rationale:**
- Reduces duplication across 8 crates
- Ensures consistency (all crates use same license, repo URL, etc.)
- Easier to maintain and update
- Native Cargo feature, no external tooling required

**Implementation:**
```toml
# Root Cargo.toml
[workspace.package]
license = "MIT OR Apache-2.0"
repository = "https://github.com/biodatageeks/datafusion-bio-formats"
homepage = "https://github.com/biodatageeks/datafusion-bio-formats"
authors = ["BiodataGeeks Team"]
edition = "2024"

# Member crate Cargo.toml
[package]
name = "datafusion-bio-format-fastq"
version = "0.1.0"
license.workspace = true
repository.workspace = true
homepage.workspace = true
authors.workspace = true
edition.workspace = true
```

### Decision 4: Sequential Publishing with Delays
**Choice:** Publish crates sequentially with 30-60 second delays

**Rationale:**
- Crates.io index updates are not instantaneous
- Publishing bio-format-fastq immediately after bio-format-core may fail
- Dependent crates need the index to resolve dependencies
- Manual workflow_dispatch trigger allows control over timing

**Alternatives Considered:**
- Parallel publishing: Would fail due to unresolved dependencies
- Automated tag-based publishing: Risky without thorough testing first
- Single monolithic crate: Loses modularity and increases compilation time

### Decision 5: Accept Git Dependencies for Publication
**Choice:** Publish crates with existing git dependencies from biodatageeks/noodles fork

**Rationale:**
- Crates.io supports git dependencies in published crates
- Git dependencies are resolved at build time for downstream users
- Fork contains necessary functionality not yet in upstream noodles
- Documenting the fork rationale is sufficient for transparency

**Implementation:**
- Keep existing git dependencies in Cargo.toml
- Document why the fork is necessary in affected crate READMEs (bio-format-cram, bio-format-fasta)
- Clearly explain what functionality the fork provides
- Note: Users will need git access to build, which is standard for Rust ecosystem

### Decision 6: Comprehensive Per-Crate READMEs
**Choice:** Each crate gets its own README with format-specific examples

**Rationale:**
- Displayed on crates.io page for each crate
- Users often discover individual crates, not the workspace
- Format-specific examples are more useful than generic ones
- Enables standalone adoption of individual crates

**Structure:**
```markdown
# datafusion-bio-format-fastq

Brief description.

## Installation
\`\`\`toml
[dependencies]
datafusion-bio-format-fastq = "0.1.0"
\`\`\`

## Usage
\`\`\`rust
// Format-specific example
\`\`\`

## Documentation
[Full API docs](https://docs.rs/datafusion-bio-format-fastq)

## License
Dual MIT/Apache-2.0
```

### Decision 7: Enhanced CI with Clippy and Doc Checks
**Choice:** Add clippy and documentation builds to CI

**Rationale:**
- Clippy catches common mistakes before merge
- Doc builds prevent broken documentation from being published
- Enforcing `-D warnings` maintains high quality
- Minimal CI time overhead (<1 minute additional)

**Configuration:**
```yaml
- name: Run clippy
  run: cargo clippy --all-targets --all-features -- -D warnings

- name: Build documentation
  run: cargo doc --no-deps --all-features
  env:
    RUSTDOCFLAGS: "-D warnings"
```

### Decision 8: Dependabot for Dependency Updates
**Choice:** Use GitHub Dependabot for automated dependency PRs

**Rationale:**
- Native GitHub feature, no external service
- Catches security vulnerabilities quickly
- Reduces manual maintenance burden
- Configurable update frequency

**Configuration:**
```yaml
version: 2
updates:
  - package-ecosystem: "cargo"
    directory: "/"
    schedule:
      interval: "monthly"
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "monthly"
```

## Risks / Trade-offs

### Risk 1: Git Dependencies Complicate User Builds
**Severity:** Low
**Mitigation:**
- Clearly document git dependencies in crate READMEs
- Most Rust developers are familiar with git dependencies
- Git dependencies are resolved automatically by Cargo
- Consider contributing changes upstream in the future

### Risk 2: Breaking Changes in 0.x Versions Disrupt Users
**Severity:** Medium
**Mitigation:**
- Document breaking changes clearly in CHANGELOG
- Use yanking for critically broken versions
- Provide migration guides for major API changes
- Consider stability as we approach 1.0

### Risk 3: Documentation Quality Burden
**Severity:** Medium
**Mitigation:**
- Use existing examples as documentation base
- Leverage AI tools for initial doc generation
- Iteratively improve based on user questions
- Add `#![warn(missing_docs)]` to catch gaps early

### Risk 4: CI/CD Token Security
**Severity:** Medium
**Mitigation:**
- Use GitHub secrets for CARGO_REGISTRY_TOKEN
- Scope token to specific crates if crates.io supports it
- Use workflow_dispatch (manual trigger) rather than auto-publish
- Rotate token if compromised

### Risk 5: Publishing Order Failures
**Severity:** Low
**Mitigation:**
- Dry-run all publishes first
- Implement delays between publishes
- Monitor crates.io index updates
- Have rollback plan (yank if needed)

## Migration Plan

### Phase 1: Preparation (No User Impact)
1. Add metadata to all Cargo.toml files
2. Create LICENSE files
3. Write documentation and READMEs (including git dependency documentation)
4. Enhance CI workflows
5. Run `cargo publish --dry-run` on all crates

**Duration:** 1-2 weeks
**Validation:** All dry-runs pass, CI green

### Phase 2: Initial Publication
1. Create Git tags for v0.1.0
2. Publish bio-format-core
3. Wait for index update
4. Publish format crates sequentially
5. Verify docs.rs builds

**Duration:** 1-2 hours
**Validation:** All crates visible on crates.io, docs.rs shows documentation

### Phase 3: Post-Publication
1. Update README with real badge URLs
2. Announce release to community
3. Monitor for issues or questions
4. Iterate on documentation based on feedback

**Duration:** Ongoing
**Validation:** Positive community feedback, adoption metrics

### Rollback
If publication fails:
1. Use `cargo yank` to hide broken versions
2. Fix issues locally
3. Increment PATCH version
4. Re-publish fixed version

If critical issues found post-publication:
1. Yank affected version immediately
2. Publish fixed version ASAP
3. Document issue in CHANGELOG
4. Notify known users if possible

## Open Questions

1. **Authors field:** Should we list individual contributors or use a team name?
   - **Recommendation:** Use "BiodataGeeks Team" initially, add individuals if they prefer

2. **Keywords:** What are the best keywords for bioinformatics discoverability?
   - **Recommendation:** Research popular crates in science category, use community terminology

3. **Categories:** Should we request a "bioinformatics" category from crates.io?
   - **Recommendation:** Start with "science", open discussion with crates.io maintainers

4. **Feature flags:** Should we split cloud storage into optional features immediately?
   - **Recommendation:** Yes, add features to bio-format-core: ["gcs", "s3", "azure", "default"]

5. **Changelog format:** Per-crate CHANGELOG or single workspace CHANGELOG?
   - **Recommendation:** Single workspace CHANGELOG, easier to maintain with cross-references

6. **Publication cadence:** How often should we release new versions?
   - **Recommendation:** As needed for bug fixes, monthly for feature releases

7. **MSRV policy:** Should we guarantee a minimum supported Rust version?
   - **Recommendation:** Document MSRV in README, test in CI if feasible
