# Tasks: Bump DataFusion to 50.3.0

## Preparation Phase

### 1. Update Rust Toolchain
- [x] Update `rust-toolchain.toml` from 1.85.0 to 1.86.0
- [x] Verify rustfmt component is still available for 1.86.0
- [x] Test local compilation with new toolchain
- **Validation**: `rustc --version` shows 1.86.0

### 2. Update Workspace Dependencies
- [x] Update `datafusion` from "48.0.1" to "50.3.0" in Cargo.toml [workspace.dependencies]
- [x] Update `datafusion-execution` from "48.0.1" to "50.3.0"
- [x] Run `cargo update` to fetch new versions
- [x] Review Cargo.lock changes for any unexpected dependency shifts
- **Validation**: `cargo tree | grep datafusion` shows version 50.3.0

## Compilation Phase

### 3. Fix Core Crate (bio-format-core)
- [x] Attempt compilation: `cargo build --package datafusion-bio-format-core`
- [x] Address any ConfigOptions API changes (`.as_ref()` where needed)
- [x] Fix any ProjectionExpr tuple-to-struct changes if present
- [x] Fix any other breaking API changes
- **Validation**: Core crate compiles without errors

### 4. Fix Individual Format Crates
- [x] **FASTQ**: `cargo build --package datafusion-bio-format-fastq`
  - Fix table_provider.rs compilation errors
  - Fix physical_exec.rs compilation errors
  - Fix any schema or execution plan changes
- [x] **VCF**: `cargo build --package datafusion-bio-format-vcf`
  - Address any INFO/FORMAT field handling changes
  - Fix table provider and execution plan issues
- [x] **BAM**: `cargo build --package datafusion-bio-format-bam`
  - Fix any alignment-specific execution changes
- [x] **BED**: `cargo build --package datafusion-bio-format-bed`
  - Update table provider if needed
- [x] **GFF**: `cargo build --package datafusion-bio-format-gff`
  - Fix attribute parsing integration if affected
- [x] **FASTA**: `cargo build --package datafusion-bio-format-fasta`
  - Address sequence handling changes if any
- [x] **CRAM**: `cargo build --package datafusion-bio-format-cram`
  - Fix any CRAM-specific issues
- **Validation**: All format crates compile successfully

### 5. Full Workspace Compilation
- [x] Run `cargo build --workspace` to ensure all crates compile together
- [x] Run `cargo clippy --workspace` to catch any new linter warnings
- [x] Run `cargo fmt --all -- --check` to verify formatting
- **Validation**: Clean workspace build with no errors or warnings

## Testing Phase

### 6. Unit Tests
- [x] Run `cargo test --package datafusion-bio-format-core`
- [x] Run `cargo test --package datafusion-bio-format-fastq`
- [x] Run `cargo test --package datafusion-bio-format-vcf`
- [x] Run `cargo test --package datafusion-bio-format-bam`
- [x] Run `cargo test --package datafusion-bio-format-bed`
- [x] Run `cargo test --package datafusion-bio-format-gff`
- [x] Run `cargo test --package datafusion-bio-format-fasta`
- [x] Run `cargo test --package datafusion-bio-format-cram`
- [x] Address any test failures or behavioral changes
- **Validation**: All unit tests pass

### 7. Integration Tests
- [x] Run full workspace test suite: `cargo test --workspace`
- [x] Check for any test regressions or new failures
- [x] Investigate and fix any Hive partition auto-detection issues
- [x] Verify no VARCHAR to Utf8View mapping issues in tests
- **Validation**: Complete test suite passes

### 8. Example Programs
- [x] Test FASTQ example: `cargo run --example test_reader --package datafusion-bio-format-fastq`
- [x] Test VCF example: `cargo run --example test_reader --package datafusion-bio-format-vcf`
- [x] Test any performance benchmarks if they exist
- [x] Verify examples produce expected output
- **Validation**: All examples run without errors

## Verification Phase

### 9. CI/CD Validation
- [x] Ensure CI workflow runs with Rust 1.86.0
- [x] Verify CI passes all checks (fmt, clippy, tests)
- [x] Review CI logs for any warnings or deprecation notices
- **Validation**: Green CI build

### 10. Documentation Updates
- [x] Update CLAUDE.md to reflect DataFusion 50.3.0 in Tech Stack section
- [x] Update openspec/project.md Tech Stack section
- [x] Add any migration notes to documentation if needed
- [x] Update README.md if it references DataFusion version
- **Validation**: Documentation is current and accurate

## Completion Criteria
- [x] All workspace crates compile with DataFusion 50.3.0
- [x] All tests pass without regression
- [x] Example programs run successfully
- [x] CI pipeline is green
- [x] Documentation is updated
- [x] No clippy warnings introduced
- [x] Code formatting is correct

## Rollback Plan
If critical issues arise:
1. Revert Cargo.toml changes to DataFusion 48.0.1
2. Revert rust-toolchain.toml to 1.85.0
3. Run `cargo update` to restore previous Cargo.lock
4. Rebuild and test to confirm rollback successful

## Notes
- **Parallelizable**: Format crate fixes (Task 4) can be done in parallel after core is fixed
- **Blocking**: Task 3 (core crate) blocks Task 4 (format crates)
- **Blocking**: Tasks 3-5 must complete before testing phase (Tasks 6-8)
- **Watch for**: ConfigOptions API changes are most likely to appear in table provider code
- **Watch for**: Any Hive partitioning behavior if listing tables are used
