# Bump DataFusion to 50.3.0

## Why
Upgrade Apache DataFusion from 48.0.1 to 50.3.0 to benefit from significant performance improvements (5x faster joins, 99% memory reduction), bug fixes across two major versions, and maintain compatibility with the current Apache Arrow ecosystem.

## What Changes
- **BREAKING**: Update workspace dependency `datafusion` from "48.0.1" to "50.3.0"
- **BREAKING**: Update workspace dependency `datafusion-execution` from "48.0.1" to "50.3.0"
- **BREAKING**: Update `rust-toolchain.toml` from Rust 1.85.0 to 1.86.0
- Update all table provider implementations for DataFusion 50.x API changes
- Fix ConfigOptions API usage (now returns `&Arc<ConfigOptions>`)
- Fix ProjectionExpr usage (changed from tuple to struct with `.expr` and `.alias`)
- Handle Hive partition auto-detection behavior in ListingTable
- Update physical execution plans for API compatibility
- Ensure all tests pass with DataFusion 50.x behavior

## Impact
- **Affected specs**: dependencies (CREATED)
- **Affected code**:
  - `rust-toolchain.toml` - Rust version bump
  - `Cargo.toml` - DataFusion version update in [workspace.dependencies]
  - `datafusion/bio-format-*/src/table_provider.rs` - API updates
  - `datafusion/bio-format-*/src/physical_exec.rs` - Execution plan updates
  - All workspace tests - Verification of compatibility

## Breaking Changes (48.x → 50.x)
1. **Rust MSRV**: Requires Rust 1.86.0 (current: 1.85.0)
2. **Hive Partition Auto-Detection**: `ListingTable` now auto-detects Hive partitions by default
3. **ConfigOptions API**: Returns `&Arc<ConfigOptions>` instead of `&ConfigOptions`
4. **ProjectionExpr**: Changed from tuple to named struct with `.expr` and `.alias` fields
5. **VARCHAR Mapping**: SQL `VARCHAR` now maps to `Utf8View` by default (configurable)

## Risks
- Compilation issues from breaking API changes may require iterative fixes
- Hive partition auto-detection behavioral change could affect code paths
- OpenDAL or other dependencies may have version constraints
- Test suite may reveal subtle behavioral differences

## Success Criteria
- All workspace crates compile with DataFusion 50.3.0
- All tests pass (with minimal adjustments if needed)
- CI pipeline passes (formatting, clippy, tests)
- Example programs run correctly
- No functional regressions
