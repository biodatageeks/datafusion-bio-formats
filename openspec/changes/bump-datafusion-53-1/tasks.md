## 1. Implementation
- [x] 1.1 Update workspace dependency versions in `Cargo.toml`.
- [x] 1.2 Run `cargo update -p datafusion --precise 53.1.0`.
- [x] 1.3 Update custom execution plans to return `&Arc<PlanProperties>`.
- [x] 1.4 Replace custom `statistics` implementations with `partition_statistics`.
- [x] 1.5 Compile and fix DataFusion 53 API errors in all workspace crates.
- [x] 1.6 Run `cargo fmt --all`.
- [x] 1.7 Run `cargo check --workspace --all-targets`.
- [x] 1.8 Run focused package tests for all bio-format crates.
- [x] 1.9 Run `openspec validate bump-datafusion-53-1 --strict`.
