## 1. Partition Planning

- [ ] 1.1 Add a chunk-aligned row selection partition helper.
- [ ] 1.2 Reuse or move `variant_position` chunk-size discovery for scan planning.
- [ ] 1.3 Split VCF Zarr pruning into planning-time candidate pruning and partition-local exact fallback pruning.
- [ ] 1.4 Add a small internal helper or option field that returns `CodecOptions::default().with_concurrent_target(1).with_chunk_concurrent_minimum(1)` for partition reads without mutating global zarrs config.
- [ ] 1.5 Add unit tests for single partition, capped partitions, grouped chunks, sparse selections, empty selections, no intra-chunk splitting, and single-concurrency zarrs codec-option selection.

## 2. Execution Integration

- [ ] 2.1 Update `VcfZarrTableProvider::scan` to read `target_partitions` and build partition selections after pruning.
- [ ] 2.2 Update `VcfZarrExec` to store per-partition row selections.
- [ ] 2.3 Update `PlanProperties` to advertise `UnknownPartitioning(effective_partition_count)`.
- [ ] 2.4 Update `execute(partition)` to read only the selected rows for that partition.
- [ ] 2.5 Update `execute(partition)` to apply exact fallback position-array pruning inside the partition when `region_index` was not used.
- [ ] 2.6 Update `arrays.rs` read helpers to use zarrs `_opt` read methods with the partition's scoped `CodecOptions`.
- [ ] 2.7 Make partition-local array reads serial over owned chunks or chunk subsets where practical, avoiding zarrs multi-chunk read APIs as the source of parallelism.
- [ ] 2.8 Ensure empty selections produce a valid empty partition with the projected schema.

## 3. Documentation And Tests

- [ ] 3.1 Document that parallel VCF Zarr output order is not guaranteed.
- [ ] 3.2 Document that selected Zarr variant chunks are not split across partitions.
- [ ] 3.3 Document that unused DataFusion target partition capacity is not handed to zarrs and that single-concurrency zarrs options do not guarantee OS-thread affinity beyond zarrs/rayon behavior.
- [ ] 3.4 Add integration tests comparing equivalent results for `target_partitions=1` and `target_partitions>1`.
- [ ] 3.5 Add an integration test that confirms the physical partition count is chunk-bounded and target-controlled.
- [ ] 3.6 Add an integration test for fallback pruning without `region_index` in parallel mode.

## 4. Verification

- [ ] 4.1 Run `cargo test -p datafusion-bio-format-vcf vcf_zarr`.
- [ ] 4.2 Run `cargo fmt --all -- --check`.
- [ ] 4.3 Run `openspec validate add-vcf-zarr-parallel-partitions --strict`.
