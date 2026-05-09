## 1. Partition Planning

- [x] 1.1 Add a chunk-aligned row selection partition helper.
- [x] 1.2 Reuse or move `variant_position` chunk-size discovery for scan planning.
- [x] 1.3 Split VCF Zarr pruning into planning-time candidate pruning and partition-local exact fallback pruning.
- [x] 1.4 Add a small internal helper or option field that returns `CodecOptions::default().with_concurrent_target(1).with_chunk_concurrent_minimum(1)` for partition reads without mutating global zarrs config.
- [x] 1.5 Add unit tests for single partition, capped partitions, grouped chunks, sparse selections, empty selections, no intra-chunk splitting, and single-concurrency zarrs codec-option selection.

## 2. Execution Integration

- [x] 2.1 Update `VcfZarrTableProvider::scan` to read `target_partitions` and build partition selections after pruning.
- [x] 2.2 Update `VcfZarrExec` to store per-partition row selections.
- [x] 2.3 Update `PlanProperties` to advertise `UnknownPartitioning(effective_partition_count)`.
- [x] 2.4 Update `execute(partition)` to read only the selected rows for that partition.
- [x] 2.5 Update `execute(partition)` to apply exact fallback position-array pruning inside the partition when `region_index` was not used.
- [x] 2.6 Update `arrays.rs` read helpers to use zarrs `_opt` read methods with the partition's scoped `CodecOptions`.
- [x] 2.7 Make partition-local array reads serial over owned chunks or chunk subsets where practical, avoiding zarrs multi-chunk read APIs as the source of parallelism.
- [x] 2.8 Ensure empty selections produce a valid empty partition with the projected schema.

## 3. Documentation And Tests

- [x] 3.1 Document that parallel VCF Zarr output order is not guaranteed.
- [x] 3.2 Document that selected Zarr variant chunks are not split across partitions.
- [x] 3.3 Document that unused DataFusion target partition capacity is not handed to zarrs and that single-concurrency zarrs options do not guarantee OS-thread affinity beyond zarrs/rayon behavior.
- [x] 3.4 Add integration tests comparing equivalent results for `target_partitions=1` and `target_partitions>1`.
- [x] 3.5 Add an integration test that confirms the physical partition count is chunk-bounded and target-controlled.
- [x] 3.6 Add an integration test for fallback pruning without `region_index` in parallel mode.

## 4. Verification

- [x] 4.1 Run `cargo test -p datafusion-bio-format-vcf vcf_zarr`.
- [x] 4.2 Run `cargo fmt --all -- --check`.
- [x] 4.3 Run `openspec validate add-vcf-zarr-parallel-partitions --strict`.

## 5. Review Performance Follow-Up

- [x] 5.1 Reuse a shared local `FilesystemStore` for VCF Zarr array opens.
- [x] 5.2 Offload blocking zarrs reads from Tokio worker threads in `VcfZarrExec::execute`.
- [x] 5.3 Split partition output into RecordBatches capped by the DataFusion session batch size.
- [x] 5.4 Group contiguous selected FORMAT sample indexes into wider zarrs subset reads.
- [x] 5.5 Add unit tests for row-selection batch splitting and contiguous sample-span grouping.
- [x] 5.6 Add an integration test for session batch-sized VCF Zarr output.
