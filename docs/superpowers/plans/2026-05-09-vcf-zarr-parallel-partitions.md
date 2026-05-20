# VCF Zarr Parallel Partitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make VCF Zarr scans expose chunk-aligned DataFusion physical partitions controlled by `target_partitions`, while keeping zarrs reads single-concurrency inside each DataFusion stream.

**Architecture:** Scan planning computes chunk-aligned candidate partitions from `variant_position` chunk metadata. Region-index pruning remains planning-time, fallback position-array pruning becomes partition-local, and `VcfZarrExec::execute(partition)` reads only its assigned rows with scoped zarrs `CodecOptions`.

**Tech Stack:** Rust, DataFusion `TableProvider`/`ExecutionPlan`, Arrow `RecordBatch`, zarrs `CodecOptions`, OpenSpec.

---

### Task 1: Partition Planning And Read Options

**Files:**
- Modify: `datafusion/bio-format-vcf/src/zarr/planning.rs`
- Modify: `datafusion/bio-format-vcf/src/zarr/pruning.rs`
- Test: `datafusion/bio-format-vcf/src/zarr/planning.rs`

- [ ] **Step 1: Add failing tests for chunk-aligned partition planning**

Add unit tests under `#[cfg(test)] mod tests` in `planning.rs`:

```rust
#[test]
fn partitions_preserve_chunks_and_cap_at_selected_chunk_count() {
    let selection = RowSelection { ranges: vec![0..30] };
    let partitions = selection
        .chunk_aligned_partitions(10, 8, PartitioningMode::ExactRows)
        .expect("partitioning should succeed");
    assert_eq!(
        partitions.iter().map(|p| p.selection.ranges.clone()).collect::<Vec<_>>(),
        vec![vec![0..10], vec![10..20], vec![20..30]]
    );
}
```

Also add tests for grouping 5 chunks into 2 partitions, sparse ranges inside one chunk staying in one partition, empty selections returning one empty partition, invalid zero chunk size, and `zarr_read_options()` producing `concurrent_target == 1` and `chunk_concurrent_minimum == 1`.

- [ ] **Step 2: Run failing tests**

Run: `cargo test -p datafusion-bio-format-vcf zarr::planning::tests --lib`

Expected: FAIL because `PartitioningMode`, `PartitionRowSelection`, `chunk_aligned_partitions`, and `zarr_read_options` do not exist yet.

- [ ] **Step 3: Implement planning helpers**

Add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitioningMode {
    ExactRows,
    ChunkCandidates,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionRowSelection {
    pub selection: RowSelection,
    pub mode: PartitioningMode,
}
```

Implement `RowSelection::chunk_aligned_partitions(chunk_size, target_partitions, mode) -> Result<Vec<PartitionRowSelection>>` that maps selected rows to chunk buckets, never splits a selected chunk, groups adjacent chunk buckets when selected chunk count exceeds target, avoids empty partitions, and returns one empty partition for empty selections.

Add:

```rust
pub(crate) fn zarr_read_options() -> zarrs::array::codec::CodecOptions {
    zarrs::array::codec::CodecOptions::default()
        .with_concurrent_target(1)
        .with_chunk_concurrent_minimum(1)
}
```

Move or expose `variant_chunk_size(metadata)` from `pruning.rs` as `pub(crate)` with an error mentioning `variant_position`.

- [ ] **Step 4: Run green tests**

Run: `cargo test -p datafusion-bio-format-vcf zarr::planning::tests --lib`

Expected: PASS.

- [ ] **Step 5: Run VCF Zarr regression gate**

Run: `cargo test -p datafusion-bio-format-vcf vcf_zarr`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-format-vcf/src/zarr/planning.rs datafusion/bio-format-vcf/src/zarr/pruning.rs
git commit -m "Add VCF Zarr partition planning helpers"
```

### Task 2: Planning-Time Candidate Pruning

**Files:**
- Modify: `datafusion/bio-format-vcf/src/zarr/pruning.rs`
- Modify: `datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Test: `datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add failing tests for deferred fallback pruning plan shape**

Update the no-`region_index` scan test so the plan reports `pruning=position_arrays`, candidate chunk ranges rather than exact `1..3` rows, and no `region_index` raw array. Add a second test that uses a `LIMIT 1` fallback-filtered scan and asserts collection still returns one exact row after execution.

- [ ] **Step 2: Run failing tests**

Run: `cargo test -p datafusion-bio-format-vcf vcf_zarr_scan_without_region_index_uses_position_array_pruning vcf_zarr_collects_fallback_pruned_limit_without_region_index`

Expected: FAIL because fallback pruning still performs exact planning-time row reads and execution has no deferred pruning.

- [ ] **Step 3: Split pruning responsibilities**

Add `RowPruning` data sufficient for execution to know whether exact fallback pruning is deferred, including the original predicate constraints and limit. Keep `region_index` exact-enough candidate pruning in scan. For fallback without `region_index`, return chunk-candidate rows and mark them as `PartitioningMode::ChunkCandidates`.

- [ ] **Step 4: Wire table provider to partition candidates**

In `scan`, obtain `target_partitions`, compute `variant_chunk_size`, and build `PartitionRowSelection` values after planning-time pruning. Pass partitions and deferred pruning context to `VcfZarrExec::new`.

- [ ] **Step 5: Run green tests**

Run the two focused tests from Step 2, then `cargo test -p datafusion-bio-format-vcf vcf_zarr`.

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-format-vcf/src/zarr/pruning.rs datafusion/bio-format-vcf/src/zarr/table_provider.rs datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git commit -m "Defer VCF Zarr fallback pruning to partitions"
```

### Task 3: Multi-Partition Exec Integration

**Files:**
- Modify: `datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
- Modify: `datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Test: `datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add failing tests for physical partition count**

Add integration tests that call `scan` with `target_partitions = 8` and assert a full scan over the 1,000-row fixture exposes the expected chunk-bounded partition count. Add another test using a copied fixture with only five selected chunks and assert 5 partitions, not 8.

- [ ] **Step 2: Run failing tests**

Run the new focused partition-count tests.

Expected: FAIL because `VcfZarrExec` still advertises `UnknownPartitioning(1)`.

- [ ] **Step 3: Store partition selections in exec**

Replace `row_selection` with `partition_selections: Vec<PartitionRowSelection>`, set `PlanProperties` with `UnknownPartitioning(partition_selections.len())`, bounds-check `execute(partition)`, and display partition count plus partition ranges.

- [ ] **Step 4: Make execute read the requested partition**

Use only `partition_selections[partition]` when reading projected arrays. Return a valid empty batch stream for empty selections.

- [ ] **Step 5: Run green tests**

Run focused partition-count tests, then `cargo test -p datafusion-bio-format-vcf vcf_zarr`.

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-format-vcf/src/zarr/physical_exec.rs datafusion/bio-format-vcf/src/zarr/table_provider.rs datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git commit -m "Expose VCF Zarr chunk partitions in exec"
```

### Task 4: Scoped zarrs Reads And Partition-Local Filtering

**Files:**
- Modify: `datafusion/bio-format-vcf/src/zarr/arrays.rs`
- Modify: `datafusion/bio-format-vcf/src/zarr/pruning.rs`
- Modify: `datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
- Test: `datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add failing tests for equivalent results and scoped read options**

Add integration tests comparing sorted results for `target_partitions=1` and `target_partitions=4`, including projected columns and fallback pruning without `region_index`. Add a unit test or instrumentation-friendly helper test proving the read options helper returns single-concurrency settings.

- [ ] **Step 2: Run failing tests**

Run the new focused tests.

Expected: FAIL until partition-local exact fallback pruning and scoped zarrs options are wired through all read helpers.

- [ ] **Step 3: Thread scoped codec options through array reads**

Change `read_projected_arrays` and private read helpers to accept `&CodecOptions`. Replace `retrieve_array_subset` calls with `_opt` variants and invoke zarrs serially over row ranges/chunk subsets where practical.

- [ ] **Step 4: Implement partition-local exact fallback pruning**

When a partition is in candidate mode, read lightweight arrays for that partition with scoped options, build the exact mask, apply the scan limit in global stream semantics where possible, and then read projected arrays for the exact partition rows.

- [ ] **Step 5: Run green tests**

Run focused tests, `cargo test -p datafusion-bio-format-vcf vcf_zarr`, `cargo fmt --all -- --check`, and `openspec validate add-vcf-zarr-parallel-partitions --strict`.

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-format-vcf/src/zarr/arrays.rs datafusion/bio-format-vcf/src/zarr/pruning.rs datafusion/bio-format-vcf/src/zarr/physical_exec.rs datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git commit -m "Use scoped zarrs reads for VCF Zarr partitions"
```

### Task 5: Documentation Checklist And Final Verification

**Files:**
- Modify: `openspec/changes/add-vcf-zarr-parallel-partitions/tasks.md`

- [ ] **Step 1: Update OpenSpec task checklist**

Mark implemented tasks complete only after the code and tests are green.

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo test -p datafusion-bio-format-vcf vcf_zarr
cargo fmt --all -- --check
openspec validate add-vcf-zarr-parallel-partitions --strict
```

Expected: PASS.

- [ ] **Step 3: Commit checklist**

```bash
git add openspec/changes/add-vcf-zarr-parallel-partitions/tasks.md
git commit -m "Complete VCF Zarr parallel partition tasks"
```
