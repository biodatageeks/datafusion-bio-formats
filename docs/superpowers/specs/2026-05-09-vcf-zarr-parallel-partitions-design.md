# VCF Zarr Parallel Partition Scan Design

## Context

The VCF Zarr provider in `datafusion/bio-format-vcf/src/zarr/` already exposes local VCF Zarr stores through a DataFusion `TableProvider`. The current scan path builds a projection plan, performs row pruning from genomic predicates, and constructs a `VcfZarrExec`. That exec currently reports one physical partition and `execute()` reads the full selected `RowSelection` in a single partition.

The plain VCF provider already uses DataFusion session `target_partitions` to expose multiple execution partitions for indexed reads. VCF Zarr should follow the same user-facing control model, but its partition unit should be the Zarr variant chunk rather than a BGZF/index byte range.

## Goals

- Let VCF Zarr scans expose multiple DataFusion physical partitions.
- Control effective partition count with `SessionConfig::target_partitions()`.
- Preserve Zarr variant chunk boundaries; do not split inside a selected variant chunk in this change.
- Keep existing projection pruning, predicate pruning, sample selection, schema, and error behavior.
- Keep the implementation scoped to `datafusion-bio-format-vcf`.
- Document that parallel scans do not guarantee global row order.

## Non-Goals

- No polars-bio API changes.
- No cloud/object-storage VCF Zarr support.
- No intra-chunk splitting for stores with fewer selected chunks than `target_partitions`.
- No internal parallel array-read scheduler inside a single DataFusion partition.
- No global output order preservation for `target_partitions > 1`.

## Recommended Approach

Use chunk-aligned DataFusion execution partitions.

`VcfZarrTableProvider::scan` will continue to build the projection plan and row pruning first. After pruning, it will read `state.config().target_partitions()` and convert the selected `RowSelection` into per-partition `RowSelection`s. The split will be aligned to the first dimension chunk shape of `variant_position`.

`VcfZarrExec` will store `partition_selections: Vec<RowSelection>`, report `Partitioning::UnknownPartitioning(partition_selections.len())`, and have `execute(partition)` read only the row ranges for that partition.

This matches the plain VCF provider's session-controlled partition model while keeping the VCF Zarr implementation simple and chunk-aware.

## Components

### `planning.rs`

Add a partition-planning helper, either as methods on `RowSelection` or as a small dedicated type. It should:

- take `chunk_size` and `target_partitions`,
- preserve selected rows exactly,
- assign every selected Zarr variant chunk to at most one partition,
- group chunks when selected chunk count exceeds `target_partitions`,
- avoid empty partitions,
- return one empty partition for empty selections so DataFusion receives a valid physical plan.

The helper should not split a selected chunk even when `target_partitions` is larger than selected chunk count.

### `pruning.rs` / shared helper

The existing private `variant_chunk_size()` helper should be made reusable or moved so scan planning can use it. Errors should name `variant_position` because that array defines variant chunk boundaries for this feature.

### `table_provider.rs`

After `build_row_pruning`, call the chunk-size and partition-planning helpers. Pass the resulting partition selections into `VcfZarrExec::new`.

When `row_pruning.method == PruningMethod::RegionIndex`, continue adding `region_index` to the projection plan raw arrays as today.

### `physical_exec.rs`

Replace the single `row_selection` field with `partition_selections`. The exec should:

- set `UnknownPartitioning(partition_count)`,
- display partition count and selected row ranges in the plan string,
- bounds-check `execute(partition)`,
- read projected arrays using only the partition's `RowSelection`,
- return a valid empty stream for empty selections.

## Data Flow

1. DataFusion calls `VcfZarrTableProvider::scan`.
2. The provider builds projection dependencies from the requested columns and filters.
3. The provider builds exact selected row ranges from genomic pruning.
4. The provider obtains the `variant_position` chunk size.
5. The provider splits selected rows into chunk-aligned partition selections bounded by `target_partitions`.
6. The exec advertises that effective partition count.
7. DataFusion schedules `execute(partition)` for each partition.
8. Each partition reads only its selected row ranges and emits normal projected Arrow batches.

## Ordering

Parallel VCF Zarr scans do not guarantee original variant row order when effective partition count is greater than one. This is normal for DataFusion multi-partition execution. Users who require deterministic genomic order should add an explicit sort.

Tests for parallel output should compare sorted or set-equivalent results unless the test is explicitly verifying partition-local behavior.

## Edge Cases

- `target_partitions <= 1`: use one partition and preserve current behavior.
- selected chunks fewer than `target_partitions`: cap effective partitions to selected chunk count.
- selected rows are sparse within a chunk: preserve the exact row ranges but keep that chunk assigned to one partition.
- empty selection: return one valid empty partition with the projected schema.
- invalid `variant_position` chunk metadata: fail with a clear `DataFusionError::Execution`.

## Alternatives Considered

### Internal parallel reads inside one partition

Rejected for the first implementation. It would hide concurrency from DataFusion, diverge from the plain VCF provider's `target_partitions` model, and be harder to validate through execution plans.

### Exact row-balanced partitioning

Rejected because it can split inside Zarr chunks and cause repeated chunk decompression or worse I/O behavior. Chunk preservation is the correct default for Zarr-backed scans.

## Testing

Add unit tests for partition planning:

- one partition when `target_partitions` is one,
- chunk-boundary preservation,
- partition count capped by selected chunk count,
- grouped chunks when selected chunks exceed target,
- sparse selections,
- empty selections,
- no intra-chunk splitting when target is larger than selected chunks.

Add integration tests in `vcf_zarr_provider_test.rs`:

- same query with `target_partitions=1` and `target_partitions=4` returns equivalent rows after sorting,
- physical plan or exec downcast reports the expected chunk-bounded partition count,
- projection and predicate pruning still work in parallel mode.

Verification before implementation completion:

- `cargo test -p datafusion-bio-format-vcf vcf_zarr`
- `cargo fmt --all -- --check`
- `openspec validate add-vcf-zarr-parallel-partitions --strict`
