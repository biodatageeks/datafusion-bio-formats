# VCF Zarr Parallel Partition Scan Design

## Context

The VCF Zarr provider in `datafusion/bio-format-vcf/src/zarr/` already exposes local VCF Zarr stores through a DataFusion `TableProvider`. The current scan path builds a projection plan, performs row pruning from genomic predicates, and constructs a `VcfZarrExec`. That exec currently reports one physical partition and `execute()` reads the full selected `RowSelection` in a single partition.

The plain VCF provider already uses DataFusion session `target_partitions` to expose multiple execution partitions for indexed reads. VCF Zarr should follow the same user-facing control model, but its partition unit should be the Zarr variant chunk rather than a BGZF/index byte range.

## Goals

- Let VCF Zarr scans expose multiple DataFusion physical partitions.
- Control effective partition count with `SessionConfig::target_partitions()`.
- Preserve Zarr variant chunk boundaries; do not split inside a selected variant chunk in this change.
- Keep existing projection pruning, predicate pruning, sample selection, schema, and error behavior.
- Avoid making fallback position-array pruning a single-threaded scan-planning bottleneck.
- Avoid nested all-core zarrs reads inside each DataFusion partition by using scoped per-operation zarrs codec options.
- Keep the implementation scoped to `datafusion-bio-format-vcf`.
- Document that parallel scans do not guarantee global row order.
- Document that zarrs concurrency is controlled as a target/limit, not as an exact process-wide thread-count guarantee.

## Non-Goals

- No polars-bio API changes.
- No cloud/object-storage VCF Zarr support.
- No intra-chunk splitting for stores with fewer selected chunks than `target_partitions`.
- No internal parallel array-read scheduler inside a single DataFusion partition.
- No global output order preservation for `target_partitions > 1`.
- No public user-facing option for zarrs inner codec concurrency in this change.
- No mutation of process-global zarrs configuration.
- No exact thread-count guarantee.

## Recommended Approach

Use chunk-aligned DataFusion execution partitions.

`VcfZarrTableProvider::scan` will continue to build the projection plan first. It will then read `state.config().target_partitions()` and create chunk-aligned partition selections using the first dimension chunk shape of `variant_position`.

Pruning has two execution modes:

- `region_index` pruning may run during `scan` because it reads compact chunk metadata and can cheaply narrow candidate chunks before partition planning.
- fallback position-array pruning must not perform a full exact row-pruning pass during `scan`; instead, it should partition candidate chunks first and apply exact `variant_contig`, `variant_position`, and `variant_length` filtering inside each partition's `execute(partition)`.

`VcfZarrExec` will store `partition_selections: Vec<RowSelection>`, report `Partitioning::UnknownPartitioning(partition_selections.len())`, and have `execute(partition)` read only the row ranges for that partition.

Partition reads should use zarrs `_opt` APIs with scoped `CodecOptions`. For multi-partition scans, the first implementation should reduce zarrs inner codec concurrency within each DataFusion partition, initially with `CodecOptions::default().with_concurrent_target(1)`, so DataFusion owns coarse scan parallelism and zarrs does not start all-core work inside every partition. Single-effective-partition scans may keep the current default zarrs behavior unless implementation testing shows a need to apply the same scoped target uniformly.

The implementation must not call `zarrs::config::global_config_mut()` for this feature. `datafusion-bio-formats` is a library, and mutating zarrs global configuration would affect unrelated scans or concurrent zarrs users in the same process.

This matches the plain VCF provider's session-controlled partition model while keeping the VCF Zarr implementation simple, chunk-aware, and explicit about nested concurrency.

## Components

### `planning.rs`

Add a partition-planning helper, either as methods on `RowSelection` or as a small dedicated type. It should:

- take `chunk_size` and `target_partitions`,
- preserve selected rows exactly when it receives exact selected rows,
- also support chunk candidate selections that will be filtered exactly during partition execution,
- assign every selected Zarr variant chunk to at most one partition,
- group chunks when selected chunk count exceeds `target_partitions`,
- avoid empty partitions,
- return one empty partition for empty selections so DataFusion receives a valid physical plan.

The helper should not split a selected chunk even when `target_partitions` is larger than selected chunk count.

### `pruning.rs` / shared helper

The existing private `variant_chunk_size()` helper should be made reusable or moved so scan planning can use it. Errors should name `variant_position` because that array defines variant chunk boundaries for this feature.

Pruning should be split into planning-time and execution-time responsibilities:

- planning-time `region_index` pruning returns chunk-aligned candidate rows or chunks;
- fallback planning without `region_index` returns chunk-aligned candidates without scanning all position arrays;
- partition execution applies exact predicate pruning for its assigned candidate rows by reading only that partition's lightweight arrays before reading heavy projected arrays.

Fallback pruning should avoid sparse tiny array reads after it has identified exact rows inside a partition. Prefer reading chunk-aligned lightweight arrays first, building a partition-local exact selection, then reading projected arrays through the same scoped read path and filtering in memory where that avoids repeated partial chunk decompression.

### `table_provider.rs`

Call the chunk-size and partition-planning helpers after projection planning. If `region_index` can narrow candidates, use it before partitioning. If not, partition the relevant chunk candidates and defer exact position-array pruning to `execute(partition)`. Pass the resulting partition selections and pruning mode into `VcfZarrExec::new`.

When `row_pruning.method == PruningMethod::RegionIndex`, continue adding `region_index` to the projection plan raw arrays as today.

### `physical_exec.rs`

Replace the single `row_selection` field with `partition_selections` plus enough pruning context to apply deferred fallback pruning. The exec should:

- set `UnknownPartitioning(partition_count)`,
- display partition count and selected row ranges in the plan string,
- bounds-check `execute(partition)`,
- for deferred fallback pruning, read lightweight arrays for the partition, build an exact partition-local selection, then read projected arrays for that exact selection,
- read projected arrays using only the partition's exact `RowSelection`,
- build and pass scoped zarrs `CodecOptions` into projected-array and fallback-pruning reads,
- return a valid empty stream for empty selections.

### `arrays.rs`

Thread scoped zarrs codec options through the VCF Zarr array-read helpers. Replace direct calls to `retrieve_array_subset` with `retrieve_array_subset_opt` so reads performed by `execute(partition)` use the partition's explicit `CodecOptions`.

## Data Flow

1. DataFusion calls `VcfZarrTableProvider::scan`.
2. The provider builds projection dependencies from the requested columns and filters.
3. The provider identifies planning-time chunk candidates. With `region_index`, this can use region-index metadata; without `region_index`, this should avoid scanning all position arrays during planning.
4. The provider obtains the `variant_position` chunk size.
5. The provider splits candidate rows or chunks into chunk-aligned partition selections bounded by `target_partitions`.
6. The exec advertises that effective partition count.
7. DataFusion schedules `execute(partition)` for each partition.
8. Each partition applies any deferred exact fallback pruning locally.
9. Each partition reads only its exact selected row ranges using scoped zarrs codec options.
10. Each partition emits normal projected Arrow batches.

## Ordering

Parallel VCF Zarr scans do not guarantee original variant row order when effective partition count is greater than one. This is normal for DataFusion multi-partition execution. Users who require deterministic genomic order should add an explicit sort.

Tests for parallel output should compare sorted or set-equivalent results unless the test is explicitly verifying partition-local behavior.

## Zarrs Concurrency

DataFusion `target_partitions` controls the number of physical VCF Zarr scan partitions exposed to the query engine. zarrs also has internal chunk/codec concurrency for individual array read operations. This design keeps those layers explicit:

- DataFusion owns outer scan parallelism through physical partitions.
- zarrs inner concurrency is controlled per read operation with `CodecOptions`.
- multi-partition scans should use a low zarrs inner concurrent target per partition to avoid oversubscription;
- process-global zarrs config must not be modified;
- the configured zarrs concurrent target is not a hard guarantee for the total number of OS threads used by the full query.

This is intentionally similar to zarrs guidance for concurrent external tasks: when many tasks are executing, reduce the codec concurrent target inside each task.

## Edge Cases

- `target_partitions <= 1`: use one partition and preserve current behavior.
- selected chunks fewer than `target_partitions`: cap effective partitions to selected chunk count.
- selected rows are sparse within a chunk: preserve the exact row ranges but keep that chunk assigned to one partition.
- empty selection: return one valid empty partition with the projected schema.
- invalid `variant_position` chunk metadata: fail with a clear `DataFusionError::Execution`.
- no `region_index`: fallback pruning should not scan all candidate rows in `scan`; exact filtering runs per partition.
- concurrent unrelated zarrs scans in the same process: must not inherit VCF Zarr scan-specific codec settings.

## Alternatives Considered

### Internal parallel reads inside one partition

Rejected for the first implementation. It would hide concurrency from DataFusion, diverge from the plain VCF provider's `target_partitions` model, and be harder to validate through execution plans.

### Process-global zarrs concurrency configuration

Rejected. It would be easy to set `zarrs::config::global_config_mut().set_codec_concurrent_target(...)`, but that is process-wide state and would leak this scan's concurrency policy into unrelated zarrs work. Per-operation `CodecOptions` keeps concurrency local to the DataFusion scan.

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
- fallback pruning without `region_index` returns equivalent rows and does not require a full planning-time position-array pass.
- array reads use scoped zarrs `CodecOptions` and do not mutate zarrs global config.
- documentation describes the zarrs thread-count limitation without promising exact thread counts.

Verification before implementation completion:

- `cargo test -p datafusion-bio-format-vcf vcf_zarr`
- `cargo fmt --all -- --check`
- `openspec validate add-vcf-zarr-parallel-partitions --strict`
