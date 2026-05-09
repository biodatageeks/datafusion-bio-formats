## Context

The VCF Zarr provider builds a projected logical schema, prunes rows from genomic predicates, and then creates a `VcfZarrExec`. The exec currently advertises one physical partition and reads the whole selected row set in one `execute()` call.

The plain VCF provider already uses DataFusion session `target_partitions` to drive parallel indexed scans. VCF Zarr should use the same control surface, with Zarr variant chunks as the natural partitioning unit.

## Goals

- Expose VCF Zarr scans as multiple DataFusion physical partitions when useful.
- Bound effective partition count by `SessionConfig::target_partitions()`.
- Preserve Zarr variant chunk boundaries.
- Avoid planning-time full scans of fallback position arrays before partitions exist.
- Avoid nested all-core zarrs reads inside each DataFusion partition by passing scoped zarrs codec options to array reads.
- Preserve current logical schema, projection pruning, predicate pruning, sample selection, and read options.
- Keep the change local to `datafusion-bio-format-vcf`.

## Non-Goals

- No polars-bio API changes.
- No object-storage VCF Zarr support.
- No intra-chunk splitting.
- No guaranteed global output order for parallel scans.
- No public API for configuring zarrs inner codec concurrency.
- No exact process-wide thread-count guarantee.

## Decisions

- Partition after projection planning and planning-time candidate selection.
- Keep `region_index` candidate pruning in `scan`, because it reads compact chunk metadata.
- Do not run a full exact fallback position-array pruning pass in `scan`; when `region_index` is absent, partition chunk candidates first and apply exact position filtering inside `execute(partition)`.
- Use the first dimension chunk shape from `variant_position` as the variant chunk size.
- Assign each selected variant chunk to at most one execution partition.
- Group selected chunks when there are more selected chunks than target partitions.
- Return one empty partition for empty selections rather than advertising zero partitions.
- Pass per-operation zarrs `CodecOptions` into all partition array reads.
- Do not call `zarrs::config::global_config_mut()` or otherwise mutate process-global zarrs configuration.
- For parallel scans, reduce zarrs inner codec concurrency inside each DataFusion partition, initially with `CodecOptions::default().with_concurrent_target(1)`.
- Keep existing default zarrs behavior for single-effective-partition scans unless implementation testing shows that the same scoped target should be applied uniformly.
- Document that parallel output order is not guaranteed.
- Document that the zarrs concurrent target is a per-operation target/limit; it helps avoid nested oversubscription but does not guarantee an exact OS thread count.

## Alternatives Considered

### Internal parallel reads within one partition

Rejected because DataFusion would still see one physical partition and could not schedule scan work like it does for indexed VCF reads.

### Process-global zarrs concurrency configuration

Rejected because `datafusion-bio-formats` is a library and global zarrs settings would affect unrelated arrays, scans, or concurrent queries in the same process.

### Exact row-balanced partitioning

Rejected because it can split inside Zarr chunks and cause repeated chunk decompression or inefficient I/O. Chunk alignment is more important than filling every requested partition.

## Risks / Trade-offs

- Stores with fewer selected chunks than `target_partitions` will use fewer partitions. This is intentional and should be documented.
- Parallel execution may emit rows in a different global order. Tests and documentation must not imply stable row order.
- Sparse selected rows inside the same chunk can make partition sizes uneven. This avoids intra-chunk splitting and keeps I/O chunk-aware.
- Fallback pruning may read lightweight arrays in each partition before heavy projected arrays. This is intentional so the expensive fallback path benefits from DataFusion partition scheduling.
- Setting zarrs inner concurrency to a low per-partition target may reduce codec parallelism within an individual partition. This is the intended trade-off when DataFusion already schedules multiple partitions concurrently.
- The configured zarrs concurrent target is not a contractual thread-count cap for the whole query. It should be documented as an oversubscription control, not as a thread guarantee.

## Validation

- Unit-test partition planning for chunk preservation, capping, grouping, sparse selections, and empty selections.
- Integration-test equivalent results for `target_partitions=1` and `target_partitions>1`.
- Integration-test fallback pruning without `region_index` to confirm equivalent results when exact pruning runs partition-locally.
- Inspect plan metadata or downcast the exec to confirm partition count changes.
- Unit-test or inspect the array-read path to confirm partition reads use scoped `CodecOptions` rather than the zarrs global config.
- Run `cargo test -p datafusion-bio-format-vcf vcf_zarr`.
- Run `openspec validate add-vcf-zarr-parallel-partitions --strict`.
