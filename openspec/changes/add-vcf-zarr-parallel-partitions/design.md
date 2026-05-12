## Context

The VCF Zarr provider builds a projected logical schema, prunes rows from genomic predicates, and then creates a `VcfZarrExec`. The exec currently advertises one physical partition and reads the whole selected row set in one `execute()` call.

The plain VCF provider already uses DataFusion session `target_partitions` to drive parallel indexed scans. VCF Zarr should use the same control surface, with Zarr variant chunks as the natural partitioning unit.

## Goals

- Expose VCF Zarr scans as multiple DataFusion physical partitions when useful.
- Bound effective partition count by `SessionConfig::target_partitions()`.
- Preserve Zarr variant chunk boundaries.
- Avoid planning-time full scans of fallback position arrays before partitions exist.
- Avoid nested zarrs parallelism inside each DataFusion partition by passing scoped single-concurrency zarrs codec options to array reads.
- Preserve current logical schema, projection pruning, predicate pruning, sample selection, and read options.
- Keep the change local to `datafusion-bio-format-vcf`.

## Non-Goals

- No polars-bio API changes.
- No object-storage VCF Zarr support.
- No intra-chunk splitting.
- No guaranteed global output order for parallel scans.
- No public API for configuring zarrs inner codec concurrency.
- No exact process-wide thread-count guarantee.
- No handoff of unused DataFusion target partition capacity to zarrs.

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
- Use `CodecOptions::default().with_concurrent_target(1).with_chunk_concurrent_minimum(1)` for partition reads so zarrs is not asked to add chunk or codec parallelism inside each DataFusion stream.
- Prefer serial one-chunk or one-chunk-subset zarrs calls inside each partition instead of multi-chunk zarrs array reads, so zarrs does not route partition-local chunk iteration through its internal parallel read path.
- Split each physical partition's final exact row selection into output batches capped by `TaskContext::session_config().batch_size()`.
- Run partition pruning and output-batch reads through `tokio::task::spawn_blocking` so blocking zarrs filesystem I/O does not occupy Tokio worker threads while the stream is polled.
- Keep one local `Arc<FilesystemStore>` in `VcfZarrMetadata` and clone that handle for array opens, instead of constructing a new filesystem store for every array metadata read.
- Group contiguous selected FORMAT sample indexes into wider zarrs subset reads, preserving requested output order while reducing per-sample read calls for common contiguous selections.
- Do not increase zarrs concurrency when the effective partition count is lower than `target_partitions`; unused target capacity remains unused rather than creating empty DataFusion streams or inner zarrs parallelism.
- Document that parallel output order is not guaranteed.
- Document that the single-concurrency zarrs options are the intended no-extra-zarrs-parallelism setting, but same-OS-thread execution cannot be guaranteed beyond what zarrs/rayon and individual codecs provide.

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
- Disabling zarrs inner chunk/codec parallelism may underuse CPU when a scan selects fewer chunks than `target_partitions`. This is the intended trade-off because DataFusion streams are the only concurrency layer for this change.
- zarrs/rayon or individual codecs may still control which OS thread executes internal work. The implementation should avoid zarrs multi-chunk parallel read paths and use single-concurrency options, but it must not promise strict thread affinity unless zarrs exposes such a guarantee.

## Validation

- Unit-test partition planning for chunk preservation, capping, grouping, sparse selections, and empty selections.
- Integration-test equivalent results for `target_partitions=1` and `target_partitions>1`.
- Integration-test fallback pruning without `region_index` to confirm equivalent results when exact pruning runs partition-locally.
- Inspect plan metadata or downcast the exec to confirm partition count changes.
- Unit-test or inspect the array-read path to confirm partition reads use scoped single-concurrency `CodecOptions`, avoid zarrs multi-chunk read paths where practical, and do not mutate the zarrs global config.
- Unit-test row-selection batch splitting and contiguous sample-span grouping.
- Integration-test that VCF Zarr output batches honor the DataFusion session batch size.
- Run `cargo test -p datafusion-bio-format-vcf vcf_zarr`.
- Run `openspec validate add-vcf-zarr-parallel-partitions --strict`.
