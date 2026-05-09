## Context

The VCF Zarr provider builds a projected logical schema, prunes rows from genomic predicates, and then creates a `VcfZarrExec`. The exec currently advertises one physical partition and reads the whole selected row set in one `execute()` call.

The plain VCF provider already uses DataFusion session `target_partitions` to drive parallel indexed scans. VCF Zarr should use the same control surface, with Zarr variant chunks as the natural partitioning unit.

## Goals

- Expose VCF Zarr scans as multiple DataFusion physical partitions when useful.
- Bound effective partition count by `SessionConfig::target_partitions()`.
- Preserve Zarr variant chunk boundaries.
- Preserve current logical schema, projection pruning, predicate pruning, sample selection, and read options.
- Keep the change local to `datafusion-bio-format-vcf`.

## Non-Goals

- No polars-bio API changes.
- No object-storage VCF Zarr support.
- No intra-chunk splitting.
- No guaranteed global output order for parallel scans.

## Decisions

- Partition after row pruning. This keeps full scans, region-index scans, and position-array pruned scans on the same execution path.
- Use the first dimension chunk shape from `variant_position` as the variant chunk size.
- Assign each selected variant chunk to at most one execution partition.
- Group selected chunks when there are more selected chunks than target partitions.
- Return one empty partition for empty selections rather than advertising zero partitions.
- Document that parallel output order is not guaranteed.

## Alternatives Considered

### Internal parallel reads within one partition

Rejected because DataFusion would still see one physical partition and could not schedule scan work like it does for indexed VCF reads.

### Exact row-balanced partitioning

Rejected because it can split inside Zarr chunks and cause repeated chunk decompression or inefficient I/O. Chunk alignment is more important than filling every requested partition.

## Risks / Trade-offs

- Stores with fewer selected chunks than `target_partitions` will use fewer partitions. This is intentional and should be documented.
- Parallel execution may emit rows in a different global order. Tests and documentation must not imply stable row order.
- Sparse selected rows inside the same chunk can make partition sizes uneven. This avoids intra-chunk splitting and keeps I/O chunk-aware.

## Validation

- Unit-test partition planning for chunk preservation, capping, grouping, sparse selections, and empty selections.
- Integration-test equivalent results for `target_partitions=1` and `target_partitions>1`.
- Inspect plan metadata or downcast the exec to confirm partition count changes.
- Run `cargo test -p datafusion-bio-format-vcf vcf_zarr`.
- Run `openspec validate add-vcf-zarr-parallel-partitions --strict`.
