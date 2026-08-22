## Context

The BBI providers already enumerate genomic regions and use the file's embedded
chromosome and cir-tree indexes. Their execution plans nevertheless report one
partition and ignore the partition passed to `execute`. Whole-genome BigWig
files are commonly dominated by one or two chromosomes, so assigning whole
chromosomes by length leaves large stragglers.

BigTools can traverse the primary cir-tree without reading or decompressing
payload blocks. Its block coordinates and compressed sizes provide a cheap,
format-native estimate of actual scan work.

## Goals / Non-Goals

- Goals:
  - expose up to the requested number of source partitions for whole-file BBI
    scans;
  - balance encoded on-disk work, including files with one dominant chromosome;
  - preserve identical rows and coordinates at every partition count;
  - retain streaming, bounded-memory execution and projection pushdown.
- Non-Goals:
  - guarantee output ordering across partitions;
  - parallelize a deliberately narrow single-region lookup;
  - optimize the downstream DataFusion-to-Polars Python bridge.

## Decisions

### Read block layout during provider construction

The provider reads the primary cir-tree leaf layout once and stores normalized
chromosome-local work units. This adds bounded index I/O to construction but no
payload reads or decompression, and it avoids reparsing the layout for every
scan.

### Balance using compressed block work

The existing core partition balancer receives each selected region's compressed
byte total plus observed block positions. It may split a dominant region at
block-informed coordinate boundaries, then assigns the resulting regions to the
requested partitions.

### Separate query windows from row ownership

Adjacent independent interval queries can both return a boundary-overlapping
record. Each shard therefore carries an inclusive ownership start and exclusive
ownership end for original record starts.

BigBed returns original record coordinates, so shards query their coordinate
window directly. BigWig clips intervals to query boundaries, so non-first
shards query one base earlier and retain the original upper query bound while
ownership filters remove overlap. This preserves original coordinates and
prevents duplicates.

### Avoid fan-out for narrow lookups

A scan selecting one explicit genomic region remains one partition. Whole-file
scans may split even when the file contains only one chromosome; multi-region
scans may also split a dominant chromosome.

### Keep decoded batches bounded

Decoded projections retain the existing 8,192-row batch bound. Empty projections
carry only a logical row count and use a larger bound to reduce scheduler
overhead without allocating value buffers.

## Risks / Trade-offs

- Boundary ownership mistakes could duplicate, omit, or clip rows. Tests compare
  complete sorted content and row counts across partition counts and filtered
  scans.
- Independent shards can read a compressed block on both sides of a boundary.
  Plan diagnostics retain this conservative duplication in per-partition byte
  estimates.
- Provider construction performs extra cir-tree index I/O. The layout is cached
  on the provider, and the benchmark records end-to-end construction separately
  from source execution.
- Downstream consumers may still scale below the provider because Python batch
  conversion, aggregation, or full DataFrame materialization is outside this
  change.

## Migration Plan

1. Merge and release the BigTools block-layout API.
2. Replace the temporary git revision with the released crate version.
3. Merge the provider change and release datafusion-bio-formats.
4. Bump polars-bio and update its BBI parallel-scan capability documentation.

## Open Questions

- Whether decoded BBI batch size should become adaptive belongs to a separate
  downstream/materialization optimization.
