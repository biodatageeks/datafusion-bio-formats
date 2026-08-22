## Context

The BBI providers already enumerate genomic regions and use the file's embedded
chromosome and cir-tree indexes. Their execution plans nevertheless report one
partition and ignore the partition passed to `execute`. Whole-genome BigWig
files are commonly dominated by one or two chromosomes, so assigning whole
chromosomes by length leaves large stragglers.

BigTools can traverse the primary cir-tree without reading or decompressing
payload blocks. Its block coordinates and encoded on-disk sizes provide a cheap,
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

The provider reads the primary cir-tree leaf layout once and stores normalized,
chromosome-indexed work units. This adds bounded index I/O to construction but
no payload reads or decompression, and it avoids reparsing the layout for every
scan. If a valid file exceeds BigTools' safety limits for complete traversal,
provider construction succeeds and scanning falls back to one source partition.

### Balance using encoded block work

A BBI-specific linear partitioner groups blocks by start coordinate and places
cuts using cumulative encoded byte weights. It never cuts below an observed
block boundary, so the useful source-partition count may be lower than the
requested target when a file contains fewer independently readable blocks.

### Separate query windows from row ownership

Adjacent independent interval queries can both return a boundary-overlapping
record. Each shard therefore carries an inclusive ownership start and exclusive
ownership end for original record starts.

BigBed returns original record coordinates. BigWig uses BigTools' bounded
unclipped interval API. Both therefore query only their shard coordinate window;
ownership filters remove overlap returned at a non-first shard's lower edge.
This preserves original coordinates, prevents duplicates, and avoids building a
suffix block list for every BigWig shard.

### Avoid fan-out for narrow lookups

A scan selecting one explicit index region remains one partition. Residual
coordinate-column predicates that do not produce an index region retain
whole-file partitioning. Whole-file scans may split even when the file contains
only one chromosome; multi-region scans may also split a dominant chromosome.

### Keep decoded batches bounded

Decoded projections retain the existing 8,192-row batch bound. Empty projections
carry only a logical row count and use a larger bound to reduce scheduler
overhead without allocating value buffers.

## Risks / Trade-offs

- Boundary ownership mistakes could duplicate, omit, or clip rows. Tests compare
  complete sorted content and row counts across partition counts and filtered
  scans.
- Independent shards can read a payload block on both sides of a boundary.
  Plan diagnostics retain this conservative duplication in per-partition byte
  estimates.
- Provider construction performs extra cir-tree index I/O. The layout is cached
  on the provider, the benchmark includes construction in end-to-end timing,
  and limit exhaustion falls back to the pre-existing serial scan path.
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
