# Index-Based Predicate Pushdown & Partition Balancing

This document describes how indexed genomic file formats (BAM, CRAM, VCF, GFF) use
index metadata to partition reads across DataFusion's parallel execution engine.

## High-Level Pipeline

All four indexed formats follow the same pipeline inside their
`TableProvider::scan()` implementation:

```
                          SQL Query
                             |
                             v
               +----------------------------+
               | 1. extract_genomic_regions  |  (bio-format-core/genomic_filter.rs)
               |    Parse WHERE clause for   |
               |    chrom/start/end filters  |
               +-------------+--------------+
                             |
                  regions found?
                 /              \
               yes               no
                |                 |
                v                 v
     use extracted        +----------------------------+
       regions            | 2. build_full_scan_regions  |  (bio-format-core/genomic_filter.rs)
                          |    One region per contig    |
                          |    from file header/index   |
                          +-------------+--------------+
                                        |
                  \                    /
                   \                  /
                    v                v
              +-------------------------------+
              | 3. estimate_sizes_from_*      |  (per-format storage.rs)
              |    BAI / CRAI / TBI index     |
              |    -> Vec<RegionSizeEstimate>  |
              +---------------+---------------+
                              |
                              v
              +-------------------------------+
              | 4. balance_partitions         |  (bio-format-core/partition_balancer.rs)
              |    Linear-scan balancer       |
              |    -> Vec<PartitionAssignment> |
              +---------------+---------------+
                              |
                              v
                  N parallel ExecutionPlan
                    partitions in DataFusion
```

## Stage 0: Index & Contig Discovery

Before `scan()` is called, each `TableProvider::new()` discovers companion index
files and collects contig metadata:

```
TableProvider::new(file_path)
|
|-- discover_*_index(file_path)         # bio-format-core/index_utils.rs
|   |-- try {path}.bai / .csi / .crai / .tbi
|   +-- return (index_path, IndexFormat)
|
|-- Read contig names & lengths
|   |-- BAM/CRAM: from file header (reference_sequences)
|   |-- VCF: from ##contig header lines
|   |   +-- fallback: read names from TBI header (*)
|   +-- GFF: from TBI/CSI index header
|
+-- Store: index_path, contig_names, contig_lengths
```

### VCF TBI Contig Name Fallback

Some VCF files (e.g., Ensembl releases) lack `##contig` header lines entirely.
Without contig names, the indexed partitioning path is skipped and all reads go
through a single partition.

When the VCF header yields zero contig names but a TBI index exists,
`VcfTableProvider::new()` reads contig names from the TBI header:

```
VCF header has ##contig lines?
           /              \
         yes                no
          |                  |
     use header          TBI index exists?
     names + lengths    /              \
                      yes                no
                       |                  |
                  read TBI header     no indexed scan
                  extract names       (sequential only)
                  lengths = [0, ...]
```

This is implemented via `noodles_csi::binning_index::BinningIndex::header()`
which provides the reference sequence names embedded in the TBI file.

## Stage 1: Filter Extraction

`extract_genomic_regions()` parses DataFusion `Expr` filters and recognizes:

```
WHERE chrom = 'chr1'                              -> 1 region (chr1, full)
WHERE chrom = 'chr1' AND start >= 1000            -> 1 region (chr1:1000-..)
WHERE chrom IN ('chr1','chr2') AND end <= 5000    -> 2 regions (chr1:..5000, chr2:..5000)
```

Filters that don't involve `chrom`/`start`/`end` are passed through as residual
filters for DataFusion to evaluate after reading.

```
+-----------+     +---------------------+     +------------------+
| SQL WHERE | --> | Genomic constraints  | --> | GenomicRegion[]  |
| clauses   |     | chrom, start, end   |     | + residual Exprs |
+-----------+     +---------------------+     +------------------+
```

If no genomic filters are found, the full-scan fallback creates one region per
contig from the file header (BAM reference sequences, VCF contigs, etc.).

### Coordinate System Conversion

Genomic positions in filters are interpreted based on the table's coordinate system
setting (`coordinate_system_zero_based`). Internally, all regions use **1-based
closed** coordinates to match noodles/htslib conventions.

```
User query (0-based):  WHERE start >= 999 AND end < 10008
                              |                    |
                        val=999, +1 -> 1000   val=10008, saturating_sub(1) -> 10007
                              |                    |
Internal region:       GenomicRegion { start: Some(1000), end: Some(10007) }
```

Conversion rules per column and operator:

| Column | Operator | 0-based mode | 1-based mode |
|--------|----------|--------------|--------------|
| `start` | `=`     | val + 1      | val          |
| `start` | `>`     | val + 2      | val + 1      |
| `start` | `>=`    | val + 1      | val          |
| `end`   | `=`     | val          | val          |
| `end`   | `<`     | val - 1 (saturating) | val - 1 (saturating) |
| `end`   | `<=`    | val          | val          |

The `end` column is always treated as 1-based because noodles `alignment_end()`
returns 1-based inclusive positions regardless of the user-facing coordinate system.

### Filter Pushdown Levels

Each table provider reports two levels of filter pushdown:

```
supports_filters_pushdown(filters)
    |
    +-- for each filter:
        |
        is_genomic_coordinate_filter(expr) AND index exists?
       /                                        \
     yes                                          no
      |                                            |
  Inexact (index seek)                    can_push_down_filter(expr)?
                                         /                          \
                                       yes                            no
                                        |                              |
                                   Inexact                        Unsupported
                                   (record-level)                 (DataFusion post-filters)
```

`Inexact` tells DataFusion to pass the filter to `scan()` AND verify it post-scan
(since index seeks return approximate results). All non-genomic filters matching
schema columns are evaluated at the record level during reading.

## Stage 2: Size Estimation

Each format reads its own index file to estimate compressed bytes per region:

```
+-----+     +-----+     +-----+     +-----+
| BAM |     |CRAM |     | VCF |     | GFF |
+--+--+     +--+--+     +--+--+     +--+--+
   |           |            |           |
   v           v            v           v
 .bai        .crai        .tbi        .tbi
 index       index       index       index
   |           |            |           |
   v           v            v           v
estimate_  estimate_   estimate_   estimate_
sizes_     sizes_      sizes_      sizes_
from_bai   from_crai   from_tbi   from_tbi
   |           |            |           |
   +-----+-----+-----+-----+
         |
         v
  Vec<RegionSizeEstimate>
  {
    region:          GenomicRegion,
    estimated_bytes: u64,           // compressed bytes from index
    contig_length:   Option<u64>,   // enables sub-splitting
    unmapped_count:  u64,           // BAM-only unmapped tail
    nonempty_bin_positions: Vec,    // leaf-bin occupancy for data-aware splits
    leaf_bin_span:   u64,           // bp per leaf bin (16384 for BAI/TBI)
  }
```

### TBI Contig Length Inference

For sub-splitting to work, the balancer needs `contig_length`. When file headers
don't provide contig lengths (VCF without `##contig=<...,length=N>`, GFF), the
TBI size estimator infers lengths from the index's binning structure using a
two-tier fallback:

```
contig_length resolution:

  1. Header-declared length (contig_lengths[idx] > 0)?
     |                                      |
    yes -> use it                          no
                                            |
  2. Has non-empty leaf bins (level 5, indices 4681-37448)?
     |                                      |
    yes -> max_leaf_position                no
           + TBI_LEAF_SPAN (16384)           |
                                            |
  3. Has ANY non-empty bin at any level?
     |                                      |
    yes -> compute max end position        no
           from bin hierarchy               |
           (LEVEL_OFFSETS)              contig_length = None
                                        (unsplittable)
```

**Tier 1 (leaf bins)** uses the finest granularity. Each leaf bin covers 16 KiB
(2^14 bp). The max non-empty leaf bin position + span gives a tight lower bound.

**Tier 2 (any bin level)** handles files with small coordinate ranges that don't
populate leaf bins (common for GFF). It uses the BAI binning hierarchy:

```
Level | Bin offset | Span      | Bin count
------|------------|-----------|----------
  0   |     0      | 2^29 (512M) |    1
  1   |     1      | 2^26 (64M)  |    8
  2   |     9      | 2^23 (8M)   |   64
  3   |    73      | 2^20 (1M)   |  512
  4   |   585      | 2^17 (128K) | 4096
  5   |  4681      | 2^14 (16K)  | 32768
```

For each non-empty bin, the end position is computed as
`(bin_index_in_level + 1) * span`, and the maximum across all bins is used.

### Format-Specific Differences

| Property | BAM (.bai) | CRAM (.crai) | VCF (.tbi) | GFF (.tbi) |
|---|---|---|---|---|
| Contig lengths | From header | From header | From header | N/A |
| Length inference | N/A (always in header) | N/A | TBI leaf bins | TBI any-level bins |
| Contig name source | File header | File header | Header or TBI fallback | TBI index header |
| Unmapped reads | Explicit tail seek | Implicit (container-level) | N/A | N/A |
| Leaf-bin positions | BAI bins | N/A | TBI bins | TBI bins |
| Sub-splitting | Yes | Yes | Yes | Yes (via inference) |

## Stage 3: Partition Balancing

`balance_partitions()` in `bio-format-core` distributes regions across
`target_partitions` (from DataFusion's session config). This is the shared
implementation that all four formats call identically.

### Linear-Scan Algorithm

```
Input: regions sorted by reference order, each with estimated_bytes
       target_partitions = T

budget_per_partition = total_bytes / T

    Region A (500 bytes)   Region B (1200 bytes)      Region C (300 bytes)
    |==================|   |========================|  |======|
                           ^                ^
                    budget boundary    budget boundary

    Partition 1            Partition 2            Partition 3
    [A, B:1..split1]      [B:split1+1..split2]   [B:split2+1.., C]
```

### Splitting Decision Tree

```
                    For each region:
                         |
                  estimated_bytes == 0?
                 /                    \
               yes                     no
                |                       |
                v                       v
      Assign to partition        remaining <= budget?
      with fewest regions       /                    \
      (even distribution)     yes                     no
                               |                       |
                               v                       v
                         Assign whole            has contig_length?
                         to current             /                  \
                         partition            yes                    no
                                               |                     |
                                               v                     v
                                         Split at byte-         Assign whole
                                         budget boundary        (unsplittable)
                                               |
                                        has bin positions?
                                       /                  \
                                     yes                    no
                                      |                      |
                                      v                      v
                                Data-aware split        BP-proportional
                                at k-th non-empty       split (uniform
                                leaf bin                 distribution)
```

### Zero-Estimate Region Distribution

When contigs have 0 estimated bytes (common for alt contigs, decoys, and
unplaced scaffolds in human genomes), they are distributed evenly across
existing partitions rather than piling onto the last one:

```
Before fix:                          After fix:
+----------+                         +----------+
| Part. 1  | chr1 (500 bytes)        | Part. 1  | chr1 (500 bytes)
+----------+                         |          | alt_3, alt_7
| Part. 2  | chr2 (500 bytes)        +----------+
+----------+                         | Part. 2  | chr2 (500 bytes)
| Part. 3  | chr3 (500 bytes)        |          | alt_4, alt_8
+----------+                         +----------+
| Part. 4  | chrX (500 bytes)        | Part. 3  | chr3 (500 bytes)
|          | alt_1, alt_2, alt_3,    |          | alt_5, alt_9
|          | alt_4, alt_5, alt_6,    +----------+
|          | alt_7, alt_8, alt_9,    | Part. 4  | chrX (500 bytes)
|          | alt_10, alt_11, alt_12  |          | alt_1, alt_2, alt_6
+----------+                         |          | alt_10, alt_11, alt_12
                                     +----------+
  -> tail latency from Part. 4         -> balanced region counts
```

The assignment uses `min_by_key(regions.len())` to pick the partition with
the fewest regions so far.

### Data-Aware Splitting (BAI/TBI Leaf Bins)

When an index provides non-empty leaf-bin positions, splits are placed at
actual data boundaries rather than uniform base-pair positions:

```
Chromosome with uneven data distribution:

 Data density: |####|    |    |####|####|####|    |    |    |####|
 Leaf bins:     B0   B1   B2   B3   B4   B5   B6   B7   B8   B9
                ^                        ^
                |                        |

 BP-proportional split (naive):   split at midpoint bp
                      |####|    |    |##|##|####|####|    |    |    |####|
                                     ^-- splits data region B3 in half

 Data-aware split (actual):       split at bin boundary between B4 and B5
                      |####|    |    |####|####|  |####|    |    |    |####|
                                              ^-- clean split between bins
```

### Unmapped Read Handling

Unmapped reads tied to a reference (reads with a `reference_sequence_id` but no
`alignment_start`) require different handling depending on the index format.

#### BAM: Explicit Unmapped Tail Seek

BAI uses a **binning scheme** with precise virtual file offsets. Ranged queries
return only records whose bins overlap the query interval — unmapped reads
(which have no position) fall outside all bins and are never returned. The BAI
**metadata pseudobin** stores the unmapped count and the virtual offset of the
last aligned record, enabling a direct BGZF seek past mapped data.

When the balancer splits a chromosome, it emits an `unmapped_tail` sentinel
region after the last sub-region to trigger this seek:

```
  chr1 split into 3 sub-regions + unmapped tail:

  Partition 1: [chr1:1-50000000]
  Partition 2: [chr1:50000001-100000000]
  Partition 3: [chr1:100000001-.., chr1:unmapped_tail]
                                    ^^^^^^^^^^^^^^^^
                                    Direct BGZF seek to unmapped reads
```

#### CRAM: No Special Handling Needed

CRAI operates at **container/slice granularity** — a query returns all records
in every container that overlaps the region, not individual records. Unmapped
reads tied to a reference are stored in containers alongside or immediately
after the mapped reads for that reference.

When the last sub-region has `end: None` (open-ended), noodles returns all
remaining containers for that reference, including those containing unmapped
reads. Since unmapped reads have `alignment_start = None`, they pass through the
deduplication filter (which only checks positioned records):

```
  CRAM containers for chr1:

  Container A        Container B        Container C
  [mapped records]   [mapped records]   [unmapped records]
  pos: 1..25M        pos: 25M..50M      pos: None

  Query chr1:25000001-.. (open-ended last sub-region)
        +-----------------------------------------+
        | Returns containers B + C                |
        | B: mapped reads, filtered by position   |
        | C: unmapped reads, pass through          |
        |    (alignment_start = None skips dedup)  |
        +-----------------------------------------+
```

This is why `estimate_sizes_from_crai` sets `unmapped_count: 0` — the balancer
never needs to emit an `unmapped_tail` region because CRAM's container-level
indexing naturally includes unmapped reads in open-ended queries.

```
  Index granularity comparison:

  BAI (record-level bins):
  |bin0|bin1|bin2|...|binN| unmapped |    <- unmapped outside all bins
                                              requires separate seek

  CRAI (container-level):
  |  container 0  |  container 1  |  container 2  |
  |  mapped data  |  mapped data  | unmapped data |  <- returned by
                                                       open-ended query
```

#### VCF / GFF: Not Applicable

Tabular formats (VCF, GFF) do not have a concept of unmapped reads.

## Complete Call Graph

```
TableProvider::new(file_path)
|
|-- discover_*_index(file_path)              # bio-format-core/index_utils.rs
|-- read contig names + lengths from header
|-- (VCF) fallback: read contig names from TBI if header lacks ##contig
+-- store index_path, contig_names, contig_lengths

TableProvider::scan(filters)
|
|-- extract_genomic_regions(filters, coord_system)  # shared (bio-format-core)
|   |-- collect_genomic_constraints()               #   parse chrom/start/end
|   |-- convert coordinates (0-based -> 1-based)    #   if coordinate_system_zero_based
|   +-- returns GenomicFilterAnalysis               #   regions + residual filters
|
|-- if no regions:
|   +-- build_full_scan_regions(contigs)            # shared (bio-format-core)
|
|-- estimate_sizes_from_*()                         # per-format (storage.rs)
|   |-- read index file (BAI/CRAI/TBI)
|   |-- compute compressed byte ranges
|   |-- extract leaf-bin occupancy
|   +-- infer contig_length (TBI: two-tier fallback)
|
|-- balance_partitions(estimates, target)            # shared (bio-format-core)
|   |-- compute per-partition byte budget
|   |-- linear scan: assign/split regions
|   |-- distribute zero-estimate regions evenly
|   +-- emit unmapped tails (BAM only)
|
+-- return ExecutionPlan with N partitions
```

## Diagnostic Logging

All four formats emit structured debug logs at each pipeline stage. Enable with:

```bash
RUST_LOG=debug cargo run ...
# or for a specific crate:
RUST_LOG=datafusion_bio_format_vcf=debug cargo run ...
```

### What Gets Logged

| Stage | Log message pattern | Information |
|-------|-------------------|-------------|
| Index discovery | `Discovered {format} index for {path}: {index_path}` | Which index was found |
| TBI contig fallback | `VCF header lacks ##contig lines; inferred N contigs from TBI index` | VCF-specific fallback triggered |
| Filter receipt | `{Format}TableProvider::scan - N filters received, index=bool, contig_names=[..]` | Filters passed to scan |
| Per-filter detail | `filter[i]: {expr_debug}` | Each filter expression |
| Region extraction | `{Format} scan: using N filter-derived region(s)` | Filter-to-region conversion |
| Full scan fallback | `{Format} scan: no genomic filters pushed down, using full-scan on N contig(s)` | No filters matched |
| Sequential fallback | `{Format} scan: no index regions available, falling back to sequential scan` | No index or no contigs |
| Partition info | `{Format} indexed scan: N partitions (from M regions, target T), R residual filters` | Final partition layout |
| Split point | `Splitting {chrom} at bp {pos} (budget=N, remaining=M)` | Where a contig is split |

These logs are useful for diagnosing:
- Whether filters from upstream libraries (e.g., polars_bio) are reaching DataFusion's `scan()`
- Whether indexed or sequential scan is chosen
- How regions are distributed across partitions

## Source Locations

| Component | File |
|---|---|
| Index discovery | `datafusion/bio-format-core/src/index_utils.rs` |
| Filter extraction | `datafusion/bio-format-core/src/genomic_filter.rs` |
| Record-level filters | `datafusion/bio-format-core/src/record_filter.rs` |
| Partition balancer | `datafusion/bio-format-core/src/partition_balancer.rs` |
| BAM size estimator | `datafusion/bio-format-bam/src/storage.rs` |
| CRAM size estimator | `datafusion/bio-format-cram/src/storage.rs` |
| VCF size estimator | `datafusion/bio-format-vcf/src/storage.rs` |
| GFF size estimator | `datafusion/bio-format-gff/src/storage.rs` |
| BAM scan entry | `datafusion/bio-format-bam/src/table_provider.rs` |
| CRAM scan entry | `datafusion/bio-format-cram/src/table_provider.rs` |
| VCF scan entry | `datafusion/bio-format-vcf/src/table_provider.rs` |
| GFF scan entry | `datafusion/bio-format-gff/src/table_provider.rs` |
