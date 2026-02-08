# Index-Based Predicate Pushdown & Partition Balancing

This document describes how indexed genomic file formats (BAM, CRAM, VCF, GFF) use
index metadata to partition reads across DataFusion's parallel execution engine.

## High-Level Pipeline

All four indexed formats follow the same three-stage pipeline inside their
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
                          |    from file header         |
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

### Format-Specific Differences

| Property | BAM (.bai) | CRAM (.crai) | VCF (.tbi) | GFF (.tbi) |
|---|---|---|---|---|
| Contig lengths | From header | From header | From header | Empty `&[]` |
| Unmapped reads | Explicit tail seek | Implicit (container-level) | N/A | N/A |
| Leaf-bin positions | BAI bins | N/A | TBI bins | TBI bins |
| Sub-splitting | Yes | Yes | Yes | No (no lengths) |

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
TableProvider::scan(filters)
|
|-- extract_genomic_regions(filters)           # shared (bio-format-core)
|   |-- collect_genomic_constraints()          #   parse chrom/start/end
|   +-- returns GenomicFilterAnalysis          #   regions + residual filters
|
|-- if no regions:
|   +-- build_full_scan_regions(contigs)       # shared (bio-format-core)
|
|-- estimate_sizes_from_*()                    # per-format (storage.rs)
|   |-- read index file (BAI/CRAI/TBI)
|   |-- compute compressed byte ranges
|   +-- extract leaf-bin occupancy
|
|-- balance_partitions(estimates, target)       # shared (bio-format-core)
|   |-- compute per-partition byte budget
|   |-- linear scan: assign/split regions
|   |-- distribute zero-estimate regions evenly
|   +-- emit unmapped tails (BAM only)
|
+-- return ExecutionPlan with N partitions
```

## Source Locations

| Component | File |
|---|---|
| Filter extraction | `datafusion/bio-format-core/src/genomic_filter.rs` |
| Partition balancer | `datafusion/bio-format-core/src/partition_balancer.rs` |
| BAM size estimator | `datafusion/bio-format-bam/src/storage.rs` |
| CRAM size estimator | `datafusion/bio-format-cram/src/storage.rs` |
| VCF size estimator | `datafusion/bio-format-vcf/src/storage.rs` |
| GFF size estimator | `datafusion/bio-format-gff/src/storage.rs` |
| BAM scan entry | `datafusion/bio-format-bam/src/table_provider.rs` |
| CRAM scan entry | `datafusion/bio-format-cram/src/table_provider.rs` |
| VCF scan entry | `datafusion/bio-format-vcf/src/table_provider.rs` |
| GFF scan entry | `datafusion/bio-format-gff/src/table_provider.rs` |
