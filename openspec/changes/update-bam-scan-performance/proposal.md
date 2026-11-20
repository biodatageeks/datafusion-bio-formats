## Why

Indexed BAM scans stop scaling well before the machine runs out of cores, and
every `SELECT *` pays for a slow per-base decode. Measured on an M3 Max
(12 performance + 4 efficiency cores) with a 2.1 GB exome BAM of 19.3M reads:

- 16 workers are only 6.2x faster than one (10.4 s to 1.68 s for
  `SELECT chrom, start`), where the FASTQ BGZF scan reaches about 13x on the
  same machine. The 16 partitions hold between 538k and 3.04M records and the
  scan takes as long as the largest one (1.77 s against a 0.79 s mean).
- Decoding `sequence` and `quality_scores` costs 16-23% of single-core
  `SELECT *` CPU, because each base goes through a per-base iterator and a
  `char` push into a `String`.

## What Changes

- Split a reference sequence across partitions by the compressed bytes the
  index reports for each 16 kb window, instead of treating every non-empty
  window as equal work. BAM supplies these weights from its BAI or CSI index.
- Keep the current splitting behaviour for formats that do not supply weights
  (CRAM, VCF, BCF, GFF, GTF, pairs).
- Decode packed BAM bases with a 256-entry lookup table and encode quality
  scores with a slice loop, then append each value to Arrow in one step.
- Add `window_bytes` to `RegionSizeEstimate` (a public struct in
  `bio-format-core`). **BREAKING** at source level for external code that
  builds this struct with a literal; all workspace crates are updated here.

Ships as two pull requests: partition balance first, decoding second.

## Impact

- Affected specs: `indexed-partition-balance` (new),
  `alignment-field-decoding` (new)
- Affected code:
  - `datafusion/bio-format-core/src/partition_balancer.rs`
  - `datafusion/bio-format-core/src/alignment_utils.rs`
  - `datafusion/bio-format-bam/src/storage.rs` (index estimates)
  - `datafusion/bio-format-bam/src/physical_exec.rs` (four decode sites)
  - struct-literal updates in the CRAM, VCF, GFF, GTF and pairs crates
- Query results are unchanged. No new dependencies, no `unsafe`, no change to
  the one-thread-per-partition contract.
