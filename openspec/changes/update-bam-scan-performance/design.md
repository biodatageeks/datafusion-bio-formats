## Context

The goal is faster BAM scans. The work started as a question about SIMD and
was narrowed by measurement: profiling showed that for BAM the two largest
avoidable costs are uneven partitions and a slow sequence/quality decode.
Explicit SIMD is deliberately not part of this change (see Decisions).

### Evidence

Machine: Apple M3 Max, 12 performance + 4 efficiency cores, macOS, arm64.
File: `NA12878.proper.wes.md.chr1.bam`, 2.1 GB, 19.3M reads, BAI index.
Binary: `benchmark_read` example, release build, median of 3 runs, no profiler
attached. Worker count set with `TOKIO_WORKER_THREADS`; the plan always has 16
partitions.

| Workers | `SELECT chrom, start` | `SELECT *` |
|---|---|---|
| 1 | 10.4 s | 16.7 s |
| 2 | 5.35 s | 8.84 s |
| 4 | 3.15 s | 4.94 s |
| 8 | 1.89 s | 3.20 s |
| 16 | 1.68 s | 2.81 s |

Per-partition work at 16 workers, `SELECT chrom, start`, one run:

| | Records | Elapsed |
|---|---|---|
| Smallest partition | 538,336 | 0.39 s |
| Mean | 1,206,401 | 0.79 s |
| Largest partition | 3,041,123 | 1.77 s |

The run took 1.78 s, the time of the largest partition.

Single-worker CPU profile (samply), `SELECT *`:

| Cost | Share |
|---|---|
| Inflate, decode tables, CRC (libdeflate) | 40% |
| `noodles_bam::record::sequence::Iter::next` | 9.5% |
| `String: Extend<char>` | 6.7% |
| Scan closure (includes the quality loop) | 7.0% |
| `memmove` | 6.6% |

Sequence and quality decoding is therefore 16% at minimum and up to 23% if
the whole closure share is counted.

Two things were tested and ruled out:

- Buffering the file under the BGZF reader. In one binary, 16 workers took
  1.66-1.70 s unbuffered and 1.62-1.67 s through a `BufReader`.
- `read()` contention. An earlier profile suggested it, but that profile was
  taken under samply, which slows multi-threaded runs (3.05 s under the
  profiler against 1.68 s without). Timing must never be taken under a
  profiler.

### How partitions are chosen today

`estimate_sizes_from_binning_index` (`bio-format-bam/src/storage.rs`) reports,
per reference sequence, the compressed byte range of all its chunks and the
start positions of its non-empty leaf bins (16 kb windows for BAI).
`balance_partitions` (`bio-format-core/src/partition_balancer.rs`) walks the
references with a byte budget per partition. When a reference exceeds the
budget it is cut "at the k-th non-empty bin", so every non-empty window counts
as equal work. Exome coverage per window varies by orders of magnitude, which
produces the spread above.

Each partition then runs one index query per assigned region and keeps a
record only when its alignment start lies inside the region bounds, so cut
positions can move freely without duplicating or losing records.

## Goals / Non-Goals

- Goals:
  - Partitions of an indexed BAM scan carry close to equal compressed bytes.
  - `sequence` and `quality_scores` are produced without per-base iterator
    and `char` overhead.
  - Query results are identical to today's for every `target_partitions`.
- Non-Goals:
  - Explicit SIMD (`std::arch`) kernels.
  - Changing how CRAM, VCF, BCF, GFF, GTF or pairs split regions. They share
    the balancer and keep today's behaviour.
  - Applying the new decode helpers to the CRAM crate. The helpers are placed
    in `bio-format-core` so that is a small follow-up.
  - Splitting inside a single 16 kb window. A window that alone exceeds a
    partition's share stays in one partition.
  - Replacing region queries with file-offset-range reads for unfiltered
    scans.
  - Read-name handling (`read_name.to_string()` per record, about 2%), CIGAR
    formatting, and the inflate library.
  - FASTQ and VCF scan work, which are separate changes.
- Anti-goals (the change has failed if any of these happen, even if it is
  faster):
  - Any row differs, or the set of rows differs, for any partition count.
  - `unsafe` or per-architecture code enters the reader crates. They contain
    none today.
  - A single-partition scan, or a minimal-projection scan, gets slower.
  - The split is tuned to the one benchmark file rather than to the index.

## Constraints

- The one-OS-thread-per-partition contract (issue #212) is unchanged: decode
  stays inline on the consuming worker.
- No changes to the noodles fork; the pinned revision stays.
- No new dependencies. Rust 1.94, DataFusion 55.2.
- Planning cost stays small: index data is already read during `scan()`; the
  extra work is one pass over each reference's windows.

## Approaches

### Part 1: partition balance

1. **Byte-weighted static split (chosen).** Read the position-to-file-offset
   mapping from the index, turn it into bytes per 16 kb window, and cut where
   cumulative bytes cross each partition's share. The plan stays static and
   deterministic, partition count is unchanged, and the change is confined to
   the estimate and the split rule.
2. **Many small regions pulled from a shared queue at run time.** Tolerates
   any estimate error, including decode-cost differences. Rejected for now:
   partition contents would vary between runs and partition streams would
   share state, which is a change of execution model for a problem the index
   can already answer.
3. **File-offset-range partitions for unfiltered scans.** Each partition seeks
   to a record-start offset taken from the index and reads sequentially to the
   next partition's offset. This gives the same balance and removes boundary
   re-reads, but it is a second execution path that only applies when no
   genomic filter is present. Left as a possible later step.

The user chose approach 1.

### Part 2: sequence and quality decoding

1. **Lookup table and slice loops in safe Rust (chosen).** One table lookup
   yields two bases per packed byte; quality is a `+33` loop over a slice,
   which the compiler vectorizes. Estimated to recover 11-17% of single-core
   `SELECT *`.
2. **Explicit NEON/SSSE3 kernels.** Estimated to add a further 4-5%. Rejected
   for now: it needs `unsafe`, runtime dispatch on x86 and a scalar fallback.
3. **A bulk decode API in the noodles fork.** Rejected: it adds fork
   maintenance for something this repository can do with the public
   `Sequence::as_bytes()` and `QualityScores::as_bytes()` accessors.

The user chose approach 1, with approach 2 to be reconsidered only if
decoding still shows in the profile afterwards.

## Decisions

### Part 1

**Weights come from the index's own offset lookup.**
`noodles_csi::binning_index::index::ReferenceSequence::min_offset(min_shift,
depth, start)` returns the smallest virtual offset of records overlapping the
window that contains `start`. It works for both BAI (linear index) and CSI
(per-bin offsets), so one generic implementation covers both.

For each reference sequence, sample `min_offset(..).compressed()` at the start
of every leaf window, from window 0 up to the contig length when the header
gives one, otherwise up to the last non-empty leaf bin. Turn the samples into
a non-decreasing curve:

- start the curve at the reference's smallest chunk start;
- a sample that is zero or lower than the running maximum inherits the running
  maximum (BAI writers may leave empty windows unset).

The weight of a window is the next window's curve value minus its own; the
last window's weight is the reference's largest chunk end minus its curve
value. Keep only windows with a non-zero weight. The weights of a reference
then sum to its existing `estimated_bytes`.

**The estimate carries the weights.** `RegionSizeEstimate` gains
`window_bytes: Vec<(u64, u64)>`: 1-based window start and compressed bytes,
sorted by position, empty when unknown. `nonempty_bin_positions` and
`leaf_bin_span` stay as they are. This is the builder's default shape; an
equivalent layout (for example two parallel vectors) is acceptable.

Adding a public field is a source-level break for external code that builds
the struct with a literal. That is accepted (my call): the struct is a
planning detail, and all seven constructing crates live in this workspace.

**Split rule.** In the split branch of `balance_partitions`, when the region
has at least two weighted windows inside `[pos, eff_end]`:

- `W` = sum of weights of windows in range;
- `target = W * budget / remaining` (128-bit arithmetic, as today);
- choose the first window at which the cumulative weight reaches `target`,
  clamped so that at least one weighted window remains on each side;
- `sub_end = min(window_start + leaf_bin_span - 1, eff_end - 1)`.

With fewer than two weighted windows in range, fall through to the existing
logic unchanged (k-th non-empty bin, then base-pair proportional). Everything
else in the balancer is untouched: budgets, zero-estimate references, the
open-ended last sub-region, unmapped tails and the partition-count cap.

**Other formats are not rewired.** CRAM, VCF, BCF, GFF, GTF and pairs set
`window_bytes: Vec::new()` and behave exactly as before. Moving them to
weights is a follow-up once BAM results are in.

**Known limit.** Compressed offsets resolve to BGZF blocks, and a single
window cannot be split. Data concentrated in one window (amplicon panels, a
mitochondrial pile-up) still produces one heavy partition.

### Part 2

**Helpers live in `bio-format-core::alignment_utils`,** next to
`CoreBatchBuilders`, so the BAM crate uses them now and CRAM can later.

- Packed bases: write exactly `base_count` ASCII bytes into a reusable
  `Vec<u8>` from a 256-entry `[[u8; 2]; 256]` table built from
  `=ACMGRSVTWYHKDBN`. An odd `base_count` takes only the high nibble of the
  last byte.
- Quality: if every score is at most 93, write `score + 33` into a reusable
  `Vec<u8>` with a plain slice loop. Otherwise the caller uses the existing
  per-`char` expression unchanged.
- Sequences that are already ASCII letters (the SAM `RecordBuf` path): if the
  bytes are ASCII, append them directly; otherwise use the existing
  expression.

**Strings are built with `std::str::from_utf8`,** not `from_utf8_unchecked`.
The check costs a few percent of the gain and keeps the crates free of
`unsafe` (my call).

**Output is byte-identical to today's** for every input:

- empty sequence gives an empty string;
- missing quality (all `0xFF`, which noodles returns as an empty slice) gives
  an empty string;
- scores above 93 keep whatever today's expression produces, because that
  path still runs it.

**Call sites** are the four in `bio-format-bam/src/physical_exec.rs`: the
remote async macro, the local synchronous reader, the SAM `RecordBuf` macro
and the indexed reader. BAM records expose packed bases through
`record.sequence().as_bytes()` with `record.sequence().len()` as the base
count, and raw scores through `record.quality_scores().as_bytes()`.

**Explicit SIMD is deferred with a stated trigger:** reconsider it only if a
single-worker profile taken after this change still attributes 5% or more of
`SELECT *` to sequence and quality decoding.

## Verification

Timing protocol: release build, no profiler, median of 3 runs, the benchmark
file above, worker count via `TOKIO_WORKER_THREADS`. Report before and after
from the same session.

Targets (my call; they are targets, not gates):

| Measure | Today | Target |
|---|---|---|
| Largest partition, records relative to mean (16 partitions) | 2.5x | at most 1.5x |
| `SELECT chrom, start`, 16 workers | 1.68 s | at most 1.2 s |
| `SELECT *`, 1 worker | 16.7 s | at most 15.4 s (8% less) |
| `SELECT chrom, start`, 1 worker | 10.4 s | no slower |

If a target is missed, report the per-partition breakdown or the profile
rather than tuning further against this file.

Correctness:

- Balancer unit tests with synthetic estimates: skewed weights give partition
  byte shares within one window of the budget; empty weights reproduce
  today's assignments exactly; one weighted window falls back; weights across
  several references; an explicit region sub-range.
- BAM integration test on a fixture with uneven coverage spanning many leaf
  windows: identical rows (compared as a sorted set) at `target_partitions`
  1, 2, 8 and 16, and per-partition row counts within the stated bound.
  Covers BAI and CSI.
- Decode unit tests comparing the helpers with today's expressions over all
  256 packed byte values, lengths 0 to 65, and scores 0 to 255.
- Existing BAM, SAM and CRAM tests pass unchanged.

## Left to the builder

- Names and exact signatures of the new field and helpers.
- Whether the weighted split is a separate function or inline in the loop.
- How the uneven-coverage fixture is produced (generated in the test with
  noodles, or a small committed file plus index).
- Whether per-partition work is exposed through the existing `debug!` line,
  plan metrics, or only measured in tests by executing partitions one by one.
- Whether `benchmark_read` gains command-line arguments for the file path.

## Risks / Trade-offs

- A BAI without a usable linear index, or non-monotonic offsets, gives few
  weighted windows. Mitigation: the running-maximum rule and the fallback to
  today's logic.
- CSI indexes give coarser offsets than BAI. Mitigation: covered by a CSI
  test; balance may be weaker but never worse than today's fallback.
- Whole-genome data is more uniform than exome data, so the gain there is
  expected to be smaller. Not measured yet.
- All numbers are from macOS on arm64. Linux x86-64 is not measured.

## Migration Plan

Two pull requests, each releasable alone: partition balance, then decoding.
Rolling back either is a plain revert; no data or schema changes are involved.

## Open Questions

- None blocking.
