# PGEN Provider Audit And Performance Plan

## Scope And Evidence

This audit was performed on 2026-08-16 before production implementation work.
It covers the open genotype-provider pull-request stack, the draft PGEN
provider in PR #221, the pinned PLINK 2 PGEN specification, current PLINK 2 and
`pgenlib` behavior, and current `AI-sandbox/snputils` behavior and performance.

Reviewed baselines:

- repository `master`: `7b7b9bf`;
- genotype specification PR #216: `b12e28d`;
- genotype core/BCF rollup PR #217: `558503c`;
- draft PGEN PR #221: `d13ac8e`;
- normative PGEN specification: PLINK 2 `pgen_spec` at `9ee41ce`;
- current PLINK 2 oracle: `7b30cf1`;
- Python `pgenlib`: 0.94.1; and
- current `snputils`: `482c6d1`.

The current PLINK 2 `pgen_spec` tree is byte-for-byte unchanged from the pinned
`9ee41ce` tree. The oracle implementation and `snputils` have advanced, but the
normative byte-level contract reviewed by PR #216 has not changed.

## Pull-Request Stack

| PR | State | Role | Audit conclusion |
| --- | --- | --- | --- |
| #216 | Draft | OpenSpec contract for BCF, PLINK 1, BGEN, PGEN, and GRG | Correct place for this amendment; it lacked a measurable PGEN parity gate and explicit ploidy/effective-dosage contracts. |
| #217 | Ready | Shared genotype provider core plus merged BCF work | Current stack base. Its head moved after #221 was created. |
| #219 | Draft | PLINK 1 provider | Sibling implementation; does not block PGEN design. |
| #220 | Ready | BGEN provider | Sibling implementation; later review fixes are stacked in #229. |
| #221 | Draft | PGEN provider | Broad feature coverage and passing tests, but its base is stale and its hot path is not release-ready. |
| #222 | Draft | GRG licensing/compatibility gate | Independent of PGEN. |
| #229 | Ready | BGEN review fixes | Demonstrates that the genotype stack is still moving under #221. |

PR #221 is based on the older #217 base `db5db5f`, while #217 now points at
`558503c`. A direct comparison against the current core therefore contains
unrelated reversions. Rebase/restack is a correctness prerequisite before any
PGEN optimization is reviewed.

## PGEN Physical Contract

### Headers And Indexing

The supported modes `0x01`, `0x02`, `0x03`, `0x04`, `0x10`, `0x11`, `0x20`,
and `0x21` select fixed or variable-width header/index arrangements. Variable
records are grouped into blocks of exactly `2^16` variants. The implementation
must validate control bytes, count widths, record widths, offsets, monotonicity,
and object bounds before using file-declared values for allocation or I/O.

The index should be consumed blockwise. A vector of rich `RecordInfo` objects
for the entire dataset delays first output and multiplies memory on very large
cohorts. A compact block descriptor plus on-demand record metadata is sufficient
for range planning and streaming execution.

### Main Hardcall Representations

The low record-type bits distinguish:

- dense two-bit hardcalls (`0`);
- one-bit base values plus exceptions (`1`);
- LD delta from the most recent eligible non-LD record (`2`);
- inverse LD delta (`3`); and
- difflists with common hardcall categories 0, 2, or 3 (`4`, `6`, `7`).

Type `5` is reserved. In ordinary PGEN hardcall encoding, category 0 is
homozygous reference, 1 is heterozygous, 2 is homozygous alternate, and 3 is
missing. These codes must not be confused with PLINK 1 BED bit semantics.

An LD record depends on the most recent eligible non-LD record in the same
`2^16` block and cannot be the block's first record. A partition may need a
dependency prelude, but only its owned records are emitted. One current base
workspace is sufficient; retaining every prior base is neither required by the
specification nor scalable.

### Auxiliary Tracks

Up to ten auxiliary portions can follow the main hardcall representation,
including multiallelic hardcall patches, phase presence/information, dosage
presence/values, multiallelic dosage, phased dosage, and multiallelic phased
dosage. Some multiallelic dosage layouts remain non-finalized in the draft
specification and must be rejected distinctly rather than guessed.

Biallelic dosage integers use the PLINK 2 fixed-point scaling; for encoded
diploid dosage the integer is divided by 16384. PGEN does not encode biological
ploidy. PVAR chromosome and PSAM sex alone cannot safely imply a biological
ploidy without a genome build and PAR policy.

The public raw genotype schema uses `UInt16` allele indices, so its independent
implementation limit is 65,536 alleles per variant. Current official `pgenlib`
uses an 8-bit allele code and limits differential fixtures to 255 alleles. The
oracle's implementation limit is not a normative PGEN limit.

## Oracle Findings

### PLINK 2 And `pgenlib`

The official reader uses aligned word/vector workspaces, direct copies for
dense packed genotype vectors, reusable buffers, and alternative compact
difflist/genovec states. It retains only the LD state needed for the next
dependent records and does not create an extra reader thread pool.

`pgenlib.read_dosages()` returns effective dosage. On a fixture with hardcalls
and no stored dosage track, it returns exact 0/1/2 values and its missing-value
sentinel. This contradicts the original OpenSpec statement that `DS` is always
null when no dosage is physically stored. The amended contract therefore uses:

- `DS`: stored dosage when present, otherwise exact hardcall dosage, otherwise
  null; and
- optional `DS_STORED`: the source dosage only, null when fallback was used.

### `snputils`

Current `snputils` delegates PGEN payload decoding to `pgenlib`, so it is a
performance and behavioral oracle, not an acceptable runtime dependency or
architecture for this Apache-licensed Rust provider.

For exactly `fields=["GT"]` with no sample/variant selector, `snputils` skips
PVAR and PSAM and opens PGEN directly. Its phased fast path:

- materializes compact `int8` output shaped as variants × samples × 2;
- reads a whole temporary `int32` result only when it stays under 64 MiB;
- otherwise uses chunks capped at about 16 MiB and 512 variants;
- uses contiguous range calls for contiguous selections and list calls for
  sparse selections; and
- copies each `int32` chunk into the final `int8` array.

Current `snputils` also has explicit chromosome-ploidy handling. The parity
benchmark must use `chromosome_ploidy="autosomal"` because the initial Rust
contract exposes PGEN's encoded two-allele representation and deliberately
does not guess biological ploidy.

## Draft PR #221 Correctness Status

The draft has valuable coverage: all declared header modes, embedded/external
indexes, dense/one-bit/difflist/LD hardcalls, phase, biallelic dosage/HDS,
multiallelic hardcall patches, PVAR/PSAM companions, exact metadata filters,
sample selection, and local/object-store range paths.

Validation performed during this audit:

- `cargo test -p datafusion-bio-format-pgen --all-targets --no-run`: passed;
- 13 unit tests and 8 integration tests: passed; and
- required external differential test with Python `pgenlib` 0.94.1: passed in
  15.3 seconds.

The differential fixture is intentionally small and does not demonstrate
throughput, allocation behavior, catalog scalability, or all current oracle
semantics. In particular, the draft's `DS` behavior is inconsistent: it may use
hardcall fallback within a record carrying dosage while returning null for the
same hardcall in a record without any dosage track.

## Draft PR #221 Hot-Path Audit

The following findings are release blockers, ordered by expected impact.

1. `decode_record_and_main` constructs full-sample hardcall categories, full
   `GT`, `PHASED`, `DS`, and `HDS` vectors for every row regardless of projected
   genotype children. Projection currently avoids only some final copies, not
   the dominant decode/allocation work.
2. Decoded rows are retained as a second intermediate representation and then
   copied into Arrow arrays at batch flush. Wide cohorts therefore pay for full
   row materialization plus Arrow materialization.
3. The dense path uses a generic per-sample packed-value reader that assembles a
   word from several bytes. Dense two-bit decoding needs a specialized byte or
   word kernel and a direct builder/gather path.
4. LD bases are stored in a map, cloned before decode, and copied again by the
   main decoder. The specification requires only the latest eligible base.
5. Local coalesced reads open/seek and allocate per range instead of reusing one
   partition-local handle and buffer.
6. PVAR and PSAM are fully read into owned strings; compressed metadata can
   exist in both compressed and decompressed buffers. Large catalogs need
   streaming or compact interned storage.
7. The complete PGEN record index is eagerly expanded into owned descriptors.
   Block-lazy metadata is more scalable and can produce the first batch sooner.
8. Partition weights are encoded bytes only. Decoded sample width, projected
   fields, sparse representation work, and LD preludes can dominate bytes and
   produce imbalance.
9. The default genotype projection includes all children. The draft Criterion
   benchmark selecting `genotypes` therefore does substantially more work than
   the `snputils` `GT` benchmark.
10. Conventional companion handling should explicitly cover the supported
    compressed PVAR/PSAM suffix matrix and enforce official PVAR header rules.

## Measured Baseline

The audit generated a fully phased biallelic PGEN with the official Python
writer: 16,384 variants × 1,024 samples with 0.5% missing calls. File sizes were
5,296,184 bytes PGEN, 305,486 bytes PVAR, and 5,039 bytes PSAM.

Host: Apple M3 Max, 16 physical cores. Rust 1.91.0, Python 3.11.9,
`snputils` `482c6d1`, and `pgenlib` 0.94.1 were used. DataFusion target
partitions and all Python/native numerical pools were constrained to one for
the parity measurement. The page cache was warm.

| Path | Time | Relative to Rust p1 |
| --- | ---: | ---: |
| `snputils` phased `GT`, median | 70.4 ms | 5.56x faster |
| direct `pgenlib` list/read/cast, median | 79.4 ms | 4.93x faster |
| draft Rust, 1 partition | 391.4 ms | 1.00x |
| draft Rust, 2 partitions | 208.2 ms | 1.88x |
| draft Rust, 4 partitions | 114.3 ms | 3.42x |
| draft Rust, 8 partitions, fresh run | 105.2 ms | 3.72x |

The Rust Arrow output estimate was 203,555,664 bytes versus 33,554,432 bytes
for the compact `snputils` allele array. This is not a physically identical
output-layout comparison; output bytes must always accompany the timing.
Nevertheless, the Rust one-partition path is about 5.5 times slower, and even
four partitions do not reach the one-thread oracle. The parity requirement is
therefore unmet.

The existing draft benchmark uses a 2,048 × 128 synthetic fixture, reuses an
already-open context, selects the entire `genotypes` struct, and has no pinned
external baseline or thread-pool controls. It is useful for local regression
tracking but not for release qualification.

## Target Architecture

```text
PVAR/PSAM + block index
          |
          v
 projection/sample/filter plans (once)
          |
          v
 contiguous cost-balanced DataFusion partitions
          |
          v
 per-partition reader + reusable DecodeWorkspace
          |
          +--> skip/validate unprojected tracks
          +--> specialized dense/one-bit/difflist kernels
          +--> one in-place LD base
          +--> selected samples in requested order
          |
          v
 direct Arrow builders --> bounded RecordBatch stream
```

Key invariants:

- no nested thread pool;
- no full-cohort logical vector for an unprojected child;
- no retained decoded-row copy after values enter Arrow builders;
- no LD state proportional to the number of base records;
- no metadata-only PGEN record reads;
- memory bounded by projected output/batch budget plus one workspace per active
  partition; and
- partition weights include decode/output work as well as encoded bytes.

Because PGEN exposes exactly two encoded allele slots in this initial contract,
the inner Arrow value is a nullable `FixedSizeList<UInt16, 2>`, not a variable
list. This removes a 32-bit offset per sample without changing allele order or
missingness.

## Implementation And Acceptance Sequence

### Gate 0: Contract And Stack

1. Approve this OpenSpec amendment.
2. Rebase PR #221 onto current PR #217 without reverting core/BCF changes.
3. Add the pinned benchmark fixture generator and oracle runner before changing
   hot loops so every optimization has a comparable baseline.

### Gate 1: Biallelic `GT` Parity

1. Introduce `ProjectionPlan`, `SamplePlan`, and reusable partition workspaces.
2. Add a dedicated `GT`-only path with no phase/dosage/HDS allocations.
3. Decode dense two-bit data with byte/word lookup or measured portable SIMD and
   append directly to Arrow builders.
4. Reuse a local handle/range buffer and remove intermediate decoded rows.
5. Pass all correctness tests and the hard one-thread no-slower-than-`snputils`
   median gate.

This gate is first because it is the user's explicit performance criterion and
the least ambiguous like-for-like semantic path.

### Gate 2: Compressed Representations And Selection

1. Specialize one-bit and difflist paths for identity and sparse sample plans.
2. Replace the LD map/clones with one in-place base workspace.
3. Validate selected-sample ordering and representation-level differential
   fixtures.
4. Benchmark dense, sparse-sample, one-bit, difflist, and LD-heavy datasets.

### Gate 3: Auxiliary Semantics

1. Implement effective `DS` and optional stored-only `DS_STORED` exactly.
2. Make phase, dosage, and HDS parsers allocation-free when unprojected.
3. Validate missing dosage/hardcall coupling and HDS/DS quantization.
4. Complete multiallelic hardcall and allele-width boundary suites; keep
   non-finalized multiallelic dosage explicitly unsupported.

### Gate 4: Catalog, Remote I/O, And Multicore Scale

1. Make index metadata block-lazy and PVAR/PSAM storage compact/streaming.
2. Validate local and object-store request/byte metrics.
3. Balance partitions by estimated decode/output cost with valid LD preludes.
4. Publish 1/2/4/8-partition throughput, efficiency, balance, duplicate prelude
   work, output bytes, allocations, and peak RSS.
5. Run workspace format, check, clippy, tests, conformance, fuzz/property, and
   release gates before enabling the provider.

## Gate 1 Implementation Result

The approved Gate 1 work was implemented after this initial audit. PR #221 was
restacked onto genotype-core `558503c`; a recoverable local backup preserves its
old `d13ac8e` head. The exact `GT` path now uses a partition-local sample map and
workspace, one LD base, direct Arrow buffers, nullable fixed-size allele pairs,
and a dense lookup kernel that consumes four hardcalls and their phase bits at a
time.

The committed fixture generator parameters are 16,384 variants, 1,024 samples,
0.5% missing calls, and seed `20260816`. On the audit host, ten measured
post-warmup iterations gave:

| Reader | Partitions | Median | Speedup vs Rust p1 |
| --- | ---: | ---: | ---: |
| pinned `snputils` | 1 | 71.60 ms | 0.46x |
| Rust PGEN | 1 | 32.92 ms | 1.00x |
| Rust PGEN | 2 | 18.26 ms | 1.80x |
| Rust PGEN | 4 | 9.72 ms | 3.39x |
| Rust PGEN | 8 | 7.03 ms | 4.68x |

Rust and the oracle produced the identical digest
`16692912:8342602:8347936:560154221234404`. Maximum RSS in the one-thread
process runs was 163,889,152 bytes for Rust and 587,841,536 bytes for Python
`snputils`. Gate 1 therefore passes even though Rust materializes a wider Arrow
representation.

## Auxiliary Semantics Implementation Result

The first Gate 3 contract item is also implemented. `DS` now returns the
physically stored biallelic dosage when present and exact hardcall dosage
otherwise; `DS_STORED` exposes only the physical track. Hand-checked fixtures
cover hardcall-only, sparse stored, dense stored, missing, and override cases.
The decoder rejects the fixed-width `65535` dosage sentinel when its hardcall
is present, as required by the format. Existing explicit and specification-
defined implicit haplotype dosage behavior remains intact.

## Release Decision

The original draft should not be merged, but its restacked Gate 1 successor has
resolved the explicit single-thread performance blocker. It can proceed through
the compressed-representation, dosage/ploidy, catalog, remote-I/O, and full
workspace release gates with the oracle harness kept independent from runtime
code.
