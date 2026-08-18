# PGEN scan performance — handover

State as of 2026-08-17. Everything below is measured on this machine unless
marked otherwise.

## Where things stand

Whole chromosome 22 of 1000 Genomes: 993,881 variants × 2,548 samples =
2,532,408,788 genotypes. Single partition, release build with
`-C target-cpu=native`.

The Rust-only scan, excluding materialization — `pgen_ds_profile`, interleaved
against the previous build in one session:

| field | before fusion | after fusion | speedup |
|---|---:|---:|---:|
| `DS` (float32) | 2.31 s | **1.19 s** | 1.94× |
| `ALT_COUNT` (int8) | 1.65 s | **0.59 s** | 2.80× |

Down from 11.2 s for `DS` at the start of the work. Peak RSS is unchanged at
9.97 GB for `DS` and 2.9 GB for `ALT_COUNT`.

The full through-Python comparison, re-measured with this build, all readers
interleaved in one session:

| | dosage (float32) | hardcall (int8) |
|---|---:|---:|
| pgenlib | 1.78 s | 0.83 s |
| snputils (wraps pgenlib) | 3.18 s | 1.49 s |
| polars-bio, start of this work | 4.34 s | 2.96 s |
| **polars-bio, now** | **1.85 s** | **0.94 s** |

pgenlib and snputils reproduced their earlier figures to within 1%, so the
polars-bio deltas are the change and not session drift.

Fusion alone took the end-to-end figures to 3.23 s / 1.94 s — much less than the
scan's 1.94× / 2.80×, because materialization was then the larger term. Removing
it (polars-bio's `read_pgen_matrix`, which streams the scan's batches into a
preallocated array instead of consolidating them into an Arrow buffer first) is
what produced the rest. polars-bio is now 1.04× pgenlib on dosage and 1.14× on
hardcall.

Note the harness no longer charges module import time to the read; see
polars-bio's `HANDOVER-pgen-perf.md`.

Correctness: bit-identical to pgenlib across all 2,532,408,788 cells in both
workloads, at every partition count, with a self-test proving the comparison
can fail. `ALT_COUNT` and `DS` additionally match the `GT` scan — which never
takes the fused path — across all 2,532,408,788 cells of chr22, via
`cargo run --release --example pgen_field_parity -- <path.pgen>`.

## Branches

| Branch | Commit | State |
|---|---|---|
| `perf/pgen-batch-array-build` | branch tip | PR #232, open, pushed. The tip moves with docs commits; the code state is `25d6bd2` |
| `perf/pgen-2bit-packed` | `52e9fcf` | pushed, **no PR** — one commit, misleadingly named |

`perf/pgen-2bit-packed` contains a difflist-buffer reuse, not a packed
representation. Fold it into #232 or rename it.

polars-bio `feat/bgen-pr220-bench` (pushed) pins the provider at `25d6bd2`, the
last commit that touched code on this branch, so it carries the fusion. Later
commits here are documentation only — no need to re-pin for them.

## What was already done

1. Arrow values/validity buffers built directly instead of a per-cell
   `ListBuilder::append_option`. Isolated bench: 352 → 860 Melem/s.
2. `DS` joined the single-field fast path that `GT` had, removing a per-variant
   `Vec<Option<f32>>`.
3. `append_codes` rewritten table-driven with bulk validity.
4. Hardcall phase orientation skipped for dosage projections — a phased
   heterozygote and an unphased one carry the same dosage, so applying it was a
   full pass over every sample of every phased record for no change in output.
5. `ALT_COUNT` column added: hardcall allele count as `Int8`, one byte per
   genotype instead of the four `DS` needs.
6. Expansion loops made auto-vectorizable — no lookup table, no `Vec::push`.
   The count for codes 0..=4 is `0,1,2,0,1`, which is
   `code - 3 * (code >= 3)`. LLVM now emits NEON.
7. Difflist buffer reused across variants instead of allocated per record.
8. **The common-value + difflist branch fused into the output buffer** — what
   was task 1 below. `DS` and `ALT_COUNT` fill their Arrow values slice from the
   common category and patch the difflist into it, one pass instead of two, for
   81% of records. See "How the fused decode works" below.

## Correction to issue #233

**Issue #233 proposes the wrong optimization for this workload.** It argues for
keeping the decoded main track 2-bit packed, the way pgenlib does. That reading
came from the LD-compressed branch. It is wrong for the record type that
actually dominates.

Tracing which path records take on a `plink2 --make-pgen` fileset:

| record type | share | branch |
|---|---:|---|
| `0x14` | 81% | common value + difflist (`record_type & 7 == 4`) |
| `0x11`, `0x12` | 13% | LD-compressed |
| `0x10` | 3.8% | dense, eligible for the direct decode |

The dominant branch has **no per-sample base to pack**. `decode_main_into` does
`output.resize(sample_count, common)` and then patches a sparse difflist.
pgenlib's equivalent is:

```c
vecset(genovec, vrtype_low2 * kMask5555, vec_ct);   // packed, vectorized
```

— a SIMD memset over `sample_ct/4` bytes, followed by `Expand2bitTo8` writing
`sample_ct` bytes at the end. Total `sample_ct/4 + sample_ct` written.

Our output is one value per sample regardless. A **fused** fill writes
`sample_ct` bytes and nothing else, which is *fewer* writes than pgenlib's
packed-then-expand. Packing would make us slower here, not faster.

Update #233 before anyone acts on it.

## How the fused decode works

`supports_common_difflist_fast_path` picks out main-track representations 4, 6
and 7. `decode_common_difflist_into` parses only the difflist, returns the common
category, and leaves validated patches in `GtDecodeWorkspace::patches`. The
builders' `append_common_difflist` then `resize`s their values slice to the
mapped common category — one write per sample, nothing read back — and patches
into it. Validity is one `append_all_valid`/`append_all_invalid` run plus a
`set_invalid`/`set_valid` per patch, so bitmap work is proportional to the
difflist, not the sample count.

Three exclusions, each load-bearing:

- **`GT`.** It distinguishes `(0,1)` from `(1,0)`, and the fused decode discards
  the phase orientation. `FastFieldBuilder::needs_phase()` gates this.
- **LD bases.** A record a later LD-compressed record uses as its base needs its
  full main track materialized anyway, so `retain_main` skips fusion. That is
  ~13% of records, and it is why representation 4 still reaches
  `decode_biallelic_gt_into` sometimes.
- **Anything with a dosage, HDS or multiallelic track.**
  `supports_biallelic_gt_fast_path` rejects those; their bytes would be left
  unparsed.

The hardcall-phase track is still validated and consumed — its length depends on
the heterozygote count, so misparsing it would shift every following track. The
count is derived from the difflist rather than from the per-sample buffer fusion
removes:

```
het_count = (common == 1 ? sample_count - difflist_len : 0)
          + (number of difflist entries whose value is 1)
```

PGEN reserves representation 5, so `common == 1` cannot occur and the first term
is always zero today; it is written out anyway because the identity is what makes
the count right. Phase validation is shared with the dense path via
`validate_phase_track`.

## Optimizations to implement, in order

### 1. Return sample names from the matrix decode

`read_genotype_matrix` is wired into polars-bio and returns positions, but
`read_pgen_matrix` still opens the fileset a second time through `scan_pgen`
purely to read sample names out of the schema metadata. That second open parses
the 108 MB PVAR again.

On dosage the decode is large enough to absorb it. On hardcall it is not: that
workload **regressed 9% at one partition and 4% at four** when this landed
(0.694 s to 0.759 s, 0.365 s to 0.380 s), while gaining 1.36x at eight. The fix
is to return the selected sample names alongside the shape and positions, the
same way positions were moved, and drop the metadata scan from `read_pgen_matrix`
entirely.

Measured, correctness-gated against pgenlib:

| | 1 partition | 4 | 8 | scaling |
|---|---:|---:|---:|---:|
| dosage | 1.345 s | 0.574 s | **0.468 s** | 2.87x |
| hardcall | 0.759 s | 0.380 s | **0.304 s** | 2.50x |

### 2. SIMD the difflist patch loop

The remaining per-variant work is the sparse patch. Patches are scattered writes,
so vectorization is limited, but the sample-index delta decoding
(`Cursor::varint`, `decode_difflist_into`) is sequential varint work that showed
up at ~10% of profile samples before fusion — re-profile, since fusion changed
the denominator substantially.

### 3. Peak RSS — resolved

Dosage now peaks at 13.50 GB against pgenlib's 12.38 GB for the identical
10.13 GB output, down from 22.31 GB; hardcall is 6.13 GB against 5.14 GB, down
from 8.25 GB. Task 1 should close the rest, since it removes the Arrow
intermediate entirely. The old 17.9 → 22.8 GB regression was the DataFrame path's second
full copy of the values, which `read_pgen_matrix` does not make. Nothing in the
decode was ever implicated: the Rust-only scan has been flat at 9.97 GB for `DS`
and 2.9 GB for `ALT_COUNT` throughout.

### 4. Consider exposing a sparse genotype form

Out of scope for matrix materialization, but pgenlib's
`ReadDifflistOrGenovecSubsetUnsafe` returns `difflist_common_geno` + `raregeno`
+ `sample_ids` without ever materializing a full genotype vector. Callers that
only aggregate — allele counts, frequencies, missingness — skip O(sample_ct)
work per variant entirely. That is exactly the shape of a query-engine
workload, and it is a capability polars-bio does not have. Larger than a
perf change; a real feature.

## What not to do

**Do not narrow `DS` from `Float32`.** PGEN dosages are genuinely fractional —
a dosage fileset holds values like `0.125`, and the hardcall and dosage tracks
of the same record disagree completely:

```
dosages   : [ 0.125  1.0  1.875  missing ]
hardcalls : [ missing  1  missing  missing ]
```

`ALT_COUNT` already covers the hardcall case at one byte per genotype. An
intermediate version of the fast path returned hardcalls where dosages were
asked for; the `pgenlib_written_filesets_decode_without_the_oracle` and
`differential_pgenlib_and_snputils_oracles_when_installed` tests caught it.
Keep both green.

## How to measure

Rust only, no Python, no materialization — this is the number to optimize. The
third argument is the genotype field, `DS` by default:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release \
  -p datafusion-bio-format-pgen --example pgen_ds_profile -- \
  /path/to/chr22.full.pgen 3 DS
```

Build the binary once and interleave it against the previous one rather than
comparing across sessions — see pitfall 5:

```bash
cp target/release/examples/pgen_ds_profile /tmp/prof_new   # then git stash, rebuild, /tmp/prof_old
for i in 1 2 3; do /tmp/prof_old $PGEN 1 DS; /tmp/prof_new $PGEN 1 DS; done
```

Correctness gate on real records at full scale, no Python: `ALT_COUNT` and `DS`
against the `GT` scan, which never takes the fused decode. Exits nonzero on any
disagreement:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release \
  -p datafusion-bio-format-pgen --example pgen_field_parity -- \
  /path/to/chr22.full.pgen
```

Full reader comparison with the correctness gate:

```bash
cd bioformats-benchmark
.venv-bcf/bin/python run_pgen_benchmarks.py --runs 3 --modes dosage hardcall \
  --polars-bio-partitions 1 8 --pgen /path/to/chr22.full.pgen \
  --expected-rows 993881 --expected-samples 2548 \
  --output results/pgen_full_t1.json
```

Isolated array-build microbenchmark:

```bash
RUSTFLAGS="-C target-cpu=native" cargo bench \
  -p datafusion-bio-format-pgen --bench pgen_float_build
```

## Measurement pitfalls, each of which produced a wrong number here

1. **Build profile.** A plain `maturin develop` is a debug build and measured
   3.1× slower — enough to invert the comparison against snputils. Always
   `RUSTFLAGS="-C target-cpu=native" maturin develop --release`. The release
   extension is ~228 MB, debug ~336 MB.
2. **Core count.** pgenlib, snputils, and the `bgen` package are all
   single-threaded. Comparing polars-bio at 8 partitions against them measures
   core count, not the reader. Use one partition for any claim.
3. **Native APIs.** Every reader must use its own fastest path.
   `pgenlib.read_list` (bulk) is 5.5× faster than a per-variant `read` loop;
   `snputils.read_pgen(genotype_mode="dosage")` is 27× faster than
   `PGENReader().read()` plus a 3-D sum.
4. **Workload naming.** snputils' `genotype_mode="dosage"` returns int8
   *hardcall counts*. pgenlib separates `read_list` from `read_dosages_list`.
   Comparing across that boundary compares different data.
5. **Cross-session timings drift.** polars-bio measurements varied by up to
   1.6× across this session while pgenlib stayed within 4%. Interleave the
   readers in a single session; do not compare against a number from an earlier
   run.
6. **snputils is not an independent oracle for PGEN** — it calls
   `pgenlib.read_list` directly. The load-bearing check is polars-bio against
   pgenlib.

## Source references

Read from `plink-ng/2.0`:

- `include/pgenlib_read.cc` — `ParseAndApplyDifflist` (LD diffs applied
  directly into the packed array), `PrunedDifflistToGenovecSubsetUnsafe` and
  `ReadDifflistOrGenovecSubsetUnsafe` (the `vecset` common-value fill and the
  sparse-form return).
- `include/pgenlib_misc.h` — `GenoarrLookup256x1bx4`, `InitLookup256x1bx4`,
  `Expand2bitTo8`. Note the comment in `Expand2bitTo8`: the 256-entry table
  lookup "takes ~3-4x as long" as the SIMD path, so the table is their
  fallback, not their fast path.
