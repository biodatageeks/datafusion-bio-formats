# PGEN scan performance — handover

State as of 2026-08-17. Everything below is measured on this machine unless
marked otherwise.

## Where things stand

Whole chromosome 22 of 1000 Genomes: 993,881 variants × 2,548 samples =
2,532,408,788 genotypes. Single partition, release build with
`-C target-cpu=native`, interleaved against pgenlib in one session.

| | dosage (float32) | hardcall (int8) |
|---|---:|---:|
| pgenlib | 1.77 s | 0.83 s |
| snputils (wraps pgenlib) | 3.24 s | 1.51 s |
| **polars-bio** | **4.34 s** | **2.96 s** |

The scan alone, excluding materialization, is 2.42 s against pgenlib's 1.51 s
for the same float32 output — **1.60×**. Down from 11.2 s at the start of the
work.

Correctness: bit-identical to pgenlib across all 2,532,408,788 cells in both
workloads, at every partition count, with a self-test proving the comparison
can fail.

## Branches

| Branch | Commit | State |
|---|---|---|
| `perf/pgen-batch-array-build` | `099be29` | PR #232, open |
| `perf/pgen-2bit-packed` | `52e9fcf` | pushed, **no PR** — one commit, misleadingly named |

`perf/pgen-2bit-packed` contains a difflist-buffer reuse, not a packed
representation. Fold it into #232 or rename it.

polars-bio `feat/bgen-pr220-bench` pins the provider at `52e9fcf`
(uncommitted `Cargo.toml`/`Cargo.lock` at time of writing).

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

## Optimizations to implement, in order

### 1. Fuse the common-value + difflist branch into the output buffer

The highest-value remaining change, targeting 81% of records.

Today: `decode_main_into` fills a `Vec<u8>` of codes with the common value,
patches the difflist, and then `append_codes` reads that buffer and writes the
final `f32`/`i8`. Two full passes over every sample.

Fused: fill the builder's output slice directly with `map(common)` — where
`map` is `alt_count_from_code` for `ALT_COUNT`, or its `f32` cast for `DS` —
then apply the difflist patches straight into that slice. One pass.

Expected: removes one full read+write pass over 2.53 billion cells.

**The obstacle, and it is the whole difficulty of this change.** Records of
type `0x14` carry a hardcall-phase track that must still be validated and
consumed even when its orientation is discarded. Validating it needs the
heterozygote count, which today comes from scanning the per-sample buffer that
fusion eliminates. It is derivable without that buffer:

```
het_count = (common == 1 ? sample_count - difflist_len : 0)
          + (number of difflist entries whose value is 1)
```

Both terms are available while parsing the difflist. Get this wrong and the
phase track is misparsed, which will show up as a trailing-bytes error rather
than silently — but verify against `validated_dense_hardcalls`, which already
does the equivalent validation for the dense branch.

Applies only when the field needs no phase orientation, i.e. `DS` and
`ALT_COUNT`, not `GT`. `FastFieldBuilder::needs_phase()` already encodes this.

### 2. SIMD the difflist patch loop

After (1), the remaining per-variant work is the sparse patch. Patches are
scattered writes, so vectorization is limited, but the sample-index delta
decoding (`Cursor::varint`, `decode_difflist_into`) is sequential varint work
that showed up at ~10% of profile samples. Worth looking at only after (1).

### 3. Investigate the dosage peak-RSS increase

Between the `8fbed14` and `52e9fcf` builds, dosage peak RSS rose from
17.9 GB to 22.8 GB while the hardcall path stayed flat at 8.45 GB. This was not
a target of any change and has no explanation yet. It should be understood
before these numbers are published; a benchmark that quietly regressed memory
by 27% is not a clean result.

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

Rust only, no Python, no materialization — this is the number to optimize:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release \
  -p datafusion-bio-format-pgen --example pgen_ds_profile -- \
  /path/to/chr22.full.pgen 3
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
