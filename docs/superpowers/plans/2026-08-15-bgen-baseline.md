# BGEN scan benchmark, before and after

Rust-level scan time only, from the opt-in `BGEN_BENCH_PATH` bench in
`datafusion/bio-format-bgen/benches/bgen_scan.rs`. This deliberately excludes
the Python round trip: the handover's stage split put the Rust scan at 0.27 s of
a 0.393 s end-to-end probability read, and 253 ms below is the same quantity
measured directly.

Host: Apple M3 Max, 16 cores, 64 GiB, macOS 15.6 arm64.

Command, identical for every column:

```bash
BGEN_BENCH_PATH=~/research/data/BGEN/<fixture>.bgen \
  cargo bench -p datafusion-bio-format-bgen --bench bgen_scan -- real_ \
  --sample-size 10 --warm-up-time 2 --measurement-time 10
```

Criterion reports `[lower median upper]`; the table carries the median.

## Baseline — `2c06f23` plus tasks 1-2

### `chr22.first-25000.unphased.bgen` — 25,000 x 2,548, uniform width 3

| Mode | Layout | Partitions | Baseline | After | Change |
| --- | --- | --- | --- | --- | --- |
| probability | nested | 1 | 1.0190 s | 783.36 ms | **-23%** |
| probability | nested | 8 | 329.72 ms | 220.02 ms | **-33%** |
| probability | fixed | 1 | 925.68 ms | 746.23 ms | **-19%** |
| probability | fixed | 8 | 253.04 ms | 203.17 ms | **-20%** |
| dosage | nested | 1 | 630.90 ms | 590.69 ms | -6% |
| dosage | nested | 8 | 165.64 ms | 158.16 ms | -5% |

### `chr22.first-25000.bgen` — 25,000 x 2,548, mixed widths 3/4

| Mode | Layout | Partitions | Baseline | After | Change |
| --- | --- | --- | --- | --- | --- |
| probability | nested | 1 | 1.1211 s | 891.06 ms | **-21%** |
| probability | nested | 8 | 524.85 ms | 253.88 ms | **-52%** |
| probability | fixed | 1 | **unsupported** | 787.63 ms | **now runs** |
| probability | fixed | 8 | **unsupported** | 225.67 ms | **now runs** |
| dosage | nested | 1 | 633.00 ms | 583.03 ms | -8% |
| dosage | nested | 8 | 169.67 ms | 156.41 ms | -8% |

Every row improved, and dosage — already 1.9x ahead of snputils and the thing
this rework most risked spending — improved too.

The headline for the phased fixture is the pair of rows together: its best
available probability read went from 524.85 ms (nested, 8 partitions, the only
option) to 225.67 ms (fixed, 8 partitions, newly possible). That is 2.3x, and
it is the fixture the project was furthest behind snputils on.

**The mixed-width file gained the fixed layout early.** The plan expected this
at task 9; it arrived with the batch buffers, because moving padding into the
buffers replaced the per-variant width equality check with a per-sample bound.
Variant 0 of this fixture is phased and four states wide, which happens to be
the file's widest, so the probed width already covers every sample and the
narrower unphased variants pad to it. Task 8's catalog-derived width is still
needed for the general case — a file whose variant 0 is *not* the widest.

## End to end, against snputils

`run_bgen_benchmarks.py --runs 3`, fresh process per reader, polars-bio through
the fixed probability layout at 8 partitions. This is the comparison the goal is
stated in; the tables above are the Rust scan inside it.

### Probabilities, 25,000-variant slices

| Fixture | Reader | Before | After |
| --- | --- | --- | --- |
| unphased | **polars-bio, 8 partitions** | 0.393 s | **0.292 s** |
| unphased | snputils | 0.361 s | 0.363 s |
| unphased | bgen | — | 0.319 s |
| phased, mixed width | **polars-bio, 8 partitions** | 0.660 s | **0.340 s** |
| phased, mixed width | snputils | 0.386 s | 0.397 s |
| phased, mixed width | bgen | — | 0.332 s |

Both goals are met. The unphased slice went from 9% behind snputils to **1.24x
ahead**; the phased slice from 71% behind to **1.17x ahead**. Every reader
produced the same `value_sha256` on each fixture.

polars-bio is still marginally behind the `bgen` package on the phased fixture
(0.340 s against 0.332 s) and ahead of it on the unphased one. `bgen` is the
oracle, not the target, but it is the honest bound on what is left.

### Dosage, full chromosome 22 — the number that must not move

993,881 variants x 2,548 samples.

| Reader | Published | Now |
| --- | --- | --- |
| polars-bio, 8 partitions | 7.073 s | 7.334 s |
| snputils | 13.403 s | 13.665 s |
| ratio | 1.895x | **1.86x** |

Both readers came in about 3% slower than the published run, so the absolute
shift is machine state rather than code; the ratio is what this row is for and
it held. Dosage was the workload this rework most risked spending.

## Partition scaling is capped by range granularity, not by the decoder

Measured on `chr22.first-25000.unphased.bgen`, probability output, fixed layout,
Rust scan only:

| Partitions | Time | Speedup |
| --- | --- | --- |
| 1 | 743.97 ms | 1.00x |
| 2 | 641.80 ms | **1.16x** |
| 4 | 354.23 ms | 2.10x |
| 8 | 204.37 ms | 3.64x |

The 2-partition step is 1.16x, and it is 1.16-1.17x on every workload measured —
dosage and probability, phased and unphased. That reproducibility is what makes
it a plan property rather than noise.

**Root cause.** `plan_payload_partitions` caps each coalesced range at
`payload_bytes / target_partitions`, and a variant's payload cannot be split
across ranges, so the scan gets `target_partitions + 1` indivisible chunks of
roughly equal size. `target + 1` chunks never divide evenly into `target`
partitions: one partition always takes two of them. The planned byte shares are

| Target | Shares |
| --- | --- |
| 2 | **87.2%**, 12.8% |
| 4 | 43.6%, 21.8%, 21.8%, 12.9% |
| 8 | 21.8%, 10.9% x5, 21.8%, 2.0% |

and 1 / 0.872 = 1.15, which is the measured 2-partition speedup. The decoder is
not the limit; the largest partition is.

**Confirmed by experiment.** Capping ranges at a fixed 128 KiB instead of one
partition's share, changing nothing else:

| Partitions | Default cap | 128 KiB cap | Speedup, 128 KiB |
| --- | --- | --- | --- |
| 1 | 743.97 ms | 752.56 ms | 1.00x |
| 2 | 641.80 ms | **414.36 ms** | 1.82x |
| 4 | 354.23 ms | **234.60 ms** | 3.21x |
| 8 | 204.37 ms | **147.76 ms** | 5.09x |

One partition is unchanged, so this is purely balance: 8 partitions gain another
28%. Set `BGEN_BENCH_MAX_RANGE_BYTES` to reproduce.

**Not fixed here, deliberately.** This is pre-existing behaviour on the base
branch, unrelated to the decode path this work changed, and the fix is a
trade-off rather than a constant: finer ranges mean more object-store requests,
which is cheap locally and expensive against remote storage. A real fix wants a
cap of `payload_bytes / (target_partitions * k)` with a floor, chosen with
remote reads in mind, plus its own spec scenario and review. Worth its own
change.

## The inner loop was not touched

The plan gated a `byte_probabilities_into` rewrite (23% of the profile) on this
measurement: run it only if a fixture was still behind snputils. Neither is, so
it was not run. That work remains available if the gap to the `bgen` package on
the phased fixture is ever worth closing.

## A measurement bug found and fixed here

The first run of this comparison reported the phased fixture as *regressed* by
4-16%. It had not. Criterion saves a baseline per benchmark id, and the ids did
not include the fixture, so the phased run compared itself against the unphased
run's saved numbers — reporting the difference between two files as a change in
the code. Ids are now `real_<fixture>_<mode>_<layout>_p<n>`. The absolute
numbers above are unaffected; the change column is computed against the recorded
baseline medians, not against criterion's `change:` line.

## Noise

The 8-partition rows are noisy at this sample size — `real_probability_nested_p8`
spans 297-413 ms on the unphased fixture and 355-879 ms on the phased one,
against a 253-255 ms spread for `real_probability_fixed_p8`. Treat an 8-partition
change under ~15% as unproven and re-run with a larger `--sample-size` before
believing it. The 1-partition rows are tight (under 1%) and are the better
signal for a decoder change.

## Discipline

- Every column in these tables uses the command above, unchanged.
- Nothing else runs on the machine during a measurement. The first attempt at
  this baseline was discarded because a `cargo test` and a `cargo clippy` ran
  against the same target directory while the bench was sampling.
- Fresh-process numbers compare only against fresh-process numbers, and
  `datafusion.execution.batch_size` does not matter — an apparent win from
  raising it was a cold-versus-warm artifact.
