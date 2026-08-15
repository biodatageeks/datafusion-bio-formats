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
