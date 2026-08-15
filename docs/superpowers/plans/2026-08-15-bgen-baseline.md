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

| Mode | Layout | Partitions | Baseline | After tasks 4-5 | Change |
| --- | --- | --- | --- | --- | --- |
| probability | nested | 1 | 1.0190 s | | |
| probability | nested | 8 | 329.72 ms | | |
| probability | fixed | 1 | 925.68 ms | | |
| probability | fixed | 8 | 253.04 ms | | |
| dosage | nested | 1 | 630.90 ms | | |
| dosage | nested | 8 | 165.64 ms | | |

### `chr22.first-25000.bgen` — 25,000 x 2,548, mixed widths 3/4

| Mode | Layout | Partitions | Baseline | After tasks 4-5 | Change |
| --- | --- | --- | --- | --- | --- |
| probability | nested | 1 | 1.1211 s | | |
| probability | nested | 8 | 524.85 ms | | |
| probability | fixed | 1 | **unsupported** | | |
| probability | fixed | 8 | **unsupported** | | |
| dosage | nested | 1 | 633.00 ms | | |
| dosage | nested | 8 | 169.67 ms | | |

**"unsupported" is a baseline result, not a gap in the measurement.** The fixed
layout rejects a mixed-width file, so the bench harness skips those two cases
and says so on stderr. Task 9 is what makes them run; when they appear in the
"after" column, that is the deliverable landing.

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
