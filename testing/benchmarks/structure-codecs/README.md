# Paired migration measurements

Run from the repository root with Python 3.12+, Rust 1.91.0 and `/usr/bin/time`:

```sh
python3 testing/benchmarks/structure-codecs/run.py \
  --output target/codec-benchmark-release.json --samples 9 --seconds 0.2
```

The driver builds current Rust release workers and separate native workers from
immutable historical commit `fe9c879b87e71b61f39c16d8cccd4f607b3f08eb`, extracted
under ignored `target/codec-benchmark-baseline/`. Only that development reference
build needs C++; current reader crates contain no native build scripts or FFI.
The backends use the same inputs, options, schemas, copies, worker counts and
complete materialization. Toolchain, lock and binary provenance are retained.
Two untimed rounds warm code/allocators per fresh process; backend order alternates.
Historical reports preceding cutover used both backends in one test executable.

Frozen local protocol:

- Nine fresh-process samples per backend/case; a separate native calibration fixes
  equal iteration counts for both backends, targeting 0.2 seconds of timed work.
- Input hashes cover 1UBQ CIF/FCZ, a deterministic 64-block CIF expansion, both long
  FCZ stress cases, database keys 0/7, and all 24 database entries.
- Stages separate raw CIF parsing/views, normalized decoding, Arrow-only work,
  decode-to-Arrow, and full DataFusion query execution over shared benchmark sources.
- Atom and residue schemas are fully materialized. Queries use 1/2/4/8 configured
  workers. The small CIF query uses eight sources; other cases preserve their
  natural source counts. Configured workers do not imply every case can occupy them.
- Every sample pair must agree on row counts, decoder calls and processed bytes.
  Query decoder calls must equal the number of selected benchmark sources.
- Peak RSS is per fresh process, including runtime/startup/input buffers and
  warmups. Time and first-batch latency exclude setup and preloading.
- Median Rust/native time or RSS ratio above 1.10 is flagged. A median absolute
  deviation above 5% of either backend's median time **or RSS** marks the case
  noisy/inconclusive. Noise does not grant acceptance or a threshold exception.

These measurements use preloaded bytes and warm filesystem/code caches; no cache
flush is attempted. The shared `EntrySource` exercises the real provider and
Arrow/geometry path, but excludes production storage/sidecar-selection overhead.
The two database cases decode identical frozen selected ranges. Existing provider
regressions separately verify actual selection, zero/K decode counts and corrupt
unselected records. Python collection, cold storage, large external databases and
installed wheels remain outside this harness; local results do not close the whole
performance gate.

The output retains all samples, iteration counts, input/binary hashes, dependency
lock hash, platform/toolchain, medians, noise estimates and local budget flags.
`--datasets` and `--stages` restrict a diagnostic run; do not present those subsets
as a full acceptance run. Optional binary paths avoid rebuilding; provide all four `--structure-bin`,
`--foldcomp-bin`, `--native-structure-bin` and `--native-foldcomp-bin` paths.
Run performance measurements without concurrent builds/fuzz campaigns.

For a native hosted Linux ARM64 follow-up, dispatch:

```sh
gh workflow run structures.yml --repo biodatageeks/datafusion-bio-formats \
  --ref feat/rust-structure-codecs -f performance_validation=true
```

Separate runners collect the full 93-case matrix with one-second calibration
and the targeted Arrow diagnostics. The latter repeats ordinary Arrow cases
first, then uses `--inspect-layout` with `arrow,arrow_clone,residue` stages.
`arrow_clone` deep-clones decoded entries once before timing; `residue` measures
the common conformer/geometry construction without Arrow columns. Inspection
records atom-vector capacity, selected string capacities and distinct 4 KiB
pages containing selected string starts, plus a normalized Debug-value hash
that must match between backends. These are allocation-layout indicators, not
total allocation or resident-page measurements. The inspection and clone alter
setup/heap state, so these runs are diagnostics, not substitutes for ordinary
acceptance measurements. `profile_arrow.py` subsequently samples the common
Arrow/residue paths with Linux perf's software CPU clock. It retains profiler
failures explicitly and never turns an unavailable profile into acceptance.

For a reproducible Linux ARM64 container run, including the source-identical
array comparison first:

```sh
python3 testing/fuzz/structure-codecs/linux_check.py --arch arm64 --benchmark
```

This writes `target/codec-linux-arm64/benchmark.json` and image metadata. On an
ARM host, `--arch amd64` uses emulation and cannot establish native x86 performance.

The committed [macOS ARM64 observations](results/2026-09-20-macos-arm64.json)
retain all 93 cases and samples. Seventeen cases meet the local budget; 76 are
noisy/inconclusive, including one CIF pipeline case above the median RSS threshold.
This run does not establish performance acceptance.

The [Linux ARM64 container observations](results/2026-09-20-linux-arm64.json)
contain 85 cases within budget, seven noisy cases, and one isolated
residue-to-Arrow regression after decoding the mixed long chain. All 28 CIF cases
meet the local budget. A longer [nine-sample, one-second diagnostic](results/2026-09-20-linux-arm64-arrow-diagnostic.json) repeats the
Arrow finding at about 10.6% (versus 17.7% in the full run); the corresponding
full pipeline/query cases remain within budget. Keep that isolated finding and
the noisy cases open rather than treating them as a complete acceptance result.

## Installed Python wheels and local storage

`consumer.py` compares separately installed native and Rust release wheels. Build
both from the same polars-bio source/dependency lock, toolchain, default mimalloc
allocator and release options; change only the formats backend routing. Install
them into separate Python 3.12 environments with identical package versions.
Pass each environment's executable without resolving its virtualenv symlink:

```sh
python3 testing/benchmarks/structure-codecs/consumer.py \
  --native-python /path/to/native/.venv/bin/python \
  --rust-python /path/to/rust/.venv/bin/python \
  --native-wheel /path/to/native.whl --rust-wheel /path/to/rust.whl \
  --output target/codec-consumer-benchmark.json --samples 9 --seconds 0.5
```

Every sample runs with isolated Python (`-I`) in a temporary directory outside
the source checkout. Before measurement, the driver verifies identical Python
and installed package versions, and matches each installed extension's hash to
its supplied wheel. The public eager reader constructs the scan, reads real
files/sidecars, selects database entries and materializes every output column.
Inputs are the seven frozen core workloads plus empty database selection; atom
and residue levels use 1/2/4/8 DataFusion partitions and Polars/Rayon threads.
Single-source inputs may not occupy every configured worker.

Nine fresh-process sample pairs alternate backend order. Native calibration
sets an equal iteration count targeting 0.5 seconds, with two untimed full
reads per process. Each pair must match schemas, row counts and sorted seeded
row hashes covering every column. This additional hash check requires bitwise
output agreement on the measured host; it does not replace tolerance-aware
cross-platform oracle checks. Samples retain time, process peak RSS, first
collection latency, iterations, package provenance and input/wheel hashes.

The same 10% median time/RSS and 5% MAD/median noise thresholds apply. RSS includes
imports and warmups but is captured before untimed output hashing. The recorded
first collection excludes imports and is neither first-batch latency nor a cold
filesystem measurement. Filesystem caches are warm; no cache flush is attempted.
Actual bytes read and decoder calls are not collected by this harness; input
sizes are provenance, not I/O counters. Provider tests separately enforce zero/K
decodes. Large external databases, cold storage and other wheel platforms remain
separate acceptance work. Use `--datasets`/`--workers` only for clearly labeled
diagnostics and run benchmarks after local builds/fuzzing have stopped.

The [macOS ARM64 installed-wheel run](results/2026-09-20-consumer-macos-arm64.json)
completes all 64 cases with matching schemas and every-column hashes. Forty-five
cases meet the local time/RSS budget; 19 exceed the noise threshold. One noisy
full-database atom case at one worker has a 1.578 median time ratio (native time
MAD/median 58%, Rust 13%). No stable case exceeds the budget, but these results
do not establish complete performance acceptance. Keep that case and the noisy
observations open; the earlier isolated Linux Arrow regression is also unresolved.

A [longer database-only diagnostic](results/2026-09-20-consumer-database-diagnostic.json)
uses nine pairs and a two-second native calibration at one worker. The slower
atom observation does not repeat: atom/residue time ratios are 0.728/0.729 and
RSS ratios 0.962/0.966, with both cases within budget. This targeted follow-up
supplements the retained full run; it does not clear every noisy case.

After timing, `consumer_counters.py` runs separate untimed `EXPLAIN ANALYZE`
checks through the installed wheels:

```sh
python3 testing/benchmarks/structure-codecs/consumer_counters.py \
  --native-python /path/to/native/.venv/bin/python \
  --rust-python /path/to/rust/.venv/bin/python \
  --output target/codec-consumer-counters.json
```

[All 64 atom/residue/worker cases](results/2026-09-20-consumer-counters-macos-arm64.json)
have matching source/entry/row/payload counters. Database reads decode exactly
0/2/24 selected entries. CIF's counter counts normalized blocks, so the expanded
file reports 64 entries from one source. DataFusion formats large byte/row counts
with rounded K/M suffixes; reports preserve those strings and compare payload
totals within their displayed precision against exact fixture/index lengths.
Payload bytes include FCZ database terminators and exclude sidecar reads. These
are logical counters, not physical I/O measurements. Extension hashes tie the
untimed counter checks to the measured wheel installations.


The [2026-09-21 hosted ARM64 Arrow follow-up](results/2026-09-21-hosted-arrow.json)
uses nine pairs with two-second calibration at pre-cutover revision `fe9c879`.
The mixed-chain residue case is noisy (time ratio 1.123); the long single-segment
residue case is stable above budget (1.211). Atom cases remain within budget.
The separate [layout/clone diagnostic](results/2026-09-21-hosted-layout.json)
has matching normalized value hashes and all ratios near parity (at most 1.016).
Inspection changes setup allocations, so this demonstrates sensitivity to setup
and does not invalidate the uninstrumented finding or establish its precise cause.
[CPU sampling was unavailable](results/2026-09-21-hosted-profile-status.json):
the runner rejected software-clock sampling with a PMU sampling/interrupt error.
No profiler-based explanation or performance acceptance is claimed.

The matching [93-case uninstrumented release run](results/2026-09-21-hosted-release.json)
completes with 89 cases within budget, two regressions and two noisy cases.
The isolated mixed-chain and single-segment residue-to-Arrow time ratios are
1.194 and 1.187; their RSS ratios are 0.975 and 0.944. Complete residue pipelines
for those same inputs remain faster (time ratios 0.733 and 0.672). The two noisy
cases are single-segment atom queries at four/eight workers. These pre-cutover
measurements retain their original acceptance status after production cutover;
workflow success alone does not close the performance gate.
