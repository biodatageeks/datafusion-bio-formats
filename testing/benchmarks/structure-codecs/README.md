# Paired migration measurements

Run from the repository root with Python 3.12+, Rust 1.91.0 and `/usr/bin/time`:

```sh
python3 testing/benchmarks/structure-codecs/run.py \
  --output target/codec-benchmark-release.json --samples 9 --seconds 0.2
```

The driver builds the two release unit-test binaries once. The ignored
`migration_benchmarks::worker` test selects its backend from an explicit benchmark
configuration. This selection is confined to tests; no production flag or fallback
is added. Both backends run from the same executable, with the same inputs,
options, schema, copies, worker count and materialization. Two untimed rounds warm
code/allocators in each fresh process. Backend order alternates by sample pair.

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
as a full acceptance run. Optional binary paths avoid rebuilding; both are required.
Run performance measurements without concurrent builds/fuzz campaigns.

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
Actual bytes read and decoder calls are not exposed by this Python API; input
sizes are provenance, not I/O counters. Provider tests separately enforce zero/K
decodes. Large external databases, cold storage and other wheel platforms remain
separate acceptance work. Use `--datasets`/`--workers` only for clearly labeled
diagnostics and run benchmarks after local builds/fuzzing have stopped.
