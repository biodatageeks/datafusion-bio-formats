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
