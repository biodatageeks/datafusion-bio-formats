# Structure benchmark smoke run

`cargo run -p datafusion-bio-format-foldcomp --features text-formats --example
benchmark_structures` records parse-only, atom/residue-to-Arrow, and complete scans
using 1/2/4/8 DataFusion target partitions. The example reports time to first batch
separately from completion, processes 32 copies of 1UBQ, and selects keys 0 and 7
from the frozen Foldcomp example database. Run with `--release` for performance
investigation and compare equivalent work before drawing speedup conclusions.

The checked-in CSV is a **development-build smoke run**, Rust 1.91.0, macOS arm64,
2026-09-09. The input hashes and native/library versions are in the oracle manifest
and native notices. Iteration 0 is the first execution in an existing process;
iteration 1 is a repeat. Filesystem caches were not flushed. These small workloads
check completion, stable row counts and usable partition scaling; they are not a
production throughput/RSS guarantee or an oracle-library speed comparison.

The companion polars-bio script `scripts/benchmark_structures.py` measures the full
Python API in a fresh process per format/level/worker case, including startup RSS,
with three repeated collections. Query metrics independently test exactly K native
payload decodes. Large-database, cold-cache and release-profile comparative
benchmarking remains an explicitly separate performance acceptance exercise.
