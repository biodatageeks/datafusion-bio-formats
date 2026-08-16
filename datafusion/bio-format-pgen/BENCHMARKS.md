# PGEN benchmark snapshot

The Criterion fixture is generated at runtime and contains 2,048 biallelic
variants by 128 samples. It commits no genotype data.

The matrix measures dense, one-bit, difflist, and LD-heavy hardcalls; metadata
projection; 1-in-16 sparse variant selection; three selected samples; dosage
output; and a four-partition dense scan.

Snapshot from Apple M3 Max, arm64, macOS 15.6, Rust 1.91.0:

| Scan | Median |
| --- | ---: |
| Dense hardcalls | 6.84 ms |
| One-bit hardcalls | 6.55 ms |
| Difflist hardcalls | 6.56 ms |
| LD-heavy hardcalls | 6.47 ms |
| Metadata only | 112.00 us |
| Sparse variants, 1/16 selected | 5.99 ms |
| Three selected samples | 2.69 ms |
| Dosage output | 5.37 ms |
| Dense hardcalls, four partitions | 1.93 ms |

Run it with:

```shell
cargo bench -p datafusion-bio-format-pgen --bench pgen_scan
```

Treat these as same-machine regression baselines, not cross-machine targets.

## Pinned `snputils` parity gate

The release gate uses an official-`pgenlib` writer fixture with 16,384 fully
phased biallelic variants, 1,024 samples, 0.5% missing calls, and RNG seed
`20260816`. Both readers materialize all `GT` calls. DataFusion target
partitions, Tokio workers, Polars, Rayon, OpenMP, and numerical-library pools
are constrained to one for the single-thread comparison.

Pinned oracle versions:

- `snputils` `482c6d1dfd6c4001935dfaec81ae01a5e0ec3e53`;
- `pgenlib` 0.94.1; and
- NumPy 2.4.6.

Apple M3 Max result from 2026-08-16, ten measured iterations after warmup:

| Reader | Partitions/threads | Median | Output bytes | Maximum RSS |
| --- | ---: | ---: | ---: | ---: |
| `snputils` phased `GT` | 1 | 71.60 ms | 33,554,432 | 587,841,536 |
| Rust PGEN `GT` | 1 | 32.92 ms | 69,272,392 | 163,889,152 |
| Rust PGEN `GT` | 2 | 18.26 ms | 69,272,392 | not sampled |
| Rust PGEN `GT` | 4 | 9.72 ms | 69,273,232 | not sampled |
| Rust PGEN `GT` | 8 | 7.03 ms | 69,274,912 | not sampled |

The Rust single-partition path is 2.18x faster despite emitting a nullable
Arrow `UInt16` allele-pair representation. Four partitions reach 3.39x the
one-partition throughput. The Rust and Python genotype digests were identical:

```text
16692912:8342602:8347936:560154221234404
```

Create an isolated oracle environment, generate the fixture, and run both
readers with:

```shell
python -m venv .venv-pgen-parity
.venv-pgen-parity/bin/pip install -r \
  datafusion/bio-format-pgen/benchmark/requirements-pgen-parity.txt
.venv-pgen-parity/bin/python \
  datafusion/bio-format-pgen/benchmark/pgen_oracle.py \
  generate /tmp/pgen-parity/fixture

POLARS_MAX_THREADS=1 RAYON_NUM_THREADS=1 OMP_NUM_THREADS=1 \
OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
.venv-pgen-parity/bin/python \
  datafusion/bio-format-pgen/benchmark/pgen_oracle.py \
  benchmark /tmp/pgen-parity/fixture --iterations 10

cargo run --release -p datafusion-bio-format-pgen \
  --example pgen_parity -- /tmp/pgen-parity/fixture.pgen 10 1
```

Set `PGEN_PROFILE=1` on the Rust command to print per-partition read, decode,
append, and Arrow-finalization timing together with record-representation
counts. Profiling is disabled by default and does not create another thread
pool.

The Python dependencies are external benchmark/oracle tools. They are not
linked, imported, or vendored by the Rust crate.
