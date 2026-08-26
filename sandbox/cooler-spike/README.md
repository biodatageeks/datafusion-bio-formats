# cooler-spike

HDF5 static-link feasibility spike for the cooler (`.cool`/`.mcool`) format
provider (polars-bio OpenSpec change `add-cool-mcool-support`, task 1.2).

Builds a standalone binary that statically links libhdf5 1.14.6 via
`hdf5-metno` (`static` + `zlib` features) alongside DataFusion 53 / Arrow
58.3, then reads cooler-generated fixtures end to end: root attributes,
fixed-ASCII chrom names, enum-typed `bins/chrom` (soft conversion to i32),
balancing weights, `chrom_offset`/`bin1_offset` indexes, offset hyperslab
pixel reads, int-vs-float count dtype detection, and `.mcool` resolution
discovery.

Run against the provider crate's fixtures:

```
cargo run --release -- ../../datafusion/bio-format-cooler/tests/data
```

## Wheel-CI validation results (2026-08-25)

Validated in polars-bio's wheel CI matrix (run
https://github.com/biodatageeks/polars-bio/actions/runs/32836622787, branch
`spike/hdf5-static-wheels`, since deleted). All targets built the static
libhdf5, ran the binary against the fixtures, and produced correct output:

| Target | Result | Job time (build + run) |
|---|---|---|
| manylinux2014 x86_64 (glibc 2.17, in-container) | pass | 11m59s |
| ubuntu-latest | pass | 11m30s |
| Windows MSVC x64 | pass | 23m42s |
| macOS aarch64 (macos-latest) | pass | 13m30s |
| macOS x86_64 (cross-compiled on arm64 macos-14) | pass | 9m33s |

Local macOS arm64: marginal HDF5 build cost ~35s; statically linked binary
depends only on system libs (no HDF5 dylib).

Key finding: the `zlib` feature is mandatory alongside `static` — without it
attribute reads succeed but any gzip-compressed dataset read fails at runtime
with `can't open directory (/usr/local/hdf5/lib/plugin)`.
