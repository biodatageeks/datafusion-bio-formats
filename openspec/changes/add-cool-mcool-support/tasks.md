# Tasks: add-cool-mcool-support

## 1. Crate and HDF5 foundation

- [x] 1.1 Add `datafusion-bio-format-cooler` to the workspace with DataFusion,
  Arrow, async-stream, and logging dependencies.
- [x] 1.2 Configure `hdf5-metno` for static linking with zlib so standard
  shuffle/deflate Cooler datasets work in self-contained builds.
- [x] 1.3 Add local-path validation and contextual HDF5 error conversion.

## 2. Collection resolution and schema

- [x] 2.1 Parse plain paths and cooler `file::/group` URIs, resolve `.cool`
  roots and `.mcool` resolutions, and report ambiguous or missing resolutions.
- [x] 2.2 Read `chroms`, `bins`, `indexes`, and collection attributes with
  shape, range, and reference validation.
- [x] 2.3 Provide joined-coordinate and raw-COO schemas, optional balancing
  weights, and lossless signed, unsigned, or floating `count` types.
- [x] 2.4 List data-collection metadata without scanning pixels and preserve
  signed, unsigned, and floating `sum` attribute classes.

## 3. Scan execution and pushdown

- [x] 3.1 Stream pixel row ranges into bounded Arrow batches and join bin
  coordinates by validated array indexing.
- [x] 3.2 Push projections into execution, including an empty-projection row
  count path that does not index or decode pixel datasets.
- [x] 3.3 Prune supported first-axis genomic filters through `chrom_offset`
  and `bin1_offset`, while reporting filter support as inexact.
- [x] 3.4 Partition scans on `bin1` boundaries and preserve equivalence with a
  single-partition scan.
- [x] 3.5 Decode supported HDF5 chunks directly and select the libhdf5 fallback
  before execution for unsupported filters, masks, byte order, or layouts.

## 4. Correctness and documentation

- [x] 4.1 Generate and commit small `.cool`/`.mcool` fixtures with the Python
  `cooler` reference implementation.
- [x] 4.2 Cover full, projected, raw, weighted, filtered, partitioned,
  metadata-only, and error paths with integration tests.
- [x] 4.3 Cover Int64, UInt32, UInt64, Float64, coordinates above `u32::MAX`,
  exact metadata sums above `2^53`, and direct-chunk fallback cases.
- [x] 4.4 Document the public provider API, schema, addressing forms,
  coordinate behavior, weights, and local-file limitation.

## 5. Verification

- [x] 5.1 Run `cargo fmt --all -- --check`.
- [x] 5.2 Run `cargo test -p datafusion-bio-format-cooler`.
- [x] 5.3 Run `cargo clippy -p datafusion-bio-format-cooler --all-targets -- -D warnings`.
- [x] 5.4 Validate this change with `openspec validate add-cool-mcool-support --strict`.
