# Change: Add Cooler (.cool/.mcool) format provider

## Why

Cooler `.cool` and `.mcool` files are the standard sparse HDF5 containers for
binned Hi-C contact matrices. DataFusion needs a native table provider so
clients can query those matrices lazily without routing data through the
Python `cooler`/pandas stack.

The companion polars-bio `add-cool-mcool-support` change is the original
cross-repository feature plan. This local change records the provider's own
schema, execution, correctness, and performance acceptance criteria so they
can be reviewed and archived in this repository.

## What Changes

- Add the `datafusion-bio-format-cooler` workspace crate for local `.cool` and
  `.mcool` data collections.
- Resolve single- and multi-resolution collections from a path, explicit
  resolution, or cooler `file::/group` URI.
- Expose joined genomic coordinates or raw COO pixels, optional balancing
  weights, lossless count dtypes, and metadata-only collection discovery.
- Push projections and supported first-axis genomic predicates into the scan,
  and partition pixel row ranges along `bin1` boundaries.
- Add an optimized direct HDF5 chunk path with a libhdf5 fallback for layouts
  or filter masks that cannot be decoded safely.
- Add reference-generated fixtures and integration coverage for scan modes,
  pushdown, partitions, exact wide values, and fallback behavior.

## Impact

- Affected specs: `cooler-format-provider` (new capability)
- Affected code:
  - workspace `Cargo.toml` and `Cargo.lock`
  - `datafusion/bio-format-cooler/`
- New build dependency: statically linked `hdf5-metno` with zlib support
- Runtime scope: local seekable files only; remote object stores remain out of
  scope for this provider version
- Companion consumer: `biodatageeks/polars-bio` Cooler APIs
