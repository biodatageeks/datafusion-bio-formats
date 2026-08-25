# Design: Cooler (.cool/.mcool) format provider

## Context

A Cooler data collection is an HDF5 group containing:

- `chroms`: chromosome names and lengths
- `bins`: chromosome references and genomic start/end coordinates, with
  optional value columns such as balancing `weight`
- `pixels`: sorted sparse COO rows (`bin1_id`, `bin2_id`, `count`)
- `indexes`: `chrom_offset` and CSR-like `bin1_offset` arrays

A `.cool` file stores one collection at the root. An `.mcool` file stores one
collection per resolution under `/resolutions/<bin-size>`. Root/group
attributes describe the collection without reading pixel data.

Unlike most formats in this workspace, Cooler has no noodles reader. The
provider therefore needs an HDF5 access layer while retaining DataFusion's
projection, filter, statistics, and partition contracts.

## Goals / Non-Goals

### Goals

- Scan local Cooler collections as bounded Arrow record batches.
- Preserve valid stored numeric ranges and metadata values without narrowing.
- Avoid reading or decoding arrays excluded by the projection.
- Use Cooler indexes to prune supported first-axis genomic predicates.
- Accelerate common chunked shuffle/deflate files without weakening the
  compatibility of the libhdf5 path.

### Non-Goals

- Writing, balancing, zoomifying, or aggregating Cooler files.
- Dense matrix output or `.scool` single-cell containers.
- Remote object-store reads in the initial provider.
- Exact second-axis predicate pruning.

## Decisions

### HDF5 access and packaging

Use `hdf5-metno` with the `static` and `zlib` features. Static linking keeps
downstream artifacts self-contained, while zlib is required to read the
shuffle/deflate datasets emitted by `cooler` and h5py. All structural and
range errors include the offending group or dataset context.

### Collection addressing

`CoolerTableProvider::new` accepts a local file path, an optional resolution,
or a cooler URI such as `contacts.mcool::/resolutions/10000`.

- A root collection is selected directly.
- An explicit group URI selects that group; a conflicting resolution errors.
- An `.mcool` resolution selects `/resolutions/<resolution>`.
- A file with one stored resolution can resolve it implicitly.
- Multiple stored resolutions without a selection error and list the choices.

Detection follows HDF5 structure rather than relying only on the extension.

### Output schemas and numeric fidelity

Joined mode emits `chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, and
`count`. Non-negative signed or unsigned stored coordinates are emitted as
Arrow UInt64 so the complete standard int64 coordinate range is retained;
one-based starts use checked addition. Raw mode emits `bin1_id`, `bin2_id`, and
`count`.

The `pixels/count` physical dtype selects Int32, Int64, UInt32, UInt64, or
Float64 output without narrowing. Narrow signed and unsigned storage may widen
to Int32 when that preserves the complete storage range. Optional balancing
weights are emitted as Float64 for both axes.

Collection `sum` attributes remain a tagged Int64, UInt64, or Float64 value.
Legacy string-typed numeric attributes are parsed into the narrowest exact
class rather than through a floating intermediate.

### Projection and statistics

Only datasets required by the projected fields are loaded. Joined-coordinate
projections load only the required chromosome, start, end, or weight bin
metadata once per scan and skip unused bin/axis/count work. An empty
projection constructs row counts from metadata/index ranges without building
the direct pixel cache or decoding any pixel column. The execution plan
reports the projection for inspection. Direct-chunk indexes are also built and
cached independently per projected pixel column, so projecting `count` does
not visit either ID dataset and projecting one axis does not index the other.

### Predicate pruning

The sorted `bin1_id` order maps first-axis chromosome and coordinate predicates
to bin ranges, then to pixel row ranges through `chrom_offset` and
`bin1_offset`. Supported filters are reported `Inexact` so consumers reapply
them and preserve exact semantics. Unsupported or second-axis predicates do
not alter the row range.

### Partitions and HDF5 locking

Partition planning divides the pruned row space into contiguous ranges aligned
to `bin1` boundaries. HDF5 calls remain short and coarse because the library
serializes access; decompression, joining, and Arrow construction happen
outside that critical section where possible. Every partition produces a
disjoint slice of the same rows as a single-partition scan.

### Direct chunk path and compatibility fallback

For contiguous logical rows in chunked numeric datasets, the optimized path
reads raw chunks, validates filter metadata and byte order, inflates deflate,
undoes byte shuffle, and slices values directly. It is enabled only for
unfiltered chunks or the supported shuffle-plus-deflate pipeline with no
skipped filter masks.

Dataset indexing validates all recorded chunks before scan execution. Any
unsupported layout, filter, per-chunk mask, byte order, reference-probe
mismatch, or decode precondition disables the optimized path for that column;
the entire column then uses libhdf5. This avoids mid-stream failures and keeps
the optimization observationally equivalent to the compatibility reader.

At provider construction, the three parallel pixel arrays are required to be
one-dimensional and equal-length. Before joined indexing, projected bin arrays
are required to match `bins/chrom`,
`bins/chrom` must reference `chroms/name`, and decoded pixel bin IDs must fall
within the bins table. CSR offsets are validated whenever pruning or aligned
partition planning consumes them. Malformed references return contextual
DataFusion errors rather than panics.

## Risks / Trade-offs

- Static HDF5 increases clean build time. This is accepted to avoid a runtime
  system-library dependency.
- libhdf5's global lock limits parallel read scaling. Coarse reads and
  out-of-lock Arrow work reduce, but do not remove, that constraint.
- Direct decompression duplicates a narrow part of HDF5 filter handling. A
  conservative preflight and whole-column fallback contain this risk.
- Very high-resolution collections can have millions of bins. Bin metadata is
  loaded once, while the much larger pixel table stays batch-bounded.

## Migration Plan

The change is additive: add the crate and workspace member, then let consumers
adopt its public provider. Rollback removes the new workspace member and
consumer dependency without changing existing format providers.

## Open Questions

- Should a future release add a seekable local cache for remote Cooler files?
- Should supported second-axis predicates use per-bin binary search within
  each `bin1` row?
