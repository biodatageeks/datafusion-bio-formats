# Change: Refactor the PGEN companion memory model

## Why

The PGEN provider rejects the published PGS Catalog 1000 Genomes reference
panel (`pgsc_1000G_v1`, biodatageeks/polars-bio#453): its `.pvar.zst` is
541–592 MiB, over the 512 MiB `max_companion_bytes` default, and decodes to
2.3–2.5 GiB of 75.2M (GRCh38) and 84.8M (GRCh37) variants, over the 1 GiB
`max_decompressed_companion_bytes` default and within 15% of the 100M
`max_variants` default. Those
caps exist because `PgenFileset::open` holds the whole compressed companion,
the whole decoded text, and a `Vec<PvarVariant>` with six heap objects per
row at once. Measured on a chr22 slice of the same panel (993,881 variants,
108 MiB of text) the open peaks at ~600 B per variant, so lifting the caps
alone would make the full panel cost ~45 GB to describe and ~85 GB to scan.
The caps are the symptom; the eager, per-row-allocated materialization is the
cause.

## What Changes

- Decode and parse text companions as a bounded stream of newline-aligned
  blocks instead of two whole-file buffers, keeping the existing chunk-parallel
  parse across blocks in flight. Transient memory becomes a fixed window that
  does not grow with the PVAR.
- Replace `Vec<PvarVariant>` with a columnar `PvarTable`: interned contig
  index, positions, and one byte arena with offsets for IDs and alleles. The
  metadata builders, predicate pruning, and the dense matrix reader read
  through accessors instead of owned `String`s.
- Raise the companion and variant-count defaults so the standard reference
  panels open without tuning, keep every cap as a sanity bound, and name the option to raise in
  each limit error.
- Represent a scan's variant selection compactly: a full or contiguous
  selection carries no per-variant vector, and a sparse one uses `u32`
  indices. Today a full scan of the panel allocates a 574 MB `Vec<usize>`
  just to name every row.
- Add a per-variant memory accounting hook and a test that pins the resident
  cost, plus an opt-in real-panel check.

## Impact

- Affected specs: `pgen` (Standard PGEN Fileset Resolution, PVAR Variant
  Semantics, PGEN Object-Store Range Access gain companion-loading
  requirements).
- Affected code: `datafusion/bio-format-pgen/src/fileset.rs` (companion
  loading and parse), `physical_exec.rs` and `filter.rs` (variant accessors),
  `matrix.rs` (positions), `table_provider.rs` (defaults, validation, selection representation),
  `source.rs` (streaming companion reader).
- Downstream: polars-bio mirrors this change under the same id to expose
  `max_companion_bytes`, `max_decompressed_companion_bytes`, and
  `max_variants`, and bumps to the release carrying this work.
- No output schema or genotype semantics change. Row order, coordinates, and
  error line numbers are preserved.
