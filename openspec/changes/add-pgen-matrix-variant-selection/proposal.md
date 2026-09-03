# Change: Row selection for the dense PGEN genotype matrix

## Why

`GenotypeMatrixReader` always decodes every variant: `read_into` builds the
selection as `0..variants` and requires a destination of `variants × samples`
cells. On the PGS Catalog 1000 Genomes panel (75.2M variants, 3,202 samples)
that is 224 GiB for `ALT_COUNT` and 896 GiB for `DS`, so once the companion
cap is fixed (`refactor-pgen-companion-memory-model`) the call fails on
allocation instead. Scoring and QC workflows only need the variants that
match a score file or a region, typically well under a few million rows.

## What Changes

- `GenotypeMatrixReader` accepts a row selection: a contiguous index range or
  a sorted list of PVAR row indices, expressed through `VariantSelection`.
  The shape, positions, and destination size follow the selection.
- The fileset exposes index lookup by contig and coordinate range, so a caller
  can turn a region into a row range without parsing the PVAR itself.
- No change for callers that select everything.

## Impact

- Affected specs: `pgen` (Dense matrix requirements gain a selection clause).
- Affected code: `datafusion/bio-format-pgen/src/matrix.rs`, `fileset.rs`
  (contig and range lookup on the variant table).
- Depends on `refactor-pgen-companion-memory-model` for `VariantSelection`
  and the columnar table.
