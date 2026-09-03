## Context

`read_into` (matrix.rs:182) plans payload partitions from
`Arc::new((0..variants).collect())`, and `positions()` returns every start.
The PVAR table after the memory-model change is columnar and sorted in file
order, with contigs interned, so contiguous regions map to contiguous row
ranges when the PVAR is coordinate-sorted, which PLINK 2 requires.

## Goals / Non-Goals

- Goals: decode only the rows a caller asks for; make a region request cost
  one binary search; keep partitioning and the direct-to-destination write
  path unchanged.
- Non-Goals: predicate expressions on the matrix path; reordering rows;
  sample selection changes (already supported).

## Decisions

- **Decision: selection is `VariantSelection`, resolved by the caller.**
  `GenotypeMatrixReader::with_selection(VariantSelection)` sets the rows;
  `shape()`, `positions()`, and `read_into` honour it. Sparse selections are
  sorted, deduplicated `u32` indices; an out-of-range index is a planning
  error naming the variant count.
- **Decision: region lookup lives on the variant table.**
  `PvarTable::row_range(contig, start, end, coordinates) -> Range<usize>`
  uses the contig's row span and a binary search on positions. An unknown
  contig yields an empty range, not an error.
- Alternatives: pushing DataFusion predicates through the matrix path
  (heavier, and the scan path already covers it); returning an iterator of
  row chunks (a caller can already loop over index ranges).

## Risks / Trade-offs

- A PVAR that is not coordinate-sorted makes region ranges wrong; PLINK 2
  rejects such files, and the lookup verifies the span is monotone and
  errors otherwise.
