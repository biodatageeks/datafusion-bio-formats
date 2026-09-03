## 1. Reader selection
- [ ] 1.1 Add `GenotypeMatrixReader::with_selection(VariantSelection)`; validate indices against the variant count.
- [ ] 1.2 Make `shape()`, `positions()`, and `read_into` follow the selection; destination size check uses the selected count.
- [ ] 1.3 `PvarTable::row_range(contig, start, end, coordinates)` with a monotone-span check.

## 2. Tests
- [ ] 2.1 Range and sparse selections decode the same cells as the full read sliced by the same rows, on the oracle fixtures.
- [ ] 2.2 Region lookup on a multi-contig fixture; unknown contig is empty; out-of-range index errors name the count.
- [ ] 2.3 `pgenlib` parity on a sparse selection.
