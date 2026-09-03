## ADDED Requirements

### Requirement: PGEN Matrix Row Selection

The dense genotype matrix reader SHALL decode only a caller-selected set of
PVAR rows, given as a contiguous index range or sorted row indices, and SHALL
resolve a contig coordinate range to a row range without a full PVAR pass.

#### Scenario: Range selection
- **WHEN** a matrix is read for rows `a..b`
- **THEN** the shape is `(b - a) × samples`, positions cover only those rows,
  and the cells equal the corresponding rows of a full read.

#### Scenario: Sparse selection
- **WHEN** a matrix is read for a sorted list of row indices
- **THEN** rows appear in that order and no other record is decoded.

#### Scenario: Region lookup
- **WHEN** a caller asks for the rows of a contig between two coordinates
- **THEN** the reader returns the contiguous row range by binary search
- **AND** an unknown contig yields an empty range.

#### Scenario: Out-of-range index
- **WHEN** a selected index is not below the variant count
- **THEN** planning fails with an error naming the variant count.
