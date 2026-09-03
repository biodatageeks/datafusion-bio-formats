## ADDED Requirements

### Requirement: Bounded PVAR Companion Loading

The system SHALL load text companions as a bounded stream of newline-aligned
blocks so that transient memory does not grow with the companion size, and
SHALL hold the parsed variant table in a columnar form whose resident cost is
a small constant per variant.

#### Scenario: Compressed companion larger than the block window
- **WHEN** a `.pvar.zst` decodes to many times the block window
- **THEN** the provider decodes and parses it block by block
- **AND** at no point holds more decoded text than the configured window of
  blocks in flight.

#### Scenario: Resident cost per variant
- **WHEN** a PVAR of biallelic SNP rows is loaded
- **THEN** the resident variant table costs at most 80 bytes per variant
  beyond the allele and ID bytes themselves.

#### Scenario: Order and errors are preserved across blocks
- **WHEN** a malformed line or the `max_variants` limit falls in a later block
- **THEN** rows are emitted in source order
- **AND** the error reports the same line number a single-pass parse would.

#### Scenario: Header must fit the first block
- **WHEN** the PVAR header does not end within the first block
- **THEN** planning fails with an error naming the companion.

### Requirement: Compact Variant Selection

The system SHALL represent a scan's selected variants without a per-variant
index vector when the selection is every row or one contiguous run, and with
four-byte indices otherwise.

#### Scenario: Full scan
- **WHEN** a scan has no exact PVAR filter and no limit
- **THEN** the selection is a count, not an index vector
- **AND** partition planning and LD dependency lookups behave as before.

#### Scenario: Sparse filter
- **WHEN** exact PVAR filters select scattered rows
- **THEN** the selection holds one four-byte index per selected row in source
  order.

### Requirement: Reference Panel Companion Defaults

The system SHALL open the published PLINK 2 reference panels with default
options, and SHALL keep every companion cap as a configurable sanity bound
whose error names the option to raise.

#### Scenario: PGS Catalog 1000 Genomes panel
- **WHEN** `pgsc_1000G_v1/GRCh38_1000G_ALL.pgen` or the GRCh37 fileset is
  opened with default options
- **THEN** the fileset opens and the variant count matches the PGEN header.

#### Scenario: Limit error names the option
- **WHEN** a companion exceeds `max_companion_bytes`,
  `max_decompressed_companion_bytes`, `max_variants`, or `max_samples`
- **THEN** the error states the option name and the configured value.

#### Scenario: Caps remain enforceable
- **WHEN** a caller lowers a companion cap below the fileset's size
- **THEN** planning fails with that limit error before genotype records are
  read.
