## ADDED Requirements

### Requirement: Bulk Sequence Decoding

BAM scans SHALL decode a record's packed 4-bit bases into the `sequence`
column in bulk, producing two bases per packed byte from a lookup table,
rather than through a per-base iterator. The resulting string MUST be
byte-identical to the per-base decoding it replaces.

#### Scenario: Even and odd base counts
- **WHEN** a record has an odd number of bases
- **THEN** the decoded string has exactly that many characters
- **AND** the low nibble of the last packed byte is ignored.

#### Scenario: All base codes
- **WHEN** every one of the 256 packed byte values is decoded
- **THEN** the output matches `=ACMGRSVTWYHKDBN` indexed by the high and low
  nibble.

#### Scenario: Empty sequence
- **WHEN** a record has no bases
- **THEN** the `sequence` value is an empty string.

### Requirement: Bulk Quality Encoding

BAM scans SHALL convert a record's quality scores to the `quality_scores`
column with a slice-wide offset of 33 when every score is at most 93. When any
score exceeds 93, the scan SHALL produce the same string as the per-character
conversion it replaces. Missing quality scores SHALL produce an empty string.

#### Scenario: Valid Phred scores
- **WHEN** every score of a record is between 0 and 93
- **THEN** the value is the scores offset by 33 as ASCII characters.

#### Scenario: Out-of-range scores
- **WHEN** at least one score is above 93
- **THEN** the value equals the output of the per-character conversion used
  before this change.

#### Scenario: Missing quality
- **WHEN** a record's quality scores are all `0xFF`
- **THEN** the value is an empty string.

### Requirement: Safe Decoding Helpers

The decoding helpers SHALL live in `bio-format-core` so that alignment format
crates share them, and SHALL be implemented without `unsafe` code or
architecture-specific intrinsics.

#### Scenario: Shared by alignment paths
- **WHEN** the BAM crate decodes records on its local, remote, indexed and SAM
  paths
- **THEN** all four use the shared helpers.

#### Scenario: No unsafe code
- **WHEN** the helpers and their call sites are compiled
- **THEN** they contain no `unsafe` blocks and no `std::arch` usage.
