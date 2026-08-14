## ADDED Requirements

### Requirement: BGEN Version And Layout Support

The system SHALL read BGEN 1.2 and 1.3 files using Layout 2 as the primary
encoding and Layout 1 as a compatibility encoding.

#### Scenario: Layout 2 input
- **WHEN** a BGEN 1.2 or 1.3 header declares Layout 2
- **THEN** the provider supports its valid phased or unphased probability block.

#### Scenario: Layout 1 input
- **WHEN** a supported BGEN header declares Layout 1
- **THEN** the provider reads its restricted biallelic diploid unphased data
- **AND** does not route it through Layout 2 assumptions.

#### Scenario: Unknown layout or version
- **WHEN** a BGEN header declares an unsupported version, layout, or flag
  combination
- **THEN** planning fails with an unsupported-feature error that identifies the
  combination.

### Requirement: BGEN Header Integrity

The system SHALL validate header size, variant count, sample count, first
variant offset, magic value, flags, and free-data boundaries using checked
arithmetic before variant block decoding.

#### Scenario: Valid extended header
- **WHEN** a BGEN header contains valid free-data bytes before the variant data
- **THEN** those bytes are skipped according to the declared header length
- **AND** the first variant is read at the declared offset.

#### Scenario: Invalid first variant offset
- **WHEN** the declared first variant offset overlaps the header or exceeds
  object length
- **THEN** planning fails before a variant range is issued.

#### Scenario: Count exceeds limits
- **WHEN** declared samples or variants exceed configured hard limits
- **THEN** planning fails before allocating arrays based on those counts.

### Requirement: BGEN Sample Identity Resolution

The system SHALL resolve sample identities from an embedded sample identifier
block, an explicit supported sample companion, or deterministic synthetic
ordinal names, in that precedence order.

#### Scenario: Embedded sample identifiers
- **WHEN** the BGEN sample-identifier flag is set
- **THEN** the provider reads exactly the declared number of identifiers
- **AND** preserves their encoded order.

#### Scenario: External sample metadata
- **WHEN** the BGEN has no embedded identifiers
- **AND** an external sample companion is supplied
- **THEN** its identifiers are used after its count is validated against BGEN.

#### Scenario: No source identifiers
- **WHEN** neither embedded nor external identifiers are available
- **THEN** the provider creates documented one-based names `sample_1` through
  `sample_N`
- **AND** metadata marks them as synthetic.

#### Scenario: Inconsistent sample count
- **WHEN** embedded or external sample metadata count differs from the BGEN
  header
- **THEN** planning fails before probability decoding.

### Requirement: BGEN Variant Metadata Schema

The system SHALL expose the BGEN variant identifier as `id`, the RS identifier
as `rsid`, chromosome, site coordinates, and all encoded alleles in ordered
`alleles: List<Utf8>`.

#### Scenario: Multiallelic metadata
- **WHEN** a BGEN variant declares more than two alleles
- **THEN** every allele is preserved in encoded order.

#### Scenario: BGEN allele identity
- **WHEN** BGEN alleles are projected
- **THEN** the provider does not label the first allele `ref` or the remaining
  alleles `alt`
- **AND** allele indices are documented as zero-based positions in `alleles`.

#### Scenario: Invalid allele count or length
- **WHEN** a variant declares zero alleles, an unsupported allele count, or a
  string length beyond configured limits
- **THEN** decoding fails with variant byte-offset context.

### Requirement: Layout 2 Probability Decode

The system SHALL decode Layout 2 probability blocks for variable ploidy,
phased/unphased state order, multiallelic variants, missing samples, and valid
bit precision without discarding source states.

#### Scenario: Unphased state vector
- **WHEN** an unphased sample has ploidy `P` and allele count `K`
- **THEN** `GP` contains the complete BGEN-specification ordering of
  `C(P + K - 1, K - 1)` genotype probabilities.

#### Scenario: Phased state vector
- **WHEN** a phased sample has ploidy `P` and allele count `K`
- **THEN** `GP` contains the complete BGEN-specification haplotype state order
- **AND** the variant `phased` column is true.

#### Scenario: Variable ploidy
- **WHEN** selected samples in one block have different declared ploidies
- **THEN** each sample has its own `PLOIDY` value
- **AND** its inner `GP` length matches that ploidy.

#### Scenario: Missing sample
- **WHEN** the per-sample ploidy byte marks a genotype missing
- **THEN** that sample's `GP` is null
- **AND** its declared ploidy remains available when defined by the format.

#### Scenario: Omitted final probability
- **WHEN** BGEN stores all but the final probability state
- **THEN** the provider reconstructs the final integer probability from the
  quantization denominator and stored sum
- **AND** rejects a stored sum greater than the denominator.

### Requirement: BGEN Probability Output Mode

The system SHALL provide a default probability-preserving output with
`GP: List<List<Float32>>`, `PLOIDY: List<UInt8>`, variant `phased`, and metadata
describing state order and source bit precision.

#### Scenario: Probability conversion
- **WHEN** source integer probabilities are converted to `Float32`
- **THEN** values are divided by the exact source quantization denominator
- **AND** no probability state is dropped or merged.

#### Scenario: Variable state counts
- **WHEN** samples or variants have different probability state counts
- **THEN** inner `GP` lists retain their actual lengths
- **AND** are not padded with zeros.

#### Scenario: Probability sum tolerance
- **WHEN** reconstructed probabilities are inspected
- **THEN** their sum differs from one only by documented source quantization and
  `Float32` conversion tolerance.

### Requirement: BGEN Biallelic Dosage Output

The system SHALL offer an explicit dosage mode for biallelic variants where
`DS` is the expected count of allele index one and its counted allele is
recorded in Arrow metadata.

#### Scenario: Unphased biallelic dosage
- **WHEN** dosage mode reads a valid unphased biallelic sample
- **THEN** `DS` is computed from all genotype-state probabilities and ploidy.

#### Scenario: Phased biallelic dosage
- **WHEN** dosage mode reads a valid phased biallelic sample
- **THEN** `DS` is the sum of expected allele-index-one copies across
  haplotypes.

#### Scenario: Missing dosage
- **WHEN** the selected BGEN sample is missing
- **THEN** its `DS` value is null.

#### Scenario: Multiallelic dosage request
- **WHEN** dosage mode selects a variant with more than two alleles
- **THEN** the scan fails with a multiallelic-dosage unsupported error
- **AND** does not collapse alternate alleles.

### Requirement: BGEN Compression Support

The system SHALL decode compression combinations allowed by the selected BGEN
layout, including uncompressed and zlib blocks and Layout 2 zstd blocks.

#### Scenario: Uncompressed block
- **WHEN** the header declares no genotype-block compression
- **THEN** the block is decoded without a decompression copy where practical.

#### Scenario: Zlib block
- **WHEN** the header declares zlib compression
- **THEN** the provider validates compressed and uncompressed lengths before
  decoding.

#### Scenario: Layout 2 zstd block
- **WHEN** Layout 2 declares zstd compression
- **THEN** the provider independently decompresses each selected variant block.

#### Scenario: Invalid compression/layout pair
- **WHEN** a compression flag is invalid for the declared layout
- **THEN** the provider returns an unsupported or malformed-header error before
  scanning blocks.

### Requirement: BGI Compatibility And Validation

The system SHALL read the standard SQLite BGI variant index, validate its
variant offsets and sizes against the selected BGEN object, and use a bounded
local cache when the BGI object is remote.

#### Scenario: Local BGI
- **WHEN** a valid local BGI is resolved
- **THEN** it is queried directly without copying the complete BGEN file.

#### Scenario: Remote BGI
- **WHEN** a valid BGI is remote
- **THEN** it is downloaded atomically to the shared bounded cache
- **AND** cache reuse is keyed by remote object identity.

#### Scenario: Stale or inconsistent BGI
- **WHEN** BGI count, offset, size, or object identity is inconsistent with
  BGEN
- **THEN** planning fails or ignores an optional stale index according to an
  explicit documented policy
- **AND** it never reads an out-of-bounds block.

### Requirement: Exact BGI Predicate Pushdown

The system SHALL evaluate supported predicates over standard BGI chromosome,
position, and RS identifier columns through SQLite before BGEN block reads,
then apply variant-identifier predicates to the transient BGEN metadata catalog
because standard BGI does not store the BGEN variant identifier.

#### Scenario: Indexed RS identifier query
- **WHEN** an equality or `IN` predicate selects RS identifiers
- **THEN** only exact matching BGI rows become block candidates
- **AND** pushdown is reported as `Exact`.

#### Scenario: Indexed genomic query
- **WHEN** supported chromosome and position predicates are supplied
- **THEN** BGI selects the exact site rows under the requested coordinate mode.

#### Scenario: Variant identifier query
- **WHEN** an exact predicate selects the BGEN variant identifier
- **THEN** the provider evaluates it against parsed BGEN identifying metadata
- **AND** does not claim that the standard BGI schema contains that identifier.

#### Scenario: BGI filtered limit
- **WHEN** an exact BGI predicate and safe limit are supplied
- **THEN** block planning may stop after the required matching index rows.

### Requirement: BGEN Operation Without BGI

The system SHALL support a valid BGEN without BGI by scanning lightweight
variant metadata and declared block lengths into a transient offset catalog
without decompressing probability payloads.

#### Scenario: Unindexed metadata query
- **WHEN** BGI is absent
- **THEN** the provider can evaluate variant metadata filters by sequentially
  parsing metadata and skipping probability blocks by length.

#### Scenario: Unindexed genotype query
- **WHEN** the transient catalog selects variants
- **THEN** only selected probability blocks are subsequently decompressed
- **AND** source order is preserved for a one-partition scan.

### Requirement: BGEN Projection And Sample Pushdown

The system SHALL skip unprojected probability blocks and construct probability
or dosage values only for selected samples.

#### Scenario: Metadata-only BGEN query
- **WHEN** no genotype field is projected
- **THEN** selected variant metadata is returned
- **AND** probability blocks are not decompressed.

#### Scenario: Sparse selected samples
- **WHEN** only a subset of samples is selected
- **THEN** block decompression may still cover the complete compressed block
- **BUT** bit unpacking and Arrow construction are limited to selected sample
  state ranges where the layout permits.

#### Scenario: Empty sample selection
- **WHEN** the explicit sample set is empty
- **THEN** probability payload decompression is skipped.

### Requirement: Independent BGEN Block Partitioning

The system SHALL partition selected independent variant blocks by estimated
compressed/decompressed work and coalesce only nearby bounded byte ranges.

#### Scenario: Indexed sparse blocks
- **WHEN** BGI selects independent sparse blocks and target partitions exceed
  one
- **THEN** blocks are assigned to no more than target partitions
- **AND** each selected variant is emitted once.

#### Scenario: Coalescing preserves requested parallelism
- **WHEN** consecutive selected payloads are separated only by the following
  variant's metadata and target partitions exceed one
- **THEN** the metadata gaps are bridged so a sequential scan does not issue one
  object read per variant
- **AND** coalesced ranges stay small enough to fill the requested partitions.

#### Scenario: Decompression isolation
- **WHEN** one selected BGEN block is malformed
- **THEN** its partition fails with variant context
- **AND** the decoder does not use bytes from a neighboring block to mask the
  error.

### Requirement: BGEN Integrity And Conformance

The system SHALL enforce bit-width, ploidy, allele-count, probability-count,
decompressed-size, and block-boundary invariants and agree with independent
BGEN readers within source quantization tolerance.

#### Scenario: Invalid probability dimensions
- **WHEN** the declared probability bytes cannot contain the required states
  for sample ploidies and allele count
- **THEN** decoding fails before reading beyond the block.

#### Scenario: Decompression size limit
- **WHEN** a block declares or expands beyond the configured hard limit
- **THEN** decompression is aborted with a resource-limit error.

#### Scenario: Cross-tool probability fixture
- **WHEN** a supported file is read by this provider and bgenix/qctool or an
  independent BGEN library
- **THEN** variant metadata, ploidy, missingness, phase, and probabilities agree
  within documented quantization tolerance.

### Requirement: Read-Only BGEN Scope

The system SHALL expose BGEN reading without requiring BGEN writing, BGI
creation, probability requantization, or sample-file generation.

#### Scenario: Provider registration
- **WHEN** BGEN support is enabled
- **THEN** supported BGEN can be registered as a DataFusion table
- **AND** absent BGI does not trigger implicit persistent index creation.
