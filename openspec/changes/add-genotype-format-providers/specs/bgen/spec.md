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

Validation SHALL be split by what it costs. Checks that do not require reading
the object's variants — declared size, identifying prefix, row count against the
header's variant count, and row ranges tiling the variant region in order — SHALL
run when the index is opened. The comparison of each row's contents against the
record it points at SHALL run when a scan reads that record, so that opening a
table never walks the object.

#### Scenario: Local BGI
- **WHEN** a valid local BGI is resolved
- **THEN** it is queried directly without copying the complete BGEN file.

#### Scenario: Opening an indexed BGEN
- **WHEN** a BGEN with a valid BGI is opened
- **THEN** the object is read only for its header and the bytes the index's
  identity check covers
- **AND** no variant record or probability payload is read, because the index
  already holds every variant's location, chromosome, position, RS identifier
  and allele count.

#### Scenario: Remote BGI
- **WHEN** a valid BGI is remote
- **THEN** it is downloaded atomically to the shared bounded cache
- **AND** cache reuse is keyed by remote object identity.

#### Scenario: Stale or inconsistent BGI
- **WHEN** BGI object identity, row count, or row ranges are inconsistent with
  BGEN
- **THEN** planning fails or ignores an optional stale index according to an
  explicit documented policy
- **AND** it never reads an out-of-bounds block.

#### Scenario: Index describing different variants
- **WHEN** an index identifies the selected object but a row's chromosome,
  position, RS identifier, allele count, leading alleles, or record size differs
  from the record it points at
- **THEN** the scan reading that record fails and names the mismatched field
- **AND** it never emits the index's values in place of the object's.

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
- **AND** does not claim that the standard BGI schema contains that identifier
- **AND** parsing reads the candidates' variant metadata only, leaving their
  probability payloads unread.

#### Scenario: Projecting a field the index does not record
- **WHEN** a projection selects the variant identifier, or the alleles of a
  variant declaring more than two, and no probability payload is read
- **THEN** the provider reads those variants' record metadata to supply them
- **AND** a scan that reads payloads takes them from the records it already
  fetched, without an additional request.

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

### Requirement: BGEN Probability Output Layout

The system SHALL emit probability states as a variable-length list per sample by
default, and SHALL offer a fixed-width layout whose width covers the widest
sample the file's catalog allows.

#### Scenario: Default variable-length layout
- **WHEN** probability output is requested without selecting a layout
- **THEN** each sample's states are emitted as a variable-length list
- **AND** variants storing different numbers of states are all representable.

#### Scenario: Fixed-width layout
- **WHEN** the fixed probability layout is selected
- **THEN** the emitted schema declares the state count per sample
- **AND** no per-sample list offsets are emitted
- **AND** a sample with no called genotype is null while still occupying its
  declared width.

#### Scenario: Fixed-width layout derives its width from the catalog
- **WHEN** the fixed probability layout is selected
- **THEN** the width is the widest state count implied by the first variant's
  declared ploidy and phasing together with every catalog variant's allele count
- **AND** no probability payload beyond the first variant is read to determine
  it.

#### Scenario: Fixed-width layout pads a narrower sample
- **WHEN** the fixed probability layout is selected and a sample stores fewer
  states than the declared width
- **THEN** its remaining states are emitted as NaN
- **AND** the sample's own states are unchanged
- **AND** a variant declaring a variable ploidy is representable, because
  padding is decided per sample rather than per variant.

#### Scenario: A missing sample's reserved states read as NaN
- **WHEN** the fixed probability layout is selected and a sample has no called
  genotype
- **THEN** the sample is null
- **AND** the states it reserves are NaN rather than zero, so a consumer reading
  the values buffer directly does not observe a probability of zero where there
  is no genotype.

#### Scenario: Fixed-width layout rejects a wider sample
- **WHEN** the fixed probability layout is selected and a sample stores more
  states than the declared width
- **THEN** the scan fails naming both counts and directs the caller to the
  default layout
- **AND** the values are neither padded nor truncated to fit.

#### Scenario: A derived width honours the per-sample state limit
- **WHEN** the fixed probability layout is selected and the width the catalog
  implies exceeds the configured maximum states per sample
- **THEN** planning fails naming both counts and directs the caller to the
  default layout, because every emitted sample is padded to that width whether
  or not the widest variant is selected by a filter.

#### Scenario: Fixed-width layout needs a determinable width
- **WHEN** the fixed probability layout is selected and the first variant
  declares a variable ploidy, which has no single state count
- **THEN** planning fails and directs the caller to the default layout.

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

#### Scenario: Coalesced reads are attributed to the right counters
- **WHEN** a coalesced range bridges the metadata between consecutive payloads
- **THEN** every byte fetched is reported as primary bytes read
- **AND** only the genotype payload bytes are reported as compressed bytes, so a
  compression ratio derived from the counters is not skewed by bridged metadata.

#### Scenario: Coalescing leaves partitions comparably loaded
- **WHEN** payload ranges are coalesced for a scan with more than one partition
- **THEN** a coalesced range covers at most a fraction of one partition's byte
  share, so several ranges are available to each partition
- **AND** no partition is assigned materially more than its share of the
  selected payload bytes.

Sizing a coalesced range at exactly one partition's share cannot balance the
scan: a variant's payload is indivisible, so the plan is handed one more chunk
than there are partitions, and one partition always takes two of them.

#### Scenario: Range sizing has a floor that never starves a partition
- **WHEN** the byte share implied by the partition count falls below the minimum
  useful object read
- **THEN** ranges are not split further, so a scan does not issue many reads far
  below a useful size
- **AND** a payload smaller than that minimum is still divided across the
  requested partitions rather than collapsing into one range.

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

#### Scenario: Reconstruction size limit
- **WHEN** the probability states the selected samples of one variant would
  reconstruct need more memory than the configured decompressed-block budget
- **THEN** the variant is rejected before any of that reconstruction is built,
  because low bit precision expands each stored state into a wider output value
  and a block inside the limit can otherwise reconstruct into many times it
- **AND** a per-sample state limit does not substitute for this, since every
  sample may sit under it while their sum does not.

#### Scenario: Metadata limit binds on parsed bytes
- **WHEN** variant metadata exceeds the configured maximum but the read-ahead
  buffer happens to hold it
- **THEN** parsing is still rejected with a resource-limit error, because the
  limit binds on the bytes handed to the parser rather than on the bytes
  fetched.

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
