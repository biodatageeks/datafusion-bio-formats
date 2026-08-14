## ADDED Requirements

### Requirement: BCF 2.2 Input

The system SHALL read BCF 2.2 as a binary representation of VCF through the
existing VCF provider architecture and SHALL reject unsupported BCF major or
minor versions before record decoding.

#### Scenario: Valid BCF 2.2 header
- **WHEN** the BCF magic and version identify BCF 2.2
- **THEN** the provider parses the embedded VCF header
- **AND** makes the source available for scanning.

#### Scenario: Unsupported BCF version
- **WHEN** the BCF version is not supported
- **THEN** planning fails with an unsupported-version error
- **AND** the reported version is included in the error.

#### Scenario: Text VCF input
- **WHEN** an existing VCF or BGZF-VCF source is opened
- **THEN** its existing behavior and public schema remain unchanged.

### Requirement: VCF Logical Schema Compatibility

The system SHALL map a BCF header and records to the same Arrow schema that the
existing VCF provider produces for an equivalent header and records.

#### Scenario: Equivalent VCF and BCF
- **WHEN** logically equivalent VCF and BCF fixtures are scanned with identical
  read options
- **THEN** their field names, Arrow data types, nullability, and metadata match
- **AND** their normalized rows are equal.

#### Scenario: Multi-sample BCF
- **WHEN** multiple BCF samples are selected
- **THEN** FORMAT values are emitted through the existing multi-sample
  `genotypes` struct representation
- **AND** selected sample order is recorded in metadata.

#### Scenario: Single-sample compatibility
- **WHEN** BCF is opened in the existing single-sample schema mode
- **THEN** the current top-level FORMAT-field behavior is preserved.

### Requirement: Typed Header Dictionary Resolution

The system SHALL resolve BCF contig, FILTER, INFO, FORMAT, and allele references
through the embedded header dictionaries and validate every dictionary index.

#### Scenario: Valid dictionary reference
- **WHEN** a record references a valid header dictionary index
- **THEN** the provider emits the original case-sensitive header name.

#### Scenario: Invalid dictionary reference
- **WHEN** a record references an absent dictionary entry
- **THEN** the scan fails with a corruption error containing record context
- **AND** it does not invent a name or silently discard the field.

### Requirement: Lossless BCF Value Semantics

The system SHALL preserve BCF scalar/vector types, missing values, vector-end
sentinels, genotype allele indices, ploidy, and phase according to the BCF 2.2
specification and the existing VCF Arrow type mapping.

#### Scenario: Shorter vector with vector-end
- **WHEN** a typed BCF vector ends before its encoded storage width
- **THEN** values after the vector-end sentinel are not emitted
- **AND** the sentinel is not exposed as a biological value.

#### Scenario: Missing vector element
- **WHEN** a vector element is the type-specific missing sentinel
- **THEN** that element is represented as null
- **AND** later non-vector-end elements remain addressable.

#### Scenario: Mixed ploidy and phase
- **WHEN** selected genotypes have different ploidy or phase separators
- **THEN** every genotype retains its allele count and phase information.

#### Scenario: Multiallelic genotype
- **WHEN** a genotype refers to an alternate allele beyond the first
- **THEN** its allele index is preserved relative to the BCF allele list.

#### Scenario: Number=G without GT
- **WHEN** a record contains a `Number=G` FORMAT field but no `GT` field
- **THEN** cardinality validation assumes diploidy as required by the VCF
  specification.

#### Scenario: Number=P without GT
- **WHEN** a record contains a `Number=P` FORMAT field but no `GT` field
- **THEN** decoding fails with an error identifying that `GT` is required.

### Requirement: Streaming BGZF BCF Decode

The system SHALL decode BCF records incrementally from BGZF data using
partition-local reusable buffers and SHALL NOT inflate the complete file into
memory.

#### Scenario: Large sequential BCF
- **WHEN** an unindexed BCF exceeds the configured output batch size
- **THEN** records are streamed into multiple bounded RecordBatches
- **AND** memory does not scale with complete compressed file size.

#### Scenario: Repeated record decoding
- **WHEN** a partition decodes consecutive records
- **THEN** record and temporary value buffers are reused where safe
- **AND** values retained by an emitted Arrow batch are not mutated.

### Requirement: Explicit BCF Genotype Output Mode

The system SHALL preserve VCF-style GT strings by default and SHALL provide an
explicit BCF dosage mode that emits nullable signed 8-bit counts of the first
ALT allele for biallelic records.

#### Scenario: Default string compatibility
- **WHEN** no genotype output mode is selected
- **THEN** GT uses the existing VCF-compatible string schema and values.

#### Scenario: Biallelic dosage
- **WHEN** dosage mode is selected for a biallelic record with called GT alleles
- **THEN** each selected sample receives the count of allele index 1
- **AND** phased and unphased representations with the same alleles produce the
  same dosage.

#### Scenario: Missing dosage
- **WHEN** any GT allele for a selected sample is missing
- **THEN** that sample dosage is null.

#### Scenario: Multiallelic dosage rejection
- **WHEN** dosage mode encounters a selected record with more than one ALT
  allele
- **THEN** the scan fails with an unsupported-dosage error
- **AND** does not collapse distinct alternate alleles.

#### Scenario: Identifier-filtered dosage
- **WHEN** dosage mode scans candidate records containing an unrelated
  multiallelic record
- **AND** an identifier predicate selects only a biallelic record
- **THEN** identifier evaluation removes the unrelated record before dosage
  compatibility validation
- **AND** the selected biallelic record is emitted.

#### Scenario: Scalar-filtered dosage
- **WHEN** a pushable scalar core-column or INFO predicate selects a biallelic
  record from candidates that also contain an unrelated multiallelic record
- **THEN** the BCF decoder evaluates every admitted scalar predicate, including
  SQL null behavior, before dosage compatibility validation
- **AND** sequential and CSI-indexed scans emit the same selected record.

#### Scenario: Unsupported dosage ploidy
- **WHEN** a selected genotype dosage exceeds the signed 8-bit output range
- **THEN** the scan fails with an unsupported-dosage error
- **AND** the caller can use default string mode to preserve the genotype.

#### Scenario: Shared dosage metadata
- **WHEN** dosage mode is selected
- **THEN** schema metadata records the output mode and counted allele under the
  shared format-neutral genotype metadata keys
- **AND** existing VCF-specific aliases remain available for compatibility.

### Requirement: Direct Typed BCF FORMAT Decode

The system SHALL scan each BCF FORMAT series into a validated borrowed payload
view and SHALL allow projected typed sinks to materialize values without an
intermediate per-sample string or dynamically boxed value representation.

#### Scenario: Direct GT dosage projection
- **WHEN** only GT is requested in dosage mode
- **THEN** the provider validates and writes dosage directly from the encoded GT
  payload into bounded Arrow batches
- **AND** does not construct VCF GT strings.

#### Scenario: Unprojected FORMAT series
- **WHEN** a record contains FORMAT children that are not requested
- **THEN** their required integrity checks still run
- **AND** no Arrow values or per-sample decoded objects are constructed for
  those children.

#### Scenario: Cohort-scale FORMAT validation
- **WHEN** FORMAT cardinality is validated across many source samples
- **THEN** samples are checked incrementally without a sample-count-sized
  temporary collection or per-sample diagnostic allocation
- **AND** `Number=G` and `Number=P` payload descriptors retained from the first
  pass are validated without reparsing the complete FORMAT byte slice.

#### Scenario: Unsupported direct decoder
- **WHEN** a requested representation has no direct typed sink
- **THEN** the existing conformant decoder remains available
- **AND** output semantics do not depend on whether the direct path is used.

### Requirement: BCF Projection And Sample Pushdown

The system SHALL apply INFO, FORMAT, and selected-sample projection before
Arrow value construction.

#### Scenario: INFO-only projection
- **WHEN** a query projects variant columns and selected INFO fields but no
  FORMAT fields
- **THEN** the provider does not build FORMAT Arrow arrays.

#### Scenario: FORMAT child subset
- **WHEN** a query requests `GT` but not other FORMAT children
- **THEN** unrequested FORMAT children are not converted into Arrow values.

#### Scenario: Sample subset
- **WHEN** a query selects a subset of header samples
- **THEN** only those sample values are appended to output builders
- **AND** unselected sample values are skipped using the BCF field layout.

#### Scenario: Metadata-only BCF
- **WHEN** `genotypes` and FORMAT fields are unprojected
- **THEN** BCF record bytes needed for projected variant metadata may be read
- **AND** no genotype Arrow arrays are constructed.

### Requirement: CSI Indexed BCF Pruning

The system SHALL discover an explicit or conventional CSI index and use it to
prune BCF BGZF ranges for supported genomic predicates.

#### Scenario: Indexed region query
- **WHEN** a supported region predicate is issued against BCF with CSI
- **THEN** only CSI candidate chunks are scheduled
- **AND** pushdown is reported as `Inexact`.

#### Scenario: Candidate boundary record
- **WHEN** a CSI chunk contains a record outside the requested interval
- **THEN** record-level coordinate evaluation removes it before output.

#### Scenario: Overlapping CSI chunks
- **WHEN** multiple requested regions return overlapping virtual-offset chunks
- **THEN** chunks are merged or de-duplicated
- **AND** each matching BCF record is emitted once.

#### Scenario: Unsatisfiable indexed predicate
- **WHEN** genomic filter analysis proves no region can match
- **THEN** no BCF or CSI data chunks are read.

### Requirement: BCF Indexed Partitioning

The system SHALL group selected CSI chunks by estimated compressed bytes into
no more than the DataFusion target partition count.

#### Scenario: Multiple indexed chunks
- **WHEN** selected CSI chunks can be read independently
- **AND** target partitions exceed one
- **THEN** the execution plan exposes multiple byte-balanced partitions.

#### Scenario: Range-specific byte estimates
- **WHEN** a bounded region intersects only a subset of populated CSI bins
- **THEN** partition byte estimates use chunks from the intersecting bins rather
  than the whole contig.

#### Scenario: Indexed parallel order
- **WHEN** more than one BCF partition executes
- **THEN** matching records are complete and unique
- **AND** global file order is not promised.

### Requirement: Unindexed BCF Fallback

The system SHALL scan valid BCF without CSI sequentially in one physical
partition and apply supported record-level filters.

#### Scenario: Region filter without CSI
- **WHEN** a BCF has no CSI
- **AND** a region predicate is supplied
- **THEN** the provider scans records sequentially
- **AND** emits only records satisfying the predicate.

#### Scenario: Parallel target without CSI
- **WHEN** target partitions exceed one but the BCF has no safe split index
- **THEN** the effective scan partition count is one.

### Requirement: BCF Object-Store Range Access

The system SHALL support BCF and CSI through the same local and OpenDAL-backed
storage schemes as the existing VCF provider.

#### Scenario: Remote indexed BCF
- **WHEN** BCF and CSI are stored in a supported object store
- **THEN** header and selected BGZF chunks are read through bounded ranges
- **AND** an individual CSI-selected BCF span is streamed through hard-capped
  sequential reads rather than materialized as one buffer
- **AND** the complete BCF object is not downloaded for a sparse region query.

#### Scenario: Explicit remote CSI
- **WHEN** a caller supplies an explicit CSI location
- **THEN** that location takes precedence over conventional discovery.

#### Scenario: Bounded CSI companion
- **WHEN** a local or remote CSI companion exceeds the configured safety limit
- **THEN** the provider rejects it before buffering beyond that limit
- **AND** a remote CSI is consumed incrementally without requiring a metadata
  request before the object body.

### Requirement: BCF Integrity And Conformance

The system SHALL reject truncated records, impossible typed lengths, invalid
dictionary indices, invalid genotype encodings, and decompressed sizes beyond
configured limits with contextual errors.

#### Scenario: Truncated record
- **WHEN** a BCF record body ends before its declared shared or individual
  section length
- **THEN** the stream terminates with a record-offset corruption error.

#### Scenario: Cross-tool conformance
- **WHEN** supported fixtures are read by this provider and an independent BCF
  implementation
- **THEN** normalized header, variant, INFO, FORMAT, missingness, and phase
  values agree.

### Requirement: Read-Only BCF Scope

The system SHALL expose BCF reading and querying without promising BCF writing,
index construction, or in-place mutation in this capability.

#### Scenario: Provider registration
- **WHEN** BCF support is enabled
- **THEN** users can register and query BCF as a table
- **AND** no writer API is required.

### Requirement: BCF Dosage Performance Gate

The system SHALL validate BCF dosage performance with fresh one-thread
processes, an optimized release build with native CPU tuning, equivalent output
cells, and both wall-time and peak-memory measurements.

#### Scenario: Independent one-thread comparison
- **WHEN** the representative cohort is decoded to biallelic hard-call dosage
  by this provider and the pinned snputils baseline in at least three
  interleaved runs
- **THEN** every normalized dosage cell and output row matches
- **AND** this provider's median wall time is lower before the performance task
  is marked complete
- **AND** median peak RSS is reported for both implementations.
