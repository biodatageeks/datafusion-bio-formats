## ADDED Requirements

### Requirement: Standard PGEN Fileset Resolution

The system SHALL read standard PLINK 2 PGEN/PVAR/PSAM filesets through explicit
locations or documented conventional companion names.

#### Scenario: Conventional fileset
- **WHEN** a caller opens a PGEN without explicit companions
- **THEN** the provider resolves the corresponding PVAR and PSAM in the same
  storage namespace.

#### Scenario: Explicit compressed PVAR
- **WHEN** an explicit supported compressed PVAR location is supplied
- **THEN** it is used and decompressed according to its detected compression.

#### Scenario: Hybrid PLINK 1 companions
- **WHEN** a PGEN is paired only with BIM/FAM
- **THEN** planning fails with an unsupported-hybrid-fileset error
- **AND** does not reinterpret BIM/FAM as PVAR/PSAM.

### Requirement: PGEN Header Mode Support

The system SHALL parse and validate PGEN header modes `0x01`, `0x02`, `0x03`,
`0x04`, `0x10`, `0x11`, `0x20`, and `0x21` according to the pinned PLINK 2
PGEN specification.

#### Scenario: Supported mode
- **WHEN** the PGEN magic and mode identify one of the supported modes
- **THEN** the provider locates counts, flags, record metadata, and genotype
  payload using that mode's layout.

#### Scenario: Unknown mode
- **WHEN** a PGEN declares an unknown or unimplemented mode
- **THEN** planning fails with the hexadecimal mode and specification baseline
  in the error.

#### Scenario: Malformed mode-specific header
- **WHEN** required header fields, control bytes, or offsets are inconsistent
- **THEN** planning fails before genotype record decoding.

### Requirement: PGEN Index Support

The system SHALL use mode-appropriate embedded indexes and supported external
PGEN indexes to locate records and SHALL validate every offset against object
length and record order.

#### Scenario: Embedded index
- **WHEN** the selected PGEN mode includes an embedded index
- **THEN** the provider uses it without requiring a separate index object.

#### Scenario: External index
- **WHEN** the selected mode requires an external index
- **THEN** an explicit or conventional index is resolved and validated.

#### Scenario: Missing required index
- **WHEN** a required external index cannot be resolved
- **THEN** planning fails before record payload I/O.

#### Scenario: Invalid record offset
- **WHEN** an index offset is out of bounds or not monotonic where required
- **THEN** the fileset is rejected with index and variant context.

### Requirement: PVAR Variant Semantics

The system SHALL expose PVAR chromosome, site coordinates, variant ID,
reference allele, and all alternate alleles without reducing multiallelic rows.

#### Scenario: Biallelic PVAR row
- **WHEN** a PVAR row contains one alternate allele
- **THEN** the provider exposes `ref` and a one-element `alt` list.

#### Scenario: Multiallelic PVAR row
- **WHEN** a PVAR row contains multiple alternate alleles
- **THEN** their source order and exact strings are preserved.

#### Scenario: PVAR metadata lines
- **WHEN** PVAR includes supported VCF-style header metadata and optional
  columns
- **THEN** required column interpretation follows that header
- **AND** unsupported optional annotations do not alter allele indices.

#### Scenario: Invalid PVAR position
- **WHEN** a PVAR row has an invalid one-based position or malformed allele list
- **THEN** planning fails with the PVAR line number.

### Requirement: PSAM Sample Identity

The system SHALL derive ordered sample names from the configured PSAM
identifier column or documented default and SHALL validate sample count against
PGEN.

#### Scenario: IID default
- **WHEN** PSAM contains unique `IID` values and no alternate ID column is
  configured
- **THEN** `IID` defines sample names in PSAM order.

#### Scenario: Duplicate selected identifier
- **WHEN** the configured PSAM identifier column contains duplicate values
- **THEN** planning fails with an ambiguous-sample error.

#### Scenario: Sample count mismatch
- **WHEN** PSAM row count differs from the PGEN header sample count
- **THEN** planning fails before genotype decoding.

### Requirement: PGEN Raw Allele Output

The system SHALL provide a raw allele mode with
`GT: List<List<UInt16>>` containing PVAR allele indices and
`PHASED: List<Boolean>` describing whether allele order is phased for each
selected sample.

#### Scenario: Homozygous reference hardcall
- **WHEN** a biallelic hardcall is homozygous reference
- **THEN** `GT` contains allele indices `[0, 0]`.

#### Scenario: Heterozygous unphased hardcall
- **WHEN** a biallelic hardcall is heterozygous without phase
- **THEN** `GT` contains allele indices `0` and `1`
- **AND** `PHASED` is false.

#### Scenario: Phased hardcall
- **WHEN** a record stores phase for a selected heterozygous hardcall
- **THEN** `GT` allele order follows the stored haplotype order
- **AND** `PHASED` is true.

#### Scenario: Missing hardcall
- **WHEN** a selected hardcall is missing
- **THEN** `GT` and `PHASED` are null for that sample.

### Requirement: PGEN Biallelic Hardcall Representations

The system SHALL decode the PGEN biallelic hardcall representations selected by
record type, including dense two-bit, one-bit, difflist, and LD-compressed
forms.

#### Scenario: Dense record
- **WHEN** a record uses the dense two-bit representation
- **THEN** every selected code maps to the same logical hardcall as PLINK 2.

#### Scenario: Sparse difflist record
- **WHEN** a record uses a common genotype plus differences
- **THEN** selected differences override the common genotype exactly once
- **AND** sample indices are bounds-checked.

#### Scenario: One-bit record
- **WHEN** a record uses one-bit values plus an exception list
- **THEN** selected base values and exceptions reconstruct the complete logical
  hardcalls.

### Requirement: PGEN Phase Information

The system SHALL decode phase-present and phase-information tracks only for
eligible heterozygous hardcalls and SHALL reject inconsistent phase bits.

#### Scenario: Partially phased record
- **WHEN** only a subset of heterozygous samples has phase information
- **THEN** `PHASED` is true only for that subset
- **AND** unphased heterozygotes retain correct unordered allele values.

#### Scenario: Phase bit on ineligible call
- **WHEN** phase data addresses a homozygous, missing, or out-of-range sample in
  a way forbidden by the PGEN specification
- **THEN** decoding fails with record and sample context.

### Requirement: PGEN Biallelic Dosage Output

The system SHALL decode stored biallelic dosage and dosage-presence tracks into
`DS: List<Float32>` where values count PVAR alternate allele index one.

#### Scenario: Stored dosage
- **WHEN** a selected sample has a stored dosage value
- **THEN** `DS` contains the specification-scaled alternate dosage
- **AND** counted-allele and scaling metadata are present.

#### Scenario: No stored dosage
- **WHEN** a selected sample has a hardcall but no stored dosage
- **THEN** `DS` is null
- **AND** `GT` remains available when requested.

#### Scenario: Missing genotype and dosage
- **WHEN** both hardcall and dosage are missing
- **THEN** their output values are null.

#### Scenario: Invalid dosage
- **WHEN** a dosage integer or presence track violates bounds or record length
- **THEN** the scan fails rather than clamping the dosage.

### Requirement: PGEN Phased Dosage Output

The system SHALL decode valid phased dosage tracks into
`HDS: List<List<Float32>>` in stored haplotype order and preserve their
relationship to total dosage.

#### Scenario: Complete phased dosage
- **WHEN** both haplotype dosages are stored for a selected sample
- **THEN** `HDS` contains both values in haplotype order
- **AND** their sum agrees with `DS` within source quantization tolerance when
  total dosage is present.

#### Scenario: Phased dosage unavailable
- **WHEN** a selected sample has no phased dosage
- **THEN** its `HDS` value is null.

### Requirement: PGEN Multiallelic Hardcalls

The system SHALL apply multiallelic hardcall patch tracks to reconstruct raw
PVAR allele indices without collapsing alternate alleles.

#### Scenario: Heterozygous alternate patch
- **WHEN** a patch replaces a biallelic placeholder with a higher alternate
  allele
- **THEN** `GT` contains that exact PVAR allele index.

#### Scenario: Two-alternate patch
- **WHEN** both allele positions are patched
- **THEN** both exact allele indices and stored phase order are preserved.

#### Scenario: Invalid patch allele
- **WHEN** a patch references an allele outside the PVAR allele list
- **THEN** decoding fails with variant and sample context.

### Requirement: Explicit Multiallelic Dosage Limitation

The system SHALL reject PGEN multiallelic dosage sections that are not covered
by the pinned stable format specification or implemented support matrix.

#### Scenario: Unsupported multiallelic dosage
- **WHEN** a selected record contains an unsupported multiallelic dosage
  portion
- **THEN** the scan fails with a distinct unsupported-feature error
- **AND** does not reinterpret it as biallelic dosage or hardcall-only data.

### Requirement: LD Dependency-Aware PGEN Partitioning

The system SHALL plan PGEN record blocks of up to `2^16` variants with the
dependency anchors required to decode LD-compressed records independently.

#### Scenario: Partition begins with LD-compressed record
- **WHEN** the first owned record depends on an earlier eligible non-LD record
- **THEN** the partition plan includes that dependency anchor and required
  forward prelude
- **AND** begins output only at its first owned variant.

#### Scenario: Dependency record ownership
- **WHEN** a dependency record is read by multiple partitions
- **THEN** only its owning partition emits it
- **AND** dependency metrics record the duplicate internal read.

#### Scenario: Invalid LD chain
- **WHEN** an LD record has no valid preceding base under the specification
- **THEN** decoding fails with the dependent variant index.

#### Scenario: Parallel block output
- **WHEN** multiple independent record blocks are selected
- **THEN** they execute in no more than target partitions
- **AND** every selected PVAR variant is emitted once.

### Requirement: Exact PVAR Predicate Pushdown

The system SHALL evaluate supported chromosome, coordinate, and variant-ID
predicates exactly against PVAR before PGEN payload planning.

#### Scenario: PVAR region query
- **WHEN** a supported genomic interval is supplied
- **THEN** only exact matching PVAR row indices and required PGEN dependencies
  are scheduled.

#### Scenario: PVAR ID query
- **WHEN** equality or `IN` selects PVAR variant IDs
- **THEN** pushdown is reported as `Exact`.

#### Scenario: Exact PVAR limit
- **WHEN** a safe limit follows only exact PVAR predicates
- **THEN** owned output record planning may stop after the required matches
- **AND** required LD preludes are still included.

### Requirement: PGEN Projection And Sample Pushdown

The system SHALL skip PGEN payload for PVAR-only queries and decode only
projected genotype tracks and selected samples within each supported
representation.

#### Scenario: PVAR-only projection
- **WHEN** no genotype child is projected
- **THEN** no PGEN variant record payload bytes are read
- **AND** minimal PGEN header/index bytes may still be read for fileset
  validation.

#### Scenario: Hardcall-only projection
- **WHEN** `GT` is requested but dosage fields are not
- **THEN** dosage tracks are skipped by their declared lengths.

#### Scenario: Dosage-only projection
- **WHEN** `DS` is requested but phase and phased dosage are not
- **THEN** unrequested phase tracks are not converted into Arrow arrays.

#### Scenario: Sparse sample selection
- **WHEN** a sample subset is selected
- **THEN** dense and sparse record paths append only selected sample values
- **AND** difflist membership tests preserve request order.

### Requirement: PGEN Object-Store Range Access

The system SHALL support standard PGEN filesets on local and OpenDAL-backed
storage using bounded header, index, record, PVAR, and PSAM reads.

#### Scenario: Remote sparse PVAR selection
- **WHEN** exact PVAR pruning selects sparse PGEN records
- **THEN** the provider range-reads selected records and dependency preludes
- **AND** does not download the complete PGEN object.

#### Scenario: Adjacent selected records
- **WHEN** selected records and their dependencies occupy nearby ranges
- **THEN** requests may be coalesced within configured thresholds.

### Requirement: PGEN Integrity And Conformance

The system SHALL bounds-check varints, sample indices, difflists, record lengths,
patches, dosage tracks, indexes, and allocation dimensions and SHALL agree with
PLINK 2 for supported features.

#### Scenario: Truncated variable-length record
- **WHEN** a record ends during a varint, difflist, patch, or dosage track
- **THEN** the stream fails with variant and byte-offset context.

#### Scenario: Differential fixture
- **WHEN** a supported synthetic fileset is read by this provider and PLINK 2
  or external `pgenlib`
- **THEN** hardcalls, phase, allele indices, dosage, phased dosage, missingness,
  and sample order agree within source quantization tolerance.

### Requirement: Independent Read-Only PGEN Scope

The system SHALL provide an independently implemented read-only PGEN decoder
without requiring PGEN writing or linking `pgenlib` into default artifacts.

#### Scenario: Runtime dependency graph
- **WHEN** the PGEN provider is built in a normal workspace build
- **THEN** default binaries do not link or vendor `pgenlib`.

#### Scenario: External conformance oracle
- **WHEN** `pgenlib` is used in optional differential tests
- **THEN** it executes outside the production decoder boundary.
