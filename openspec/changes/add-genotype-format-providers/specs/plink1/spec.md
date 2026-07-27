## ADDED Requirements

### Requirement: PLINK 1 Fileset Resolution

The system SHALL read a PLINK 1 binary genotype fileset consisting of BED, BIM,
and FAM objects resolved through explicit locations or conventional shared
basenames.

#### Scenario: Conventional fileset
- **WHEN** a caller opens `cohort.bed` without explicit companions
- **THEN** the provider resolves `cohort.bim` and `cohort.fam` in the same
  storage namespace.

#### Scenario: Explicit fileset
- **WHEN** explicit BIM or FAM locations are supplied
- **THEN** they take precedence over basename discovery.

#### Scenario: Missing required member
- **WHEN** BIM or FAM cannot be resolved
- **THEN** planning fails before BED decoding
- **AND** identifies the missing fileset role.

### Requirement: Current Variant-Major BED Validation

The system SHALL support current variant-major BED with leading bytes
`0x6c 0x1b 0x01` and SHALL reject legacy or sample-major encodings.

#### Scenario: Valid variant-major BED
- **WHEN** the BED magic and mode are `0x6c 0x1b 0x01`
- **THEN** each BIM row is mapped to one fixed-width BED variant payload.

#### Scenario: Sample-major BED
- **WHEN** the BED mode byte identifies sample-major storage
- **THEN** planning fails with an unsupported-layout error
- **AND** advises conversion to variant-major BED.

#### Scenario: Invalid or legacy magic
- **WHEN** the BED prefix is absent or identifies a legacy layout
- **THEN** the provider rejects the file
- **AND** does not guess its encoding.

### Requirement: PLINK 1 Fileset Count Integrity

The system SHALL derive sample count from FAM and variant count from BIM and
validate exact BED length as
`3 + variant_count * ceil(sample_count / 4)` using checked arithmetic.

#### Scenario: Consistent fileset length
- **WHEN** BED length equals the checked expected length
- **THEN** the fileset is eligible for scanning.

#### Scenario: Truncated BED
- **WHEN** BED is shorter than the expected length
- **THEN** planning fails with expected and observed byte counts.

#### Scenario: Trailing BED bytes
- **WHEN** BED is longer than the expected current-format length
- **THEN** planning fails rather than ignoring trailing data.

#### Scenario: Count arithmetic overflow
- **WHEN** declared companion row counts overflow offset arithmetic or configured
  limits
- **THEN** planning fails before allocating buffers or issuing BED ranges.

### Requirement: BIM Variant Metadata Schema

The system SHALL expose BIM chromosome, variant ID, centimorgan position,
one-based base-pair position, `A1`, and `A2` as native metadata columns.

#### Scenario: BIM row
- **WHEN** a valid six-field BIM row is parsed
- **THEN** `chrom`, `id`, `cm`, `start`, `end`, `a1`, and `a2` are available
- **AND** site coordinate conversion follows the shared coordinate contract.

#### Scenario: PLINK allele naming
- **WHEN** PLINK 1 metadata is projected
- **THEN** alleles remain named `a1` and `a2`
- **AND** neither is asserted to be biological reference or alternate.

#### Scenario: Malformed BIM row
- **WHEN** a BIM row lacks required fields or has an invalid finite position
- **THEN** planning fails with the BIM row number and offending field.

### Requirement: FAM Sample Identity

The system SHALL preserve FAM family and individual identifiers and SHALL use a
documented sample-name mode for selection and Arrow metadata.

#### Scenario: Default unique IID mode
- **WHEN** `sample_id_mode = iid` or no mode is supplied
- **AND** all FAM individual IDs are unique
- **THEN** individual IDs are used as selected sample names.

#### Scenario: Duplicate IID in default mode
- **WHEN** FAM contains duplicate individual IDs
- **AND** `sample_id_mode = iid`
- **THEN** planning fails with an ambiguous-sample error
- **AND** recommends the family-and-individual mode.

#### Scenario: Family-and-individual mode
- **WHEN** `sample_id_mode = fid_iid`
- **THEN** the provider uses a documented collision-free escaped representation
  of the `(FID, IID)` pair
- **AND** retains the original pair in PLINK-specific sample metadata.

### Requirement: PLINK 1 Genotype Decode

The system SHALL decode BED two-bit calls to nullable A1 dosage in a
`GT: List<UInt8>` child with `00 -> 2`, `10 -> 1`, `11 -> 0`, and `01 -> null`.

#### Scenario: Homozygous A1
- **WHEN** a packed call has code `00`
- **THEN** `GT` contains `2`.

#### Scenario: Heterozygous
- **WHEN** a packed call has code `10`
- **THEN** `GT` contains `1`.

#### Scenario: Homozygous A2
- **WHEN** a packed call has code `11`
- **THEN** `GT` contains `0`.

#### Scenario: Missing call
- **WHEN** a packed call has code `01`
- **THEN** the selected sample value is null.

#### Scenario: Counted allele metadata
- **WHEN** the PLINK `GT` field is emitted
- **THEN** its Arrow metadata states that values count `A1` copies.

### Requirement: BED Padding Validation

The system SHALL validate that unused high-order two-bit slots in the final byte
of each variant are zero when sample count is not divisible by four.

#### Scenario: Zero padding
- **WHEN** unused final-byte slots are zero
- **THEN** they are ignored and do not create samples.

#### Scenario: Non-zero padding
- **WHEN** an unused final-byte slot is non-zero
- **THEN** the scan fails with a variant-index corruption error.

### Requirement: PLINK 1 Sample Decode Pushdown

The system SHALL extract only selected sample codes from each packed variant
and SHALL NOT allocate a full-sample genotype vector as an intermediate.

#### Scenario: Sparse sample selection
- **WHEN** a small subset of FAM samples is requested
- **THEN** only their two-bit positions are appended to `GT`
- **AND** metrics report skipped sample calls.

#### Scenario: Reordered samples
- **WHEN** requested sample order differs from FAM order
- **THEN** decoded `GT` values follow request order.

### Requirement: Exact BIM Predicate Pushdown

The system SHALL evaluate supported chromosome, coordinate, and variant-ID
predicates exactly against BIM before BED range planning.

#### Scenario: Sparse ID selection
- **WHEN** a query filters to a set of BIM variant IDs
- **THEN** only matching variant indices are scheduled for BED reads
- **AND** pushdown is reported as `Exact`.

#### Scenario: Genomic interval selection
- **WHEN** a supported chromosome and position interval is supplied
- **THEN** BIM rows are evaluated using requested coordinate semantics
- **AND** only exact matches are emitted.

#### Scenario: Exact filtered limit
- **WHEN** an exact BIM predicate and limit are supplied
- **THEN** the provider may restrict BED planning to the first matching BIM
  variants needed for the limit.

### Requirement: PLINK 1 Metadata-Only Execution

The system SHALL answer BIM-only projections, exact counts, and empty-sample
scans without reading BED genotype payload bytes.

#### Scenario: Variant catalog query
- **WHEN** a query projects only BIM-derived columns
- **THEN** BED genotype payload bytes read is zero
- **AND** minimal BED header or object metadata may still be read for fileset
  validation.

#### Scenario: Unfiltered row count
- **WHEN** an exact row count is requested without a variant predicate
- **THEN** the provider uses the validated BIM count
- **AND** does not scan BED.

### Requirement: Fixed-Offset BED Range Planning

The system SHALL compute each selected variant offset from its BIM row index and
fixed bytes-per-variant value and SHALL group adjacent variants into bounded
ranges.

#### Scenario: Contiguous variants
- **WHEN** selected BIM indices are contiguous
- **THEN** their BED payloads may be read as one bounded range.

#### Scenario: Distant variants
- **WHEN** selected BIM indices are sparse beyond the coalescing threshold
- **THEN** the provider issues separate ranges
- **AND** does not read every intervening variant.

#### Scenario: Parallel variant ranges
- **WHEN** selected ranges and target partitions permit parallelism
- **THEN** byte-balanced ranges are assigned to no more than target partitions
- **AND** each selected variant is owned by one partition.

### Requirement: PLINK 1 Object-Store Support

The system SHALL read BED/BIM/FAM from supported local and OpenDAL-backed
storage and SHALL keep all three objects associated with the same validated
fileset.

#### Scenario: Remote sparse genotype query
- **WHEN** BED/BIM/FAM are remote and exact BIM pruning selects sparse variants
- **THEN** the provider range-reads only selected BED payload groups
- **AND** does not download the entire BED object.

### Requirement: PLINK 1 Conformance

The system SHALL produce calls, sample order, allele metadata, and variant
metadata consistent with the PLINK 1 binary specification and an independent
reader for supported files.

#### Scenario: Differential fixture
- **WHEN** a supported fileset is read by this provider and PLINK or
  `bed-reader`
- **THEN** normalized A1 dosages and missingness agree for every sample/variant.

#### Scenario: Invalid FAM or BIM text
- **WHEN** a companion contains invalid encoding, fields, counts, or positions
- **THEN** the provider returns a contextual companion and line-number error.

### Requirement: Read-Only PLINK 1 Scope

The system SHALL expose PLINK 1 fileset reading without requiring BED writing,
fileset transposition, or fileset conversion.

#### Scenario: Provider registration
- **WHEN** PLINK 1 support is enabled
- **THEN** a valid fileset can be registered as a DataFusion table
- **AND** unsupported sample-major input is not automatically rewritten.
