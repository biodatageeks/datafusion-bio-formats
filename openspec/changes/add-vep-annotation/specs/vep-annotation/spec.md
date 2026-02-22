# VEP Annotation Specification

## ADDED Requirements

### Requirement: Variant Lookup Against Cached Known Variants
The system SHALL provide a `lookup_variants()` table function that matches input variants against a cached known variant table, returning annotation columns such as rs-ID, clinical significance, and population frequencies.

#### Scenario: Lookup SNV variants
- **WHEN** a user executes `SELECT * FROM lookup_variants('my_vcf', 'vep_variation')`
- **THEN** the system performs an interval join between the VCF table and the variation cache table
- **AND** returns matching known variant annotations (variation_name, allele_string, clin_sig) for each input variant
- **AND** allele matching ensures the ALT allele matches an allele in the cache's allele_string field

#### Scenario: Lookup indel variants with range matching
- **WHEN** a VCF contains an indel spanning positions 1000-1005
- **THEN** the system uses interval overlap (not position equality) to find matching cache entries
- **AND** correctly matches deletions and insertions that span multiple positions

#### Scenario: Lookup with custom column selection
- **WHEN** a user executes `SELECT * FROM lookup_variants('my_vcf', 'vep_variation', 'variation_name,gnomADg_AF')`
- **THEN** the system applies projection pushdown to the variation cache table scan
- **AND** only reads the requested columns from disk (reducing I/O from 30 GB to ~8 GB for this example)
- **AND** returns only the specified columns alongside the VCF identity columns

#### Scenario: Lookup with default columns
- **WHEN** a user executes `SELECT * FROM lookup_variants('my_vcf', 'vep_variation')` without specifying columns
- **THEN** the system returns default core columns: variation_name, allele_string, clin_sig

#### Scenario: Sparse column auto-pruning
- **WHEN** lookup results contain columns that are entirely null across all matched rows
- **THEN** the system automatically removes those columns from the output (default behavior)
- **AND** this behavior can be disabled via a flag parameter

### Requirement: Consequence Prediction With Transcript Overlap
The system SHALL provide an `annotate_variants()` table function that finds overlapping transcripts for input variants and computes functional consequences using Sequence Ontology (SO) terms.

#### Scenario: Annotate coding SNV
- **WHEN** a user executes `SELECT * FROM annotate_variants('my_vcf', 'vep_transcript')`
- **AND** a variant overlaps a coding region of a transcript
- **THEN** the system classifies the variant with the correct SO consequence term (e.g., missense_variant, synonymous_variant, stop_gained)
- **AND** returns the impact level (HIGH, MODERATE, LOW, or MODIFIER)
- **AND** returns gene_symbol, transcript_id, biotype, amino_acids, and codons

#### Scenario: Annotate frameshift variant
- **WHEN** an insertion or deletion causes a reading frame shift in a coding region
- **THEN** the system assigns the consequence `frameshift_variant` with impact HIGH
- **AND** reports the affected exon number and CDS position

#### Scenario: Annotate splice site variant
- **WHEN** a variant falls within 2bp of an exon-intron boundary (splice donor or acceptor)
- **THEN** the system assigns `splice_donor_variant` or `splice_acceptor_variant` with impact HIGH

#### Scenario: Annotate intronic variant
- **WHEN** a variant falls within an intron but outside splice regions
- **THEN** the system assigns `intron_variant` with impact MODIFIER
- **AND** reports the intron number

#### Scenario: Annotate intergenic variant
- **WHEN** a variant does not overlap any transcript
- **THEN** the system assigns `intergenic_variant` with impact MODIFIER

#### Scenario: Chromosome-partitioned parallel execution
- **WHEN** a multi-chromosome VCF is annotated
- **THEN** the system creates one execution partition per chromosome
- **AND** partitions execute in parallel on separate threads
- **AND** each partition builds its own transcript index (COITree) for its chromosome

### Requirement: Column Selection and Projection Pushdown
The system SHALL support user-specified column selection for cache table reads, pushing projections down to the underlying table provider to minimize I/O.

#### Scenario: Projection pushdown into Parquet cache
- **WHEN** `lookup_variants()` is called with a column list argument
- **THEN** only the specified columns are read from the Parquet file
- **AND** DataFusion's Parquet reader skips column chunks that are not requested

#### Scenario: Projection pushdown into transcript cache
- **WHEN** `annotate_variants()` reads the transcript table
- **THEN** the system only reads columns required for consequence prediction (exons, CDS boundaries, sequences)
- **AND** large columns like `cdna_seq` and `peptide_seq` are only parsed when needed for codon analysis

### Requirement: Output Modes
The system SHALL support two output granularities for `annotate_variants()`: most-severe-per-variant and per-transcript.

#### Scenario: Most severe output mode (default)
- **WHEN** `annotate_variants()` runs in most-severe mode (default)
- **THEN** the output contains one row per input variant
- **AND** shows the single most severe consequence across all overlapping transcripts
- **AND** no fan-out occurs (output row count equals input row count)

#### Scenario: Per-transcript output mode
- **WHEN** `annotate_variants()` runs in per-transcript mode
- **THEN** the output contains one row per (variant, transcript) pair
- **AND** each row shows the consequence for that specific transcript
- **AND** output row count is typically 3-5x the input row count

### Requirement: Coordinate System Support
The system SHALL handle mixed coordinate systems between input variants and cache tables, normalizing coordinates before interval operations.

#### Scenario: VCF (1-based) against 1-based cache
- **WHEN** a 1-based VCF is annotated against a 1-based Ensembl cache
- **THEN** no coordinate conversion is applied
- **AND** interval operations use `FilterOp::Weak` for inclusive boundaries

#### Scenario: BAM-derived (0-based) against 1-based cache
- **WHEN** a 0-based half-open input table is annotated against a 1-based closed cache
- **THEN** the system reads `bio.coordinate_system_zero_based` from both tables' schema metadata
- **AND** converts input coordinates to the cache coordinate system before interval matching
- **AND** uses `FilterOp::Strict` to account for half-open boundary semantics

#### Scenario: Output preserves input coordinate system
- **WHEN** annotation completes
- **THEN** the output table uses the input VCF's original coordinate system
- **AND** the `bio.coordinate_system_zero_based` metadata key is set to match the input

### Requirement: Pluggable Cache Format
The system SHALL consume cache data through DataFusion `TableProvider` instances, enabling any storage format that exposes the required schema contract.

#### Scenario: Annotation with Parquet cache
- **WHEN** the user registers a Parquet file as the transcript or variation cache table
- **THEN** `annotate_variants()` and `lookup_variants()` work without code changes
- **AND** Parquet-specific optimizations (row group pruning, column projection) are applied automatically

#### Scenario: Annotation with native Ensembl cache
- **WHEN** the user registers an Ensembl cache `TranscriptTableProvider` as the transcript table
- **THEN** `annotate_variants()` works identically to the Parquet case
- **AND** projection pushdown is applied to skip parsing unrequested fields

#### Scenario: Schema contract validation
- **WHEN** a cache table is registered that does not expose required columns
- **THEN** the system returns a clear error identifying which required columns are missing
- **AND** the error includes the expected column names and types

### Requirement: Allele Conversion UDFs
The system SHALL provide scalar UDFs for converting between VCF and VEP allele representations and for matching alleles against cache entries.

#### Scenario: Convert VCF SNV to VEP allele
- **WHEN** `vep_allele('A', 'G')` is called
- **THEN** the function returns `'A/G'`

#### Scenario: Convert VCF deletion to VEP allele
- **WHEN** `vep_allele('ACGT', 'A')` is called
- **THEN** the function returns `'CGT/-'` (VEP deletion notation)

#### Scenario: Convert VCF insertion to VEP allele
- **WHEN** `vep_allele('A', 'ACGT')` is called
- **THEN** the function returns `'-/CGT'` (VEP insertion notation)

#### Scenario: Match allele against cache
- **WHEN** `match_allele('G', 'A/G/T')` is called
- **THEN** the function returns `true` because `G` is present in the allele string
- **AND** `match_allele('C', 'A/G/T')` returns `false`

### Requirement: Transcript Model Structured Columns
The system SHALL provide structured columns in the Ensembl cache transcript table for exon arrays, coding sequences, and annotation metadata required by the consequence engine.

#### Scenario: Query exon structure
- **WHEN** a user executes `SELECT stable_id, exons FROM vep_transcript LIMIT 10`
- **THEN** the `exons` column contains a `List<Struct<start:Int64, end:Int64, phase:Int8>>` with genomic coordinates and reading frame phase for each exon

#### Scenario: Query coding sequences
- **WHEN** a user executes `SELECT stable_id, cdna_seq, peptide_seq FROM vep_transcript WHERE is_canonical = true`
- **THEN** `cdna_seq` contains the translateable cDNA sequence (nullable for non-coding transcripts)
- **AND** `peptide_seq` contains the protein sequence (nullable for non-coding transcripts)

#### Scenario: Projection pushdown skips sequence parsing
- **WHEN** a query does not select `cdna_seq` or `peptide_seq`
- **THEN** the parser skips extraction of `_variation_effect_feature_cache.translateable_seq` and `peptide`
- **AND** parsing throughput increases due to reduced per-record processing
