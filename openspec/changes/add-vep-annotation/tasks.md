# Implementation Tasks

## 1. Phase 1: Lookup Mode (bio-functions, no bio-formats changes needed)

### 1.1 Scaffold `bio-function-vep` Crate
- [x] 1.1.1 Create `datafusion/bio-function-vep/Cargo.toml` with dependencies on `datafusion`, `bio-function-ranges`, `ahash`, `arrow`
- [x] 1.1.2 Create `datafusion/bio-function-vep/src/lib.rs` with `register_vep_functions()` export
- [x] 1.1.3 Add crate to workspace `Cargo.toml`
- [x] 1.1.4 Verify `cargo build --package bio-function-vep` compiles

### 1.2 Implement Allele Conversion (`allele.rs`)
- [x] 1.2.1 Implement `vcf_to_vep_allele(ref, alt) -> (String, String)` for SNV, insertion, deletion
- [x] 1.2.2 Implement `allele_matches(alt, allele_string) -> bool` for matching against cache format
- [x] 1.2.3 Write unit tests covering SNV (`A/G`), insertion (`-/CGT`), deletion (`CGT/-`), MNV, and multi-allelic cases
- [x] 1.2.4 Implement `match_allele()` scalar UDF wrapping `allele_matches`
- [x] 1.2.5 Implement `vep_allele()` scalar UDF wrapping `vcf_to_vep_allele`

### 1.3 Implement Coordinate Normalization (`coordinate.rs`)
- [x] 1.3.1 Create `CoordinateNormalizer` struct reading `bio.coordinate_system_zero_based` from schema metadata
- [ ] 1.3.2 Implement `to_cache_start()` / `to_cache_end()` for coordinate conversion (deferred to Phase 3 — only needed for COITree probing in `annotate_variants()`)
- [x] 1.3.3 Implement `filter_op()` returning `FilterOp::Strict` or `FilterOp::Weak` based on coordinate systems
- [x] 1.3.4 Write unit tests for all combinations: 0-based -> 1-based, 1-based -> 0-based, same system

### 1.4 Implement Schema Contract (`schema_contract.rs`)
- [x] 1.4.1 Define expected column names and types for variation cache table
- [x] 1.4.2 Define default columns (variation_name, allele_string, clin_sig)
- [x] 1.4.3 Implement schema validation function that checks a `TableProvider` exposes required columns
- [x] 1.4.4 Implement column list parsing from user-provided comma-separated string

### 1.5 Implement Lookup Provider (`lookup_provider.rs`)
- [x] 1.5.1 Create `LookupFunction` implementing `TableFunctionImpl`
- [x] 1.5.2 Parse arguments: VCF table name, variation table name, optional column list, optional prune flag
- [x] 1.5.3 Generate internal interval join SQL using `IntervalJoinExec` pattern from `bio-function-ranges`
- [x] 1.5.4 Apply coordinate normalization to the generated SQL based on both tables' schema metadata
- [x] 1.5.5 Add `match_allele()` post-filter for allele matching
- [x] 1.5.6 Implement projection pushdown — only read user-requested columns from cache table
- [ ] 1.5.7 Implement sparse column pruning (auto-drop all-null columns from output)

### 1.6 Register and Test Lookup Mode
- [x] 1.6.1 Register `lookup_variants` UDTF and allele UDFs in `register_vep_functions()`
- [ ] 1.6.2 Create integration test: lookup against Parquet variation cache with SNV variants
- [ ] 1.6.3 Create integration test: lookup with indel variants (range-based matching)
- [ ] 1.6.4 Create integration test: column selection reducing I/O
- [ ] 1.6.5 Create integration test: mixed coordinate systems (0-based VCF input)
- [ ] 1.6.6 Verify `SELECT * FROM lookup_variants('my_vcf', 'vep_variation')` works end-to-end

## 2. Phase 2: Transcript Model (bio-formats)

### 2.1 Extend Transcript Schema
- [x] 2.1.1 Add `exons` column as `List<Struct<start:Int64, end:Int64, phase:Int8>>` to `transcript_schema()`
- [x] 2.1.2 Add `cdna_seq` column as nullable `Utf8`
- [x] 2.1.3 Add `peptide_seq` column as nullable `Utf8`
- [x] 2.1.4 Add `codon_table` column as nullable `Int32` (default 1)
- [x] 2.1.5 Add `tsl` column as nullable `Int32`
- [x] 2.1.6 Add `mane_select` column as nullable `Utf8`
- [x] 2.1.7 Add `mane_plus_clinical` column as nullable `Utf8`

### 2.2 Add `List<Struct>` Builder Support
- [x] 2.2.1 Add `ListBuilder<StructBuilder>` support to `BatchBuilder` in `util.rs`
- [x] 2.2.2 Implement `set_exon_list(idx, &[(i64, i64, i8)])` method
- [x] 2.2.3 Write unit tests for exon list building with empty, single, and multi-exon cases

### 2.3 Update Text Parser
- [x] 2.3.1 Parse `_trans_exon_array` from JSON into `List<Struct<start, end, phase>>` in `parse_transcript_line_into()`
- [x] 2.3.2 Parse `_variation_effect_feature_cache.translateable_seq` into `cdna_seq`
- [x] 2.3.3 Parse `_variation_effect_feature_cache.peptide` into `peptide_seq`
- [x] 2.3.4 Parse `codon_table`, `tsl`, `mane_select`, `mane_plus_clinical` as simple fields
- [x] 2.3.5 Add `exons_projected` flag — skip `_trans_exon_array` parsing when not projected
- [x] 2.3.6 Add `sequences_projected` flag — skip translateable_seq/peptide parsing when not projected

### 2.4 Update Storable Binary Parser
- [x] 2.4.1 Extract exon array from SValue tree in `parse_transcript_storable_file()`
- [x] 2.4.2 Extract cdna_seq and peptide_seq from SValue tree
- [x] 2.4.3 Extract codon_table, tsl, mane_select, mane_plus_clinical from SValue tree
- [x] 2.4.4 Apply same projection pushdown flags as text parser

### 2.5 Test Transcript Model
- [x] 2.5.1 Write test verifying `SELECT exons FROM vep_transcript LIMIT 10` returns structured exon data
- [x] 2.5.2 Write test verifying `SELECT cdna_seq, peptide_seq FROM vep_transcript` returns sequences
- [x] 2.5.3 Write test verifying projection pushdown skips sequence parsing when not selected
- [x] 2.5.4 Write test verifying backward compatibility — existing queries still work with new schema

## 3. Phase 3: Consequence Engine (bio-functions)

### 3.1 Implement Genetic Code Tables (`codon.rs`)
- [ ] 3.1.1 Define standard genetic code table (NCBI table 1)
- [ ] 3.1.2 Define vertebrate mitochondrial code table
- [ ] 3.1.3 Implement `translate_codon(bases, table_id) -> amino_acid`
- [ ] 3.1.4 Implement `is_start_codon()` and `is_stop_codon()`
- [ ] 3.1.5 Write tests for all 64 codons in standard code

### 3.2 Implement SO Consequence Hierarchy (`so_terms.rs`)
- [ ] 3.2.1 Define `Impact` enum: High, Moderate, Low, Modifier
- [ ] 3.2.2 Define `ConsequenceTerm` struct with name, SO ID, impact, severity rank
- [ ] 3.2.3 Create static consequence table covering all terms: transcript_ablation through intergenic_variant
- [ ] 3.2.4 Implement `most_severe(terms) -> ConsequenceTerm` for picking highest-impact consequence
- [ ] 3.2.5 Write tests verifying severity ordering matches Ensembl VEP

### 3.3 Implement Position Classification (`position.rs`)
- [ ] 3.3.1 Implement `genomic_to_cds(pos, transcript) -> Option<CdsPosition>` with forward strand support
- [ ] 3.3.2 Add reverse strand support (iterate exons in reverse, complement bases)
- [ ] 3.3.3 Implement exon/intron boundary detection
- [ ] 3.3.4 Implement UTR detection (5' and 3')
- [ ] 3.3.5 Implement splice region detection (2bp donor/acceptor + 3-8bp region)
- [ ] 3.3.6 Write tests with known transcript structures

### 3.4 Implement Consequence Prediction (`consequence.rs`)
- [ ] 3.4.1 Implement `classify_variant(variant, transcript) -> ConsequenceResult`
- [ ] 3.4.2 Handle SNV in coding region: missense, synonymous, stop_gained, stop_lost, start_lost
- [ ] 3.4.3 Handle insertions: inframe_insertion, frameshift_variant
- [ ] 3.4.4 Handle deletions: inframe_deletion, frameshift_variant
- [ ] 3.4.5 Handle splice site variants: splice_acceptor_variant, splice_donor_variant
- [ ] 3.4.6 Handle UTR variants: 5_prime_UTR_variant, 3_prime_UTR_variant
- [ ] 3.4.7 Handle intronic variants: intron_variant, splice_region_variant
- [ ] 3.4.8 Handle non-coding transcript variants
- [ ] 3.4.9 Handle upstream/downstream gene variants
- [ ] 3.4.10 Handle intergenic variants (no overlapping transcript)
- [ ] 3.4.11 Write unit tests comparing predictions against known VEP output for BRCA1, TP53, and other well-characterized variants

### 3.5 Implement Transcript Index (`transcript_index.rs`)
- [ ] 3.5.1 Create `TranscriptRecord` struct from Arrow columns
- [ ] 3.5.2 Implement `TranscriptIndex::build_from_batches()` building per-chromosome COITree
- [ ] 3.5.3 Implement `TranscriptIndex::query(chrom, pos) -> Vec<&TranscriptRecord>`
- [ ] 3.5.4 Add projection: only read columns needed from transcript table
- [ ] 3.5.5 Write tests for index building and querying with multi-chromosome data

## 4. Phase 4: Annotation Pipeline (bio-functions)

### 4.1 Implement Output Schema (`output_schema.rs`)
- [ ] 4.1.1 Define annotated output schema: original VCF columns + consequence, impact, gene_symbol, transcript_id, biotype, exon_number, intron_number, cdna_position, cds_position, protein_position, amino_acids, codons
- [ ] 4.1.2 Support schema variation for MostSevere vs PerTranscript output modes

### 4.2 Implement AnnotateExec (`annotate_exec.rs`)
- [ ] 4.2.1 Create `AnnotateExec` struct implementing `ExecutionPlan`
- [ ] 4.2.2 Implement partition count = number of chromosomes
- [ ] 4.2.3 Implement `execute(partition, ctx)` that loads transcript index per chromosome
- [ ] 4.2.4 Use `Arc` + `OnceLock` for shared transcript index across partitions

### 4.3 Implement Annotation Stream (`annotate_stream.rs`)
- [ ] 4.3.1 Create per-partition stream that pulls VCF RecordBatches
- [ ] 4.3.2 For each batch, probe transcript tree for all variants
- [ ] 4.3.3 Compute consequences for each (variant, transcript) overlap
- [ ] 4.3.4 Implement MostSevere mode: pick single most severe consequence per variant
- [ ] 4.3.5 Implement PerTranscript mode: emit one row per (variant, transcript) pair
- [ ] 4.3.6 Build output RecordBatch with annotated columns

### 4.4 Implement Table Function (`table_function.rs`)
- [ ] 4.4.1 Create `AnnotateFunction` implementing `TableFunctionImpl`
- [ ] 4.4.2 Parse arguments: VCF table name, transcript table name, optional output mode
- [ ] 4.4.3 Construct `AnnotateExec` plan with VCF scan as input
- [ ] 4.4.4 Register as `annotate_variants` UDTF

### 4.5 Integration Tests
- [ ] 4.5.1 Test `annotate_variants()` with small test VCF + real Ensembl cache
- [ ] 4.5.2 Compare output against `vep --cache` for the same input
- [ ] 4.5.3 Test MostSevere vs PerTranscript output modes
- [ ] 4.5.4 Test combined mode: `annotate_variants` + `lookup_variants` via SQL JOIN
- [ ] 4.5.5 Test with multi-chromosome VCF verifying parallel execution
- [ ] 4.5.6 Benchmark throughput on 1M-variant VCF against full GRCh38 cache

## 5. Phase 5: Polish (deferred)

### 5.1 HGVS Notation
- [ ] 5.1.1 Implement HGVS coding notation (c. notation)
- [ ] 5.1.2 Implement HGVS protein notation (p. notation)
- [ ] 5.1.3 Implement HGVS genomic notation (g. notation)

### 5.2 Additional Features
- [ ] 5.2.1 SIFT score extraction from cache
- [ ] 5.2.2 PolyPhen score extraction from cache
- [ ] 5.2.3 Regulatory consequence terms
