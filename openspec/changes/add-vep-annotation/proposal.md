# Add VEP Variant Annotation Engine

## Why

Variant Effect Predictor (VEP) annotation is a critical step in genomic analysis pipelines. Currently, users must shell out to Ensembl's Perl-based VEP tool, which breaks the SQL-native workflow, introduces serialization overhead, and cannot leverage DataFusion's parallelism or predicate/projection pushdown. A native DataFusion implementation enables sub-minute annotation of whole-genome VCFs directly in SQL, composable with existing bio-format table providers and bio-function range operations.

## What Changes

### Two-Repository Split

The VEP annotation engine spans two repositories with clear separation of concerns:

1. **`datafusion-bio-formats`** (this repo) — Extend the Ensembl cache transcript table provider with structured columns needed for consequence prediction (exon arrays, coding sequences, peptide sequences, codon tables, MANE annotations)
   and promote frequently used VEP CSQ metadata out of `raw_object_json`
   (gene phenotype, CCDS/UniProt IDs, CDS incompleteness flags, mature miRNA
   regions, motif transcription factors). The native cache providers also expose
   explicit VEP cache source metadata for Ensembl, merged, and RefSeq cache
   modes.

2. **`datafusion-bio-functions`** (separate repo) — New `bio-function-vep` crate containing:
   - Variant lookup against cached known variants (`lookup_variants()` table function)
   - Consequence prediction with transcript overlap (`annotate_variants()` table function)
   - Explicit cache source mode handling for `ensembl`, `merged`, and `refseq`
   - Consequence engine (position classification, codon analysis, SO terms)
   - Allele conversion UDFs (`match_allele`, `vep_allele`)

### Pluggable Cache Format

The VEP engine is format-agnostic. It consumes cache data through DataFusion `TableProvider` instances — any format (Parquet, Vortex, native Ensembl cache) that exposes the required schema contract works without code changes to the VEP crate.

### Explicit Cache Source Mode

Cache source mode is an explicit part of the schema and execution contract.
Users must provide `cache_source_type` with one of `ensembl`, `merged`, or
`refseq` when registering or exporting VEP cache tables. The implementation does
not infer source mode from directory names such as `homo_sapiens_refseq`, and it
does not support a legacy `merged=true` compatibility option.

### Two Annotation Modes

- **`lookup_variants()`** — Match input variants against cached known variants (rs-IDs, frequencies, clinical significance). Uses `IntervalJoinExec` from `bio-function-ranges`. No biology computation.
- **`annotate_variants()`** — Find overlapping transcripts and compute consequences (missense, frameshift, splice, etc.). The core VEP algorithm with chromosome-partitioned streaming and per-chromosome COITree indexes.

## Impact

### Affected Specs
- **NEW**: `vep-annotation` — Complete VEP annotation system specification

### Affected Code

**This repo (`datafusion-bio-formats`):**
- `datafusion/bio-format-ensembl-cache/src/info.rs` — Add explicit cache source mode configuration and validation
- `datafusion/bio-format-ensembl-cache/src/schema.rs` — Add structured columns to `transcript_schema()` and cache source metadata
- `datafusion/bio-format-ensembl-cache/src/transcript.rs` — Parse exons, sequences, transcript flags, miRNA regions, CSQ metadata, and transcript `dbID` from JSON/storable
- `datafusion/bio-format-ensembl-cache/src/discovery.rs` — Support RefSeq/merged region-file pruning without deriving source mode from paths
- `datafusion/bio-format-ensembl-cache/src/export_query.rs` — Apply source-aware transcript de-duplication for exported cache tables
- `datafusion/bio-format-ensembl-cache/src/regulatory.rs` — Parse motif transcription factors from JSON/storable
- `datafusion/bio-format-ensembl-cache/src/util.rs` — Add `List<Struct>` builder support
- `datafusion/bio-format-ensembl-cache/tests/integration_tests.rs` — Cover promoted transcript and motif columns
- `datafusion/bio-format-ensembl-cache/README.md` — Document the expanded schema contract
- Complete issue #190 raw-free transcript schema contract by promoting
  `display_xref_id`, `source_cache`, `refseq_match`, `refseq_edits`,
  `is_gencode_basic`, and `is_gencode_primary` from transcript objects.

**Separate repo (`datafusion-bio-functions`):**
- New crate `datafusion/bio-function-vep/` with consequence engine, index layer, execution layer, source-mode handling, and UDFs

### Breaking Changes
The VEP annotation API requires explicit `cache_source_type` source mode and does
not accept a legacy `merged=true` option. Existing Ensembl cache schema gains new
nullable columns.

### Dependencies
- `bio-function-ranges` (for `IntervalJoinExec` and `BioQueryPlanner`)
- `bio-format-ensembl-cache` (for transcript data)
- `superintervals`/`coitrees` (already vendored in `bio-function-ranges`)
- `ahash` (for O(1) hash map lookups)
