# Add VEP Variant Annotation Engine

## Why

Variant Effect Predictor (VEP) annotation is a critical step in genomic analysis pipelines. Currently, users must shell out to Ensembl's Perl-based VEP tool, which breaks the SQL-native workflow, introduces serialization overhead, and cannot leverage DataFusion's parallelism or predicate/projection pushdown. A native DataFusion implementation enables sub-minute annotation of whole-genome VCFs directly in SQL, composable with existing bio-format table providers and bio-function range operations.

## What Changes

### Two-Repository Split

The VEP annotation engine spans two repositories with clear separation of concerns:

1. **`datafusion-bio-formats`** (this repo) — Extend the Ensembl cache transcript table provider with structured columns needed for consequence prediction (exon arrays, coding sequences, peptide sequences, codon tables, MANE annotations)

2. **`datafusion-bio-functions`** (separate repo) — New `bio-function-vep` crate containing:
   - Variant lookup against cached known variants (`lookup_variants()` table function)
   - Consequence prediction with transcript overlap (`annotate_variants()` table function)
   - Consequence engine (position classification, codon analysis, SO terms)
   - Allele conversion UDFs (`match_allele`, `vep_allele`)

### Pluggable Cache Format

The VEP engine is format-agnostic. It consumes cache data through DataFusion `TableProvider` instances — any format (Parquet, Vortex, native Ensembl cache) that exposes the required schema contract works without code changes to the VEP crate.

### Two Annotation Modes

- **`lookup_variants()`** — Match input variants against cached known variants (rs-IDs, frequencies, clinical significance). Uses `IntervalJoinExec` from `bio-function-ranges`. No biology computation.
- **`annotate_variants()`** — Find overlapping transcripts and compute consequences (missense, frameshift, splice, etc.). The core VEP algorithm with chromosome-partitioned streaming and per-chromosome COITree indexes.

## Impact

### Affected Specs
- **NEW**: `vep-annotation` — Complete VEP annotation system specification

### Affected Code

**This repo (`datafusion-bio-formats`):**
- `datafusion/bio-format-ensembl-cache/src/schema.rs` — Add structured columns to `transcript_schema()`
- `datafusion/bio-format-ensembl-cache/src/transcript.rs` — Parse exons, sequences, codon tables from JSON/storable
- `datafusion/bio-format-ensembl-cache/src/util.rs` — Add `List<Struct>` builder support

**Separate repo (`datafusion-bio-functions`):**
- New crate `datafusion/bio-function-vep/` with consequence engine, index layer, execution layer, and UDFs

### Breaking Changes
None — This is a purely additive change. Existing Ensembl cache schema gains new nullable columns.

### Dependencies
- `bio-function-ranges` (for `IntervalJoinExec` and `BioQueryPlanner`)
- `bio-format-ensembl-cache` (for transcript data)
- `superintervals`/`coitrees` (already vendored in `bio-function-ranges`)
- `ahash` (for O(1) hash map lookups)
