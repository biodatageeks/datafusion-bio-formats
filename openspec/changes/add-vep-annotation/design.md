# VEP Annotation Engine — Design

## Context

Ensembl VEP (Variant Effect Predictor) annotates genomic variants with predicted functional consequences. The reference implementation is a Perl tool that reads Ensembl cache files and produces tab-separated output. This design describes a native DataFusion implementation that operates on Arrow columnar data, enabling SQL-based annotation pipelines with full parallelism and pushdown optimization.

The design spans two repositories:
- **`datafusion-bio-formats`** — Transcript model expansion in the Ensembl cache table provider
- **`datafusion-bio-functions`** — New `bio-function-vep` crate with the annotation engine

## Goals / Non-Goals

**Goals:**
- Native SQL annotation: `SELECT * FROM annotate_variants('my_vcf', 'vep_transcript')`
- Chromosome-partitioned parallelism matching existing `bio-function-ranges` patterns
- Format-agnostic cache consumption via DataFusion `TableProvider` abstraction
- Two independent, composable operations: lookup and prediction
- Column selection / projection pushdown into cache reads
- Correct consequence prediction matching Ensembl VEP SO term assignments

**Non-Goals:**
- HGVS notation (deferred to Phase 5)
- SIFT/PolyPhen score computation (deferred)
- Regulatory consequence terms (deferred)
- VEP plugin compatibility
- Writing/generating cache files

---

## Architecture: Chromosome-Partitioned Streaming Pipeline

```
+--------------------------------------------------------------+
|  annotate_variants('vcf_table', 'transcript_table')          |
|  Table Function (registered via ctx.register_udtf)           |
+-----------------------------+--------------------------------+
                              |
                 +------------v--------------+
                 |   AnnotateExec            |
                 |   (ExecutionPlan)         |
                 |   N partitions = N chroms |
                 +------+------+------+-----+
                        |      |      |        parallel threads
                 +------v-+  +-v--+  +v-----+
                 | chr1   |  |chr2|  |chrX  | ...
                 |partition|  |part|  |part. |
                 +----+---+  +--+-+  +--+---+
                      |         |       |
   +------------------v---------v-------v---------------------+
   |      Per-Chromosome Pipeline                              |
   |                                                           |
   |  SETUP (once per chromosome):                             |
   |  +------------------------------------------------------+ |
   |  | 1. Load transcripts for this chrom                   | |
   |  |    -> Build COITree<TranscriptRecord>                | |
   |  |    (reuse superintervals crate)                      | |
   |  +------------------------------------------------------+ |
   |                                                           |
   |  STREAM (per RecordBatch of VCF variants):                |
   |  +------------------------------------------------------+ |
   |  | For each variant in batch:                           | |
   |  |   +- Probe transcript tree -> overlaps               | |
   |  |   |  +- For each overlapping transcript:             | |
   |  |   |  |  +- Classify position (coding/intron/UTR/     | |
   |  |   |  |  |  splice)                                   | |
   |  |   |  |  +- If coding: codon analysis                 | |
   |  |   |  |  +- Assign consequence + impact               | |
   |  |   |  +- Pick most severe consequence                 | |
   |  |   +- Emit annotated row                              | |
   |  +------------------------------------------------------+ |
   |                                                           |
   |  OUTPUT: Annotated RecordBatch                            |
   |  (original VCF cols + consequence + gene + impact + ...)  |
   +-----------------------------------------------------------+
```

### Design Rationale

| Design Choice | Rationale |
|---|---|
| **Chromosome partitions** | Natural parallelism boundary. Each chromosome is independent. VCF is typically chromosome-sorted. Matches existing `bio-function-ranges` pattern. |
| **Pre-built index per chromosome** | Transcript tree built once at partition startup, probed millions of times. ~240K transcripts total, ~1-15K per chromosome — fits easily in memory. |
| **Streaming RecordBatches** | VCF is streamed through the index, not materialized. Memory proportional to batch size (~8K rows), not input size. |
| **Table function, not scalar UDF** | Owns the execution plan. Controls parallelism, index lifecycle, partition strategy. Scalar UDFs cannot share state across rows. |
| **Reuse `superintervals`/`coitrees`** | Already vendored in `bio-function-ranges`. SIMD-optimized interval queries. |

---

## Two Annotation Modes

| Mode | Function | What it does | Needs |
|---|---|---|---|
| **Lookup** | `lookup_variants('vcf', 'vep_variation')` | Match input variants against cached known variants. Returns rs-ID, frequencies, clinical significance. No biology computation. | Variation cache table |
| **Predict** | `annotate_variants('vcf', 'vep_transcript')` | Find overlapping transcripts and compute consequence (missense, frameshift, etc.). The core VEP algorithm. | Transcript cache table |

### Usage Patterns

```sql
-- Mode 1: Lookup only (fast, no consequence prediction)
SELECT * FROM lookup_variants('my_vcf', 'vep_variation')

-- Mode 2: Prediction only (CPU-intensive)
SELECT * FROM annotate_variants('my_vcf', 'vep_transcript')

-- Mode 3: Combined (composed via SQL JOIN)
SELECT a.*, l.rsid, l.clin_sig, l.gnomADg_AF
FROM annotate_variants('my_vcf', 'vep_transcript') a
LEFT JOIN lookup_variants('my_vcf', 'vep_variation') l
  ON a.chrom = l.chrom AND a.pos = l.pos AND a.alt = l.alt

-- Mode 4: Lookup-first pipeline (filter by frequency, then predict)
CREATE TEMP TABLE rare AS
SELECT * FROM lookup_variants('my_vcf', 'vep_variation')
WHERE gnomADg_AF IS NULL OR gnomADg_AF < 0.01;

SELECT * FROM annotate_variants('rare', 'vep_transcript')
WHERE impact IN ('HIGH', 'MODERATE');
```

### Output Modes

1. **Most severe per variant** (default, VEP `--pick` equivalent):
   - One row per input variant
   - Shows the single most severe consequence across all transcripts
   - Fastest — no fan-out

2. **Per transcript** (VEP default):
   - One row per (variant, transcript) pair
   - Shows consequence for each overlapping transcript
   - Fan-out: ~3-5x more output rows than input

---

## Pluggable Cache Format

The VEP crate never reads files directly. It consumes cache data through DataFusion tables registered by the user:

```rust
// Option A: Parquet
ctx.register_parquet("vep_transcript", "/path/to/transcripts.parquet", ...).await?;

// Option B: Vortex (future)
let vtx = VortexTableProvider::open("/path/to/transcripts.vtx")?;
ctx.register_table("vep_transcript", Arc::new(vtx))?;

// Option C: Native Ensembl cache
let tp = TranscriptTableProvider::new(EnsemblCacheOptions::new(cache_root))?;
ctx.register_table("vep_transcript", Arc::new(tp))?;

// VEP annotation — format-agnostic
SELECT * FROM annotate_variants('my_vcf', 'vep_transcript')
```

### Schema Contracts

The VEP crate depends only on column names and types:

**Transcript table contract:**
```
chrom: Utf8, start: Int64, end: Int64, strand: Int8,
stable_id: Utf8, gene_symbol: Utf8, biotype: Utf8,
is_canonical: Boolean,
coding_region_start: Int64, coding_region_end: Int64,
exons: List<Struct<start:Int64, end:Int64, phase:Int8>>,
cdna_seq: Utf8, peptide_seq: Utf8, codon_table: Int32
```

**Variation table contract:**
```
chrom: Utf8, start: Int64, end: Int64,
variation_name: Utf8, allele_string: Utf8,
clin_sig: Utf8, gnomADg_AF: Float64, ...
```

### Migration Path

```
Phase 1 (now):   Parquet (30 GB)  -> register_parquet() -> VEP works
Phase 2 (future): Vortex (9.5 GB) -> VortexTableProvider -> VEP works (no changes)
                  Benefits: 5-10x faster decode, 68% less storage
```

---

## Join Strategy: Two Joins, Two Approaches

| Join | Build Side | Probe Side | Strategy |
|---|---|---|---|
| **Variant -> Transcript** | Transcripts (~240K, ~720 MB) | VCF (streamed) | **COITree in memory** |
| **Variant -> Known Variants** | VCF (~10-100M, ~2-4 GB) | Variation cache (1.17B rows, 30 GB) | **IntervalJoinExec** |

### Join 1: VCF x Transcripts (consequence prediction)

Transcripts are the build side (~15K per chromosome x ~2 KB each = ~30 MB per chromosome). VCF variants stream through and probe the COITree for overlapping transcripts.

### Join 2: VCF x Variation Cache (known variant lookup)

The variation cache (1.17B rows, 30 GB Parquet) does not fit in memory. Uses `IntervalJoinExec` from `bio-function-ranges` with VCF as build side:

1. VCF is hashed by (chrom) equi-join key, intervals stored for range matching
2. Variation cache streams through, row groups pruned by chrom min/max stats
3. Each probe row checks the VCF hash map for interval overlap
4. Allele matching applied as post-filter

---

## Coordinate System Support

Different formats use different coordinate conventions:

| Format | Native System |
|---|---|
| VCF | 1-based closed |
| BAM/CRAM/BED | 0-based half-open |
| GFF / Ensembl Cache | 1-based closed |

**Existing infrastructure:**
- All TableProviders store `bio.coordinate_system_zero_based` in schema metadata
- `IntervalJoinExec` uses `FilterOp::Strict` vs `FilterOp::Weak` for boundary adjustment

**Design:**

```rust
struct CoordinateNormalizer {
    input_zero_based: bool,   // from VCF schema metadata
    cache_zero_based: bool,   // from cache table schema metadata
}

impl CoordinateNormalizer {
    fn to_cache_start(&self, start: i64) -> i64 { ... }
    fn to_cache_end(&self, end: i64) -> i64 { ... }
    fn filter_op(&self) -> FilterOp { ... }
}
```

Output coordinates preserve the input VCF's coordinate system with the metadata key set accordingly.

---

## Column Selection and Sparse Optimization

The variation cache has 78 columns. Users specify which annotation columns to retrieve:

```sql
-- Only rsID + gnomAD (reads ~8 GB instead of 30 GB)
SELECT * FROM lookup_variants('my_vcf', 'vep_variation', 'variation_name,gnomADg_AF')

-- Default: core columns only
SELECT * FROM lookup_variants('my_vcf', 'vep_variation')
```

The column list becomes projection pushdown into the cache table scan. Auto-pruning of all-null columns is supported (controlled by flag, default true).

| Columns Requested | Parquet I/O | % of File |
|---|---|---|
| `variation_name` only | ~5.8 GB | 19% |
| `variation_name, clin_sig` | ~5.8 GB | 19% |
| `variation_name, gnomADg_AF` | ~7.9 GB | 26% |
| All gnomAD frequencies | ~18.2 GB | 61% |
| All 78 columns | ~30 GB | 100% |

---

## `lookup_variants()` Implementation

This is an interval join (indels span ranges). Internally generates an interval overlap SQL plan using `IntervalJoinExec` from `bio-function-ranges`:

```sql
-- Generated internally by lookup_variants('my_vcf', 'vep_variation', 'variation_name,clin_sig')
SELECT a.chrom, a.start AS pos, a.ref, a.alt,
       b.variation_name AS rsid, b.clin_sig
FROM my_vcf AS a, vep_variation AS b
WHERE a.chrom = b.chrom
  AND CAST(a.end AS INTEGER) >= CAST(b.start AS INTEGER)
  AND CAST(a.start AS INTEGER) <= CAST(b.end AS INTEGER)
```

Post-join allele matching: `AND match_allele(a.alt, b.allele_string)`

**Dependency:** Requires `bio-function-ranges` for `IntervalJoinExec` and `BioQueryPlanner`. Uses `create_bio_session()` to get the optimized planner.

---

## Transcript Model Expansion (bio-formats)

### New columns in `transcript_schema()`

```
exons               List<Struct<start:Int64, end:Int64, phase:Int8>>
cdna_seq            Utf8 (nullable)
peptide_seq         Utf8 (nullable)
codon_table         Int32 (nullable, default 1 = standard)
tsl                 Int32 (nullable, transcript support level)
mane_select         Utf8 (nullable)
mane_plus_clinical  Utf8 (nullable)
```

### Parser changes

In `parse_transcript_line_into()` and `parse_transcript_storable_file()`:
- Parse `_trans_exon_array` -> `List<Struct<start, end, phase>>`
- Parse `_variation_effect_feature_cache.translateable_seq` -> `cdna_seq`
- Parse `_variation_effect_feature_cache.peptide` -> `peptide_seq`
- Parse `codon_table`, `tsl`, `mane_select`, `mane_plus_clinical`

### Projection pushdown

New projection flags (same pattern as existing `translation_projected`):
- `exons_projected: bool` — skip `_trans_exon_array` parsing if not selected
- `sequences_projected: bool` — skip `_variation_effect_feature_cache` if cdna_seq/peptide_seq not selected

Critical: `cdna_seq` + `peptide_seq` can be 500+ bytes per transcript. Only parse when projected.

---

## Consequence Engine

### Core Algorithm

```
classify_variant(variant, transcript) -> ConsequenceResult

ConsequenceResult {
    consequence: &'static str,       // SO term
    impact: Impact,                   // HIGH/MODERATE/LOW/MODIFIER
    gene_symbol: Option<String>,
    transcript_id: String,
    biotype: String,
    exon_number: Option<String>,      // "3/12"
    intron_number: Option<String>,    // "2/11"
    cdna_position: Option<i64>,
    cds_position: Option<i64>,
    protein_position: Option<i64>,
    amino_acids: Option<String>,      // "R/W"
    codons: Option<String>,           // "Cgg/Tgg"
}
```

### Position Classification

Given genomic position + transcript exon array + strand:

```
genomic_to_cds(pos, transcript) -> Option<CdsPosition>

CdsPosition {
    offset: i64,          // 0-based offset from CDS start
    exon_index: usize,    // which exon
    codon_index: i64,     // which codon (offset / 3)
    codon_offset: i64,    // position within codon (offset % 3)
}
```

For reverse strand: iterate exons in reverse, complement bases.

### Genetic Code Tables

```rust
fn translate_codon(bases: [u8; 3], table_id: u8) -> u8   // amino acid
fn is_start_codon(bases: [u8; 3], table_id: u8) -> bool
fn is_stop_codon(bases: [u8; 3], table_id: u8) -> bool
```

Supports standard code (table 1) and vertebrate mitochondrial code.

### Allele Conversion

```rust
/// VCF: REF="ACGT", ALT="A"    -> VEP: "CGT/-" (deletion)
/// VCF: REF="A", ALT="ACGT"    -> VEP: "-/CGT" (insertion)
/// VCF: REF="A", ALT="G"       -> VEP: "A/G" (SNV)
fn vcf_to_vep_allele(ref_allele: &str, alt_allele: &str) -> (String, String)
fn allele_matches(alt: &str, allele_string: &str) -> bool
```

### SO Consequence Hierarchy

```
HIGH:     transcript_ablation, splice_acceptor_variant, splice_donor_variant,
          stop_gained, frameshift_variant, stop_lost, start_lost
MODERATE: inframe_insertion, inframe_deletion, missense_variant
LOW:      splice_region_variant, synonymous_variant
MODIFIER: intron_variant, 5_prime_UTR_variant, 3_prime_UTR_variant,
          non_coding_transcript_exon_variant, upstream_gene_variant,
          downstream_gene_variant, intergenic_variant
```

---

## Crate Structure (`bio-function-vep`)

```
datafusion/bio-function-vep/
+-- Cargo.toml
+-- src/
|   +-- lib.rs                         # register_vep_functions(), exports
|   |
|   +-- -- Index Layer --
|   +-- transcript_index.rs            # Per-chrom COITree<TranscriptRecord>
|   +-- variation_index.rs             # Per-chrom hash map for known variants
|   +-- transcript_record.rs           # Pre-parsed transcript model from Arrow
|   |
|   +-- -- Consequence Engine --
|   +-- consequence.rs                 # Position classification + codon analysis
|   +-- codon.rs                       # Genetic code tables (standard + mito)
|   +-- coordinate.rs                  # 0-based/1-based normalization
|   +-- position.rs                    # Genomic->CDS coordinate mapping
|   +-- so_terms.rs                    # SO consequence terms, severity hierarchy
|   +-- allele.rs                      # VCF<->VEP allele conversion, matching
|   |
|   +-- -- Execution Layer --
|   +-- table_function.rs              # annotate_variants() + lookup_variants()
|   +-- annotate_exec.rs              # AnnotateExec: consequence prediction plan
|   +-- annotate_stream.rs            # Per-partition streaming logic
|   +-- lookup_provider.rs            # LookupProvider: SQL-plan-based cache lookup
|   +-- output_schema.rs              # Arrow schema for annotated output
|   +-- schema_contract.rs            # Column name/type contracts for cache tables
|   |
|   +-- -- UDFs --
|   +-- udfs/
|   |   +-- mod.rs
|   |   +-- match_allele.rs           # Scalar: match_allele(alt, allele_string)
|   |   +-- vep_allele.rs             # Scalar: vep_allele(ref, alt)
|   |   +-- consequence_udf.rs        # Scalar: predict_consequence(...)
|   |
|   +-- -- HGVS (Phase 5) --
|       +-- hgvs.rs                    # HGVS notation (deferred)
|
+-- tests/
    +-- consequence_test.rs
    +-- allele_test.rs
    +-- index_test.rs
    +-- integration_test.rs
```

---

## Key Data Structures

### TranscriptRecord

```rust
struct TranscriptRecord {
    stable_id: String,
    gene_symbol: Option<String>,
    biotype: String,
    strand: i8,
    tx_start: i64,
    tx_end: i64,
    cds_start: Option<i64>,
    cds_end: Option<i64>,
    exons: Vec<ExonRecord>,
    cdna_seq: Option<Vec<u8>>,
    peptide_seq: Option<Vec<u8>>,
    codon_table: u8,
    is_canonical: bool,
    mane_select: Option<String>,
}

struct ExonRecord {
    start: i64,
    end: i64,
    phase: i8,
}
```

### TranscriptIndex

```rust
struct TranscriptIndex {
    trees: AHashMap<String, COITree<Arc<TranscriptRecord>, u32>>,
}

impl TranscriptIndex {
    fn build_from_batches(batches: Vec<RecordBatch>) -> Self { ... }
    fn query(&self, chrom: &str, pos: i64) -> Vec<&TranscriptRecord> { ... }
}
```

### AnnotateExec

```rust
struct AnnotateExec {
    vcf_input: Arc<dyn ExecutionPlan>,
    cache_dir: String,
    output_schema: SchemaRef,
    output_mode: OutputMode,
    properties: PlanProperties,
}

enum OutputMode {
    MostSevere,      // 1 row per variant (--pick)
    PerTranscript,   // 1 row per (variant, transcript)
}
```

---

## Parallelism Strategy

**Recommendation:** Build once, share via `Arc` + `OnceLock` per chromosome.

Index for chr1 (~15K transcripts) takes <100ms to build. Total for all chromosomes: <1 second.

## Memory Budget (consequence prediction only)

| Component | Per Chromosome | Total (24 chroms) |
|---|---|---|
| Transcript index (COITree) | ~15K transcripts x ~2KB = ~30 MB | ~720 MB |
| VCF batch buffer | ~8K rows x ~200B = ~1.6 MB | ~1.6 MB per partition |
| **Total** | | **~720 MB** |

The variation cache lookup adds no memory — it streams from Parquet via `IntervalJoinExec`.

---

## Risks / Trade-offs

- **Risk:** Consequence prediction correctness — SO term assignments must match Ensembl VEP output.
  **Mitigation:** Unit tests comparing against known VEP output for well-characterized variants (BRCA1, TP53, etc.).

- **Risk:** Memory pressure from transcript index on low-memory machines.
  **Mitigation:** ~720 MB total is manageable; per-chromosome lazy loading via `OnceLock` limits peak usage.

- **Risk:** Variation cache I/O bottleneck for whole-genome VCFs.
  **Mitigation:** Column selection reduces I/O (19% for core columns vs 100%), and Parquet row group pruning skips irrelevant chromosomes.

- **Trade-off:** Two separate table functions vs one combined function.
  **Decision:** Keep separate for composability — users can run lookup-only, prediction-only, or combine via SQL JOIN. This matches the Unix philosophy and enables pipeline patterns (filter by frequency first, then predict).

## Open Questions

- Exact `IntervalJoinExec` build/probe side configuration for optimal performance with very large VCFs (>100M variants)
- Whether to support VEP `--per_gene` output mode in addition to `--pick` and per-transcript
- HGVS notation complexity and whether to implement in Phase 5 or defer further
