# datafusion-bio-format-ensembl-cache

DataFusion `TableProvider` implementations for raw Ensembl VEP cache directories.

## Supported entities

- `variation`
- `transcript`
- `exon`
- `translation`
- `regulatory_feature`
- `motif_feature`

## Notes

- Reads `.gz` files directly (no manual `gunzip` required).
- Uses streaming execution and emits batches incrementally.
- Supports file-level parallel reading (partitioned by source files).
- Supports native Storable (`pst0`) decoding for transcript/regulatory cache files.
- Supports configurable coordinate output:
  - `coordinate_system_zero_based = false` (default): 1-based closed `[start, end]`
  - `coordinate_system_zero_based = true`: 0-based half-open `[start, end)`
- Writes coordinate mode to Arrow schema metadata as
  `bio.coordinate_system_zero_based`.
- Supports fixture/test payload shortcuts for unit fixtures:
  - `storable`: `JSON:{...}`
  - `sereal`: `SRL1{...}`

## Filtering

The following entries are automatically filtered out during extraction to match
VEP's own annotation behaviour:

- **LOC-prefixed gene pseudo-records**: Gene-level placeholder entries with
  `LOC*` stable IDs (e.g. `LOC644525`) are not real transcripts. They have
  `biotype = "pseudogene"`, empty exon arrays, and are skipped by VEP during
  annotation. Filtered from transcript, exon, and translation tables.
- **Exon slice/padding artefacts**: Perl Storable serialization can embed
  chromosome-spanning slice objects (`start=1`, `end=<chr_length>`, no
  `stable_id`) into `_trans_exon_array`. These are skipped in all exon
  extraction paths.
- **Transcript/gene objects in exon arrays**: Occasionally `_trans_exon_array`
  contains objects with transcript (`ENST*`) or gene (`ENSG*`) stable IDs
  instead of exon IDs (`ENSE*`, `exon-*`). These are filtered out based on
  the `stable_id` prefix.
- **Generic `pseudogene` biotype**: The bare `pseudogene` biotype (as opposed
  to specific subtypes like `processed_pseudogene`, `unprocessed_pseudogene`,
  `transcribed_processed_pseudogene`, etc.) is not used by VEP for consequence
  annotation. Filtered from transcript, exon, and translation tables.
- **`aligned_transcript` biotype**: An Ensembl-internal biotype that VEP does
  not evaluate for consequence annotation. Filtered from transcript, exon, and
  translation tables.
- **`compmerge.*` pseudo-exons**: Collapsed/merged exon entries (e.g.
  `compmerge.435.pooled.chr22`) found in havana\_tagene and lncRNA transcripts.
  These span entire intron regions (~25 kb average vs ~320 bp for real exons)
  and do not represent actual exon boundaries. VEP treats the underlying
  individual exons, not the merged entry. Filtered from exon tables and
  transcript exon lists based on the `stable_id` prefix.
- **Duplicate transcripts across region bins**: The VEP cache bins
  transcripts by genomic region. A transcript near a bin boundary can appear
  in multiple bins, potentially across files assigned to different partitions.
  The `TableProvider` emits all occurrences without dedup so that consumers
  can choose their own strategy. The `storable_to_parquet` export example
  applies SQL-level dedup using `ROW_NUMBER()` window functions that prefer
  entries with non-null CDS boundaries: transcripts by `stable_id` (ordered
  by `cds_start NULLS LAST`), translations by `transcript_id` (ordered by
  `cdna_coding_start NULLS LAST`), and exons by `(transcript_id, exon_number)`
  (ordered by `stable_id NULLS LAST`).

## Exon Source Fallback

The VEP Perl cache stores exon data in two locations within each transcript:

1. `_trans_exon_array` — the primary source, but sometimes empty or containing
   only integer/Mapper references instead of materialised exon objects.
2. `_variation_effect_feature_cache.sorted_exons` — a fully materialised array
   of `Bio::EnsEMBL::Exon` objects, always present when exon data exists.

The extractor tries `_trans_exon_array` first and falls back to `sorted_exons`
when the primary source is missing or empty. This recovers exon data for ~46%
of transcripts that would otherwise have no exon entries.

## Entity Schemas

### Transcript

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Genomic start position |
| `end` | Int64 | no | Genomic end position |
| `strand` | Int8 | no | Strand (+1/-1) |
| `stable_id` | Utf8 | no | Ensembl transcript ID (ENST...) |
| `version` | Int32 | yes | Transcript version |
| `biotype` | Utf8 | yes | Transcript biotype |
| `source` | Utf8 | yes | Source database |
| `is_canonical` | Boolean | yes | Whether this is the canonical transcript |
| `gene_stable_id` | Utf8 | yes | Parent gene ID (ENSG...) |
| `gene_symbol` | Utf8 | yes | Gene symbol (e.g. BRCA1) |
| `gene_symbol_source` | Utf8 | yes | Gene symbol source (e.g. HGNC) |
| `gene_hgnc_id` | Utf8 | yes | HGNC ID |
| `refseq_id` | Utf8 | yes | RefSeq transcript ID |
| `cds_start` | Int64 | yes | CDS genomic start |
| `cds_end` | Int64 | yes | CDS genomic end |
| `cdna_coding_start` | Int64 | yes | cDNA coding start offset |
| `cdna_coding_end` | Int64 | yes | cDNA coding end offset |
| `translation_stable_id` | Utf8 | yes | Translation ID (ENSP...) |
| `translation_start` | Int64 | yes | Translation start |
| `translation_end` | Int64 | yes | Translation end |
| `exon_count` | Int32 | yes | Number of exons |
| `exons` | `List<Struct<start:Int64, end:Int64, phase:Int8>>` | yes | Exon boundaries with reading frame phase |
| `cdna_seq` | Utf8 | yes | Translatable CDS nucleotide sequence |
| `peptide_seq` | Utf8 | yes | Protein sequence |
| `codon_table` | Int32 | yes | NCBI genetic code table ID (1 = standard) |
| `tsl` | Int32 | yes | Transcript support level (1–5) |
| `appris` | Utf8 | yes | APPRIS annotation (e.g. `principal1`, `alternative2`) |
| `mane_select` | Utf8 | yes | MANE Select transcript identifier |
| `mane_plus_clinical` | Utf8 | yes | MANE Plus Clinical transcript identifier |
| `gene_phenotype` | Boolean | yes | Gene phenotype flag used by VEP `GENE_PHENO` |
| `ccds` | Utf8 | yes | CCDS identifier |
| `swissprot` | Utf8 | yes | UniProt Swiss-Prot accession |
| `trembl` | Utf8 | yes | UniProt TrEMBL accession |
| `uniparc` | Utf8 | yes | UniParc identifier |
| `uniprot_isoform` | Utf8 | yes | UniProt isoform identifier |
| `cds_start_nf` | Boolean | yes | Transcript has incomplete CDS start (`cds_start_NF`); `false` when absent |
| `cds_end_nf` | Boolean | yes | Transcript has incomplete CDS end (`cds_end_NF`); `false` when absent |
| `mature_mirna_regions` | `List<Struct<start:Int64, end:Int64>>` | yes | Mature miRNA genomic regions derived from transcript attributes; null for non-miRNA transcripts |
| `translateable_seq` | Utf8 | yes | Translatable CDS sequence (top-level promoted field) |
| `cdna_mapper_segments` | `List<Struct<genomic_start:Int64, genomic_end:Int64, cdna_start:Int64, cdna_end:Int64, ori:Int8>>` | yes | cDNA ↔ genomic coordinate mapping segments |
| `bam_edit_status` | Utf8 | yes | BAM edit status flag |
| `has_non_polya_rna_edit` | Boolean | yes | Whether transcript has non-poly-A RNA edits |
| `spliced_seq` | Utf8 | yes | Spliced transcript sequence |
| `flags_str` | Utf8 | yes | CDS NF flags in VEP encounter order (e.g. `cds_start_NF&cds_end_NF`) |

### Exon

Standalone exon table — one row per exon per transcript.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Exon genomic start |
| `end` | Int64 | no | Exon genomic end |
| `strand` | Int8 | no | Strand (+1/-1) |
| `stable_id` | Utf8 | yes | Exon stable ID (ENSE...) |
| `version` | Int32 | yes | Exon version |
| `phase` | Int8 | yes | Start phase |
| `end_phase` | Int8 | yes | End phase |
| `is_current` | Boolean | yes | Whether exon is current |
| `is_constitutive` | Boolean | yes | Whether exon is constitutive |
| `transcript_id` | Utf8 | no | Parent transcript ID (ENST...) |
| `gene_stable_id` | Utf8 | yes | Parent gene ID (ENSG...) |
| `exon_number` | Int32 | no | 1-based exon number within transcript |

### Translation

Standalone translation table — one row per coding transcript.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Translation genomic start |
| `end` | Int64 | no | Translation genomic end |
| `stable_id` | Utf8 | yes | Translation stable ID (ENSP...) |
| `version` | Int32 | yes | Translation version |
| `translation_start` | Int64 | yes | Translation start position |
| `translation_end` | Int64 | yes | Translation end position |
| `protein_len` | Int64 | yes | Protein length (amino acids) |
| `transcript_id` | Utf8 | no | Parent transcript ID (ENST...) |
| `gene_stable_id` | Utf8 | yes | Parent gene ID (ENSG...) |
| `cdna_coding_start` | Int64 | yes | cDNA coding start offset |
| `cdna_coding_end` | Int64 | yes | cDNA coding end offset |
| `cds_len` | Int64 | yes | CDS length (derived: `cdna_coding_end - cdna_coding_start + 1`) |
| `translation_seq` | Utf8 | yes | Protein/peptide sequence |
| `cds_sequence` | Utf8 | yes | Translatable CDS nucleotide sequence |
| `protein_features` | `List<Struct<analysis:Utf8, hseqname:Utf8, start:Int64, end:Int64>>` | yes | Protein domain/feature annotations (see below) |
| `sift_predictions` | `List<Struct<position:Int32, amino_acid:Utf8, prediction:Utf8, score:Float32>>` | yes | SIFT pathogenicity predictions (see below) |
| `polyphen_predictions` | `List<Struct<position:Int32, amino_acid:Utf8, prediction:Utf8, score:Float32>>` | yes | PolyPhen-2 pathogenicity predictions (see below) |

#### Protein Features

The `protein_features` column stores domain and feature annotations from
`_variation_effect_feature_cache.protein_features`. Each entry contains:

- `analysis`: Analysis/database name (e.g. `Pfam`, `PANTHER`, `Gene3D`)
- `hseqname`: Domain/feature accession (e.g. `PF00069`)
- `start`: Feature start position in protein coordinates
- `end`: Feature end position in protein coordinates

At annotation time, VEP checks which protein features overlap the variant's
protein position and formats them as `analysis:hseqname` joined with `&`.

#### SIFT and PolyPhen Predictions

The `sift_predictions` and `polyphen_predictions` columns contain per-position,
per-amino-acid pathogenicity scores decoded from VEP's
`ProteinFunctionPredictionMatrix` binary format. Each entry contains:

- `position`: 1-based protein position
- `amino_acid`: Single-letter amino acid substitution (one of 20 standard AAs)
- `prediction`: Qualitative prediction string
- `score`: Numeric score (0.0–1.0)

**SIFT prediction values** (lower score = more damaging):

| Code | Prediction |
|------|------------|
| 0 | `tolerated` |
| 1 | `deleterious` |
| 2 | `tolerated - low confidence` |
| 3 | `deleterious - low confidence` |

**PolyPhen-2 prediction values** (higher score = more damaging):

| Code | Prediction |
|------|------------|
| 0 | `probably damaging` |
| 1 | `possibly damaging` |
| 2 | `benign` |
| 3 | `unknown` |

**Binary matrix format** (decoded natively from raw VEP cache):

The VEP cache stores SIFT and PolyPhen scores as gzip-compressed binary
matrices in `_variation_effect_feature_cache.protein_function_predictions`.
Each matrix has a 3-byte `VEP` header followed by concatenated 2-byte
little-endian unsigned shorts — 20 per protein position (one per amino acid
in the order `A C D E F G H I K L M N P Q R S T V W Y`).

Each 16-bit value encodes:
- **Top 2 bits** (bits 14–15): Qualitative prediction code (see tables above)
- **Bottom 10 bits** (bits 0–9): Score × 1000 (divide by 1000 for 3 d.p. float)
- **0xFFFF**: No prediction (reference amino acid at this position)

The decoder handles both gzip-compressed matrices (raw VEP cache) and
uncompressed matrices, producing the structured list column directly
during cache-to-parquet conversion.

### Regulatory Feature

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Feature start |
| `end` | Int64 | no | Feature end |
| `strand` | Int8 | no | Strand |
| `stable_id` | Utf8 | yes | Regulatory feature stable ID (ENSR...) |
| `db_id` | Int64 | yes | Internal database ID |
| `feature_type` | Utf8 | yes | Feature type (e.g. Promoter, Enhancer) |
| `epigenome_count` | Int32 | yes | Number of active epigenomes |
| `regulatory_build_id` | Int64 | yes | Regulatory build ID |
| `cell_types` | Utf8 | yes | Active cell types (JSON) |

### Motif Feature

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Feature start |
| `end` | Int64 | no | Feature end |
| `strand` | Int8 | no | Strand |
| `motif_id` | Utf8 | yes | Motif feature stable ID |
| `db_id` | Int64 | yes | Internal database ID |
| `score` | Float64 | yes | Motif score |
| `binding_matrix` | Utf8 | yes | Binding matrix identifier |
| `cell_types` | Utf8 | yes | Cell types (JSON) |
| `overlapping_regulatory_feature` | Utf8 | yes | Overlapping regulatory feature ID |
| `transcription_factors` | Utf8 | yes | Transcription factor annotation for TFBS consequences |

### Variation

| Column | Type | Nullable | Description |
|---|---|---|---|
| `chrom` | Utf8 | no | Chromosome name |
| `start` | Int64 | no | Variant start |
| `end` | Int64 | no | Variant end |
| `variation_name` | Utf8 | no | Variant name (e.g. rs123) |
| `allele_string` | Utf8 | no | Allele string (e.g. A/G) |
| `region_bin` | Int64 | no | Region bin for indexing |
| `failed` | Int8 | yes | Failed QC flag |
| `somatic` | Int8 | yes | Somatic flag |
| `strand` | Int8 | yes | Strand |
| `minor_allele` | Utf8 | yes | Minor allele |
| `minor_allele_freq` | Float64 | yes | Minor allele frequency |
| `clin_sig` | Utf8 | yes | ClinVar clinical significance |
| `phenotype_or_disease` | Int8 | yes | Phenotype/disease flag |
| `clinical_impact` | Utf8 | yes | Clinical impact annotation |
| `pubmed` | Utf8 | yes | PubMed IDs |
| `var_synonyms` | Utf8 | yes | Variant synonyms |
| `dbsnp_ids` | Utf8 | yes | Extracted dbSNP IDs (dynamic, from `info.txt` sources) |
| `cosmic_ids` | Utf8 | yes | Extracted COSMIC IDs (dynamic) |
| `clinvar_ids` | Utf8 | yes | Extracted ClinVar IDs (dynamic) |
| _extra columns_ | Utf8 | yes | Additional population frequency columns from `variation_cols` in `info.txt` (e.g. `AFR`, `gnomADe_AFR`, etc.) |

All entity schemas also include **provenance columns**: `species`, `assembly`, `cache_version`, `serializer_type`, `source_cache_path`, `source_file`, plus `raw_object_json` and `object_hash` for full object traceability.

## Projection Pushdown

Transcript, exon, and translation schemas support projection pushdown. When VEP-related
columns (e.g. `exons`, `cdna_seq`, `peptide_seq`, `mature_mirna_regions`,
`cdna_mapper_segments`, `spliced_seq`, `translation_seq`, `cds_sequence`,
`protein_features`, `sift_predictions`, `polyphen_predictions`) are not
selected in a query, the parser skips extracting those fields, significantly
reducing parse overhead.

## Parallel Scanning

- By default, parallelism follows DataFusion session `target_partitions`.
- You can override per table via `EnsemblCacheOptions::target_partitions`.
- Effective partition count is capped by discovered source file count.
- Gzip files are not split internally; parallelism is across files.

## Example: Entity-Specific Provider

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_bio_format_ensembl_cache::{EnsemblCacheOptions, VariationTableProvider};
use std::sync::Arc;

# async fn example() -> datafusion::common::Result<()> {
let ctx = SessionContext::new();
let mut options = EnsemblCacheOptions::new("/path/to/cache");
options.coordinate_system_zero_based = true;
options.target_partitions = Some(8);
let table = VariationTableProvider::new(options)?;
ctx.register_table("vep_variation", Arc::new(table))?;
# Ok(())
# }
```

## Example: Generic Provider Factory

Use `EnsemblCacheTableProvider::for_entity(...)` when you want one entrypoint
and switch by entity type yourself.

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind,
};

fn parse_cache_type(cache_type: &str) -> Option<EnsemblEntityKind> {
    match cache_type.to_ascii_lowercase().as_str() {
        "variation" => Some(EnsemblEntityKind::Variation),
        "transcript" => Some(EnsemblEntityKind::Transcript),
        "exon" => Some(EnsemblEntityKind::Exon),
        "translation" => Some(EnsemblEntityKind::Translation),
        "regulatory_feature" => Some(EnsemblEntityKind::RegulatoryFeature),
        "motif_feature" => Some(EnsemblEntityKind::MotifFeature),
        _ => None,
    }
}

# async fn example() -> datafusion::common::Result<()> {
let cache_type = "variation";
let kind = parse_cache_type(cache_type).expect("unsupported cache type");

let table = EnsemblCacheTableProvider::for_entity(
    kind,
    EnsemblCacheOptions::new("/path/to/cache"),
)?;

let ctx = SessionContext::new();
ctx.register_table("vep_cache", table)?;
# Ok(())
# }
```

## Parquet Cache Generation

The `storable_to_parquet` example converts raw VEP cache directories into
Parquet files suitable for downstream annotation engines.

### Prerequisites

1. **VEP cache**: Download and install an Ensembl VEP cache. For merged
   (Ensembl + RefSeq) caches:

   ```bash
   # Example: VEP 115, GRCh38, merged cache
   vep_install --CACHEDIR /data/vep --SPECIES homo_sapiens_merged --ASSEMBLY GRCh38 --VERSION 115
   ```

   The cache root is the version/assembly directory, e.g.
   `/data/vep/homo_sapiens_merged/115_GRCh38`.

2. **Build the converter** (release mode recommended for large caches):

   ```bash
   cargo build --release --example storable_to_parquet --package datafusion-bio-format-ensembl-cache
   ```

### Usage

```
cargo run --release --example storable_to_parquet -- \
  <cache_root> <output_dir> <entity> [partitions] [--chrom CHROM]
```

| Argument | Description |
|---|---|
| `cache_root` | Path to VEP cache version directory (e.g. `.../115_GRCh38`) |
| `output_dir` | Directory for output Parquet files |
| `entity` | Entity type: `transcript`, `exon`, `translation`, `regulatory`, `motif`, `variation` |
| `partitions` | Number of parallel partitions (default: 8) |
| `--chrom CHROM` | Optional chromosome filter (e.g. `22`, `X`) |

Output filename: `<output_dir>/<version>_<assembly>_<entity>[_<chrom>].parquet`

### Example: Generate all chr22 Parquet files

```bash
CACHE=/data/vep/homo_sapiens_merged/115_GRCh38
OUT=/data/vep/parquet

for entity in transcript exon translation regulatory motif variation; do
  cargo run --release --example storable_to_parquet -- \
    $CACHE $OUT $entity 8 --chrom 22
done
```

This produces:

```
115_GRCh38_transcript_22.parquet
115_GRCh38_exon_22.parquet
115_GRCh38_translation_22.parquet
115_GRCh38_regulatory_22.parquet
115_GRCh38_motif_22.parquet
115_GRCh38_variation_22.parquet
```

### Example: Generate whole-genome Parquet files

Omit `--chrom` to export all chromosomes into a single file per entity:

```bash
cargo run --release --example storable_to_parquet -- \
  $CACHE $OUT transcript 8
```

### Deduplication

The VEP cache bins transcripts by genomic region, so transcripts near bin
boundaries appear in multiple bins. The converter applies SQL-level dedup
using `ROW_NUMBER()` window functions:

- **Transcripts**: deduplicated by `stable_id`, preferring entries with
  non-null `cds_start`
- **Translations**: deduplicated by `transcript_id`, preferring entries
  with non-null `cdna_coding_start`
- **Exons**: deduplicated by `(transcript_id, exon_number)`, preferring
  entries with non-null `stable_id`
- **Regulatory/Motif/Variation**: no dedup needed (no cross-bin overlap)

### Performance notes

- Storable-format entities (transcript, exon, translation, regulatory, motif)
  require full Perl Storable binary deserialization — expect ~80-90s per
  entity for a single chromosome on an M-series Mac.
- Memory scales with partition count. For Storable entities, 4-8 partitions
  is a good balance; higher values increase RSS without improving throughput.
- Variation entities use a lightweight TSV parser and are significantly faster.
- Output uses ZSTD compression for good size/speed tradeoff.
