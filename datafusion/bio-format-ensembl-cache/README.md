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
| `tsl` | Int32 | yes | Transcript support level |
| `mane_select` | Utf8 | yes | MANE Select transcript identifier |
| `mane_plus_clinical` | Utf8 | yes | MANE Plus Clinical transcript identifier |

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

All entity schemas also include **provenance columns**: `species`, `assembly`, `cache_version`, `serializer_type`, `source_cache_path`, `source_file`, plus `raw_object_json` and `object_hash` for full object traceability.

## Projection Pushdown

Transcript, exon, and translation schemas support projection pushdown. When VEP-related
columns (e.g. `exons`, `cdna_seq`, `peptide_seq`, `translation_seq`, `cds_sequence`) are
not selected in a query, the parser skips extracting those fields, significantly reducing
parse overhead.

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

## Example: Parquet Export

Export any entity type to a single Parquet file:

```bash
cargo run --release --example storable_to_parquet -- \
  /path/to/homo_sapiens_merged/115_GRCh38 \
  /output/dir \
  transcript \
  8 \
  --chrom 22
```

Supported entity types: `transcript`, `exon`, `translation`, `regulatory`, `motif`, `variation`.
Output naming: `<output_dir>/115_GRCh38_transcript_22.parquet`.
