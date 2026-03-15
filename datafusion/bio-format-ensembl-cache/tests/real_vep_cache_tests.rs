//! Integration tests using real VEP 115 GRCh38 cache files (chr22:15M-16M region).
//!
//! These tests exercise the full pipeline against actual Ensembl VEP cache data
//! (storable binary format) rather than synthetic fixtures, covering:
//! - Real Perl Storable binary deserialization (nstore format)
//! - Real transcript, exon, translation, regulatory, and motif parsing
//! - Real variation TSV parsing with gnomAD/1000G population frequencies
//! - Parquet generation from real data
//! - Cross-entity consistency with real IDs (ENST/ENSP/ENSE)

use datafusion::arrow::array::{
    Array, Float32Array, Int32Array, Int64Array, ListArray, StructArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind, ExonTableProvider,
    MotifFeatureTableProvider, RegulatoryFeatureTableProvider, TranscriptTableProvider,
    TranslationTableProvider, VariationTableProvider,
};
use std::sync::Arc;
use tempfile::TempDir;

const REAL_FIXTURE: &str = "real_vep_115_chr22";

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array")
        .value(0)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn write_to_parquet(
    provider: Arc<dyn TableProvider>,
    table_name: &str,
    temp_dir: &TempDir,
) -> String {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, provider).unwrap();
    let batches: Vec<RecordBatch> = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let path = temp_dir
        .path()
        .join(format!("{table_name}.parquet"))
        .to_str()
        .unwrap()
        .to_string();
    let schema = batches[0].schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    for batch in &batches {
        if batch.num_rows() > 0 {
            writer.write(batch).unwrap();
        }
    }
    writer.close().unwrap();
    path
}

// ===========================================================================
// Transcript tests — real Perl Storable binary
// ===========================================================================

#[tokio::test]
async fn real_transcript_storable_produces_rows() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    let count = first_i64(&batches);
    assert!(
        count > 10,
        "expected many transcripts from real VEP chr22 region, got {count}"
    );

    Ok(())
}

#[tokio::test]
async fn real_transcript_has_ensembl_ids() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Should have ENST-prefixed transcript IDs
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE stable_id LIKE 'ENST%'")
        .await?
        .collect()
        .await?;
    let enst_count = first_i64(&batches);
    assert!(
        enst_count > 0,
        "expected Ensembl transcript IDs (ENST*), got {enst_count}"
    );

    // Should have ENSG-prefixed gene IDs
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE gene_stable_id LIKE 'ENSG%'")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0, "expected Ensembl gene IDs (ENSG*)");

    Ok(())
}

#[tokio::test]
async fn real_transcript_biotypes_present() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // protein_coding should be present in real data
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE biotype = 'protein_coding'")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected protein_coding transcripts in real VEP data"
    );

    // LOC-prefixed pseudo-records should be filtered out
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE stable_id LIKE 'LOC%'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "LOC pseudo-records should be excluded"
    );

    Ok(())
}

#[tokio::test]
async fn real_transcript_coordinates_valid() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // All coordinates should be valid (start <= end, chrom = '22')
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE start > \"end\"")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "start should be <= end for all transcripts"
    );

    // Chrom should be '22' for this fixture
    let batches = ctx
        .sql("SELECT COUNT(DISTINCT chrom) FROM tx")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        1,
        "expected single chromosome in fixture"
    );

    Ok(())
}

#[tokio::test]
async fn real_transcript_coding_features() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Coding transcripts should have cdna_coding_start/end
    let batches = ctx
        .sql(
            "SELECT COUNT(*) FROM tx \
             WHERE biotype = 'protein_coding' AND cdna_coding_start IS NOT NULL",
        )
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "coding transcripts should have cdna_coding_start"
    );

    // Coding region should be within transcript span
    let batches = ctx
        .sql(
            "SELECT COUNT(*) FROM tx \
             WHERE cds_start IS NOT NULL AND cds_end IS NOT NULL \
             AND cds_start <= cds_end \
             AND cds_start >= start AND cds_end <= \"end\"",
        )
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "coding region should fall within transcript span"
    );

    Ok(())
}

#[tokio::test]
async fn real_transcript_vep_annotations() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // MANE Select annotations should be present for some transcripts
    let _batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE mane_select IS NOT NULL")
        .await?
        .collect()
        .await?;
    // May or may not have MANE in this region — just verify it doesn't error

    // APPRIS annotations
    let _batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE appris IS NOT NULL")
        .await?
        .collect()
        .await?;

    // Gene symbols should be populated
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE gene_symbol IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected gene_symbol to be populated in real data"
    );

    // is_canonical should have some true values
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE is_canonical = true")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected some canonical transcripts"
    );

    Ok(())
}

#[tokio::test]
async fn real_transcript_exon_lists() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT exons, exon_count FROM tx WHERE exon_count IS NOT NULL AND exon_count > 0 LIMIT 5")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert!(batch.num_rows() > 0);

    let exons_col = batch
        .column_by_name("exons")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exons should be ListArray");

    let exon_counts = batch
        .column_by_name("exon_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("exon_count should be Int32Array");

    for row in 0..batch.num_rows() {
        if !exons_col.is_null(row) && !exon_counts.is_null(row) {
            let exon_list = exons_col.value(row);
            let struct_array = exon_list
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("exon list should be StructArray");

            assert_eq!(
                struct_array.len(),
                exon_counts.value(row) as usize,
                "exon list length should match exon_count"
            );

            // Verify struct fields
            assert!(struct_array.column_by_name("start").is_some());
            assert!(struct_array.column_by_name("end").is_some());
            assert!(struct_array.column_by_name("phase").is_some());
        }
    }

    Ok(())
}

// ===========================================================================
// Exon tests — real data
// ===========================================================================

#[tokio::test]
async fn real_exon_storable_produces_rows() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM exons")
        .await?
        .collect()
        .await?;
    let count = first_i64(&batches);
    assert!(count > 10, "expected many exons, got {count}");

    // All exon IDs should be ENSE-prefixed (Ensembl) or exon-* (RefSeq)
    let batches = ctx
        .sql(
            "SELECT COUNT(*) FROM exons WHERE stable_id IS NOT NULL \
             AND stable_id NOT LIKE 'ENSE%' AND stable_id NOT LIKE 'exon-%' \
             AND stable_id NOT LIKE 'id-%'",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "all exon IDs should be ENSE*/exon-*/id-* patterns"
    );

    Ok(())
}

#[tokio::test]
async fn real_exon_coordinates_valid() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM exons WHERE start > \"end\"")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 0, "exon start should be <= end");

    let batches = ctx
        .sql("SELECT COUNT(*) FROM exons WHERE exon_number < 1")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 0, "exon_number should be >= 1");

    Ok(())
}

#[tokio::test]
async fn real_exon_phases() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    // Phase values should be -1, 0, 1, or 2 (or null)
    let batches = ctx
        .sql(
            "SELECT COUNT(*) FROM exons \
             WHERE phase IS NOT NULL AND phase NOT IN (-1, 0, 1, 2)",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 0, "phase should be -1, 0, 1, or 2");

    Ok(())
}

// ===========================================================================
// Translation tests — real data
// ===========================================================================

#[tokio::test]
async fn real_translation_produces_rows() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tl").await?.collect().await?;
    let count = first_i64(&batches);
    assert!(count > 0, "expected translations from real data");

    // Translation IDs should start with ENSP
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE stable_id LIKE 'ENSP%'")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0, "expected ENSP translation IDs");

    Ok(())
}

#[tokio::test]
async fn real_translation_protein_features() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE protein_features IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected some translations with protein_features in real data"
    );

    Ok(())
}

#[tokio::test]
async fn real_translation_sift_polyphen_decoded() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    // SIFT predictions should be decoded from binary matrix in real cache
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE sift_predictions IS NOT NULL")
        .await?
        .collect()
        .await?;
    let sift_count = first_i64(&batches);
    assert!(
        sift_count > 0,
        "expected SIFT predictions decoded from real VEP cache binary matrices"
    );

    // PolyPhen predictions
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE polyphen_predictions IS NOT NULL")
        .await?
        .collect()
        .await?;
    let pp_count = first_i64(&batches);
    assert!(
        pp_count > 0,
        "expected PolyPhen predictions decoded from real VEP cache"
    );

    // Verify prediction structure on first non-null row
    let batches = ctx
        .sql("SELECT sift_predictions FROM tl WHERE sift_predictions IS NOT NULL LIMIT 1")
        .await?
        .collect()
        .await?;

    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let entries = list.value(0);
    let struct_arr = entries.as_any().downcast_ref::<StructArray>().unwrap();

    assert!(
        struct_arr.len() > 0,
        "SIFT prediction list should not be empty"
    );

    // Verify scores are in [0,1] range
    let scores = struct_arr
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    for i in 0..scores.len() {
        let s = scores.value(i);
        assert!(
            (0.0..=1.0).contains(&s),
            "SIFT score {s} out of [0,1] range"
        );
    }

    Ok(())
}

#[tokio::test]
async fn real_translation_sequences() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    // translation_seq should be populated for some translations
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE translation_seq IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected translation_seq populated in real data"
    );

    // cds_len should be derived correctly (cdna_coding_end - cdna_coding_start + 1)
    let batches = ctx
        .sql(
            "SELECT COUNT(*) FROM tl \
             WHERE cdna_coding_start IS NOT NULL AND cdna_coding_end IS NOT NULL \
             AND cds_len != (cdna_coding_end - cdna_coding_start + 1)",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "cds_len should equal cdna_coding_end - cdna_coding_start + 1"
    );

    Ok(())
}

// ===========================================================================
// Regulatory and motif tests — real data
// ===========================================================================

#[tokio::test]
async fn real_regulatory_storable_produces_rows() -> datafusion::common::Result<()> {
    let provider =
        RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("reg", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM reg").await?.collect().await?;
    let count = first_i64(&batches);
    assert!(count > 0, "expected regulatory features from real data");

    // Verify stable_id pattern (ENSR*)
    let batches = ctx
        .sql("SELECT COUNT(*) FROM reg WHERE stable_id LIKE 'ENSR%'")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected ENSR regulatory feature IDs"
    );

    // Verify feature_type is populated
    let batches = ctx
        .sql("SELECT COUNT(*) FROM reg WHERE feature_type IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0, "expected feature_type populated");

    Ok(())
}

#[tokio::test]
async fn real_motif_storable_queryable() -> datafusion::common::Result<()> {
    let provider =
        MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("motif", Arc::new(provider))?;

    // Motif features may or may not be present in this specific 1M region.
    // The key test is that the query doesn't error.
    let batches = ctx
        .sql("SELECT COUNT(*) FROM motif")
        .await?
        .collect()
        .await?;
    let count = first_i64(&batches);
    // Count >= 0 is fine — some regions have no motif features
    assert!(count >= 0);

    Ok(())
}

// ===========================================================================
// Variation tests — real TSV data
// ===========================================================================

#[tokio::test]
async fn real_variation_produces_rows() -> datafusion::common::Result<()> {
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM var").await?.collect().await?;
    assert_eq!(
        first_i64(&batches),
        100,
        "fixture should contain exactly 100 variation rows"
    );

    Ok(())
}

#[tokio::test]
async fn real_variation_has_rsids() -> datafusion::common::Result<()> {
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE variation_name LIKE 'rs%'")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected rs-prefixed variant IDs in real data"
    );

    // dbsnp_ids should be populated from var_synonyms/pattern matching
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE dbsnp_ids IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected dbsnp_ids populated via source ID extraction"
    );

    Ok(())
}

#[tokio::test]
async fn real_variation_gnomad_frequencies() -> datafusion::common::Result<()> {
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    // Schema should include gnomAD columns
    let schema = provider.schema();
    assert!(
        schema.field_with_name("gnomADe").is_ok(),
        "gnomADe column missing"
    );
    assert!(
        schema.field_with_name("gnomADg").is_ok(),
        "gnomADg column missing"
    );
    assert!(
        schema.field_with_name("gnomADe_AFR").is_ok(),
        "gnomADe_AFR column missing"
    );

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    // Some variants should have gnomAD frequencies
    let _batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE \"gnomADg\" IS NOT NULL")
        .await?
        .collect()
        .await?;
    // May or may not have frequencies in this subset — just verify no error

    Ok(())
}

#[tokio::test]
async fn real_variation_source_provenance() -> datafusion::common::Result<()> {
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    // Schema should have source columns from info.txt
    let schema = provider.schema();
    assert!(schema.field_with_name("source_dbsnp").is_ok());
    assert!(schema.field_with_name("source_cosmic").is_ok());
    assert!(schema.field_with_name("source_clinvar").is_ok());
    assert!(schema.field_with_name("source_hgmd_public").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    // source_dbsnp should be '156' (from info.txt)
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE source_dbsnp = '156'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        100,
        "all rows should have source_dbsnp='156'"
    );

    Ok(())
}

// ===========================================================================
// Cross-entity consistency — real data
// ===========================================================================

#[tokio::test]
async fn real_cross_entity_translation_transcript_join() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;
    let tl_provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("tl", Arc::new(tl_provider))?;

    // Every translation's transcript_id should match a transcript stable_id
    let batches = ctx
        .sql(
            "SELECT tl.transcript_id FROM tl \
             LEFT JOIN tx ON tl.transcript_id = tx.stable_id \
             WHERE tx.stable_id IS NULL",
        )
        .await?
        .collect()
        .await?;

    let orphans = total_rows(&batches);
    assert_eq!(
        orphans, 0,
        "all translation transcript_ids should exist in transcript table"
    );

    // Translation count should be <= transcript count
    let tx_count_batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    let tl_count_batches = ctx.sql("SELECT COUNT(*) FROM tl").await?.collect().await?;
    assert!(
        first_i64(&tl_count_batches) <= first_i64(&tx_count_batches),
        "translations should be <= transcripts"
    );

    Ok(())
}

#[tokio::test]
async fn real_cross_entity_exon_transcript_join() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;
    let exon_provider =
        ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("exons", Arc::new(exon_provider))?;

    // Every exon's transcript_id should match a transcript stable_id
    let batches = ctx
        .sql(
            "SELECT exons.transcript_id FROM exons \
             LEFT JOIN tx ON exons.transcript_id = tx.stable_id \
             WHERE tx.stable_id IS NULL",
        )
        .await?
        .collect()
        .await?;

    let orphans = total_rows(&batches);
    assert_eq!(
        orphans, 0,
        "all exon transcript_ids should exist in transcript table"
    );

    Ok(())
}

#[tokio::test]
async fn real_cross_entity_exon_count_consistency() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;
    let exon_provider =
        ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("exons", Arc::new(exon_provider))?;

    // For each transcript, exon_count should match actual exon row count
    let batches = ctx
        .sql(
            "SELECT tx.stable_id, tx.exon_count, e.actual_count \
             FROM tx \
             JOIN (SELECT transcript_id, COUNT(*) AS actual_count FROM exons GROUP BY transcript_id) e \
               ON tx.stable_id = e.transcript_id \
             WHERE tx.exon_count IS NOT NULL AND tx.exon_count != e.actual_count",
        )
        .await?
        .collect()
        .await?;

    let mismatches = total_rows(&batches);
    assert_eq!(
        mismatches, 0,
        "transcript exon_count should match actual exon row count"
    );

    Ok(())
}

// ===========================================================================
// Parquet generation from real data
// ===========================================================================

#[tokio::test]
async fn real_transcript_parquet_roundtrip() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    // Get original count
    let ctx1 = SessionContext::new();
    ctx1.register_table("tx", Arc::new(provider.clone()))?;
    let orig_count = first_i64(&ctx1.sql("SELECT COUNT(*) FROM tx").await?.collect().await?);
    assert!(orig_count > 0);

    // Write to parquet and read back
    let path = write_to_parquet(Arc::new(provider), "tx", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("tx_pq", &path, ParquetReadOptions::default())
        .await?;

    let read_count = first_i64(
        &ctx2
            .sql("SELECT COUNT(*) FROM tx_pq")
            .await?
            .collect()
            .await?,
    );
    assert_eq!(read_count, orig_count, "parquet round-trip count mismatch");

    // Verify ENST IDs survived
    let enst_count = first_i64(
        &ctx2
            .sql("SELECT COUNT(*) FROM tx_pq WHERE stable_id LIKE 'ENST%'")
            .await?
            .collect()
            .await?,
    );
    assert!(enst_count > 0);

    Ok(())
}

#[tokio::test]
async fn real_translation_parquet_roundtrip() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let ctx1 = SessionContext::new();
    ctx1.register_table("tl", Arc::new(provider.clone()))?;
    let orig_count = first_i64(&ctx1.sql("SELECT COUNT(*) FROM tl").await?.collect().await?);

    let path = write_to_parquet(Arc::new(provider), "tl", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("tl_pq", &path, ParquetReadOptions::default())
        .await?;

    let read_count = first_i64(
        &ctx2
            .sql("SELECT COUNT(*) FROM tl_pq")
            .await?
            .collect()
            .await?,
    );
    assert_eq!(read_count, orig_count);

    // Verify protein_features survived parquet
    let pf_count = first_i64(
        &ctx2
            .sql("SELECT COUNT(*) FROM tl_pq WHERE protein_features IS NOT NULL")
            .await?
            .collect()
            .await?,
    );
    assert!(pf_count > 0, "protein_features should survive parquet");

    // Verify SIFT predictions survived parquet
    let sift_count = first_i64(
        &ctx2
            .sql("SELECT COUNT(*) FROM tl_pq WHERE sift_predictions IS NOT NULL")
            .await?
            .collect()
            .await?,
    );
    assert!(sift_count > 0, "SIFT predictions should survive parquet");

    Ok(())
}

#[tokio::test]
async fn real_variation_parquet_roundtrip() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;

    let path = write_to_parquet(Arc::new(provider), "var", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("var_pq", &path, ParquetReadOptions::default())
        .await?;

    let count = first_i64(
        &ctx.sql("SELECT COUNT(*) FROM var_pq")
            .await?
            .collect()
            .await?,
    );
    assert_eq!(count, 100);

    // Verify source provenance survived
    let src_count = first_i64(
        &ctx.sql("SELECT COUNT(*) FROM var_pq WHERE source_dbsnp = '156'")
            .await?
            .collect()
            .await?,
    );
    assert_eq!(src_count, 100);

    Ok(())
}

// ===========================================================================
// Parallel partition tests — real data
// ===========================================================================

#[tokio::test]
async fn real_transcript_parallel_invariant() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)))?;
    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;
    let base_count = first_i64(&ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?);

    for partitions in [1usize, 2, 4] {
        let mut options = EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE));
        options.max_storable_partitions = Some(partitions);
        let provider = TranscriptTableProvider::new(options)?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(partitions),
        );
        ctx.register_table("tx", Arc::new(provider))?;
        let count = first_i64(&ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?);
        assert_eq!(
            count, base_count,
            "transcript count mismatch with partitions={partitions}"
        );
    }

    Ok(())
}

// ===========================================================================
// EnsemblCacheTableProvider factory — real data
// ===========================================================================

#[tokio::test]
async fn real_factory_all_entities() -> datafusion::common::Result<()> {
    let entities = [
        (EnsemblEntityKind::Transcript, "tx"),
        (EnsemblEntityKind::Exon, "exon"),
        (EnsemblEntityKind::Translation, "tl"),
        (EnsemblEntityKind::RegulatoryFeature, "reg"),
        (EnsemblEntityKind::MotifFeature, "motif"),
        (EnsemblEntityKind::Variation, "var"),
    ];

    for (kind, name) in entities {
        let provider = EnsemblCacheTableProvider::for_entity(
            kind,
            EnsemblCacheOptions::new(fixture_path(REAL_FIXTURE)),
        )?;

        let ctx = SessionContext::new();
        ctx.register_table(name, provider)?;

        let batches = ctx
            .sql(&format!("SELECT COUNT(*) FROM {name}"))
            .await?
            .collect()
            .await?;
        let count = first_i64(&batches);
        // MotifFeature may have 0 rows in some 1M regions
        if kind != EnsemblEntityKind::MotifFeature {
            assert!(
                count > 0,
                "expected rows for {kind:?} from real VEP cache, got {count}"
            );
        }
    }

    Ok(())
}
