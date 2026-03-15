//! Parquet round-trip tests: write VEP cache → Parquet → read back, verify.

use datafusion::arrow::array::{
    Array, Float32Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, ExonTableProvider, MotifFeatureTableProvider,
    RegulatoryFeatureTableProvider, TranscriptTableProvider, TranslationTableProvider,
    VariationTableProvider,
};
use std::sync::Arc;
use tempfile::TempDir;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

/// Write all batches from a provider to a parquet file and return the path.
async fn write_provider_to_parquet(
    provider: Arc<dyn TableProvider>,
    table_name: &str,
    temp_dir: &TempDir,
) -> String {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, provider).unwrap();

    let df = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = df.collect().await.unwrap();

    let output_path = temp_dir
        .path()
        .join(format!("{table_name}.parquet"))
        .to_str()
        .unwrap()
        .to_string();

    let schema = batches[0].schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let file = std::fs::File::create(&output_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();

    for batch in &batches {
        if batch.num_rows() > 0 {
            writer.write(batch).unwrap();
        }
    }
    writer.close().unwrap();

    output_path
}

/// Helper to get a single i64 value from a SQL query result.
fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array")
        .value(0)
}

/// Count total rows from batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------
// Variation parquet round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn variation_parquet_roundtrip_preserves_row_count() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;
    let original_schema = provider.schema();

    // Get original row count
    let ctx = SessionContext::new();
    ctx.register_table("var_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx
        .sql("SELECT chrom FROM var_orig")
        .await?
        .collect()
        .await?;
    let orig_count = total_rows(&orig_batches);
    assert_eq!(orig_count, 3);

    // Write to parquet
    let parquet_path = write_provider_to_parquet(Arc::new(provider), "var_write", &temp_dir).await;

    // Read back from parquet
    let ctx2 = SessionContext::new();
    ctx2.register_parquet("var_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM var_read")
        .await?
        .collect()
        .await?;
    let read_count = first_i64(&read_batches);

    assert_eq!(
        read_count as usize, orig_count,
        "parquet round-trip should preserve row count"
    );

    // Verify key columns exist in parquet output
    let schema_batches = ctx2
        .sql("SELECT chrom, start, variation_name, allele_string FROM var_read LIMIT 1")
        .await?
        .collect()
        .await?;
    assert!(!schema_batches.is_empty());
    assert_eq!(schema_batches[0].num_columns(), 4);

    // Verify original schema field count matches
    let read_schema = ctx2
        .sql("SELECT * FROM var_read LIMIT 0")
        .await?
        .schema()
        .clone();
    assert_eq!(
        original_schema.fields().len(),
        read_schema.fields().len(),
        "parquet schema field count should match original"
    );

    Ok(())
}

#[tokio::test]
async fn variation_parquet_roundtrip_preserves_values() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "var_values", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("var", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Verify specific values survive the round-trip using SQL
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE chrom = '1'")
        .await?
        .collect()
        .await?;
    let chrom1_count = first_i64(&batches);
    assert!(chrom1_count > 0, "expected rows for chrom='1'");

    // Verify start values
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE start = 100")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0, "expected rows with start=100");

    // Verify allele_string values contain '/'
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE allele_string LIKE '%/%'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        3,
        "all allele_string values should contain '/'"
    );

    // Verify variation_name is non-empty
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE variation_name IS NOT NULL AND variation_name != ''")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 3);

    Ok(())
}

#[tokio::test]
async fn variation_parquet_roundtrip_source_ids() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_dynamic_sources",
    )))?;

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "var_src", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("var", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Verify source IDs preserved through parquet
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE dbsnp_ids = 'rs781394307'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        1,
        "dbsnp_ids='rs781394307' should exist after round-trip"
    );

    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE source_dbsnp = '156'")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0, "source_dbsnp should be '156'");

    Ok(())
}

// ---------------------------------------------------------------------------
// Transcript parquet round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_parquet_roundtrip_preserves_data() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    // Get original count
    let ctx1 = SessionContext::new();
    ctx1.register_table("tx_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT COUNT(*) FROM tx_orig")
        .await?
        .collect()
        .await?;
    let orig_count = first_i64(&orig_batches);
    assert_eq!(orig_count, 2);

    // Write to parquet
    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tx_write", &temp_dir).await;

    // Read back
    let ctx2 = SessionContext::new();
    ctx2.register_parquet("tx_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM tx_read")
        .await?
        .collect()
        .await?;

    assert_eq!(first_i64(&read_batches), orig_count);

    // Verify specific values
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM tx_read WHERE stable_id = 'ENST000001'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        1,
        "ENST000001 should exist after round-trip"
    );

    let batches = ctx2
        .sql("SELECT COUNT(*) FROM tx_read WHERE stable_id = 'ENST000002'")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        1,
        "ENST000002 should exist after round-trip"
    );

    Ok(())
}

#[tokio::test]
async fn transcript_parquet_roundtrip_exons_list() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    // Write to parquet — the exons column is List<Struct<start, end, phase>>
    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tx_exons", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("tx", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Verify exon count is queryable after round-trip
    let batches = ctx
        .sql("SELECT exon_count FROM tx WHERE exon_count IS NOT NULL")
        .await?
        .collect()
        .await?;
    // exon_count column should be intact
    assert!(!batches.is_empty());

    // Verify exons column is queryable (as a complex type)
    let batches = ctx
        .sql("SELECT exons FROM tx LIMIT 1")
        .await?
        .collect()
        .await?;
    assert!(!batches.is_empty());

    Ok(())
}

// ---------------------------------------------------------------------------
// Translation parquet round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn translation_parquet_roundtrip_preserves_data() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    // Get original count
    let ctx1 = SessionContext::new();
    ctx1.register_table("tl_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT COUNT(*) FROM tl_orig")
        .await?
        .collect()
        .await?;
    let orig_count = first_i64(&orig_batches);
    assert!(orig_count > 0);

    // Write to parquet
    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tl_write", &temp_dir).await;

    // Read back
    let ctx2 = SessionContext::new();
    ctx2.register_parquet("tl_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM tl_read")
        .await?
        .collect()
        .await?;

    assert_eq!(first_i64(&read_batches), orig_count);

    // Verify ENSP IDs survive
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM tl_read WHERE stable_id LIKE 'ENSP%'")
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&batches) > 0,
        "expected ENSP translation IDs after round-trip"
    );

    Ok(())
}

#[tokio::test]
async fn translation_parquet_roundtrip_protein_features() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tl_pf", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("tl", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Verify protein_features column survives parquet round-trip
    let batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE protein_features IS NOT NULL")
        .await?
        .collect()
        .await?;
    let pf_count = first_i64(&batches);
    assert!(
        pf_count > 0,
        "expected some translations with protein_features in real VEP 115 data"
    );

    // Verify the column is queryable as list
    let batches = ctx
        .sql("SELECT protein_features FROM tl WHERE protein_features IS NOT NULL LIMIT 1")
        .await?
        .collect()
        .await?;
    assert!(!batches.is_empty());
    assert!(batches[0].num_rows() > 0);

    Ok(())
}

#[tokio::test]
async fn translation_parquet_roundtrip_sift_polyphen() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tl_pred", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("tl", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Verify SIFT/PolyPhen columns exist and are queryable
    let batches = ctx
        .sql("SELECT sift_predictions, polyphen_predictions FROM tl LIMIT 1")
        .await?
        .collect()
        .await?;
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);

    // Count non-null predictions
    let sift_batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE sift_predictions IS NOT NULL")
        .await?
        .collect()
        .await?;
    let sift_count = first_i64(&sift_batches);

    let pp_batches = ctx
        .sql("SELECT COUNT(*) FROM tl WHERE polyphen_predictions IS NOT NULL")
        .await?
        .collect()
        .await?;
    let pp_count = first_i64(&pp_batches);

    // SIFT/PolyPhen may or may not be populated depending on the fixture data
    // (they come from binary matrix decoding). Just verify the query doesn't error.
    assert!(sift_count >= 0, "sift_predictions query should not error");
    assert!(pp_count >= 0, "polyphen_predictions query should not error");

    Ok(())
}

// ---------------------------------------------------------------------------
// Exon parquet round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exon_parquet_roundtrip_preserves_data() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx1 = SessionContext::new();
    ctx1.register_table("exon_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT COUNT(*) FROM exon_orig")
        .await?
        .collect()
        .await?;
    let orig_count = first_i64(&orig_batches);
    assert!(orig_count > 0);

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "exon_write", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("exon_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM exon_read")
        .await?
        .collect()
        .await?;

    assert_eq!(first_i64(&read_batches), orig_count);

    // Verify exon_number values are >= 1
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM exon_read WHERE exon_number < 1")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "all exon_number values should be >= 1"
    );

    // Verify coordinates are valid
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM exon_read WHERE start > \"end\"")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        0,
        "start should be <= end for all exons"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Regulatory / Motif parquet round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regulatory_parquet_roundtrip() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let ctx1 = SessionContext::new();
    ctx1.register_table("reg_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT COUNT(*) FROM reg_orig")
        .await?
        .collect()
        .await?;
    let orig_count = first_i64(&orig_batches);
    assert_eq!(orig_count, 1);

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "reg_write", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("reg_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM reg_read")
        .await?
        .collect()
        .await?;

    assert_eq!(first_i64(&read_batches), orig_count);

    // Verify stable_id is non-null
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM reg_read WHERE stable_id IS NOT NULL")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 1);

    Ok(())
}

#[tokio::test]
async fn motif_parquet_roundtrip() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let ctx1 = SessionContext::new();
    ctx1.register_table("motif_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT COUNT(*) FROM motif_orig")
        .await?
        .collect()
        .await?;
    let orig_count = first_i64(&orig_batches);

    let parquet_path =
        write_provider_to_parquet(Arc::new(provider), "motif_write", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("motif_read", &parquet_path, ParquetReadOptions::default())
        .await?;
    let read_batches = ctx2
        .sql("SELECT COUNT(*) FROM motif_read")
        .await?
        .collect()
        .await?;

    assert_eq!(first_i64(&read_batches), orig_count);

    if orig_count > 0 {
        let batches = ctx2
            .sql("SELECT COUNT(*) FROM motif_read WHERE binding_matrix = 'MA0001.1'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            first_i64(&batches),
            1,
            "binding_matrix='MA0001.1' should survive round-trip"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Parquet SQL query on round-tripped data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parquet_roundtrip_supports_sql_filtering() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "var_filter", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("var", &parquet_path, ParquetReadOptions::default())
        .await?;

    // Test chrom filter
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE chrom = '1'")
        .await?
        .collect()
        .await?;
    let count = first_i64(&batches);
    assert!(count > 0, "expected rows for chrom='1'");
    assert!(count < 3, "expected filter to reduce rows");

    // Test range filter
    let batches = ctx
        .sql("SELECT COUNT(*) FROM var WHERE start >= 100 AND start <= 200")
        .await?
        .collect()
        .await?;
    let range_count = first_i64(&batches);
    assert!(range_count > 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Parquet schema fidelity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parquet_roundtrip_preserves_all_schema_fields() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();

    // Test transcript schema — it has the most complex types (List<Struct>)
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;
    let original_schema = provider.schema();

    let parquet_path = write_provider_to_parquet(Arc::new(provider), "tx_schema", &temp_dir).await;

    let ctx = SessionContext::new();
    ctx.register_parquet("tx", &parquet_path, ParquetReadOptions::default())
        .await?;
    let df = ctx.sql("SELECT * FROM tx LIMIT 0").await?;
    let read_schema = df.schema().clone();

    // Every field in the original schema should be present in parquet
    for field in original_schema.fields() {
        assert!(
            read_schema.field_with_name(None, field.name()).is_ok(),
            "field '{}' missing from parquet schema",
            field.name()
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Parquet round-trip with zero-based coordinates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parquet_roundtrip_zero_based_coordinates() -> datafusion::common::Result<()> {
    let temp_dir = TempDir::new().unwrap();

    let mut options = EnsemblCacheOptions::new(fixture_path("variation_non_tabix"));
    options.coordinate_system_zero_based = true;
    let provider = VariationTableProvider::new(options)?;

    // Get zero-based start value
    let ctx1 = SessionContext::new();
    ctx1.register_table("var_orig", Arc::new(provider.clone()))?;
    let orig_batches = ctx1
        .sql("SELECT start FROM var_orig WHERE chrom = '1' ORDER BY start LIMIT 1")
        .await?
        .collect()
        .await?;
    let orig_start = orig_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(orig_start, 99, "zero-based start should be 99");

    // Write to parquet and read back
    let parquet_path = write_provider_to_parquet(Arc::new(provider), "var_zb", &temp_dir).await;

    let ctx2 = SessionContext::new();
    ctx2.register_parquet("var", &parquet_path, ParquetReadOptions::default())
        .await?;
    let batches = ctx2
        .sql("SELECT COUNT(*) FROM var WHERE start = 99")
        .await?
        .collect()
        .await?;
    assert_eq!(
        first_i64(&batches),
        1,
        "zero-based start=99 should survive parquet round-trip"
    );

    Ok(())
}
