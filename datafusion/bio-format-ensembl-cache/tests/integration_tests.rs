use datafusion::arrow::array::{
    Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::test_utils::find_leaf_exec;
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, MotifFeatureTableProvider, RegulatoryFeatureTableProvider,
    TranscriptTableProvider, VariationTableProvider,
};
use std::sync::Arc;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn first_i64(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> i64 {
    first_i64_at(batches, 0)
}

fn first_i64_at(batches: &[datafusion::arrow::record_batch::RecordBatch], column: usize) -> i64 {
    let array = batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array");
    array.value(0)
}

fn first_string(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    column: usize,
) -> Option<String> {
    let array = batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected StringArray");
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0).to_string())
    }
}

fn session_ctx_with_target_partitions(target_partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    SessionContext::new_with_config(config)
}

#[tokio::test]
async fn variation_non_tabix_streaming_query_works() -> datafusion::common::Result<()> {
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_non_tabix"));
    options.batch_size_hint = Some(1);

    let provider = VariationTableProvider::new(options)?;
    assert!(provider.schema().field_with_name("AFR").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("variation", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM variation")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 3);

    let batches = ctx
        .sql("SELECT COUNT(*) FROM variation WHERE chrom = '1' AND start >= 150")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 1);

    Ok(())
}

#[tokio::test]
async fn variation_parallel_row_count_invariant() -> datafusion::common::Result<()> {
    for partitions in [1usize, 2, 4, 8] {
        let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
            "variation_non_tabix",
        )))?;
        let ctx = session_ctx_with_target_partitions(partitions);
        ctx.register_table("variation", Arc::new(provider))?;

        let batches = ctx
            .sql("SELECT COUNT(*) FROM variation")
            .await?
            .collect()
            .await?;
        assert_eq!(
            first_i64(&batches),
            3,
            "count mismatch with target_partitions={partitions}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn variation_target_partitions_override_applied() -> datafusion::common::Result<()> {
    let ctx = session_ctx_with_target_partitions(8);

    let mut one_partition_options = EnsemblCacheOptions::new(fixture_path("variation_non_tabix"));
    one_partition_options.target_partitions = Some(1);
    let provider = VariationTableProvider::new(one_partition_options)?;
    ctx.register_table("variation_one", Arc::new(provider))?;
    let df = ctx.sql("SELECT chrom, start FROM variation_one").await?;
    let plan = df.create_physical_plan().await?;
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "EnsemblCacheExec");
    assert_eq!(leaf.output_partitioning().partition_count(), 1);

    let mut many_partition_options = EnsemblCacheOptions::new(fixture_path("variation_non_tabix"));
    many_partition_options.target_partitions = Some(8);
    let provider = VariationTableProvider::new(many_partition_options)?;
    ctx.register_table("variation_many", Arc::new(provider))?;
    let df = ctx.sql("SELECT chrom, start FROM variation_many").await?;
    let plan = df.create_physical_plan().await?;
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "EnsemblCacheExec");
    // variation_non_tabix fixture has 2 source files, so partitions are capped at 2.
    assert_eq!(leaf.output_partitioning().partition_count(), 2);

    Ok(())
}

#[tokio::test]
async fn variation_tabix_mode_prefers_all_vars() -> datafusion::common::Result<()> {
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path("variation_tabix")))?;

    let ctx = SessionContext::new();
    ctx.register_table("variation", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT COUNT(*) FROM variation")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&batches), 2);

    Ok(())
}

#[tokio::test]
async fn variation_dynamic_source_schema_and_ids_work() -> datafusion::common::Result<()> {
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_dynamic_sources",
    )))?;

    assert!(provider.schema().field_with_name("source_dbsnp").is_ok());
    assert!(provider.schema().field_with_name("source_cosmic").is_ok());
    assert!(
        provider
            .schema()
            .field_with_name("source_hgmd_public")
            .is_ok()
    );
    assert!(provider.schema().field_with_name("source_clinvar").is_ok());
    assert!(provider.schema().field_with_name("source_foobar").is_ok());
    assert!(provider.schema().field_with_name("dbsnp_ids").is_ok());
    assert!(provider.schema().field_with_name("cosmic_ids").is_ok());
    assert!(provider.schema().field_with_name("hgmd_public_ids").is_ok());
    assert!(provider.schema().field_with_name("clinvar_ids").is_ok());
    assert!(provider.schema().field_with_name("foobar_ids").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("variation", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT dbsnp_ids, cosmic_ids, hgmd_public_ids, clinvar_ids, foobar_ids, \
             source_dbsnp, source_foobar FROM variation LIMIT 1",
        )
        .await?
        .collect()
        .await?;

    assert_eq!(first_string(&batches, 0).as_deref(), Some("rs781394307"));
    assert_eq!(first_string(&batches, 1).as_deref(), Some("COSM10665947"));
    assert_eq!(first_string(&batches, 2).as_deref(), Some("CM123456"));
    assert_eq!(
        first_string(&batches, 3).as_deref(),
        Some("VCV0001,RCV0002")
    );
    assert_eq!(first_string(&batches, 4).as_deref(), Some("FB1"));
    assert_eq!(first_string(&batches, 5).as_deref(), Some("156"));
    assert_eq!(first_string(&batches, 6).as_deref(), Some("v1"));

    Ok(())
}

#[tokio::test]
async fn variation_coordinate_system_metadata_and_values_work() -> datafusion::common::Result<()> {
    let one_based = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;
    assert_eq!(
        one_based
            .schema()
            .metadata()
            .get(COORDINATE_SYSTEM_METADATA_KEY),
        Some(&"false".to_string())
    );

    let ctx = SessionContext::new();
    ctx.register_table("variation", Arc::new(one_based))?;
    let one_based_rows = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64_at(&one_based_rows, 0), 100);

    let mut options = EnsemblCacheOptions::new(fixture_path("variation_non_tabix"));
    options.coordinate_system_zero_based = true;
    let zero_based = VariationTableProvider::new(options)?;
    assert_eq!(
        zero_based
            .schema()
            .metadata()
            .get(COORDINATE_SYSTEM_METADATA_KEY),
        Some(&"true".to_string())
    );

    let ctx = SessionContext::new();
    ctx.register_table("variation", Arc::new(zero_based))?;
    let zero_based_rows = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64_at(&zero_based_rows, 0), 99);

    let filtered = ctx
        .sql("SELECT COUNT(*) FROM variation WHERE chrom = '1' AND start = 99")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&filtered), 1);

    Ok(())
}

#[tokio::test]
async fn transcript_storable_query_works() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    assert_eq!(first_i64(&batches), 2);

    let batches = ctx
        .sql("SELECT stable_id FROM tx WHERE is_canonical = true")
        .await?
        .collect()
        .await?;
    let stable_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected StringArray");
    assert_eq!(stable_ids.value(0), "ENST000001");

    Ok(())
}

#[tokio::test]
async fn transcript_storable_streams_with_small_batch_size() -> datafusion::common::Result<()> {
    let mut options = EnsemblCacheOptions::new(fixture_path("transcript_storable"));
    options.batch_size_hint = Some(1);
    let provider = TranscriptTableProvider::new(options)?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT stable_id FROM tx").await?.collect().await?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();

    assert_eq!(total_rows, 2);
    assert!(
        batches.len() >= 2,
        "expected multiple batches with batch_size_hint=1, got {}",
        batches.len()
    );

    Ok(())
}

#[tokio::test]
async fn transcript_sereal_query_works() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("transcript_sereal")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    assert_eq!(first_i64(&batches), 1);

    Ok(())
}

#[tokio::test]
async fn transcript_coordinate_system_metadata_and_values_work() -> datafusion::common::Result<()> {
    let mut options = EnsemblCacheOptions::new(fixture_path("transcript_sereal"));
    options.coordinate_system_zero_based = true;
    let provider = TranscriptTableProvider::new(options)?;
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(COORDINATE_SYSTEM_METADATA_KEY),
        Some(&"true".to_string())
    );

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let rows = ctx
        .sql("SELECT start FROM tx WHERE stable_id = 'ENST000010'")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64_at(&rows, 0), 2999);

    let filtered = ctx
        .sql("SELECT COUNT(*) FROM tx WHERE start = 2999")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&filtered), 1);

    Ok(())
}

#[tokio::test]
async fn transcript_merged_layout_without_serializer_works() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_merged_layout",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    assert_eq!(first_i64(&batches), 2);

    Ok(())
}

#[tokio::test]
async fn regulatory_and_motif_storable_work() -> datafusion::common::Result<()> {
    let reg = RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;
    let motif = MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("reg", Arc::new(reg))?;
    ctx.register_table("motif", Arc::new(motif))?;

    let reg_batches = ctx.sql("SELECT COUNT(*) FROM reg").await?.collect().await?;
    assert_eq!(first_i64(&reg_batches), 1);

    let motif_batches = ctx
        .sql("SELECT binding_matrix FROM motif")
        .await?
        .collect()
        .await?;
    let values = motif_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected StringArray");
    assert_eq!(values.value(0), "MA0001.1");

    Ok(())
}

#[tokio::test]
async fn regulatory_and_motif_sereal_work() -> datafusion::common::Result<()> {
    let reg = RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_sereal",
    )))?;
    let motif = MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_sereal",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("reg", Arc::new(reg))?;
    ctx.register_table("motif", Arc::new(motif))?;

    let reg_batches = ctx.sql("SELECT COUNT(*) FROM reg").await?.collect().await?;
    assert_eq!(first_i64(&reg_batches), 1);

    let motif_batches = ctx
        .sql("SELECT COUNT(*) FROM motif")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64(&motif_batches), 1);

    Ok(())
}

#[tokio::test]
async fn regulatory_and_motif_coordinate_system_metadata_and_values_work()
-> datafusion::common::Result<()> {
    let mut reg_options = EnsemblCacheOptions::new(fixture_path("regulatory_sereal"));
    reg_options.coordinate_system_zero_based = true;
    let reg = RegulatoryFeatureTableProvider::new(reg_options)?;
    assert_eq!(
        reg.schema().metadata().get(COORDINATE_SYSTEM_METADATA_KEY),
        Some(&"true".to_string())
    );

    let mut motif_options = EnsemblCacheOptions::new(fixture_path("regulatory_sereal"));
    motif_options.coordinate_system_zero_based = true;
    let motif = MotifFeatureTableProvider::new(motif_options)?;
    assert_eq!(
        motif
            .schema()
            .metadata()
            .get(COORDINATE_SYSTEM_METADATA_KEY),
        Some(&"true".to_string())
    );

    let ctx = SessionContext::new();
    ctx.register_table("reg", Arc::new(reg))?;
    ctx.register_table("motif", Arc::new(motif))?;

    let reg_rows = ctx
        .sql("SELECT start FROM reg LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64_at(&reg_rows, 0), 6999);

    let motif_rows = ctx
        .sql("SELECT start FROM motif LIMIT 1")
        .await?
        .collect()
        .await?;
    assert_eq!(first_i64_at(&motif_rows, 0), 7049);

    Ok(())
}

// ---------------------------------------------------------------------------
// VEP column tests (Phase 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_schema_contains_vep_columns() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;
    let schema = provider.schema();

    // Verify new columns exist with correct types
    let exons_field = schema
        .field_with_name("exons")
        .expect("exons column missing");
    assert!(
        matches!(exons_field.data_type(), DataType::List(_)),
        "exons should be List type, got {:?}",
        exons_field.data_type()
    );
    assert!(exons_field.is_nullable());

    assert!(schema.field_with_name("cdna_seq").is_ok());
    assert!(schema.field_with_name("peptide_seq").is_ok());
    assert!(schema.field_with_name("codon_table").is_ok());
    assert!(schema.field_with_name("tsl").is_ok());
    assert!(schema.field_with_name("mane_select").is_ok());
    assert!(schema.field_with_name("mane_plus_clinical").is_ok());

    Ok(())
}

#[tokio::test]
async fn transcript_storable_exons_query_returns_structured_data() -> datafusion::common::Result<()>
{
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Query exons column — should return List<Struct<start, end, phase>>
    let batches = ctx
        .sql("SELECT stable_id, exons, exon_count FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert!(batch.num_rows() > 0);

    // The exons column should be a ListArray
    let exons_col = batch.column_by_name("exons").expect("exons column missing");
    let list_array = exons_col
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exons column should be ListArray");

    // Check any non-null exon lists have the right structure
    for row in 0..list_array.len() {
        if !list_array.is_null(row) {
            let exon_list = list_array.value(row);
            let struct_array = exon_list
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("exon list values should be StructArray");
            if struct_array.len() > 0 {
                // Verify struct fields exist
                assert!(struct_array.column_by_name("start").is_some());
                assert!(struct_array.column_by_name("end").is_some());
                assert!(struct_array.column_by_name("phase").is_some());

                // Verify exon_count matches
                let exon_count_col = batch
                    .column_by_name("exon_count")
                    .expect("exon_count column missing");
                let counts = exon_count_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("expected Int32Array");
                if !counts.is_null(row) {
                    assert_eq!(
                        counts.value(row) as usize,
                        struct_array.len(),
                        "exon_count should match exons array length"
                    );
                }
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_sereal_exons_query_returns_structured_data() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("transcript_sereal")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id, exons FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];

    // The exons column should exist and be queryable
    let exons_col = batch.column_by_name("exons").expect("exons column missing");
    assert_eq!(batch.num_rows(), 1);

    // Exon column should be a ListArray (may be null if fixture lacks exon data)
    let _list_array = exons_col
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("exons column should be ListArray");

    Ok(())
}

#[tokio::test]
async fn transcript_vep_sequences_queryable() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Query sequence columns — should not error
    let batches = ctx
        .sql("SELECT stable_id, cdna_seq, peptide_seq, codon_table FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert!(batch.num_rows() > 0);

    // Verify columns exist in output
    assert!(batch.column_by_name("cdna_seq").is_some());
    assert!(batch.column_by_name("peptide_seq").is_some());
    assert!(batch.column_by_name("codon_table").is_some());

    Ok(())
}

#[tokio::test]
async fn transcript_projection_pushdown_excludes_vep_columns() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Query only core columns — VEP columns should NOT appear in output
    let batches = ctx
        .sql("SELECT stable_id, chrom, start FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 3);
    assert!(batch.column_by_name("exons").is_none());
    assert!(batch.column_by_name("cdna_seq").is_none());
    assert!(batch.column_by_name("peptide_seq").is_none());

    Ok(())
}

#[tokio::test]
async fn transcript_backward_compat_existing_queries_work() -> datafusion::common::Result<()> {
    // Ensure existing queries from before the VEP columns were added still produce
    // the same results.
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    assert_eq!(first_i64(&batches), 2);

    let batches = ctx
        .sql("SELECT stable_id FROM tx WHERE is_canonical = true")
        .await?
        .collect()
        .await?;
    let stable_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected StringArray");
    assert_eq!(stable_ids.value(0), "ENST000001");

    Ok(())
}
