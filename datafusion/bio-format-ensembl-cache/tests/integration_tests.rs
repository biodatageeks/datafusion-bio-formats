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
    EnsemblCacheOptions, ExonTableProvider, MotifFeatureTableProvider,
    RegulatoryFeatureTableProvider, TranscriptTableProvider, TranslationTableProvider,
    VariationTableProvider,
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

// ---------------------------------------------------------------------------
// Coding region (CDS) column tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_coding_region_populated_for_protein_coding() -> datafusion::common::Result<()> {
    // Text fixture: ENST000001 is protein_coding with cds_start=1010, cds_end=1090
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT stable_id, cds_start, cds_end \
             FROM tx WHERE stable_id = 'ENST000001'",
        )
        .await?
        .collect()
        .await?;

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(first_i64_at(&batches, 1), 1010);
    assert_eq!(first_i64_at(&batches, 2), 1090);

    Ok(())
}

#[tokio::test]
async fn transcript_coding_region_null_for_non_coding() -> datafusion::common::Result<()> {
    // Text fixture: ENST000002 is lncRNA — cds_start/end should be null
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT cds_start, cds_end \
             FROM tx WHERE stable_id = 'ENST000002'",
        )
        .await?
        .collect()
        .await?;

    assert_eq!(batches[0].num_rows(), 1);
    let crs = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let cre = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(crs.is_null(0), "cds_start should be null for lncRNA");
    assert!(cre.is_null(0), "cds_end should be null for lncRNA");

    Ok(())
}

#[tokio::test]
async fn transcript_coding_region_derived_in_storable_binary() -> datafusion::common::Result<()> {
    // VEP storable binary stores cds_start/end as undef, but the
    // parser derives them from cdna_coding_start/end + exon array.
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Coding transcripts (those with cdna_coding_start) should have derived values.
    let batches = ctx
        .sql(
            "SELECT cds_start, cds_end, start, \"end\" \
             FROM tx WHERE cdna_coding_start IS NOT NULL",
        )
        .await?
        .collect()
        .await?;

    let coding_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(coding_rows > 0, "expected some coding transcripts");

    let mut populated = 0usize;
    for batch in &batches {
        let cr_starts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cr_ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tx_starts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tx_ends = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !cr_starts.is_null(i) && !cr_ends.is_null(i) {
                populated += 1;
                let cr_start = cr_starts.value(i);
                let cr_end = cr_ends.value(i);
                let tx_start = tx_starts.value(i);
                let tx_end = tx_ends.value(i);
                assert!(
                    cr_start <= cr_end,
                    "cds_start ({}) should be <= cds_end ({})",
                    cr_start,
                    cr_end
                );
                // Coding region must fall within (or equal) the transcript span
                assert!(
                    cr_start >= tx_start && cr_end <= tx_end,
                    "coding region [{}, {}] should be within transcript [{}, {}]",
                    cr_start,
                    cr_end,
                    tx_start,
                    tx_end
                );
            }
        }
    }

    assert!(
        populated > 0,
        "expected cds_start/end to be derived for coding transcripts"
    );

    Ok(())
}

#[tokio::test]
async fn transcript_cdna_coding_populated_in_real_115() -> datafusion::common::Result<()> {
    // cdna_coding_start/end are populated for some coding transcripts in VEP
    // storable binary (unlike cds_start/end which are always undef).
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT cdna_coding_start, cdna_coding_end \
             FROM tx WHERE cdna_coding_start IS NOT NULL",
        )
        .await?
        .collect()
        .await?;

    let non_null_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        non_null_rows > 0,
        "expected some transcripts with cdna_coding_start populated in real VEP data"
    );

    // Verify cdna_coding_start <= cdna_coding_end
    for batch in &batches {
        let starts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !starts.is_null(i) && !ends.is_null(i) {
                assert!(
                    starts.value(i) <= ends.value(i),
                    "cdna_coding_start ({}) should be <= cdna_coding_end ({})",
                    starts.value(i),
                    ends.value(i)
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Exon tests (real VEP 115 fixture)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exon_real_115_query_returns_rows() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT chrom, start, \"end\", strand, stable_id, \
             transcript_id, exon_number FROM exons",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "expected exon rows from real VEP 115 fixture"
    );

    Ok(())
}

#[tokio::test]
async fn exon_schema_has_all_columns() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let schema = provider.schema();

    for col in &[
        "chrom",
        "start",
        "end",
        "strand",
        "stable_id",
        "version",
        "phase",
        "end_phase",
        "is_current",
        "is_constitutive",
        "transcript_id",
        "gene_stable_id",
        "exon_number",
        "raw_object_json",
        "object_hash",
    ] {
        assert!(schema.field_with_name(col).is_ok(), "missing column: {col}");
    }

    Ok(())
}

#[tokio::test]
async fn exon_foreign_key_populated() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT transcript_id FROM exons")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let col = batch
            .column_by_name("transcript_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.null_count(), 0, "transcript_id should never be null");
    }

    Ok(())
}

#[tokio::test]
async fn exon_number_is_non_negative() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    // Verify exon_number is always non-negative
    let batches = ctx
        .sql("SELECT exon_number FROM exons")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let ranks = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                ranks.value(i) >= 0,
                "exon_number should be non-negative, got {}",
                ranks.value(i)
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn exon_coordinates_valid() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT start, \"end\" FROM exons")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let starts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            assert!(
                starts.value(i) <= ends.value(i),
                "start ({}) should be <= end ({})",
                starts.value(i),
                ends.value(i)
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn exon_stable_id_populated() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id FROM exons WHERE stable_id IS NOT NULL")
        .await?
        .collect()
        .await?;

    let non_null_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        non_null_rows > 0,
        "expected some exons with stable_id populated in real VEP data"
    );

    // Merged cache may contain both Ensembl (ENSE...) and RefSeq (id-...) exon IDs
    if let Some(batch) = batches.first() {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        if !col.is_empty() {
            let id = col.value(0);
            assert!(
                id.starts_with("ENSE") || id.starts_with("id-"),
                "exon stable_id should start with ENSE or id-, got: {id}"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn exon_parallel_row_count_invariant() -> datafusion::common::Result<()> {
    let base_provider =
        ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(base_provider))?;

    let base_batches = ctx.sql("SELECT chrom FROM exons").await?.collect().await?;
    let base_count: usize = base_batches.iter().map(|b| b.num_rows()).sum();

    for partitions in [1usize, 2, 4] {
        let mut options = EnsemblCacheOptions::new(fixture_path("exon_real_115"));
        options.max_storable_partitions = Some(partitions);
        let provider = ExonTableProvider::new(options)?;
        let ctx = session_ctx_with_target_partitions(partitions);
        ctx.register_table("exons", Arc::new(provider))?;

        let batches = ctx.sql("SELECT chrom FROM exons").await?.collect().await?;
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            count, base_count,
            "count mismatch with partitions={partitions}: got {count}, expected {base_count}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn exon_projection_pushdown() -> datafusion::common::Result<()> {
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id, exon_number FROM exons")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);

    Ok(())
}

#[tokio::test]
async fn exon_text_format_works() -> datafusion::common::Result<()> {
    // The transcript_storable text fixture lacks _trans_exon_array, so no exon
    // rows will be produced — but the query must not error.
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT start, \"end\", phase, transcript_id FROM exons")
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // This fixture has no _trans_exon_array, so 0 rows is expected
    assert_eq!(
        total_rows, 0,
        "expected 0 exon rows from text fixture without _trans_exon_array"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Translation tests (real VEP 115 fixture)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn translation_real_115_query_returns_rows() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT chrom, start, \"end\", stable_id, \
             transcript_id FROM translations",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "expected translation rows from real VEP 115 fixture"
    );

    Ok(())
}

#[tokio::test]
async fn translation_count_leq_transcript_count() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let tl_provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("tl", Arc::new(tl_provider))?;

    let tx_batches = ctx.sql("SELECT stable_id FROM tx").await?.collect().await?;
    let tx_count: usize = tx_batches.iter().map(|b| b.num_rows()).sum();

    let tl_batches = ctx.sql("SELECT stable_id FROM tl").await?.collect().await?;
    let tl_count: usize = tl_batches.iter().map(|b| b.num_rows()).sum();

    assert!(
        tl_count <= tx_count,
        "translations ({tl_count}) should be <= transcripts ({tx_count})"
    );
    assert!(tl_count > 0, "expected at least some translations");

    Ok(())
}

#[tokio::test]
async fn translation_foreign_key_populated() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT transcript_id FROM translations")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let col = batch
            .column_by_name("transcript_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.null_count(), 0, "transcript_id should never be null");
    }

    Ok(())
}

#[tokio::test]
async fn translation_stable_id_populated() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id FROM translations WHERE stable_id IS NOT NULL")
        .await?
        .collect()
        .await?;

    let non_null_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        non_null_rows > 0,
        "expected some translations with stable_id (ENSP)"
    );

    if let Some(batch) = batches.first() {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        if !col.is_empty() {
            assert!(
                col.value(0).starts_with("ENSP"),
                "translation stable_id should start with ENSP, got: {}",
                col.value(0)
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn translation_parallel_row_count_invariant() -> datafusion::common::Result<()> {
    let base_provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(base_provider))?;

    let base_batches = ctx
        .sql("SELECT chrom FROM translations")
        .await?
        .collect()
        .await?;
    let base_count: usize = base_batches.iter().map(|b| b.num_rows()).sum();

    for partitions in [1usize, 2, 4] {
        let mut options = EnsemblCacheOptions::new(fixture_path("exon_real_115"));
        options.max_storable_partitions = Some(partitions);
        let provider = TranslationTableProvider::new(options)?;
        let ctx = session_ctx_with_target_partitions(partitions);
        ctx.register_table("translations", Arc::new(provider))?;

        let batches = ctx
            .sql("SELECT chrom FROM translations")
            .await?
            .collect()
            .await?;
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            count, base_count,
            "count mismatch with partitions={partitions}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn translation_projection_pushdown() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id, transcript_id FROM translations")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);

    Ok(())
}

#[tokio::test]
async fn translation_sequence_columns_populated() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT cdna_coding_start, cdna_coding_end, translation_seq, cds_sequence \
             FROM translations",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "expected translation rows");

    // At least some rows should have cdna_coding_start and cdna_coding_end populated
    let mut has_cdna_coding_start = false;
    let mut has_cdna_coding_end = false;
    for batch in &batches {
        let start_col = batch
            .column_by_name("cdna_coding_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let end_col = batch
            .column_by_name("cdna_coding_end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        if start_col.null_count() < start_col.len() {
            has_cdna_coding_start = true;
        }
        if end_col.null_count() < end_col.len() {
            has_cdna_coding_end = true;
        }
        // When both are present, end >= start
        for i in 0..batch.num_rows() {
            if !start_col.is_null(i) && !end_col.is_null(i) {
                assert!(
                    end_col.value(i) >= start_col.value(i),
                    "cdna_coding_end ({}) < cdna_coding_start ({})",
                    end_col.value(i),
                    start_col.value(i)
                );
            }
        }
    }
    assert!(
        has_cdna_coding_start,
        "expected some translations with cdna_coding_start"
    );
    assert!(
        has_cdna_coding_end,
        "expected some translations with cdna_coding_end"
    );

    // Verify sequence columns exist in output (may be null if VEF cache
    // was evicted in the fixture, but columns must be present and queryable)
    assert!(batches[0].column_by_name("translation_seq").is_some());
    assert!(batches[0].column_by_name("cds_sequence").is_some());

    Ok(())
}

#[tokio::test]
async fn translation_sequence_projection_pushdown() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("translations", Arc::new(provider))?;

    // Project only the new sequence columns — triggers sequences_projected flag
    let batches = ctx
        .sql("SELECT translation_seq, cds_sequence FROM translations")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "expected translation rows");

    // Project only cdna_coding columns (non-VEF path)
    let batches2 = ctx
        .sql("SELECT cdna_coding_start, cdna_coding_end FROM translations")
        .await?
        .collect()
        .await?;
    assert!(!batches2.is_empty());
    assert_eq!(batches2[0].num_columns(), 2);

    Ok(())
}

// ---------------------------------------------------------------------------
// Gnomon / RefSeq predicted transcript tests (real VEP 115 fixture)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_gnomon_source_included() -> datafusion::common::Result<()> {
    // VEP evaluates Gnomon (XM_/XR_) transcripts in --merged mode.
    // They must NOT be filtered out.
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT source FROM tx WHERE source = 'Gnomon'")
        .await?
        .collect()
        .await?;

    let gnomon_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        gnomon_rows > 0,
        "Gnomon transcripts should be present for VEP merged mode parity"
    );

    Ok(())
}

#[tokio::test]
async fn exon_stable_ids_are_exon_patterns() -> datafusion::common::Result<()> {
    // Exon stable_id column should contain ENSE*/exon-* patterns,
    // never transcript (ENST*) or gene (ENSG*) IDs.
    let provider = ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let ctx = SessionContext::new();
    ctx.register_table("exons", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id FROM exons WHERE stable_id IS NOT NULL")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..ids.len() {
            let id = ids.value(i);
            assert!(
                !id.starts_with("ENST") && !id.starts_with("ENSG"),
                "exon stable_id '{id}' looks like a transcript/gene, not an exon"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_loc_pseudo_records_excluded() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id FROM tx WHERE stable_id LIKE 'LOC%'")
        .await?
        .collect()
        .await?;

    let loc_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        loc_rows, 0,
        "LOC-prefixed gene pseudo-records should be filtered out, found {loc_rows}"
    );

    Ok(())
}
