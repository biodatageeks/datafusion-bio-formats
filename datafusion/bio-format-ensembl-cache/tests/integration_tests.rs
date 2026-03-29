use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int8Array, Int32Array, Int64Array, ListArray, StringArray,
    StructArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::test_utils::find_leaf_exec;
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind, ExonTableProvider,
    MotifFeatureTableProvider, RegulatoryFeatureTableProvider, TranscriptTableProvider,
    TranslationTableProvider, VEP_CHROMOSOMES_METADATA_KEY, VariationTableProvider,
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

// ---------------------------------------------------------------------------
// Chromosome metadata from file paths (zero I/O)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn variation_chromosomes_from_file_paths() -> datafusion::common::Result<()> {
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    // Chromosomes are available directly from the provider API
    let chroms = provider
        .chromosomes()
        .expect("chroms should be extractable");
    assert_eq!(chroms, &["1", "2"]);

    // They are also stored in schema metadata
    let meta = provider
        .schema()
        .metadata()
        .get(VEP_CHROMOSOMES_METADATA_KEY)
        .cloned();
    assert!(meta.is_some());
    let parsed: Vec<String> = serde_json::from_str(&meta.unwrap()).unwrap();
    assert_eq!(parsed, vec!["1", "2"]);

    Ok(())
}

#[tokio::test]
async fn variation_tabix_chroms_unavailable_from_paths() -> datafusion::common::Result<()> {
    // variation_tabix has `variation/all_vars.gz` — parent dir is "variation",
    // so chroms can't be extracted from paths.
    let provider =
        VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path("variation_tabix")))?;

    assert!(provider.chromosomes().is_none());
    assert!(
        provider
            .schema()
            .metadata()
            .get(VEP_CHROMOSOMES_METADATA_KEY)
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn transcript_storable_chroms_from_file_paths() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let chroms = provider
        .chromosomes()
        .expect("chroms should be extractable");
    assert_eq!(chroms, &["1"]);

    Ok(())
}

#[tokio::test]
async fn transcript_merged_chroms_from_file_paths() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_merged_layout",
    )))?;

    let chroms = provider
        .chromosomes()
        .expect("chroms should be extractable");
    assert_eq!(chroms, &["1"]);

    Ok(())
}

#[tokio::test]
async fn regulatory_chroms_from_file_paths() -> datafusion::common::Result<()> {
    let provider = RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let chroms = provider
        .chromosomes()
        .expect("chroms should be extractable");
    assert_eq!(chroms, &["1"]);

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
async fn motif_schema_contains_transcription_factors() -> datafusion::common::Result<()> {
    let provider = MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;
    assert!(
        provider
            .schema()
            .field_with_name("transcription_factors")
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn motif_transcription_factors_queryable() -> datafusion::common::Result<()> {
    for fixture in ["regulatory_storable", "regulatory_sereal"] {
        let provider =
            MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(fixture)))?;
        let ctx = SessionContext::new();
        ctx.register_table("motif", Arc::new(provider))?;

        let batches = ctx
            .sql("SELECT transcription_factors FROM motif")
            .await?
            .collect()
            .await?;

        assert!(
            !batches.is_empty(),
            "expected motif rows for fixture {fixture}"
        );
        assert!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .is_some(),
            "transcription_factors should be StringArray for fixture {fixture}"
        );
    }

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
    let gene_phenotype = schema
        .field_with_name("gene_phenotype")
        .expect("gene_phenotype column missing");
    assert_eq!(gene_phenotype.data_type(), &DataType::Boolean);
    assert!(schema.field_with_name("ccds").is_ok());
    assert!(schema.field_with_name("swissprot").is_ok());
    assert!(schema.field_with_name("trembl").is_ok());
    assert!(schema.field_with_name("uniparc").is_ok());
    assert!(schema.field_with_name("uniprot_isoform").is_ok());
    assert!(schema.field_with_name("cds_start_nf").is_ok());
    assert!(schema.field_with_name("cds_end_nf").is_ok());
    let mirna_regions = schema
        .field_with_name("mature_mirna_regions")
        .expect("mature_mirna_regions column missing");
    assert!(
        matches!(mirna_regions.data_type(), DataType::List(_)),
        "mature_mirna_regions should be List type, got {:?}",
        mirna_regions.data_type()
    );
    let ncrna_structure = schema
        .field_with_name("ncrna_structure")
        .expect("ncrna_structure column missing");
    assert_eq!(ncrna_structure.data_type(), &DataType::Utf8);

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
async fn transcript_promoted_metadata_columns_queryable() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT gene_phenotype, ccds, swissprot, trembl, uniparc, \
             uniprot_isoform, cds_start_nf, cds_end_nf FROM tx",
        )
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 8);
    assert!(
        batches[0]
            .column_by_name("gene_phenotype")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .is_some(),
        "gene_phenotype should be BooleanArray"
    );

    let metadata_present = ctx
        .sql(
            "SELECT COUNT(*) FROM tx \
             WHERE ccds IS NOT NULL OR swissprot IS NOT NULL OR trembl IS NOT NULL \
                OR uniparc IS NOT NULL OR uniprot_isoform IS NOT NULL \
                OR gene_phenotype IS NOT NULL",
        )
        .await?
        .collect()
        .await?;
    assert!(
        first_i64(&metadata_present) > 0,
        "expected at least one transcript row with promoted CSQ metadata"
    );

    Ok(())
}

#[tokio::test]
async fn transcript_mature_mirna_regions_queryable() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT stable_id, mature_mirna_regions FROM tx LIMIT 10")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let regions_col = batches[0]
        .column_by_name("mature_mirna_regions")
        .expect("mature_mirna_regions column missing");
    let list_array = regions_col
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("mature_mirna_regions should be ListArray");

    for row in 0..list_array.len() {
        if !list_array.is_null(row) {
            let regions_value = list_array.value(row);
            let regions = regions_value
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("mature_mirna_regions values should be StructArray");
            assert!(regions.column_by_name("start").is_some());
            assert!(regions.column_by_name("end").is_some());
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_ncrna_structure_queryable() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // ncrna_structure should be queryable as a string column
    let batches = ctx
        .sql("SELECT stable_id, ncrna_structure FROM tx LIMIT 10")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let ncrna_col = batches[0]
        .column_by_name("ncrna_structure")
        .expect("ncrna_structure column missing");
    let string_array = ncrna_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("ncrna_structure should be StringArray");
    // Non-miRNA transcripts should have NULL ncrna_structure
    assert_eq!(string_array.len(), batches[0].num_rows());

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
                    "cds_start ({cr_start}) should be <= cds_end ({cr_end})"
                );
                // Coding region must fall within (or equal) the transcript span
                assert!(
                    cr_start >= tx_start && cr_end <= tx_end,
                    "coding region [{cr_start}, {cr_end}] should be within transcript [{tx_start}, {tx_end}]"
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

// ---------------------------------------------------------------------------
// EnsemblCacheTableProvider::for_entity() factory tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn factory_for_entity_variation() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::Variation,
        EnsemblCacheOptions::new(fixture_path("variation_non_tabix")),
    )?;
    assert!(provider.schema().field_with_name("variation_name").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("var", provider)?;
    let batches = ctx.sql("SELECT COUNT(*) FROM var").await?.collect().await?;
    assert_eq!(first_i64(&batches), 3);

    Ok(())
}

#[tokio::test]
async fn factory_for_entity_transcript() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::Transcript,
        EnsemblCacheOptions::new(fixture_path("transcript_storable")),
    )?;
    assert!(provider.schema().field_with_name("stable_id").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("tx", provider)?;
    let batches = ctx.sql("SELECT COUNT(*) FROM tx").await?.collect().await?;
    assert_eq!(first_i64(&batches), 2);

    Ok(())
}

#[tokio::test]
async fn factory_for_entity_exon() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::Exon,
        EnsemblCacheOptions::new(fixture_path("exon_real_115")),
    )?;
    assert!(provider.schema().field_with_name("exon_number").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("exon", provider)?;
    let batches = ctx
        .sql("SELECT COUNT(*) FROM exon")
        .await?
        .collect()
        .await?;
    assert!(first_i64(&batches) > 0);

    Ok(())
}

#[tokio::test]
async fn factory_for_entity_translation() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::Translation,
        EnsemblCacheOptions::new(fixture_path("exon_real_115")),
    )?;
    assert!(provider.schema().field_with_name("protein_len").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("tl", provider)?;
    let batches = ctx.sql("SELECT COUNT(*) FROM tl").await?.collect().await?;
    assert!(first_i64(&batches) > 0);

    Ok(())
}

#[tokio::test]
async fn factory_for_entity_regulatory() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::RegulatoryFeature,
        EnsemblCacheOptions::new(fixture_path("regulatory_storable")),
    )?;
    assert!(provider.schema().field_with_name("feature_type").is_ok());

    let ctx = SessionContext::new();
    ctx.register_table("reg", provider)?;
    let batches = ctx.sql("SELECT COUNT(*) FROM reg").await?.collect().await?;
    assert_eq!(first_i64(&batches), 1);

    Ok(())
}

#[tokio::test]
async fn factory_for_entity_motif() -> datafusion::common::Result<()> {
    let provider = EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::MotifFeature,
        EnsemblCacheOptions::new(fixture_path("regulatory_storable")),
    )?;
    assert!(provider.schema().field_with_name("binding_matrix").is_ok());

    Ok(())
}

// ---------------------------------------------------------------------------
// Variation field value verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn variation_core_field_values() -> datafusion::common::Result<()> {
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT chrom, start, \"end\", variation_name, allele_string, region_bin \
             FROM var ORDER BY chrom, start",
        )
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert!(batch.num_rows() >= 2);

    // Verify chrom values
    let chroms = batch
        .column_by_name("chrom")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(chroms.value(0), "1");

    // Verify start values are positive
    let starts = batch
        .column_by_name("start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(starts.value(i) > 0, "start should be positive");
    }

    // Verify end >= start
    let ends = batch
        .column_by_name("end")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(
            ends.value(i) >= starts.value(i),
            "end ({}) should be >= start ({})",
            ends.value(i),
            starts.value(i)
        );
    }

    // Verify variation_name is non-empty
    let var_names = batch
        .column_by_name("variation_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(
            !var_names.value(i).is_empty(),
            "variation_name should be non-empty"
        );
    }

    // Verify allele_string contains '/'
    let alleles = batch
        .column_by_name("allele_string")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(
            alleles.value(i).contains('/'),
            "allele_string '{}' should contain '/'",
            alleles.value(i)
        );
    }

    // Verify region_bin is non-negative
    let bins = batch
        .column_by_name("region_bin")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(bins.value(i) >= 0, "region_bin should be non-negative");
    }

    Ok(())
}

#[tokio::test]
async fn variation_optional_fields_nullable() -> datafusion::common::Result<()> {
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT failed, somatic, strand, minor_allele, minor_allele_freq, \
             clin_sig, phenotype_or_disease FROM var",
        )
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];

    // failed and somatic should be Int8
    let failed = batch
        .column_by_name("failed")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .expect("failed should be Int8Array");
    for i in 0..batch.num_rows() {
        if !failed.is_null(i) {
            assert!(
                failed.value(i) == 0 || failed.value(i) == 1,
                "failed should be 0 or 1"
            );
        }
    }

    // minor_allele_freq should be Float64
    let maf = batch
        .column_by_name("minor_allele_freq")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("minor_allele_freq should be Float64Array");
    for i in 0..batch.num_rows() {
        if !maf.is_null(i) {
            let freq = maf.value(i);
            assert!(
                (0.0..=1.0).contains(&freq),
                "minor_allele_freq {freq} should be in [0,1]"
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Variation provenance columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn variation_provenance_values() -> datafusion::common::Result<()> {
    let provider = VariationTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "variation_non_tabix",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("var", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT species, assembly, cache_version, source_file FROM var LIMIT 1")
        .await?
        .collect()
        .await?;

    assert_eq!(first_string(&batches, 0).as_deref(), Some("homo_sapiens"));
    assert_eq!(first_string(&batches, 1).as_deref(), Some("GRCh38"));
    assert_eq!(first_string(&batches, 2).as_deref(), Some("112"));
    assert!(
        first_string(&batches, 3).is_some(),
        "source_file should be populated"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Transcript detailed field tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_storable_gene_fields() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT stable_id, gene_stable_id, gene_symbol, biotype, strand \
             FROM tx WHERE stable_id = 'ENST000001'",
        )
        .await?
        .collect()
        .await?;

    assert_eq!(batches[0].num_rows(), 1);

    let gene_id = batches[0]
        .column_by_name("gene_stable_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        !gene_id.is_null(0),
        "gene_stable_id should be populated for ENST000001"
    );

    let biotype = batches[0]
        .column_by_name("biotype")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(biotype.value(0), "protein_coding");

    Ok(())
}

#[tokio::test]
async fn transcript_real_115_translation_fields() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Coding transcripts should have translation fields populated
    let batches = ctx
        .sql(
            "SELECT translation_stable_id, translation_start, translation_end \
             FROM tx WHERE cdna_coding_start IS NOT NULL LIMIT 5",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);

    // At least some should have translation_stable_id starting with ENSP
    let mut has_ensp = false;
    for batch in &batches {
        let tl_ids = batch
            .column_by_name("translation_stable_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !tl_ids.is_null(i) && tl_ids.value(i).starts_with("ENSP") {
                has_ensp = true;
                break;
            }
        }
    }
    assert!(
        has_ensp,
        "expected translation_stable_id starting with ENSP"
    );

    Ok(())
}

#[tokio::test]
async fn transcript_cdna_mapper_segments_schema() -> datafusion::common::Result<()> {
    // Use text-format fixture since the exon_real_115 fixture has truncated
    // storable binary that errors when deep VEF cache fields are projected.
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Verify the column exists and is queryable
    let batches = ctx
        .sql("SELECT cdna_mapper_segments FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());

    // Verify column is List type
    let col = batches[0].column(0);
    let list_array = col
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("cdna_mapper_segments should be ListArray");

    // If any non-null values exist, verify struct fields
    for row in 0..list_array.len() {
        if !list_array.is_null(row) {
            let segments = list_array.value(row);
            let struct_array = segments
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("segments should be StructArray");
            if struct_array.len() > 0 {
                assert!(struct_array.column_by_name("genomic_start").is_some());
                assert!(struct_array.column_by_name("genomic_end").is_some());
                assert!(struct_array.column_by_name("cdna_start").is_some());
                assert!(struct_array.column_by_name("cdna_end").is_some());
                assert!(struct_array.column_by_name("ori").is_some());
            }
            break;
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_translateable_seq_queryable() -> datafusion::common::Result<()> {
    // Use text-format fixture since the exon_real_115 fixture has truncated
    // storable binary that errors when deep VEF cache fields are projected.
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // Verify the column exists and is queryable
    let batches = ctx
        .sql("SELECT translateable_seq FROM tx")
        .await?
        .collect()
        .await?;

    assert!(!batches.is_empty());
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("translateable_seq should be StringArray");

    // If any non-null values exist, verify they contain valid DNA characters
    for i in 0..col.len() {
        if !col.is_null(i) {
            let seq = col.value(i);
            assert!(
                !seq.is_empty(),
                "translateable_seq should be non-empty when not null"
            );
            assert!(
                seq.chars().all(|c| "ACGTNacgtn".contains(c)),
                "translateable_seq contains invalid DNA characters: {}",
                &seq[..seq.len().min(50)]
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_real_115_flags_str() -> datafusion::common::Result<()> {
    let provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    // flags_str should be queryable (may be null for most transcripts)
    let batches = ctx.sql("SELECT flags_str FROM tx").await?.collect().await?;

    assert!(!batches.is_empty());
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("flags_str should be StringArray");

    // Verify non-null values are plausible (e.g., "gencode_basic", "cds_start_NF")
    for i in 0..col.len() {
        if !col.is_null(i) {
            let flags = col.value(i);
            assert!(
                !flags.is_empty(),
                "flags_str should be non-empty when not null"
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Translation detailed field tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn translation_protein_features_populated() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT protein_features FROM tl WHERE protein_features IS NOT NULL")
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "expected some translations with protein_features from real VEP 115"
    );

    // Verify protein_features List<Struct> structure
    let col = batches[0].column(0);
    let list_array = col
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("protein_features should be ListArray");

    for row in 0..list_array.len() {
        if !list_array.is_null(row) {
            let features = list_array.value(row);
            let struct_array = features
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("protein_features values should be StructArray");
            assert!(
                struct_array.len() > 0,
                "protein feature list should not be empty"
            );
            assert!(struct_array.column_by_name("analysis").is_some());
            assert!(struct_array.column_by_name("hseqname").is_some());
            assert!(struct_array.column_by_name("start").is_some());
            assert!(struct_array.column_by_name("end").is_some());

            // Verify analysis values are populated (issue #128 fix)
            let analyses = struct_array
                .column_by_name("analysis")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let mut any_analysis_populated = false;
            for i in 0..analyses.len() {
                if !analyses.is_null(i) {
                    any_analysis_populated = true;
                    let label = analyses.value(i);
                    assert!(
                        !label.is_empty(),
                        "protein feature analysis should not be empty"
                    );
                }
            }
            assert!(
                any_analysis_populated,
                "expected at least one non-NULL analysis in protein_features (issue #128)"
            );

            // Verify start <= end for features
            let starts = struct_array
                .column_by_name("start")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ends = struct_array
                .column_by_name("end")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..struct_array.len() {
                if !starts.is_null(i) && !ends.is_null(i) {
                    assert!(
                        starts.value(i) <= ends.value(i),
                        "protein feature start ({}) should be <= end ({})",
                        starts.value(i),
                        ends.value(i)
                    );
                }
            }
            break;
        }
    }

    Ok(())
}

#[tokio::test]
async fn translation_sift_predictions_from_storable() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT sift_predictions FROM tl WHERE sift_predictions IS NOT NULL")
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // SIFT predictions are decoded from binary matrix in storable format
    // They may be non-null if the raw VEP cache has matrix data
    if total_rows > 0 {
        let col = batches[0].column(0);
        let list_array = col
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("sift_predictions should be ListArray");

        for row in 0..list_array.len() {
            if !list_array.is_null(row) {
                let entries = list_array.value(row);
                let struct_array = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("sift prediction values should be StructArray");
                assert!(struct_array.len() > 0);

                // Verify position is 1-based
                let positions = struct_array
                    .column_by_name("position")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..positions.len() {
                    assert!(
                        positions.value(i) >= 1,
                        "SIFT position should be 1-based, got {}",
                        positions.value(i)
                    );
                }

                // Verify amino acids are single characters
                let aas = struct_array
                    .column_by_name("amino_acid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..aas.len() {
                    assert_eq!(
                        aas.value(i).len(),
                        1,
                        "amino_acid should be single char, got '{}'",
                        aas.value(i)
                    );
                }
                break;
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn translation_cds_len_derived_correctly() -> datafusion::common::Result<()> {
    let provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tl", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT cdna_coding_start, cdna_coding_end, cds_len \
             FROM tl WHERE cdna_coding_start IS NOT NULL AND cdna_coding_end IS NOT NULL",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);

    for batch in &batches {
        let starts = batch
            .column_by_name("cdna_coding_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = batch
            .column_by_name("cdna_coding_end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cds_lens = batch
            .column_by_name("cds_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if !starts.is_null(i) && !ends.is_null(i) && !cds_lens.is_null(i) {
                let expected_len = ends.value(i) - starts.value(i) + 1;
                assert_eq!(
                    cds_lens.value(i),
                    expected_len,
                    "cds_len should be cdna_coding_end - cdna_coding_start + 1"
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-entity consistency tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cross_entity_translation_transcript_ids_exist() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let tl_provider =
        TranslationTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("tl", Arc::new(tl_provider))?;

    // Every translation's transcript_id should exist as a transcript stable_id
    let batches = ctx
        .sql(
            "SELECT tl.transcript_id FROM tl \
             LEFT JOIN tx ON tl.transcript_id = tx.stable_id \
             WHERE tx.stable_id IS NULL",
        )
        .await?
        .collect()
        .await?;

    let orphan_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        orphan_count, 0,
        "all translation transcript_ids should match a transcript stable_id, \
         found {orphan_count} orphans"
    );

    Ok(())
}

#[tokio::test]
async fn cross_entity_exon_transcript_ids_exist() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let exon_provider =
        ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("exons", Arc::new(exon_provider))?;

    // Every exon's transcript_id should exist as a transcript stable_id
    let batches = ctx
        .sql(
            "SELECT exons.transcript_id FROM exons \
             LEFT JOIN tx ON exons.transcript_id = tx.stable_id \
             WHERE tx.stable_id IS NULL",
        )
        .await?
        .collect()
        .await?;

    let orphan_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        orphan_count, 0,
        "all exon transcript_ids should match a transcript stable_id, \
         found {orphan_count} orphans"
    );

    Ok(())
}

#[tokio::test]
async fn cross_entity_exon_count_matches_exon_rows() -> datafusion::common::Result<()> {
    let tx_provider =
        TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;
    let exon_provider =
        ExonTableProvider::new(EnsemblCacheOptions::new(fixture_path("exon_real_115")))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(tx_provider))?;
    ctx.register_table("exons", Arc::new(exon_provider))?;

    // For each transcript, exon_count should match the number of exon rows
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

    let mismatch_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        mismatch_count, 0,
        "transcript exon_count should match actual exon row count for all transcripts"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Regulatory and motif detailed field tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn regulatory_field_values_storable() -> datafusion::common::Result<()> {
    let provider = RegulatoryFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("reg", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT chrom, start, \"end\", strand, stable_id, feature_type FROM reg")
        .await?
        .collect()
        .await?;

    assert_eq!(batches[0].num_rows(), 1);

    // Verify chrom is non-null
    let chrom = batches[0]
        .column_by_name("chrom")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!chrom.is_null(0));

    // Verify start <= end
    let starts = batches[0]
        .column_by_name("start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ends = batches[0]
        .column_by_name("end")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(starts.value(0) <= ends.value(0));

    // Verify strand is -1 or 1
    let strand = batches[0]
        .column_by_name("strand")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert!(
        strand.value(0) == 1 || strand.value(0) == -1 || strand.value(0) == 0,
        "strand should be -1, 0, or 1"
    );

    Ok(())
}

#[tokio::test]
async fn motif_field_values_storable() -> datafusion::common::Result<()> {
    let provider = MotifFeatureTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "regulatory_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("motif", Arc::new(provider))?;

    let batches = ctx
        .sql(
            "SELECT chrom, start, \"end\", motif_id, score, binding_matrix, \
             transcription_factors FROM motif",
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);

    let batch = &batches[0];

    // Verify binding_matrix is populated
    let binding = batch
        .column_by_name("binding_matrix")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!binding.is_null(0));
    assert_eq!(binding.value(0), "MA0001.1");

    // Verify score is Float64
    let _score = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("score should be Float64Array");
    // Score can be null but column must exist

    Ok(())
}

// ---------------------------------------------------------------------------
// Schema metadata tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn all_entities_have_coordinate_system_metadata() -> datafusion::common::Result<()> {
    let fixtures_and_providers: Vec<(&str, Arc<dyn TableProvider>)> = vec![
        (
            "variation",
            Arc::new(VariationTableProvider::new(EnsemblCacheOptions::new(
                fixture_path("variation_non_tabix"),
            ))?) as Arc<dyn TableProvider>,
        ),
        (
            "transcript",
            Arc::new(TranscriptTableProvider::new(EnsemblCacheOptions::new(
                fixture_path("transcript_storable"),
            ))?),
        ),
        (
            "exon",
            Arc::new(ExonTableProvider::new(EnsemblCacheOptions::new(
                fixture_path("exon_real_115"),
            ))?),
        ),
        (
            "translation",
            Arc::new(TranslationTableProvider::new(EnsemblCacheOptions::new(
                fixture_path("exon_real_115"),
            ))?),
        ),
        (
            "regulatory",
            Arc::new(RegulatoryFeatureTableProvider::new(
                EnsemblCacheOptions::new(fixture_path("regulatory_storable")),
            )?),
        ),
        (
            "motif",
            Arc::new(MotifFeatureTableProvider::new(EnsemblCacheOptions::new(
                fixture_path("regulatory_storable"),
            ))?),
        ),
    ];

    for (name, provider) in fixtures_and_providers {
        let schema = provider.schema();
        assert!(
            schema
                .metadata()
                .contains_key(COORDINATE_SYSTEM_METADATA_KEY),
            "{name} schema should have coordinate system metadata"
        );
        assert_eq!(
            schema
                .metadata()
                .get(COORDINATE_SYSTEM_METADATA_KEY)
                .unwrap(),
            "false",
            "{name} should default to 1-based (false)"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Object hash and raw JSON tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transcript_object_hash_stable() -> datafusion::common::Result<()> {
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT object_hash, raw_object_json FROM tx")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let hashes = batch
            .column_by_name("object_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let jsons = batch
            .column_by_name("raw_object_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            // object_hash should never be null
            assert!(!hashes.is_null(i), "object_hash should never be null");
            // raw_object_json should never be null
            assert!(!jsons.is_null(i), "raw_object_json should never be null");
            // raw_object_json should be valid JSON
            let json_str = jsons.value(i);
            assert!(
                serde_json::from_str::<serde_json::Value>(json_str).is_ok(),
                "raw_object_json should be valid JSON: {}",
                &json_str[..json_str.len().min(100)]
            );
            // object_hash should be a hex string
            assert!(
                hashes.value(i).chars().all(|c| c.is_ascii_hexdigit()),
                "object_hash should be hex, got: {}",
                hashes.value(i)
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn transcript_object_hash_stable_text() -> datafusion::common::Result<()> {
    // Use the text-format storable fixture to verify object_hash and raw_object_json
    // (exon_real_115 storable binary has truncation issues with deep VEF cache).
    let provider = TranscriptTableProvider::new(EnsemblCacheOptions::new(fixture_path(
        "transcript_storable",
    )))?;

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT object_hash, raw_object_json FROM tx")
        .await?
        .collect()
        .await?;

    for batch in &batches {
        let hashes = batch
            .column_by_name("object_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let jsons = batch
            .column_by_name("raw_object_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!hashes.is_null(i), "object_hash should never be null");
            assert!(!jsons.is_null(i), "raw_object_json should never be null");
            // raw_object_json should be valid JSON
            let json_str = jsons.value(i);
            assert!(
                serde_json::from_str::<serde_json::Value>(json_str).is_ok(),
                "raw_object_json should be valid JSON: {}",
                &json_str[..json_str.len().min(100)]
            );
            // object_hash should be a hex string
            assert!(
                hashes.value(i).chars().all(|c| c.is_ascii_hexdigit()),
                "object_hash should be hex, got: {}",
                hashes.value(i)
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sort-preserving merge and parallel execution tests
//
// Uses the `variation_multi_region` fixture which has 4 chr1 files and
// 2 chr2 files, enough to distribute across multiple partitions and
// verify true parallel execution (not just optimizer-added merge).
// ---------------------------------------------------------------------------

/// Recursively search for an execution plan node by name.
fn find_exec_by_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    for child in plan.children() {
        if let Some(found) = find_exec_by_name(child, name) {
            return Some(found);
        }
    }
    None
}

/// Check that a plan tree does NOT contain a node with the given name.
fn assert_no_exec_named(plan: &Arc<dyn ExecutionPlan>, name: &str) {
    assert!(
        find_exec_by_name(plan, name).is_none(),
        "unexpected {name} found in plan"
    );
}

#[tokio::test]
async fn variation_multi_region_parallel_exec_with_merge() -> datafusion::common::Result<()> {
    // With 4 chr1 files and target_partitions=2, the EnsemblCacheExec should
    // have 2 partitions (2 files each) and be wrapped by SortPreservingMergeExec.
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
    options.target_partitions = Some(2);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(2);
    ctx.register_table("variation", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start")
        .await?;
    let plan = df.create_physical_plan().await?;

    // SortPreservingMergeExec should be present (from our scan() wrapping)
    let merge = find_exec_by_name(&plan, "SortPreservingMergeExec");
    assert!(merge.is_some(), "expected SortPreservingMergeExec in plan");

    // No CoalescePartitionsExec (which would serialize to 1 thread)
    assert_no_exec_named(&plan, "CoalescePartitionsExec");

    // The EnsemblCacheExec must have >1 partition for true parallelism
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "EnsemblCacheExec");
    let num_partitions = leaf.output_partitioning().partition_count();
    assert!(
        num_partitions > 1,
        "expected >1 partitions for parallel exec, got {num_partitions}"
    );

    Ok(())
}

#[tokio::test]
async fn variation_multi_region_partition_count_scales() -> datafusion::common::Result<()> {
    // Verify that EnsemblCacheExec partition count scales with target_partitions
    // up to the number of matching files (4 chr1 files).
    for target_partitions in [2, 3, 4] {
        let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
        options.target_partitions = Some(target_partitions);
        let provider = VariationTableProvider::new(options)?;
        let ctx = session_ctx_with_target_partitions(target_partitions);
        let table_name = format!("var_{target_partitions}");
        ctx.register_table(&table_name, Arc::new(provider))?;

        let df = ctx
            .sql(&format!(
                "SELECT start FROM {table_name} WHERE chrom = '1' ORDER BY start"
            ))
            .await?;
        let plan = df.create_physical_plan().await?;

        let leaf = find_leaf_exec(&plan);
        let actual_partitions = leaf.output_partitioning().partition_count();
        let expected = target_partitions.min(4); // capped by 4 chr1 files
        assert_eq!(
            actual_partitions, expected,
            "target_partitions={target_partitions}: expected {expected} partitions, got {actual_partitions}"
        );

        // Merge should be present when >1 partition
        if expected > 1 {
            assert!(
                find_exec_by_name(&plan, "SortPreservingMergeExec").is_some(),
                "expected SortPreservingMergeExec with {expected} partitions"
            );
        }

        // Never coalesce
        assert_no_exec_named(&plan, "CoalescePartitionsExec");
    }

    Ok(())
}

#[tokio::test]
async fn variation_multi_region_single_partition_no_merge() -> datafusion::common::Result<()> {
    // With 1 partition, no SortPreservingMergeExec is needed.
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
    options.target_partitions = Some(1);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(1);
    ctx.register_table("variation", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start")
        .await?;
    let plan = df.create_physical_plan().await?;

    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.output_partitioning().partition_count(), 1);

    // Our scan() should not wrap with merge for single partition
    // (DataFusion's optimizer may add one, but we don't explicitly)
    Ok(())
}

#[tokio::test]
async fn variation_multi_region_no_chrom_filter_no_merge() -> datafusion::common::Result<()> {
    // Without WHERE chrom = '...', scan() should NOT wrap with
    // SortPreservingMergeExec (karyotypic != lexicographic).
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
    options.target_partitions = Some(4);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(4);
    ctx.register_table("variation", Arc::new(provider))?;

    let df = ctx.sql("SELECT chrom, start FROM variation").await?;
    let plan = df.create_physical_plan().await?;

    // No SortPreservingMergeExec from our scan() for unfiltered queries.
    // DataFusion may add its own CoalescePartitionsExec, which is fine —
    // we just don't add merge because our ordering declaration would be wrong.
    assert_no_exec_named(&plan, "SortPreservingMergeExec");

    Ok(())
}

#[tokio::test]
async fn variation_multi_region_no_coalesce_for_various_partitions()
-> datafusion::common::Result<()> {
    // Verify that for partition counts 2..4, the plan never coalesces
    // when filtering to a single chromosome with ORDER BY.
    for target_partitions in 2..=4 {
        let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
        options.target_partitions = Some(target_partitions);
        let provider = VariationTableProvider::new(options)?;
        let ctx = session_ctx_with_target_partitions(target_partitions);
        let table_name = format!("var_{target_partitions}");
        ctx.register_table(&table_name, Arc::new(provider))?;

        let df = ctx
            .sql(&format!(
                "SELECT start FROM {table_name} WHERE chrom = '1' ORDER BY start"
            ))
            .await?;
        let plan = df.create_physical_plan().await?;

        assert_no_exec_named(&plan, "CoalescePartitionsExec");
    }

    Ok(())
}

#[tokio::test]
async fn variation_multi_region_results_ordered_with_parallel_exec()
-> datafusion::common::Result<()> {
    // End-to-end: with 4 chr1 files across 2 partitions, verify that
    // results come back correctly ordered by start.
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_multi_region"));
    options.target_partitions = Some(2);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(2);
    ctx.register_table("variation", Arc::new(provider))?;

    let batches = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start")
        .await?
        .collect()
        .await?;

    let starts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();

    // Should have all 8 rows from 4 chr1 files (2 rows each)
    assert_eq!(starts.len(), 8, "expected 8 rows from 4 chr1 files");

    // Verify monotonically non-decreasing
    for window in starts.windows(2) {
        assert!(
            window[0] <= window[1],
            "results not ordered: {} > {}",
            window[0],
            window[1]
        );
    }

    // Verify expected start values span all 4 regions
    assert_eq!(starts[0], 100, "first row should be from region 1");
    assert!(
        starts.last().unwrap() >= &3000100,
        "last row should be from region 4"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Tabix bgzf intra-file parallelism tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tabix_bgzf_parallel_partitions() -> datafusion::common::Result<()> {
    // With a single bgzf all_vars.gz and target_partitions=4,
    // the EnsemblCacheExec should split into multiple bgzf byte-range
    // partitions for true intra-file parallelism.
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_tabix_bgzf"));
    options.target_partitions = Some(4);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(4);
    ctx.register_table("variation", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT start FROM variation WHERE chrom = '1' ORDER BY start")
        .await?;
    let plan = df.create_physical_plan().await?;

    // EnsemblCacheExec should have multiple partitions (bgzf byte-range split)
    let leaf = find_leaf_exec(&plan);
    assert_eq!(leaf.name(), "EnsemblCacheExec");
    let num_partitions = leaf.output_partitioning().partition_count();
    assert!(
        num_partitions > 1,
        "expected >1 partitions for tabix bgzf split, got {num_partitions}"
    );

    // SortPreservingMergeExec should be present
    assert!(
        find_exec_by_name(&plan, "SortPreservingMergeExec").is_some(),
        "expected SortPreservingMergeExec in plan"
    );

    // No CoalescePartitionsExec
    assert_no_exec_named(&plan, "CoalescePartitionsExec");

    Ok(())
}

#[tokio::test]
async fn tabix_bgzf_results_correct_and_ordered() -> datafusion::common::Result<()> {
    // End-to-end: verify that parallel bgzf reading produces correct
    // ordered results that match single-partition reading.
    let fixture = fixture_path("variation_tabix_bgzf");

    // Single partition (baseline)
    let mut options_1 = EnsemblCacheOptions::new(&fixture);
    options_1.target_partitions = Some(1);
    let provider_1 = VariationTableProvider::new(options_1)?;
    let ctx_1 = session_ctx_with_target_partitions(1);
    ctx_1.register_table("var", Arc::new(provider_1))?;
    let batches_1 = ctx_1
        .sql("SELECT start FROM var WHERE chrom = '1' ORDER BY start")
        .await?
        .collect()
        .await?;

    // Multiple partitions (parallel bgzf)
    let mut options_4 = EnsemblCacheOptions::new(&fixture);
    options_4.target_partitions = Some(4);
    let provider_4 = VariationTableProvider::new(options_4)?;
    let ctx_4 = session_ctx_with_target_partitions(4);
    ctx_4.register_table("var", Arc::new(provider_4))?;
    let batches_4 = ctx_4
        .sql("SELECT start FROM var WHERE chrom = '1' ORDER BY start")
        .await?
        .collect()
        .await?;

    let starts_1: Vec<i64> = batches_1
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();

    let starts_4: Vec<i64> = batches_4
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();

    // Both should have the same number of chr1 rows
    assert_eq!(
        starts_1.len(),
        starts_4.len(),
        "row count mismatch: single={} vs parallel={}",
        starts_1.len(),
        starts_4.len()
    );
    assert_eq!(starts_1.len(), 500, "expected 500 chr1 rows");

    // Results should be identical
    assert_eq!(
        starts_1, starts_4,
        "parallel results differ from single-partition"
    );

    // Verify ordering
    for window in starts_4.windows(2) {
        assert!(
            window[0] <= window[1],
            "results not ordered: {} > {}",
            window[0],
            window[1]
        );
    }

    Ok(())
}

#[tokio::test]
async fn tabix_bgzf_single_partition_no_split() -> datafusion::common::Result<()> {
    // With target_partitions=1, no bgzf splitting should occur.
    let mut options = EnsemblCacheOptions::new(fixture_path("variation_tabix_bgzf"));
    options.target_partitions = Some(1);
    let provider = VariationTableProvider::new(options)?;
    let ctx = session_ctx_with_target_partitions(1);
    ctx.register_table("variation", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT COUNT(*) FROM variation WHERE chrom = '1'")
        .await?;

    let batches = df.collect().await?;
    let count = first_i64(&batches);
    assert_eq!(count, 500, "expected 500 chr1 rows");

    Ok(())
}
