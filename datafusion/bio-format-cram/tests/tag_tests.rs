use datafusion::arrow::array::Array;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use std::sync::Arc;

/// Helper: execute a SQL query and return total row count across all batches.
async fn count_rows(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.expect("SQL execution failed");
    let batches = df.collect().await.expect("collect failed");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Test reading CRAM with tags — verifies schema has extra tag columns and values are populated.
///
/// Uses multi_chrom.cram (421 reads, 9 tags per record: MQ, PG, UQ, NM, OQ, E2, ZQ, MD, RG).
#[tokio::test]
async fn test_cram_read_with_tags() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None, // no_ref mode
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "MQ".to_string(),
            "RG".to_string(),
        ]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    // Verify schema: 12 core + 4 tag fields = 16
    let df = ctx.table("cram").await.unwrap();
    let schema = df.schema();
    assert_eq!(schema.fields().len(), 16);

    // Query all records with tags
    let total = count_rows(
        &ctx,
        "SELECT chrom, start, \"NM\", \"MD\", \"MQ\", \"RG\" FROM cram",
    )
    .await;
    assert_eq!(total, 421, "Should have 421 reads");

    // Verify NM values are small non-negative integers
    use datafusion::arrow::array::Int32Array;
    let df = ctx
        .sql("SELECT \"NM\" FROM cram WHERE \"NM\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let nm_count: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(
        nm_count > 400,
        "NM should be present in most reads, got {nm_count}"
    );

    for batch in &results {
        let nm_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..nm_col.len() {
            if !nm_col.is_null(i) {
                let nm = nm_col.value(i);
                assert!(
                    (0..100).contains(&nm),
                    "NM={nm} should be a small non-negative integer"
                );
            }
        }
    }

    // Verify MD values are non-empty strings
    use datafusion::arrow::array::StringArray;
    let df = ctx
        .sql("SELECT \"MD\" FROM cram WHERE \"MD\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let md_count: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(
        md_count > 400,
        "MD should be present in most reads, got {md_count}"
    );

    for batch in &results {
        let md_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..md_col.len() {
            if !md_col.is_null(i) {
                let md = md_col.value(i);
                assert!(!md.is_empty(), "MD value should not be empty");
            }
        }
    }

    // Verify RG is present in all reads
    let rg_count = count_rows(&ctx, "SELECT \"RG\" FROM cram WHERE \"RG\" IS NOT NULL").await;
    assert_eq!(rg_count, 421, "RG should be present in all reads");

    // Verify MQ is present in most reads
    let mq_count = count_rows(&ctx, "SELECT \"MQ\" FROM cram WHERE \"MQ\" IS NOT NULL").await;
    assert!(
        mq_count > 400,
        "MQ should be present in most reads, got {mq_count}"
    );
}

/// Test nullable tags with mixed presence across records.
#[tokio::test]
async fn test_cram_nullable_tags_with_mixed_presence() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MQ".to_string(), "E2".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    let total = count_rows(&ctx, "SELECT chrom FROM cram").await;
    assert_eq!(total, 421);

    // Count non-null values for each tag
    let nm_count = count_rows(&ctx, "SELECT \"NM\" FROM cram WHERE \"NM\" IS NOT NULL").await;
    let mq_count = count_rows(&ctx, "SELECT \"MQ\" FROM cram WHERE \"MQ\" IS NOT NULL").await;
    let e2_count = count_rows(&ctx, "SELECT \"E2\" FROM cram WHERE \"E2\" IS NOT NULL").await;

    // NM and MQ should be present in most reads
    assert!(
        nm_count > 400,
        "NM should be present in most reads, got {nm_count}"
    );
    assert!(
        mq_count > 400,
        "MQ should be present in most reads, got {mq_count}"
    );

    // E2 may be present in some or all — just verify the query runs and count is reasonable
    assert!(e2_count <= 421, "E2 count should not exceed total reads");
}

/// Test that projection pushdown excludes tags when only core fields are selected.
#[tokio::test]
async fn test_cram_tag_projection_pushdown() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "MQ".to_string(),
            "RG".to_string(),
        ]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    // Query only core fields — tags should not be in output
    let df = ctx.sql("SELECT chrom, start FROM cram").await.unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 421);

    // Should have 2 columns only (tags not projected)
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 2);
    }
}

/// Test tag reading with a chromosome filter.
#[tokio::test]
async fn test_cram_tag_with_filter() {
    let provider = CramTableProvider::new(
        "tests/multi_chrom.cram".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT chrom, \"NM\" FROM cram WHERE chrom = 'chr1'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 160, "chr1 should have 160 reads");

    // Verify result has 2 columns
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 2);
    }
}

// --- Nanopore custom tag inference tests (CRAM parity with BAM) ---

const NANOPORE_CRAM: &str = "tests/nanopore_custom_tags.cram";

#[tokio::test]
async fn test_cram_unknown_integer_tag_inferred() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let schema = provider.schema();
    let pt_field = schema.field_with_name("pt").unwrap();
    assert_eq!(
        pt_field.data_type(),
        &datafusion::arrow::datatypes::DataType::Int32,
        "pt should be inferred as Int32"
    );

    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();
    let df = ctx
        .sql("SELECT \"pt\" FROM cram WHERE \"pt\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total > 0, "Should have records with pt tag");

    use datafusion::arrow::array::Int32Array;
    for batch in &results {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            if !col.is_null(i) {
                let v = col.value(i);
                assert!(v >= 0, "pt value {v} should be non-negative");
            }
        }
    }
}

#[tokio::test]
async fn test_cram_unknown_float_tag_inferred() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["de".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let schema = provider.schema();
    let de_field = schema.field_with_name("de").unwrap();
    assert_eq!(
        de_field.data_type(),
        &datafusion::arrow::datatypes::DataType::Float32,
        "de should be inferred as Float32"
    );
}

#[tokio::test]
async fn test_cram_mixed_known_and_unknown_tags() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "pt".to_string(),
            "de".to_string(),
        ]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    use datafusion::arrow::datatypes::DataType;
    let schema = provider.schema();
    assert_eq!(
        schema.field_with_name("NM").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("MD").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("pt").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("de").unwrap().data_type(),
        &DataType::Float32
    );
}

#[tokio::test]
async fn test_cram_all_nanopore_tags_inferred() {
    // Note: pa (B:i array) is excluded — CRAM's noodles decoder doesn't reliably
    // round-trip B-type array tags. BAM test covers pa array inference.
    let tag_fields: Vec<String> = [
        "qs", "du", "ns", "ts", "mx", "ch", "st", "rn", "fn", "sm", "sd", "sv", "dx", "RG", "NM",
        "ms", "AS", "nn", "de", "tp", "cm", "s1", "MD", "rl", "pt",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(tag_fields),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    use datafusion::arrow::datatypes::DataType;
    let schema = provider.schema();

    // 12 core + 25 tags = 37 fields
    assert_eq!(schema.fields().len(), 37);

    // Known SAM spec tags
    assert_eq!(
        schema.field_with_name("NM").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("MD").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("AS").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("RG").unwrap().data_type(),
        &DataType::Utf8
    );

    // Inferred integer tags
    for tag in &[
        "pt", "ch", "cm", "dx", "ms", "mx", "nn", "ns", "rl", "rn", "ts", "s1",
    ] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Int32,
            "Tag {tag} should be Int32"
        );
    }

    // Inferred float tags
    for tag in &["de", "du", "qs", "sd", "sm"] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Float32,
            "Tag {tag} should be Float32"
        );
    }

    // Inferred string tags
    for tag in &["fn", "st", "sv"] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Utf8,
            "Tag {tag} should be Utf8"
        );
    }

    // Verify data can be read
    let ctx = SessionContext::new();
    ctx.register_table("cram", Arc::new(provider)).unwrap();
    let df = ctx.sql("SELECT \"pt\", \"de\" FROM cram").await.unwrap();
    let results = df.collect().await.unwrap();
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total >= 20, "Should have at least 20 reads, got {total}");
}

#[tokio::test]
async fn test_cram_inference_disabled_falls_back_to_utf8() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false, // inference disabled
        100,
        None,
    )
    .await
    .unwrap();

    let schema = provider.schema();
    assert_eq!(
        schema.field_with_name("pt").unwrap().data_type(),
        &datafusion::arrow::datatypes::DataType::Utf8,
        "pt should fall back to Utf8 when inference is disabled"
    );
}

#[tokio::test]
async fn test_cram_tag_type_hints_override_default() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false,
        100,
        Some(vec!["pt:i".to_string()]),
    )
    .await
    .unwrap();

    let schema = provider.schema();
    assert_eq!(
        schema.field_with_name("pt").unwrap().data_type(),
        &datafusion::arrow::datatypes::DataType::Int32,
        "pt should be Int32 from type hint"
    );
}

#[tokio::test]
async fn test_cram_inference_overrides_hints() {
    let provider = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        true,
        100,
        Some(vec!["pt:Z".to_string()]),
    )
    .await
    .unwrap();

    let schema = provider.schema();
    assert_eq!(
        schema.field_with_name("pt").unwrap().data_type(),
        &datafusion::arrow::datatypes::DataType::Int32,
        "Inference (Int32) should override hint (Utf8)"
    );
}

#[tokio::test]
async fn test_cram_invalid_hint_format_error() {
    let result = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false,
        100,
        Some(vec!["pt".to_string()]),
    )
    .await;
    assert!(result.is_err(), "Should error on malformed hint 'pt'");

    let result = CramTableProvider::new(
        NANOPORE_CRAM.to_string(),
        None,
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false,
        100,
        Some(vec!["pt:X:extra".to_string()]),
    )
    .await;
    assert!(
        result.is_err(),
        "Should error on malformed hint 'pt:X:extra'"
    );
}
