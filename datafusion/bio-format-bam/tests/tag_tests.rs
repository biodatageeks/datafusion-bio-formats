use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::test]
async fn test_bam_without_tags() {
    // Test that BAM table works without any tag fields (baseline)
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        None, // No tags
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core fields only
    assert_eq!(schema.fields().len(), 11);
    assert!(schema.field_with_name(None, "name").is_ok());
    assert!(schema.field_with_name(None, "chrom").is_ok());
    assert!(schema.field_with_name(None, "start").is_ok());
}

#[tokio::test]
async fn test_bam_with_specified_tags() {
    // Test that specifying tags adds them to the schema
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core fields + 2 tag fields = 13
    assert_eq!(schema.fields().len(), 13);

    // Verify tag fields are present
    assert!(schema.field_with_name(None, "NM").is_ok());
    assert!(schema.field_with_name(None, "MD").is_ok());

    // Verify tag field metadata
    let nm_field = schema.field_with_name(None, "NM").unwrap();
    assert_eq!(
        nm_field.data_type(),
        &datafusion::arrow::datatypes::DataType::Int32
    );
    assert!(nm_field.is_nullable());
}

#[tokio::test]
async fn test_query_with_tag_projection() {
    // Test that we can actually query and project tag fields
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Query with tag projection
    let df = ctx
        .sql("SELECT name, chrom, \"NM\" FROM bam LIMIT 5")
        .await
        .unwrap();

    let results = df.collect().await.unwrap();
    assert!(!results.is_empty());

    // Should have 3 columns
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 3);
    }
}

#[tokio::test]
async fn test_query_without_tag_projection() {
    // Test that queries without tag projection work correctly
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Query only core fields - tags should not be parsed
    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM bam LIMIT 5")
        .await
        .unwrap();

    let results = df.collect().await.unwrap();
    assert!(!results.is_empty());

    // Should have 3 columns (not including tags)
    if let Some(batch) = results.first() {
        assert_eq!(batch.num_columns(), 3);
    }
}

#[tokio::test]
async fn test_count_query() {
    // Test that COUNT(*) works correctly with tags in schema
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string()]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT COUNT(*) FROM bam").await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty());
    // Just verify query executes successfully
}

#[tokio::test]
async fn test_unknown_tag_error() {
    // Test that unknown tags produce a helpful error message
    let result = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["INVALID_TAG".to_string()]),
    );

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown BAM tag"));
    assert!(err.contains("Available tags:"));
}

#[tokio::test]
async fn test_multiple_tags() {
    // Test that multiple tags can be specified
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "AS".to_string(),
            "RG".to_string(),
        ]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core + 4 tag fields = 15
    assert_eq!(schema.fields().len(), 15);
    assert!(schema.field_with_name(None, "NM").is_ok());
    assert!(schema.field_with_name(None, "MD").is_ok());
    assert!(schema.field_with_name(None, "AS").is_ok());
    assert!(schema.field_with_name(None, "RG").is_ok());
}

#[tokio::test]
async fn test_empty_tag_list() {
    // Test that Some(vec![]) behaves like None (no tags)
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec![]),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core fields only
    assert_eq!(schema.fields().len(), 11);
}
