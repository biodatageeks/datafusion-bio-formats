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
    .await
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
    .await
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
#[ignore = "Requires valid BAM test file"]
async fn test_query_with_tag_projection() {
    // Test that we can actually query and project tag fields
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
    )
    .await
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
#[ignore = "Requires valid BAM test file"]
async fn test_query_without_tag_projection() {
    // Test that queries without tag projection work correctly
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
    )
    .await
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
#[ignore = "Requires valid BAM test file"]
async fn test_count_query() {
    // Test that COUNT(*) works correctly with tags in schema
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string()]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT COUNT(*) FROM bam").await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty());
    // Just verify query executes successfully
}

#[tokio::test]
async fn test_unknown_tag_accepted() {
    // Test that unknown tags are accepted and treated as Utf8 by default
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["UNKNOWN_TAG".to_string()]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core + 1 unknown tag = 12 fields
    assert_eq!(schema.fields().len(), 12);

    // Verify unknown tag field is present with Utf8 type
    let unknown_field = schema.field_with_name(None, "UNKNOWN_TAG").unwrap();
    assert_eq!(
        unknown_field.data_type(),
        &datafusion::arrow::datatypes::DataType::Utf8
    );
    assert!(unknown_field.is_nullable());

    // Verify metadata
    let metadata = unknown_field.metadata();
    assert_eq!(metadata.get("bio.bam.tag.tag").unwrap(), "UNKNOWN_TAG");
    assert_eq!(metadata.get("bio.bam.tag.type").unwrap(), "Z");
    assert_eq!(
        metadata.get("bio.bam.tag.description").unwrap(),
        "Unknown tag"
    );
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
    .await
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
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 11 core fields only
    assert_eq!(schema.fields().len(), 11);
}

#[tokio::test]
#[ignore = "Requires valid BAM test file"]
async fn test_describe_discovers_tags() {
    use datafusion::prelude::*;

    let provider = BamTableProvider::new("tests/rev_reads.bam".to_string(), None, None, true, None)
        .await
        .unwrap();

    let ctx = SessionContext::new();

    // Discover schema by reading 50 records
    let schema_df = provider.describe(&ctx, Some(50)).await.unwrap();
    let results = schema_df.collect().await.unwrap();

    assert!(!results.is_empty());
    let batch = &results[0];

    // Should have at least 11 rows (core fields)
    assert!(batch.num_rows() >= 11);

    // Verify schema columns
    assert_eq!(batch.schema().field(0).name(), "column_name");
    assert_eq!(batch.schema().field(1).name(), "data_type");
    assert_eq!(batch.schema().field(2).name(), "nullable");
    assert_eq!(batch.schema().field(3).name(), "category");
    assert_eq!(batch.schema().field(4).name(), "sam_type");
    assert_eq!(batch.schema().field(5).name(), "description");

    // Check that core fields are present
    use datafusion::arrow::array::{Array, StringArray};
    let column_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let has_core_field = (0..column_names.len()).any(|i| column_names.value(i) == "name");
    assert!(has_core_field, "Should have core 'name' field");

    // Check that discovered tags are marked correctly
    let categories = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let tag_count = (0..categories.len())
        .filter(|&i| categories.value(i) == "tag")
        .count();

    println!("Discovered {} tags in sample", tag_count);
    // Should discover at least some tags if present in data
}

#[tokio::test]
#[ignore = "Requires valid BAM test file"]
async fn test_describe_with_display() {
    use datafusion::prelude::*;

    let provider = BamTableProvider::new("tests/rev_reads.bam".to_string(), None, None, true, None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    let schema_df = provider.describe(&ctx, Some(100)).await.unwrap();

    // Should be able to display the DataFrame
    schema_df.show().await.unwrap();
}

#[tokio::test]
#[ignore = "Requires valid BAM test file"]
async fn test_describe_method_signature() {
    // Test that describe method exists with correct signature
    use datafusion::prelude::*;

    let provider = BamTableProvider::new("tests/rev_reads.bam".to_string(), None, None, true, None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    let result = provider.describe(&ctx, Some(10)).await;
    assert!(result.is_ok(), "Should successfully describe BAM schema");
}
