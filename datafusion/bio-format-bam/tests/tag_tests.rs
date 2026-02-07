use datafusion::arrow::array::Array;
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

/// All 13 tags present in the bam_with_tags.bam test file
const ALL_TAGS: [&str; 13] = [
    "E2", "MD", "MQ", "NM", "OC", "OP", "OQ", "PG", "RG", "UQ", "XN", "XT", "ZQ",
];

#[tokio::test]
async fn test_read_all_13_tags() {
    // Tests reading all 13 tags from a real BAM file extracted from NA12878 WES data.
    // This file contains 14 reads with a variety of tags including:
    // - Integer tags stored as Int8 in BAM binary (MQ, NM, UQ, XN, XT)
    // - String tags (E2, MD, OC, OQ, PG, RG, ZQ)
    // - XT tag that is defined as type 'A' (character) in SAM spec but encoded as 'c' (Int8) in BAM
    // - OP tag stored as Int32
    // - ZQ tag not in standard registry (defaults to Utf8)
    let tag_fields: Vec<String> = ALL_TAGS.iter().map(|s| s.to_string()).collect();

    let provider = BamTableProvider::new(
        "tests/bam_with_tags.bam".to_string(),
        None,
        None,
        true, // 0-based coordinates
        Some(tag_fields),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Verify schema: 11 core + 13 tags = 24 fields
    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();
    assert_eq!(schema.fields().len(), 24);

    // Verify all tag fields are in schema with expected types
    use datafusion::arrow::datatypes::DataType;
    let nm_field = schema.field_with_name(None, "NM").unwrap();
    assert_eq!(nm_field.data_type(), &DataType::Int32);
    let md_field = schema.field_with_name(None, "MD").unwrap();
    assert_eq!(md_field.data_type(), &DataType::Utf8);
    let xt_field = schema.field_with_name(None, "XT").unwrap();
    assert_eq!(xt_field.data_type(), &DataType::Utf8); // SAM type 'A' -> Utf8
    let mq_field = schema.field_with_name(None, "MQ").unwrap();
    assert_eq!(mq_field.data_type(), &DataType::Int32);
    let rg_field = schema.field_with_name(None, "RG").unwrap();
    assert_eq!(rg_field.data_type(), &DataType::Utf8);
    let op_field = schema.field_with_name(None, "OP").unwrap();
    assert_eq!(op_field.data_type(), &DataType::Int32);

    // Read all records and verify no errors (the main bug was type mismatch causing errors)
    let df = ctx
        .sql("SELECT name, chrom, start, \"NM\", \"MD\", \"MQ\", \"XT\", \"RG\", \"PG\", \"UQ\", \"OQ\", \"E2\", \"OC\", \"OP\", \"XN\", \"ZQ\" FROM bam")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 14, "Should have 14 reads");
}

#[tokio::test]
async fn test_xt_tag_character_values() {
    // Specifically tests that the XT tag (SAM type 'A' / character) is correctly
    // read even when the BAM binary encodes it as Int8 instead of Character.
    // The XT tag values should be ASCII characters like 'S', 'V', 'W'.
    let provider = BamTableProvider::new(
        "tests/bam_with_tags.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["XT".to_string()]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Query only records that have XT
    let df = ctx
        .sql("SELECT name, \"XT\" FROM bam WHERE \"XT\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    use datafusion::arrow::array::StringArray;
    let mut xt_values: Vec<String> = Vec::new();
    for batch in &results {
        let xt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..xt_col.len() {
            if !xt_col.is_null(i) {
                xt_values.push(xt_col.value(i).to_string());
            }
        }
    }

    // Should have XT values (reads with XT in the test file)
    assert!(!xt_values.is_empty(), "Should find reads with XT tag");

    // All XT values should be single ASCII characters
    for val in &xt_values {
        assert_eq!(
            val.len(),
            1,
            "XT value '{}' should be single character",
            val
        );
        assert!(
            val.chars().next().unwrap().is_ascii(),
            "XT value '{}' should be ASCII",
            val,
        );
    }
}

#[tokio::test]
async fn test_integer_tags_from_int8_encoding() {
    // Tests that integer tags encoded as Int8 in BAM binary (NM, MQ, UQ) are
    // correctly read into Int32 Arrow columns.
    let provider = BamTableProvider::new(
        "tests/bam_with_tags.bam".to_string(),
        None,
        None,
        true,
        Some(vec!["NM".to_string(), "MQ".to_string(), "UQ".to_string()]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT \"NM\", \"MQ\", \"UQ\" FROM bam WHERE \"NM\" IS NOT NULL")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    use datafusion::arrow::array::Int32Array;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Should have reads with NM tag");

    // Verify values are reasonable
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
                    nm >= 0 && nm < 100,
                    "NM={} should be a small non-negative integer",
                    nm
                );
            }
        }
    }
}

#[tokio::test]
async fn test_nullable_tags_with_mixed_presence() {
    // Tests that tags which are only present in some reads produce correct NULL values.
    // In the test file: XT is in ~5 reads, XN is in ~2 reads, OC is in ~2 reads.
    let provider = BamTableProvider::new(
        "tests/bam_with_tags.bam".to_string(),
        None,
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "XT".to_string(),
            "XN".to_string(),
            "OC".to_string(),
        ]),
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT \"NM\", \"XT\", \"XN\", \"OC\" FROM bam")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 14);

    // Count non-null values for each tag
    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    let mut nm_count = 0;
    let mut xt_count = 0;
    let mut xn_count = 0;
    let mut oc_count = 0;
    for batch in &results {
        let nm_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let xt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let xn_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let oc_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if !nm_col.is_null(i) {
                nm_count += 1;
            }
            if !xt_col.is_null(i) {
                xt_count += 1;
            }
            if !xn_col.is_null(i) {
                xn_count += 1;
            }
            if !oc_col.is_null(i) {
                oc_count += 1;
            }
        }
    }

    // NM is present in most reads (12 of 14 mapped reads have it)
    assert!(
        nm_count >= 10,
        "NM should be present in most reads, got {}",
        nm_count
    );
    // XT is present in fewer reads (5 of 14)
    assert!(
        xt_count >= 3 && xt_count < 14,
        "XT should be present in some reads, got {}",
        xt_count
    );
    // XN is rare (2 of 14)
    assert!(
        xn_count >= 1 && xn_count <= 5,
        "XN should be present in few reads, got {}",
        xn_count
    );
    // OC is rare (2 of 14)
    assert!(
        oc_count >= 1 && oc_count <= 5,
        "OC should be present in few reads, got {}",
        oc_count
    );
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
