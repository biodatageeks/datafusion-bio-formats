use datafusion::arrow::array::Array;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::test]
async fn test_bam_without_tags() {
    // Test that BAM table works without any tag fields (baseline)
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true,
        None,  // No tags
        false, // String CIGAR (default)
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 12 core fields only
    assert_eq!(schema.fields().len(), 12);
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
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 12 core fields + 2 tag fields = 14
    assert_eq!(schema.fields().len(), 14);

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
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
        false,
        true,
        100,
        None,
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
        true,
        Some(vec!["NM".to_string(), "MD".to_string()]),
        false,
        true,
        100,
        None,
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
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT COUNT(*) FROM bam").await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty());
    // Just verify query executes successfully
}

#[tokio::test]
async fn test_unknown_tag_accepted() {
    // Test that unknown tags are accepted and treated as Utf8 by default
    // Uses infer_tag_types=false to test the static fallback path
    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true,
        Some(vec!["UNKNOWN_TAG".to_string()]),
        false,
        false,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 12 core + 1 unknown tag = 13 fields
    assert_eq!(schema.fields().len(), 13);

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
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "AS".to_string(),
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
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 12 core + 4 tag fields = 16
    assert_eq!(schema.fields().len(), 16);
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
        true,
        Some(vec![]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();

    // Should have 12 core fields only
    assert_eq!(schema.fields().len(), 12);
}

/// All 14 tags present in the 10x_pbmc_tags.bam test file
const TENX_TAGS: [&str; 14] = [
    "NH", "HI", "AS", "nM", "ts", "RG", "RE", "xf", "CR", "CY", "CB", "UR", "UY", "UB",
];

#[tokio::test]
async fn test_read_10x_genomics_tags() {
    // Tests reading all 14 tags from a 10X Genomics Cell Ranger BAM file.
    // This file contains 10 reads with single-cell barcodes, UMIs, and 10X-specific tags.
    // Key characteristics:
    // - RG values start with "10k_..." (numeric prefix that can cause SQL parsing issues)
    // - RE tag is type 'A' (character) with values like 'I' (intronic)
    // - ts tag is only present in some reads (5 of 10), testing nullable handling
    // - nM and xf are lowercase/mixed-case tag names (10X-specific)
    let tag_fields: Vec<String> = TENX_TAGS.iter().map(|s| s.to_string()).collect();

    let provider = BamTableProvider::new(
        "tests/10x_pbmc_tags.bam".to_string(),
        None,
        true, // 0-based coordinates
        Some(tag_fields),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Verify schema: 12 core + 14 tags = 26 fields
    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();
    assert_eq!(
        schema.fields().len(),
        26,
        "Expected 12 core + 14 tag fields = 26"
    );

    // Verify tag field types
    use datafusion::arrow::datatypes::DataType;

    // Integer tags
    for tag in &["NH", "HI", "AS", "nM", "ts", "xf"] {
        let field = schema.field_with_name(None, tag).unwrap();
        assert_eq!(
            field.data_type(),
            &DataType::Int32,
            "Tag {tag} should be Int32"
        );
    }

    // String tags
    for tag in &["RG", "CR", "CY", "CB", "UR", "UY", "UB"] {
        let field = schema.field_with_name(None, tag).unwrap();
        assert_eq!(
            field.data_type(),
            &DataType::Utf8,
            "Tag {tag} should be Utf8"
        );
    }

    // Character tag (RE) → stored as Utf8
    let re_field = schema.field_with_name(None, "RE").unwrap();
    assert_eq!(re_field.data_type(), &DataType::Utf8);

    // Read all records with all tags projected
    let df = ctx
        .sql(
            "SELECT name, chrom, start, \
             \"NH\", \"HI\", \"AS\", \"nM\", \"ts\", \
             \"RG\", \"RE\", \"xf\", \
             \"CR\", \"CY\", \"CB\", \"UR\", \"UY\", \"UB\" \
             FROM bam",
        )
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10, "Should have 10 reads");

    // Verify RG values contain the "10k_..." prefix (the problematic pattern from polars-bio#319)
    use datafusion::arrow::array::StringArray;
    for batch in &results {
        let rg_col = batch
            .column_by_name("RG")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..rg_col.len() {
            if !rg_col.is_null(i) {
                let rg_val = rg_col.value(i);
                assert!(
                    rg_val.starts_with("10k_"),
                    "RG value '{rg_val}' should start with '10k_'"
                );
            }
        }
    }

    // Verify CB/CR barcode values are non-null and look like barcodes (16bp + suffix)
    for batch in &results {
        let cb_col = batch
            .column_by_name("CB")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cr_col = batch
            .column_by_name("CR")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !cb_col.is_null(i) {
                let cb = cb_col.value(i);
                assert!(cb.len() >= 16, "CB '{cb}' should be at least 16 chars");
                assert!(
                    cb.chars()
                        .all(|c| c.is_ascii_alphabetic() || c == '-' || c.is_ascii_digit()),
                    "CB '{cb}' should contain only ACGT and barcode suffix chars"
                );
            }
            if !cr_col.is_null(i) {
                let cr = cr_col.value(i);
                assert!(cr.len() >= 16, "CR '{cr}' should be at least 16 chars");
            }
        }
    }

    // Verify RE character tag values are readable single characters
    for batch in &results {
        let re_col = batch
            .column_by_name("RE")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..re_col.len() {
            if !re_col.is_null(i) {
                let re_val = re_col.value(i);
                assert_eq!(
                    re_val.len(),
                    1,
                    "RE value '{re_val}' should be a single character"
                );
                assert!(
                    re_val.chars().next().unwrap().is_ascii_alphabetic(),
                    "RE value '{re_val}' should be an ASCII letter"
                );
            }
        }
    }

    // Verify ts tag is nullable (present in only some reads)
    use datafusion::arrow::array::Int32Array;
    let mut ts_non_null = 0;
    let mut ts_null = 0;
    for batch in &results {
        let ts_col = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..ts_col.len() {
            if ts_col.is_null(i) {
                ts_null += 1;
            } else {
                ts_non_null += 1;
            }
        }
    }
    assert!(
        ts_non_null > 0 && ts_null > 0,
        "ts tag should be present in some reads ({ts_non_null}) and absent in others ({ts_null})"
    );
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
        true, // 0-based coordinates
        Some(tag_fields),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();

    // Verify schema: 12 core + 13 tags = 25 fields
    let df = ctx.table("bam").await.unwrap();
    let schema = df.schema();
    assert_eq!(schema.fields().len(), 25);

    // Verify all tag fields are in schema with expected types
    use datafusion::arrow::datatypes::DataType;
    let nm_field = schema.field_with_name(None, "NM").unwrap();
    assert_eq!(nm_field.data_type(), &DataType::Int32);
    let md_field = schema.field_with_name(None, "MD").unwrap();
    assert_eq!(md_field.data_type(), &DataType::Utf8);
    let xt_field = schema.field_with_name(None, "XT").unwrap();
    // XT is not in the SAM spec registry; inferred from BAM binary as Int32 (encoded as Int8)
    assert_eq!(xt_field.data_type(), &DataType::Int32);
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
    // XT is not in the SAM spec registry; it's inferred from the BAM binary.
    // BAM encodes XT as Int8, so with inference it's detected as Int32.
    // The values are ASCII codes for characters like 'S' (83), 'U' (85), etc.
    let provider = BamTableProvider::new(
        "tests/bam_with_tags.bam".to_string(),
        None,
        true,
        Some(vec!["XT".to_string()]),
        false,
        true,
        100,
        None,
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

    use datafusion::arrow::array::Int32Array;
    let mut xt_values: Vec<i32> = Vec::new();
    for batch in &results {
        let xt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..xt_col.len() {
            if !xt_col.is_null(i) {
                xt_values.push(xt_col.value(i));
            }
        }
    }

    // Should have XT values (reads with XT in the test file)
    assert!(!xt_values.is_empty(), "Should find reads with XT tag");

    // All XT values should be valid ASCII character codes
    for val in &xt_values {
        assert!(
            (32..=126).contains(val),
            "XT value {val} should be a printable ASCII code",
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
        true,
        Some(vec!["NM".to_string(), "MQ".to_string(), "UQ".to_string()]),
        false,
        true,
        100,
        None,
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
                    (0..100).contains(&nm),
                    "NM={nm} should be a small non-negative integer"
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
        true,
        Some(vec![
            "NM".to_string(),
            "XT".to_string(),
            "XN".to_string(),
            "OC".to_string(),
        ]),
        false,
        true,
        100,
        None,
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
            .downcast_ref::<Int32Array>()
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
        "NM should be present in most reads, got {nm_count}"
    );
    // XT is present in fewer reads (5 of 14)
    assert!(
        (3..14).contains(&xt_count),
        "XT should be present in some reads, got {xt_count}"
    );
    // XN is rare (2 of 14)
    assert!(
        (1..=5).contains(&xn_count),
        "XN should be present in few reads, got {xn_count}"
    );
    // OC is rare (2 of 14)
    assert!(
        (1..=5).contains(&oc_count),
        "OC should be present in few reads, got {oc_count}"
    );
}

#[tokio::test]
#[ignore = "Requires valid BAM test file"]
async fn test_describe_discovers_tags() {
    use datafusion::prelude::*;

    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
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

    println!("Discovered {tag_count} tags in sample");
    // Should discover at least some tags if present in data
}

#[tokio::test]
#[ignore = "Requires valid BAM test file"]
async fn test_describe_with_display() {
    use datafusion::prelude::*;

    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
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

    let provider = BamTableProvider::new(
        "tests/rev_reads.bam".to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    let result = provider.describe(&ctx, Some(10)).await;
    assert!(result.is_ok(), "Should successfully describe BAM schema");
}

// --- Nanopore custom tag inference tests ---

const NANOPORE_BAM: &str = "tests/nanopore_custom_tags.bam";

#[tokio::test]
async fn test_unknown_integer_tag_inferred_by_new() {
    // pt (poly-T tail length) is a nanopore-specific integer tag not in SAM spec registry.
    // With infer_tag_types=true, it should be detected as Int32 from the file.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
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

    // Verify actual values are integers
    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();
    let df = ctx
        .sql("SELECT \"pt\" FROM bam WHERE \"pt\" IS NOT NULL")
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
async fn test_unknown_float_tag_inferred_by_new() {
    // de (error rate) is a nanopore-specific float tag.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
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
async fn test_unknown_array_tag_inferred_by_new() {
    // pa (poly-A boundaries) is a nanopore-specific B:i array tag.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pa".to_string()]),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let schema = provider.schema();
    let pa_field = schema.field_with_name("pa").unwrap();
    assert!(
        matches!(
            pa_field.data_type(),
            datafusion::arrow::datatypes::DataType::List(_)
        ),
        "pa should be inferred as List, got {:?}",
        pa_field.data_type()
    );
}

#[tokio::test]
async fn test_mixed_known_and_unknown_tags() {
    // Mix of SAM spec tags (NM, MD) and nanopore custom tags (pt, de, pa).
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec![
            "NM".to_string(),
            "MD".to_string(),
            "pt".to_string(),
            "de".to_string(),
            "pa".to_string(),
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
    assert!(matches!(
        schema.field_with_name("pa").unwrap().data_type(),
        DataType::List(_)
    ));
}

/// All 26 tags in the nanopore test BAM
const NANOPORE_TAGS: [&str; 26] = [
    "qs", "du", "ns", "ts", "mx", "ch", "st", "rn", "fn", "sm", "sd", "sv", "dx", "RG", "NM", "ms",
    "AS", "nn", "de", "tp", "cm", "s1", "MD", "rl", "pt", "pa",
];

#[tokio::test]
async fn test_all_nanopore_tags_inferred() {
    let tag_fields: Vec<String> = NANOPORE_TAGS.iter().map(|s| s.to_string()).collect();
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
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

    // 12 core + 26 tags = 38 fields
    assert_eq!(schema.fields().len(), 38);

    // Verify known SAM spec tags
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

    // Verify inferred nanopore integer tags
    for tag in &[
        "pt", "ch", "cm", "dx", "ms", "mx", "nn", "ns", "rl", "rn", "ts", "s1",
    ] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Int32,
            "Tag {tag} should be Int32"
        );
    }

    // Verify inferred nanopore float tags
    for tag in &["de", "du", "qs", "sd", "sm"] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Float32,
            "Tag {tag} should be Float32"
        );
    }

    // Verify inferred nanopore string tags
    for tag in &["fn", "st", "sv"] {
        assert_eq!(
            schema.field_with_name(tag).unwrap().data_type(),
            &DataType::Utf8,
            "Tag {tag} should be Utf8"
        );
    }

    // tp is type 'A' (character) → Utf8
    assert_eq!(
        schema.field_with_name("tp").unwrap().data_type(),
        &DataType::Utf8
    );

    // pa is B:i array → List<Int32>
    assert!(matches!(
        schema.field_with_name("pa").unwrap().data_type(),
        DataType::List(_)
    ));

    // Verify data can actually be read
    let ctx = SessionContext::new();
    ctx.register_table("bam", Arc::new(provider)).unwrap();
    let df = ctx
        .sql("SELECT \"pt\", \"de\", \"pa\" FROM bam")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 20);
}

#[tokio::test]
async fn test_inferred_schema_matches_try_new() {
    // new() with infer_tag_types=true and try_new_with_inferred_schema() should produce
    // identical types for the same tags.
    let tags = vec!["NM".to_string(), "pt".to_string(), "de".to_string()];

    let provider_new = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(tags.clone()),
        false,
        true,
        100,
        None,
    )
    .await
    .unwrap();

    let provider_inferred = BamTableProvider::try_new_with_inferred_schema(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(tags),
        Some(100),
        false,
    )
    .await
    .unwrap();

    for tag in &["NM", "pt", "de"] {
        let t1 = provider_new
            .schema()
            .field_with_name(tag)
            .unwrap()
            .data_type()
            .clone();
        let t2 = provider_inferred
            .schema()
            .field_with_name(tag)
            .unwrap()
            .data_type()
            .clone();
        assert_eq!(
            t1, t2,
            "Type mismatch for tag {tag}: new={t1:?} vs inferred={t2:?}"
        );
    }
}

#[tokio::test]
async fn test_inference_disabled_falls_back_to_utf8() {
    // With infer_tag_types=false, unknown tags should default to Utf8 (old behavior).
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
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
    let pt_field = schema.field_with_name("pt").unwrap();
    assert_eq!(
        pt_field.data_type(),
        &datafusion::arrow::datatypes::DataType::Utf8,
        "pt should fall back to Utf8 when inference is disabled"
    );
}

#[tokio::test]
async fn test_custom_sample_size() {
    // Inference with a small sample size should still work.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pt".to_string(), "de".to_string()]),
        false,
        true,
        5, // Only sample 5 records
        None,
    )
    .await
    .unwrap();

    use datafusion::arrow::datatypes::DataType;
    let schema = provider.schema();
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
async fn test_tag_type_hints_override_default() {
    // With inference disabled, tag_type_hints should provide the type.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false, // inference disabled
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
async fn test_inference_overrides_hints() {
    // Inference should win over hints: hint says Utf8, but file says Int32.
    let provider = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        true, // inference enabled
        100,
        Some(vec!["pt:Z".to_string()]), // hint says string
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
async fn test_invalid_hint_format_error() {
    // Malformed hints should return an error.
    let result = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false,
        100,
        Some(vec!["pt".to_string()]), // missing :TYPE
    )
    .await;
    assert!(result.is_err(), "Should error on malformed hint 'pt'");

    let result = BamTableProvider::new(
        NANOPORE_BAM.to_string(),
        None,
        true,
        Some(vec!["pt".to_string()]),
        false,
        false,
        100,
        Some(vec!["pt:X:extra".to_string()]), // too many colons
    )
    .await;
    assert!(
        result.is_err(),
        "Should error on malformed hint 'pt:X:extra'"
    );
}
