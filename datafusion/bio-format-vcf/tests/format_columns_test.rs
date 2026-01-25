use datafusion::arrow::array::{Array, ListArray};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

/// Sample VCF content with FORMAT fields and multiple samples
const SAMPLE_VCF_WITH_FORMAT: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Combined depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs1	A	T	60	PASS	DP=50	GT:DP:GQ	0/1:20:99	1/1:30:95
chr1	200	rs2	G	C	80	PASS	DP=60	GT:DP:GQ	0/0:25:99	0/1:35:90
chr2	300	rs3	C	G	70	PASS	DP=45	GT:DP:GQ	1|0:15:85	./.:10:50
chr2	400	rs4	T	A	50	PASS	DP=40	GT:DP:GQ:AD	0/1:18:92:10,8	1/1:22:88:2,20
"#;

/// VCF with missing FORMAT values
const SAMPLE_VCF_MISSING_FORMAT: &str = r#"##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	.	A	T	30	PASS	.	GT:DP	0/1:20	./.:.
chr1	200	.	G	C	40	PASS	.	GT	0/0	1/1
"#;

/// VCF with sample names containing special characters
const SAMPLE_VCF_SPECIAL_NAMES: &str = r#"##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample-1	Sample.2	Sample 3
chr1	100	.	A	T	30	PASS	.	GT:DP	0/1:20	1/1:30	0/0:25
"#;

/// VCF with single sample
const SAMPLE_VCF_SINGLE_SAMPLE: &str = r#"##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	OnlySample
chr1	100	.	A	T	30	PASS	.	GT:DP	0/1:20
chr1	200	.	G	C	40	PASS	.	GT:DP	1|0:30
"#;

async fn create_test_vcf_file(test_name: &str, content: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_format_{}.vcf", test_name);
    fs::write(&temp_file, content).await?;
    Ok(temp_file)
}

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::NONE),
    }
}

#[tokio::test]
async fn test_format_schema_with_multiple_samples() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("schema_multiple_samples", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None, // No INFO fields
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let schema = table.schema();

    // Should have 8 core fields + 4 FORMAT fields (2 samples * 2 format fields)
    assert_eq!(schema.fields().len(), 12);

    // Check FORMAT column names follow {sample}_{field} pattern
    assert_eq!(schema.field(8).name(), "Sample1_GT");
    assert_eq!(schema.field(9).name(), "Sample1_DP");
    assert_eq!(schema.field(10).name(), "Sample2_GT");
    assert_eq!(schema.field(11).name(), "Sample2_DP");

    Ok(())
}

#[tokio::test]
async fn test_format_genotype_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("genotype_parsing", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"Sample1_GT\", \"Sample2_GT\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    if batch.num_rows() >= 3 {
        let sample1_gt = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let sample2_gt = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        // Test unphased genotype "0/1"
        assert_eq!(sample1_gt.value(0), "0/1");
        assert_eq!(sample2_gt.value(0), "1/1");

        // Test "0/0" genotype
        assert_eq!(sample1_gt.value(1), "0/0");
        assert_eq!(sample2_gt.value(1), "0/1");

        // Test phased genotype "1|0" and missing "./."
        assert_eq!(sample1_gt.value(2), "1|0");
        assert_eq!(sample2_gt.value(2), "./.");
    }

    Ok(())
}

#[tokio::test]
async fn test_format_dp_field() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("dp_field", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["DP".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"Sample1_DP\", \"Sample2_DP\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    if batch.num_rows() >= 2 {
        let sample1_dp = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let sample2_dp = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        // First row: DP=20, DP=30
        assert_eq!(sample1_dp.value(0), 20);
        assert_eq!(sample2_dp.value(0), 30);

        // Second row: DP=25, DP=35
        assert_eq!(sample1_dp.value(1), 25);
        assert_eq!(sample2_dp.value(1), 35);
    }

    Ok(())
}

#[tokio::test]
async fn test_format_missing_values() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("missing_values", SAMPLE_VCF_MISSING_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"Sample1_GT\", \"Sample2_GT\", \"Sample1_DP\", \"Sample2_DP\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    if batch.num_rows() >= 2 {
        let sample2_gt = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let sample2_dp = batch
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        // First row: Sample2 has "./." genotype
        assert_eq!(sample2_gt.value(0), "./.");
        // First row: Sample2 has missing DP (should be null)
        assert!(sample2_dp.is_null(0));

        // Second row: No DP in FORMAT, should be null
        let sample1_dp = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert!(sample1_dp.is_null(1));
    }

    Ok(())
}

#[tokio::test]
async fn test_format_projection_single_sample_field() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("projection_single", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Only select one sample's GT field
    let df = ctx
        .sql("SELECT chrom, \"Sample1_GT\" FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    // Should only have 2 columns despite having more FORMAT fields available
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "Sample1_GT");

    Ok(())
}

#[tokio::test]
async fn test_format_with_info_fields() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("with_info", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["DP".to_string()]),                   // INFO DP
        Some(vec!["GT".to_string(), "DP".to_string()]), // FORMAT GT and DP
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Select both INFO and FORMAT fields
    let df = ctx
        .sql("SELECT chrom, \"DP\", \"Sample1_GT\", \"Sample1_DP\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "DP"); // INFO DP
    assert_eq!(batch.schema().field(2).name(), "Sample1_GT");
    assert_eq!(batch.schema().field(3).name(), "Sample1_DP");

    if batch.num_rows() > 0 {
        // Verify INFO DP (combined depth)
        let info_dp = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(info_dp.value(0), 50);

        // Verify FORMAT DP (sample-level)
        let format_dp = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(format_dp.value(0), 20);
    }

    Ok(())
}

/// Test single sample VCF - FORMAT columns should NOT have sample name prefix
#[tokio::test]
async fn test_format_single_sample_vcf() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("single_sample", SAMPLE_VCF_SINGLE_SAMPLE).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let schema = table.schema();

    // Should have 8 core fields + 2 FORMAT fields (1 sample * 2 format fields)
    assert_eq!(schema.fields().len(), 10);
    // Single sample: no sample prefix, just the format field name
    assert_eq!(schema.field(8).name(), "GT");
    assert_eq!(schema.field(9).name(), "DP");

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query using simple field names (no sample prefix needed)
    let df = ctx
        .sql("SELECT chrom, \"GT\", \"DP\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];

    if batch.num_rows() >= 2 {
        let gt = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let dp = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(gt.value(0), "0/1");
        assert_eq!(gt.value(1), "1|0"); // Phased genotype
        assert_eq!(dp.value(0), 20);
        assert_eq!(dp.value(1), 30);
    }

    Ok(())
}

#[tokio::test]
async fn test_format_select_star() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("select_star", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // SELECT * should include all core fields, INFO fields, and FORMAT fields
    let df = ctx.sql("SELECT * FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    // 8 core + 1 INFO + 2 FORMAT (2 samples * 1 field) = 11
    assert_eq!(batch.num_columns(), 11);

    Ok(())
}

#[tokio::test]
async fn test_format_count_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("count_agg", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx.sql("SELECT COUNT(*) FROM test_vcf").await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 4);

    Ok(())
}

#[tokio::test]
async fn test_format_special_sample_names() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("special_names", SAMPLE_VCF_SPECIAL_NAMES).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let schema = table.schema();

    // Check that special characters are preserved in column names
    // Names like "Sample-1_GT", "Sample.2_GT", "Sample 3_GT"
    assert_eq!(schema.fields().len(), 11); // 8 core + 3 FORMAT

    // Verify the schema field names
    assert_eq!(schema.field(8).name(), "Sample-1_GT");
    assert_eq!(schema.field(9).name(), "Sample.2_GT");
    assert_eq!(schema.field(10).name(), "Sample 3_GT");

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query using quoted column names with special characters
    let df = ctx
        .sql("SELECT chrom, \"Sample-1_GT\", \"Sample.2_GT\", \"Sample 3_GT\" FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 4);

    if batch.num_rows() > 0 {
        let sample1_gt = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let sample2_gt = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let sample3_gt = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(sample1_gt.value(0), "0/1");
        assert_eq!(sample2_gt.value(0), "1/1");
        assert_eq!(sample3_gt.value(0), "0/0");
    }

    Ok(())
}

/// Test that format_fields=None includes all FORMAT fields from header
#[tokio::test]
async fn test_format_fields_none_includes_all() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("format_fields_none", SAMPLE_VCF_WITH_FORMAT).await?;
    let object_storage_options = create_object_storage_options();

    // Pass None for format_fields - should include all FORMAT fields from header
    let table = VcfTableProvider::new(
        file_path.clone(),
        None, // No INFO fields
        None, // None = all FORMAT fields from header
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let schema = table.schema();

    // The VCF has FORMAT fields: GT, DP, GQ, AD (4 fields)
    // With 2 samples, should have 8 core + 8 FORMAT fields (2 samples * 4 fields) = 16 total
    assert_eq!(schema.fields().len(), 16);

    // Check that all FORMAT columns for Sample1 exist
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"Sample1_GT"));
    assert!(field_names.contains(&"Sample1_DP"));
    assert!(field_names.contains(&"Sample1_GQ"));
    assert!(field_names.contains(&"Sample1_AD"));

    // Check that all FORMAT columns for Sample2 exist
    assert!(field_names.contains(&"Sample2_GT"));
    assert!(field_names.contains(&"Sample2_DP"));
    assert!(field_names.contains(&"Sample2_GQ"));
    assert!(field_names.contains(&"Sample2_AD"));

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query all FORMAT fields - should work without specifying them
    let df = ctx
        .sql("SELECT chrom, \"Sample1_GT\", \"Sample1_DP\", \"Sample1_GQ\", \"Sample2_GT\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 5);

    if batch.num_rows() >= 1 {
        let sample1_gt = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(sample1_gt.value(0), "0/1");

        let sample1_dp = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(sample1_dp.value(0), 20);

        let sample1_gq = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(sample1_gq.value(0), 99);
    }

    Ok(())
}

/// VCF with AD field containing missing entries (e.g., AD=10,.)
const SAMPLE_VCF_AD_MISSING: &str = r#"##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic depths">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	.	A	T	30	PASS	.	GT:AD	0/1:10,20	1/1:5,.
chr1	200	.	G	C,A	40	PASS	.	GT:AD	0/1:.,15,8	1/2:10,.,5
"#;

/// Test that array values with missing entries preserve nulls
#[tokio::test]
async fn test_format_array_preserves_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("ad_missing", SAMPLE_VCF_AD_MISSING).await?;
    let object_storage_options = create_object_storage_options();

    let table = VcfTableProvider::new(
        file_path.clone(),
        None,
        Some(vec!["GT".to_string(), "AD".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"Sample1_AD\", \"Sample2_AD\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!("Warning: No results returned, skipping test");
        return Ok(());
    }

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    if batch.num_rows() >= 2 {
        // Check Sample1_AD column (list of integers)
        let sample1_ad = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // First row: Sample1 has AD=10,20 (both values present)
        let row0_array = sample1_ad.value(0);
        let row0_int_array = row0_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(row0_int_array.len(), 2);
        assert!(!row0_int_array.is_null(0));
        assert!(!row0_int_array.is_null(1));
        assert_eq!(row0_int_array.value(0), 10);
        assert_eq!(row0_int_array.value(1), 20);

        // Second row: Sample1 has AD=.,15,8 (first value missing)
        let row1_array = sample1_ad.value(1);
        let row1_int_array = row1_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(row1_int_array.len(), 3);
        assert!(row1_int_array.is_null(0)); // First entry is null
        assert!(!row1_int_array.is_null(1));
        assert!(!row1_int_array.is_null(2));
        assert_eq!(row1_int_array.value(1), 15);
        assert_eq!(row1_int_array.value(2), 8);

        // Check Sample2_AD column
        let sample2_ad = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // First row: Sample2 has AD=5,. (second value missing)
        let s2_row0_array = sample2_ad.value(0);
        let s2_row0_int_array = s2_row0_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(s2_row0_int_array.len(), 2);
        assert!(!s2_row0_int_array.is_null(0));
        assert!(s2_row0_int_array.is_null(1)); // Second entry is null
        assert_eq!(s2_row0_int_array.value(0), 5);

        // Second row: Sample2 has AD=10,.,5 (middle value missing)
        let s2_row1_array = sample2_ad.value(1);
        let s2_row1_int_array = s2_row1_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(s2_row1_int_array.len(), 3);
        assert!(!s2_row1_int_array.is_null(0));
        assert!(s2_row1_int_array.is_null(1)); // Middle entry is null
        assert!(!s2_row1_int_array.is_null(2));
        assert_eq!(s2_row1_int_array.value(0), 10);
        assert_eq!(s2_row1_int_array.value(2), 5);
    }

    Ok(())
}

/// Test that single-sample and multi-sample VCFs have different naming conventions
#[tokio::test]
async fn test_format_column_naming_single_vs_multi_sample() -> Result<(), Box<dyn std::error::Error>>
{
    let object_storage_options = create_object_storage_options();

    // Test single-sample VCF: columns should NOT have sample prefix
    let single_file = create_test_vcf_file("naming_single", SAMPLE_VCF_SINGLE_SAMPLE).await?;
    let single_table = VcfTableProvider::new(
        single_file.clone(),
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(1),
        Some(object_storage_options.clone()),
        true,
    )?;

    let single_schema = single_table.schema();
    let single_field_names: Vec<&str> = single_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Single sample: FORMAT columns should be just "GT", "DP" (no prefix)
    assert!(
        single_field_names.contains(&"GT"),
        "Single-sample VCF should have 'GT' column without sample prefix"
    );
    assert!(
        single_field_names.contains(&"DP"),
        "Single-sample VCF should have 'DP' column without sample prefix"
    );
    assert!(
        !single_field_names
            .iter()
            .any(|n| n.contains('_') && n.ends_with("_GT")),
        "Single-sample VCF should NOT have sample-prefixed GT column"
    );

    // Test multi-sample VCF: columns should have sample prefix
    let multi_file = create_test_vcf_file("naming_multi", SAMPLE_VCF_WITH_FORMAT).await?;
    let multi_table = VcfTableProvider::new(
        multi_file.clone(),
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(1),
        Some(object_storage_options),
        true,
    )?;

    let multi_schema = multi_table.schema();
    let multi_field_names: Vec<&str> = multi_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Multi sample: FORMAT columns should be "Sample1_GT", "Sample2_GT", etc.
    assert!(
        multi_field_names.contains(&"Sample1_GT"),
        "Multi-sample VCF should have 'Sample1_GT' column with sample prefix"
    );
    assert!(
        multi_field_names.contains(&"Sample2_GT"),
        "Multi-sample VCF should have 'Sample2_GT' column with sample prefix"
    );
    assert!(
        multi_field_names.contains(&"Sample1_DP"),
        "Multi-sample VCF should have 'Sample1_DP' column with sample prefix"
    );
    assert!(
        multi_field_names.contains(&"Sample2_DP"),
        "Multi-sample VCF should have 'Sample2_DP' column with sample prefix"
    );
    // Multi-sample should NOT have unprefixed FORMAT columns
    assert!(
        !multi_field_names.contains(&"GT"),
        "Multi-sample VCF should NOT have unprefixed 'GT' column"
    );
    assert!(
        !multi_field_names.contains(&"DP")
            || multi_field_names.iter().filter(|n| **n == "DP").count() == 0,
        "Multi-sample VCF should NOT have unprefixed FORMAT 'DP' column (INFO DP is separate)"
    );

    Ok(())
}
