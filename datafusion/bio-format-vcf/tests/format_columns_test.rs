use datafusion::arrow::array::{Array, Int32Array, ListArray, StringArray, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_VCF_MULTI: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Combined depth\">
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype quality\">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description=\"Allelic depths\">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs1	A	T	60	PASS	DP=50	GT:DP:GQ	0/1:20:99	1/1:30:95
chr1	200	rs2	G	C	80	PASS	DP=60	GT:DP:GQ	0/0:25:99	0/1:35:90
chr2	300	rs3	C	G	70	PASS	DP=45	GT:DP:GQ	1|0:15:85	./.:10:50
chr2	400	rs4	T	A	50	PASS	DP=40	GT:DP:GQ:AD	0/1:18:92:10,8	1/1:22:88:2,20
"#;

const SAMPLE_VCF_SINGLE: &str = r#"##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	OnlySample
chr1	100	.	A	T	30	PASS	.	GT:DP	0/1:20
chr1	200	.	G	C	40	PASS	.	GT:DP	1|0:30
"#;

async fn create_test_vcf_file(test_name: &str, content: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_format_{test_name}.vcf");
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
async fn test_single_sample_schema_keeps_top_level_format_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("single_schema", SAMPLE_VCF_SINGLE).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();
    assert!(schema.field_with_name("GT").is_ok());
    assert!(schema.field_with_name("DP").is_ok());
    assert!(schema.field_with_name("genotypes").is_err());

    Ok(())
}

#[tokio::test]
async fn test_multisample_schema_uses_nested_genotypes() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_schema", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();
    assert!(schema.field_with_name("genotypes").is_ok());
    assert!(schema.field_with_name("Sample1_GT").is_err());

    // Columnar schema: genotypes: Struct<GT: List<Utf8>, DP: List<Int32>>
    let genotypes_field = schema.field_with_name("genotypes")?;
    match genotypes_field.data_type() {
        DataType::Struct(struct_fields) => {
            let gt_field = struct_fields
                .iter()
                .find(|f| f.name() == "GT")
                .expect("GT field in genotypes struct");
            assert!(matches!(gt_field.data_type(), DataType::List(_)));

            let dp_field = struct_fields
                .iter()
                .find(|f| f.name() == "DP")
                .expect("DP field in genotypes struct");
            assert!(matches!(dp_field.data_type(), DataType::List(_)));
        }
        other => panic!("expected Struct type for genotypes, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_multisample_nested_values_are_readable() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_values", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new(
        file_path,
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT genotypes FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    // Columnar layout: genotypes is a StructArray with List children
    let genotypes = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes struct");

    // GT column: List<Utf8> — each element is a list of sample GTs for that row
    let gt_list = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let dp_list = genotypes
        .column_by_name("DP")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // First row (chr1:100): GT=["0/1", "1/1"], DP=[20, 30]
    let row0_gt = gt_list.value(0);
    let row0_gt_values = row0_gt.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(row0_gt_values.value(0), "0/1");
    assert_eq!(row0_gt_values.value(1), "1/1");

    let row0_dp = dp_list.value(0);
    let row0_dp_values = row0_dp.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(row0_dp_values.value(0), 20);
    assert_eq!(row0_dp_values.value(1), 30);

    Ok(())
}

#[tokio::test]
async fn test_single_sample_query_stays_simple() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("single_query", SAMPLE_VCF_SINGLE).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT \"GT\", \"DP\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    let gt = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dp = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(gt.value(0), "0/1");
    assert_eq!(gt.value(1), "1|0");
    assert_eq!(dp.value(0), 20);
    assert_eq!(dp.value(1), 30);

    Ok(())
}

#[tokio::test]
async fn test_info_none_includes_all_header_info_fields() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path = create_test_vcf_file("info_default", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        Some(vec!["GT".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();
    assert!(schema.field_with_name("DP").is_ok());

    Ok(())
}

#[tokio::test]
async fn test_multisample_sample_subset_preserves_requested_order()
-> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_subset_order", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new_with_samples(
        file_path,
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(vec!["Sample2".to_string(), "Sample1".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT genotypes FROM test_vcf ORDER BY start LIMIT 1")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    // Columnar layout: genotypes is a StructArray
    let genotypes = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes struct");

    let gt_list = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let dp_list = genotypes
        .column_by_name("DP")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // First row: samples requested in order [Sample2, Sample1]
    // So GT[0]=Sample2's GT ("1/1"), GT[1]=Sample1's GT ("0/1")
    let row0_gt = gt_list.value(0);
    let row0_gt_values = row0_gt.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(row0_gt_values.len(), 2);
    assert_eq!(row0_gt_values.value(0), "1/1"); // Sample2
    assert_eq!(row0_gt_values.value(1), "0/1"); // Sample1

    let row0_dp = dp_list.value(0);
    let row0_dp_values = row0_dp.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(row0_dp_values.value(0), 30); // Sample2
    assert_eq!(row0_dp_values.value(1), 20); // Sample1

    Ok(())
}

#[tokio::test]
async fn test_multisample_single_selected_stays_nested() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_single_selected_nested", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new_with_samples(
        file_path,
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(vec!["Sample2".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();
    assert!(schema.field_with_name("genotypes").is_ok());
    assert!(schema.field_with_name("GT").is_err());
    assert!(schema.field_with_name("Sample2_GT").is_err());

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT genotypes FROM test_vcf ORDER BY start LIMIT 1")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    // Columnar layout: genotypes is a StructArray
    let genotypes = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes struct");

    // With only Sample2 selected, each list should have length 1
    let gt_list = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let row0_gt = gt_list.value(0);
    let row0_gt_values = row0_gt.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(row0_gt_values.len(), 1);
    assert_eq!(row0_gt_values.value(0), "1/1"); // Sample2's GT

    Ok(())
}

/// Regression test: VCF files where FORMAT/AD is declared with Number=. (Unknown)
/// instead of Number=R (ReferenceAlternateBases) should still be readable.
/// noodles strict validation rejects this as FormatDefinitionMismatch.
#[tokio::test]
async fn test_read_vcf_with_ad_number_dot() -> Result<(), Box<dyn std::error::Error>> {
    let table = VcfTableProvider::new(
        "tests/head_106667_tail_6.vcf".to_string(),
        Some(vec![]),
        Some(vec![
            "GT".to_string(),
            "AD".to_string(),
            "DP".to_string(),
            "GQ".to_string(),
            "PL".to_string(),
        ]),
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("vcf", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom, start, genotypes FROM vcf").await?;
    let results = df.collect().await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6, "expected 6 data rows");

    Ok(())
}

#[tokio::test]
async fn test_missing_requested_samples_are_skipped() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_missing_requested", SAMPLE_VCF_MULTI).await?;

    let table = VcfTableProvider::new_with_samples(
        file_path,
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string()]),
        Some(vec!["MissingSample".to_string(), "Sample1".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT genotypes FROM test_vcf ORDER BY start LIMIT 1")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    // Columnar layout: genotypes is a StructArray
    let genotypes = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes struct");

    // MissingSample is skipped, only Sample1 remains → lists have length 1
    let gt_list = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let row0_gt = gt_list.value(0);
    let row0_gt_values = row0_gt.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(row0_gt_values.len(), 1);
    assert_eq!(row0_gt_values.value(0), "0/1"); // Sample1's GT

    Ok(())
}

// --- Tests for INFO/FORMAT column name collision (issue polars-bio#350) ---

/// VCF with both INFO=DP and FORMAT=DP in a single-sample file.
/// This is the exact scenario from the reported bug.
const SAMPLE_VCF_SINGLE_COLLISION: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Combined depth across samples">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleA
chr1	100	rs1	A	T	60	PASS	DP=50;AF=0.5	GT:DP:GQ	0/1:20:99
chr1	200	rs2	G	C	80	PASS	DP=60;AF=0.3	GT:DP:GQ	1/1:30:95
"#;

/// Multi-sample VCF with INFO/FORMAT collision — should remain unaffected
/// since FORMAT fields are nested under genotypes struct.
const SAMPLE_VCF_MULTI_COLLISION: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Combined depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs1	A	T	60	PASS	DP=50	GT:DP	0/1:20	1/1:30
"#;

#[tokio::test]
async fn test_single_sample_info_format_collision_schema() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path =
        create_test_vcf_file("single_collision_schema", SAMPLE_VCF_SINGLE_COLLISION).await?;

    let table = VcfTableProvider::new(
        file_path,
        None, // all INFO fields
        None, // all FORMAT fields
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();

    // INFO DP should keep its original name
    assert!(
        schema.field_with_name("DP").is_ok(),
        "INFO DP should be present as 'DP'"
    );

    // FORMAT DP should be renamed to fmt_DP
    assert!(
        schema.field_with_name("fmt_DP").is_ok(),
        "FORMAT DP should be renamed to 'fmt_DP'"
    );

    // Non-colliding FORMAT fields keep original names
    assert!(schema.field_with_name("GT").is_ok());
    assert!(schema.field_with_name("GQ").is_ok());

    Ok(())
}

#[tokio::test]
async fn test_single_sample_info_format_collision_query() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path =
        create_test_vcf_file("single_collision_query", SAMPLE_VCF_SINGLE_COLLISION).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    // Query both INFO DP and FORMAT fmt_DP
    let df = ctx
        .sql("SELECT \"DP\", \"fmt_DP\", \"GT\", \"GQ\" FROM test_vcf ORDER BY start")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    assert_eq!(batch.num_rows(), 2);

    // INFO DP values
    let info_dp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(info_dp.value(0), 50);
    assert_eq!(info_dp.value(1), 60);

    // FORMAT DP values (renamed to fmt_DP)
    let fmt_dp = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(fmt_dp.value(0), 20);
    assert_eq!(fmt_dp.value(1), 30);

    // GT and GQ should work normally
    let gt = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gt.value(0), "0/1");
    assert_eq!(gt.value(1), "1/1");

    let gq = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(gq.value(0), 99);
    assert_eq!(gq.value(1), 95);

    Ok(())
}

#[tokio::test]
async fn test_multi_sample_info_format_collision_unaffected()
-> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_vcf_file("multi_collision", SAMPLE_VCF_MULTI_COLLISION).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();

    // INFO DP at top level
    assert!(schema.field_with_name("DP").is_ok());
    // FORMAT fields nested in genotypes — no collision
    assert!(schema.field_with_name("genotypes").is_ok());
    // No fmt_ prefix needed
    assert!(schema.field_with_name("fmt_DP").is_err());

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx.sql("SELECT \"DP\", genotypes FROM test_vcf").await?;
    let results = df.collect().await?;
    let batch = &results[0];

    let info_dp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(info_dp.value(0), 50);

    Ok(())
}

#[tokio::test]
async fn test_single_sample_no_collision_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    // SAMPLE_VCF_SINGLE has no INFO fields, so FORMAT DP should keep its name
    let file_path = create_test_vcf_file("single_no_collision", SAMPLE_VCF_SINGLE).await?;

    let table = VcfTableProvider::new(
        file_path,
        None,
        Some(vec!["GT".to_string(), "DP".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    let schema = table.schema();
    // No collision: FORMAT DP keeps original name
    assert!(schema.field_with_name("DP").is_ok());
    assert!(schema.field_with_name("fmt_DP").is_err());

    Ok(())
}

/// Regression test: write round-trip for single-sample VCF with INFO/FORMAT DP collision.
/// Verifies the written file has correct ##FORMAT and ##INFO header entries with their
/// original descriptions, and that data values are preserved.
#[tokio::test]
async fn test_single_sample_collision_write_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let input_path =
        create_test_vcf_file("collision_roundtrip_in", SAMPLE_VCF_SINGLE_COLLISION).await?;
    let output_path = "/tmp/test_collision_roundtrip_out.vcf";

    // Read source
    let source = VcfTableProvider::new(
        input_path.clone(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;
    let source_schema = source.schema();

    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source))?;

    // Register write destination
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string(), "AF".to_string()],
        vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()],
        vec!["SampleA".to_string()],
        true,
    );
    ctx.register_table("dest", Arc::new(dest))?;

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await?
        .collect()
        .await?;

    // Read the written file and verify headers
    let content = fs::read_to_string(output_path).await?;

    // INFO DP should have its own description
    assert!(
        content.contains("##INFO=<ID=DP,"),
        "written file should have ##INFO=<ID=DP> header"
    );
    // FORMAT DP should have the FORMAT description, not the INFO description
    assert!(
        content.contains("##FORMAT=<ID=DP,"),
        "written file should have ##FORMAT=<ID=DP> header"
    );
    assert!(
        content.contains("Description=\"Read depth\""),
        "FORMAT DP should have 'Read depth' description, not the INFO description"
    );
    assert!(
        content.contains("Description=\"Combined depth across samples\""),
        "INFO DP should have 'Combined depth across samples' description"
    );

    // Read back and verify data values
    let readback = VcfTableProvider::new(
        output_path.to_string(),
        None,
        None,
        Some(create_object_storage_options()),
        true,
    )?;
    let ctx2 = SessionContext::new();
    ctx2.register_table("readback", Arc::new(readback))?;

    let df = ctx2
        .sql("SELECT \"DP\", \"fmt_DP\", \"GT\" FROM readback ORDER BY start")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);

    let info_dp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(info_dp.value(0), 50);
    assert_eq!(info_dp.value(1), 60);

    let fmt_dp = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(fmt_dp.value(0), 20);
    assert_eq!(fmt_dp.value(1), 30);

    // Cleanup
    let _ = fs::remove_file(output_path).await;
    let _ = fs::remove_file(&input_path).await;

    Ok(())
}
