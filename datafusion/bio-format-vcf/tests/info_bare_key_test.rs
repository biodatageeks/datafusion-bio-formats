use datafusion::arrow::array::{Array, BooleanArray, Float32Array, Int32Array, ListArray};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;

const VCF_WITH_BARE_NON_FLAG_INFO_KEYS: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=ALLELE_ID,Number=.,Type=String,Description="Allele identifiers">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	DP;AF=0.5;ALLELE_ID=alt1;DB
chr1	200	rs2	G	C	80	PASS	DP=42;AF;ALLELE_ID=alt2
chr1	300	rs3	C	T	70	PASS	DP=7;AF=0.2;ALLELE_ID
chr1	400	rs4	T	G	90	PASS	DP=9;AF=0.3;ALLELE_ID=alt4;DB
"#;

const VCF_REALDATA_CHRX_EVIDENCE: &str = r#"##fileformat=VCFv4.2
##contig=<ID=chrX,length=156040895>
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count for each ALT allele">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency for each ALT allele">
##INFO=<ID=EVIDENCE,Number=.,Type=String,Description="Classes of random forest support">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chrX	1946351	HGSV_249298	A	<DEL>	.	.	AC=2;AF=0.998595;EVIDENCE
"#;

const VCF_WITH_INVALID_FLAG_VALUE: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	DB=unexpected_payload
"#;

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

async fn collect_vcf_query(
    contents: &str,
    info_fields: &[&str],
    query: &str,
) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("input.vcf");
    std::fs::write(&path, contents)?;

    let table = VcfTableProvider::new(
        path.to_string_lossy().into_owned(),
        Some(
            info_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
        ),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx.sql(query).await?;
    Ok(df.collect().await?)
}

#[tokio::test]
async fn test_bare_non_flag_info_keys_become_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let results = collect_vcf_query(
        VCF_WITH_BARE_NON_FLAG_INFO_KEYS,
        &["DP", "AF", "ALLELE_ID", "DB"],
        "SELECT `DP`, `AF`, `ALLELE_ID`, `DB` FROM test_vcf",
    )
    .await?;

    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "bare non-Flag INFO keys must not abort or drop rows"
    );

    let batch = &results[0];
    let dp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let af = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let allele_id = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let db = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    assert!(
        dp.is_null(0),
        "bare scalar Integer INFO key must be read as null"
    );
    assert_eq!(dp.value(1), 42);
    assert_eq!(dp.value(2), 7);
    assert_eq!(dp.value(3), 9);

    let af_row0 = af.value(0);
    let af_row0_floats = af_row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row0_floats.value(0), 0.5);
    assert!(
        af.is_null(1),
        "bare Number=A Float INFO key must be read as null"
    );
    let af_row2 = af.value(2);
    let af_row2_floats = af_row2.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row2_floats.value(0), 0.2);

    assert!(
        allele_id.is_null(2),
        "bare Number=. String INFO key must be read as null"
    );

    assert!(db.value(0), "present Flag key must remain true");
    assert!(!db.value(1), "absent Flag key must remain false");
    assert!(!db.value(2), "absent Flag key must remain false");
    assert!(db.value(3), "present Flag key must remain true");

    Ok(())
}

#[tokio::test]
async fn test_unrequested_bare_info_key_does_not_abort_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let results = collect_vcf_query(
        VCF_WITH_BARE_NON_FLAG_INFO_KEYS,
        &["AF"],
        "SELECT chrom, `AF` FROM test_vcf",
    )
    .await?;

    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "bare keys outside the requested INFO projection must not abort the scan"
    );

    let batch = &results[0];
    let af = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let af_row0 = af.value(0);
    let af_row0_floats = af_row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row0_floats.value(0), 0.5);
    assert!(
        af.is_null(1),
        "projected bare Number=A field must still read as null"
    );

    Ok(())
}

#[tokio::test]
async fn test_real_data_evidence_bare_key_becomes_null() -> Result<(), Box<dyn std::error::Error>> {
    let results = collect_vcf_query(
        VCF_REALDATA_CHRX_EVIDENCE,
        &["AC", "AF", "EVIDENCE"],
        "SELECT `AC`, `AF`, `EVIDENCE` FROM test_vcf",
    )
    .await?;

    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "real chrX:1946351 EVIDENCE row must not abort the batch"
    );

    let batch = &results[0];
    let ac = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let af = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let evidence = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let ac_row0 = ac.value(0);
    let ac_row0_ints = ac_row0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ac_row0_ints.value(0), 2);

    let af_row0 = af.value(0);
    let af_row0_floats = af_row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row0_floats.value(0), 0.998595);

    assert!(
        evidence.is_null(0),
        "bare EVIDENCE key declared as Number=.,Type=String must be null"
    );

    Ok(())
}

#[tokio::test]
async fn test_explicit_value_for_flag_still_errors() -> Result<(), Box<dyn std::error::Error>> {
    let err = collect_vcf_query(
        VCF_WITH_INVALID_FLAG_VALUE,
        &["DB"],
        "SELECT `DB` FROM test_vcf",
    )
    .await
    .expect_err("Flag INFO fields with explicit values must still fail");

    let message = err.to_string();
    assert!(
        message.contains("invalid flag") || message.contains("Error reading INFO field"),
        "expected invalid Flag payload error to surface, got: {message}"
    );

    Ok(())
}
