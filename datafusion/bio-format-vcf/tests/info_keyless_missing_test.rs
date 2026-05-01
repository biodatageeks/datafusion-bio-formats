// Regression tests for non-Flag INFO fields whose value placeholder is omitted
// entirely (the noodles parser raises `io::Error("missing value")`).
//
// This complements `info_missing_value_test.rs` (which covers `.` placeholders
// inside multi-valued arrays such as `AD=.,15`). The bug exercised here is the
// case where a non-Flag INFO key appears without an `=value` separator at all,
// e.g. `DP;AF=0.5`. Per VCF 4.3 §1.4.2 only `Flag` fields may legally appear
// without a value, but real-world VCFs (notably gnomAD slices) sometimes emit
// a bare key for non-Flag fields. The reader must treat such occurrences as
// null rather than aborting the whole batch with
// `InvalidArgumentError("Error reading INFO field: missing value")`.

use datafusion::arrow::array::{Array, Float32Array, Int32Array, ListArray, StringArray};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

/// VCF where row 2 has bare INFO keys (`DP;AF`) for non-Flag fields. Row 1 has
/// fully-populated values; rows 3 & 4 mix `.` placeholders with bare keys to
/// exercise scalar Integer, Number=A Float, and Number=. String paths together.
const VCF_WITH_BARE_KEYS: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth (scalar)">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=ALLELE_ID,Number=.,Type=String,Description="Allele identifiers">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	DP=10;AF=0.5;ALLELE_ID=alt1
chr1	200	rs2	G	C	80	PASS	DP;AF=0.3;ALLELE_ID=alt2
chr1	300	rs3	C	T	70	PASS	DP=15;AF;ALLELE_ID=alt3
chr1	400	rs4	T	G	90	PASS	DP=20;AF=0.6;ALLELE_ID
"#;

/// VCF whose Flag-typed INFO field carries an explicit value. noodles raises
/// an `InvalidData` `"invalid flag"` error here, which is structurally distinct
/// from the `"missing value"` error fixed in this PR. The fix MUST keep
/// surfacing this error — only the literal "missing value" message is mapped
/// to null.
const VCF_WITH_INVALID_FLAG_VALUE: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=H3,Number=0,Type=Flag,Description="HapMap3 membership flag">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	H3=unexpected_payload
"#;

/// Real-data regression: row 4,032,720 of `HG00096.hg38.wgs.vcf.bgz`
/// (1000 Genomes high-coverage WGS), chrX:1946351 `<DEL>`, where the bare
/// INFO key `EVIDENCE` (declared `Number=.,Type=String`) appears at the
/// end of the INFO column with no `=value` separator. Without the fix at
/// `physical_exec.rs` L483 this row crashes the entire batch with
/// `InvalidArgumentError("Error reading INFO field: missing value")`.
const VCF_REALDATA_CHRX_EVIDENCE: &str = r#"##fileformat=VCFv4.2
##contig=<ID=chrX,length=156040895>
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count for each ALT allele">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency for each ALT allele">
##INFO=<ID=EVIDENCE,Number=.,Type=String,Description="Classes of random forest support">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chrX	1946351	HGSV_249298	A	<DEL>	.	.	AC=2;AF=0.998595;EVIDENCE
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

#[tokio::test]
async fn test_info_bare_keys_become_null() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = "/tmp/test_info_bare_keys.vcf";
    fs::write(temp_file, VCF_WITH_BARE_KEYS).await?;

    let table = VcfTableProvider::new(
        temp_file.to_string(),
        Some(vec![
            "DP".to_string(),
            "AF".to_string(),
            "ALLELE_ID".to_string(),
        ]),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, `DP`, `AF`, `ALLELE_ID` FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows, got {total_rows}: bare-key INFO must not abort the batch"
    );

    let batch = &results[0];

    let dp = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let af = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let allele_id = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Row 0: DP=10, AF=0.5, ALLELE_ID=alt1 -> all populated
    assert_eq!(dp.value(0), 10);
    let af_row0 = af.value(0);
    let af_row0_floats = af_row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row0_floats.value(0), 0.5);
    let aid_row0 = allele_id.value(0);
    let aid_row0_strs = aid_row0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(aid_row0_strs.value(0), "alt1");

    // Row 1: DP (bare) -> null, AF=0.3, ALLELE_ID=alt2
    assert!(dp.is_null(1), "DP must be null when key has no value");
    let af_row1 = af.value(1);
    let af_row1_floats = af_row1.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row1_floats.value(0), 0.3);

    // Row 2: DP=15, AF (bare) -> null array, ALLELE_ID=alt3
    assert_eq!(dp.value(2), 15);
    assert!(af.is_null(2), "AF must be null when key has no value");

    // Row 3: DP=20, AF=0.6, ALLELE_ID (bare) -> null array
    assert_eq!(dp.value(3), 20);
    assert!(
        allele_id.is_null(3),
        "ALLELE_ID must be null when key has no value"
    );

    Ok(())
}

#[tokio::test]
async fn test_info_bare_key_chrx_evidence_realdata() -> Result<(), Box<dyn std::error::Error>> {
    // Real-data regression: HG00096.hg38.wgs.vcf.bgz row 4,032,720, chrX:1946351,
    // bare INFO key EVIDENCE (Number=.,Type=String). See module-level docstring
    // and VCF_REALDATA_CHRX_EVIDENCE for context.
    let temp_file = "/tmp/test_info_bare_key_chrx_evidence.vcf";
    fs::write(temp_file, VCF_REALDATA_CHRX_EVIDENCE).await?;

    let table = VcfTableProvider::new(
        temp_file.to_string(),
        Some(vec![
            "AC".to_string(),
            "AF".to_string(),
            "EVIDENCE".to_string(),
        ]),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, pos, `AC`, `AF`, `EVIDENCE` FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "Expected 1 row, got {total_rows}: bare EVIDENCE key must not abort the batch"
    );

    let batch = &results[0];
    let evidence = batch
        .column(4)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(
        evidence.is_null(0),
        "EVIDENCE must be null when the key has no =value (real-data chrX:1946351 case)"
    );

    Ok(())
}

#[tokio::test]
async fn test_info_invalid_flag_payload_still_errors() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = "/tmp/test_info_invalid_flag.vcf";
    fs::write(temp_file, VCF_WITH_INVALID_FLAG_VALUE).await?;

    let table = VcfTableProvider::new(
        temp_file.to_string(),
        Some(vec!["H3".to_string()]),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom, `H3` FROM test_vcf").await?;
    let outcome = df.collect().await;

    let err = outcome.expect_err(
        "InvalidData errors other than the literal \"missing value\" message must still propagate",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("invalid flag") || msg.contains("Error reading INFO field"),
        "expected the underlying noodles parse error to surface, got: {msg}"
    );

    Ok(())
}
