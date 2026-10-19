//! Percent-encoded INFO and FORMAT values must survive a read/write round trip.
//!
//! VCF 4.3 §1.2 encodes the characters that would otherwise end a value — `;`
//! and `=` in INFO, `:` in FORMAT, `,` between list elements, `%` itself and the
//! line controls — as `%XX`. The reader decodes them, which is what a DataFrame
//! user wants, so the writer has to encode them again: written back raw, a `;`
//! ends the INFO field early and a `,` silently adds a list element.
//! (biodatageeks/vepyr#207)
//!
//! Characters that are not reserved stay literal on both sides: a `:` inside an
//! INFO value (ClinVar `CLNDISDB=MedGen:C1`) and any non-ASCII byte.
//!
//! Before VCF 4.3 an INFO value may not hold a space either, so under a header
//! declaring 4.1 or 4.2 a space is escaped too (`%20`).

use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const HEADER: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease names, | separated">
##INFO=<ID=X,Number=1,Type=String,Description="Scalar string">
##INFO=<ID=L,Number=.,Type=String,Description="List of strings">
##INFO=<ID=P,Number=1,Type=String,Description="Contains a percent sign">
##INFO=<ID=T,Number=1,Type=String,Description="Contains a tab">
##INFO=<ID=U,Number=1,Type=String,Description="Non-ASCII text">
##INFO=<ID=D,Number=.,Type=String,Description="Literal colons">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=FS,Number=1,Type=String,Description="Sample string">
##contig=<ID=chr1,length=1000>
"#;

const INFO_1: &str = "CLNDN=Lung_disease%3B|not_provided;X=k%3Dv;L=a%2Cb,c;P=50%25;\
T=a%09b;U=a\u{2013}b;D=MedGen:C1|MONDO:M2;DP=5";
const INFO_2: &str = "CLNDN=t(12%3B21)(p13%3Bq22)%3B_TEL-AML1;X=base_change%3Dins;\
L=p%2Cq%3Br;P=100%25;T=x;U=\u{e9};D=Orphanet:267;DP=7";

fn single_sample_vcf() -> String {
    format!(
        "{HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
         chr1\t100\trs1\tA\tT\t60\tPASS\t{INFO_1}\tGT:FS\t0/1:x%3Ay\n\
         chr1\t200\trs2\tG\tC\t80\tPASS\t{INFO_2}\tGT:FS\t1/1:a%2Cb%25\n"
    )
}

/// `;` separates nothing in a sample column, so it stays literal there.
fn literal_format_semicolon_vcf() -> String {
    format!(
        "{HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
         chr1\t100\trs1\tA\tT\t60\tPASS\t{INFO_1}\tGT:FS\t0/1:p;q\n"
    )
}

fn multisample_vcf() -> String {
    format!(
        "{HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
         chr1\t100\trs1\tA\tT\t60\tPASS\t{INFO_1}\tGT:FS\t0/1:x%3Ay\t0/0:p%2Cq%3Ar\n\
         chr1\t200\trs2\tG\tC\t80\tPASS\t{INFO_2}\tGT:FS\t1/1:a%2Cb%25\t0/1:z\n"
    )
}

/// Reads `content` and writes it straight back, with every header field as an
/// INFO or FORMAT field in header order.
async fn round_trip(name: &str, content: &str) -> String {
    let input_path = std::env::temp_dir().join(format!("test_info_percent_{name}.vcf"));
    fs::write(&input_path, content).await.unwrap();
    let input_path = input_path.to_string_lossy().into_owned();
    let output_path = std::env::temp_dir()
        .join(format!("test_info_percent_{name}_out.vcf"))
        .to_string_lossy()
        .into_owned();

    let ids = |prefix: &str| -> Vec<String> {
        content
            .lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .map(|rest| rest.split(',').next().unwrap().to_string())
            .collect()
    };
    let info_fields = ids("##INFO=<ID=");
    let format_fields = ids("##FORMAT=<ID=");
    let sample_names: Vec<String> = content
        .lines()
        .find(|line| line.starts_with("#CHROM"))
        .unwrap()
        .split('\t')
        .skip(9)
        .map(str::to_string)
        .collect();

    let source = VcfTableProvider::new(input_path.clone(), None, None, None, true).unwrap();
    let schema = source.schema();
    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source)).unwrap();
    ctx.register_table(
        "dest",
        Arc::new(VcfTableProvider::new_for_write(
            output_path.clone(),
            schema,
            info_fields,
            format_fields,
            sample_names,
            true,
        )),
    )
    .unwrap();
    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let written = fs::read_to_string(&output_path).await.unwrap();
    let _ = fs::remove_file(&input_path).await;
    let _ = fs::remove_file(&output_path).await;
    written
}

fn body(content: &str) -> Vec<String> {
    content
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn percent_encoded_values_round_trip_in_a_single_sample_file() {
    let input = single_sample_vcf();
    let written = round_trip("single_sample", &input).await;
    assert_eq!(body(&written), body(&input));
}

#[tokio::test]
async fn percent_encoded_values_round_trip_in_a_multisample_file() {
    let input = multisample_vcf();
    let written = round_trip("multisample", &input).await;
    assert_eq!(body(&written), body(&input));
}

#[tokio::test]
async fn the_reader_still_hands_out_decoded_values() {
    let input_path = std::env::temp_dir().join("test_info_percent_decoded.vcf");
    fs::write(&input_path, single_sample_vcf()).await.unwrap();
    let source = VcfTableProvider::new(
        input_path.to_string_lossy().into_owned(),
        None,
        None,
        None,
        true,
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source)).unwrap();
    let batches = ctx
        .sql("SELECT \"X\", \"P\" FROM source ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let _ = fs::remove_file(&input_path).await;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(formatted.contains("k=v"), "{formatted}");
    assert!(formatted.contains("50%"), "{formatted}");
}

#[tokio::test]
async fn a_literal_semicolon_in_a_sample_value_stays_literal() {
    let input = literal_format_semicolon_vcf();
    let written = round_trip("format_semicolon", &input).await;
    assert_eq!(body(&written), body(&input));
}

/// One record with `info` under a header declaring `file_format`.
fn space_vcf(file_format: &str, info: &str) -> String {
    format!(
        "##fileformat={file_format}\n\
         ##INFO=<ID=CLNVI,Number=.,Type=String,Description=\"Variant identifiers\">\n\
         ##contig=<ID=chr13,length=114364328>\n\
         #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
         chr13\t32333365\t51220\tT\tTA\t.\t.\t{info}\n"
    )
}

/// VCF 4.1 and 4.2 allow no whitespace in INFO, so ClinVar (`VCFv4.1`)
/// escapes a space as `%20`, and the writer has to as well: written as a plain
/// space it breaks the version the header declares.
#[tokio::test]
async fn an_escaped_space_round_trips_under_a_header_before_vcf_4_3() {
    let info = "CLNVI=Breast_Cancer_Information_Core_(BIC)_(BRCA2):2116&base_change%3Dins%20A|ClinGen:CA013728";
    for file_format in ["VCFv4.1", "VCFv4.2"] {
        let input = space_vcf(file_format, info);
        let written = round_trip("space_before_4_3", &input).await;
        assert!(written.starts_with(&format!("##fileformat={file_format}\n")));
        assert_eq!(body(&written), body(&input), "{file_format}");
    }
}

/// VCF 4.3 allows spaces in INFO values, so a literal one stays literal.
#[tokio::test]
async fn a_literal_space_stays_literal_from_vcf_4_3() {
    let input = space_vcf("VCFv4.3", "CLNVI=base_change%3Dins A");
    let written = round_trip("space_4_3", &input).await;
    assert_eq!(body(&written), body(&input));
}
