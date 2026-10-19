//! A value nobody changed after the read must be written back with its source
//! text, byte for byte (biodatageeks/vepyr#252).
//!
//! The reader decodes percent escapes, and decoding loses information: `%2B`
//! and `+` read as the same `+`, `%3b` and `%3B` as the same `;`, `50%` and
//! `50%25` as the same `50%`. The writer escapes only what VCF reserves, so from
//! the parsed value alone it cannot restore an escape the source did not need,
//! and it escapes a literal `=`, a `,` in a single String and, before VCF 4.3, a
//! space in INFO. Ensembl VEP copies the input line, so its output keeps every
//! one of those bytes. With `with_raw_values` the reader carries the source text
//! of exactly those values, and the writer uses it for a value that still reads
//! as it.
//!
//! `tests/data/raw_values.vcf` holds one record per kind, in INFO and in both
//! FORMAT layouts (two samples, nested; one sample, flat), plus a record with
//! nothing to carry and the BRCA1 CLNVI of ClinVar 125781, whose value has both
//! `%2B` and a literal `+`.

use datafusion::arrow::array::{Array, as_string_array};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::{VCF_FORMAT_RAW_COLUMN, VCF_INFO_RAW_COLUMN};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

const MULTISAMPLE: &str = "tests/data/raw_values.vcf";
const SINGLE_SAMPLE: &str = "tests/data/raw_values.single_sample.vcf";

/// Reads `input` (carrying the layout, and the raw values when `raw`) and
/// writes it straight back, with every header field as an INFO or FORMAT field
/// in header order. `predicate` narrows the read, which takes the indexed
/// record loop for a chrom predicate on a tabix-indexed file.
async fn round_trip(name: &str, input: &str, raw: bool, predicate: &str) -> String {
    let content = std::fs::read_to_string(text_of(input)).unwrap();
    let output_path = std::env::temp_dir()
        .join(format!("test_raw_values_{name}_out.vcf"))
        .to_string_lossy()
        .into_owned();
    let ids = |prefix: &str| -> Vec<String> {
        content
            .lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .map(|rest| rest.split(',').next().unwrap().to_string())
            .collect()
    };
    let sample_names: Vec<String> = content
        .lines()
        .find(|line| line.starts_with("#CHROM"))
        .unwrap()
        .split('\t')
        .skip(9)
        .map(str::to_string)
        .collect();

    let source = VcfTableProvider::new(input.to_string(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .unwrap();
    let source = if raw {
        source.with_raw_values().unwrap()
    } else {
        source
    };
    let schema = source.schema();
    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source)).unwrap();
    ctx.register_table(
        "dest",
        Arc::new(VcfTableProvider::new_for_write(
            output_path.clone(),
            schema,
            ids("##INFO=<ID="),
            ids("##FORMAT=<ID="),
            sample_names,
            true,
        )),
    )
    .unwrap();
    ctx.sql(&format!(
        "INSERT OVERWRITE dest SELECT * FROM source {predicate}"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let written = fs::read_to_string(&output_path).await.unwrap();
    let _ = fs::remove_file(&output_path).await;
    written
}

/// The text fixture a compressed one was made from.
fn text_of(input: &str) -> &str {
    if input.ends_with(".gz") {
        MULTISAMPLE
    } else {
        input
    }
}

fn body(content: &str) -> Vec<String> {
    content
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

fn expected(input: &str) -> Vec<String> {
    body(&std::fs::read_to_string(text_of(input)).unwrap())
}

#[tokio::test]
async fn unchanged_values_round_trip_byte_for_byte_in_a_multisample_file() {
    let written = round_trip("multisample", MULTISAMPLE, true, "").await;
    assert_eq!(body(&written), expected(MULTISAMPLE));
}

#[tokio::test]
async fn unchanged_values_round_trip_byte_for_byte_in_a_single_sample_file() {
    let written = round_trip("single_sample", SINGLE_SAMPLE, true, "").await;
    assert_eq!(body(&written), expected(SINGLE_SAMPLE));
}

/// The reader has four record loops, and each captures on its own: plain/BGZF
/// above, GZIP and indexed here. The remote loop is not reachable from a local
/// fixture.
#[tokio::test]
async fn a_gzip_scan_carries_the_raw_values() {
    let written = round_trip("gzip", "tests/data/raw_values.plain.vcf.gz", true, "").await;
    assert_eq!(body(&written), expected(MULTISAMPLE));
}

#[tokio::test]
async fn an_indexed_scan_carries_the_raw_values() {
    let written = round_trip(
        "indexed",
        "tests/data/raw_values.vcf.gz",
        true,
        "WHERE chrom = 'chr1'",
    )
    .await;
    assert_eq!(body(&written), expected(MULTISAMPLE));
}

/// Without the raw values the writer encodes from the parsed value: valid VCF,
/// but not the source's bytes. This pins what the carry changes.
#[tokio::test]
async fn without_the_raw_values_the_writer_encodes_from_the_parsed_value() {
    let written = body(&round_trip("encoded", MULTISAMPLE, false, "").await);
    let info = |row: usize| written[row].split('\t').nth(7).unwrap().to_string();
    assert!(
        info(0).starts_with("X=a+b;L=t(12%3B21),c;P=50%25;"),
        "{}",
        info(0)
    );
    assert!(
        info(0).contains(":5271+80&base_change%3Ddel%20T|"),
        "{}",
        info(0)
    );
    assert!(
        info(1).starts_with("X=a%3Db;L=.,.,x;P=a%20b;"),
        "{}",
        info(1)
    );
    assert!(info(3).starts_with("X=a%2Cb%3Bc;"), "{}", info(3));
    let samples = |row: usize| {
        written[row]
            .split('\t')
            .skip(9)
            .collect::<Vec<_>>()
            .join("\t")
    };
    assert_eq!(samples(0), "0/1:p;q:a%2Cb,c\t0/0:a%2Cb:%25ZZ%25");
}

/// The carried text is limited to the values the writer could get wrong, so a
/// file with none of those bytes pays one null per record and column.
#[tokio::test]
async fn only_values_the_writer_could_change_are_carried() {
    let ctx = SessionContext::new();
    ctx.register_table(
        "source",
        Arc::new(
            VcfTableProvider::new(MULTISAMPLE.to_string(), None, None, None, true)
                .unwrap()
                .with_record_layout()
                .unwrap()
                .with_raw_values()
                .unwrap(),
        ),
    )
    .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT \"{VCF_INFO_RAW_COLUMN}\", \"{VCF_FORMAT_RAW_COLUMN}\" FROM source ORDER BY start"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows: Vec<(Option<String>, Option<String>)> = Vec::new();
    for batch in &batches {
        let info = as_string_array(batch.column(0));
        let format = as_string_array(batch.column(1));
        for i in 0..batch.num_rows() {
            let text = |a: &datafusion::arrow::array::StringArray| {
                (!a.is_null(i)).then(|| a.value(i).to_string())
            };
            rows.push((text(info), text(format)));
        }
    }
    let clnvi = "CLNVI=Breast_Cancer_Information_Core_(BIC)_(BRCA1):5271%2B80&base_change%3Ddel%20T\
                 |Breast_Cancer_Information_Core_(BIC)_(BRCA1):5271+80&base_change%3Ddel_T\
                 |ClinGen:CA290825713";
    assert_eq!(
        rows,
        vec![
            (
                Some(format!("X=a%2Bb;L=t(12%3b21),c;P=50%;{clnvi}")),
                Some("0:FS:p%3Bq\t0:FL:a%2Cb,c\t1:FS:a,b\t1:FL:%ZZ%".to_string()),
            ),
            (
                Some("X=a=b;L=%2E,.,x;P=a b".to_string()),
                Some("0:FS:x%3Ay\t1:FL:q%2bp".to_string()),
            ),
            (None, None),
            (Some("X=a,b%3Bc".to_string()), Some("0:FS:m,n".to_string())),
        ]
    );
}

#[tokio::test]
async fn the_raw_values_need_the_record_layout() {
    let refused = VcfTableProvider::new(MULTISAMPLE.to_string(), None, None, None, true)
        .unwrap()
        .with_raw_values();
    let Err(error) = refused else {
        panic!("raw values without the layout must be refused");
    };
    assert!(error.to_string().contains("with_record_layout"), "{error}");
}

/// Asking twice must not append the columns twice, since the reader locates
/// them by marker and a duplicate makes that ambiguous.
#[tokio::test]
async fn asking_for_the_raw_values_twice_is_a_no_op() {
    let once = VcfTableProvider::new(MULTISAMPLE.to_string(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .unwrap()
        .with_raw_values()
        .unwrap();
    let columns = once.schema().fields().len();
    let twice = once.with_raw_values().unwrap();
    assert_eq!(twice.schema().fields().len(), columns);
}
