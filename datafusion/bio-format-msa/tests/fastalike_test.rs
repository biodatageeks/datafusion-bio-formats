//! Integration tests for the A2M / A3M (FASTA-like) provider.
//!
//! Expectations come from `tests/data/expected.json` in polars-bio, which was
//! produced by `esl-alistat` and hh-suite `reformat.pl` (see the fixture README
//! there). Sequence prefixes are taken from the fixture bytes themselves.

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use datafusion_bio_format_msa::{FastaLikeTableProvider, MsaFlavor};
use std::sync::Arc;

fn data(name: &str) -> String {
    format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"))
}

async fn scan(name: &str, flavor: MsaFlavor, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let provider = FastaLikeTableProvider::new(data(name), flavor, None).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn strings(batches: &[RecordBatch], col: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col).unwrap();
        match arr.data_type() {
            DataType::Utf8 => {
                let a = arr.as_string::<i32>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            }
            DataType::LargeUtf8 => {
                let a = arr.as_string::<i64>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            }
            other => panic!("unexpected type for {col}: {other}"),
        }
    }
    out
}

#[tokio::test]
async fn a3m_exposes_fasta_schema_and_all_records() {
    let batches = scan("query.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 59);
    let schema = batches[0].schema();
    let fields: Vec<(&str, &DataType, bool)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("name", &DataType::Utf8, false),
            ("description", &DataType::Utf8, true),
            ("sequence", &DataType::LargeUtf8, false),
        ]
    );
}

#[tokio::test]
async fn a3m_header_is_split_on_first_whitespace_only() {
    let batches = scan("query.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    let names = strings(&batches, "name");
    let descs = strings(&batches, "description");
    assert_eq!(names[0].as_deref(), Some("sp|Q5VUD6|FA69B_HUMAN"));
    assert_eq!(
        descs[0].as_deref(),
        Some("Protein FAM69B OS=Homo sapiens GN=FAM69B PE=2 SV=3")
    );
    // A comma is not an identifier terminator (deviation from the SAM A2M page).
    assert_eq!(names[1].as_deref(), Some("tr|Q4S137|Q4S137_TETNG"));
    assert!(
        descs[1]
            .as_deref()
            .unwrap()
            .contains("SCAF14770, whole genome")
    );
}

#[tokio::test]
async fn a3m_sequences_are_verbatim_and_ragged() {
    let batches = scan("query.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    let seqs: Vec<String> = strings(&batches, "sequence")
        .into_iter()
        .flatten()
        .collect();
    assert!(seqs[0].starts_with("MRRLRRLAHLVLFCPFSKRLQGRLPGLRV"));
    // Lowercase insert states and leading gap characters preserved.
    assert!(seqs[1].starts_with("------------------YvqrkesgiegplgsratdgraggLDARF"));
    let lengths: std::collections::HashSet<usize> = seqs.iter().map(|s| s.len()).collect();
    assert!(lengths.len() > 1, "a3m rows must stay ragged");
}

#[tokio::test]
async fn description_is_null_when_header_has_no_whitespace() {
    let batches = scan("no_desc.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    assert_eq!(
        strings(&batches, "name"),
        vec![Some("nodesc".into()), Some("with".into())]
    );
    assert_eq!(
        strings(&batches, "description"),
        vec![None, Some("desc".into())]
    );
}

#[tokio::test]
async fn a3m_skips_hash_lines_before_first_record() {
    let plain = scan("query.a3m", MsaFlavor::A3m, "SELECT name, sequence FROM t").await;
    let headed = scan("hdr.a3m", MsaFlavor::A3m, "SELECT name, sequence FROM t").await;
    assert_eq!(rows(&headed), 59);
    assert_eq!(strings(&plain, "name"), strings(&headed, "name"));
    assert_eq!(strings(&plain, "sequence"), strings(&headed, "sequence"));
}

#[tokio::test]
async fn a3m_pseudo_sequences_are_ordinary_rows_in_file_order() {
    let batches = scan("test_head.a3m", MsaFlavor::A3m, "SELECT name FROM t").await;
    assert_eq!(rows(&batches), 204);
    let names: Vec<String> = strings(&batches, "name").into_iter().flatten().collect();
    assert_eq!(&names[..4], ["ss_dssp", "ss_pred", "ss_conf", "1a7j_A"]);
}

#[tokio::test]
async fn a2m_dotted_rows_are_rectangular_and_keep_dots() {
    let batches = scan("query_dotted.a2m", MsaFlavor::A2m, "SELECT sequence FROM t").await;
    let seqs: Vec<String> = strings(&batches, "sequence")
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(seqs.len(), 59);
    assert!(seqs.iter().all(|s| s.len() == 849));
    assert!(
        seqs[1].contains('.'),
        "insert-column dots must be preserved"
    );
}

#[tokio::test]
async fn gzip_input_matches_plain_input() {
    let plain = scan("query.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    let gz = scan("query.a3m.gz", MsaFlavor::A3m, "SELECT * FROM t").await;
    assert_eq!(strings(&plain, "name"), strings(&gz, "name"));
    assert_eq!(strings(&plain, "sequence"), strings(&gz, "sequence"));
}

#[tokio::test]
async fn empty_file_yields_no_rows() {
    let batches = scan("empty.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 0);
}

#[tokio::test]
async fn a_zero_limit_reads_nothing() {
    use datafusion::catalog::TableProvider;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let provider = FastaLikeTableProvider::new(data("query.a3m"), MsaFlavor::A3m, None).unwrap();
    let plan = provider
        .scan(&ctx.state(), None, &[], Some(0))
        .await
        .unwrap();
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    assert_eq!(rows(&batches), 0);
}

#[tokio::test]
async fn local_file_uri_is_accepted() {
    let plain = scan("query.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    let ctx = SessionContext::new();
    let uri = format!("file://{}", data("query.a3m"));
    let provider = FastaLikeTableProvider::new(uri, MsaFlavor::A3m, None).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let via_uri = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(strings(&plain, "name"), strings(&via_uri, "name"));
    assert_eq!(strings(&plain, "sequence"), strings(&via_uri, "sequence"));
}

#[tokio::test]
async fn single_record_without_trailing_newline_handling() {
    let batches = scan("single.a3m", MsaFlavor::A3m, "SELECT * FROM t").await;
    assert_eq!(strings(&batches, "name"), vec![Some("only".into())]);
    assert_eq!(
        strings(&batches, "description"),
        vec![Some("description here".into())]
    );
    assert_eq!(
        strings(&batches, "sequence"),
        vec![Some("MKVLaaGG-".into())]
    );
}

#[tokio::test]
async fn projection_and_count_and_limit() {
    let only_name = scan("query.a3m", MsaFlavor::A3m, "SELECT name FROM t").await;
    assert_eq!(only_name[0].num_columns(), 1);
    assert_eq!(rows(&only_name), 59);

    let count = scan("query.a3m", MsaFlavor::A3m, "SELECT count(*) AS n FROM t").await;
    let n = count[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    assert_eq!(n.value(0), 59);

    let limited = scan("query.a3m", MsaFlavor::A3m, "SELECT name FROM t LIMIT 5").await;
    assert_eq!(rows(&limited), 5);
}

#[tokio::test]
async fn non_record_line_before_first_header_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.a3m");
    std::fs::write(&path, "ACGT\n>x\nACGT\n").unwrap();
    let ctx = SessionContext::new();
    let provider =
        FastaLikeTableProvider::new(path.to_str().unwrap().to_string(), MsaFlavor::A3m, None)
            .unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let err = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("bad.a3m"), "error should name the path: {err}");
    assert!(
        err.contains('>'),
        "error should mention the expected '>': {err}"
    );
}
