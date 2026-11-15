//! Every local read strategy returns the same records.
//!
//! The BGZF fixture is re-encoded as plain gzip, as BGZF without its `.gzi`
//! index, and as uncompressed FASTQ. Each copy is read at one and four target
//! partitions, with every column, with a projection, and with a LIMIT. Plain
//! gzip, index-less BGZF and one-partition uncompressed reads take the
//! synchronous `LocalSequential` path; the indexed BGZF and multi-partition
//! uncompressed reads take the parallel paths they are compared against.

use std::io::{Read, Write};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;

fn fixture() -> String {
    format!("{}/data/sample.fastq.bgz", env!("CARGO_MANIFEST_DIR"))
}

fn plain_bytes() -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(std::fs::File::open(fixture()).unwrap())
        .read_to_end(&mut out)
        .unwrap();
    out
}

/// (label, path) for every input variant, written to `dir`.
fn variants(dir: &std::path::Path) -> Vec<(&'static str, String)> {
    let plain = plain_bytes();
    let plain_path = dir.join("sample.fastq");
    std::fs::write(&plain_path, &plain).unwrap();

    let gz_path = dir.join("sample.fastq.gz");
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(&gz_path).unwrap(),
        flate2::Compression::default(),
    );
    gz.write_all(&plain).unwrap();
    gz.finish().unwrap();

    // Same BGZF bytes, but no .gzi next to it.
    let noindex_path = dir.join("noindex.fastq.bgz");
    std::fs::copy(fixture(), &noindex_path).unwrap();

    let s = |p: std::path::PathBuf| p.to_string_lossy().into_owned();
    vec![
        ("bgzf+gzi", fixture()),
        ("gzip", s(gz_path)),
        ("bgzf without gzi", s(noindex_path)),
        ("uncompressed", s(plain_path)),
    ]
}

/// Rows as strings, sorted so that partition order does not matter.
async fn rows(path: &str, partitions: usize, sql: &str) -> Vec<String> {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
    let provider = FastqTableProvider::new(path.to_string(), None).unwrap();
    ctx.register_table("fastq", Arc::new(provider)).unwrap();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| {
                    let c = datafusion::arrow::compute::cast(
                        c,
                        &datafusion::arrow::datatypes::DataType::Utf8,
                    )
                    .unwrap();
                    let c = c.as_string::<i32>();
                    if c.is_null(row) {
                        "<null>".to_string()
                    } else {
                        c.value(row).to_string()
                    }
                })
                .collect();
            out.push(cells.join("\t"));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn every_local_strategy_reads_the_same_records() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = variants(dir.path());
    for sql in [
        "SELECT * FROM fastq",
        "SELECT sequence, quality_scores FROM fastq",
        "SELECT name FROM fastq",
    ] {
        let expected = rows(&inputs[0].1, 1, sql).await;
        assert!(!expected.is_empty());
        for (label, path) in &inputs {
            for partitions in [1, 4] {
                assert_eq!(
                    rows(path, partitions, sql).await,
                    expected,
                    "{label}, {partitions} partitions: {sql}"
                );
            }
        }
    }
}

#[tokio::test]
async fn limit_and_count_on_sequential_reads() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = variants(dir.path());
    let total = rows(&inputs[0].1, 1, "SELECT name FROM fastq").await.len();
    for (label, path) in &inputs {
        assert_eq!(
            rows(path, 1, "SELECT name FROM fastq LIMIT 3").await.len(),
            3,
            "{label}"
        );
        assert_eq!(
            rows(path, 1, "SELECT count(*) FROM fastq").await,
            vec![total.to_string()],
            "{label}"
        );
    }
}
