//! The `alt` column's encoding for a record that carries no alternate allele.
//!
//! `ALT=.` is not a variant. The column is non-nullable
//! (`table_provider.rs`: `Field::new("alt", DataType::Utf8, false)`), so the
//! reader cannot express "absent" as NULL — it stores the **empty string**,
//! because noodles erases the `.` in `record/fields.rs` before the
//! `AlternateBases` wrapper is built and the join over zero alleles produces
//! nothing.
//!
//! That is a real contract and it currently rests on nothing but noodles'
//! internals: no test in this crate has ever read an `ALT=.` record. A
//! consumer now depends on it — the VEP engine classifies an ALT by its first
//! `|`/`,`-separated token and drops the record when that token is empty or
//! `.` (biodatageeks/vepyr#97) — so a silent change here would turn every
//! non-variant record back into a fabricated one-base deletion. These tests
//! pin the encoding for each ALT shape that classification distinguishes.

use datafusion::arrow::array::{Array, StringArray};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;

/// Copy a fixture somewhere with no index beside it.
///
/// `VcfTableProvider::new` discovers an adjacent `.tbi` automatically, and a
/// discovered index routes the scan through `get_indexed_vcf_stream`. So
/// reading `non_variant_alt.vcf.gz` in place does NOT exercise the unindexed
/// BGZF branch of `get_local_vcf_sync`, whatever the extension suggests --
/// copying the data file on its own is what forces that branch. (The gzip
/// fixture has no sidecar at all, so it is read in place.)
fn without_index(name: &str, dir: &tempfile::TempDir) -> String {
    let dest = dir.path().join(name);
    std::fs::copy(fixture(name), &dest).unwrap();
    dest.to_string_lossy().into_owned()
}

fn fixture(name: &str) -> String {
    format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn storage_options(compression: CompressionType) -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(compression),
    }
}

/// `(pos, alt)` for every record, in file order.
async fn read_alts(path: String, compression: CompressionType) -> Vec<(i64, String)> {
    let table =
        VcfTableProvider::new(path, None, None, Some(storage_options(compression)), true).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("v", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT start, alt FROM v ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut out = Vec::new();
    for batch in &batches {
        let starts = batch.column(0);
        let starts = datafusion::arrow::compute::cast(
            starts,
            &datafusion::arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let starts = starts
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let alts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                !alts.is_null(row),
                "the alt column is declared non-nullable, so it must never be NULL"
            );
            out.push((starts.value(row), alts.value(row).to_string()));
        }
    }
    out
}

/// What every reader path must produce for `tests/data/non_variant_alt.vcf`.
///
/// The separator is `|`, not the VCF spec's `,` — that is this crate's own
/// joined encoding, and it is what makes "the first ALT" a token rather than
/// the whole string on the consuming side.
///
/// `start` is zero-based, so it is one below the file's POS.
fn expected() -> Vec<(i64, String)> {
    vec![
        (99, "C".to_string()),
        // ALT=. — no alternate allele at all. THE case this file exists for.
        (199, String::new()),
        // A dot that is not the first ALT. VEP keeps such a record, so the
        // reader must keep the dot distinguishable from the absent case above.
        (299, "C|.".to_string()),
        // A dot that IS the first ALT. VEP drops this record, and the encoding
        // has to let a consumer see that without re-parsing the file.
        (399, ".|C".to_string()),
        // The star allele, for contrast: a real token, never empty.
        (499, "*".to_string()),
        (599, "G".to_string()),
    ]
}

/// Plain text — `get_local_vcf_sync`.
#[tokio::test]
async fn a_plain_reader_encodes_an_absent_alt_as_the_empty_string() {
    let alts = read_alts(fixture("non_variant_alt.vcf"), CompressionType::NONE).await;
    assert_eq!(alts, expected());
}

/// BGZF — the other branch of `get_local_vcf_sync`. Read from a copy with no
/// `.tbi` beside it, or the provider routes this through the indexed reader
/// and the unindexed BGZF path goes untested.
#[tokio::test]
async fn a_bgzf_reader_encodes_an_absent_alt_as_the_empty_string() {
    let dir = tempfile::tempdir().unwrap();
    let path = without_index("non_variant_alt.vcf.gz", &dir);
    let alts = read_alts(path, CompressionType::BGZF).await;
    assert_eq!(alts, expected());
}

/// GZIP — `get_local_vcf`, the async `read_records()` loop.
#[tokio::test]
async fn a_gzip_reader_encodes_an_absent_alt_as_the_empty_string() {
    let alts = read_alts(
        fixture("non_variant_alt.gzip.vcf.gz"),
        CompressionType::GZIP,
    )
    .await;
    assert_eq!(alts, expected());
}

/// The indexed path — `get_indexed_vcf_stream`. It reaches records through
/// `IndexedVcfReader::query` rather than `read_record`/`read_records`, so a
/// grep for those names does not find it; it is the loop a tabix input takes
/// and it needs its own assertion.
#[tokio::test]
async fn an_indexed_read_encodes_an_absent_alt_as_the_empty_string() {
    let table = VcfTableProvider::new(
        fixture("non_variant_alt.vcf.gz"),
        None,
        Some(vec!["chr1".to_string()]),
        Some(storage_options(CompressionType::BGZF)),
        true,
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("v", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT alt FROM v WHERE start >= 199 AND start <= 399 ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut alts = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!col.is_null(row), "the alt column must never be NULL");
            alts.push(col.value(row).to_string());
        }
    }
    assert_eq!(
        alts,
        vec![String::new(), "C|.".to_string(), ".|C".to_string()]
    );
}
