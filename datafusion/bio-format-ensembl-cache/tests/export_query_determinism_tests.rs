//! Determinism of the export queries under parallel execution.
//!
//! A transcript that straddles a 1 Mb cache-region boundary is stored in *both*
//! region files, so the export queries see two copies of every one of its
//! features. Which copy survives deduplication must be a function of the data,
//! not of which scan partition happened to emit its batch first — otherwise two
//! builds of the same raw cache produce different Parquet shards.
//!
//! These tests drive the real SQL against an in-memory table whose partitions
//! hold the two copies separately, at `target_partitions` > 1, and assert both
//! that the surviving copy is the one Ensembl VEP would keep (the lowest region)
//! and that repeated executions agree.

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{EnsemblEntityKind, build_export_query};
use std::sync::Arc;

const TARGET_PARTITIONS: usize = 8;
const TRANSCRIPTS: usize = 300;
const EXONS_PER_TRANSCRIPT: i32 = 3;
const RUNS: usize = 12;

/// The region file that contains the transcript start, and therefore the copy
/// Ensembl VEP's `merge_features` keeps.
const LOW_REGION: &str = "/cache/homo_sapiens/116_GRCh38/22/21000001-22000000.gz";
/// The neighbouring region the same transcript also spills into.
const HIGH_REGION: &str = "/cache/homo_sapiens/116_GRCh38/22/22000001-23000000.gz";

fn exon_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("transcript_id", DataType::Utf8, false),
        Field::new("exon_number", DataType::Int32, false),
        Field::new("source_file", DataType::Utf8, false),
    ]))
}

/// One batch holding every exon of `transcripts`, all attributed to `source_file`.
fn exon_batch(schema: &Arc<Schema>, transcripts: &[usize], source_file: &str) -> RecordBatch {
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut stable_ids = Vec::new();
    let mut transcript_ids = Vec::new();
    let mut exon_numbers = Vec::new();

    for &t in transcripts {
        for exon in 1..=EXONS_PER_TRANSCRIPT {
            // The transcript starts inside LOW_REGION and runs past its end, so
            // both region files carry it.
            let start = 21_900_000 + (t as i64) * 100 + i64::from(exon) * 10;
            starts.push(start);
            ends.push(start + 5);
            // Identical in both copies — this is what leaves the dedup tied.
            stable_ids.push(format!("ENSE{:011}", t * 10 + exon as usize));
            transcript_ids.push(format!("ENST{:011}", t));
            exon_numbers.push(exon);
        }
    }
    let rows = starts.len();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(vec!["22"; rows])),
            Arc::new(Int64Array::from(starts)),
            Arc::new(Int64Array::from(ends)),
            Arc::new(StringArray::from(stable_ids)),
            Arc::new(StringArray::from(transcript_ids)),
            Arc::new(Int32Array::from(exon_numbers)),
            Arc::new(StringArray::from(vec![source_file; rows])),
        ],
    )
    .expect("exon batch")
}

/// A `MemTable` with one partition per scan partition: the low-region copies sit
/// in the first half, the high-region copies in the second, so the two copies of
/// an exon always reach the window operator from different partitions.
fn duplicated_exon_table() -> MemTable {
    let schema = exon_schema();
    let half = TARGET_PARTITIONS / 2;
    let mut partitions = Vec::with_capacity(TARGET_PARTITIONS);
    for source_file in [LOW_REGION, HIGH_REGION] {
        for slot in 0..half {
            let transcripts: Vec<usize> = (0..TRANSCRIPTS).filter(|t| t % half == slot).collect();
            partitions.push(vec![exon_batch(&schema, &transcripts, source_file)]);
        }
    }
    MemTable::try_new(schema, partitions).expect("mem table")
}

/// Execute the exon export query once and return `(transcript_id, exon_number,
/// source_file)` in the order the rows are produced.
async fn run_exon_export() -> Vec<(String, i32, String)> {
    let config = SessionConfig::new().with_target_partitions(TARGET_PARTITIONS);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("exon", Arc::new(duplicated_exon_table()))
        .expect("register exon");

    let query = build_export_query(EnsemblEntityKind::Exon, "exon", Some("22"), None);
    let batches = ctx
        .sql(&query)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let transcript_ids = batch
            .column_by_name("transcript_id")
            .expect("transcript_id")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("transcript_id utf8");
        let exon_numbers = batch
            .column_by_name("exon_number")
            .expect("exon_number")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("exon_number i32");
        let source_files = batch
            .column_by_name("source_file")
            .expect("source_file")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("source_file utf8");
        for row in 0..batch.num_rows() {
            rows.push((
                transcript_ids.value(row).to_string(),
                exon_numbers.value(row),
                source_files.value(row).to_string(),
            ));
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exon_dedup_always_keeps_the_lowest_region_copy() {
    let expected_rows = TRANSCRIPTS * EXONS_PER_TRANSCRIPT as usize;

    for run in 0..RUNS {
        let rows = run_exon_export().await;
        assert_eq!(
            rows.len(),
            expected_rows,
            "run {run}: dedup must leave exactly one copy per (transcript, exon)"
        );
        let from_high: Vec<&(String, i32, String)> = rows
            .iter()
            .filter(|(_, _, src)| src == HIGH_REGION)
            .collect();
        assert!(
            from_high.is_empty(),
            "run {run}: {} of {expected_rows} exons were taken from the higher region \
             (e.g. {:?}); the surviving copy must not depend on scan arrival order",
            from_high.len(),
            from_high.first()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exon_export_is_reproducible_across_runs() {
    let first = run_exon_export().await;
    for run in 1..RUNS {
        let again = run_exon_export().await;
        assert_eq!(
            first, again,
            "run {run} produced a different row sequence than run 0"
        );
    }
}
