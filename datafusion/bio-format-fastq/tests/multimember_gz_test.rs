use datafusion::prelude::*;
use datafusion_bio_format_fastq::FastqTableProvider;
use std::sync::Arc;

async fn count_reads(file_path: String) -> usize {
    let ctx = SessionContext::new();
    let provider = FastqTableProvider::new(file_path, None).expect("Failed to create provider");
    ctx.register_table("fastq", Arc::new(provider))
        .expect("Failed to register table");
    let df = ctx
        .sql("SELECT * FROM fastq")
        .await
        .expect("Failed to execute query");
    let batches = df.collect().await.expect("Failed to collect results");
    batches.iter().map(|b| b.num_rows()).sum()
}

// Multi-member gzip whose member boundary falls BETWEEN records.
// Pre-fix: returns only the first member (40). Post-fix: 100.
#[tokio::test]
async fn test_multimember_gz_clean_boundary_reads_all_members() {
    let path = format!(
        "{}/data/multimember_clean.fastq.gz",
        env!("CARGO_MANIFEST_DIR")
    );
    assert_eq!(
        count_reads(path).await,
        100,
        "must decode all gzip members, not just the first"
    );
}

// Multi-member gzip whose member boundary falls MID-RECORD.
// Pre-fix: panics with DataFusion External(UnexpectedEof). Post-fix: 100.
#[tokio::test]
async fn test_multimember_gz_split_record_boundary_reads_all_members() {
    let path = format!(
        "{}/data/multimember_split.fastq.gz",
        env!("CARGO_MANIFEST_DIR")
    );
    assert_eq!(
        count_reads(path).await,
        100,
        "must not lose data or crash on a mid-record gzip member boundary"
    );
}

// Real tool-produced multi-member gzip: four pigz members concatenated
// (the `cat lane1.fastq.gz lane2.fastq.gz ...` pattern), 500 reads each.
// Pre-fix: returns only the first member (500). Post-fix: 2000.
#[tokio::test]
async fn test_multimember_gz_pigz_concatenated_reads_all_members() {
    let path = format!(
        "{}/data/multimember_pigz.fastq.gz",
        env!("CARGO_MANIFEST_DIR")
    );
    assert_eq!(
        count_reads(path).await,
        2000,
        "must decode all pigz members of a concatenated gzip stream"
    );
}
