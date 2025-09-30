use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_fastq::table_provider::{FastqByteRange, FastqTableProvider};
use std::sync::Arc;
use tempfile::NamedTempFile;

fn create_test_fastq_file() -> std::io::Result<NamedTempFile> {
    let mut temp_file = NamedTempFile::new()?;
    let fastq_content = concat!(
        "@read1\n",
        "ATCGATCGATCGATCG\n",
        "+\n",
        "IIIIIIIIIIIIIIII\n",
        "@read2\n",
        "GCTAGCTAGCTAGCTA\n",
        "+\n",
        "HHHHHHHHHHHHHHHH\n",
        "@read3\n",
        "TTTTAAAACCCCGGGG\n",
        "+\n",
        "JJJJJJJJJJJJJJJJ\n",
        "@read4\n",
        "AAAATTTTCCCCGGGG\n",
        "+\n",
        "KKKKKKKKKKKKKKKK\n",
    );
    use std::io::Write;
    temp_file.write_all(fastq_content.as_bytes())?;
    temp_file.flush()?;
    Ok(temp_file)
}

async fn count_rows_for_range(range: FastqByteRange) -> datafusion::error::Result<usize> {
    let file = create_test_fastq_file().expect("create temp fastq");
    let path = file.path().to_string_lossy().to_string();

    let provider = FastqTableProvider::new_with_range(
        path,
        Some(range),
        Some(1),
        Some(ObjectStorageOptions::default()),
    )
    .expect("provider with range");

    let ctx = SessionContext::new();
    ctx.register_table("fastq", Arc::new(provider))?;

    let df = ctx.sql("SELECT name FROM fastq").await?;
    let batches = df.collect().await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

#[tokio::test]
async fn fastq_rows_respect_byte_range() -> datafusion::error::Result<()> {
    let rows = count_rows_for_range(FastqByteRange { start: 0, end: 86 }).await?;
    assert_eq!(rows, 2, "expected two records from the byte range");
    Ok(())
}

#[tokio::test]
async fn fastq_rows_skip_partial_leading_record() -> datafusion::error::Result<()> {
    let rows = count_rows_for_range(FastqByteRange {
        start: 20,
        end: 120,
    })
    .await?;
    assert_eq!(
        rows, 2,
        "expected two complete records after skipping partial start"
    );
    Ok(())
}
