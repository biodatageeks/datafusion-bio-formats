use datafusion_bio_format_fastq::FastqTableProvider;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path =
        "gs://genomics-public-data/platinum-genomes/fastq/ERR194146.fastq.gz".to_string();
    let object_storage_options = datafusion_bio_format_core::object_storage::ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),        // 16 MB
        concurrent_fetches: Some(8), // Number of concurrent requests
        compression_type: None,
    };
    let table = FastqTableProvider::new(file_path.clone(), Some(object_storage_options)).unwrap();

    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await?;
    ctx.register_table("example", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT * FROM example").await?;
    let results = df.count().await?;
    println!("Count: {results:?}");
    Ok(())
}
