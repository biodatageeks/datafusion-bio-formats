use datafusion_bio_format_bam::table_provider::BamTableProvider;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path =
        "gs://genomics-public-data/1000-genomes/bam/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam".to_string();
    let object_storage_options = datafusion_bio_format_core::object_storage::ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(4),
        timeout: Some(300),
        chunk_size: Some(8),         // 16 MB
        concurrent_fetches: Some(1), // Number of concurrent requests
        compression_type: None,
    };
    let table = BamTableProvider::new(
        file_path.clone(),
        Some(1),
        Some(object_storage_options),
        true,
        None, // No tag fields
    )
    .unwrap();

    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await?;
    ctx.register_table("example", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT * FROM example").await?;
    // let results = df.count().await?;
    // println!("Count: {:?}", results);
    df.limit(0, Some(10)).unwrap().show().await?;

    // let file_path = "/Users/mwiewior/research/data/NA12878.proper.wes.md.chr1.bam".to_string();
    // let reader = BamReader::new(
    //     file_path,
    //     Some(4), // Number of threads
    //     None,    // Object storage options
    // );
    // let mut reader = reader.await;
    //
    // let mut records = reader.read_records().await;
    // let mut cnt = 0;
    // while let Some(record_result) = records.next().await {
    //     match record_result {
    //         Ok(record) => {
    //             // println!("Record: {:?}", record);
    //             cnt += 1;
    //         }
    //         Err(e) => eprintln!("Error reading record: {}", e),
    //     }
    // }
    // println!("Total records read: {}", cnt);
    Ok(())
}
