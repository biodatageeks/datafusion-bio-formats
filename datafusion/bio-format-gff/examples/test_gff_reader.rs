use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    // let file_path = "gs://polars-bio-it/gencode.v38.annotation.gff3.bgz".to_string();
    let file_path = "/Users/mwiewior/Downloads/gencode.v38.annotation.gff3.bgz".to_string();
    let _object_storage_options =
        datafusion_bio_format_core::object_storage::ObjectStorageOptions {
            allow_anonymous: false,
            enable_request_payer: false,
            max_retries: Some(1),
            timeout: Some(300),
            chunk_size: Some(16),        // 16 MB
            concurrent_fetches: Some(8), // Number of concurrent requests
            compression_type: None,
        };
    // let mut reader = GffRemoteReader::new(file_path, object_storage_options).await?;
    // let mut reader = GffRemoteReader::new(file_path, object_storage_options).await?;
    let table = GffTableProvider::new(
        file_path.clone(),
        // None,
        Some(vec!["ID".to_string()]),
        None,
        true, // Use 0-based coordinates (default)
    )
    .unwrap();

    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await?;
    ctx.register_table("gff_table", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT start,`ID` FROM gff_table").await?;
    let results = df.collect().await?;
    println!("Count: {:?}", results.len());
    // let batches = results.await.unwrap().show().await?;
    // for batch in batches {
    //     println!("{:?}", batch);
    // }

    // let mut reader = GffLocalReader::new(file_path, 1).await?;
    // let mut records = reader.read_records().await;
    // let mut cnt = 0;
    // while let Some(record_result) = records.next().await {
    //     match record_result {
    //         Ok(record) => {
    //             println!("Record: {:?}", record);
    //             cnt += 1;
    //         }
    //         Err(e) => eprintln!("Error reading record: {}", e),
    //     }
    //     if cnt >= 2 {
    //         break; // Limit to 10 records for testing
    //     }
    // }
    //
    // println!("Total records read: {}", cnt);
    Ok(())
}
