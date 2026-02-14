use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use std::sync::Arc;

// use noodles::bed;
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // let file_path = "gs://polars-bio-it/chainMonDom5Link.bed.bgz".to_string();
    let file_path = "/tmp/chainMonDom5Link.bed.bgz".to_string();
    let object_storage_options = ObjectStorageOptions {
        allow_anonymous: false,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),        // 16 MB
        concurrent_fetches: Some(8), // Number of concurrent requests
        compression_type: None,
    };
    // let mut reader = BedRemoteReader::new(file_path.clone(), object_storage_options).await;

    // let file_path = "/tmp/chr16_fragile_site.bed.bgz".to_string();
    // let file_path =
    //     "/Users/mwiewior/CLionProjects/datafusion-bio-formats/sandbox/chr16_fragile_site.bed.bgz"
    //         .to_string();
    // let file_path = "gs://polars-bio-it/chr16_fragile_site.bed.bgz".to_string();
    // let n: usize = 6; // Number of threads
    // let mut reader = BedRemoteReader::<6>::new(file_path, object_storage_options).await;
    let table = BedTableProvider::new(
        file_path.clone(),
        BEDFields::BED4,
        Some(object_storage_options),
        true, // Use 0-based coordinates (default)
    )
    .unwrap();
    let ctx = datafusion::execution::context::SessionContext::new();
    ctx.sql("set datafusion.execution.skip_physical_aggregate_schema_check=true")
        .await?;
    ctx.register_table("example", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT * FROM example limit 2").await?;
    df.show().await?;
    // println!("Count: {:?}", results);

    // read all records
    // let mut cnt = 0;
    // let mut records = reader.read_records().await;
    //
    // while let Some(record_result) = records.next().await {
    //     match record_result {
    //         Ok(record) => println!("Record: {:?}", record),
    //         Err(e) => eprintln!("Error reading record: {}", e),
    //     }
    // }
    //
    // // let mut lines = reader.lines();
    // // while let Some(record_result) = lines.next().await {
    // //     match record_result {
    // //         Ok(record) => cnt += 1,
    // //         Err(e) => eprintln!("Error reading record: {}", e),
    // //     }
    // // }
    // println!("Total records read: {}", cnt);
    Ok(())
}
