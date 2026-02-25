use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;
use std::time::Instant;

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::AUTO),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v49.annotation.gff3.bgz";
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.to_string(),
        None,
        Some(object_storage_options),
        true, // Use 0-based coordinates (default)
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("gff", Arc::new(table))?;

    // Get actual counts
    println!("Getting actual row counts...");

    let queries = vec![
        ("SELECT COUNT(*) as total FROM gff", "Total records"),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1'",
            "chr1 records",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE chrom = 'chr1' AND type = 'gene'",
            "chr1 genes",
        ),
        (
            "SELECT COUNT(*) FROM gff WHERE start > 100000 AND start < 200000",
            "Position range records",
        ),
        (
            "SELECT DISTINCT chrom FROM gff ORDER BY chrom LIMIT 10",
            "Sample chromosomes",
        ),
        (
            "SELECT DISTINCT type FROM gff ORDER BY type LIMIT 10",
            "Sample feature types",
        ),
    ];

    for (query, description) in queries {
        println!("\n{description}: {query}");
        let start = Instant::now();
        let df = ctx.sql(query).await?;
        let results = df.collect().await?;
        let duration = start.elapsed();

        for batch in results {
            for row in 0..batch.num_rows() {
                let mut values = Vec::new();
                for col in 0..batch.num_columns() {
                    let array = batch.column(col);
                    let value = match array.data_type() {
                        datafusion::arrow::datatypes::DataType::Int64 => {
                            let int_array = array
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                                .unwrap();
                            int_array.value(row).to_string()
                        }
                        datafusion::arrow::datatypes::DataType::Utf8 => {
                            let str_array = array
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                                .unwrap();
                            str_array.value(row).to_string()
                        }
                        _ => "?".to_string(),
                    };
                    values.push(value);
                }
                println!("  {}", values.join(", "));
            }
        }
        println!("  Time: {:.3}s", duration.as_secs_f64());
    }

    Ok(())
}
