use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_FASTQ_CONTENT: &str = r#"@read1
ATCGATCGATCG
+
IIIIIIIIIIII
@read2
GCTAGCTAGCTA
+
JJJJJJJJJJJJ
@read3
TTTTAAAACCCC
+
KKKKKKKKKKKK
"#;

async fn create_test_fastq_file(test_name: &str) -> std::io::Result<String> {
    let temp_file = format!("/tmp/test_projection_{}.fastq", test_name);
    fs::write(&temp_file, SAMPLE_FASTQ_CONTENT).await?;
    Ok(temp_file)
}

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::NONE),
    }
}

#[tokio::test]
async fn test_projection_single_column_name() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("single_column_name").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting only the 'name' column
    let df = ctx.sql("SELECT name FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should only have 1 column (name)
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.num_rows(), 3);

    // Verify the data
    let name_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(name_array.value(0), "read1");
    assert_eq!(name_array.value(1), "read2");
    assert_eq!(name_array.value(2), "read3");

    Ok(())
}

#[tokio::test]
async fn test_projection_two_columns() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("two_columns").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting name and sequence columns
    let df = ctx.sql("SELECT name, sequence FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should only have 2 columns
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "sequence");
    assert_eq!(batch.num_rows(), 3);

    // Verify the data
    let name_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let sequence_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(name_array.value(0), "read1");
    assert_eq!(sequence_array.value(0), "ATCGATCGATCG");
    assert_eq!(name_array.value(1), "read2");
    assert_eq!(sequence_array.value(1), "GCTAGCTAGCTA");

    Ok(())
}

#[tokio::test]
async fn test_projection_quality_scores_only() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("quality_scores_only").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting only quality_scores column
    let df = ctx.sql("SELECT quality_scores FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should only have 1 column
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "quality_scores");
    assert_eq!(batch.num_rows(), 3);

    // Verify the data
    let quality_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(quality_array.value(0), "IIIIIIIIIIII");
    assert_eq!(quality_array.value(1), "JJJJJJJJJJJJ");
    assert_eq!(quality_array.value(2), "KKKKKKKKKKKK");

    Ok(())
}

#[tokio::test]
async fn test_no_projection_all_columns() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("no_projection_all_columns").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting all columns
    let df = ctx.sql("SELECT * FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should have all 4 columns
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "description");
    assert_eq!(batch.schema().field(2).name(), "sequence");
    assert_eq!(batch.schema().field(3).name(), "quality_scores");
    assert_eq!(batch.num_rows(), 3);

    Ok(())
}

#[tokio::test]
async fn test_projection_with_count() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("projection_with_count").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test COUNT - this will use empty projection optimization
    let df = ctx.sql("SELECT COUNT(name) FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify count result - COUNT(name) should return 3
    let count_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 3);

    Ok(())
}

#[tokio::test]
async fn test_projection_reordered_columns() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("projection_reordered_columns").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting columns in different order
    let df = ctx
        .sql("SELECT sequence, name, quality_scores FROM test_fastq")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should have 3 columns in the requested order
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.schema().field(0).name(), "sequence");
    assert_eq!(batch.schema().field(1).name(), "name");
    assert_eq!(batch.schema().field(2).name(), "quality_scores");
    assert_eq!(batch.num_rows(), 3);

    // Verify the data is in correct order
    let sequence_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let name_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let quality_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(sequence_array.value(0), "ATCGATCGATCG");
    assert_eq!(name_array.value(0), "read1");
    assert_eq!(quality_array.value(0), "IIIIIIIIIIII");

    Ok(())
}

#[tokio::test]
async fn test_projection_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("projection_with_limit").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(file_path.clone(), Some(1), Some(object_storage_options))?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test projection with LIMIT
    let df = ctx
        .sql("SELECT name, sequence FROM test_fastq LIMIT 2")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should only have 2 columns and 2 rows due to LIMIT
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 2); // Limited to 2 rows
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "sequence");

    // Verify the first 2 records
    let name_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let sequence_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(name_array.value(0), "read1");
    assert_eq!(sequence_array.value(0), "ATCGATCGATCG");
    assert_eq!(name_array.value(1), "read2");
    assert_eq!(sequence_array.value(1), "GCTAGCTAGCTA");

    Ok(())
}

#[tokio::test]
async fn test_multithreaded_projection() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_fastq_file("multithreaded_projection").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(
        file_path.clone(),
        Some(4), // Use 4 threads to test multithreaded projection
        Some(object_storage_options),
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_fastq", Arc::new(table))?;

    // Test selecting only the 'name' column with multithreading
    let df = ctx.sql("SELECT name FROM test_fastq").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should only have 1 column (name)
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.num_rows(), 3);

    // Verify the data
    let name_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(name_array.value(0), "read1");
    assert_eq!(name_array.value(1), "read2");
    assert_eq!(name_array.value(2), "read3");

    Ok(())
}
