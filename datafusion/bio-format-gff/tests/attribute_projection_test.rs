use datafusion::arrow::array::Array;
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_GFF_CONTENT_WITH_ATTRIBUTES: &str = r#"##gff-version 3
chr1	source1	gene	100	200	.	+	.	ID=gene1;Name=GeneA
chr1	source1	exon	100	150	.	+	.	ID=exon1;Parent=gene1
chr1	source1	exon	170	200	.	+	.	ID=exon2;Parent=gene1;Note=frameshift
chr2	source2	gene	300	400	.	-	.	ID=gene2;Name=GeneB;Alias=B,b
"#;

async fn create_test_gff_file_with_attributes() -> std::io::Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_file = format!("/tmp/test_attribute_projection_{}.gff", nanos);
    fs::write(&temp_file, SAMPLE_GFF_CONTENT_WITH_ATTRIBUTES).await?;
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
async fn test_attribute_projection_single_attribute() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file_with_attributes().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec!["ID".to_string(), "Name".to_string()]),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom, \"ID\" FROM test_gff").await?;
    let results = df.collect().await?;

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "ID");

    let seqname_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let id_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(seqname_array.value(0), "chr1");
    assert_eq!(id_array.value(0), "gene1");
    assert_eq!(id_array.value(1), "exon1");

    Ok(())
}

#[tokio::test]
async fn test_attribute_projection_multiple_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file_with_attributes().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec![
            "ID".to_string(),
            "Name".to_string(),
            "Parent".to_string(),
        ]),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"ID\", \"Parent\" FROM test_gff")
        .await?;
    let results = df.collect().await?;

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "ID");
    assert_eq!(batch.schema().field(2).name(), "Parent");

    let parent_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert!(parent_array.is_null(0));
    assert_eq!(parent_array.value(1), "gene1");

    Ok(())
}

#[tokio::test]
async fn test_attribute_projection_mixed_core_and_attributes()
-> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file_with_attributes().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec![
            "ID".to_string(),
            "Name".to_string(),
            "Note".to_string(),
        ]),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    let df = ctx
        .sql("SELECT \"Note\", start, \"ID\" FROM test_gff")
        .await?;
    let results = df.collect().await?;

    if results.is_empty() {
        return Ok(());
    }

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.schema().field(0).name(), "Note");
    assert_eq!(batch.schema().field(1).name(), "start");
    assert_eq!(batch.schema().field(2).name(), "ID");

    let note_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let start_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();

    assert!(note_array.is_null(1));
    assert_eq!(note_array.value(2), "frameshift");
    // With 0-based coordinates (default), start position is 169 (was 170 in 1-based)
    assert_eq!(start_array.value(2), 169);

    Ok(())
}

#[tokio::test]
async fn test_attribute_projection_no_attributes_queried() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path = create_test_gff_file_with_attributes().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec!["ID".to_string(), "Name".to_string()]),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM test_gff")
        .await?;
    let results = df.collect().await?;

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.schema().field(0).name(), "chrom");
    assert_eq!(batch.schema().field(1).name(), "start");
    assert_eq!(batch.schema().field(2).name(), "end");

    Ok(())
}

#[tokio::test]
async fn test_attribute_projection_all_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file_with_attributes().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec![
            "ID".to_string(),
            "Name".to_string(),
            "Parent".to_string(),
            "Note".to_string(),
            "Alias".to_string(),
        ]),
        Some(object_storage_options),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, \"ID\", \"Name\", \"Parent\", \"Note\", \"Alias\" FROM test_gff")
        .await?;
    let results = df.collect().await?;

    let batch = &results[0];

    assert_eq!(batch.num_columns(), 6);
    assert_eq!(batch.schema().field(1).name(), "ID");
    assert_eq!(batch.schema().field(5).name(), "Alias");

    let alias_array = batch
        .column(5)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(alias_array.value(3), "B,b");

    Ok(())
}
