//! Integration tests for BAM write functionality
//!
//! Tests verify that BAM files can be written correctly and that data
//! round-trips properly through read -> write -> read cycles.

use datafusion::arrow::array::{
    Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema, UInt8Type,
    UInt16Type, UInt64Type,
};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_core::{
    BAM_REFERENCE_SEQUENCES_KEY, BAM_SORT_ORDER_KEY, BAM_TAG_DESCRIPTION_KEY, BAM_TAG_TAG_KEY,
    BAM_TAG_TYPE_KEY,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn tag_field(name: &str, data_type: DataType, sam_type: &str, description: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert(BAM_TAG_TAG_KEY.to_string(), name.to_string());
    metadata.insert(BAM_TAG_TYPE_KEY.to_string(), sam_type.to_string());
    metadata.insert(BAM_TAG_DESCRIPTION_KEY.to_string(), description.to_string());
    Field::new(name, data_type, true).with_metadata(metadata)
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item_type, true)))
}

/// Test that alignment tags are properly preserved during write operations
#[tokio::test]
async fn test_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_tags.bam");

    // Create a schema with tag fields
    let mut tag_nm_metadata = HashMap::new();
    tag_nm_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "NM".to_string());
    tag_nm_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "i".to_string());
    tag_nm_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Edit distance".to_string(),
    );

    let mut tag_md_metadata = HashMap::new();
    tag_md_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "MD".to_string());
    tag_md_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "Z".to_string());
    tag_md_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Mismatch positions".to_string(),
    );

    let mut tag_as_metadata = HashMap::new();
    tag_as_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "AS".to_string());
    tag_as_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "i".to_string());
    tag_as_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Alignment score".to_string(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        Field::new("NM", DataType::Int32, true).with_metadata(tag_nm_metadata),
        Field::new("MD", DataType::Utf8, true).with_metadata(tag_md_metadata),
        Field::new("AS", DataType::Int32, true).with_metadata(tag_as_metadata),
    ]));

    // Create test data with tags
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2", "read3"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 200, 300])),
            Arc::new(UInt32Array::from(vec![0, 16, 0])), // flags
            Arc::new(StringArray::from(vec!["10M", "10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60, 60])), // mapq
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None, None])),
            Arc::new(StringArray::from(vec![
                "ACGTACGTAC",
                "ACGTACGTAC",
                "TTTTTTTTTT",
            ])),
            Arc::new(StringArray::from(vec![
                "IIIIIIIIII",
                "IIIIIIIIII",
                "IIIIIIIIII",
            ])),
            Arc::new(Int32Array::from(vec![0i32; 3])),
            // Tags
            Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(0)])), // NM
            Arc::new(StringArray::from(vec![
                Some("10"),
                Some("5^A4"),
                Some("10"),
            ])), // MD
            Arc::new(Int32Array::from(vec![Some(50), Some(45), Some(60)])), // AS
        ],
    )?;

    // Create DataFusion context and register batch as input table
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Create output table provider
    let tag_fields = vec!["NM".to_string(), "MD".to_string(), "AS".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,  // 0-based coordinates
        false, // sort_on_write
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    // Write the data using SQL
    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Now read back and verify tags are preserved
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None, // storage options
        true, // 0-based coordinates
        Some(tag_fields.clone()),
        false, // String CIGAR (default)
        true,
        100,
        None,
    )
    .await?;

    ctx.register_table("test_bam", Arc::new(read_provider))?;

    // Query with tag projection
    let df = ctx
        .sql("SELECT name, \"NM\", \"MD\", \"AS\" FROM test_bam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    // Verify names
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "read1");
    assert_eq!(names.value(1), "read2");
    assert_eq!(names.value(2), "read3");

    // Verify NM tag (integer)
    let nm_values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(nm_values.value(0), 2);
    assert_eq!(nm_values.value(1), 1);
    assert_eq!(nm_values.value(2), 0);

    // Verify MD tag (string)
    let md_values = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(md_values.value(0), "10");
    assert_eq!(md_values.value(1), "5^A4");
    assert_eq!(md_values.value(2), "10");

    // Verify AS tag (integer)
    let as_values = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(as_values.value(0), 50);
    assert_eq!(as_values.value(1), 45);
    assert_eq!(as_values.value(2), 60);

    Ok(())
}

/// Test that character tags are properly preserved
#[tokio::test]
async fn test_character_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_char_tags.bam");

    // Create a schema with a character tag field
    let mut tag_xt_metadata = HashMap::new();
    tag_xt_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "XT".to_string());
    tag_xt_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "A".to_string());
    tag_xt_metadata.insert(BAM_TAG_DESCRIPTION_KEY.to_string(), "Type tag".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        Field::new("XT", DataType::Utf8, true).with_metadata(tag_xt_metadata),
    ]));

    // Create test data with character tag
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 2])),
            Arc::new(StringArray::from(vec![Some("U"), Some("R")])), // XT character tag
        ],
    )?;

    // Create DataFusion context and register batch as input table
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Create output table provider
    let tag_fields = vec!["XT".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    // Write the data using SQL
    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify using inferred schema
    let read_provider = BamTableProvider::try_new_with_inferred_schema(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        Some(10), // Sample 10 records
        false,    // String CIGAR (default)
    )
    .await?;

    ctx.register_table("test_bam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, \"XT\" FROM test_bam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let xt_values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify character values
    assert_eq!(xt_values.value(0), "U");
    assert_eq!(xt_values.value(1), "R");

    Ok(())
}

/// Test that array tags (integer arrays) are properly preserved
#[tokio::test]
async fn test_integer_array_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_int_array_tags.bam");

    // Create a schema with an integer array tag field
    let mut tag_zb_metadata = HashMap::new();
    tag_zb_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "ZB".to_string());
    tag_zb_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "B:i".to_string());
    tag_zb_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Base qualities".to_string(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        Field::new(
            "ZB",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )
        .with_metadata(tag_zb_metadata),
    ]));

    // Create test data with integer array tag
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(10), Some(20), Some(30)]),
        Some(vec![Some(5), Some(15), Some(25), Some(35)]),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 2])),
            Arc::new(list_array),
        ],
    )?;

    // Create DataFusion context and register batch as input table
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Create output table provider
    let tag_fields = vec!["ZB".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    // Write the data using SQL
    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify using inferred schema
    let read_provider = BamTableProvider::try_new_with_inferred_schema(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        Some(10), // Sample 10 records
        false,    // String CIGAR (default)
    )
    .await?;

    ctx.register_table("test_bam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, \"ZB\" FROM test_bam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let zb_values = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Verify first array [10, 20, 30]
    let array1 = zb_values.value(0);
    let int_array1 = array1.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_array1.len(), 3);
    assert_eq!(int_array1.value(0), 10);
    assert_eq!(int_array1.value(1), 20);
    assert_eq!(int_array1.value(2), 30);

    // Verify second array [5, 15, 25, 35]
    let array2 = zb_values.value(1);
    let int_array2 = array2.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_array2.len(), 4);
    assert_eq!(int_array2.value(0), 5);
    assert_eq!(int_array2.value(1), 15);
    assert_eq!(int_array2.value(2), 25);
    assert_eq!(int_array2.value(3), 35);

    Ok(())
}

/// Test that byte array tags (UInt8 arrays) are properly preserved
#[tokio::test]
async fn test_byte_array_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_byte_array_tags.bam");

    // Create a schema with a byte array tag field
    let mut tag_zc_metadata = HashMap::new();
    tag_zc_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "ZC".to_string());
    tag_zc_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "B:C".to_string());
    tag_zc_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Color space".to_string(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        Field::new(
            "ZC",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            true,
        )
        .with_metadata(tag_zc_metadata),
    ]));

    // Create test data with byte array tag
    let list_array = ListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(10), Some(20)]),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 2])),
            Arc::new(list_array),
        ],
    )?;

    // Create DataFusion context and register batch as input table
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Create output table provider
    let tag_fields = vec!["ZC".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    // Write the data using SQL
    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify using inferred schema
    let read_provider = BamTableProvider::try_new_with_inferred_schema(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        Some(10), // Sample 10 records
        false,    // String CIGAR (default)
    )
    .await?;

    ctx.register_table("test_bam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, \"ZC\" FROM test_bam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let zc_values = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Verify first array [1, 2, 3]
    let array1 = zc_values.value(0);
    let int_array1 = array1.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(int_array1.len(), 3);
    assert_eq!(int_array1.value(0), 1);
    assert_eq!(int_array1.value(1), 2);
    assert_eq!(int_array1.value(2), 3);

    // Verify second array [10, 20]
    let array2 = zc_values.value(1);
    let int_array2 = array2.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(int_array2.len(), 2);
    assert_eq!(int_array2.value(0), 10);
    assert_eq!(int_array2.value(1), 20);

    Ok(())
}

/// Test that float tags are properly preserved
#[tokio::test]
async fn test_float_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_float_tags.bam");

    // Create a schema with a float tag field
    let mut tag_xs_metadata = HashMap::new();
    tag_xs_metadata.insert(BAM_TAG_TAG_KEY.to_string(), "XS".to_string());
    tag_xs_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "f".to_string());
    tag_xs_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Suboptimal alignment score".to_string(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        Field::new("XS", DataType::Float64, true).with_metadata(tag_xs_metadata),
    ]));

    // Create test data with float tag
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 2])),
            Arc::new(Float64Array::from(vec![Some(45.5), Some(38.2)])), // XS float tag
        ],
    )?;

    // Create DataFusion context and register batch as input table
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Create output table provider
    let tag_fields = vec!["XS".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    // Write the data using SQL
    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify using inferred schema
    let read_provider = BamTableProvider::try_new_with_inferred_schema(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        Some(10), // Sample 10 records
        false,    // String CIGAR (default)
    )
    .await?;

    ctx.register_table("test_bam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, \"XS\" FROM test_bam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let xs_values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    // Verify float values with tolerance
    assert!((xs_values.value(0) - 45.5).abs() < 0.01);
    assert!((xs_values.value(1) - 38.2).abs() < 0.01);

    Ok(())
}

#[tokio::test]
async fn test_full_tag_type_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_full_tag_types.bam");

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
        tag_field("ch", DataType::Utf8, "A", "Custom character tag"),
        tag_field("sv", DataType::Utf8, "Z", "Custom string tag"),
        tag_field("hx", DataType::Utf8, "H", "Custom hex tag"),
        tag_field("de", DataType::Float64, "f", "Custom float tag"),
        tag_field("ni", DataType::Int64, "i", "Custom integer tag"),
        tag_field("ui", DataType::UInt64, "I", "Custom unsigned integer tag"),
        tag_field(
            "pc",
            list_type(DataType::Int8),
            "B:c",
            "Custom int8 array tag",
        ),
        tag_field(
            "pC",
            list_type(DataType::UInt8),
            "B:C",
            "Custom uint8 array tag",
        ),
        tag_field(
            "ps",
            list_type(DataType::Int16),
            "B:s",
            "Custom int16 array tag",
        ),
        tag_field(
            "pS",
            list_type(DataType::UInt16),
            "B:S",
            "Custom uint16 array tag",
        ),
        tag_field(
            "pa",
            list_type(DataType::Int64),
            "B:i",
            "Custom int32 array tag",
        ),
        tag_field(
            "pI",
            list_type(DataType::UInt64),
            "B:I",
            "Custom uint32 array tag",
        ),
        tag_field(
            "pf",
            list_type(DataType::Float64),
            "B:f",
            "Custom float array tag",
        ),
        tag_field(
            "ML",
            list_type(DataType::UInt8),
            "B:C",
            "Base modification probabilities",
        ),
        tag_field(
            "FZ",
            list_type(DataType::UInt16),
            "B:S",
            "Flow signal intensities",
        ),
        tag_field(
            "CG",
            list_type(DataType::UInt64),
            "B:I",
            "BAM-only CIGAR overflow tag",
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 220])),
            Arc::new(UInt32Array::from(vec![0, 16])),
            Arc::new(StringArray::from(vec!["10M", "8M2S"])),
            Arc::new(UInt32Array::from(vec![60, 42])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "TTTTGGGGAA"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "JJJJJJJJJJ"])),
            Arc::new(Int32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec![Some("A"), Some("Z")])),
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])),
            Arc::new(StringArray::from(vec![Some("0fa0"), Some("beef")])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.25)])),
            Arc::new(Int64Array::from(vec![Some(42), Some(-7)])),
            Arc::new(UInt64Array::from(vec![Some(3_000_000_000), Some(7)])),
            Arc::new(ListArray::from_iter_primitive::<Int8Type, _, _>(vec![
                Some(vec![Some(-1), Some(0), Some(5)]),
                Some(vec![Some(12), Some(34)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(250), Some(4)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>(vec![
                Some(vec![Some(-30), Some(40)]),
                Some(vec![Some(300), Some(400)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(vec![
                Some(vec![Some(10), Some(20)]),
                Some(vec![Some(50000)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(100000), Some(200000)]),
                Some(vec![Some(-5), Some(17)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
                Some(vec![Some(100000), Some(200000)]),
                Some(vec![Some(4_000_000_000), Some(17)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(1.25), Some(2.5)]),
                Some(vec![Some(3.75)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                Some(vec![Some(4), Some(5), Some(6)]),
                Some(vec![Some(7), Some(8)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(vec![
                Some(vec![Some(10), Some(1000)]),
                Some(vec![Some(65000)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
                None::<Vec<Option<u64>>>,
                None::<Vec<Option<u64>>>,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let tag_fields = vec![
        "ch".to_string(),
        "sv".to_string(),
        "hx".to_string(),
        "de".to_string(),
        "ni".to_string(),
        "ui".to_string(),
        "pc".to_string(),
        "pC".to_string(),
        "ps".to_string(),
        "pS".to_string(),
        "pa".to_string(),
        "pI".to_string(),
        "pf".to_string(),
        "ML".to_string(),
        "FZ".to_string(),
        "CG".to_string(),
    ];
    let read_hints = vec![
        "ch:A".to_string(),
        "sv:Z".to_string(),
        "hx:H".to_string(),
        "de:f".to_string(),
        "ni:i".to_string(),
        "ui:I".to_string(),
        "pc:B:c".to_string(),
        "pC:B:C".to_string(),
        "ps:B:s".to_string(),
        "pS:B:S".to_string(),
        "pa:B:i".to_string(),
        "pI:B:I".to_string(),
        "pf:B:f".to_string(),
    ];

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        false,
        false,
        100,
        Some(read_hints),
    )
    .await?;

    let read_schema = read_provider.schema();
    assert_eq!(
        read_schema.field_with_name("pc")?.data_type(),
        &list_type(DataType::Int8)
    );
    assert_eq!(
        read_schema.field_with_name("pC")?.data_type(),
        &list_type(DataType::UInt8)
    );
    assert_eq!(
        read_schema.field_with_name("ps")?.data_type(),
        &list_type(DataType::Int16)
    );
    assert_eq!(
        read_schema.field_with_name("pS")?.data_type(),
        &list_type(DataType::UInt16)
    );
    assert_eq!(
        read_schema.field_with_name("pa")?.data_type(),
        &list_type(DataType::Int32)
    );
    assert_eq!(
        read_schema.field_with_name("pI")?.data_type(),
        &list_type(DataType::UInt32)
    );
    assert_eq!(
        read_schema.field_with_name("pf")?.data_type(),
        &list_type(DataType::Float32)
    );
    assert_eq!(
        read_schema.field_with_name("ML")?.data_type(),
        &list_type(DataType::UInt8)
    );
    assert_eq!(
        read_schema.field_with_name("FZ")?.data_type(),
        &list_type(DataType::UInt16)
    );
    assert_eq!(
        read_schema.field_with_name("CG")?.data_type(),
        &list_type(DataType::UInt32)
    );

    ctx.register_table("test_bam", Arc::new(read_provider))?;
    let df = ctx
        .sql(
            "SELECT name, \"ch\", \"sv\", \"hx\", \"de\", \"ni\", \"ui\", \
             \"pc\", \"pC\", \"ps\", \"pS\", \"pa\", \"pI\", \"pf\", \"ML\", \"FZ\", \"CG\" \
             FROM test_bam ORDER BY name",
        )
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let ch = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sv = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let hx = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let de = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let ni = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let ui = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    assert_eq!(ch.value(0), "A");
    assert_eq!(ch.value(1), "Z");
    assert_eq!(sv.value(0), "alpha");
    assert_eq!(sv.value(1), "beta");
    assert_eq!(hx.value(0), "0FA0");
    assert_eq!(hx.value(1), "BEEF");
    assert!((de.value(0) - 1.5).abs() < 0.01);
    assert!((de.value(1) - 2.25).abs() < 0.01);
    assert_eq!(ni.value(0), 42);
    assert_eq!(ni.value(1), -7);
    assert_eq!(ui.value(0), 3_000_000_000);
    assert_eq!(ui.value(1), 7);

    let pc = batch
        .column(7)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let pc = pc.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(pc.value(0), -1);
    assert_eq!(pc.value(1), 0);
    assert_eq!(pc.value(2), 5);

    let p_c = batch
        .column(8)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(1);
    let p_c = p_c.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(p_c.value(0), 250);
    assert_eq!(p_c.value(1), 4);

    let ps = batch
        .column(9)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let ps = ps.as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(ps.value(0), -30);
    assert_eq!(ps.value(1), 40);

    let p_s = batch
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(1);
    let p_s = p_s.as_any().downcast_ref::<UInt16Array>().unwrap();
    assert_eq!(p_s.value(0), 50000);

    let pa = batch
        .column(11)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let pa = pa.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(pa.value(0), 100000);
    assert_eq!(pa.value(1), 200000);

    let p_i = batch
        .column(12)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(1);
    let p_i = p_i.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(p_i.value(0), 4_000_000_000);
    assert_eq!(p_i.value(1), 17);

    let pf = batch
        .column(13)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let pf = pf.as_any().downcast_ref::<Float32Array>().unwrap();
    assert!((pf.value(0) - 1.25).abs() < 0.01);
    assert!((pf.value(1) - 2.5).abs() < 0.01);

    let ml = batch
        .column(14)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let ml = ml.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(ml.value(0), 4);
    assert_eq!(ml.value(1), 5);
    assert_eq!(ml.value(2), 6);

    let fz = batch
        .column(15)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(1);
    let fz = fz.as_any().downcast_ref::<UInt16Array>().unwrap();
    assert_eq!(fz.value(0), 65000);

    let cg = batch
        .column(16)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(cg.is_null(0));
    assert!(cg.is_null(1));

    Ok(())
}

#[tokio::test]
async fn test_read_add_write_read_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let input_path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/multi_chrom.bam");
    let output_path = temp_dir.path().join("test_read_add_write_read.bam");

    let ctx = SessionContext::new();
    let read_provider = BamTableProvider::new(
        input_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
    .await?;
    ctx.register_table("input_bam", Arc::new(read_provider))?;

    let source = ctx
        .sql(
            "SELECT name, chrom, start, \"end\", flags, cigar, mapping_quality, mate_chrom, \
             mate_start, sequence, quality_scores, template_length \
             FROM input_bam ORDER BY chrom, start LIMIT 2",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(source.len(), 1);
    let source_batch = &source[0];

    let mut fields: Vec<Field> = source_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(tag_field("de", DataType::Float64, "f", "Added float tag"));
    fields.push(tag_field("sv", DataType::Utf8, "Z", "Added string tag"));
    fields.push(tag_field(
        "pa",
        list_type(DataType::Int64),
        "B:i",
        "Added integer array tag",
    ));
    fields.push(tag_field(
        "ML",
        list_type(DataType::UInt8),
        "B:C",
        "Added standard array tag",
    ));

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_batch.schema().metadata().clone(),
    ));

    let mut columns = source_batch.columns().to_vec();
    columns.push(Arc::new(Float64Array::from(vec![Some(10.5), Some(11.25)])));
    columns.push(Arc::new(StringArray::from(vec![
        Some("added-a"),
        Some("added-b"),
    ])));
    columns.push(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
        ],
    )));
    columns.push(Arc::new(ListArray::from_iter_primitive::<UInt8Type, _, _>(
        vec![
            Some(vec![Some(9), Some(8)]),
            Some(vec![Some(7), Some(6), Some(5)]),
        ],
    )));

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    ctx.register_batch("input_with_tags", batch)?;

    let tag_fields = vec![
        "de".to_string(),
        "sv".to_string(),
        "pa".to_string(),
        "ML".to_string(),
    ];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema,
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_with_tags")
        .await?
        .collect()
        .await?;

    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields.clone()),
        false,
        false,
        100,
        Some(vec![
            "de:f".to_string(),
            "sv:Z".to_string(),
            "pa:B:i".to_string(),
        ]),
    )
    .await?;
    ctx.register_table("round_trip_bam", Arc::new(read_provider))?;

    let results = ctx
        .sql(
            "SELECT name, chrom, start, \"de\", \"sv\", \"pa\", \"ML\" \
             FROM round_trip_bam ORDER BY chrom, start",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);

    let de = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let sv = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let pa = batch
        .column(5)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let ml = batch
        .column(6)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert!((de.value(0) - 10.5).abs() < 0.01);
    assert!((de.value(1) - 11.25).abs() < 0.01);
    assert_eq!(sv.value(0), "added-a");
    assert_eq!(sv.value(1), "added-b");

    let pa0 = pa.value(0);
    let pa0 = pa0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(pa0.value(0), 1);
    assert_eq!(pa0.value(1), 2);
    assert_eq!(pa0.value(2), 3);

    let ml1 = ml.value(1);
    let ml1 = ml1.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(ml1.value(0), 7);
    assert_eq!(ml1.value(1), 6);
    assert_eq!(ml1.value(2), 5);

    Ok(())
}

/// Test writing without tags (tags should be optional)
#[tokio::test]
async fn test_write_without_tags() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_no_tags.bam");

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mapping_quality", DataType::UInt32, false),
        Field::new("mate_chrom", DataType::Utf8, true),
        Field::new("mate_start", DataType::UInt32, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
        Field::new("template_length", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1"])),
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![100])),
            Arc::new(UInt32Array::from(vec![0])),
            Arc::new(StringArray::from(vec!["10M"])),
            Arc::new(UInt32Array::from(vec![60])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(UInt32Array::from(vec![None::<u32>])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 1])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Write without tags
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None, // No tags
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Verify file was written
    assert!(output_path.exists());

    Ok(())
}

/// Test that sort_on_write=true sorts records by coordinate and sets SO:coordinate
#[tokio::test]
async fn test_sort_on_write_coordinate_order() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_sorted.bam");

    // Schema with reference sequences (needed for coordinate sort)
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        BAM_REFERENCE_SEQUENCES_KEY.to_string(),
        r#"[{"name":"chr1","length":249250621},{"name":"chr2","length":243199373}]"#.to_string(),
    );

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("flags", DataType::UInt32, false),
            Field::new("cigar", DataType::Utf8, false),
            Field::new("mapping_quality", DataType::UInt32, false),
            Field::new("mate_chrom", DataType::Utf8, true),
            Field::new("mate_start", DataType::UInt32, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
            Field::new("template_length", DataType::Int32, false),
        ],
        schema_metadata,
    ));

    // Create out-of-order records: chr2:300, chr1:100, chr1:200
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read_c", "read_a", "read_b"])),
            Arc::new(StringArray::from(vec!["chr2", "chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![300, 100, 200])),
            Arc::new(UInt32Array::from(vec![0, 0, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None, None])),
            Arc::new(StringArray::from(vec![
                "ACGTACGTAC",
                "ACGTACGTAC",
                "TTTTTTTTTT",
            ])),
            Arc::new(StringArray::from(vec![
                "IIIIIIIIII",
                "IIIIIIIIII",
                "IIIIIIIIII",
            ])),
            Arc::new(Int32Array::from(vec![0i32; 3])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Write with sort_on_write=true
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        true, // sort_on_write
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify order: chr1:100, chr1:200, chr2:300
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
    .await?;

    // Verify header has SO:coordinate
    let read_schema = read_provider.schema();
    let sort_order = read_schema.metadata().get(BAM_SORT_ORDER_KEY);
    assert_eq!(
        sort_order,
        Some(&"coordinate".to_string()),
        "Header should have SO:coordinate"
    );

    ctx.register_table("sorted_bam", Arc::new(read_provider))?;

    // Don't ORDER BY in SQL - we want to verify the file's physical order
    let df = ctx.sql("SELECT name, chrom, start FROM sorted_bam").await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let chroms = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let starts = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    // Should be sorted: chr1:100, chr1:200, chr2:300
    assert_eq!(chroms.value(0), "chr1");
    assert_eq!(starts.value(0), 100);
    assert_eq!(names.value(0), "read_a");

    assert_eq!(chroms.value(1), "chr1");
    assert_eq!(starts.value(1), 200);
    assert_eq!(names.value(1), "read_b");

    assert_eq!(chroms.value(2), "chr2");
    assert_eq!(starts.value(2), 300);
    assert_eq!(names.value(2), "read_c");

    Ok(())
}

/// Test that sort_on_write=false sets SO:unsorted in the header
#[tokio::test]
async fn test_sort_on_write_false_sets_unsorted() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_unsorted.bam");

    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        BAM_REFERENCE_SEQUENCES_KEY.to_string(),
        r#"[{"name":"chr1","length":249250621}]"#.to_string(),
    );

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("flags", DataType::UInt32, false),
            Field::new("cigar", DataType::Utf8, false),
            Field::new("mapping_quality", DataType::UInt32, false),
            Field::new("mate_chrom", DataType::Utf8, true),
            Field::new("mate_start", DataType::UInt32, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
            Field::new("template_length", DataType::Int32, false),
        ],
        schema_metadata,
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1"])),
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![100])),
            Arc::new(UInt32Array::from(vec![0])),
            Arc::new(StringArray::from(vec!["10M"])),
            Arc::new(UInt32Array::from(vec![60])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(UInt32Array::from(vec![None::<u32>])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0i32; 1])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Write with sort_on_write=false
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false, // sort_on_write
    );
    ctx.register_table("output_bam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back and verify header has SO:unsorted
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
        true,
        100,
        None,
    )
    .await?;

    let read_schema = read_provider.schema();
    let sort_order = read_schema.metadata().get(BAM_SORT_ORDER_KEY);
    assert_eq!(
        sort_order,
        Some(&"unsorted".to_string()),
        "Header should have SO:unsorted"
    );

    Ok(())
}
