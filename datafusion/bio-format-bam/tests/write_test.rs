//! Integration tests for BAM write functionality
//!
//! Tests verify that BAM files can be written correctly and that data
//! round-trips properly through read -> write -> read cycles.

use datafusion::arrow::array::{
    Float32Array, Int32Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, UInt8Type};
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
    tag_zb_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "B".to_string());
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
    tag_zc_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "B".to_string());
    tag_zc_metadata.insert(
        BAM_TAG_DESCRIPTION_KEY.to_string(),
        "Color space".to_string(),
    );

    // Note: Schema uses UInt8 for writing, but will be inferred as Int32 when reading
    // This is because all integer array types are normalized to Int32 during inference
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
    // Note: All integer arrays are normalized to Int32 during schema inference
    let array1 = zc_values.value(0);
    let int_array1 = array1.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_array1.len(), 3);
    assert_eq!(int_array1.value(0), 1);
    assert_eq!(int_array1.value(1), 2);
    assert_eq!(int_array1.value(2), 3);

    // Verify second array [10, 20]
    let array2 = zc_values.value(1);
    let int_array2 = array2.as_any().downcast_ref::<Int32Array>().unwrap();
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
        Field::new("XS", DataType::Float32, true).with_metadata(tag_xs_metadata),
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
            Arc::new(Float32Array::from(vec![Some(45.5), Some(38.2)])), // XS float tag
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
    let read_provider =
        BamTableProvider::new(output_path.to_str().unwrap().to_string(), None, true, None).await?;

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
    let read_provider =
        BamTableProvider::new(output_path.to_str().unwrap().to_string(), None, true, None).await?;

    let read_schema = read_provider.schema();
    let sort_order = read_schema.metadata().get(BAM_SORT_ORDER_KEY);
    assert_eq!(
        sort_order,
        Some(&"unsorted".to_string()),
        "Header should have SO:unsorted"
    );

    Ok(())
}
