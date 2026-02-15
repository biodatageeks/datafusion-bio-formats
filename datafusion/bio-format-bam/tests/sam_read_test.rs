//! Integration tests for SAM read functionality
//!
//! Tests verify that SAM files can be read correctly, including round-trip
//! through write -> read cycles and format detection.

use datafusion::arrow::array::{Array, Int32Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_bam::storage::is_sam_file;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_core::{
    BAM_REFERENCE_SEQUENCES_KEY, BAM_TAG_DESCRIPTION_KEY, BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY,
    COORDINATE_SYSTEM_METADATA_KEY,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a basic schema with optional tag fields.
/// Includes reference sequence metadata needed for SAM round-trip.
fn create_test_schema(tag_fields: &[(&str, DataType, &str, &str)]) -> Arc<Schema> {
    let mut fields = vec![
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
    ];

    for (name, dtype, sam_type, desc) in tag_fields {
        let mut metadata = HashMap::new();
        metadata.insert(BAM_TAG_TAG_KEY.to_string(), name.to_string());
        metadata.insert(BAM_TAG_TYPE_KEY.to_string(), sam_type.to_string());
        metadata.insert(BAM_TAG_DESCRIPTION_KEY.to_string(), desc.to_string());
        fields.push(Field::new(*name, dtype.clone(), true).with_metadata(metadata));
    }

    // Add schema-level metadata with reference sequences for SAM round-trip
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        BAM_REFERENCE_SEQUENCES_KEY.to_string(),
        r#"[{"name":"chr1","length":249250621},{"name":"chr2","length":243199373}]"#.to_string(),
    );
    schema_metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        "true".to_string(),
    );

    Arc::new(Schema::new_with_metadata(fields, schema_metadata))
}

/// Helper to create basic test data without tags
fn create_basic_test_batch(schema: Arc<Schema>) -> datafusion::arrow::array::RecordBatch {
    datafusion::arrow::array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2", "read3"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 200, 300])),
            Arc::new(UInt32Array::from(vec![0, 16, 0])),
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
            Arc::new(Int32Array::from(vec![150, -150, 0])),
        ],
    )
    .unwrap()
}

/// Write test data to a SAM file using the existing write support,
/// then read it back and verify all fields.
#[tokio::test]
async fn test_sam_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_round_trip.sam");

    let schema = create_test_schema(&[]);
    let batch = create_basic_test_batch(schema.clone());

    // Write to SAM
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back from SAM
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;

    ctx.register_table("test_sam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, chrom, start, flags, cigar, mapping_quality, sequence, quality_scores FROM test_sam ORDER BY name")
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

    // Verify chromosomes
    let chroms = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(chroms.value(0), "chr1");
    assert_eq!(chroms.value(1), "chr1");
    assert_eq!(chroms.value(2), "chr2");

    // Verify start positions (0-based)
    let starts = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(starts.value(0), 100);
    assert_eq!(starts.value(1), 200);
    assert_eq!(starts.value(2), 300);

    // Verify flags
    let flags = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(flags.value(0), 0);
    assert_eq!(flags.value(1), 16);
    assert_eq!(flags.value(2), 0);

    // Verify sequences
    let seqs = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(seqs.value(0), "ACGTACGTAC");
    assert_eq!(seqs.value(1), "ACGTACGTAC");
    assert_eq!(seqs.value(2), "TTTTTTTTTT");

    // Verify quality scores
    let quals = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(quals.value(0), "IIIIIIIIII");
    assert_eq!(quals.value(1), "IIIIIIIIII");
    assert_eq!(quals.value(2), "IIIIIIIIII");

    Ok(())
}

/// Write test data with tags to SAM, read back and verify tag values.
#[tokio::test]
async fn test_sam_tags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_tags.sam");

    let schema = create_test_schema(&[
        ("NM", DataType::Int32, "i", "Edit distance"),
        ("MD", DataType::Utf8, "Z", "Mismatch positions"),
        ("AS", DataType::Int32, "i", "Alignment score"),
    ]);

    let batch = datafusion::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2", "read3"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 200, 300])),
            Arc::new(UInt32Array::from(vec![0, 16, 0])),
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
            Arc::new(Int32Array::from(vec![150, -150, 0])),
            Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(0)])),
            Arc::new(StringArray::from(vec![
                Some("10"),
                Some("5^A4"),
                Some("10"),
            ])),
            Arc::new(Int32Array::from(vec![Some(50), Some(45), Some(60)])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let tag_fields = vec!["NM".to_string(), "MD".to_string(), "AS".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back from SAM with tags
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields),
        false,
    )
    .await?;

    ctx.register_table("test_sam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, \"NM\", \"MD\", \"AS\" FROM test_sam ORDER BY name")
        .await?;

    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    // Verify NM tag
    let nm = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(nm.value(0), 2);
    assert_eq!(nm.value(1), 1);
    assert_eq!(nm.value(2), 0);

    // Verify MD tag
    let md = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(md.value(0), "10");
    assert_eq!(md.value(1), "5^A4");
    assert_eq!(md.value(2), "10");

    // Verify AS tag
    let as_tag = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(as_tag.value(0), 50);
    assert_eq!(as_tag.value(1), 45);
    assert_eq!(as_tag.value(2), 60);

    Ok(())
}

/// Write SAM with tags, then use try_new_with_inferred_schema to verify
/// that tag types are correctly discovered from the file.
#[tokio::test]
async fn test_sam_schema_inference() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_infer.sam");

    let schema = create_test_schema(&[
        ("NM", DataType::Int32, "i", "Edit distance"),
        ("MD", DataType::Utf8, "Z", "Mismatch positions"),
    ]);

    let batch = datafusion::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 16])),
            Arc::new(StringArray::from(vec!["10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "ACGTACGTAC"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![100, -100])),
            Arc::new(Int32Array::from(vec![Some(2), Some(1)])),
            Arc::new(StringArray::from(vec![Some("10"), Some("5^A4")])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let tag_fields = vec!["NM".to_string(), "MD".to_string()];
    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        Some(tag_fields.clone()),
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Use inferred schema to read back
    let read_provider = BamTableProvider::try_new_with_inferred_schema(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        Some(tag_fields),
        Some(10),
        false,
    )
    .await?;

    // Verify schema has correct types
    let inferred_schema = read_provider.schema();
    let nm_field = inferred_schema
        .fields()
        .iter()
        .find(|f| f.name() == "NM")
        .expect("NM field should be present");
    assert_eq!(nm_field.data_type(), &DataType::Int32);

    let md_field = inferred_schema
        .fields()
        .iter()
        .find(|f| f.name() == "MD")
        .expect("MD field should be present");
    assert_eq!(md_field.data_type(), &DataType::Utf8);

    // Verify data reads correctly
    ctx.register_table("test_sam", Arc::new(read_provider))?;
    let df = ctx
        .sql("SELECT name, \"NM\", \"MD\" FROM test_sam ORDER BY name")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    Ok(())
}

/// Write data as BAM, read it, then write as SAM, read SAM, and verify consistency.
#[tokio::test]
async fn test_bam_to_sam_conversion() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let bam_path = temp_dir.path().join("intermediate.bam");
    let sam_path = temp_dir.path().join("converted.sam");

    let schema = create_test_schema(&[]);
    let batch = create_basic_test_batch(schema.clone());

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    // Step 1: Write to BAM
    let write_bam = BamTableProvider::new_for_write(
        bam_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_bam", Arc::new(write_bam))?;

    ctx.sql("INSERT OVERWRITE output_bam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Step 2: Read BAM
    let read_bam = BamTableProvider::new(
        bam_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    let bam_read_schema = read_bam.schema();
    ctx.register_table("bam_data", Arc::new(read_bam))?;

    // Step 3: Write BAM data to SAM (use the read schema which includes `end` column)
    let write_sam = BamTableProvider::new_for_write(
        sam_path.to_str().unwrap().to_string(),
        bam_read_schema,
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_sam))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM bam_data")
        .await?
        .collect()
        .await?;

    // Step 4: Read SAM and verify
    let read_sam = BamTableProvider::new(
        sam_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    ctx.register_table("sam_data", Arc::new(read_sam))?;

    let df = ctx
        .sql("SELECT name, chrom, start, sequence FROM sam_data ORDER BY name")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "read1");
    assert_eq!(names.value(1), "read2");
    assert_eq!(names.value(2), "read3");

    let seqs = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(seqs.value(0), "ACGTACGTAC");
    assert_eq!(seqs.value(2), "TTTTTTTTTT");

    Ok(())
}

/// Verify is_sam_file() correctly identifies SAM files from various paths.
#[test]
fn test_sam_format_detection() {
    // Positive cases
    assert!(is_sam_file("data/alignments.sam"));
    assert!(is_sam_file("/path/to/file.SAM"));
    assert!(is_sam_file("file.Sam"));
    assert!(is_sam_file("/tmp/test.sam"));

    // Negative cases
    assert!(!is_sam_file("data/alignments.bam"));
    assert!(!is_sam_file("data/file.cram"));
    assert!(!is_sam_file("data/file.sam.bai"));
    assert!(!is_sam_file("data/file.txt"));
    assert!(!is_sam_file("data/samfile.bam"));
}

/// Verify that MAPQ=255 is preserved through SAM round-trip (not converted to null).
#[tokio::test]
async fn test_mapq_255_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_mapq255.sam");

    let schema = create_test_schema(&[]);
    let batch = datafusion::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2", "read3"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 200, 300])),
            Arc::new(UInt32Array::from(vec![0, 16, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M", "10M"])),
            Arc::new(UInt32Array::from(vec![255, 60, 0])), // MAPQ 255, 60, 0
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
            Arc::new(Int32Array::from(vec![0, 0, 0])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    ctx.register_table("test_sam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, mapping_quality FROM test_sam ORDER BY name")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let mapq = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    // MAPQ 255 must be preserved, not null
    assert_eq!(mapq.value(0), 255); // read1
    assert_eq!(mapq.value(1), 60); // read2
    assert_eq!(mapq.value(2), 0); // read3
    assert!(!mapq.is_null(0));
    assert!(!mapq.is_null(1));
    assert!(!mapq.is_null(2));

    Ok(())
}

/// Verify that QNAME "*" is preserved through SAM round-trip (not converted to null).
#[tokio::test]
async fn test_name_star_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_star_name.sam");

    let schema = create_test_schema(&[]);
    let batch = datafusion::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "*"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![100, 200])),
            Arc::new(UInt32Array::from(vec![0, 4])), // second is unmapped
            Arc::new(StringArray::from(vec!["10M", "*"])),
            Arc::new(UInt32Array::from(vec![60, 0])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(UInt32Array::from(vec![None::<u32>, None])),
            Arc::new(StringArray::from(vec!["ACGTACGTAC", "NNNNNNNNNN"])),
            Arc::new(StringArray::from(vec!["IIIIIIIIII", "IIIIIIIIII"])),
            Arc::new(Int32Array::from(vec![0, 0])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    ctx.register_table("test_sam", Arc::new(read_provider))?;

    let df = ctx.sql("SELECT name FROM test_sam ORDER BY name").await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // "*" name must be preserved as a string, not null
    assert!(!names.is_null(0));
    assert!(!names.is_null(1));
    // One of them should be "*"
    let values: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    assert!(values.contains(&"*"));
    assert!(values.contains(&"read1"));

    Ok(())
}

/// Verify that template_length (TLEN) is preserved through SAM round-trip.
#[tokio::test]
async fn test_template_length_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_tlen.sam");

    let schema = create_test_schema(&[]);
    let batch = datafusion::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["read1", "read2", "read3"])),
            Arc::new(StringArray::from(vec!["chr1", "chr1", "chr2"])),
            Arc::new(UInt32Array::from(vec![100, 200, 300])),
            Arc::new(UInt32Array::from(vec![0, 16, 0])),
            Arc::new(StringArray::from(vec!["10M", "10M", "10M"])),
            Arc::new(UInt32Array::from(vec![60, 60, 60])),
            Arc::new(StringArray::from(vec![Some("chr1"), Some("chr1"), None])),
            Arc::new(UInt32Array::from(vec![Some(250u32), Some(50u32), None])),
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
            Arc::new(Int32Array::from(vec![160, -160, 0])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        false,
    )
    .await?;
    ctx.register_table("test_sam", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT name, template_length FROM test_sam ORDER BY name")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let tlen = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(tlen.value(0), 160); // read1
    assert_eq!(tlen.value(1), -160); // read2
    assert_eq!(tlen.value(2), 0); // read3

    Ok(())
}

/// Write a SAM file with string CIGAR, read back with binary_cigar=true,
/// verify the column is BinaryArray and decodes back to the original ops.
#[tokio::test]
async fn test_binary_cigar_read_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::arrow::array::BinaryArray;
    use datafusion_bio_format_core::alignment_utils::decode_binary_cigar_to_ops;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test_binary_cigar.sam");

    let schema = create_test_schema(&[]);
    let batch = create_basic_test_batch(schema.clone());

    // Write to SAM (string CIGAR)
    let ctx = SessionContext::new();
    ctx.register_batch("input_data", batch)?;

    let write_provider = BamTableProvider::new_for_write(
        output_path.to_str().unwrap().to_string(),
        schema.clone(),
        None,
        true,
        false,
    );
    ctx.register_table("output_sam", Arc::new(write_provider))?;

    ctx.sql("INSERT OVERWRITE output_sam SELECT * FROM input_data")
        .await?
        .collect()
        .await?;

    // Read back with binary_cigar=true
    let read_provider = BamTableProvider::new(
        output_path.to_str().unwrap().to_string(),
        None,
        true,
        None,
        true, // binary_cigar
    )
    .await?;

    // Verify schema has Binary type for cigar column
    let read_schema = read_provider.schema();
    let cigar_field = read_schema.field_with_name("cigar").unwrap();
    assert_eq!(cigar_field.data_type(), &DataType::Binary);

    ctx.register_table("test_binary", Arc::new(read_provider))?;

    let df = ctx
        .sql("SELECT cigar FROM test_binary ORDER BY name")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1);

    let batch = &results[0];
    let cigar_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("cigar column should be BinaryArray");

    // All 3 reads have "10M" CIGAR — decode and verify
    for i in 0..3 {
        let ops = decode_binary_cigar_to_ops(cigar_col.value(i)).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(
            ops[0].kind(),
            noodles_sam::alignment::record::cigar::op::Kind::Match
        );
        assert_eq!(ops[0].len(), 10);
    }

    Ok(())
}
