use datafusion::arrow::array::Array;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{
    Between, Expr, TableProviderFilterPushDown, col, expr::InList, lit,
};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;
use tokio::fs;

const SAMPLE_GFF_CONTENT: &str = r#"##gff-version 3
chr1	ensembl	gene	1000	2000	.	+	.	ID=gene1;Name=GENE1;Type=protein_coding
chr1	ensembl	exon	1200	1800	100.5	+	0	ID=exon1;Parent=gene1
chr2	ensembl	gene	3000	4000	200.0	-	1	ID=gene2;Name=GENE2;Type=lncRNA
chr3	refseq	CDS	5000	5500	.	+	2	ID=cds1;Product=hypothetical
chrX	havana	gene	10000	15000	50.0	-	.	ID=geneX;Biotype=pseudogene
"#;

async fn create_test_gff_file() -> std::io::Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_file = format!("/tmp/test_filter_pushdown_{}.gff3", nanos);
    fs::write(&temp_file, SAMPLE_GFF_CONTENT).await?;
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
async fn test_supports_filters_pushdown_string_equals() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test string column equality filter
    let filter_expr = col("chrom").eq(lit("chr1"));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    Ok(())
}

#[tokio::test]
async fn test_supports_filters_pushdown_numeric_range() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test numeric range filter
    let filter_expr = col("start").gt(lit(1500u32));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    Ok(())
}

#[tokio::test]
async fn test_supports_filters_pushdown_between() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test BETWEEN filter
    let filter_expr = Expr::Between(Between {
        expr: Box::new(col("start")),
        negated: false,
        low: Box::new(lit(1000u32)),
        high: Box::new(lit(5000u32)),
    });
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    Ok(())
}

#[tokio::test]
async fn test_supports_filters_pushdown_in_list() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test IN list filter
    let filter_expr = Expr::InList(InList {
        expr: Box::new(col("chrom")),
        list: vec![lit("chr1"), lit("chr2")],
        negated: false,
    });
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    Ok(())
}

#[tokio::test]
async fn test_supports_filters_pushdown_unsupported() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test unsupported filter (function call)
    let filter_expr = Expr::IsNull(Box::new(col("chrom")));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Unsupported
    ));

    Ok(())
}

#[tokio::test]
async fn test_supports_filters_pushdown_and_expression() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    // Test AND expression with two pushable filters
    let filter_expr = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").gt(lit(1000u32)));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_string_equality() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering by chromosome
    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM test_gff WHERE chrom = 'chr1'")
        .await?;
    let results = df.collect().await?;

    assert!(!results.is_empty(), "Should have results for chr1 filter");

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    // All results should have chrom = 'chr1'
    let chrom_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(
            chrom_array.value(i),
            "chr1",
            "All results should be from chr1"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_numeric_range() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering by start position
    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM test_gff WHERE start > 2000")
        .await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results for start > 2000 filter"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    // All results should have start > 2000
    let start_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(
            start_array.value(i) > 2000,
            "All results should have start > 2000"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_between() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering with BETWEEN
    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM test_gff WHERE start BETWEEN 1000 AND 4000")
        .await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results for BETWEEN filter"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    // All results should have start between 1000 and 4000
    let start_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        let start_val = start_array.value(i);
        assert!(
            (1000..=4000).contains(&start_val),
            "All results should have start between 1000 and 4000, got {}",
            start_val
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_in_list() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering with IN list
    let df = ctx
        .sql("SELECT chrom, start, type FROM test_gff WHERE chrom IN ('chr1', 'chr2')")
        .await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results for IN list filter"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    // All results should have chrom in ('chr1', 'chr2')
    let chrom_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        let chrom_val = chrom_array.value(i);
        assert!(
            chrom_val == "chr1" || chrom_val == "chr2",
            "All results should be from chr1 or chr2, got {}",
            chrom_val
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_multiple_conditions() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering with multiple AND conditions
    let df = ctx.sql("SELECT chrom, start, type, source FROM test_gff WHERE chrom = 'chr1' AND type = 'gene' AND source = 'ensembl'").await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results for multiple AND conditions"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 4);

    // All results should match all conditions
    let chrom_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let type_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let source_array = batch
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    for i in 0..batch.num_rows() {
        assert_eq!(chrom_array.value(i), "chr1");
        assert_eq!(type_array.value(i), "gene");
        assert_eq!(source_array.value(i), "ensembl");
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_score_field() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering by score (float field)
    let df = ctx
        .sql("SELECT chrom, score FROM test_gff WHERE score > 75.0")
        .await?;
    let results = df.collect().await?;

    assert!(!results.is_empty(), "Should have results for score filter");

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 2);

    // All results should have score > 75.0
    let score_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Float32Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        if !score_array.is_null(i) {
            let score_val = score_array.value(i);
            assert!(
                score_val > 75.0,
                "All results should have score > 75.0, got {}",
                score_val
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_strand_field() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering by strand
    let df = ctx
        .sql("SELECT chrom, strand FROM test_gff WHERE strand = '+'")
        .await?;
    let results = df.collect().await?;

    assert!(!results.is_empty(), "Should have results for strand filter");

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 2);

    // All results should have strand = '+'
    let strand_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(strand_array.value(i), "+");
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_no_results() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering that should return no results
    let df = ctx
        .sql("SELECT chrom, start FROM test_gff WHERE chrom = 'chr999'")
        .await?;
    let results = df.collect().await?;

    // Should have empty result or single empty batch
    if !results.is_empty() {
        let batch = &results[0];
        assert_eq!(
            batch.num_rows(),
            0,
            "Should have 0 rows for non-existent chromosome"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_with_projection_optimization()
-> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table =
        GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test that filters work correctly with column projection
    let df = ctx
        .sql("SELECT start FROM test_gff WHERE chrom = 'chr2' AND type = 'gene'")
        .await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results for filtered projection"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 1); // Only start column projected

    // Should have the expected start value for chr2 gene
    let start_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    assert!(batch.num_rows() > 0);
    // With 0-based coordinates (default), chr2 gene starts at 2999 (was 3000 in 1-based)
    assert_eq!(start_array.value(0), 2999);

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_attribute_fields() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    // Test with specific attribute fields requested
    let table = GffTableProvider::new(
        file_path,
        Some(vec!["ID".to_string(), "Name".to_string()]), // Request specific attributes
        Some(1),
        Some(object_storage_options),
        true, // Use 0-based coordinates (default)
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_gff", Arc::new(table))?;

    // Test filtering with attribute field (if supported in schema)
    // Note: This test may need adjustment based on how attribute filtering is implemented
    let df = ctx
        .sql(r#"SELECT chrom, start, "ID" FROM test_gff WHERE chrom = 'chr1'"#)
        .await?;
    let results = df.collect().await?;

    assert!(
        !results.is_empty(),
        "Should have results with attribute fields"
    );

    let batch = &results[0];
    assert_eq!(batch.num_columns(), 3);

    // Verify chromosome filtering still works with attribute projection
    let chrom_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert_eq!(chrom_array.value(i), "chr1");
    }

    Ok(())
}
