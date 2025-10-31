use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_gff::bgzf_parallel_reader::BgzfGffTableProvider;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use tokio::fs;

const SAMPLE_GFF_CONTENT: &str = r#"##gff-version 3
chr1	ensembl	gene	1000	2000	.	+	.	ID=gene1;Name=GENE1;Type=protein_coding
chr1	ensembl	exon	1200	1800	100.5	+	0	ID=exon1;Parent=gene1
chr2	ensembl	gene	3000	4000	200.0	-	1	ID=gene2;Name=GENE2;Type=lncRNA
"#;

async fn create_test_gff_file() -> std::io::Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_file = format!("/tmp/test_both_providers_{}.gff3", nanos);
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
async fn test_main_gff_provider_supports_filter_pushdown() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    let table = GffTableProvider::new(file_path, None, Some(1), Some(object_storage_options))?;

    // Test filter support detection
    let filter_expr = col("chrom").eq(lit("chr1"));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    println!("✅ Main GffTableProvider supports filter pushdown");
    Ok(())
}

#[tokio::test]
async fn test_bgzf_gff_provider_supports_filter_pushdown() -> Result<(), Box<dyn std::error::Error>>
{
    let file_path = create_test_gff_file().await?;

    let table = BgzfGffTableProvider::try_new(file_path, None)
        .map_err(|e| format!("Failed to create BGZF table provider: {}", e))?;

    // Test filter support detection
    let filter_expr = col("chrom").eq(lit("chr1"));
    let filters = vec![&filter_expr];

    let pushdown_result = table.supports_filters_pushdown(&filters)?;
    assert_eq!(pushdown_result.len(), 1);
    assert!(matches!(
        pushdown_result[0],
        TableProviderFilterPushDown::Inexact
    ));

    println!("✅ BGZF GffTableProvider supports filter pushdown");
    Ok(())
}

#[tokio::test]
async fn test_both_providers_unsupported_filter() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    // Main provider
    let main_table = GffTableProvider::new(
        file_path.clone(),
        None,
        Some(1),
        Some(object_storage_options),
    )?;

    // BGZF provider
    let bgzf_table = BgzfGffTableProvider::try_new(file_path, None)
        .map_err(|e| format!("Failed to create BGZF table provider: {}", e))?;

    // Test unsupported filter (function call)
    let filter_expr = datafusion::logical_expr::Expr::IsNull(Box::new(col("chrom")));
    let filters = vec![&filter_expr];

    // Both should return Unsupported
    let main_result = main_table.supports_filters_pushdown(&filters)?;
    let bgzf_result = bgzf_table.supports_filters_pushdown(&filters)?;

    assert_eq!(main_result.len(), 1);
    assert_eq!(bgzf_result.len(), 1);
    assert!(matches!(
        main_result[0],
        TableProviderFilterPushDown::Unsupported
    ));
    assert!(matches!(
        bgzf_result[0],
        TableProviderFilterPushDown::Unsupported
    ));

    println!("✅ Both providers correctly identify unsupported filters");
    Ok(())
}

#[tokio::test]
async fn test_filter_support_consistency() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = create_test_gff_file().await?;
    let object_storage_options = create_object_storage_options();

    // Main provider
    let main_table = GffTableProvider::new(
        file_path.clone(),
        None,
        Some(1),
        Some(object_storage_options),
    )?;

    // BGZF provider
    let bgzf_table = BgzfGffTableProvider::try_new(file_path, None)
        .map_err(|e| format!("Failed to create BGZF table provider: {}", e))?;

    // Test various filter types
    let test_filters = vec![
        (col("chrom").eq(lit("chr1")), "String equality"),
        (col("start").gt(lit(1500u32)), "Numeric comparison"),
        (
            col("start")
                .gt_eq(lit(1000u32))
                .and(col("start").lt_eq(lit(2000u32))),
            "AND expression",
        ),
    ];

    for (filter_expr, description) in test_filters {
        let filters = vec![&filter_expr];

        let main_result = main_table.supports_filters_pushdown(&filters)?;
        let bgzf_result = bgzf_table.supports_filters_pushdown(&filters)?;

        // Both providers should have consistent filter support
        assert_eq!(
            main_result.len(),
            bgzf_result.len(),
            "Filter count mismatch for: {}",
            description
        );

        for (main_support, bgzf_support) in main_result.iter().zip(bgzf_result.iter()) {
            assert_eq!(
                std::mem::discriminant(main_support),
                std::mem::discriminant(bgzf_support),
                "Filter support inconsistency for: {}",
                description
            );
        }

        println!("✅ Consistent support for: {}", description);
    }

    Ok(())
}
