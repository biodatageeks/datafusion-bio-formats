use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_pairs::storage::IndexedPairsReader;
use datafusion_bio_format_pairs::table_provider::PairsTableProvider;
use std::sync::Arc;

fn test_file(name: &str) -> String {
    format!("{}/tests/{}", env!("CARGO_MANIFEST_DIR"), name)
}

async fn create_context(file: &str) -> SessionContext {
    create_context_with_coord_system(file, true).await
}

async fn create_context_with_coord_system(
    file: &str,
    coordinate_system_zero_based: bool,
) -> SessionContext {
    let ctx = SessionContext::new();
    let table =
        PairsTableProvider::new(file.to_string(), None, coordinate_system_zero_based).unwrap();
    ctx.register_table("pairs", Arc::new(table)).unwrap();
    ctx
}

// ============================================================
// Plain text tests (test_small.pairs — 10 rows, no index)
// ============================================================

#[tokio::test]
async fn test_select_all() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx.sql("SELECT * FROM pairs").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);
}

#[tokio::test]
async fn test_select_limit() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx.sql("SELECT * FROM pairs LIMIT 3").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_projection_pushdown() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT chr1, pos1, chr2, pos2 FROM pairs")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 4);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);
}

#[tokio::test]
async fn test_filter_chr1() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT * FROM pairs WHERE chr1 = 'chr1'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1 rows: read1, read2, read3, read7, read8 = 5
    assert_eq!(total_rows, 5);
}

#[tokio::test]
async fn test_filter_chr2() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT * FROM pairs WHERE chr2 = 'chrX'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr2='chrX' rows: read3, read5, read6, read9 = 4
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_filter_chr1_and_chr2() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT * FROM pairs WHERE chr1 = 'chr1' AND chr2 = 'chr2'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1='chr1' AND chr2='chr2' rows: read2, read7 = 2
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_filter_pos1_range() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT \"readID\", chr1, pos1 FROM pairs WHERE pos1 >= 3000 AND pos1 <= 6000")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // pos1 in [3000,6000]: read3(3000), read5(4000), read6(5000), read7(6000) = 4
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_filter_chr1_in_list() {
    let ctx = create_context(&test_file("test_small.pairs")).await;
    let df = ctx
        .sql("SELECT \"readID\" FROM pairs WHERE chr1 IN ('chr1', 'chrX')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1='chr1': read1,read2,read3,read7,read8 = 5
    // chr1='chrX': read6,read10 = 2
    // total = 7
    assert_eq!(total_rows, 7);
}

#[tokio::test]
async fn test_schema_has_metadata() {
    let table = PairsTableProvider::new(test_file("test_small.pairs"), None, true).unwrap();
    let schema = table.schema();
    let metadata = schema.metadata();

    assert_eq!(
        metadata.get("bio.coordinate_system_zero_based"),
        Some(&"true".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.format_version"),
        Some(&"1.0".to_string())
    );
    assert!(metadata.contains_key("bio.pairs.columns"));
    assert!(metadata.contains_key("bio.pairs.chromsizes"));
    assert_eq!(
        metadata.get("bio.pairs.sorted"),
        Some(&"chr1-chr2-pos1-pos2".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.shape"),
        Some(&"upper triangle".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.genome_assembly"),
        Some(&"hg38".to_string())
    );
}

#[tokio::test]
async fn test_schema_columns() {
    let table = PairsTableProvider::new(test_file("test_small.pairs"), None, true).unwrap();
    let schema = table.schema();

    assert_eq!(schema.fields().len(), 7);
    assert_eq!(schema.field(0).name(), "readID");
    assert_eq!(schema.field(1).name(), "chr1");
    assert_eq!(schema.field(2).name(), "pos1");
    assert_eq!(schema.field(3).name(), "chr2");
    assert_eq!(schema.field(4).name(), "pos2");
    assert_eq!(schema.field(5).name(), "strand1");
    assert_eq!(schema.field(6).name(), "strand2");
}

// ============================================================
// BGZF + tabix indexed tests (test_spec.pairs.gz — 30 rows)
// Data distribution:
//   chr1: 10 rows (pos1: 10000..300000)
//   chr2:  8 rows (pos1: 5000..250000)
//   chr3:  6 rows (pos1: 10000..200000)
//   chrX:  6 rows (pos1: 15000..250000)
// ============================================================

#[tokio::test]
async fn test_indexed_select_all() {
    // Pairs spec uses 1-based coordinates, so use coordinate_system_zero_based=false
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx.sql("SELECT chr1, pos1 FROM pairs").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 30);
}

#[tokio::test]
async fn test_indexed_provider_has_index() {
    let table = PairsTableProvider::new(test_file("test_spec.pairs.gz"), None, false).unwrap();
    let schema = table.schema();
    // Should detect the .tbi index and have 4 contigs
    assert_eq!(schema.fields().len(), 7);

    let metadata = schema.metadata();
    assert_eq!(
        metadata.get("bio.pairs.format_version"),
        Some(&"1.0".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.genome_assembly"),
        Some(&"hg38".to_string())
    );
    // 4 chromsizes (chr1, chr2, chr3, chrX)
    assert!(metadata.contains_key("bio.pairs.chromsizes"));
}

#[tokio::test]
async fn test_indexed_reader_contig_names() {
    let reader = IndexedPairsReader::new(
        &test_file("test_spec.pairs.gz"),
        &test_file("test_spec.pairs.gz.tbi"),
    )
    .unwrap();
    let contigs = reader.contig_names();
    assert_eq!(contigs.len(), 4);
    assert!(contigs.contains(&"chr1".to_string()));
    assert!(contigs.contains(&"chr2".to_string()));
    assert!(contigs.contains(&"chr3".to_string()));
    assert!(contigs.contains(&"chrX".to_string()));
}

#[tokio::test]
async fn test_indexed_reader_query_chr1() {
    let mut reader = IndexedPairsReader::new(
        &test_file("test_spec.pairs.gz"),
        &test_file("test_spec.pairs.gz.tbi"),
    )
    .unwrap();
    let region: noodles_core::Region = "chr1".parse().unwrap();
    let records: Vec<_> = reader
        .query(&region)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records.len(), 10);
}

#[tokio::test]
async fn test_indexed_reader_query_chr2_range() {
    let mut reader = IndexedPairsReader::new(
        &test_file("test_spec.pairs.gz"),
        &test_file("test_spec.pairs.gz.tbi"),
    )
    .unwrap();
    // Query chr2:1-100000 — should get records with pos1: 5000, 25000, 45000, 75000, 100000 = 5
    let region: noodles_core::Region = "chr2:1-100000".parse().unwrap();
    let records: Vec<_> = reader
        .query(&region)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records.len(), 5);
}

#[tokio::test]
async fn test_indexed_filter_chr1() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chr1'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1: 10 rows
    assert_eq!(total_rows, 10);
}

#[tokio::test]
async fn test_indexed_filter_chr2() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chr2'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr2: 8 rows
    assert_eq!(total_rows, 8);
}

#[tokio::test]
async fn test_indexed_filter_chr3() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chr3'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr3: 6 rows
    assert_eq!(total_rows, 6);
}

#[tokio::test]
async fn test_indexed_filter_chr_x() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chrX'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chrX: 6 rows
    assert_eq!(total_rows, 6);
}

#[tokio::test]
async fn test_indexed_filter_pos1_range() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql(
            "SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chr1' AND pos1 >= 50000 AND pos1 <= 150000",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1 pos1 in [50000, 150000]: 50000, 60000, 80000, 100000, 120000, 150000 = 6
    assert_eq!(total_rows, 6);
}

#[tokio::test]
async fn test_indexed_residual_filter_chr2_column() {
    // chr1 filter uses index, chr2 filter is residual
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, chr2, pos1, pos2 FROM pairs WHERE chr1 = 'chr1' AND chr2 = 'chr1'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1 rows with chr2='chr1': (10000,20000), (50000,70000), (80000,90000), (200000,250000) = 4
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_indexed_residual_filter_chr2_chrx() {
    // chr1 filter uses index, chr2='chrX' is residual
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, chr2, pos1, pos2 FROM pairs WHERE chr1 = 'chr1' AND chr2 = 'chrX'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1 rows with chr2='chrX': (100000,50000), (300000,200000) = 2
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_indexed_residual_filter_pos2_range() {
    // pos1 filter uses index for region, pos2 is residual
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1, pos2 FROM pairs WHERE chr1 = 'chr2' AND pos2 >= 50000 AND pos2 <= 150000")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr2 rows with pos2 in [50000,150000]:
    //   pos1=45000 pos2=55000, pos1=75000 pos2=85000, pos1=100000 pos2=120000 = 3
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_indexed_combined_index_and_residual() {
    // chr1='chr3' (index) AND chr2='chrX' (residual)
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, chr2, pos1, pos2 FROM pairs WHERE chr1 = 'chr3' AND chr2 = 'chrX'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr3 rows with chr2='chrX': (40000,50000), (100000,110000), (200000,250000) = 3
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_indexed_projection_pushdown() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 = 'chrX'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 2);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);
}

#[tokio::test]
async fn test_indexed_chr1_in_list() {
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1 FROM pairs WHERE chr1 IN ('chr1', 'chrX')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1: 10 + chrX: 6 = 16
    assert_eq!(total_rows, 16);
}

#[tokio::test]
async fn test_indexed_verify_data_values() {
    // Verify actual returned values match test data
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1, chr2, pos2, strand1, strand2 FROM pairs WHERE chr1 = 'chrX' ORDER BY pos1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);

    // Collect all rows from all batches
    let mut all_chr1: Vec<String> = Vec::new();
    let mut all_pos1: Vec<u32> = Vec::new();
    let mut all_chr2: Vec<String> = Vec::new();
    let mut all_pos2: Vec<u32> = Vec::new();
    for batch in &batches {
        let chr1_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pos1_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let chr2_arr = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pos2_arr = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            all_chr1.push(chr1_arr.value(i).to_string());
            all_pos1.push(pos1_arr.value(i));
            all_chr2.push(chr2_arr.value(i).to_string());
            all_pos2.push(pos2_arr.value(i));
        }
    }

    // All chr1 should be chrX
    assert!(all_chr1.iter().all(|c| c == "chrX"));
    // pos1 should be sorted
    assert_eq!(all_pos1, vec![15000, 45000, 80000, 120000, 180000, 250000]);
    // chr2 should all be chrX for this subset
    assert!(all_chr2.iter().all(|c| c == "chrX"));
    // pos2 values
    assert_eq!(all_pos2, vec![25000, 65000, 95000, 140000, 200000, 300000]);
}

#[tokio::test]
async fn test_indexed_bgzf_header_metadata() {
    let table = PairsTableProvider::new(test_file("test_spec.pairs.gz"), None, false).unwrap();
    let schema = table.schema();
    let metadata = schema.metadata();

    assert_eq!(
        metadata.get("bio.coordinate_system_zero_based"),
        Some(&"false".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.format_version"),
        Some(&"1.0".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.sorted"),
        Some(&"chr1-pos1-chr2-pos2".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.shape"),
        Some(&"upper triangle".to_string())
    );
    assert_eq!(
        metadata.get("bio.pairs.genome_assembly"),
        Some(&"hg38".to_string())
    );

    // Chromsizes includes chr3 (4 total)
    let chromsizes_json = metadata.get("bio.pairs.chromsizes").unwrap();
    assert!(chromsizes_json.contains("chr3"));
    assert!(chromsizes_json.contains("198022430"));
}

#[tokio::test]
async fn test_indexed_strand_filter() {
    // Residual filter on strand1
    let ctx = create_context_with_coord_system(&test_file("test_spec.pairs.gz"), false).await;
    let df = ctx
        .sql("SELECT chr1, pos1, strand1 FROM pairs WHERE chr1 = 'chr1' AND strand1 = '-'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // chr1 rows with strand1='-': pos1=80000, pos1=120000, pos1=200000 = 3
    assert_eq!(total_rows, 3);
}
