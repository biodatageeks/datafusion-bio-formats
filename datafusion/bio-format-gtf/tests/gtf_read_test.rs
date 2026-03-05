use datafusion::arrow::array::Array;
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_gtf::table_provider::GtfTableProvider;
use std::sync::Arc;

fn test_gtf_path() -> String {
    format!("{}/tests/test.gtf", env!("CARGO_MANIFEST_DIR"))
}

async fn setup_ctx(attr_fields: Option<Vec<String>>, zero_based: bool) -> SessionContext {
    let table = GtfTableProvider::new(test_gtf_path(), attr_fields, zero_based).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("gtf", Arc::new(table)).unwrap();
    ctx
}

fn total_rows(batches: &[datafusion::arrow::array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─── Basic reading ─────────────────────────────────────────────────

#[tokio::test]
async fn test_gtf_select_all() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT * FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);
    // 8 core fields + 1 attributes column
    assert_eq!(results[0].num_columns(), 9);
}

#[tokio::test]
async fn test_gtf_select_limit() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT * FROM gtf LIMIT 5").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 5);
}

#[tokio::test]
async fn test_gtf_row_count() {
    let ctx = setup_ctx(Some(vec!["gene_id".to_string()]), true).await;
    let df = ctx.sql("SELECT gene_id FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);
}

// ─── Schema ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_gtf_schema_columns() {
    let table = GtfTableProvider::new(test_gtf_path(), None, true).unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 9);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "chrom",
            "start",
            "end",
            "type",
            "source",
            "score",
            "strand",
            "phase",
            "attributes"
        ]
    );
}

#[tokio::test]
async fn test_gtf_schema_metadata() {
    let table = GtfTableProvider::new(test_gtf_path(), None, true).unwrap();
    let schema = table.schema();
    assert!(
        schema
            .metadata()
            .contains_key("bio.coordinate_system_zero_based")
    );
    assert_eq!(
        schema
            .metadata()
            .get("bio.coordinate_system_zero_based")
            .unwrap(),
        "true"
    );
}

// ─── Projection pushdown ──────────────────────────────────────────

#[tokio::test]
async fn test_gtf_projection_single_column() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT chrom FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_columns(), 1);
    assert_eq!(total_rows(&results), 23);
}

#[tokio::test]
async fn test_gtf_projection_position_columns() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom, start, \"end\" FROM gtf")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_columns(), 3);
}

#[tokio::test]
async fn test_gtf_projection_all_columns() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT * FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_columns(), 9);
}

// ─── Filter pushdown ──────────────────────────────────────────────

#[tokio::test]
async fn test_gtf_filter_chrom() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE chrom = 'chr12'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);
}

#[tokio::test]
async fn test_gtf_filter_type_exon() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE type = 'exon'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 9);
}

#[tokio::test]
async fn test_gtf_filter_type_cds() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE type = 'CDS'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 8);
}

#[tokio::test]
async fn test_gtf_filter_type_transcript() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE type = 'transcript'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 1);
}

#[tokio::test]
async fn test_gtf_filter_start_range() {
    let ctx = setup_ctx(None, true).await;
    // 0-based: 6536000-1 = 6535999 to 6537000-1 = 6536999
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE start >= 6535999 AND start <= 6536999")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    // Records with 1-based start in [6536000, 6537000]: exon/CDS at 6536494, 6536684, 6536920
    // That's 6 records (exon+CDS for each position)
    assert!(total_rows(&results) > 0);
}

#[tokio::test]
async fn test_gtf_filter_strand() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE strand = '+'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);
}

#[tokio::test]
async fn test_gtf_filter_combined() {
    let ctx = setup_ctx(None, true).await;
    // 0-based: start > 6537000 means 1-based start > 6537001
    // exon/CDS rows with 1-based start > 6537001: 6537101 (exon+CDS), 6537309 (exon+CDS), 6537584 (exon+CDS), 6538101 (exon+CDS)
    // That's 8 exon rows with start > 6537000 in 0-based: 6537100, 6537308, 6537583, 6538100
    // Actually check: exons at 6537101->6537100, 6537309->6537308, 6537584->6537583, 6538101->6538100
    // All > 6537000, so 4 exon rows
    let df = ctx
        .sql("SELECT chrom FROM gtf WHERE type = 'exon' AND start > 6537000")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 4);
}

// ─── GTF attribute parsing ────────────────────────────────────────

#[tokio::test]
async fn test_gtf_attribute_projection_gene_id() {
    let ctx = setup_ctx(Some(vec!["gene_id".to_string()]), true).await;
    let df = ctx.sql("SELECT gene_id FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);

    // All rows should have gene_id = "ENSG00000111640.16"
    for batch in &results {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(arr.value(i), "ENSG00000111640.16");
        }
    }
}

#[tokio::test]
async fn test_gtf_attribute_projection_transcript_id() {
    let ctx = setup_ctx(Some(vec!["transcript_id".to_string()]), true).await;
    let df = ctx.sql("SELECT transcript_id FROM gtf").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 23);

    for batch in &results {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(arr.value(i), "ENST00000920777.1");
        }
    }
}

#[tokio::test]
async fn test_gtf_attribute_projection_multiple() {
    let ctx = setup_ctx(
        Some(vec![
            "gene_id".to_string(),
            "gene_name".to_string(),
            "gene_type".to_string(),
        ]),
        true,
    )
    .await;
    let df = ctx
        .sql("SELECT gene_id, gene_name, gene_type FROM gtf LIMIT 1")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_columns(), 3);

    let batch = &results[0];
    let gene_id = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let gene_name = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let gene_type = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(gene_id.value(0), "ENSG00000111640.16");
    assert_eq!(gene_name.value(0), "GAPDH");
    assert_eq!(gene_type.value(0), "protein_coding");
}

#[tokio::test]
async fn test_gtf_attribute_projection_exon_number() {
    let ctx = setup_ctx(Some(vec!["exon_number".to_string()]), true).await;
    // transcript row should have NULL exon_number
    let df = ctx
        .sql("SELECT exon_number FROM gtf WHERE type = 'transcript'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 1);
    let batch = &results[0];
    let exon_num = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert!(exon_num.is_null(0));
}

#[tokio::test]
async fn test_gtf_attribute_null_handling() {
    let ctx = setup_ctx(Some(vec!["exon_id".to_string()]), true).await;
    // transcript, start_codon, stop_codon don't have exon_id
    let df = ctx
        .sql("SELECT exon_id FROM gtf WHERE type = 'transcript'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(total_rows(&results), 1);
    let batch = &results[0];
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert!(arr.is_null(0));
}

#[tokio::test]
async fn test_gtf_attribute_unquoted_value() {
    let ctx = setup_ctx(Some(vec!["level".to_string()]), true).await;
    let df = ctx.sql("SELECT level FROM gtf LIMIT 1").await.unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let level = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    // level is unquoted "2" in GTF
    assert_eq!(level.value(0), "2");
}

#[tokio::test]
async fn test_gtf_attribute_duplicate_keys() {
    // GTF allows duplicate keys (e.g., multiple "tag" entries)
    // In unnested mode, we keep the first value
    let ctx = setup_ctx(Some(vec!["tag".to_string()]), true).await;
    let df = ctx.sql("SELECT tag FROM gtf LIMIT 1").await.unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let tag = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    // First tag value should be "basic"
    assert_eq!(tag.value(0), "basic");
}

#[tokio::test]
async fn test_gtf_nested_attributes() {
    // Default mode should return nested List<Struct> attributes
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT attributes FROM gtf LIMIT 1").await.unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    // attributes column should be a list
    let arr = batch.column(0);
    assert!(
        arr.data_type().to_string().contains("Struct"),
        "attributes should be List<Struct>"
    );
}

// ─── Coordinates ──────────────────────────────────────────────────

#[tokio::test]
async fn test_gtf_zero_based_coordinates() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx.sql("SELECT start FROM gtf LIMIT 1").await.unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let start = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    // 1-based 6534012 -> 0-based 6534011
    assert_eq!(start.value(0), 6534011);
}

#[tokio::test]
async fn test_gtf_one_based_coordinates() {
    let ctx = setup_ctx(None, false).await;
    let df = ctx.sql("SELECT start FROM gtf LIMIT 1").await.unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let start = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    // 1-based 6534012 stays 6534012
    assert_eq!(start.value(0), 6534012);
}

// ─── Data verification ────────────────────────────────────────────

#[tokio::test]
async fn test_gtf_verify_first_row() {
    let ctx = setup_ctx(
        Some(vec![
            "gene_id".to_string(),
            "transcript_id".to_string(),
            "gene_name".to_string(),
        ]),
        true,
    )
    .await;
    let df = ctx
        .sql("SELECT chrom, start, \"end\", type, source, strand, gene_id, transcript_id, gene_name FROM gtf LIMIT 1")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);

    let chrom = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let start = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    let end = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    let ty = batch
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let source = batch
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let strand_col = batch
        .column(5)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let gene_id = batch
        .column(6)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let transcript_id = batch
        .column(7)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let gene_name = batch
        .column(8)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(chrom.value(0), "chr12");
    assert_eq!(start.value(0), 6534011); // 0-based
    assert_eq!(end.value(0), 6538371);
    assert_eq!(ty.value(0), "transcript");
    assert_eq!(source.value(0), "HAVANA");
    assert_eq!(strand_col.value(0), "+");
    assert_eq!(gene_id.value(0), "ENSG00000111640.16");
    assert_eq!(transcript_id.value(0), "ENST00000920777.1");
    assert_eq!(gene_name.value(0), "GAPDH");
}

#[tokio::test]
async fn test_gtf_verify_feature_type_counts() {
    let ctx = setup_ctx(None, true).await;
    let df = ctx
        .sql("SELECT type, COUNT(*) as cnt FROM gtf GROUP BY type ORDER BY cnt DESC")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let mut type_counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for batch in &results {
        let types = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            type_counts.insert(types.value(i).to_string(), counts.value(i));
        }
    }

    assert_eq!(type_counts.get("exon"), Some(&9));
    assert_eq!(type_counts.get("CDS"), Some(&8));
    assert_eq!(type_counts.get("UTR"), Some(&3));
    assert_eq!(type_counts.get("transcript"), Some(&1));
    assert_eq!(type_counts.get("start_codon"), Some(&1));
    assert_eq!(type_counts.get("stop_codon"), Some(&1));
}

#[tokio::test]
async fn test_gtf_verify_phase_values() {
    let ctx = setup_ctx(None, true).await;

    // CDS records should have phase 0 or 1
    let df = ctx
        .sql("SELECT phase FROM gtf WHERE type = 'CDS'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    for batch in &results {
        let phase = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!phase.is_null(i), "CDS should have phase");
            let val = phase.value(i);
            assert!(val <= 2, "Phase should be 0, 1, or 2, got {val}");
        }
    }

    // UTR records should have NULL phase
    let df = ctx
        .sql("SELECT phase FROM gtf WHERE type = 'UTR'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    for batch in &results {
        let phase = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(phase.is_null(i), "UTR should have NULL phase");
        }
    }
}
