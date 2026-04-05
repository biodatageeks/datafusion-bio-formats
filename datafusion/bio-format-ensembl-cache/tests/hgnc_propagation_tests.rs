//! Tests for VEP `merge_features()` HGNC_ID propagation in the cache builder.
//!
//! The cache builder replicates VEP's symbol→HGNC propagation: if any transcript
//! sharing a `gene_symbol` has a non-null `gene_hgnc_id`, that value is propagated
//! to all other transcripts with the same symbol.
//!
//! See: biodatageeks/datafusion-bio-functions#105

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Build a minimal transcript-like table and run the HGNC propagation query.
///
/// Returns the result rows as `(stable_id, gene_symbol, gene_hgnc_id)`.
async fn run_propagation(
    rows: Vec<(&str, Option<&str>, Option<&str>)>, // (stable_id, gene_symbol, gene_hgnc_id)
) -> Vec<(String, Option<String>, Option<String>)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("stable_id", DataType::Utf8, false),
        Field::new("gene_symbol", DataType::Utf8, true),
        Field::new("gene_hgnc_id", DataType::Utf8, true),
    ]));

    let stable_ids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let gene_symbols: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let gene_hgnc_ids: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(stable_ids)),
            Arc::new(StringArray::from(gene_symbols)),
            Arc::new(StringArray::from(gene_hgnc_ids)),
        ],
    )
    .unwrap();

    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(mem_table)).unwrap();

    // Same propagation expression used by storable_to_parquet build_dedup_query():
    let query = "\
        SELECT \"stable_id\", \"gene_symbol\", \
               COALESCE(gene_hgnc_id, \
                    CASE WHEN gene_symbol IS NOT NULL \
                         THEN FIRST_VALUE(gene_hgnc_id) IGNORE NULLS \
                              OVER (PARTITION BY gene_symbol \
                                             ORDER BY gene_hgnc_id NULLS LAST) \
                         ELSE NULL END) AS gene_hgnc_id \
        FROM tx \
        ORDER BY stable_id";

    let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();

    let mut results = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let symbols = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hgncs = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            results.push((
                ids.value(i).to_string(),
                if symbols.is_null(i) {
                    None
                } else {
                    Some(symbols.value(i).to_string())
                },
                if hgncs.is_null(i) {
                    None
                } else {
                    Some(hgncs.value(i).to_string())
                },
            ));
        }
    }
    results
}

/// Helper: find a row by stable_id.
fn find_row<'a>(
    results: &'a [(String, Option<String>, Option<String>)],
    stable_id: &str,
) -> &'a (String, Option<String>, Option<String>) {
    results
        .iter()
        .find(|r| r.0 == stable_id)
        .unwrap_or_else(|| panic!("stable_id '{stable_id}' not found in results"))
}

// -----------------------------------------------------------------------
// Test cases
// -----------------------------------------------------------------------

/// Basic propagation: one transcript has HGNC_ID, two siblings with NULL get filled.
#[tokio::test]
async fn propagation_fills_nulls() {
    let results = run_propagation(vec![
        ("ENST001", Some("FGF7P3"), Some("HGNC:26671")),
        ("ENST002", Some("FGF7P3"), None),
        ("ENST003", Some("FGF7P3"), None),
    ])
    .await;

    assert_eq!(results.len(), 3);
    for row in &results {
        assert_eq!(
            row.2.as_deref(),
            Some("HGNC:26671"),
            "transcript {} should have HGNC:26671, got {:?}",
            row.0,
            row.2
        );
    }
}

/// COALESCE preserves original: transcripts that already have HGNC_ID keep it.
#[tokio::test]
async fn no_overwrite_existing_hgnc() {
    let results = run_propagation(vec![
        ("ENST001", Some("BRCA1"), Some("HGNC:1100")),
        ("ENST002", Some("BRCA1"), Some("HGNC:1100")),
    ])
    .await;

    assert_eq!(results.len(), 2);
    for row in &results {
        assert_eq!(row.2.as_deref(), Some("HGNC:1100"));
    }
}

/// NULL gene_symbol: transcripts with NULL symbol must NOT get HGNC_ID propagated.
#[tokio::test]
async fn null_gene_symbol_stays_null() {
    let results = run_propagation(vec![
        ("ENST001", Some("ABC"), Some("HGNC:999")),
        ("ENST002", None, None), // NULL symbol — should stay NULL
    ])
    .await;

    assert_eq!(results.len(), 2);
    assert_eq!(find_row(&results, "ENST001").2.as_deref(), Some("HGNC:999"));
    assert_eq!(
        find_row(&results, "ENST002").2,
        None,
        "NULL-symbol transcript should not get HGNC_ID"
    );
}

/// All transcripts for a symbol have NULL HGNC_ID: nothing changes.
#[tokio::test]
async fn all_null_hgnc_stays_null() {
    let results = run_propagation(vec![
        ("ENST001", Some("UNKNOWN"), None),
        ("ENST002", Some("UNKNOWN"), None),
        ("ENST003", Some("UNKNOWN"), None),
    ])
    .await;

    assert_eq!(results.len(), 3);
    for row in &results {
        assert_eq!(row.2, None, "transcript {} should remain NULL", row.0);
    }
}

/// Multiple gene symbols: propagation is independent per symbol.
#[tokio::test]
async fn multiple_symbols_independent() {
    let results = run_propagation(vec![
        // Symbol A: one has HGNC, one doesn't
        ("ENST001", Some("GENE_A"), Some("HGNC:100")),
        ("ENST002", Some("GENE_A"), None),
        // Symbol B: one has HGNC, one doesn't
        ("ENST003", Some("GENE_B"), Some("HGNC:200")),
        ("ENST004", Some("GENE_B"), None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(find_row(&results, "ENST001").2.as_deref(), Some("HGNC:100"));
    assert_eq!(find_row(&results, "ENST002").2.as_deref(), Some("HGNC:100"));
    assert_eq!(find_row(&results, "ENST003").2.as_deref(), Some("HGNC:200"));
    assert_eq!(find_row(&results, "ENST004").2.as_deref(), Some("HGNC:200"));
}

/// Mixed: gene with HGNC propagation + gene without any HGNC + NULL-symbol row.
#[tokio::test]
async fn mixed_scenario() {
    let results = run_propagation(vec![
        // FGF7P3-like: one HGNC source, one EntrezGene without
        ("ENST001", Some("FGF7P3"), Some("HGNC:26671")),
        ("ENST002", Some("FGF7P3"), None),
        // Gene with no HGNC at all
        ("ENST003", Some("NOVELGENE"), None),
        // NULL symbol
        ("ENST004", None, None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(
        find_row(&results, "ENST001").2.as_deref(),
        Some("HGNC:26671")
    );
    assert_eq!(
        find_row(&results, "ENST002").2.as_deref(),
        Some("HGNC:26671"),
        "FGF7P3 sibling should get propagated HGNC"
    );
    assert_eq!(
        find_row(&results, "ENST003").2,
        None,
        "gene with no HGNC should stay NULL"
    );
    assert_eq!(
        find_row(&results, "ENST004").2,
        None,
        "NULL-symbol should stay NULL"
    );
}
