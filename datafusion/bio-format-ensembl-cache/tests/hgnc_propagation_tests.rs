//! Tests for cache-region-local HGNC_ID propagation in the cache builder.
//!
//! The cache builder should only propagate `gene_hgnc_id` within the same
//! VEP-sized cache region for a `(chrom, gene_symbol)` cluster. This preserves
//! local colocated loci such as FGF7P3 while avoiding false positives across
//! distant same-symbol loci such as SNORA75, SNORA72, and LINC03025.

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_bio_format_ensembl_cache::VEP_CACHE_REGION_SIZE_BP;
use std::sync::Arc;
type PropagationInput<'a> = (&'a str, &'a str, i64, Option<&'a str>, Option<&'a str>);
type PropagationResult = (String, Option<String>, Option<String>);

/// Build a minimal transcript-like table and run the HGNC propagation query.
///
/// Returns the result rows as `(stable_id, gene_symbol, gene_hgnc_id)`.
async fn run_propagation(
    rows: Vec<PropagationInput<'_>>, // (stable_id, chrom, start, gene_symbol, gene_hgnc_id)
) -> Vec<PropagationResult> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("stable_id", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("gene_symbol", DataType::Utf8, true),
        Field::new("gene_hgnc_id", DataType::Utf8, true),
    ]));

    let stable_ids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let chroms: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let starts: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let gene_symbols: Vec<Option<&str>> = rows.iter().map(|r| r.3).collect();
    let gene_hgnc_ids: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(stable_ids)),
            Arc::new(StringArray::from(chroms)),
            Arc::new(datafusion::arrow::array::Int64Array::from(starts)),
            Arc::new(StringArray::from(gene_symbols)),
            Arc::new(StringArray::from(gene_hgnc_ids)),
        ],
    )
    .unwrap();

    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(mem_table)).unwrap();

    let region_expr = format!("CAST(FLOOR((start - 1) / {VEP_CACHE_REGION_SIZE_BP}.0) AS BIGINT)");
    let query = format!(
        "SELECT \"stable_id\", \"gene_symbol\", \
                COALESCE(gene_hgnc_id, \
                     CASE WHEN gene_symbol IS NOT NULL \
                          THEN FIRST_VALUE(gene_hgnc_id) IGNORE NULLS \
                               OVER (PARTITION BY chrom, gene_symbol, {region_expr} \
                                      ORDER BY gene_hgnc_id NULLS LAST) \
                          ELSE NULL END) AS gene_hgnc_id \
         FROM tx \
         ORDER BY stable_id"
    );

    let batches = ctx.sql(&query).await.unwrap().collect().await.unwrap();

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
fn find_row<'a>(results: &'a [PropagationResult], stable_id: &str) -> &'a PropagationResult {
    results
        .iter()
        .find(|r| r.0 == stable_id)
        .unwrap_or_else(|| panic!("stable_id '{stable_id}' not found in results"))
}

// -----------------------------------------------------------------------
// Test cases
// -----------------------------------------------------------------------

/// Basic propagation: one transcript has HGNC_ID, two local siblings with NULL get filled.
#[tokio::test]
async fn propagation_fills_nulls_within_region() {
    let results = run_propagation(vec![
        (
            "ENST001",
            "9",
            39_817_530,
            Some("FGF7P3"),
            Some("HGNC:26671"),
        ),
        ("ENST002", "9", 39_816_479, Some("FGF7P3"), None),
        ("ENST003", "9", 39_888_877, Some("FGF7P3"), None),
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
        (
            "ENST001",
            "17",
            43_044_294,
            Some("BRCA1"),
            Some("HGNC:1100"),
        ),
        (
            "ENST002",
            "17",
            43_046_030,
            Some("BRCA1"),
            Some("HGNC:1100"),
        ),
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
        ("ENST001", "1", 100_000, Some("ABC"), Some("HGNC:999")),
        ("ENST002", "1", 120_000, None, None), // NULL symbol — should stay NULL
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
        ("ENST001", "1", 100_000, Some("UNKNOWN"), None),
        ("ENST002", "1", 120_000, Some("UNKNOWN"), None),
        ("ENST003", "1", 130_000, Some("UNKNOWN"), None),
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
        ("ENST001", "1", 100_000, Some("GENE_A"), Some("HGNC:100")),
        ("ENST002", "1", 120_000, Some("GENE_A"), None),
        // Symbol B: one has HGNC, one doesn't
        ("ENST003", "1", 200_000, Some("GENE_B"), Some("HGNC:200")),
        ("ENST004", "1", 220_000, Some("GENE_B"), None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(find_row(&results, "ENST001").2.as_deref(), Some("HGNC:100"));
    assert_eq!(find_row(&results, "ENST002").2.as_deref(), Some("HGNC:100"));
    assert_eq!(find_row(&results, "ENST003").2.as_deref(), Some("HGNC:200"));
    assert_eq!(find_row(&results, "ENST004").2.as_deref(), Some("HGNC:200"));
}

/// Distant same-symbol loci must not cross-fill across cache regions.
#[tokio::test]
async fn distant_same_symbol_does_not_propagate() {
    let results = run_propagation(vec![
        (
            "ENST001",
            "9",
            41_100_793,
            Some("LINC03025"),
            Some("HGNC:56158"),
        ),
        ("ENST002", "9", 68_305_989, Some("LINC03025"), None),
    ])
    .await;

    assert_eq!(results.len(), 2);
    assert_eq!(
        find_row(&results, "ENST001").2.as_deref(),
        Some("HGNC:56158")
    );
    assert_eq!(
        find_row(&results, "ENST002").2.as_deref(),
        None,
        "distant same-symbol locus should not inherit HGNC"
    );
}

/// Same-symbol rows on different chromosomes must not cross-fill.
#[tokio::test]
async fn same_symbol_different_chroms_do_not_propagate() {
    let results = run_propagation(vec![
        (
            "ENST001",
            "2",
            231_455_800,
            Some("SNORA75"),
            Some("HGNC:32661"),
        ),
        ("ENST002", "12", 9_286_673, Some("SNORA75"), None),
    ])
    .await;

    assert_eq!(results.len(), 2);
    assert_eq!(
        find_row(&results, "ENST001").2.as_deref(),
        Some("HGNC:32661")
    );
    assert_eq!(
        find_row(&results, "ENST002").2,
        None,
        "same symbol on another chromosome should stay NULL"
    );
}

/// Mixed: local propagation + gene without any HGNC + NULL-symbol row.
#[tokio::test]
async fn mixed_scenario() {
    let results = run_propagation(vec![
        // FGF7P3-like local locus: one HGNC source, one colocated EntrezGene row.
        (
            "ENST001",
            "9",
            39_817_530,
            Some("FGF7P3"),
            Some("HGNC:26671"),
        ),
        ("ENST002", "9", 39_816_479, Some("FGF7P3"), None),
        // Gene with no HGNC at all
        ("ENST003", "9", 50_000_000, Some("NOVELGENE"), None),
        // NULL symbol
        ("ENST004", "9", 60_000_000, None, None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(
        find_row(&results, "ENST002").2.as_deref(),
        Some("HGNC:26671"),
        "local sibling should get propagated HGNC"
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
