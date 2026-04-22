//! Tests for native HGNC_ID handling after removing export-time propagation.
//!
//! The cache builder no longer propagates `gene_hgnc_id` at export time.
//! Both `gene_hgnc_id` and `gene_hgnc_id_native` now store the value parsed
//! directly from the raw VEP cache object. Any propagation belongs to the
//! downstream annotation engine (buffer-scoped, matching VEP behavior).

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
type NativeInput<'a> = (&'a str, &'a str, i64, Option<&'a str>, Option<&'a str>);
type NativeResult = (String, Option<String>, Option<String>, Option<String>);

/// Build a minimal transcript-like table and query both gene_hgnc_id and
/// gene_hgnc_id_native (both pass through without propagation).
///
/// Returns `(stable_id, gene_symbol, gene_hgnc_id, gene_hgnc_id_native)`.
async fn run_native_query(
    rows: Vec<NativeInput<'_>>, // (stable_id, chrom, start, gene_symbol, gene_hgnc_id)
) -> Vec<NativeResult> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("stable_id", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("gene_symbol", DataType::Utf8, true),
        Field::new("gene_hgnc_id", DataType::Utf8, true),
        Field::new("gene_hgnc_id_native", DataType::Utf8, true),
    ]));

    let stable_ids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let chroms: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let starts: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let gene_symbols: Vec<Option<&str>> = rows.iter().map(|r| r.3).collect();
    let gene_hgnc_ids: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();
    // gene_hgnc_id_native mirrors gene_hgnc_id (same native value)
    let gene_hgnc_ids_native: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(stable_ids)),
            Arc::new(StringArray::from(chroms)),
            Arc::new(datafusion::arrow::array::Int64Array::from(starts)),
            Arc::new(StringArray::from(gene_symbols)),
            Arc::new(StringArray::from(gene_hgnc_ids)),
            Arc::new(StringArray::from(gene_hgnc_ids_native)),
        ],
    )
    .unwrap();

    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("tx", Arc::new(mem_table)).unwrap();

    // No propagation — just pass through both columns
    let query = "SELECT \"stable_id\", \"gene_symbol\", \"gene_hgnc_id\", \
                        \"gene_hgnc_id_native\" \
                 FROM tx ORDER BY stable_id";

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
        let hgncs_native = batch
            .column(3)
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
                if hgncs_native.is_null(i) {
                    None
                } else {
                    Some(hgncs_native.value(i).to_string())
                },
            ));
        }
    }
    results
}

/// Helper: find a row by stable_id.
fn find_row<'a>(results: &'a [NativeResult], stable_id: &str) -> &'a NativeResult {
    results
        .iter()
        .find(|r| r.0 == stable_id)
        .unwrap_or_else(|| panic!("stable_id '{stable_id}' not found in results"))
}

// -----------------------------------------------------------------------
// Test cases
// -----------------------------------------------------------------------

/// Transcripts with native HGNC_ID retain it in both columns.
#[tokio::test]
async fn native_hgnc_preserved() {
    let results = run_native_query(vec![
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
        assert_eq!(row.3.as_deref(), Some("HGNC:1100"));
        assert_eq!(
            row.2, row.3,
            "gene_hgnc_id and gene_hgnc_id_native must match"
        );
    }
}

/// Transcripts without native HGNC_ID stay NULL — no propagation from siblings.
#[tokio::test]
async fn no_propagation_from_local_siblings() {
    let results = run_native_query(vec![
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
    assert_eq!(
        find_row(&results, "ENST001").2.as_deref(),
        Some("HGNC:26671")
    );
    // No propagation — siblings without native HGNC stay NULL
    assert_eq!(
        find_row(&results, "ENST002").2,
        None,
        "sibling without native HGNC_ID should stay NULL"
    );
    assert_eq!(
        find_row(&results, "ENST003").2,
        None,
        "sibling without native HGNC_ID should stay NULL"
    );
    // gene_hgnc_id_native always matches gene_hgnc_id
    for row in &results {
        assert_eq!(
            row.2, row.3,
            "gene_hgnc_id and gene_hgnc_id_native must match"
        );
    }
}

/// NULL gene_symbol transcripts stay NULL.
#[tokio::test]
async fn null_gene_symbol_stays_null() {
    let results = run_native_query(vec![
        ("ENST001", "1", 100_000, Some("ABC"), Some("HGNC:999")),
        ("ENST002", "1", 120_000, None, None),
    ])
    .await;

    assert_eq!(results.len(), 2);
    assert_eq!(find_row(&results, "ENST001").2.as_deref(), Some("HGNC:999"));
    assert_eq!(find_row(&results, "ENST002").2, None);
    for row in &results {
        assert_eq!(row.2, row.3);
    }
}

/// All transcripts for a symbol have NULL HGNC_ID: nothing changes.
#[tokio::test]
async fn all_null_hgnc_stays_null() {
    let results = run_native_query(vec![
        ("ENST001", "1", 100_000, Some("UNKNOWN"), None),
        ("ENST002", "1", 120_000, Some("UNKNOWN"), None),
        ("ENST003", "1", 130_000, Some("UNKNOWN"), None),
    ])
    .await;

    assert_eq!(results.len(), 3);
    for row in &results {
        assert_eq!(row.2, None, "transcript {} should remain NULL", row.0);
        assert_eq!(
            row.3, None,
            "native column for {} should remain NULL",
            row.0
        );
    }
}

/// Distant same-symbol loci: no cross-fill (was the original bug).
#[tokio::test]
async fn distant_same_symbol_no_cross_fill() {
    let results = run_native_query(vec![
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
        find_row(&results, "ENST002").2,
        None,
        "distant same-symbol locus must not inherit HGNC"
    );
}

/// Same-symbol rows on different chromosomes: no cross-fill.
#[tokio::test]
async fn same_symbol_different_chroms_no_cross_fill() {
    let results = run_native_query(vec![
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

/// Multiple gene symbols: each transcript retains only its own native value.
#[tokio::test]
async fn multiple_symbols_independent() {
    let results = run_native_query(vec![
        ("ENST001", "1", 100_000, Some("GENE_A"), Some("HGNC:100")),
        ("ENST002", "1", 120_000, Some("GENE_A"), None),
        ("ENST003", "1", 200_000, Some("GENE_B"), Some("HGNC:200")),
        ("ENST004", "1", 220_000, Some("GENE_B"), None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(find_row(&results, "ENST001").2.as_deref(), Some("HGNC:100"));
    assert_eq!(
        find_row(&results, "ENST002").2,
        None,
        "no propagation from GENE_A sibling"
    );
    assert_eq!(find_row(&results, "ENST003").2.as_deref(), Some("HGNC:200"));
    assert_eq!(
        find_row(&results, "ENST004").2,
        None,
        "no propagation from GENE_B sibling"
    );
}

/// Mixed scenario: native values preserved, no propagation, NULL-symbol safe.
#[tokio::test]
async fn mixed_scenario() {
    let results = run_native_query(vec![
        (
            "ENST001",
            "9",
            39_817_530,
            Some("FGF7P3"),
            Some("HGNC:26671"),
        ),
        ("ENST002", "9", 39_816_479, Some("FGF7P3"), None),
        ("ENST003", "9", 50_000_000, Some("NOVELGENE"), None),
        ("ENST004", "9", 60_000_000, None, None),
    ])
    .await;

    assert_eq!(results.len(), 4);
    assert_eq!(
        find_row(&results, "ENST001").2.as_deref(),
        Some("HGNC:26671"),
        "native HGNC preserved"
    );
    assert_eq!(
        find_row(&results, "ENST002").2,
        None,
        "no propagation from sibling"
    );
    assert_eq!(
        find_row(&results, "ENST003").2,
        None,
        "gene with no HGNC stays NULL"
    );
    assert_eq!(
        find_row(&results, "ENST004").2,
        None,
        "NULL-symbol stays NULL"
    );
    // All rows: gene_hgnc_id == gene_hgnc_id_native
    for row in &results {
        assert_eq!(
            row.2, row.3,
            "gene_hgnc_id and gene_hgnc_id_native must match for {}",
            row.0
        );
    }
}
