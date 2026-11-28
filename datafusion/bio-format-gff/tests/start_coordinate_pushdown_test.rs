use datafusion::arrow::compute::concat_batches;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;

async fn assert_start_pushdown_matches_post_scan(
    path: &str,
    zero_based: bool,
    predicate: Expr,
    expected_rows: usize,
) -> Result<()> {
    let _ = env_logger::try_init();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let provider = GffTableProvider::new(
        path.to_string(),
        Some(vec!["ID".to_string()]),
        Some(Default::default()),
        zero_based,
    )?;
    ctx.register_table("gff", Arc::new(provider))?;
    let scan = ctx.table("gff").await?;
    let schema = Arc::new(scan.schema().as_arrow().clone());

    // Materialize before applying the predicate so it cannot reach the GFF reader.
    let materialized = ctx.read_batches(scan.clone().collect().await?)?;
    let expected = materialized
        .filter(predicate.clone())?
        .sort(vec![col("\"ID\"").sort(true, true)])?
        .collect()
        .await?;
    let actual = scan
        .filter(predicate.clone())?
        .sort(vec![col("\"ID\"").sort(true, true)])?
        .collect()
        .await?;

    let expected = concat_batches(&schema, &expected)?;
    let actual = concat_batches(&schema, &actual)?;
    assert_eq!(expected.num_rows(), expected_rows, "post-scan control");
    assert_eq!(
        actual.num_rows(),
        expected.num_rows(),
        "{path}: {predicate} with zero_based={zero_based}"
    );
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn local_start_pushdown_uses_output_coordinates() -> Result<()> {
    let path = "tests/fixtures/gencode-v38-slice.gff3";
    assert_start_pushdown_matches_post_scan(path, false, col("start").lt_eq(lit(11869u32)), 3)
        .await?;
    assert_start_pushdown_matches_post_scan(path, true, col("start").lt_eq(lit(11868u32)), 3)
        .await?;
    assert_start_pushdown_matches_post_scan(
        path,
        true,
        col("start").between(lit(11867u32), lit(11868u32)),
        3,
    )
    .await
}

#[tokio::test]
async fn indexed_start_pushdown_uses_output_coordinates() -> Result<()> {
    let path = "tests/multi_chrom.gff3.gz";
    let predicate = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").lt_eq(lit(999u32)));
    assert_start_pushdown_matches_post_scan(path, false, predicate.clone(), 0).await?;
    assert_start_pushdown_matches_post_scan(path, true, predicate, 3).await
}

#[tokio::test]
async fn start_in_list_pushdown_uses_output_coordinates() -> Result<()> {
    let predicate = col("start").in_list(
        vec![lit(999u32), lit(1049u32), lit(1749u32), lit(1799u32)],
        false,
    );
    assert_start_pushdown_matches_post_scan("tests/multi_chrom.gff3.gz", true, predicate, 6).await
}
