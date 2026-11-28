use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use std::sync::Arc;

fn register_refseq(attr_fields: Option<Vec<String>>) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let provider = GffTableProvider::new(
        "tests/fixtures/refseq-percent-escapes.gff3".to_string(),
        attr_fields,
        Some(Default::default()),
        false,
    )?;
    ctx.register_table("gff", Arc::new(provider))?;
    Ok(ctx)
}

fn refseq_model_evidence(proteins: u32) -> String {
    format!(
        "Supporting evidence includes similarity to: {proteins} Proteins, and 100% coverage of the annotated genomic feature by RNAseq alignments, including 48 samples with support for all annotated introns"
    )
}

fn expected_model_evidence() -> Vec<String> {
    vec![
        refseq_model_evidence(2),
        refseq_model_evidence(6),
        "%2C;=&\t\n\r\u{1}\u{7f}+,".to_string(),
        "x�y%".to_string(),
    ]
}

#[tokio::test]
async fn percent_escapes_decode_once_in_nested_attributes() -> Result<()> {
    let ctx = register_refseq(None)?;
    let batches = ctx
        .sql("SELECT attributes FROM gff")
        .await?
        .collect()
        .await?;
    let mut evidence = Vec::new();
    for batch in batches {
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for index in 0..attributes.len() {
            let row = attributes.value(index);
            let fields = row.as_any().downcast_ref::<StructArray>().unwrap();
            let tags = fields
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = fields
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let field = tags
                .iter()
                .position(|tag| tag == Some("model_evidence"))
                .unwrap();
            evidence.push(values.value(field).to_string());
        }
    }
    assert_eq!(evidence, expected_model_evidence());
    Ok(())
}

#[tokio::test]
async fn percent_escapes_decode_once_in_flattened_attributes() -> Result<()> {
    let ctx = register_refseq(Some(vec!["model_evidence".to_string()]))?;
    let batches = ctx
        .sql("SELECT model_evidence FROM gff")
        .await?
        .collect()
        .await?;
    let evidence: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect();
    let expected = expected_model_evidence();
    assert_eq!(
        evidence,
        expected
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn pushed_attribute_predicate_uses_decoded_value() -> Result<()> {
    let ctx = register_refseq(Some(vec!["model_evidence".to_string()]))?;
    for value in [refseq_model_evidence(2), "x�y%".to_string()] {
        let batches = ctx
            .table("gff")
            .await?
            .filter(col("model_evidence").eq(lit(value.clone())))?
            .collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1,
            "{value}"
        );
    }
    Ok(())
}
