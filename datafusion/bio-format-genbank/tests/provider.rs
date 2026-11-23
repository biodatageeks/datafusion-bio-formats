use datafusion::{catalog::TableProvider, physical_plan::ExecutionPlanProperties, prelude::*};
use datafusion_bio_format_genbank::GenBankTableProvider;
use noodles_bgzf as bgzf;
use std::{io::Write, sync::Arc};
use tempfile::tempdir;

const RECORD: &str = "LOCUS       A 100 bp DNA circular\r\nFEATURES             Location/Qualifiers\r\n     gene            1..5\r\n                     /note=\"line\r\n                     //\"\r\n     CDS             complement(join(2..3,6..8))\r\n                     /translation=\"MA\"\r\nORIGIN\r\n        1 acgta\r\n//\r\n";

#[tokio::test]
async fn split_and_serial_scans_preserve_duplicates_and_counts() {
    let dir = tempdir().unwrap();
    // Every record appears twice. A set comparison would miss a lost duplicate.
    let data = (0..32)
        .map(|i| RECORD.replace("LOCUS       A", &format!("LOCUS       R{}", i / 2)))
        .collect::<String>();
    for compressed in [false, true] {
        let path = dir.path().join(if compressed {
            "multi.gbk.bgz"
        } else {
            "multi.gbk"
        });
        if compressed {
            let mut writer = bgzf::io::Writer::new(std::fs::File::create(&path).unwrap());
            for chunk in data.as_bytes().chunks(101) {
                writer.write_all(chunk).unwrap();
                writer.flush().unwrap();
            }
            writer.finish().unwrap();
        } else {
            std::fs::write(&path, &data).unwrap();
        }
        for (view, expected) in [("features", 64), ("records", 32)] {
            let mut snapshots = vec![];
            for n in [1, 4] {
                let ctx = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_target_partitions(n)
                        .with_batch_size(2),
                );
                let provider = Arc::new(
                    GenBankTableProvider::new(path.display().to_string(), None, view, None, true)
                        .unwrap(),
                );
                let scan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
                assert_eq!(scan.output_partitioning().partition_count(), n);
                ctx.register_table("gb", provider).unwrap();
                let key = if view == "features" {
                    "record_id,type"
                } else {
                    "name"
                };
                let sql = format!("SELECT {key},count(*) FROM gb GROUP BY {key} ORDER BY {key}");
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                snapshots.push(
                    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                        .unwrap()
                        .to_string(),
                );
                let count = ctx
                    .sql("SELECT count(*) FROM gb")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let count = count[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(count, expected);
            }
            assert_eq!(snapshots[0], snapshots[1]);
        }
    }
}

#[tokio::test]
async fn one_locus_yields_multiple_feature_batches_and_exact_filtered_limit() {
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use datafusion::physical_plan::collect;
    let dir = tempdir().unwrap();
    let path = dir.path().join("one.gbk");
    std::fs::write(
        &path,
        format!(
            "LOCUS       A 100 bp DNA\nFEATURES             Location/Qualifiers\n{}//\n",
            "     gene            1..2\n     CDS             5..8\n".repeat(10)
        ),
    )
    .unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(3));
    let provider =
        GenBankTableProvider::new(path.display().to_string(), None, "features", None, true)
            .unwrap();
    let predicate = col("type").eq(lit("CDS"));
    assert_eq!(
        provider.supports_filters_pushdown(&[&predicate]).unwrap(),
        vec![TableProviderFilterPushDown::Exact]
    );
    let plan = provider
        .scan(&ctx.state(), Some(&vec![]), &[predicate], Some(7))
        .await
        .unwrap();
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    assert!(
        batches
            .iter()
            .all(|b| b.num_columns() == 0 && b.num_rows() <= 3)
    );
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
}
