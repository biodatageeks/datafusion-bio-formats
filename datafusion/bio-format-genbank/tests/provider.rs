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

#[tokio::test]
async fn gzip_records_preserve_residue_units_headers_and_taxonomy() {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;
    use flate2::{Compression, write::GzEncoder};

    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed.gb.gz");
    let mut gzip = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    gzip.write_all(RECORD.as_bytes()).unwrap();
    gzip.write_all(b"LOCUS       P 4 aa linear 01-JAN-2020\n  ORGANISM  Example organism\n            First group;\n            Second group.\nORIGIN\n        1 mkwv\n//\n").unwrap();
    gzip.finish().unwrap();

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let provider =
        GenBankTableProvider::new(path.display().to_string(), None, "records", None, true).unwrap();
    let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    let column = |name| batch.column_by_name(name).unwrap();
    assert_eq!(column("name").as_string::<i32>().value(1), "P");
    assert_eq!(column("length").as_primitive::<Int64Type>().value(1), 4);
    let molecule = column("molecule_type").as_string::<i32>();
    assert_eq!((molecule.value(0), molecule.value(1)), ("DNA", "protein"));
    assert_eq!(column("sequence").as_string::<i64>().value(1), "MKWV");
    assert_eq!(
        column("organism").as_string::<i32>().value(1),
        "Example organism"
    );
    let taxonomy = column("taxonomy").as_list::<i32>();
    assert!(taxonomy.is_null(0));
    let groups = taxonomy.value(1);
    assert_eq!(
        groups.as_string::<i32>().iter().collect::<Vec<_>>(),
        [Some("First group"), Some("Second group")]
    );
}

#[tokio::test]
async fn protein_features_preserve_closed_coordinates_and_promote_first_qualifier() {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;

    let dir = tempdir().unwrap();
    let path = dir.path().join("protein.gb");
    std::fs::write(&path, "LOCUS       P 4 aa\nFEATURES             Location/Qualifiers\n     Region          2..4\n                     /note=\"first\"\n                     /note=\"second\"\n     Site            2^3\n//\n").unwrap();
    let provider = GenBankTableProvider::new(
        path.display().to_string(),
        None,
        "features",
        Some(&["note".into()]),
        false,
    )
    .unwrap();
    let ctx = SessionContext::new();
    let batches = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let column = |name| batch.column_by_name(name).unwrap();
    assert_eq!(
        column("note").as_string::<i32>().iter().collect::<Vec<_>>(),
        [Some("first"), None]
    );
    assert_eq!(
        column("start")
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(2), None]
    );
    assert_eq!(
        column("end")
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(4), None]
    );
    assert_eq!(column("location").as_string::<i32>().value(1), "2^3");
    let site = column("parts").as_list::<i32>().value(1);
    assert_eq!(site.len(), 1);
    for name in ["start", "end"] {
        assert!(site.as_struct().column_by_name(name).unwrap().is_null(0));
    }
}

#[tokio::test]
async fn http_without_head_falls_back_to_one_stream() {
    use datafusion::arrow::array::AsArray;
    use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/example.gb", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for method in ["HEAD", "GET"] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(&mut stream).lines();
            assert_eq!(
                lines.next_line().await.unwrap().unwrap(),
                format!("{method} /example.gb HTTP/1.1")
            );
            while let Some(header) = lines.next_line().await.unwrap() {
                if header.is_empty() {
                    break;
                }
                assert!(!header.to_ascii_lowercase().starts_with("range:"));
            }
            let (status, body) = if method == "HEAD" {
                ("405 Method Not Allowed", "")
            } else {
                ("200 OK", RECORD)
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let opts = ObjectStorageOptions {
        compression_type: Some(CompressionType::NONE),
        max_retries: Some(0),
        timeout: Some(5),
        ..Default::default()
    };
    let provider = GenBankTableProvider::new(url, Some(opts), "records", None, true).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column_by_name("name")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "A"
    );
}
