use datafusion::arrow::array::{Array, StringArray, UInt32Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use flate2::{Compression, write::GzEncoder};
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

fn fixture(dir: &TempDir, content: &[u8], compression: &str) -> String {
    let path = dir.path().join(format!("records.bed{compression}"));
    let bytes = match compression {
        ".gz" => {
            let mut writer = GzEncoder::new(Vec::new(), Compression::default());
            writer.write_all(content).unwrap();
            writer.finish().unwrap()
        }
        ".bgz" => {
            let mut writer = noodles_bgzf::io::Writer::new(Vec::new());
            writer.write_all(content).unwrap();
            writer.finish().unwrap()
        }
        "" => content.to_vec(),
        _ => unreachable!(),
    };
    std::fs::write(&path, bytes).unwrap();
    path.to_str().unwrap().to_owned()
}

async fn query(
    path: String,
    fields: BEDFields,
    sql: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    ctx.register_table(
        "bed",
        Arc::new(BedTableProvider::new(path, fields, None, true)?),
    )?;
    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn bed3_is_read_with_nullable_name_across_compression_and_line_endings() {
    for compression in ["", ".gz", ".bgz"] {
        for ending in ["\n", "\r\n", ""] {
            let dir = TempDir::new().unwrap();
            let content = format!("chr1\t0\t5\nchr1\t4\t8\nchr1\t21\t29{ending}");
            let batches = query(
                fixture(&dir, content.as_bytes(), compression),
                BEDFields::BED4,
                "SELECT * FROM bed",
            )
            .await
            .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                3,
                "{compression:?} {ending:?}"
            );
            let starts: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(starts, [0, 4, 21]);
            for batch in batches {
                assert_eq!(
                    batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .null_count(),
                    batch.num_rows()
                );
            }
        }
    }
}

#[tokio::test]
async fn short_records_raise_instead_of_disappearing() {
    for compression in ["", ".gz", ".bgz"] {
        for content in ["chr1\t0\n", "chr1\t0\t5\tr1\nchr1\t4\nchr1\t21\t29\tr3\n"] {
            let dir = TempDir::new().unwrap();
            let result = query(
                fixture(&dir, content.as_bytes(), compression),
                BEDFields::BED4,
                "SELECT * FROM bed",
            )
            .await;
            assert!(
                result.is_err(),
                "silently accepted {compression:?} {content:?}"
            );
        }
    }
}

#[tokio::test]
async fn all_input_widths_and_output_modes_preserve_values_and_metadata() {
    use datafusion::arrow::array::UInt16Array;
    use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
    let row = [
        "chr1", "0", "5", "feature", "1000", "-", "0", "5", "0", "1", "5", "0",
    ];
    for compression in ["", ".gz", ".bgz"] {
        for ending in ["\n", "\r\n", ""] {
            for width in 3..=12 {
                for (mode, count) in [
                    (BEDFields::BED3, 3),
                    (BEDFields::BED4, 4),
                    (BEDFields::BED5, 5),
                    (BEDFields::BED6, 6),
                ] {
                    let dir = TempDir::new().unwrap();
                    let content = format!("{}{ending}", row[..width].join("\t"));
                    let batches = query(
                        fixture(&dir, content.as_bytes(), compression),
                        mode,
                        "SELECT * FROM bed",
                    )
                    .await
                    .unwrap();
                    assert_eq!(batches.len(), 1, "{compression} {width} {mode:?}");
                    let batch = &batches[0];
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(batch.num_columns(), count);
                    assert_eq!(
                        batch
                            .schema()
                            .metadata()
                            .get(COORDINATE_SYSTEM_METADATA_KEY)
                            .map(String::as_str),
                        Some("true")
                    );
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "chr1"
                    );
                    assert_eq!(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                    assert_eq!(
                        batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .value(0),
                        5
                    );
                    if count >= 4 {
                        let names = batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert_eq!(names.is_null(0), width < 4);
                        if width >= 4 {
                            assert_eq!(names.value(0), "feature");
                        }
                    }
                    if count >= 5 {
                        let scores = batch
                            .column(4)
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap();
                        assert_eq!(scores.is_null(0), width < 5);
                        if width >= 5 {
                            assert_eq!(scores.value(0), 1000);
                        }
                    }
                    if count >= 6 {
                        let strands = batch
                            .column(5)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert_eq!(strands.is_null(0), width < 6);
                        if width >= 6 {
                            assert_eq!(strands.value(0), "-");
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn missing_dot_and_empty_names_remain_distinct_across_batches() {
    for compression in ["", ".gz", ".bgz"] {
        let dir = TempDir::new().unwrap();
        let content =
            b"chr1\t0\t5\nchr1\t5\t8\t.\nchr1\t8\t9\t\nchr1\t9\t10\tfeature with spaces\n";
        let batches = query(
            fixture(&dir, content, compression),
            BEDFields::BED4,
            "SELECT name FROM bed",
        )
        .await
        .unwrap();
        let names: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|name| name.map(str::to_owned))
            })
            .collect();
        assert_eq!(
            names,
            [
                None,
                None,
                Some(String::new()),
                Some("feature with spaces".into())
            ]
        );
    }
}

#[tokio::test]
async fn comments_blank_lines_and_directives_do_not_stop_reading() {
    for compression in ["", ".gz", ".bgz"] {
        let dir = TempDir::new().unwrap();
        let content = b"# comment\r\n\ntrack name=example\nbrowser position chr1:1-5\nchr1\t0\t5\n# middle\n \t\ntrack\t5\t8\nbrowser\t8\t9\n# final comment";
        let batches = query(
            fixture(&dir, content, compression),
            BEDFields::BED4,
            "SELECT chrom FROM bed",
        )
        .await
        .unwrap();
        let chroms: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|chrom| chrom.unwrap().to_owned())
            })
            .collect();
        assert_eq!(chroms, ["chr1", "track", "browser"]);
    }
}

#[tokio::test]
async fn empty_and_comment_only_files_have_zero_count() {
    use datafusion::arrow::array::Int64Array;
    for compression in ["", ".gz", ".bgz"] {
        for content in [b"".as_slice(), b"# comment\n\r\ntrack name=empty\n"] {
            let dir = TempDir::new().unwrap();
            let path = fixture(&dir, content, compression);
            let batches = query(path.clone(), BEDFields::BED4, "SELECT * FROM bed")
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
            let count = query(path, BEDFields::BED4, "SELECT COUNT(*) FROM bed")
                .await
                .unwrap();
            assert_eq!(
                count[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
    }
}

#[tokio::test]
async fn malformed_records_fail_at_every_position_and_under_projection() {
    let invalid = [
        b"chr1".as_slice(),
        b"chr1\t0",
        b"chr1\t0\t",
        b"\t0\t5",
        b"chr1\t\t5",
        b"chr1\t-1\t5",
        b"chr1\t+1\t5",
        b"chr1\t0\tbad",
        b"chr1\t1.0\t5",
        b"chr1\t0\t4294967296",
        b"chr1\t4294967296\t4294967296",
        b"chr1\t5\t4",
        b"chr1\t0\t5\t\xff",
        b"chr1 0 5",
    ];
    for compression in ["", ".gz", ".bgz"] {
        for bad in invalid {
            for position in 0..3 {
                let mut lines = [
                    b"chr1\t0\t5".to_vec(),
                    b"chr1\t5\t8".to_vec(),
                    b"chr1\t8\t9".to_vec(),
                ];
                lines[position] = bad.to_vec();
                // No final newline ensures EOF cannot bypass validation.
                let content = lines.join(&b'\n');
                let dir = TempDir::new().unwrap();
                let path = fixture(&dir, &content, compression);
                for sql in [
                    "SELECT * FROM bed",
                    "SELECT chrom FROM bed",
                    "SELECT COUNT(*) FROM bed",
                ] {
                    let error = query(path.clone(), BEDFields::BED4, sql)
                        .await
                        .unwrap_err()
                        .to_string();
                    assert!(
                        error.contains(&format!("BED line {}", position + 1)),
                        "{compression} {sql}: {error}"
                    );
                    assert!(error.contains("records.bed"), "{error}");
                }
            }
        }
    }
}

#[tokio::test]
async fn validates_scores_and_strands_even_when_not_projected() {
    for compression in ["", ".gz", ".bgz"] {
        for (mode, values) in [
            (
                BEDFields::BED5,
                vec![
                    "chr1\t0\t5\tname\t1001",
                    "chr1\t0\t5\tname\t-1",
                    "chr1\t0\t5\tname\tbad",
                    "chr1\t0\t5\tname\t",
                ],
            ),
            (
                BEDFields::BED6,
                vec!["chr1\t0\t5\tname\t0\t?", "chr1\t0\t5\tname\t0\t"],
            ),
        ] {
            for content in values {
                let dir = TempDir::new().unwrap();
                let path = fixture(&dir, content.as_bytes(), compression);
                for sql in ["SELECT * FROM bed", "SELECT COUNT(*) FROM bed"] {
                    assert!(
                        query(path.clone(), mode, sql).await.is_err(),
                        "{compression} {content}"
                    );
                }
            }
        }
        let dir = TempDir::new().unwrap();
        let path = fixture(
            &dir,
            b"chr1\t0\t5\t.\t.\t.\nchr1\t5\t8\tn\t0\t+",
            compression,
        );
        let batches = query(path, BEDFields::BED6, "SELECT * FROM bed")
            .await
            .unwrap();
        for column in 3..6 {
            assert!(batches[0].column(column).is_null(0));
        }
        assert_eq!(
            batches[0]
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "+"
        );
    }
}

#[tokio::test]
async fn coordinates_handle_empty_intervals_and_uint32_boundaries() {
    for compression in ["", ".gz", ".bgz"] {
        for zero_based in [true, false] {
            let dir = TempDir::new().unwrap();
            let path = fixture(
                &dir,
                b"chr1\t0\t0\nchr1\t5\t5\nchr1\t4294967294\t4294967295",
                compression,
            );
            let provider = BedTableProvider::new(path, BEDFields::BED4, None, zero_based).unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("bed", Arc::new(provider)).unwrap();
            let batches = ctx
                .sql("SELECT start, \"end\" FROM bed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let starts = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let ends = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let offset = u32::from(!zero_based);
            assert_eq!(
                starts.values().as_ref(),
                [offset, 5 + offset, 4294967294 + offset]
            );
            assert_eq!(ends.values().as_ref(), [0, 5, 4294967295]);
        }
        let dir = TempDir::new().unwrap();
        let path = fixture(&dir, b"chr1\t4294967295\t4294967295", compression);
        let batches = query(path.clone(), BEDFields::BED4, "SELECT start FROM bed")
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            u32::MAX
        );
        let ctx = SessionContext::new();
        ctx.register_table(
            "bed",
            Arc::new(BedTableProvider::new(path, BEDFields::BED4, None, false).unwrap()),
        )
        .unwrap();
        let error = ctx
            .sql("SELECT * FROM bed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("coordinate conversion"), "{error}");
    }
}

#[tokio::test]
async fn count_projection_filters_and_limits_keep_correct_rows() {
    use datafusion::arrow::array::Int64Array;
    for compression in ["", ".gz", ".bgz"] {
        let dir = TempDir::new().unwrap();
        let path = fixture(
            &dir,
            b"chr1\t0\t5\ta\nchr2\t4\t8\t.\nchr1\t21\t29\tc",
            compression,
        );
        for (sql, expected) in [
            ("SELECT COUNT(*) FROM bed", 3),
            ("SELECT COUNT(name) FROM bed", 2),
            ("SELECT COUNT(*) FROM bed WHERE start >= 4", 2),
        ] {
            let batches = query(path.clone(), BEDFields::BED4, sql).await.unwrap();
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected,
                "{sql}"
            );
        }
        let batches = query(
            path.clone(),
            BEDFields::BED4,
            "SELECT name, \"end\", chrom FROM bed WHERE chrom = 'chr1' ORDER BY start DESC LIMIT 1",
        )
        .await
        .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "c"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            29
        );
        let batches = query(
            path.clone(),
            BEDFields::BED4,
            "SELECT name AS a, name AS b FROM bed LIMIT 2",
        )
        .await
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in batches {
            assert_eq!(batch.column(0), batch.column(1));
        }
        let batches = query(path, BEDFields::BED4, "SELECT * FROM bed LIMIT 0")
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}

#[tokio::test]
async fn physical_projection_preserves_metadata_and_empty_projection_row_counts() {
    use datafusion::catalog::TableProvider;
    use datafusion::physical_plan::collect;
    use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
    let dir = TempDir::new().unwrap();
    let path = fixture(&dir, b"chr1\t0\t5\ta\nchr1\t5\t8\tb\nchr1\t8\t9\tc", "");
    let provider = BedTableProvider::new(path, BEDFields::BED4, None, false).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    for projection in [vec![], vec![3, 0, 3], vec![2, 1]] {
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.schema().fields().len(), projection.len());
        assert_eq!(
            plan.schema()
                .metadata()
                .get(COORDINATE_SYSTEM_METADATA_KEY)
                .map(String::as_str),
            Some("false")
        );
        assert!(plan.execute(1, ctx.task_ctx()).is_err());
        assert!(plan.clone().with_new_children(vec![plan.clone()]).is_err());
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        for batch in batches {
            assert_eq!(batch.num_columns(), projection.len());
        }
    }
    assert!(
        provider
            .scan(&ctx.state(), Some(&vec![4]), &[], None)
            .await
            .is_err()
    );
    let plan = provider
        .scan(&ctx.state(), None, &[], Some(1))
        .await
        .unwrap();
    assert_eq!(
        collect(plan, ctx.task_ctx())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn file_uris_explicit_compression_and_misleading_suffixes() {
    use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
    for (suffix, compression) in [
        ("", CompressionType::NONE),
        (".gz", CompressionType::GZIP),
        (".bgz", CompressionType::BGZF),
    ] {
        let dir = TempDir::new().unwrap();
        let path = fixture(&dir, b"chr1\t0\t5\n", suffix);
        let renamed = dir.path().join("wrong-suffix.data");
        std::fs::rename(path, &renamed).unwrap();
        for path in [
            renamed.to_str().unwrap().to_owned(),
            format!("file://{}", renamed.display()),
        ] {
            for explicit in [None, Some(compression.clone())] {
                let options = ObjectStorageOptions {
                    compression_type: explicit,
                    ..Default::default()
                };
                let ctx = SessionContext::new();
                ctx.register_table(
                    "bed",
                    Arc::new(
                        BedTableProvider::new(path.clone(), BEDFields::BED4, Some(options), true)
                            .unwrap(),
                    ),
                )
                .unwrap();
                let batches = ctx
                    .sql("SELECT * FROM bed")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(batches[0].num_rows(), 1);
            }
        }
    }
    // The override must actually be honored (gzip bytes cannot be plain BED).
    let dir = TempDir::new().unwrap();
    let path = fixture(&dir, b"chr1\t0\t5\n", ".gz");
    let ctx = SessionContext::new();
    let options = ObjectStorageOptions {
        compression_type: Some(CompressionType::NONE),
        ..Default::default()
    };
    ctx.register_table(
        "bed",
        Arc::new(BedTableProvider::new(path, BEDFields::BED4, Some(options), true).unwrap()),
    )
    .unwrap();
    assert!(
        ctx.sql("SELECT * FROM bed")
            .await
            .unwrap()
            .collect()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_files_and_corrupted_compressed_streams_raise() {
    let dir = TempDir::new().unwrap();
    assert!(
        query(
            dir.path().join("absent.bed").to_str().unwrap().to_owned(),
            BEDFields::BED4,
            "SELECT * FROM bed"
        )
        .await
        .is_err()
    );
    for suffix in [".gz", ".bgz"] {
        let path = fixture(&dir, b"chr1\t0\t5\nchr1\t5\t8\n", suffix);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        assert!(
            query(path, BEDFields::BED4, "SELECT * FROM bed")
                .await
                .is_err(),
            "{suffix}"
        );
    }
}

#[tokio::test]
async fn bed3_records_split_across_gzip_members_and_bgzf_blocks() {
    let dir = TempDir::new().unwrap();
    let content = b"chr1\t0\t5\nchr1\t5\t8\nchr1\t8\t9";
    let path = dir.path().join("split.bed.gz");
    let mut compressed = Vec::new();
    for chunk in content.chunks(3) {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(chunk).unwrap();
        compressed.extend(encoder.finish().unwrap());
    }
    std::fs::write(&path, compressed).unwrap();
    let batches = query(
        path.to_str().unwrap().to_owned(),
        BEDFields::BED4,
        "SELECT * FROM bed",
    )
    .await
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    // A name longer than a BGZF block must remain intact.
    let name = "x".repeat(150_000);
    let path = fixture(
        &dir,
        format!("chr1\t0\t5\t{name}\nchr1\t5\t8\n").as_bytes(),
        ".bgz",
    );
    let batches = query(path, BEDFields::BED4, "SELECT * FROM bed")
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        name
    );
    assert!(batches[0].column(3).is_null(1));
}

#[tokio::test]
async fn leading_zero_coordinates_are_valid_including_empty_intervals() {
    for compression in ["", ".gz", ".bgz"] {
        let dir = TempDir::new().unwrap();
        let path = fixture(&dir, b"chr1\t000\t000\nchr1\t001\t005\n", compression);
        let batches = query(path, BEDFields::BED4, "SELECT start, \"end\" FROM bed")
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .as_ref(),
            [0, 1]
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .as_ref(),
            [0, 5]
        );
    }
}

#[tokio::test]
async fn generated_records_match_reference_across_batch_sizes() {
    // Deterministic generated fixtures vary widths, names and coordinate spans.
    // Expected arrays are constructed from the source data, independently of BED parsing.
    let mut state = 456_u32;
    let mut content = String::new();
    let mut expected_starts = Vec::new();
    let mut expected_ends = Vec::new();
    let mut expected_names = Vec::new();
    for index in 0..65 {
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        let start = state % 1_000_000;
        let end = start + state % 200;
        let name = (index % 3 == 0).then(|| format!("feature-{index}-α"));
        content.push_str(&format!("chr{}\t{start}\t{end}", index % 4));
        if let Some(name) = &name {
            content.push_str(&format!("\t{name}"));
        }
        content.push_str(if index % 2 == 0 { "\r\n" } else { "\n" });
        expected_starts.push(start);
        expected_ends.push(end);
        expected_names.push(name);
    }
    for compression in ["", ".gz", ".bgz"] {
        let dir = TempDir::new().unwrap();
        let path = fixture(&dir, content.as_bytes(), compression);
        for batch_size in [1, 2, 3, 7, 64, 128] {
            for zero_based in [true, false] {
                let ctx = SessionContext::new_with_config(
                    SessionConfig::new().with_batch_size(batch_size),
                );
                ctx.register_table(
                    "bed",
                    Arc::new(
                        BedTableProvider::new(path.clone(), BEDFields::BED4, None, zero_based)
                            .unwrap(),
                    ),
                )
                .unwrap();
                let batches = ctx
                    .sql("SELECT * FROM bed")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert!(batches.iter().all(|batch| batch.num_rows() <= batch_size));
                let starts: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect();
                let ends: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect();
                let names: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .map(|value| value.map(str::to_owned))
                    })
                    .collect();
                assert_eq!(
                    starts,
                    expected_starts
                        .iter()
                        .map(|start| start + u32::from(!zero_based))
                        .collect::<Vec<_>>()
                );
                assert_eq!(ends, expected_ends);
                assert_eq!(names, expected_names);
            }
        }
    }
}
