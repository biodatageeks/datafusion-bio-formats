//! Integration tests for the Stockholm provider.
//!
//! Numeric expectations (63/722, 712/230, 63/724) come from `esl-alistat`; the
//! first-row sequence prefixes are the fixture bytes themselves, because the
//! reader is a verbatim passthrough. See polars-bio `tests/data/io/msa/README.md`.

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int64Type, UInt32Type};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_bio_format_msa::{StockholmTableProvider, read_stockholm_annotations};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

fn data(name: &str) -> String {
    format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn ctx_for(path: &str, gs_fields: Option<Vec<String>>, target_partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let ctx = SessionContext::new_with_config(config);
    let provider = StockholmTableProvider::new(path.to_string(), None, gs_fields).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    ctx
}

async fn scan(name: &str, sql: &str) -> Vec<RecordBatch> {
    let ctx = ctx_for(&data(name), None, 1);
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn strings(batches: &[RecordBatch], col: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col).unwrap();
        match arr.data_type() {
            DataType::Utf8 => {
                let a = arr.as_string::<i32>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            }
            DataType::LargeUtf8 => {
                let a = arr.as_string::<i64>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            }
            other => panic!("unexpected type for {col}: {other}"),
        }
    }
    out
}

/// (tag, value) pairs of one row's annotation bag.
type TagList = Vec<(String, Option<String>)>;

/// Decodes a `List<Struct<tag, value>>` column into per-row (tag, value) vectors;
/// `None` for null rows.
fn tag_lists(batches: &[RecordBatch], col: &str) -> Vec<Option<TagList>> {
    let mut out = Vec::new();
    for b in batches {
        let list = b.column_by_name(col).unwrap().as_list::<i32>();
        for i in 0..list.len() {
            if !list.is_valid(i) {
                out.push(None);
                continue;
            }
            let item = list.value(i);
            let st = item.as_struct();
            let tags = st.column_by_name("tag").unwrap().as_string::<i32>();
            let vals = st.column_by_name("value").unwrap().as_string::<i32>();
            out.push(Some(
                (0..st.len())
                    .map(|j| {
                        (
                            tags.value(j).to_string(),
                            vals.is_valid(j).then(|| vals.value(j).to_string()),
                        )
                    })
                    .collect(),
            ));
        }
    }
    out
}

fn annotation_bag_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![
            Field::new("tag", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        true,
    )))
}

#[tokio::test]
async fn pfam_seed_schema_and_row_count() {
    let batches = scan("PF00001.sto", "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 63);
    let schema = batches[0].schema();
    let fields: Vec<(&str, &DataType, bool)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
        .collect();
    let bag = annotation_bag_type();
    assert_eq!(
        fields,
        vec![
            ("alignment_id", &DataType::Utf8, false),
            ("name", &DataType::Utf8, false),
            ("sequence", &DataType::LargeUtf8, false),
            ("gs", &bag, true),
            ("gr", &bag, true),
        ]
    );
}

#[tokio::test]
async fn pfam_seed_rows_carry_id_name_sequence_and_gs() {
    let batches = scan("PF00001.sto", "SELECT * FROM t").await;
    let ids: HashSet<String> = strings(&batches, "alignment_id")
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(ids, HashSet::from(["7tm_1".to_string()]));
    let names = strings(&batches, "name");
    assert_eq!(names[0].as_deref(), Some("NPY1R_HUMAN/57-320"));
    let seqs: Vec<String> = strings(&batches, "sequence")
        .into_iter()
        .flatten()
        .collect();
    assert!(seqs.iter().all(|s| s.len() == 722));
    assert!(
        seqs[0].starts_with("GNLALIIIILK.QKE..MRN.VT..NILIVNLSFSDLLVAI.....MCLPFTF.VYTL..MDHWVF")
    );
    let gs = tag_lists(&batches, "gs");
    assert_eq!(
        gs[0],
        Some(vec![("AC".to_string(), Some("P25929.1".to_string()))])
    );
    // No #=GR lines in the Pfam seed -> null bag.
    assert!(tag_lists(&batches, "gr").iter().all(|g| g.is_none()));
}

#[tokio::test]
async fn interleaved_rfam_seed_is_concatenated_across_blocks() {
    let batches = scan("RF00001.sto", "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 712);
    let names: Vec<String> = strings(&batches, "name").into_iter().flatten().collect();
    assert_eq!(names[0], "X01556.1/3-118");
    assert_eq!(
        names.iter().collect::<HashSet<_>>().len(),
        712,
        "one row per name"
    );
    let seqs: Vec<String> = strings(&batches, "sequence")
        .into_iter()
        .flatten()
        .collect();
    assert!(
        seqs.iter().all(|s| s.len() == 230),
        "two blocks of 200 + 30"
    );
    assert!(seqs[0].starts_with("--CUUGAC-GA-U-C-AU-AGA----GC-G-U-U-G---GA----------A-CC-A"));
    // The second block starts right where the first ended.
    assert!(seqs[0][200..].starts_with("--AGUA----GG-U-CA-UC--G-UCAAGC"));
    let ids: HashSet<String> = strings(&batches, "alignment_id")
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(ids, HashSet::from(["5S_rRNA".to_string()]));
}

#[tokio::test]
async fn hmmalign_output_carries_per_residue_pp_in_gr() {
    let batches = scan("PF00001_hmmalign.sto", "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 63);
    let seqs: Vec<String> = strings(&batches, "sequence")
        .into_iter()
        .flatten()
        .collect();
    assert!(seqs.iter().all(|s| s.len() == 724));
    for (seq, gr) in seqs.iter().zip(tag_lists(&batches, "gr")) {
        let gr = gr.expect("hmmalign writes #=GR PP for every sequence");
        let pp = gr.iter().find(|(t, _)| t == "PP").expect("PP tag");
        assert_eq!(
            pp.1.as_ref().unwrap().len(),
            seq.len(),
            "PP concatenated across blocks"
        );
    }
}

#[tokio::test]
async fn multi_alignment_file_yields_rows_for_every_alignment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str(&std::fs::read_to_string(data("RF00001.sto")).unwrap());
    std::fs::write(&path, text).unwrap();

    let ctx = ctx_for(path.to_str().unwrap(), None, 1);
    let batches = ctx
        .sql(
            "SELECT alignment_id, count(*) AS n FROM t GROUP BY alignment_id ORDER BY alignment_id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut counts = BTreeMap::new();
    for b in &batches {
        let ids = b.column(0).as_string::<i32>();
        let ns = b.column(1).as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            counts.insert(ids.value(i).to_string(), ns.value(i));
        }
    }
    assert_eq!(
        counts,
        BTreeMap::from([("5S_rRNA".into(), 712), ("7tm_1".into(), 63)])
    );
}

#[tokio::test]
async fn multi_alignment_file_is_partitioned_and_rows_are_identical() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str(&std::fs::read_to_string(data("RF00001.sto")).unwrap());
    text.push_str(&std::fs::read_to_string(data("PF00001_hmmalign.sto")).unwrap());
    std::fs::write(&path, text).unwrap();
    let p = path.to_str().unwrap();

    let single = ctx_for(p, None, 1);
    let one = single.sql("SELECT * FROM t").await.unwrap();
    assert_eq!(
        one.clone()
            .create_physical_plan()
            .await
            .unwrap()
            .output_partitioning()
            .partition_count(),
        1
    );
    let one = one.collect().await.unwrap();

    let multi = ctx_for(p, None, 3);
    let many = multi.sql("SELECT * FROM t").await.unwrap();
    let plan = many.clone().create_physical_plan().await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 3);
    let many = many.collect().await.unwrap();

    assert_eq!(rows(&one), 63 + 712 + 63);
    let key = |b: &[RecordBatch]| -> Vec<(String, String, String)> {
        let mut v: Vec<_> = strings(b, "alignment_id")
            .into_iter()
            .zip(strings(b, "name"))
            .zip(strings(b, "sequence"))
            .map(|((a, n), s)| (a.unwrap(), n.unwrap(), s.unwrap()))
            .collect();
        v.sort();
        v
    };
    assert_eq!(key(&one), key(&many));
}

#[tokio::test]
async fn single_alignment_file_stays_one_partition_regardless_of_target_partitions() {
    let ctx = ctx_for(&data("PF00001.sto"), None, 8);
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    assert_eq!(rows(&df.collect().await.unwrap()), 63);
}

#[tokio::test]
async fn ordinal_fallback_when_no_id_or_ac() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("anon.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n//\n# STOCKHOLM 1.0\n#=GF AC ACC2\nseqB ACGT\n//\n# STOCKHOLM 1.0\nseqC ACGT\n//\n",
    )
    .unwrap();
    let ctx = ctx_for(path.to_str().unwrap(), None, 1);
    let batches = ctx
        .sql("SELECT alignment_id FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, "alignment_id"),
        vec![Some("0".into()), Some("ACC2".into()), Some("2".into())]
    );
}

#[tokio::test]
async fn later_alignments_without_a_header_survive_partitioning() {
    // A later alignment may omit its own header; splitting the file must not
    // turn that from a successful scan into a missing-header error.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("headerless.sto");
    let mut text = String::from("# STOCKHOLM 1.0\n#=GF ID first\nseqA ACGT\n//\n");
    for i in 1..6 {
        text.push_str(&format!("#=GF ID a{i}\nseq{i} ACGT\n//\n"));
    }
    std::fs::write(&path, &text).unwrap();
    let p = path.to_str().unwrap();

    let expected: Vec<Option<String>> = ["first", "a1", "a2", "a3", "a4", "a5"]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect();
    for partitions in [1, 3, 8] {
        let ctx = ctx_for(p, None, partitions);
        let batches = ctx
            .sql("SELECT alignment_id FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("target_partitions={partitions}: {e}"));
        let mut got = strings(&batches, "alignment_id");
        got.sort();
        let mut want = expected.clone();
        want.sort();
        assert_eq!(got, want, "target_partitions={partitions}");
    }
}

#[tokio::test]
async fn comment_between_alignments_does_not_consume_an_ordinal() {
    // Easel reads this file as exactly two alignments; a generic comment
    // between them must not be mistaken for a headerless alignment.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("comment.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n//\n\
         # a generic comment between alignments\n\
         # STOCKHOLM 1.0\nseqB ACGT\n//\n",
    )
    .unwrap();
    let ctx = ctx_for(path.to_str().unwrap(), None, 1);
    let batches = ctx
        .sql("SELECT alignment_id, name FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&batches), 2);
    assert_eq!(
        strings(&batches, "alignment_id"),
        vec![Some("0".into()), Some("1".into())]
    );
    assert_eq!(
        strings(&batches, "name"),
        vec![Some("seqA".into()), Some("seqB".into())]
    );
}

#[tokio::test]
async fn unknown_markup_between_alignments_is_a_comment() {
    // The body loop ignores any `#` line it does not recognise, and Easel reads
    // this file as two alignments, so the start search must not mistake an
    // unknown `#=GX` for the beginning of a headerless one.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unknown.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n//\n\
         #=GX ignored\n\
         # STOCKHOLM 1.0\nseqB ACGT\n//\n",
    )
    .unwrap();
    let ctx = ctx_for(path.to_str().unwrap(), None, 1);
    let batches = ctx
        .sql("SELECT alignment_id, name FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&batches), 2);
    assert_eq!(
        strings(&batches, "alignment_id"),
        vec![Some("0".into()), Some("1".into())]
    );
}

#[tokio::test]
async fn an_indented_terminator_is_read_the_same_however_the_file_is_split() {
    // Easel accepts `  //`. Planning and parsing must agree on that, or the
    // rows after it land in a different alignment depending on the partition
    // count.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indented.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\n#=GF ID first\nseqA ACGT\n  //\n\
         #=GF ID second\nseqB ACGT\n//\n",
    )
    .unwrap();
    let p = path.to_str().unwrap();

    let mut baseline: Vec<(Option<String>, Option<String>)> = Vec::new();
    for partitions in [1, 2, 4] {
        let ctx = ctx_for(p, None, partitions);
        let batches = ctx
            .sql("SELECT alignment_id, name FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got: Vec<_> = strings(&batches, "alignment_id")
            .into_iter()
            .zip(strings(&batches, "name"))
            .collect();
        got.sort();
        if baseline.is_empty() {
            assert_eq!(
                got,
                vec![
                    (Some("first".into()), Some("seqA".into())),
                    (Some("second".into()), Some("seqB".into())),
                ]
            );
            baseline = got;
        } else {
            assert_eq!(got, baseline, "target_partitions={partitions}");
        }
    }
}

#[tokio::test]
async fn a_missing_internal_terminator_keeps_ordinals_stable_when_split() {
    // The reader starts a new alignment at a `# STOCKHOLM 1.0` even without a
    // preceding `//`, so planning must count those too: otherwise a later
    // range is seeded with too small an ordinal and the fallback ids shift.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing_internal.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n\
         # STOCKHOLM 1.0\nseqB ACGT\n//\n\
         # STOCKHOLM 1.0\nseqC ACGT\n//\n\
         # STOCKHOLM 1.0\nseqD ACGT\n//\n",
    )
    .unwrap();
    let p = path.to_str().unwrap();

    let mut baseline: Vec<(Option<String>, Option<String>)> = Vec::new();
    for partitions in [1, 2, 4] {
        let ctx = ctx_for(p, None, partitions);
        let batches = ctx
            .sql("SELECT alignment_id, name FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got: Vec<_> = strings(&batches, "alignment_id")
            .into_iter()
            .zip(strings(&batches, "name"))
            .collect();
        got.sort_by(|a, b| a.1.cmp(&b.1));
        if baseline.is_empty() {
            // No alignment carries ID or AC, so every one falls back to its
            // 0-based position in the file.
            assert_eq!(
                got,
                vec![
                    (Some("0".into()), Some("seqA".into())),
                    (Some("1".into()), Some("seqB".into())),
                    (Some("2".into()), Some("seqC".into())),
                    (Some("3".into()), Some("seqD".into())),
                ]
            );
            baseline = got;
        } else {
            assert_eq!(got, baseline, "target_partitions={partitions}");
        }
    }
}

/// Collects `(alignment_id, name)` at several partition counts, asserting the
/// answer never depends on how the file was split.
async fn ids_are_split_invariant(path: &str) -> Vec<(Option<String>, Option<String>)> {
    let mut baseline: Vec<(Option<String>, Option<String>)> = Vec::new();
    for partitions in [1, 2, 4] {
        let ctx = ctx_for(path, None, partitions);
        let batches = ctx
            .sql("SELECT alignment_id, name FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got: Vec<_> = strings(&batches, "alignment_id")
            .into_iter()
            .zip(strings(&batches, "name"))
            .collect();
        got.sort_by(|a, b| a.1.cmp(&b.1));
        if baseline.is_empty() {
            baseline = got;
        } else {
            assert_eq!(got, baseline, "target_partitions={partitions}");
        }
    }
    baseline
}

#[tokio::test]
async fn an_input_opening_with_a_terminator_is_rejected_however_it_is_split() {
    // The leading `//` produces no alignment, so planning must not drop those
    // bytes: if the first range started after them, a split scan would accept a
    // file that a single-partition scan rejects for its missing header.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leading_term.sto");
    std::fs::write(&path, "//\nseqA ACGT\n//\nseqB ACGT\n//\n").unwrap();
    let p = path.to_str().unwrap();

    for partitions in [1, 2, 4] {
        let ctx = ctx_for(p, None, partitions);
        let err = ctx
            .sql("SELECT * FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err(&format!(
                "target_partitions={partitions}: a missing header must not become valid"
            ))
            .to_string();
        assert!(err.contains("# STOCKHOLM 1.0"), "{partitions}: {err}");
    }
}

#[tokio::test]
async fn an_indented_internal_header_is_counted_as_the_reader_counts_it() {
    // The reader trims only the end inside an alignment, so an indented
    // `# STOCKHOLM 1.0` there is sequence data, not a new alignment. Planning
    // must agree or it seeds the next range one ordinal too high.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indented_header.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n  # STOCKHOLM 1.0\nseqB ACGT\n//\n\
         # STOCKHOLM 1.0\nseqC ACGT\n//\n\
         # STOCKHOLM 1.0\nseqD ACGT\n//\n",
    )
    .unwrap();
    let ids = ids_are_split_invariant(path.to_str().unwrap()).await;
    // One alignment holds seqA, the indented line and seqB; then two more.
    assert_eq!(
        ids.iter().map(|(a, _)| a.clone()).collect::<Vec<_>>(),
        vec![
            Some("0".into()),
            Some("0".into()),
            Some("0".into()),
            Some("1".into()),
            Some("2".into())
        ]
    );
}

#[tokio::test]
async fn an_orphan_terminator_is_not_an_alignment() {
    // A `//` with no alignment open closes nothing, so it must not emit an
    // empty alignment and consume an ordinal — planning does not count it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("orphan.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\nseqA ACGT\n//\n//\nseqB ACGT\n//\nseqC ACGT\n//\n",
    )
    .unwrap();
    let ids = ids_are_split_invariant(path.to_str().unwrap()).await;
    assert_eq!(
        ids,
        vec![
            (Some("0".into()), Some("seqA".into())),
            (Some("1".into()), Some("seqB".into())),
            (Some("2".into()), Some("seqC".into())),
        ]
    );
}

#[tokio::test]
async fn a_markup_label_needs_a_separator() {
    // `#=GSX` is not `#=GS`: it is an unknown `#` line, so it is ignored inside
    // an alignment and consumes no ordinal between two.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("label.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\n#=GSX ignored\nseqA ACGT\n//\n\
         #=GRX ignored\n\
         # STOCKHOLM 1.0\nseqB ACGT\n//\n",
    )
    .unwrap();
    let ids = ids_are_split_invariant(path.to_str().unwrap()).await;
    assert_eq!(
        ids,
        vec![
            (Some("0".into()), Some("seqA".into())),
            (Some("1".into()), Some("seqB".into())),
        ],
        "unknown labels must not create rows or consume ordinals"
    );
}

#[tokio::test]
async fn unsupported_header_versions_are_rejected() {
    // Easel rejects every one of these with "missing Stockholm header".
    let dir = tempfile::tempdir().unwrap();
    for header in [
        "# STOCKHOLM 2.0",
        "# STOCKHOLM garbage",
        "# STOCKHOLMX",
        "#STOCKHOLM 1.0",
        // Easel requires exactly one space before the version and rejects both
        // of these, so a trimmed-suffix comparison is not enough.
        "# STOCKHOLM1.0",
        "# STOCKHOLM  1.0",
        // Easel accepts trailing whitespace but rejects leading whitespace.
        "  # STOCKHOLM 1.0",
        "\t# STOCKHOLM 1.0",
    ] {
        let path = dir.path().join("h.sto");
        std::fs::write(&path, format!("{header}\nseqA ACGT\n//\n")).unwrap();
        let ctx = ctx_for(path.to_str().unwrap(), None, 1);
        let err = ctx
            .sql("SELECT * FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("# STOCKHOLM 1.0"), "{header:?}: {err}");
    }
}

#[tokio::test]
async fn header_tolerates_trailing_whitespace() {
    // Easel accepts this.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ws.sto");
    std::fs::write(&path, "# STOCKHOLM 1.0   \nseqA ACGT\n//\n").unwrap();
    let ctx = ctx_for(path.to_str().unwrap(), None, 1);
    let batches = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&batches), 1);
}

#[tokio::test]
async fn comment_after_the_final_terminator_keeps_one_partition() {
    // The reader skips a trailing comment and returns no second alignment, so
    // the boundary scan must not advertise a partition for it either.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tail.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str("\n# a trailing comment\n   \n");
    std::fs::write(&path, text).unwrap();

    let ctx = ctx_for(path.to_str().unwrap(), None, 4);
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    assert_eq!(
        df.clone()
            .create_physical_plan()
            .await
            .unwrap()
            .output_partitioning()
            .partition_count(),
        1,
        "a comment-only tail is not an alignment"
    );
    assert_eq!(rows(&df.collect().await.unwrap()), 63);
}

#[tokio::test]
async fn headerless_alignment_after_the_final_terminator_is_still_a_partition() {
    // The counterpart to the test above: annotation or sequence data after the
    // last `//` really is another alignment and must keep its partition.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tail_data.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str("#=GF ID second\nseqZ ACGT\n");
    std::fs::write(&path, text).unwrap();

    let ctx = ctx_for(path.to_str().unwrap(), None, 4);
    let df = ctx.sql("SELECT alignment_id FROM t").await.unwrap();
    assert_eq!(
        df.clone()
            .create_physical_plan()
            .await
            .unwrap()
            .output_partitioning()
            .partition_count(),
        2
    );
    let batches = df.collect().await.unwrap();
    assert_eq!(rows(&batches), 64);
    let mut ids = strings(&batches, "alignment_id");
    ids.sort();
    ids.dedup();
    assert_eq!(ids, vec![Some("7tm_1".into()), Some("second".into())]);
}

/// Writes PF00001 + RF00001 + the hmmalign fixture into one file: three
/// alignments, so `target_partitions = 3` really splits.
fn three_alignment_file(dir: &std::path::Path) -> String {
    let path = dir.join("multi.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str(&std::fs::read_to_string(data("RF00001.sto")).unwrap());
    text.push_str(&std::fs::read_to_string(data("PF00001_hmmalign.sto")).unwrap());
    std::fs::write(&path, text).unwrap();
    path.to_str().unwrap().to_string()
}

async fn scan_with_limit(path: &str, target_partitions: usize, limit: Option<usize>) -> usize {
    use datafusion::catalog::TableProvider;
    use datafusion::physical_plan::collect;

    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let ctx = SessionContext::new_with_config(config);
    let provider = StockholmTableProvider::new(path.to_string(), None, None).unwrap();
    let plan = provider.scan(&ctx.state(), None, &[], limit).await.unwrap();
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    rows(&batches)
}

#[tokio::test]
async fn a_zero_limit_reads_nothing() {
    assert_eq!(scan_with_limit(&data("PF00001.sto"), 1, Some(0)).await, 0);
    // Zero per partition is zero overall, so this must hold when split too.
    let dir = tempfile::tempdir().unwrap();
    let path = three_alignment_file(dir.path());
    assert_eq!(scan_with_limit(&path, 3, Some(0)).await, 0);
}

#[tokio::test]
async fn a_zero_limit_does_not_touch_the_input() {
    // Planning must not open the file either: no compression sniffing, no
    // boundary scan. A path that cannot be opened at all proves it.
    use datafusion::catalog::TableProvider;
    use datafusion::physical_plan::collect;

    let missing = format!(
        "{}/tests/data/does-not-exist.sto",
        env!("CARGO_MANIFEST_DIR")
    );
    for target_partitions in [1, 4] {
        let config = SessionConfig::new().with_target_partitions(target_partitions);
        let ctx = SessionContext::new_with_config(config);
        let provider = StockholmTableProvider::new(missing.clone(), None, None).unwrap();
        let plan = provider
            .scan(&ctx.state(), None, &[], Some(0))
            .await
            .unwrap_or_else(|e| {
                panic!("target_partitions={target_partitions}: planning read the input: {e}")
            });
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(rows(&batches), 0);
    }
}

#[tokio::test]
async fn a_positive_limit_is_never_multiplied_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let path = three_alignment_file(dir.path());
    let total = 63 + 712 + 63;

    // One partition: a per-partition stop is the global stop, so it applies.
    assert_eq!(scan_with_limit(&path, 1, Some(5)).await, 5);

    // Several partitions: a per-partition stop is not the global stop. The
    // limit is a lower bound the plan may exceed, so returning everything is
    // fine — returning 5 per partition (15) is the bug.
    let split = scan_with_limit(&path, 3, Some(5)).await;
    assert_eq!(split, total, "the limit must not be applied per partition");
    assert!(split >= 5);

    // SQL is unaffected either way: DataFusion enforces the real limit above.
    let config = SessionConfig::new().with_target_partitions(3);
    let ctx = SessionContext::new_with_config(config);
    let provider = StockholmTableProvider::new(path.clone(), None, None).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let via_sql = ctx
        .sql("SELECT * FROM t LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&via_sql), 5);
}

#[tokio::test]
async fn unterminated_final_alignment_survives_partitioning() {
    // Exercises the boundary scan's tail handling: the last alignment has no
    // `//`, and the file is split.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unterminated.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str("# STOCKHOLM 1.0\n#=GF ID tail\nseqZ ACGT\n");
    std::fs::write(&path, text).unwrap();

    let ctx = ctx_for(path.to_str().unwrap(), None, 4);
    let df = ctx.sql("SELECT alignment_id FROM t").await.unwrap();
    assert_eq!(
        df.clone()
            .create_physical_plan()
            .await
            .unwrap()
            .output_partitioning()
            .partition_count(),
        2
    );
    let batches = df.collect().await.unwrap();
    assert_eq!(rows(&batches), 64);
    let mut ids = strings(&batches, "alignment_id");
    ids.sort();
    ids.dedup();
    assert_eq!(ids, vec![Some("7tm_1".into()), Some("tail".into())]);
}

#[tokio::test]
async fn local_file_uri_is_accepted() {
    let uri = format!("file://{}", data("PF00001.sto"));
    let ctx = ctx_for(&uri, None, 1);
    let batches = ctx
        .sql("SELECT name FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&batches), 63);
}

#[tokio::test]
async fn local_file_uri_is_accepted_when_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.sto");
    let mut text = std::fs::read_to_string(data("PF00001.sto")).unwrap();
    text.push_str(&std::fs::read_to_string(data("RF00001.sto")).unwrap());
    std::fs::write(&path, text).unwrap();
    let uri = format!("file://{}", path.to_str().unwrap());
    let ctx = ctx_for(&uri, None, 2);
    let df = ctx.sql("SELECT name FROM t").await.unwrap();
    assert_eq!(
        df.clone()
            .create_physical_plan()
            .await
            .unwrap()
            .output_partitioning()
            .partition_count(),
        2,
        "the boundary scan must resolve the file:// URI too"
    );
    assert_eq!(rows(&df.collect().await.unwrap()), 63 + 712);
}

#[tokio::test]
async fn missing_trailing_terminator_still_yields_rows() {
    let batches = scan("missing_terminator.sto", "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 63);
}

#[tokio::test]
async fn wrong_header_is_an_error_naming_the_path() {
    let ctx = ctx_for(&data("wrong_header.sto"), None, 1);
    let err = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("wrong_header.sto"), "{err}");
    assert!(err.contains("# STOCKHOLM 1.0"), "{err}");
}

#[tokio::test]
async fn empty_file_yields_no_rows() {
    let batches = scan("empty.sto", "SELECT * FROM t").await;
    assert_eq!(rows(&batches), 0);
}

#[tokio::test]
async fn single_sequence_alignment() {
    let batches = scan("single.sto", "SELECT * FROM t").await;
    assert_eq!(
        strings(&batches, "alignment_id"),
        vec![Some("single".into())]
    );
    assert_eq!(strings(&batches, "name"), vec![Some("only".into())]);
    assert_eq!(strings(&batches, "sequence"), vec![Some("ACDE.F-G".into())]);
    assert_eq!(tag_lists(&batches, "gs"), vec![None]);
    assert_eq!(tag_lists(&batches, "gr"), vec![None]);
}

#[tokio::test]
async fn gs_fields_promote_named_features_and_keep_bag_with_sentinel() {
    let ctx = ctx_for(
        &data("PF00001.sto"),
        Some(vec!["AC".into(), "DE".into()]),
        1,
    );
    let batches = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        ["alignment_id", "name", "sequence", "AC", "DE", "gr"]
    );
    let ac = strings(&batches, "AC");
    assert_eq!(ac[0].as_deref(), Some("P25929.1"));
    assert!(
        strings(&batches, "DE").iter().all(|d| d.is_none()),
        "no #=GS DE in the seed"
    );

    let ctx = ctx_for(
        &data("PF00001.sto"),
        Some(vec!["AC".into(), "gs".into()]),
        1,
    );
    let batches = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        ["alignment_id", "name", "sequence", "AC", "gs", "gr"]
    );
    assert_eq!(
        tag_lists(&batches, "gs")[0],
        Some(vec![("AC".to_string(), Some("P25929.1".to_string()))])
    );
}

#[tokio::test]
async fn compressed_inputs_match_plain() {
    let plain = scan("PF00001.sto", "SELECT name, sequence FROM t").await;
    for name in ["PF00001.sto.gz", "PF00001.sto.bgz"] {
        let got = scan(name, "SELECT name, sequence FROM t").await;
        assert_eq!(strings(&plain, "name"), strings(&got, "name"), "{name}");
        assert_eq!(
            strings(&plain, "sequence"),
            strings(&got, "sequence"),
            "{name}"
        );
    }
}

#[tokio::test]
async fn projection_count_and_limit() {
    let only_name = scan("PF00001.sto", "SELECT name FROM t").await;
    assert_eq!(only_name[0].num_columns(), 1);
    assert_eq!(rows(&only_name), 63);

    let count = scan("RF00001.sto", "SELECT count(*) AS n FROM t").await;
    assert_eq!(count[0].column(0).as_primitive::<Int64Type>().value(0), 712);

    let limited = scan("RF00001.sto", "SELECT name FROM t LIMIT 10").await;
    assert_eq!(rows(&limited), 10);
}

#[tokio::test]
async fn annotations_preserve_repeated_gf_features_in_order() {
    let batch = read_stockholm_annotations(data("PF00001.sto"), None)
        .await
        .unwrap();
    let kinds = strings(std::slice::from_ref(&batch), "kind");
    let features = strings(std::slice::from_ref(&batch), "feature");
    let values = strings(std::slice::from_ref(&batch), "value");
    let gf: Vec<&str> = kinds
        .iter()
        .zip(&features)
        .filter(|(k, _)| k.as_deref() == Some("GF"))
        .map(|(_, f)| f.as_deref().unwrap())
        .collect();
    assert_eq!(gf.len(), 49);
    assert_eq!(&gf[..3], ["ID", "AC", "DE"]);
    assert_eq!(gf.iter().filter(|f| **f == "DR").count(), 11);
    assert_eq!(gf.iter().filter(|f| **f == "CC").count(), 10);
    assert_eq!(values[0].as_deref(), Some("7tm_1"));
    assert_eq!(values[1].as_deref(), Some("PF00001.27"));

    let gc: Vec<(&str, usize)> = kinds
        .iter()
        .zip(&features)
        .zip(&values)
        .filter(|((k, _), _)| k.as_deref() == Some("GC"))
        .map(|((_, f), v)| (f.as_deref().unwrap(), v.as_deref().unwrap().len()))
        .collect();
    assert_eq!(gc, vec![("seq_cons", 722), ("RF", 722)]);

    let n = batch
        .column_by_name("n_sequences")
        .unwrap()
        .as_primitive::<UInt32Type>();
    let len = batch
        .column_by_name("alignment_length")
        .unwrap()
        .as_primitive::<UInt32Type>();
    assert!((0..batch.num_rows()).all(|i| n.value(i) == 63 && len.value(i) == 722));
    let ids: HashSet<String> = strings(&[batch], "alignment_id")
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(ids, HashSet::from(["7tm_1".to_string()]));
}

#[tokio::test]
async fn annotation_rows_follow_file_order_across_kinds() {
    // `#=GF` and `#=GC` lines must come back interleaved as they appear, not
    // grouped by kind, so the long format can reconstruct the alignment header.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.sto");
    std::fs::write(
        &path,
        "# STOCKHOLM 1.0\n\
         #=GF ID mixed\n\
         #=GC RF xxxx\n\
         #=GF DE after a GC line\n\
         #=GC SS_cons ....\n\
         #=GF CC trailing\n\
         seqA ACGT\n\
         #=GC RF yyyy\n\
         //\n",
    )
    .unwrap();
    let batch = read_stockholm_annotations(path.to_str().unwrap().to_string(), None)
        .await
        .unwrap();
    let kinds = strings(std::slice::from_ref(&batch), "kind");
    let features = strings(std::slice::from_ref(&batch), "feature");
    let values = strings(std::slice::from_ref(&batch), "value");
    let got: Vec<(String, String, String)> = kinds
        .into_iter()
        .zip(features)
        .zip(values)
        .map(|((k, f), v)| (k.unwrap(), f.unwrap(), v.unwrap()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("GF".into(), "ID".into(), "mixed".into()),
            // Repeated across two blocks, reported at its first position.
            ("GC".into(), "RF".into(), "xxxxyyyy".into()),
            ("GF".into(), "DE".into(), "after a GC line".into()),
            ("GC".into(), "SS_cons".into(), "....".into()),
            ("GF".into(), "CC".into(), "trailing".into()),
        ]
    );
}

#[tokio::test]
async fn annotations_of_interleaved_file_concatenate_gc_across_blocks() {
    let batch = read_stockholm_annotations(data("RF00001.sto"), None)
        .await
        .unwrap();
    let kinds = strings(std::slice::from_ref(&batch), "kind");
    let features = strings(std::slice::from_ref(&batch), "feature");
    let values = strings(std::slice::from_ref(&batch), "value");
    let gc: Vec<(&str, usize)> = kinds
        .iter()
        .zip(&features)
        .zip(&values)
        .filter(|((k, _), _)| k.as_deref() == Some("GC"))
        .map(|((_, f), v)| (f.as_deref().unwrap(), v.as_deref().unwrap().len()))
        .collect();
    assert_eq!(gc, vec![("SS_cons", 230), ("RF", 230)]);
    let n = batch
        .column_by_name("n_sequences")
        .unwrap()
        .as_primitive::<UInt32Type>();
    let len = batch
        .column_by_name("alignment_length")
        .unwrap()
        .as_primitive::<UInt32Type>();
    assert_eq!((n.value(0), len.value(0)), (712, 230));
}
