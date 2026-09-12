use datafusion::{
    arrow::array::{Array, Float64Array, UInt64Array},
    prelude::*,
};
use datafusion_bio_format_foldcomp::{FoldcompOptions, FoldcompTableProvider, codec};
use datafusion_bio_format_structure::{StructureLevel, StructureOptions, residue::residues};
use std::sync::Arc;
fn fixture(name: &str) -> String {
    format!(
        "{}/../../testing/data/structure/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}
#[test]
fn raw_codec_oracle_and_malformed_headers() {
    let data = std::fs::read(fixture("1ubq.fcz")).unwrap();
    let options = StructureOptions::default();
    let entry = codec::decode(&data, &options).unwrap();
    let gold: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../../testing/oracles/structure/1ubq.fcz.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(entry.entry_id.as_deref(), gold["title"].as_str());
    assert_eq!(entry.atoms.len(), 602);
    for (a, g) in entry.atoms.iter().zip(gold["atoms"].as_array().unwrap()) {
        assert_eq!(a.atom_name, g["atom_name"]);
        assert_eq!(a.residue_name, g["residue_name"]);
        assert_eq!(a.auth_seq_id.as_deref(), g["auth_seq_id"].as_str());
        for i in 0..3 {
            assert!(
                (a.position[i] - g["position"][i].as_f64().unwrap()).abs() < 1e-4,
                "coordinate mismatch for {} axis {i}: {} vs {}",
                a.atom_name,
                a.position[i],
                g["position"][i]
            );
        }
    }
    for (r, g) in residues(&entry, &options)
        .iter()
        .zip(gold["residues"].as_array().unwrap())
    {
        for i in 0..6 {
            assert_eq!(r.angles[i].is_none(), g["angles"][i].is_null());
            if let Some(x) = r.angles[i] {
                let y = g["angles"][i].as_f64().unwrap();
                assert!(((x - y + 180.).rem_euclid(360.) - 180.).abs() < 0.01);
            }
        }
    }
    for n in [0, 4, 75, 100, data.len() - 1] {
        assert!(codec::decode(&data[..n], &options).is_err());
    }
    for at in [4, 12, 16, 24, 76] {
        let mut corrupt = data.clone();
        corrupt[at..at + 4].fill(255);
        assert!(codec::decode(&corrupt, &options).is_err());
    }
}
#[tokio::test]
async fn subset_empty_duplicate_missing_and_repeated_execution() {
    let path = fixture("example_db");
    for (ids, count) in [(vec![], 0), (vec!["d1asha_", "d1it2a_", "d1asha_"], 2)] {
        let options = FoldcompOptions {
            ids: Some(ids.into_iter().map(str::to_owned).collect()),
            structure: StructureOptions {
                level: StructureLevel::Residue,
                ..Default::default()
            },
            ..Default::default()
        };
        let ctx = SessionContext::new();
        ctx.register_table(
            "s",
            Arc::new(FoldcompTableProvider::new(path.clone(), options).unwrap()),
        )
        .unwrap();
        for _ in 0..2 {
            let b = ctx
                .sql("SELECT DISTINCT entry_key FROM s ORDER BY entry_key")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), count);
            if count > 0 {
                let a = b[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert_eq!(a.value(0), 0);
                assert_eq!(a.value(1), 7);
            }
        }
    }
    assert!(
        FoldcompTableProvider::new(
            path.clone(),
            FoldcompOptions {
                ids: Some(vec!["missing".into()]),
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        FoldcompTableProvider::new(
            path,
            FoldcompOptions {
                entry_keys: Some(vec![999]),
                ..Default::default()
            }
        )
        .is_err()
    );
}
#[tokio::test]
async fn standalone_atom_projection() {
    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(
            FoldcompTableProvider::new(fixture("1ubq.fcz"), FoldcompOptions::default()).unwrap(),
        ))
        .unwrap()
        .select_columns(&["x"])
        .unwrap();
    let b = df.collect().await.unwrap();
    assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 602);
    assert_eq!(
        b[0].column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .null_count(),
        0
    );
}
fn copy_database() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("db").to_string_lossy().into_owned();
    for suffix in ["", ".index", ".lookup", ".dbtype"] {
        std::fs::copy(
            fixture(&format!("example_db{suffix}")),
            format!("{dest}{suffix}"),
        )
        .unwrap();
    }
    (dir, dest)
}
#[tokio::test]
async fn unselected_corruption_is_not_decoded_and_sidecar_changes_fail() {
    use std::io::{Seek, SeekFrom, Write};
    let (_dir, path) = copy_database();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(2307)).unwrap();
    file.write_all(b"BAD!").unwrap();
    drop(file);
    let ctx = SessionContext::new();
    let selected = FoldcompTableProvider::new(
        path.clone(),
        FoldcompOptions {
            entry_keys: Some(vec![0]),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        ctx.read_table(Arc::new(selected))
            .unwrap()
            .count()
            .await
            .unwrap()
            > 0
    );
    let all = FoldcompTableProvider::new(path.clone(), FoldcompOptions::default()).unwrap();
    assert!(
        ctx.read_table(Arc::new(all))
            .unwrap()
            .collect()
            .await
            .is_err()
    );
    let stable = FoldcompTableProvider::new(
        path.clone(),
        FoldcompOptions {
            entry_keys: Some(vec![0]),
            ..Default::default()
        },
    )
    .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(format!("{path}.index"))
        .unwrap()
        .write_all(b"999\t0\t1\n")
        .unwrap();
    assert!(
        ctx.read_table(Arc::new(stable))
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string()
            .contains("changed")
    );
}
#[test]
fn numeric_selection_without_lookup_and_invalid_ranges() {
    let (_dir, path) = copy_database();
    std::fs::remove_file(format!("{path}.lookup")).unwrap();
    assert!(
        FoldcompTableProvider::new(
            path.clone(),
            FoldcompOptions {
                entry_keys: Some(vec![7]),
                ..Default::default()
            }
        )
        .is_ok()
    );
    assert!(
        FoldcompTableProvider::new(
            path.clone(),
            FoldcompOptions {
                ids: Some(vec!["d1asha_".into()]),
                ..Default::default()
            }
        )
        .is_err()
    );
    std::fs::write(format!("{path}.index"), "0\t18446744073709551615\t2\n").unwrap();
    assert!(FoldcompTableProvider::new(path, FoldcompOptions::default()).is_err());
}
#[tokio::test]
async fn execution_metrics_count_only_selected_payloads() {
    use datafusion::physical_plan::{ExecutionPlan, collect};
    fn decodes(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = plan
            .metrics()
            .and_then(|m| m.sum_by_name("entries_decoded"))
            .map(|v| v.as_usize())
            .unwrap_or_default();
        own + plan.children().into_iter().map(decodes).sum::<usize>()
    }
    for keys in [vec![], vec![0, 7, 0]] {
        let expected = if keys.is_empty() { 0 } else { 2 };
        let ctx = SessionContext::new();
        let table = FoldcompTableProvider::new(
            fixture("example_db"),
            FoldcompOptions {
                entry_keys: Some(keys),
                ..Default::default()
            },
        )
        .unwrap();
        let df = ctx.read_table(Arc::new(table)).unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        collect(plan.clone(), ctx.task_ctx()).await.unwrap();
        assert_eq!(decodes(&plan), expected);
    }
}
#[tokio::test]
async fn dbtype_trailing_bytes_and_blank_metadata_lines_are_tolerated() {
    use std::io::Write;
    let (_dir, path) = copy_database();
    std::fs::write(format!("{path}.dbtype"), [12u8, 0, 0, 0, b'\n']).unwrap();
    let mut index = std::fs::read_to_string(format!("{path}.index")).unwrap();
    index.insert(0, '\n');
    index.push_str("\n  \n");
    std::fs::write(format!("{path}.index"), index).unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(format!("{path}.lookup"))
        .unwrap()
        .write_all(b"\n\n")
        .unwrap();
    let ctx = SessionContext::new();
    let table = FoldcompTableProvider::new(
        path.clone(),
        FoldcompOptions {
            ids: Some(vec!["d1it2a_".into()]),
            ..Default::default()
        },
    )
    .unwrap();
    let batches = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .select_columns(&["entry_index", "entry_key", "entry_name"])
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .collect()
        .await
        .unwrap();
    let b = &batches[0];
    let index = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    let key = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!((index.value(0), key.value(0)), (7, 7));
    for bad in [vec![13u8, 0, 0, 0], vec![12u8, 0, 0]] {
        std::fs::write(format!("{path}.dbtype"), bad).unwrap();
        assert!(FoldcompTableProvider::new(path.clone(), FoldcompOptions::default()).is_err());
    }
}
