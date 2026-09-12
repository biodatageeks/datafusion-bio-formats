#![cfg(feature = "text-formats")]
use datafusion::{
    arrow::array::{Array, Float64Array, UInt64Array},
    prelude::*,
};
use datafusion_bio_format_structure::{
    StructureLevel, StructureOptions, StructureTableProvider, mmcif, pdb, residue::residues,
};
use std::sync::Arc;
fn fixture(name: &str) -> String {
    format!(
        "{}/../../testing/data/structure/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}
fn golden(name: &str) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../../testing/oracles/structure/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn paired_atoms_and_six_angle_oracles() {
    let options = StructureOptions::default();
    let p = pdb::parse(
        &std::fs::read_to_string(fixture("1ubq.pdb")).unwrap(),
        &options,
    )
    .unwrap()
    .remove(0);
    let c = mmcif::parse(&std::fs::read(fixture("1ubq.cif")).unwrap(), &options)
        .unwrap()
        .remove(0);
    let atoms = golden("1ubq.atoms.json");
    for entry in [&p, &c] {
        assert_eq!(entry.atoms.len(), 660);
        for (a, want) in entry.atoms.iter().zip(atoms.as_array().unwrap()) {
            assert_eq!(a.atom_name, want["atom_name"]);
            assert_eq!(a.residue_name, want["residue_name"]);
            assert_eq!(a.auth_seq_id.as_deref(), want["auth_seq_id"].as_str());
            assert_eq!(a.chain_id(), want["chain_id"].as_str());
            for i in 0..3 {
                assert!((a.position[i] - want["position"][i].as_f64().unwrap()).abs() < 1e-9);
            }
        }
        let rs = residues(entry, &options);
        assert_eq!(rs.len(), 76);
        for (r, want) in rs
            .iter()
            .zip(golden("1ubq.residues.json").as_array().unwrap())
        {
            assert_eq!(r.atom.auth_seq_id.as_deref(), want["auth_seq_id"].as_str());
            for i in 0..6 {
                let expected = want["angles"][i].as_f64();
                assert_eq!(
                    r.angles[i].is_some(),
                    expected.is_some(),
                    "{} angle {i}",
                    r.atom.residue_index
                );
                if let (Some(x), Some(y)) = (r.angles[i], expected) {
                    assert!(
                        ((x - y + 180.).rem_euclid(360.) - 180.).abs() < 1e-6,
                        "angle {i}: {x} != {y}"
                    );
                }
            }
        }
    }
    assert!(p.atoms.iter().all(|a| a.label_seq_id.is_none()));
    assert_eq!(c.atoms[0].label_seq_id, Some(1));
}
#[test]
fn raw_cif_identifiers_tokens_blocks_and_errors() {
    let text = "data_one\n_note.text\n;multiline\nvalue\n;\nloop_\n_atom_site.id\n_atom_site.auth_atom_id\n_atom_site.label_atom_id\n_atom_site.auth_comp_id\n_atom_site.auth_asym_id\n_atom_site.label_asym_id\n_atom_site.auth_seq_id\n_atom_site.label_seq_id\n_atom_site.label_alt_id\n_atom_site.pdbx_PDB_ins_code\n_atom_site.Cartn_x\n_atom_site.Cartn_y\n_atom_site.Cartn_z\n1 CA CA ALA '' LABEL X1 1 . '?' 1 2 3\ndata_empty\n_entry.id second\n";
    let entries = mmcif::parse(text.as_bytes(), &StructureOptions::default()).unwrap();
    assert_eq!(entries.len(), 1);
    let a = &entries[0].atoms[0];
    assert_eq!(a.auth_seq_id.as_deref(), Some("X1"));
    assert_eq!(a.chain_id(), Some(""));
    assert_eq!(a.label_asym_id.as_deref(), Some("LABEL"));
    assert_eq!(a.alt_id, None);
    assert_eq!(a.insertion_code.as_deref(), Some("?"));
    assert!(
        mmcif::parse(
            b"data_x\nloop_\n_a.x\n_a.y\n1",
            &StructureOptions::default()
        )
        .is_err()
    );
}
#[tokio::test]
async fn projections_counts_filters_batches_and_repeated_collect() {
    for partitions in [1, 4] {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(partitions)
                .with_batch_size(7),
        );
        let opts = StructureOptions {
            level: StructureLevel::Residue,
            ..Default::default()
        };
        let table = StructureTableProvider::new(
            vec![fixture("1ubq.pdb"), fixture("1ubq.cif")],
            None,
            opts,
            None,
        )
        .unwrap();
        ctx.register_table("s", Arc::new(table)).unwrap();
        let df = ctx
            .sql("SELECT phi_deg FROM s WHERE auth_seq_id = '2' ORDER BY source_index")
            .await
            .unwrap();
        for _ in 0..2 {
            let b = df.clone().collect().await.unwrap();
            assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
            for b in b {
                let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..a.len() {
                    assert!((a.value(i) + 91.02018604276657).abs() < 1e-6);
                }
            }
        }
        let b = ctx
            .sql("SELECT count(*) FROM s")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            b[0].column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            152
        );
        let b = ctx
            .sql("SELECT source_index FROM s LIMIT 3")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert!(b[0].column(0).as_any().is::<UInt64Array>());
    }
}
#[tokio::test]
async fn schema_is_lazy_and_error_has_source() {
    let path = "/missing/structure-455.pdb";
    let table =
        StructureTableProvider::new(vec![path.into()], None, StructureOptions::default(), None)
            .unwrap();
    let ctx = SessionContext::new();
    let df = ctx.read_table(Arc::new(table)).unwrap();
    assert_eq!(df.schema().fields().len(), 37);
    let error = df.collect().await.unwrap_err().to_string();
    assert!(error.contains(path), "{error}");
}
#[test]
fn pdb_models_ter_blank_fields_charge_and_limits() {
    let text = std::fs::read_to_string(fixture("1ubq.pdb")).unwrap();
    let first = text.lines().find(|s| s.starts_with("ATOM")).unwrap();
    let block =
        format!("MODEL        7\n{first}\nTER\n{first}\nENDMDL\nMODEL        9\n{first}\nENDMDL\n");
    let e = pdb::parse(&block, &StructureOptions::default())
        .unwrap()
        .remove(0);
    assert_eq!(e.atoms.len(), 3);
    assert_ne!(e.atoms[0].residue_index, e.atoms[1].residue_index);
    assert_eq!(e.atoms[2].model_id, 9);
    assert!(
        pdb::parse(
            &block,
            &StructureOptions {
                max_atoms: 2,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(pdb::parse("ATOM      1", &StructureOptions::default()).is_err());
}
#[tokio::test]
async fn gzip_lists_globs_and_input_limits() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let data = std::fs::read(fixture("1ubq.pdb")).unwrap();
    let path = dir.path().join("one.pdb.gz");
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(&path).unwrap(),
        flate2::Compression::default(),
    );
    gz.write_all(&data).unwrap();
    gz.finish().unwrap();
    let opts = StructureOptions::default();
    let paths = vec![
        format!("{}/*.gz", dir.path().display()),
        path.to_string_lossy().into_owned(),
    ];
    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(
            StructureTableProvider::new(paths, None, opts.clone(), None).unwrap(),
        ))
        .unwrap();
    assert_eq!(df.count().await.unwrap(), 1320);
    let table = StructureTableProvider::new(
        vec![path.to_string_lossy().into_owned()],
        None,
        StructureOptions {
            max_decoded_bytes: 10,
            ..opts
        },
        None,
    )
    .unwrap();
    assert!(
        ctx.read_table(Arc::new(table))
            .unwrap()
            .collect()
            .await
            .is_err()
    );
    assert!(
        StructureTableProvider::new(
            vec![format!("{}/*.missing", dir.path().display())],
            None,
            StructureOptions::default(),
            None
        )
        .is_err()
    );
}
#[test]
fn cif_site_specific_modified_residue_parent() {
    let text = "data_modified\n_pdbx_struct_mod_residue.auth_seq_id X1\n_pdbx_struct_mod_residue.auth_asym_id A\n_pdbx_struct_mod_residue.auth_comp_id ZZZ\n_pdbx_struct_mod_residue.parent_comp_id SER\n_atom_site.auth_atom_id CA\n_atom_site.auth_comp_id ZZZ\n_atom_site.auth_asym_id A\n_atom_site.auth_seq_id X1\n_atom_site.Cartn_x 1\n_atom_site.Cartn_y 2\n_atom_site.Cartn_z 3\n";
    let options = StructureOptions::default();
    let entry = mmcif::parse(text.as_bytes(), &options).unwrap().remove(0);
    let result = residues(&entry, &options);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].one_letter_code.as_deref(), Some("S"));
    assert_eq!(result[0].parent_residue_name.as_deref(), Some("SER"));
}
fn atom_block(name: &str, atoms: usize) -> String {
    let mut s = format!(
        "data_{name}\nloop_\n_atom_site.id\n_atom_site.auth_atom_id\n_atom_site.auth_comp_id\n_atom_site.auth_asym_id\n_atom_site.auth_seq_id\n_atom_site.Cartn_x\n_atom_site.Cartn_y\n_atom_site.Cartn_z\n"
    );
    for i in 0..atoms {
        s.push_str(&format!("{i} CA ALA A {i} 1 2 3\n"));
    }
    s
}
#[test]
fn cif_blocks_decode_one_at_a_time() {
    let text = format!(
        "{}data_meta\n_entry.id only\n{}",
        atom_block("first", 2),
        atom_block("third", 3)
    );
    let options = StructureOptions::default();
    let blocks = mmcif::Blocks::parse(text.as_bytes()).unwrap();
    assert_eq!(blocks.len(), 3);
    assert!(blocks.entry(1, &options).unwrap().is_none());
    let first = blocks.entry(0, &options).unwrap().unwrap();
    assert_eq!((first.entry_index, first.atoms.len()), (0, 2));
    let third = blocks.entry(2, &options).unwrap().unwrap();
    assert_eq!((third.entry_index, third.atoms.len()), (2, 3));
    assert_eq!(third.data_block.as_deref(), Some("third"));
    // The atom limit is enforced per block, and a later oversized block fails only when reached.
    let small = StructureOptions {
        max_atoms: 2,
        ..Default::default()
    };
    assert!(blocks.entry(0, &small).unwrap().is_some());
    assert!(blocks.entry(2, &small).is_err());
    assert!(
        mmcif::Blocks::parse(b"data_x\n_a.b 1\n")
            .unwrap()
            .entry(0, &options)
            .unwrap()
            .is_none()
    );
    assert!(mmcif::parse(b"data_x\n_a.b 1\n", &options).is_err());
}
#[tokio::test]
async fn multi_block_cif_streams_entries_before_a_later_block_fails() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.cif");
    std::fs::write(
        &good,
        format!("{}{}", atom_block("a", 2), atom_block("b", 4)),
    )
    .unwrap();
    let bad = dir.path().join("bad.cif");
    std::fs::write(
        &bad,
        format!("{}{}", atom_block("a", 2), atom_block("b", 4))
            .replace("3 CA ALA A 3 1 2 3", "3 CA ALA A 3 x 2 3"),
    )
    .unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(3));
    let table = StructureTableProvider::new(
        vec![good.to_string_lossy().into_owned()],
        None,
        StructureOptions::default(),
        None,
    )
    .unwrap();
    let batches = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .select_columns(&["entry_index", "data_block"])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut indices = vec![];
    for b in &batches {
        let col = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        indices.extend(col.values().iter().copied());
    }
    assert_eq!(indices, [0, 0, 1, 1, 1, 1]);
    let table = StructureTableProvider::new(
        vec![bad.to_string_lossy().into_owned()],
        None,
        StructureOptions::default(),
        None,
    )
    .unwrap();
    let plan = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    use futures::StreamExt;
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    let error = stream.next().await.unwrap().unwrap_err().to_string();
    assert!(
        error.contains("block \"b\"") && error.contains(&bad.to_string_lossy().into_owned()),
        "{error}"
    );
}
