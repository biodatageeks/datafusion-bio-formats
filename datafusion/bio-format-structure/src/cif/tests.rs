use super::Document;
use crate::{StructureLevel, StructureOptions, batch_builder, mmcif, schema};

#[test]
fn views_borrow_owned_input_and_document_moves_between_threads() {
    let document = {
        let temporary = b"data_Original\n_a 'preserved'\n".to_vec();
        Document::parse(&temporary).unwrap()
    };
    std::thread::spawn(move || {
        let block = document.block(0).unwrap();
        assert_eq!(block.name, "Original");
        assert_eq!(block.columns["_a"], [Some("preserved")]);
    })
    .join()
    .unwrap();
}

#[test]
fn errors_report_block_and_position_and_utf8_is_deferred_per_block() {
    let error = Document::parse(b"data_Context\nloop_\n_a\n_b\n1\n")
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("Context"), "{error}");
    assert!(
        error.contains("line 2") && error.contains("byte 13"),
        "{error}"
    );
    let doc = Document::parse(b"data_first\n_a ok\ndata_second\n_b '\xff'\n").unwrap();
    assert_eq!(doc.block(0).unwrap().columns["_a"], [Some("ok")]);
    let error = doc.block(1).err().unwrap().to_string();
    assert!(
        error.contains("second") && error.contains("_b") && error.contains("UTF-8"),
        "{error}"
    );
}

#[test]
fn atom_and_residue_batches_match_retained_mapping_exactly() {
    let real = include_bytes!("../../../../testing/data/structure/1ubq.cif").to_vec();
    let synthetic = b"data_labels\nloop_\n_atom_site.id\n_atom_site.auth_atom_id\n_atom_site.label_atom_id\n_atom_site.auth_comp_id\n_atom_site.label_comp_id\n_atom_site.auth_asym_id\n_atom_site.label_asym_id\n_atom_site.auth_seq_id\n_atom_site.label_seq_id\n_atom_site.label_alt_id\n_atom_site.pdbx_PDB_ins_code\n_atom_site.Cartn_x\n_atom_site.Cartn_y\n_atom_site.Cartn_z\n_atom_site.pdbx_PDB_model_num\n1 authN N MSE MSE Author Label X1 1 . '?' 0 0 0 2\n2 authCA CA MSE MSE Author Label X1 1 . '?' 1 0 0 2\n3 authC C MSE MSE Author Label X1 1 . '?' 1 1 0 2\n_entry.id 'after atoms'\n_struct_asym.id Label\n_struct_asym.entity_id 7\n_entity_poly.entity_id 7\n_entity_poly.type 'polypeptide(L)'\n_chem_comp.id MSE\n_chem_comp.mon_nstd_parent_comp_id MET\ndata_ignored\n_note.value nothing\n".to_vec();
    for data in [&real, &synthetic] {
        for level in [StructureLevel::Atom, StructureLevel::Residue] {
            let options = StructureOptions {
                level,
                ..Default::default()
            };
            let native = mmcif::parse_native_reference(data, &options).unwrap();
            let rust = mmcif::parse(data, &options).unwrap();
            assert_eq!(native.len(), rust.len());
            let schema = schema::schema(&options);
            let projection = (0..schema.fields().len()).collect::<Vec<_>>();
            for (reference, candidate) in native.iter().zip(&rust) {
                let expected =
                    batch_builder::build(reference, &options, schema.clone(), &projection).unwrap();
                let actual =
                    batch_builder::build(candidate, &options, schema.clone(), &projection).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[test]
fn syntax_mutations_and_truncations_never_panic() {
    let source = b"data_x\n_a 'quoted'\n_b\n;text\r\nmore\n;\nloop_\n_c\n_d\n1 2 . '?'\nsave_f\n_e hidden\nsave_\n";
    let inspect = |data: &[u8]| {
        if let Ok(document) = Document::parse(data) {
            for index in 0..document.block_count() {
                let _ = document.block(index);
            }
        }
    };
    for end in 0..=source.len() {
        inspect(&source[..end]);
    }
    for offset in 0..source.len() {
        for byte in [
            0, b'\r', b'\n', b' ', b'\'', b'"', b';', b'_', b'#', b'$', 255,
        ] {
            let mut data = source.to_vec();
            data[offset] = byte;
            inspect(&data);
        }
    }
    let mut state = 0x2d15_843au32;
    for length in 0..128 {
        for _ in 0..32 {
            let bytes = (0..length)
                .map(|_| {
                    state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                    (state >> 24) as u8
                })
                .collect::<Vec<_>>();
            inspect(&bytes);
        }
    }
}
