use datafusion_bio_format_structure::{
    AltlocSelection, ModelSelection, StructureOptions,
    model::{Atom, NormalizedEntry},
    residue::{residues, selected_atoms},
};
fn atom(res: u64, name: &str, alt: Option<&str>, point: [f64; 3]) -> Atom {
    Atom {
        atom_index: res * 10
            + match name {
                "N" => 0,
                "CA" => 1,
                "C" => 2,
                _ => 3,
            },
        model_id: 1,
        auth_asym_id: Some("".into()),
        auth_seq_id: Some((res + 1).to_string()),
        atom_name: name.into(),
        residue_name: "ALA".into(),
        position: point,
        alt_id: alt.map(str::to_owned),
        occupancy: Some(1.),
        peptide: true,
        ..Default::default()
    }
}
fn entry(mut atoms: Vec<Atom>) -> NormalizedEntry {
    for (i, a) in atoms.iter_mut().enumerate() {
        a.atom_index = i as u64;
    }
    let mut e = NormalizedEntry {
        atoms,
        ..Default::default()
    };
    e.normalize(&StructureOptions::default()).unwrap();
    e
}
fn chain() -> NormalizedEntry {
    entry(vec![
        atom(0, "N", None, [0., 0., 0.]),
        atom(0, "CA", None, [1., 0., 0.]),
        atom(0, "C", None, [1., 1., 0.]),
        atom(1, "N", None, [1., 2., 1.]),
        atom(1, "CA", None, [2., 2., 1.]),
        atom(1, "C", None, [2., 3., 1.]),
    ])
}
#[test]
fn coherent_conformers_completeness_occupancy_ties_and_shared_atoms() {
    let e = entry(vec![
        atom(0, "N", None, [0., 0., 0.]),
        atom(0, "CA", Some("A"), [1., 0., 0.]),
        atom(0, "C", Some("B"), [1., 1., 0.]),
    ]);
    let opts = StructureOptions::default();
    let r = residues(&e, &opts);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].selected_alt_id.as_deref(), Some("A"));
    assert!(r[0].backbone[2].is_none());
    assert!(!r[0].backbone_complete);
    assert!(r[0].angles.iter().all(Option::is_none));
    assert_eq!(selected_atoms(&e, &opts).len(), 3);
    let b = residues(
        &e,
        &StructureOptions {
            altloc: AltlocSelection::Id("B".into()),
            ..opts
        },
    );
    assert!(b[0].backbone[1].is_none());
    assert!(b[0].backbone[2].is_some());
}
#[test]
fn label_gaps_author_gaps_ter_models_cutoff_and_alt_conflicts() {
    let opts = StructureOptions::default();
    let e = chain();
    assert!(residues(&e, &opts)[0].peptide_link_next);
    for mode in 0..6 {
        let mut e = chain();
        for a in &mut e.atoms[3..] {
            match mode {
                0 => a.auth_seq_id = Some("200".into()),
                1 => {
                    a.label_seq_id = Some(3);
                }
                2 => a.segment_index = 1,
                3 => a.model_id = 2,
                4 => a.position[2] += 10.,
                _ => a.alt_id = Some("B".into()),
            }
        }
        if mode == 1 {
            for a in &mut e.atoms[..3] {
                a.label_seq_id = Some(1);
            }
        }
        if mode == 5 {
            for a in &mut e.atoms[..3] {
                a.alt_id = Some("A".into());
            }
        }
        e.normalize(&opts).unwrap();
        let r = residues(&e, &opts);
        assert_eq!(r[0].peptide_link_next, mode == 0, "mode {mode}");
        assert_eq!(r[1].peptide_link_prev, mode == 0);
        if mode != 0 {
            assert!(r[0].angles[1].is_none());
            assert!(r[0].angles[2].is_none());
            assert!(r[1].angles[0].is_none());
        }
    }
    let r = residues(
        &e,
        &StructureOptions {
            max_peptide_bond: 1.0,
            ..opts
        },
    );
    assert!(!r[0].peptide_link_next);
}
#[test]
fn incomplete_retained_nonpeptides_and_modified_mapping() {
    let mut e = chain();
    e.atoms
        .retain(|a| !(a.auth_seq_id.as_deref() == Some("2") && a.atom_name == "CA"));
    let r = residues(&e, &StructureOptions::default());
    assert_eq!(r.len(), 2);
    assert!(r[1].angles[0].is_none());
    assert!(r[0].angles[2].is_none());
    let mut ligand = atom(0, "CA", None, [0., 0., 0.]);
    ligand.peptide = false;
    ligand.residue_name = "CA".into();
    let e = entry(vec![ligand]);
    assert!(residues(&e, &StructureOptions::default()).is_empty());
    let r = residues(
        &e,
        &StructureOptions {
            include_non_peptide: true,
            ..Default::default()
        },
    );
    assert_eq!(r.len(), 1);
    assert!(r[0].angles.iter().all(Option::is_none));
    let mut a = atom(0, "CA", None, [0., 0., 0.]);
    a.residue_name = "MSE".into();
    let r = residues(&entry(vec![a]), &StructureOptions::default());
    assert_eq!(r[0].one_letter_code.as_deref(), Some("M"));
    assert_eq!(r[0].parent_residue_name.as_deref(), Some("MET"));
}
#[test]
fn model_ordinals_interleaved_rows_duplicate_sites_and_rigid_motion() {
    let e = chain();
    let want = residues(&e, &StructureOptions::default());
    let mut moved = e.clone();
    for a in &mut moved.atoms {
        let [x, y, z] = a.position;
        a.position = [-y + 11., x - 12., z + 7.];
    }
    let got = residues(&moved, &StructureOptions::default());
    for (a, b) in want.iter().zip(got) {
        for (x, y) in a.angles.iter().zip(b.angles) {
            assert_eq!(x.is_none(), y.is_none());
            if let (Some(x), Some(y)) = (x, y) {
                assert!((x - y).abs() < 1e-10);
            }
        }
    }
    let mut interleaved = e.clone();
    interleaved.atoms.sort_by_key(|a| a.atom_name.clone());
    interleaved.normalize(&StructureOptions::default()).unwrap();
    let r = residues(&interleaved, &StructureOptions::default());
    assert_eq!(r.len(), 2);
    assert!(r.iter().all(|r| r.backbone_complete));
    let mut dup = e.clone();
    dup.atoms.push(dup.atoms[0].clone());
    assert!(dup.normalize(&StructureOptions::default()).is_err());
    let mut models = e;
    for a in &mut models.atoms[3..] {
        a.model_id = 9;
    }
    models
        .normalize(&StructureOptions {
            model: ModelSelection::Id(9),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(models.atoms.len(), 3);
    assert_eq!(models.atoms[0].model_index, 1);
}
#[test]
fn peptide_neighbors_skip_interleaved_non_peptide_sites() {
    // A water listed between two bonded residues (no TER, same chain) must not sever the link.
    let mut atoms = chain();
    let mut water = atom(0, "O", None, [5., 5., 5.]);
    water.peptide = false;
    water.residue_name = "HOH".into();
    water.auth_seq_id = Some("300".into());
    atoms.atoms.insert(3, water);
    let e = entry(atoms.atoms);
    for include_non_peptide in [false, true] {
        let opts = StructureOptions {
            include_non_peptide,
            ..Default::default()
        };
        let r = residues(&e, &opts);
        let peptides: Vec<_> = r.iter().filter(|r| r.atom.peptide).collect();
        assert_eq!(r.len(), if include_non_peptide { 3 } else { 2 });
        assert_eq!(peptides.len(), 2);
        assert!(peptides[0].peptide_link_next, "{include_non_peptide}");
        assert!(peptides[1].peptide_link_prev);
        assert!(peptides[0].angles[1].is_some(), "psi needs N(i+1)");
        assert!(peptides[1].angles[0].is_some(), "phi needs C(i-1)");
        assert!(peptides[1].angles[5].is_some());
        if include_non_peptide {
            assert_eq!(r[1].residue_kind, "water");
            assert!(!r[1].peptide_link_prev && !r[1].peptide_link_next);
            assert!(r[1].angles.iter().all(Option::is_none));
        }
    }
    // A genuinely distant peptide pair across a ligand is still a break.
    let mut far = chain();
    for a in &mut far.atoms[3..] {
        a.position[2] += 10.;
    }
    let mut ligand = atom(0, "ZN", None, [9., 9., 9.]);
    ligand.peptide = false;
    ligand.residue_name = "ZN".into();
    ligand.auth_seq_id = Some("301".into());
    far.atoms.insert(3, ligand);
    let r = residues(&entry(far.atoms), &StructureOptions::default());
    assert_eq!(r.len(), 2);
    assert!(!r[0].peptide_link_next && !r[1].peptide_link_prev);
}
