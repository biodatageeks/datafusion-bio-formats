use super::{discretize::Discretizer, header::EncodedEntry, residue::Residue};
use serde_json::{Value, json};

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn snapshot(entry: &EncodedEntry<'_>) -> Value {
    let h = entry.header();
    let parameters = entry
        .backbone()
        .iter()
        .map(|record| record.parameters(&h.discretizers))
        .collect::<Vec<_>>();
    let torsions = parameters[..parameters.len() - 1]
        .iter()
        .flat_map(|values| [values[1], values[2], values[0]])
        .map(f32::to_bits)
        .collect::<Vec<_>>();
    let bond_angles = parameters
        .iter()
        .flat_map(|values| [values[4], values[5], values[3]])
        .map(f32::to_bits)
        .collect::<Vec<_>>();
    let mut offset = 0;
    let sidechain_angles = entry
        .backbone()
        .iter()
        .map(|record| {
            let count = record.residue.sidechain_count();
            let angles = entry.sidechain()[offset..offset + count]
                .iter()
                .map(|&code| Discretizer::sidechain().restore(u16::from(code)).to_bits())
                .collect::<Vec<_>>();
            offset += count;
            angles
        })
        .collect::<Vec<_>>();
    json!({
        "header": [u32::from(h.residue_count), u32::from(h.atom_count), u32::from(h.first_residue),
            u32::from(h.first_atom), u32::from(h.anchor_count), u32::from(h.chain), h.sidechain_count,
            u32::from(h.first_letter), u32::from(h.last_letter), h.title_length],
        "mins_bits": h.discretizers.map(|d| d.minimum.to_bits()),
        "factors_bits": h.discretizers.map(|d| d.factor.to_bits()),
        "anchors": entry.anchors().iter().map(|anchor| anchor.residue).collect::<Vec<_>>(),
        "anchor_coordinates_bits": entry.anchors().iter().flat_map(|anchor| anchor.coordinates.map(|p| p.map(f32::to_bits))).collect::<Vec<_>>(),
        "title_hex": hex(entry.title_bytes()),
        "decoded_title_hex": hex(entry.title().unwrap().as_bytes()),
        "has_oxt": u8::from(entry.has_oxt()),
        "oxt_bits": entry.oxt().map(f32::to_bits),
        "backbone": entry.backbone().iter().map(|record| {
            let mut row = vec![u16::from(record.residue.code())];
            row.extend_from_slice(&record.angles); row
        }).collect::<Vec<_>>(),
        "sidechain": entry.sidechain(),
        "bfactor_discretizer_bits": [entry.bfactor_discretizer().minimum.to_bits(), entry.bfactor_discretizer().factor.to_bits()],
        "bfactor_codes": entry.bfactors(),
        "backbone_parameter_bits": parameters.iter().map(|row| row.map(f32::to_bits)).collect::<Vec<_>>(),
        "torsion_bits": torsions,
        "bond_angle_bits": bond_angles,
        "sidechain_angle_bits": sidechain_angles,
    })
}

#[test]
fn packed_fields_and_restored_parameters_match_reference() {
    let inputs: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../testing/oracles/structure-codecs/inputs.json"
    ))
    .unwrap();
    let golden = crate::codec_goldens::load();
    let mut cases = inputs
        .iter()
        .filter(|case| case["mode"] == "fcz")
        .map(|case| {
            (
                case["name"].as_str().unwrap(),
                bytes(case["input_hex"].as_str().unwrap()),
                usize::try_from(case["max_atoms"].as_u64().unwrap()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    cases.push((
        "1ubq",
        include_bytes!("../../../../testing/data/structure/1ubq.fcz").to_vec(),
        5_000_000,
    ));
    assert_eq!(cases.len(), 275);
    for (name, data, max_atoms) in cases {
        let expected = &golden.iter().find(|case| case["name"] == name).unwrap()["expected"];
        let result = EncodedEntry::parse(&data, max_atoms);
        if expected["status"] == "error" && expected["stage"] == "parse" {
            assert!(result.is_err(), "{name}: invalid encoded input accepted");
            continue;
        }
        let entry = result.unwrap_or_else(|e| panic!("{name}: {e}"));
        if expected["status"] == "error" {
            assert!(
                entry.title().is_err() || entry.chain().is_err(),
                "{name}: invalid UTF-8 accepted"
            );
            continue;
        }
        for (key, value) in snapshot(&entry).as_object().unwrap() {
            assert_eq!(value, &expected[key], "{name}: {key}");
        }
        let atoms = expected["atoms"].as_array().unwrap();
        assert_eq!(entry.reconstructed_atoms(), atoms.len(), "{name}");
        assert_eq!(
            hex(entry.chain().unwrap().as_bytes()),
            atoms[0][2],
            "{name}"
        );
        let mut atom_offset = 0;
        for (record, &bfactor) in entry.backbone().iter().zip(entry.bfactors()) {
            for atom in &atoms[atom_offset..atom_offset + record.residue.atom_count()] {
                assert_eq!(hex(record.residue.name().as_bytes()), atom[1], "{name}");
                assert_eq!(
                    u64::from(
                        entry
                            .bfactor_discretizer()
                            .restore(u16::from(bfactor))
                            .to_bits()
                    ),
                    atom[8].as_u64().unwrap(),
                    "{name}: B-factor"
                );
            }
            atom_offset += record.residue.atom_count();
        }
    }
}

#[test]
fn all_residue_codes_match_reference_tables() {
    let tables: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../testing/oracles/structure-codecs/residue-codes.json"
    ))
    .unwrap();
    for code in 0..=255 {
        let result = Residue::from_code(code);
        if let Some(table) = tables
            .get(usize::from(code))
            .filter(|table| table["supported"] == true)
        {
            let residue = result.unwrap();
            assert_eq!(hex(residue.name().as_bytes()), table["name_hex"]);
            assert_eq!(
                u64::from(residue.letter()),
                table["letter"].as_u64().unwrap()
            );
            assert_eq!(
                residue.atom_count() as u64,
                table["atom_count"].as_u64().unwrap()
            );
            assert_eq!(
                residue.sidechain_count() as u64,
                table["sidechain_count"].as_u64().unwrap()
            );
        } else {
            assert!(result.is_err(), "code {code}");
        }
    }
}

#[test]
fn official_database_entries_match_counts_and_anchors() {
    let golden = crate::codec_goldens::load();
    let database = include_bytes!("../../../../testing/data/structure/example_db");
    let index = include_str!("../../../../testing/data/structure/example_db.index");
    for line in index.lines() {
        let fields = line
            .split_whitespace()
            .map(|s| s.parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        let [key, offset, size] = fields[..] else {
            panic!("invalid database index");
        };
        let name = format!("database_{key}");
        let expected = &golden.iter().find(|case| case["name"] == name).unwrap()["expected"];
        let entry = EncodedEntry::parse(&database[offset..offset + size - 1], 5_000_000).unwrap();
        assert_eq!(
            json!(entry.reconstructed_atoms()),
            expected["atom_count"],
            "{name}"
        );
        assert_eq!(snapshot(&entry)["header"], expected["header"], "{name}");
        assert_eq!(snapshot(&entry)["anchors"], expected["anchors"], "{name}");
    }
}

#[test]
fn truncated_and_mutated_headers_never_panic_or_allocate_from_unchecked_sizes() {
    let valid = include_bytes!("../../../../testing/data/structure/1ubq.fcz");
    for size in 0..valid.len() {
        assert!(EncodedEntry::parse(&valid[..size], 5_000_000).is_err());
    }
    for offset in 0..76 {
        for byte in [0, 1, 127, 128, 254, 255] {
            let mut data = valid.to_vec();
            data[offset] = byte;
            let _ = EncodedEntry::parse(&data, 5_000_000);
        }
    }
    assert!(EncodedEntry::parse(valid, 0).is_err());
    let mut trailing = valid.to_vec();
    trailing.push(0);
    assert!(EncodedEntry::parse(&trailing, usize::MAX).is_err());
}

#[test]
fn failed_reads_do_not_wrap_offsets_or_advance_the_cursor() {
    let mut reader = super::bitstream::Reader::new(b"1234");
    assert_eq!(reader.byte().unwrap(), b'1');
    assert!(reader.take(usize::MAX).is_err());
    assert!(reader.take(4).is_err());
    assert_eq!(reader.take(3).unwrap(), b"234");
    assert!(reader.byte().is_err());
}

#[test]
fn inverse_discretization_reproduces_measured_reference_contraction() {
    // Code 93 is the first small fixture where separate rounding differs.
    // The expected bits come from the pinned native sidechain-angle array.
    let value = Discretizer::sidechain().restore(93);
    assert_eq!(
        value.to_bits(),
        if cfg!(target_arch = "aarch64") {
            3_259_159_250
        } else {
            3_259_159_248
        }
    );
    let separate = (93.0_f32 * (360.0_f32 / 255.0)) - 180.0;
    assert_eq!(separate.to_bits(), 3_259_159_248);
}

#[test]
#[ignore = "requires separately built pinned reference; run check_provider_reference.py"]
fn external_reference_reconstructed_database_atoms_match() {
    use datafusion_bio_format_structure::StructureOptions;
    let database = include_bytes!("../../../../testing/data/structure/example_db");
    let index = include_str!("../../../../testing/data/structure/example_db.index");
    let options = StructureOptions::default();
    let mut errors = Vec::new();
    let mut worst = (0.0, 0, 0, 0);
    for line in index.lines() {
        let fields = line
            .split_whitespace()
            .map(|s| s.parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        let [key, offset, size] = fields[..] else {
            panic!("invalid index")
        };
        let data = &database[offset..offset + size - 1];
        let expected = crate::reference_foldcomp::decode(data, &options).unwrap();
        let actual = super::decode(data, &options).unwrap();
        assert_eq!(actual.entry_id, expected.entry_id, "key {key}");
        assert_eq!(actual.atoms.len(), expected.atoms.len(), "key {key}");
        for (actual, expected) in
            datafusion_bio_format_structure::residue::residues(&actual, &options)
                .iter()
                .zip(datafusion_bio_format_structure::residue::residues(
                    &expected, &options,
                ))
        {
            for (a, b) in actual.angles.iter().zip(expected.angles) {
                assert_eq!(a.is_none(), b.is_none(), "key {key}: angle null mask");
                if let (Some(a), Some(b)) = (a, b) {
                    assert!(((a - b + 180.0).rem_euclid(360.0) - 180.0).abs() <= 0.01);
                }
            }
            assert_eq!(actual.peptide_link_prev, expected.peptide_link_prev);
            assert_eq!(actual.peptide_link_next, expected.peptide_link_next);
            assert_eq!(actual.backbone_complete, expected.backbone_complete);
            assert_eq!(actual.geometry_status, expected.geometry_status);
        }

        for (index, (actual, expected)) in actual.atoms.iter().zip(&expected.atoms).enumerate() {
            for axis in 0..3 {
                let delta = (actual.position[axis] - expected.position[axis]).abs();
                if delta > worst.0 {
                    worst = (delta, key, index, axis);
                }
                errors.push(delta);
            }
            // All identity, normalization, null and B-factor fields must match.
            let mut identity = actual.clone();
            identity.position = expected.position;
            assert_eq!(
                format!("{identity:?}"),
                format!("{expected:?}"),
                "key {key} atom {index}"
            );
        }
    }
    errors.sort_by(f64::total_cmp);
    eprintln!(
        "{} components: p50={} p95={} p99={}, worst={worst:?}",
        errors.len(),
        errors[errors.len() / 2],
        errors[errors.len() * 95 / 100],
        errors[errors.len() * 99 / 100]
    );
    assert!(worst.0 <= 1e-4, "worst coordinate error: {worst:?}");
}

#[test]
fn residue_geometry_is_complete_and_only_references_preceding_atoms() {
    for code in 0..=31 {
        let Ok(residue) = Residue::from_code(code) else {
            continue;
        };
        let table = super::tables::TABLES[usize::from(code).min(20)];
        assert_eq!(table.len(), residue.sidechain_count(), "code {code}");
        let mut names = std::collections::HashSet::from(["N", "CA", "C"]);
        for (index, atom) in table.iter().enumerate() {
            assert!(names.insert(atom.name), "code {code}: duplicate atom");
            assert!(atom.previous.iter().all(|&p| p < index + 3));
            assert!(atom.length.is_finite() && atom.length > 0.0);
            assert!(atom.angle.is_finite() && atom.angle > 0.0 && atom.angle < 180.0);
        }
    }
}

#[test]
fn reconstruction_rejects_degenerate_and_overflowing_geometry_without_panicking() {
    use datafusion_bio_format_structure::StructureOptions;
    let original = include_bytes!("../../../../testing/data/structure/1ubq.fcz");
    let options = StructureOptions {
        max_atoms: 10_000,
        ..Default::default()
    };
    let encoded = EncodedEntry::parse(original, options.max_atoms).unwrap();
    let coordinates = 76 + 4 * encoded.anchors().len() + encoded.title_bytes().len();
    // Identical and collinear N/CA/C anchors are finite but cannot define a frame.
    for points in [
        [[0.0f32; 3]; 3],
        [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]],
    ] {
        for (anchor_index, anchor) in encoded.anchors().iter().enumerate() {
            let mut data = original.to_vec();
            for (i, value) in points.into_iter().flatten().enumerate() {
                let offset = coordinates + anchor_index * 36 + i * 4;
                data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            }
            let error = super::decode(&data, &options).err().unwrap().to_string();
            assert!(error.contains(&format!("anchor {anchor_index}")), "{error}");
            assert!(
                error.contains(&format!("residue index {}", anchor.residue)),
                "{error}"
            );
        }
    }
    for offset in [
        28,
        52,
        coordinates,
        original.len() - encoded.bfactors().len() - 4,
    ] {
        let mut data = original.to_vec();
        data[offset..offset + 4].copy_from_slice(&f32::MAX.to_le_bytes());
        let rust = super::decode(&data, &options);
        // A large finite angular minimum may still give finite trigonometry;
        // a factor that overflows restored angles/B factors must be rejected.
        if offset == 52 || offset == original.len() - encoded.bfactors().len() - 4 {
            assert!(rust.is_err(), "overflow accepted at {offset}");
        }
    }
    // Deterministic mutations reach both checked sections and reconstruction.
    // This bounded regression run does not stand in for sustained fuzzing.
    let mut state = 0x734e_a927u32;
    for _ in 0..4096 {
        let mut data = original.to_vec();
        for _ in 0..3 {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            let offset = state as usize % data.len();
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            data[offset] = (state >> 24) as u8;
        }
        if let Ok(entry) = super::decode(&data, &options) {
            assert!(entry.atoms.len() <= options.max_atoms);
            for atom in entry.atoms {
                assert!(atom.position.into_iter().all(f64::is_finite));
                assert!(atom.b_factor.is_none_or(f64::is_finite));
            }
        }
    }
}

#[test]
#[ignore = "requires separately built pinned reference; run check_provider_reference.py"]
fn external_reference_long_chains_and_many_anchor_joins_match() {
    use datafusion_bio_format_structure::{StructureOptions, residue::residues};
    let options = StructureOptions::default();
    let cases: [(&str, &[u8], usize); 2] = [
        (
            "mixed_1040",
            include_bytes!(
                "../../../../testing/oracles/structure-codecs/fcz-stress/long_mixed.fcz"
            ),
            1040,
        ),
        (
            "single_segment_4096",
            include_bytes!(
                "../../../../testing/oracles/structure-codecs/fcz-stress/long_segment.fcz"
            ),
            4096,
        ),
    ];
    for (name, data, residue_count) in cases {
        let expected = crate::reference_foldcomp::decode(data, &options).unwrap();
        let actual = super::decode(data, &options).unwrap();
        assert_eq!(
            EncodedEntry::parse(data, options.max_atoms)
                .unwrap()
                .header()
                .residue_count as usize,
            residue_count
        );
        assert_eq!(actual.atoms.len(), expected.atoms.len());
        let mut errors = Vec::new();
        for (a, b) in actual.atoms.iter().zip(&expected.atoms) {
            errors.extend(
                a.position
                    .iter()
                    .zip(b.position)
                    .map(|(a, b)| (a - b).abs()),
            );
            let mut identity = a.clone();
            identity.position = b.position;
            assert_eq!(format!("{identity:?}"), format!("{b:?}"), "{name}");
        }
        errors.sort_by(f64::total_cmp);
        let worst = *errors.last().unwrap();
        eprintln!(
            "{name}: {} atoms, p50={} p95={} p99={} max={worst}",
            actual.atoms.len(),
            errors[errors.len() / 2],
            errors[errors.len() * 95 / 100],
            errors[errors.len() * 99 / 100]
        );
        assert!(worst <= 1e-4, "{name}: coordinate error {worst}");
        let actual = residues(&actual, &options);
        let expected = residues(&expected, &options);
        assert_eq!(actual.len(), expected.len());
        for (a, b) in actual.iter().zip(expected) {
            assert_eq!(a.peptide_link_prev, b.peptide_link_prev, "{name}");
            assert_eq!(a.peptide_link_next, b.peptide_link_next, "{name}");
            assert_eq!(a.geometry_status, b.geometry_status, "{name}");
            for (a, b) in a.angles.iter().zip(b.angles) {
                assert_eq!(a.is_none(), b.is_none(), "{name}");
                if let (Some(a), Some(b)) = (a, b) {
                    assert!(
                        ((a - b + 180.0).rem_euclid(360.0) - 180.0).abs() <= 0.01,
                        "{name}"
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "requires separately built pinned reference; run check_provider_reference.py"]
fn external_reference_degenerate_and_extreme_geometry() {
    use datafusion_bio_format_structure::StructureOptions;
    let original = include_bytes!("../../../../testing/data/structure/1ubq.fcz");
    let options = StructureOptions {
        max_atoms: 10_000,
        ..Default::default()
    };
    let encoded = EncodedEntry::parse(original, options.max_atoms).unwrap();
    let coordinates = 76 + 4 * encoded.anchors().len() + encoded.title_bytes().len();
    let mut cases = Vec::new();
    for points in [
        [[0.0f32; 3]; 3],
        [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]],
    ] {
        let mut data = original.to_vec();
        for (i, value) in points.into_iter().flatten().enumerate() {
            data[coordinates + i * 4..coordinates + i * 4 + 4]
                .copy_from_slice(&value.to_le_bytes());
        }
        cases.push(data);
    }
    for offset in [
        28,
        52,
        coordinates,
        original.len() - encoded.bfactors().len() - 4,
    ] {
        let mut data = original.to_vec();
        data[offset..offset + 4].copy_from_slice(&f32::MAX.to_le_bytes());
        cases.push(data);
    }
    for (index, data) in cases.iter().enumerate() {
        let expected = crate::reference_foldcomp::decode(data, &options);
        let actual = super::decode(data, &options);
        assert_eq!(actual.is_err(), expected.is_err(), "case {index}");
    }
}
