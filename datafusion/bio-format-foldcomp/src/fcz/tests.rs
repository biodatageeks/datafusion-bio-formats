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
    let golden: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../testing/oracles/structure-codecs/golden.json"
    ))
    .unwrap();
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
    let golden: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../testing/oracles/structure-codecs/golden.json"
    ))
    .unwrap();
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
    assert_eq!(value.to_bits(), 3_259_159_250);
    let separate = (93.0_f32 * (360.0_f32 / 255.0)) - 180.0;
    assert_eq!(separate.to_bits(), 3_259_159_248);
}
