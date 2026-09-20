//! Offline full-array/identity checks captured by the separate pinned oracle.
use super::decode_native;
use datafusion_bio_format_structure::StructureOptions;
use serde_json::Value;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn string(value: &Value) -> String {
    String::from_utf8(bytes(value.as_str().unwrap())).unwrap()
}

fn float(value: &Value) -> f64 {
    f64::from(f32::from_bits(value.as_u64().unwrap().try_into().unwrap()))
}

#[test]
fn native_fcz_matches_pinned_atoms_and_rejections() {
    check_contract(decode_native);
}

#[test]
fn rust_fcz_matches_pinned_atoms_and_rejections() {
    check_contract(crate::fcz::decode);
}

fn check_contract(
    decode: fn(
        &[u8],
        &StructureOptions,
    ) -> datafusion::common::Result<
        datafusion_bio_format_structure::model::NormalizedEntry,
    >,
) {
    let inputs: Vec<Value> = serde_json::from_str(include_str!(
        "../../../testing/oracles/structure-codecs/inputs.json"
    ))
    .unwrap();
    let golden: Vec<Value> = serde_json::from_str(include_str!(
        "../../../testing/oracles/structure-codecs/golden.json"
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
        include_bytes!("../../../testing/data/structure/1ubq.fcz").to_vec(),
        5_000_000,
    ));
    assert_eq!(cases.len(), 275);
    for (name, data, max_atoms) in cases {
        let expected = &golden.iter().find(|case| case["name"] == name).unwrap()["expected"];
        let result = decode(
            &data,
            &StructureOptions {
                max_atoms,
                ..Default::default()
            },
        );
        if expected["status"] == "error" {
            assert!(result.is_err(), "{name}: accepted invalid FCZ");
            continue;
        }
        let entry = result.unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            entry.entry_id,
            Some(string(&expected["decoded_title_hex"])),
            "{name}"
        );
        assert_eq!(entry.source_format, "foldcomp", "{name}");
        let atoms = expected["atoms"].as_array().unwrap();
        assert_eq!(entry.atoms.len(), atoms.len(), "{name}");
        for (index, (atom, row)) in entry.atoms.iter().zip(atoms).enumerate() {
            let atom_name = string(&row[0]);
            let residue_name = string(&row[1]);
            assert_eq!(atom.atom_index, index as u64, "{name}");
            assert_eq!(atom.atom_name, atom_name, "{name} atom {index}");
            assert_eq!(atom.residue_name, residue_name, "{name} atom {index}");
            assert_eq!(
                atom.auth_atom_id.as_deref(),
                Some(atom_name.as_str()),
                "{name}"
            );
            assert_eq!(
                atom.auth_comp_id.as_deref(),
                Some(residue_name.as_str()),
                "{name}"
            );
            assert_eq!(atom.auth_asym_id, Some(string(&row[2])), "{name}");
            assert_eq!(atom.atom_id, Some(row[3].to_string()), "{name}");
            assert_eq!(atom.auth_seq_id, Some(row[4].to_string()), "{name}");
            assert_eq!(atom.record_type, "ATOM", "{name}");
            assert_eq!(atom.model_id, 1, "{name}");
            assert!(atom.peptide, "{name}");
            for (axis, coordinate) in atom.position.iter().enumerate() {
                assert!(
                    (coordinate - float(&row[5 + axis])).abs() <= 1e-4,
                    "{name} atom {index} axis {axis}: {coordinate} vs {}",
                    float(&row[5 + axis])
                );
            }
            // Freeze exact B-factor bits on the baseline host. Other-platform
            // characterization must investigate drift, not widen this silently.
            assert_eq!(atom.b_factor, Some(float(&row[8])), "{name} atom {index}");
        }
    }
}
