//! Offline numeric expectations frozen from each measured arithmetic profile.
//! Coordinates retain the original golden/tolerance; only separately rounded
//! restored parameters and B-factor bits differ for baseline non-ARM64 targets.
use serde_json::Value;
pub(crate) fn load() -> Vec<Value> {
    let mut cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../../testing/oracles/structure-codecs/golden.json"
    ))
    .unwrap();
    if !cfg!(target_arch = "aarch64") {
        let profile: Value = serde_json::from_str(include_str!(
            "../../../testing/oracles/structure-codecs/unfused-parameters.json"
        ))
        .unwrap();
        for row in profile["overrides"].as_array().unwrap() {
            let expected = &mut cases
                .iter_mut()
                .find(|case| case["name"] == row["name"])
                .unwrap()["expected"];
            for (field, values) in row["fields"].as_object().unwrap() {
                if field == "bfactor_bits" {
                    let atoms = expected["atoms"].as_array_mut().unwrap();
                    let values = values.as_array().unwrap();
                    assert_eq!(atoms.len(), values.len());
                    for (atom, value) in atoms.iter_mut().zip(values) {
                        atom[8] = value.clone();
                    }
                } else {
                    expected[field] = values.clone();
                }
            }
        }
    }
    cases
}
