//! Backend-independent frozen observations. Move this module with `Document`
//! when the Rust parser replaces the native owner.
use super::Document;
use serde_json::{Value, json};

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn raw_cif_matches_pinned_contract() {
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
        .filter(|case| case["mode"] == "cif")
        .map(|case| {
            (
                case["name"].as_str().unwrap(),
                bytes(case["input_hex"].as_str().unwrap()),
            )
        })
        .collect::<Vec<_>>();
    cases.push((
        "1ubq_raw",
        include_bytes!("../../../testing/data/structure/1ubq.cif").to_vec(),
    ));
    assert_eq!(cases.len(), 56);
    for (name, data) in cases {
        let expected = &golden.iter().find(|case| case["name"] == name).unwrap()["expected"];
        let parsed = Document::parse(&data);
        if expected["status"] == "error" && expected["stage"] == "parse" {
            assert!(parsed.is_err(), "{name}: accepted invalid CIF syntax");
            continue;
        }
        let document = parsed.unwrap_or_else(|error| panic!("{name}: {error}"));
        let blocks = (0..document.block_count())
            .map(|index| {
                document
                    .block(index)
                    .map(|block| json!({"name": block.name, "columns": block.columns}))
            })
            .collect::<datafusion::common::Result<Vec<_>>>();
        if expected["status"] == "error" {
            assert!(blocks.is_err(), "{name}: exposed invalid UTF-8");
        } else {
            assert_eq!(json!(blocks.unwrap()), expected["blocks"], "{name}");
            assert!(document.block(document.block_count()).is_err(), "{name}");
        }
    }
}
