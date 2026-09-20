//! Frozen observations exercised against the retained and candidate parsers.
use crate::{cif, native_cif};
use datafusion::common::Result;
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
        check_case(name, &data, expected);
    }
}

#[test]
fn lexical_boundaries_match_pinned_contract() {
    let probes: Value = serde_json::from_str(include_str!(
        "../../../testing/oracles/structure-codecs/cif-probes.json"
    ))
    .unwrap();
    for case in probes["cases"].as_array().unwrap() {
        check_case(
            case["name"].as_str().unwrap(),
            &bytes(case["input_hex"].as_str().unwrap()),
            &case["expected"],
        );
    }
}

fn check_case(name: &str, data: &[u8], expected: &Value) {
    // Outer Result is syntax parsing; inner Result is block/UTF-8 exposure.
    type Snapshot = fn(&[u8]) -> Result<Result<Vec<Value>>>;
    for (backend, parse) in [
        ("native", native_snapshot as Snapshot),
        ("rust", rust_snapshot as Snapshot),
    ] {
        let parsed = parse(data);
        if expected["status"] == "error" && expected["stage"] == "parse" {
            assert!(
                parsed.is_err(),
                "{backend} {name}: accepted invalid CIF syntax"
            );
            continue;
        }
        let blocks = parsed.unwrap_or_else(|error| panic!("{backend} {name}: {error}"));
        if expected["status"] == "error" {
            assert!(blocks.is_err(), "{backend} {name}: exposed invalid UTF-8");
        } else {
            assert_eq!(
                json!(blocks.unwrap()),
                expected["blocks"],
                "{backend} {name}"
            );
        }
    }
}

fn native_snapshot(data: &[u8]) -> Result<Result<Vec<Value>>> {
    let document = native_cif::Document::parse(data)?;
    assert!(document.block(document.block_count()).is_err());
    Ok((0..document.block_count())
        .map(|index| {
            document
                .block(index)
                .map(|block| json!({"name": block.name, "columns": block.columns}))
        })
        .collect())
}

fn rust_snapshot(data: &[u8]) -> Result<Result<Vec<Value>>> {
    let document = cif::Document::parse(data)?;
    assert!(document.block(document.block_count()).is_err());
    Ok((0..document.block_count())
        .map(|index| {
            document
                .block(index)
                .map(|block| json!({"name": block.name, "columns": block.columns}))
        })
        .collect())
}
