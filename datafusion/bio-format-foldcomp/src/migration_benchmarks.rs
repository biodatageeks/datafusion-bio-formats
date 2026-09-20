//! Explicit legacy/candidate selection is confined to this ignored unit test.
#[path = "../../../testing/benchmarks/structure-codecs/harness.rs"]
mod harness;
use datafusion::common::Result;
use datafusion_bio_format_structure::{StructureOptions, model::NormalizedEntry};

fn native(data: &[u8], options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
    crate::codec::decode_native(data, options).map(|entry| vec![entry])
}
fn rust(data: &[u8], options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
    crate::fcz::decode(data, options).map(|entry| vec![entry])
}
#[test]
#[ignore = "run through testing/benchmarks/structure-codecs/run.py in release mode"]
fn worker() -> Result<()> {
    let config = harness::config();
    let decoder = match config["backend"].as_str().unwrap() {
        "native" => native,
        "rust" => rust,
        _ => panic!("invalid benchmark backend"),
    };
    harness::run(config, decoder, None)
}
