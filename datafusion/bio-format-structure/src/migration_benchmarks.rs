//! Explicit legacy/candidate selection is confined to this ignored unit test.
#[path = "../../../testing/benchmarks/structure-codecs/harness.rs"]
mod harness;
use crate::mmcif;
use datafusion::common::Result;

fn raw_rust(data: &[u8]) -> Result<usize> {
    let document = crate::cif::Document::parse(data)?;
    let mut cells = 0;
    for index in 0..document.block_count() {
        cells += document
            .block(index)?
            .columns
            .values()
            .map(Vec::len)
            .sum::<usize>();
    }
    std::hint::black_box(document);
    Ok(cells)
}
fn raw_native(data: &[u8]) -> Result<usize> {
    let document = crate::native_cif::Document::parse(data)?;
    let mut cells = 0;
    for index in 0..document.block_count() {
        cells += document
            .block(index)?
            .columns
            .values()
            .map(Vec::len)
            .sum::<usize>();
    }
    std::hint::black_box(document);
    Ok(cells)
}
#[test]
#[ignore = "run through testing/benchmarks/structure-codecs/run.py in release mode"]
fn worker() -> Result<()> {
    let config = harness::config();
    let (decoder, raw): (harness::Decoder, harness::Raw) = match config["backend"].as_str().unwrap()
    {
        "native" => (mmcif::parse_native_reference, raw_native),
        "rust" => (mmcif::parse, raw_rust),
        _ => panic!("invalid benchmark backend"),
    };
    harness::run(config, decoder, Some(raw))
}
