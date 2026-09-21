//! Rust release worker; the benchmark driver builds the legacy worker separately.
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
#[test]
#[ignore = "run through testing/benchmarks/structure-codecs/run.py in release mode"]
fn worker() -> Result<()> {
    let config = harness::config();
    assert_eq!(
        config["backend"], "rust",
        "use the pinned external baseline binary for native measurements"
    );
    harness::run(config, mmcif::parse, Some(raw_rust))
}
