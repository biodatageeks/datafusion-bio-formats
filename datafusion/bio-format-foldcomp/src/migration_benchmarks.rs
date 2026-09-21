//! Rust release worker; the benchmark driver builds the legacy worker separately.
#[path = "../../../testing/benchmarks/structure-codecs/harness.rs"]
mod harness;
use datafusion::common::Result;
use datafusion_bio_format_structure::{StructureOptions, model::NormalizedEntry};

fn rust(data: &[u8], options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
    crate::fcz::decode(data, options).map(|entry| vec![entry])
}
#[test]
#[ignore = "run through testing/benchmarks/structure-codecs/run.py in release mode"]
fn worker() -> Result<()> {
    let config = harness::config();
    assert_eq!(
        config["backend"], "rust",
        "use the pinned external baseline binary for native measurements"
    );
    harness::run(config, rust, None)
}
