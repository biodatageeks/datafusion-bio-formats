//! Repository-owned Rust FCZ decoding; no native bridge or runtime fallback.
use datafusion::common::Result;
use datafusion_bio_format_structure::{StructureOptions, model::NormalizedEntry};

/// Decode validated FCZ bytes directly into the common model; no PDB rounding step.
pub fn decode(data: &[u8], options: &StructureOptions) -> Result<NormalizedEntry> {
    crate::fcz::decode(data, options)
}

#[cfg(test)]
#[path = "codec_contract_tests.rs"]
mod contract_tests;
