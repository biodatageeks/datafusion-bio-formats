//! Protein atom/residue schemas, geometry, and lazy PDB/mmCIF table providers.
pub mod batch_builder;
// Candidate backend stays test-only until the migration acceptance gates pass.
#[cfg(all(test, feature = "text-formats"))]
mod cif;
#[cfg(all(test, feature = "text-formats"))]
mod cif_contract_tests;
pub mod geometry;
#[cfg(feature = "text-formats")]
pub mod manifest;
#[cfg(feature = "text-formats")]
pub mod mmcif;
pub mod model;
#[cfg(feature = "text-formats")]
mod native_cif;
pub mod options;
#[cfg(feature = "text-formats")]
pub mod pdb;
pub mod residue;
pub mod schema;
#[cfg(feature = "text-formats")]
mod storage;
pub mod table_provider;
use datafusion::common::DataFusionError;
pub use options::{AltlocSelection, ModelSelection, StructureLevel, StructureOptions};
pub use table_provider::{EntrySource, StructureTableProvider};
pub fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

// Exercise the shared integration suite with Rust Blocks in unit-test builds.
#[cfg(all(test, feature = "text-formats"))]
extern crate self as datafusion_bio_format_structure;
#[cfg(all(test, feature = "text-formats"))]
#[path = "../tests/structures.rs"]
mod rust_provider_tests;

#[cfg(all(test, feature = "text-formats"))]
mod migration_benchmarks;
