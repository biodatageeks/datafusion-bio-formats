//! Protein atom/residue schemas, geometry, and lazy PDB/mmCIF table providers.
pub mod batch_builder;
#[cfg(feature = "text-formats")]
mod cif;
#[cfg(all(test, feature = "text-formats"))]
mod cif_contract_tests;
pub mod geometry;
#[cfg(feature = "text-formats")]
pub mod manifest;
#[cfg(feature = "text-formats")]
pub mod mmcif;
pub mod model;
pub mod options;
#[cfg(feature = "text-formats")]
pub mod pdb;
#[cfg(all(test, feature = "text-formats"))]
#[path = "../../../testing/oracles/structure-codecs/reference_cif.rs"]
mod reference_cif;
#[cfg(all(test, feature = "text-formats"))]
#[path = "../../../testing/oracles/structure-codecs/reference_process.rs"]
mod reference_process;
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

// Keep the shared provider regressions available in library-only checks too.
#[cfg(all(test, feature = "text-formats"))]
extern crate self as datafusion_bio_format_structure;
#[cfg(all(test, feature = "text-formats"))]
#[path = "../tests/structures.rs"]
mod rust_provider_tests;

#[cfg(all(test, feature = "text-formats"))]
mod migration_benchmarks;
