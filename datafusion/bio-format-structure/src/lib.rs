//! Protein atom/residue schemas, geometry, and lazy PDB/mmCIF table providers.
pub mod batch_builder;
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
