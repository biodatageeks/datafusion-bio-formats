//! Cooler (.cool/.mcool) Hi-C contact matrix support for Apache DataFusion.
//!
//! Reads the pixels table of a cooler data collection through a statically
//! linked HDF5 library, exposing it either joined with bin coordinates
//! (`chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, `count`, optional
//! balancing weights) or as the raw COO triple (`bin1_id`, `bin2_id`,
//! `count`).

pub mod collection;
mod hdf5_utils;
pub mod physical_exec;
mod pruning;
pub mod table_provider;

pub use collection::{CoolerCollectionInfo, CoolerUri, list_data_collections};
pub use physical_exec::CoolerExec;
pub use table_provider::CoolerTableProvider;
