//! DataFusion support for current variant-major PLINK 1 binary filesets.
//!
//! The provider treats BIM as the variant catalog, FAM as the sample catalog,
//! and BED as a fixed-width, variant-major genotype payload. It is read-only.

#![warn(missing_docs)]

mod fileset;
mod filter;
mod physical_exec;
mod table_provider;

pub use physical_exec::PlinkExec;
pub use table_provider::{
    PLINK_SAMPLE_IDENTITIES_KEY, PlinkReadOptions, PlinkTableProvider, SampleIdMode,
};
