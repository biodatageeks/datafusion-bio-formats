//! Read-only DataFusion support for BGEN Layout 1 and Layout 2 genotype files.

#![warn(missing_docs)]

mod bgi;
mod buffers;
mod catalog;
mod decode;
mod filter;
mod header;
pub mod matrix;
mod physical_exec;
mod source;
mod table_provider;

pub use physical_exec::BgenExec;
pub use table_provider::{
    BGEN_SAMPLE_NAMES_SYNTHETIC_KEY, BgenOutputMode, BgenProbabilityLayout, BgenReadOptions,
    BgenTableProvider, StaleBgiPolicy,
};
