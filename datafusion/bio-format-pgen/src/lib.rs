//! Read-only DataFusion support for PLINK 2 PGEN/PVAR/PSAM filesets.

#![warn(missing_docs)]

mod decode;
mod fileset;
mod filter;
pub mod matrix;
mod physical_exec;
mod pvar;
mod selection;
mod source;
mod table_provider;

pub use physical_exec::PgenExec;
pub use table_provider::{PgenReadOptions, PgenTableProvider, PsamIdMode};
