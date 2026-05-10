//! VCF Zarr support for Apache DataFusion.
//!
//! This module reads VCF Zarr 0.4 local filesystem stores and exposes them
//! through the same logical schema as the regular VCF table provider.

/// Zarr-backed Arrow array readers.
pub(crate) mod arrays;
/// Metadata loading and validation for VCF Zarr stores.
pub mod metadata;
/// Physical execution plan for VCF Zarr scans.
pub(crate) mod physical_exec;
/// Logical-to-physical array projection planning.
pub mod planning;
/// Predicate-derived row pruning for VCF Zarr scans.
pub(crate) mod pruning;
/// Record batch construction helpers.
pub(crate) mod record_batch;
/// Sample and FORMAT field resolution helpers.
pub(crate) mod samples;
/// Logical Arrow schema construction for VCF Zarr stores.
pub(crate) mod schema;
/// DataFusion table provider for VCF Zarr stores.
pub mod table_provider;

pub use table_provider::{VcfZarrReadOptions, VcfZarrTableProvider};
