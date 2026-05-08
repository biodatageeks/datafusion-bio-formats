//! VCF Zarr support for Apache DataFusion.
//!
//! This module reads VCF Zarr 0.4 local filesystem stores and exposes them
//! through the same logical schema as the regular VCF table provider.

/// Metadata loading and validation for VCF Zarr stores.
pub mod metadata;
/// DataFusion table provider for VCF Zarr stores.
pub mod table_provider;

pub use table_provider::{VcfZarrReadOptions, VcfZarrTableProvider};
