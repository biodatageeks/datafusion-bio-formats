use std::path::Path;

use datafusion::common::{DataFusionError, Result};

/// VCF Zarr version supported by this provider.
pub const SUPPORTED_VCF_ZARR_VERSION: &str = "0.4";

/// Minimal VCF Zarr metadata discovered from a local store.
#[derive(Clone, Debug)]
pub struct VcfZarrMetadata {
    /// The `vcf_zarr_version` declared by the store metadata.
    pub vcf_zarr_version: String,
}

impl VcfZarrMetadata {
    /// Opens a local VCF Zarr store and validates provider-level metadata.
    pub fn open_local(path: &str) -> Result<Self> {
        if !Path::new(path).exists() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr store not found: {path}"
            )));
        }

        Err(DataFusionError::NotImplemented(format!(
            "VCF Zarr metadata reader must validate vcf_zarr_version against supported version {SUPPORTED_VCF_ZARR_VERSION}"
        )))
    }
}
