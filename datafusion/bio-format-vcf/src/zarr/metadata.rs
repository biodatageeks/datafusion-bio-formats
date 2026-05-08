use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use zarrs::config::MetadataRetrieveVersion;

/// VCF Zarr version supported by this provider.
pub const SUPPORTED_VCF_ZARR_VERSION: &str = "0.4";

/// Minimal VCF Zarr metadata discovered from a local store.
#[derive(Clone, Debug)]
pub struct VcfZarrMetadata {
    /// Root directory of the local VCF Zarr store.
    pub root_path: PathBuf,
    /// The `vcf_zarr_version` declared by the store metadata.
    pub vcf_zarr_version: String,
    /// Root-level attributes parsed from the store `.zattrs` file.
    pub root_attributes: HashMap<String, Value>,
}

impl VcfZarrMetadata {
    /// Opens a local VCF Zarr store and validates provider-level metadata.
    pub fn open_local(path: &str) -> Result<Self> {
        let root_path = PathBuf::from(path);
        if !root_path.exists() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr store not found: {path}"
            )));
        }

        if !root_path.is_dir() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr path is not a directory store: {path}"
            )));
        }

        let store = Arc::new(zarrs::filesystem::FilesystemStore::new(&root_path).map_err(
            |error| {
                DataFusionError::Execution(format!(
                    "Failed to read VCF Zarr root metadata at {}: {error}",
                    root_path.display()
                ))
            },
        )?);
        let group = zarrs::group::Group::open_opt(store, "/", &MetadataRetrieveVersion::V2)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to read VCF Zarr root metadata at {}: {error}",
                    root_path.display()
                ))
            })?;

        let root_attributes: HashMap<String, Value> =
            group.attributes().clone().into_iter().collect();

        let attrs_path = root_path.join(".zattrs");
        let version = root_attributes
            .get("vcf_zarr_version")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "VCF Zarr root attribute 'vcf_zarr_version' is missing or not a string at {}",
                    attrs_path.display()
                ))
            })?
            .to_string();

        if version != SUPPORTED_VCF_ZARR_VERSION {
            return Err(DataFusionError::Execution(format!(
                "unsupported vcf_zarr_version '{version}' at {}; expected {SUPPORTED_VCF_ZARR_VERSION}",
                attrs_path.display()
            )));
        }

        Ok(Self {
            root_path,
            vcf_zarr_version: version,
            root_attributes,
        })
    }

    /// Returns true when the named array has a `.zarray` metadata file.
    pub fn array_exists(&self, name: &str) -> bool {
        let array_path = Path::new(name);
        let mut has_component = false;

        for component in array_path.components() {
            match component {
                Component::Normal(_) => has_component = true,
                _ => return false,
            }
        }

        has_component && self.root_path.join(array_path).join(".zarray").is_file()
    }
}
