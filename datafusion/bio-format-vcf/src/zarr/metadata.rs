use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use zarrs::array::Array;
use zarrs::config::MetadataRetrieveVersion;
use zarrs::filesystem::FilesystemStore;

/// VCF Zarr version supported by this provider.
pub const SUPPORTED_VCF_ZARR_VERSION: &str = "0.4";

/// Minimal VCF Zarr metadata discovered from a local store.
#[derive(Clone, Debug)]
pub struct VcfZarrMetadata {
    /// Root directory of the local VCF Zarr store.
    pub root_path: PathBuf,
    /// Shared local filesystem store used when opening arrays.
    store: Arc<FilesystemStore>,
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

        let store = Self::open_store(&root_path)?;
        let group = zarrs::group::Group::open_opt(store.clone(), "/", &MetadataRetrieveVersion::V2)
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
            store,
            vcf_zarr_version: version,
            root_attributes,
        })
    }

    /// Returns true when the named array has a `.zarray` metadata file.
    pub fn array_exists(&self, name: &str) -> bool {
        self.array_metadata_path(name)
            .is_ok_and(|path| path.is_file())
    }

    /// Opens a local array with zarrs V2 metadata retrieval.
    pub fn open_array(&self, name: &str) -> Result<Array<FilesystemStore>> {
        let array_path = self.array_node_path(name)?;

        Array::open_opt(
            self.store.clone(),
            &array_path,
            &MetadataRetrieveVersion::V2,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to read VCF Zarr array metadata for '{name}' at {}: {error}",
                self.root_path.display()
            ))
        })
    }

    fn open_store(root_path: &Path) -> Result<Arc<FilesystemStore>> {
        Ok(Arc::new(FilesystemStore::new(root_path).map_err(
            |error| {
                DataFusionError::Execution(format!(
                    "Failed to read VCF Zarr root metadata at {}: {error}",
                    root_path.display()
                ))
            },
        )?))
    }

    fn array_node_path(&self, name: &str) -> Result<String> {
        validate_array_name(name)?;
        Ok(format!("/{name}"))
    }

    fn array_metadata_path(&self, name: &str) -> Result<PathBuf> {
        validate_array_name(name)?;
        Ok(self.root_path.join(name).join(".zarray"))
    }
}

fn validate_array_name(name: &str) -> Result<()> {
    let array_path = Path::new(name);
    let mut has_component = false;

    for component in array_path.components() {
        match component {
            Component::Normal(_) => has_component = true,
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Invalid VCF Zarr array path '{name}'; array names must be store-relative"
                )));
            }
        }
    }

    if !has_component {
        return Err(DataFusionError::Execution(
            "Invalid empty VCF Zarr array path".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SUPPORTED_VCF_ZARR_VERSION, VcfZarrMetadata};

    fn write_v2_root_metadata(root: &std::path::Path, attributes: &str) {
        std::fs::create_dir_all(root).expect("store root should be created");
        std::fs::write(root.join(".zgroup"), "{\"zarr_format\":2}")
            .expect("root group metadata should be written");
        std::fs::write(root.join(".zattrs"), attributes)
            .expect("root attributes should be written");
    }

    #[test]
    fn array_exists_checks_store_relative_array_paths() {
        let fixture = "tests/data/vcf_zarr/multi_chrom.vcz";
        let metadata = VcfZarrMetadata::open_local(fixture).expect("fixture should open");
        let absolute_array_path = std::fs::canonicalize(format!("{fixture}/variant_position"))
            .expect("fixture array should canonicalize");

        assert!(metadata.array_exists("variant_position"));
        assert!(!metadata.array_exists("missing_array"));
        assert!(!metadata.array_exists(""));
        assert!(!metadata.array_exists("."));
        assert!(!metadata.array_exists("../multi_chrom.vcz/variant_position"));
        assert!(!metadata.array_exists(&absolute_array_path.to_string_lossy()));
    }

    #[test]
    fn array_exists_requires_zarray_file() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let root = temp_dir.path().join("store.vcz");
        let array = root.join("array");

        std::fs::create_dir_all(array.join(".zarray"))
            .expect("array metadata dir should be created");
        write_v2_root_metadata(
            &root,
            &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
        );

        let metadata =
            VcfZarrMetadata::open_local(root.to_str().expect("temp path should be UTF-8"))
                .expect("temp store should open");

        assert!(!metadata.array_exists("array"));
    }
}
