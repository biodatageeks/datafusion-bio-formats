//! Cooler data-collection discovery: URI parsing, `.cool` vs `.mcool`
//! detection, resolution selection, and metadata listing.

use datafusion::common::{DataFusionError, Result};
use hdf5_metno::{File, Group};

use crate::hdf5_utils::{attr_i64, attr_string, h5_err, read_numeric_1d, read_string_dataset};

/// A parsed cooler URI: `path` or `path::/group/path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoolerUri {
    pub file_path: String,
    pub group_path: Option<String>,
}

impl CoolerUri {
    /// Split a cooler-style URI on the first `::` (e.g.
    /// `contacts.mcool::/resolutions/10000`).
    pub fn parse(uri: &str) -> Self {
        match uri.split_once("::") {
            Some((path, group)) if !group.is_empty() => CoolerUri {
                file_path: path.to_string(),
                group_path: Some(normalize_group_path(group)),
            },
            _ => CoolerUri {
                file_path: uri.to_string(),
                group_path: None,
            },
        }
    }
}

fn normalize_group_path(group: &str) -> String {
    let trimmed = group.trim_start_matches('/').trim_end_matches('/');
    format!("/{trimmed}")
}

/// Reject remote paths: HDF5 requires a local, seekable file in this version.
pub(crate) fn ensure_local_path(path: &str) -> Result<String> {
    if let Some(rest) = path.strip_prefix("file://") {
        if rest.starts_with('/') {
            return Ok(rest.to_string());
        }
        return Err(DataFusionError::NotImplemented(
            "Cooler only supports local filesystem paths in this version".to_string(),
        ));
    }
    if path.contains("://") {
        return Err(DataFusionError::NotImplemented(
            "Cooler only supports local filesystem paths in this version".to_string(),
        ));
    }
    Ok(path.to_string())
}

fn is_cooler_collection(group: &Group) -> bool {
    matches!(attr_string(group, "format"), Ok(Some(format)) if format.contains("Cooler"))
        || (group.link_exists("pixels") && group.link_exists("bins"))
}

/// List the resolutions stored under `/resolutions`, sorted numerically.
fn stored_resolutions(file: &File) -> Result<Vec<u64>> {
    let group = file
        .group("resolutions")
        .map_err(|error| h5_err("Failed to open /resolutions group", error))?;
    let mut resolutions = group
        .member_names()
        .map_err(|error| h5_err("Failed to list /resolutions members", error))?
        .iter()
        .filter_map(|name| name.parse::<u64>().ok())
        .collect::<Vec<_>>();
    resolutions.sort_unstable();
    Ok(resolutions)
}

/// Resolve which HDF5 group holds the requested data collection.
///
/// Handles: single-collection `.cool` files (root), `.mcool` files with a
/// `resolution` argument, cooler URIs (already split off into `group_path`),
/// and `.mcool` files with exactly one stored resolution.
pub fn resolve_collection_group(
    file: &File,
    group_path: Option<&str>,
    resolution: Option<u64>,
) -> Result<String> {
    if let Some(group_path) = group_path {
        if !file.link_exists(group_path.trim_start_matches('/')) && group_path != "/" {
            return Err(DataFusionError::Plan(format!(
                "Cooler group '{group_path}' does not exist in '{}'",
                file.filename()
            )));
        }
        if let Some(resolution) = resolution {
            let expected = format!("/resolutions/{resolution}");
            if group_path != expected {
                return Err(DataFusionError::Plan(format!(
                    "Conflicting cooler addressing: URI group '{group_path}' vs resolution={resolution}"
                )));
            }
        }
        return Ok(group_path.to_string());
    }

    let root = file
        .group("/")
        .map_err(|error| h5_err("Failed to open HDF5 root group", error))?;

    if is_cooler_collection(&root) {
        if let Some(resolution) = resolution {
            let bin_size = attr_i64(&root, "bin-size")?;
            if bin_size != Some(resolution as i64) {
                return Err(DataFusionError::Plan(format!(
                    "Requested resolution {resolution} but '{}' is a single-resolution cooler with bin size {}",
                    file.filename(),
                    bin_size.map_or("unknown".to_string(), |b| b.to_string())
                )));
            }
        }
        return Ok("/".to_string());
    }

    if file.link_exists("resolutions") {
        let available = stored_resolutions(file)?;
        return match resolution {
            Some(resolution) if available.contains(&resolution) => {
                Ok(format!("/resolutions/{resolution}"))
            }
            Some(resolution) => Err(DataFusionError::Plan(format!(
                "Resolution {resolution} not found in '{}'; available resolutions: {available:?}",
                file.filename()
            ))),
            None if available.len() == 1 => Ok(format!("/resolutions/{}", available[0])),
            None => Err(DataFusionError::Plan(format!(
                "'{}' is a multi-resolution cooler; pass a resolution (available: {available:?}) or use the '::/resolutions/N' URI syntax",
                file.filename()
            ))),
        };
    }

    Err(DataFusionError::Plan(format!(
        "'{}' is not a cooler file: no cooler data collection at the root and no /resolutions group",
        file.filename()
    )))
}

/// Metadata of one cooler data collection, read without touching `pixels`.
#[derive(Clone, Debug)]
pub struct CoolerCollectionInfo {
    pub group_path: String,
    pub bin_size: Option<i64>,
    pub bin_type: Option<String>,
    pub format_version: Option<i64>,
    pub assembly: Option<String>,
    pub nbins: Option<i64>,
    pub nnz: Option<i64>,
    pub sum: Option<i64>,
    pub nchroms: i64,
}

fn collection_info(file: &File, group_path: &str) -> Result<CoolerCollectionInfo> {
    let group = file
        .group(group_path)
        .map_err(|error| h5_err(&format!("Failed to open group '{group_path}'"), error))?;
    let chroms = group
        .group("chroms")
        .map_err(|error| h5_err("Failed to open chroms group", error))?;
    let nchroms = chroms
        .dataset("name")
        .map_err(|error| h5_err("Failed to open chroms/name", error))?
        .shape()[0] as i64;
    Ok(CoolerCollectionInfo {
        group_path: group_path.to_string(),
        bin_size: attr_i64(&group, "bin-size")?,
        bin_type: attr_string(&group, "bin-type")?,
        format_version: attr_i64(&group, "format-version")?,
        assembly: attr_string(&group, "genome-assembly")?,
        nbins: attr_i64(&group, "nbins")?,
        nnz: attr_i64(&group, "nnz")?,
        sum: attr_i64(&group, "sum")?,
        nchroms,
    })
}

/// List all data collections in a `.cool` or `.mcool` file, without scanning
/// pixel data. Backs `describe_cool`.
pub fn list_data_collections(path: &str) -> Result<Vec<CoolerCollectionInfo>> {
    let uri = CoolerUri::parse(path);
    let file_path = ensure_local_path(&uri.file_path)?;
    let file = File::open(&file_path)
        .map_err(|error| h5_err(&format!("Failed to open cooler file '{file_path}'"), error))?;
    if let Some(group_path) = uri.group_path.as_deref() {
        return Ok(vec![collection_info(&file, group_path)?]);
    }
    let root = file
        .group("/")
        .map_err(|error| h5_err("Failed to open HDF5 root group", error))?;
    if is_cooler_collection(&root) {
        return Ok(vec![collection_info(&file, "/")?]);
    }
    if file.link_exists("resolutions") {
        return stored_resolutions(&file)?
            .iter()
            .map(|resolution| collection_info(&file, &format!("/resolutions/{resolution}")))
            .collect();
    }
    Err(DataFusionError::Plan(format!(
        "'{file_path}' is not a cooler file: no cooler data collection at the root and no /resolutions group"
    )))
}

/// The `bins` and `chroms` tables of a collection, loaded once per scan and
/// shared across partitions for the pixel → coordinate join.
#[derive(Debug)]
pub(crate) struct BinData {
    pub chrom_names: Vec<String>,
    pub chrom_idx: Vec<i32>,
    pub start: Vec<i32>,
    pub end: Vec<i32>,
    pub weight: Option<Vec<f64>>,
}

pub(crate) fn load_bin_data(group: &Group, include_weights: bool) -> Result<BinData> {
    let chroms = group
        .group("chroms")
        .map_err(|error| h5_err("Failed to open chroms group", error))?;
    let chrom_names = read_string_dataset(
        &chroms
            .dataset("name")
            .map_err(|error| h5_err("Failed to open chroms/name", error))?,
        "chroms/name",
    )?;
    let bins = group
        .group("bins")
        .map_err(|error| h5_err("Failed to open bins group", error))?;
    let open = |name: &str| {
        bins.dataset(name)
            .map_err(|error| h5_err(&format!("Failed to open bins/{name}"), error))
    };
    // bins/chrom is enum-typed (h5py categorical); soft conversion reads it as i32.
    let chrom_idx = read_numeric_1d::<i32>(&open("chrom")?, "bins/chrom")?;
    let start = read_numeric_1d::<i32>(&open("start")?, "bins/start")?;
    let end = read_numeric_1d::<i32>(&open("end")?, "bins/end")?;
    let weight = if include_weights {
        if !bins.link_exists("weight") {
            return Err(DataFusionError::Plan(
                "include_weights requested but this cooler has no bins/weight column (run `cooler balance` first)"
                    .to_string(),
            ));
        }
        Some(read_numeric_1d::<f64>(&open("weight")?, "bins/weight")?)
    } else {
        None
    };
    Ok(BinData {
        chrom_names,
        chrom_idx,
        start,
        end,
        weight,
    })
}
