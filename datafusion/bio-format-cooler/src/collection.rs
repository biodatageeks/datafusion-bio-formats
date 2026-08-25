//! Cooler data-collection discovery: URI parsing, `.cool` vs `.mcool`
//! detection, resolution selection, and metadata listing.

use std::collections::HashMap;

use datafusion::common::{DataFusionError, Result};
use hdf5_metno::types::TypeDescriptor;
use hdf5_metno::{File, Group};

use crate::hdf5_utils::{
    CoolerCollectionSum, attr_i64, attr_string, attr_sum, h5_err, read_numeric_1d,
    read_numeric_slice, read_string_dataset,
};

const INDEX_VALIDATION_BATCH_ROWS: usize = 65_536;

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
    /// Preserves integer totals exactly and fractional totals as floating point.
    pub sum: Option<CoolerCollectionSum>,
    pub nchroms: i64,
}

fn collection_info(file: &File, group_path: &str) -> Result<CoolerCollectionInfo> {
    let group = file
        .group(group_path)
        .map_err(|error| h5_err(&format!("Failed to open group '{group_path}'"), error))?;
    let chroms = group
        .group("chroms")
        .map_err(|error| h5_err("Failed to open chroms group", error))?;
    let name_shape = chroms
        .dataset("name")
        .map_err(|error| h5_err("Failed to open chroms/name", error))?
        .shape();
    if name_shape.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "chroms/name in '{group_path}' is not a 1-D dataset"
        )));
    }
    let nchroms = name_shape[0] as i64;
    Ok(CoolerCollectionInfo {
        group_path: group_path.to_string(),
        bin_size: attr_i64(&group, "bin-size")?,
        bin_type: attr_string(&group, "bin-type")?,
        format_version: attr_i64(&group, "format-version")?,
        assembly: attr_string(&group, "genome-assembly")?,
        nbins: attr_i64(&group, "nbins")?,
        nnz: attr_i64(&group, "nnz")?,
        sum: attr_sum(&group, "sum")?,
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

/// Arrow representation selected from the storage type of `pixels/count`.
/// Narrow signed/unsigned values safely widen to Int32; wider signed,
/// unsigned, and floating values retain a representation that cannot narrow
/// their stored range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CountType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float64,
}

/// The `bins` and `chroms` tables of a collection, loaded once per scan and
/// shared across partitions for the pixel → coordinate join.
#[derive(Debug)]
pub(crate) struct BinData {
    pub nbins: usize,
    pub chrom_names: Vec<String>,
    pub chrom_idx: Vec<usize>,
    pub start: Vec<u64>,
    pub end: Vec<u64>,
    pub weight: Option<Vec<f64>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BinDataProjection {
    pub chrom: bool,
    pub start: bool,
    pub end: bool,
    pub weight: bool,
}

impl BinDataProjection {
    pub fn any(self) -> bool {
        self.chrom || self.start || self.end || self.weight
    }

    pub fn cache_key(self) -> u8 {
        u8::from(self.chrom)
            | (u8::from(self.start) << 1)
            | (u8::from(self.end) << 2)
            | (u8::from(self.weight) << 3)
    }
}

pub(crate) fn load_bin_data(group: &Group, projection: BinDataProjection) -> Result<BinData> {
    let bins = group
        .group("bins")
        .map_err(|error| h5_err("Failed to open bins group", error))?;
    let open = |name: &str| {
        bins.dataset(name)
            .map_err(|error| h5_err(&format!("Failed to open bins/{name}"), error))
    };
    let chrom_dataset = open("chrom")?;
    let chrom_shape = chrom_dataset.shape();
    if chrom_shape.len() != 1 {
        return Err(DataFusionError::Plan(
            "bins/chrom is not a 1-D dataset".to_string(),
        ));
    }
    let nbins = chrom_shape[0];

    let (chrom_names, chrom_idx) = if projection.chrom {
        let chroms = group
            .group("chroms")
            .map_err(|error| h5_err("Failed to open chroms group", error))?;
        let chrom_names = read_string_dataset(
            &chroms
                .dataset("name")
                .map_err(|error| h5_err("Failed to open chroms/name", error))?,
            "chroms/name",
        )?;
        validate_chrom_names(&chrom_names)?;
        // bins/chrom is enum-typed (h5py categorical); soft conversion reads it as i32.
        let stored_chrom_idx = read_numeric_1d::<i32>(&chrom_dataset, "bins/chrom")?;
        let chrom_idx = validate_bin_chrom(chrom_names.len(), stored_chrom_idx, nbins)?;
        (chrom_names, chrom_idx)
    } else {
        (Vec::new(), Vec::new())
    };
    let read_coordinate = |name: &str| -> Result<Vec<u64>> {
        let path = format!("bins/{name}");
        let dataset = open(name)?;
        let descriptor = dataset
            .dtype()
            .and_then(|dtype| dtype.to_descriptor())
            .map_err(|error| h5_err(&format!("Failed to read {path} dtype"), error))?;
        match descriptor {
            TypeDescriptor::Integer(_) => read_numeric_1d::<i64>(&dataset, &path)?
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    u64::try_from(value).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "{path}[{index}]={value} is not a non-negative coordinate"
                        ))
                    })
                })
                .collect(),
            TypeDescriptor::Unsigned(_) => read_numeric_1d::<u64>(&dataset, &path),
            other => Err(DataFusionError::Plan(format!(
                "Unsupported {path} dtype: {other}"
            ))),
        }
    };
    let start = if projection.start {
        let values = read_coordinate("start")?;
        validate_bin_array_len("bins/start", values.len(), nbins)?;
        values
    } else {
        Vec::new()
    };
    let end = if projection.end {
        let values = read_coordinate("end")?;
        validate_bin_array_len("bins/end", values.len(), nbins)?;
        values
    } else {
        Vec::new()
    };
    let weight = if projection.weight {
        if !bins.link_exists("weight") {
            return Err(DataFusionError::Plan(
                "include_weights requested but this cooler has no bins/weight column (run `cooler balance` first)"
                    .to_string(),
            ));
        }
        let values = read_numeric_1d::<f64>(&open("weight")?, "bins/weight")?;
        validate_bin_array_len("bins/weight", values.len(), nbins)?;
        Some(values)
    } else {
        None
    };
    Ok(BinData {
        nbins,
        chrom_names,
        chrom_idx,
        start,
        end,
        weight,
    })
}

fn validate_bin_array_len(name: &str, length: usize, nbins: usize) -> Result<()> {
    if length != nbins {
        return Err(DataFusionError::Plan(format!(
            "{name} has {length} rows but bins/chrom has {nbins}"
        )));
    }
    Ok(())
}

fn validate_chrom_names(names: &[String]) -> Result<()> {
    let mut first_indexes = HashMap::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        if let Some(first_index) = first_indexes.insert(name.as_str(), index) {
            return Err(DataFusionError::Plan(format!(
                "chroms/name[{index}]='{name}' duplicates chroms/name[{first_index}]"
            )));
        }
    }
    Ok(())
}

fn validate_bin_chrom(
    nchroms: usize,
    stored_chrom_idx: Vec<i32>,
    nbins: usize,
) -> Result<Vec<usize>> {
    validate_bin_array_len("bins/chrom", stored_chrom_idx.len(), nbins)?;
    stored_chrom_idx
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            usize::try_from(value)
                .ok()
                .filter(|&value| value < nchroms)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "bins/chrom[{index}]={value} does not reference one of the {nchroms} chromosomes"
                    ))
                })
        })
        .collect()
}

/// The CSR-style pixel indexes of a collection: `chrom_offset` maps chrom →
/// first bin id, `bin1_offset` maps bin1 id → first pixel row.
#[derive(Debug)]
pub(crate) struct IndexData {
    pub chrom_offset: Vec<i64>,
    pub bin1_offset: Vec<i64>,
}

pub(crate) fn load_index_data(group: &Group) -> Result<IndexData> {
    let indexes = group
        .group("indexes")
        .map_err(|error| h5_err("Failed to open indexes group", error))?;
    let open = |name: &str| {
        indexes
            .dataset(name)
            .map_err(|error| h5_err(&format!("Failed to open indexes/{name}"), error))
    };
    let chrom_offset = read_numeric_1d::<i64>(&open("chrom_offset")?, "indexes/chrom_offset")?;
    let bin1_offset = read_numeric_1d::<i64>(&open("bin1_offset")?, "indexes/bin1_offset")?;
    let dataset_len = |group_name: &str, dataset_name: &str| -> Result<usize> {
        let path = format!("{group_name}/{dataset_name}");
        let dataset = group
            .group(group_name)
            .and_then(|group| group.dataset(dataset_name))
            .map_err(|error| h5_err(&format!("Failed to open {path}"), error))?;
        let shape = dataset.shape();
        if shape.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{path} is not a 1-D dataset"
            )));
        }
        Ok(shape[0])
    };
    let nchroms = dataset_len("chroms", "name")?;
    let nbins = dataset_len("bins", "chrom")?;
    let nnz = dataset_len("pixels", "count")?;
    validate_offsets("indexes/chrom_offset", &chrom_offset, nchroms + 1, nbins)?;
    validate_offsets("indexes/bin1_offset", &bin1_offset, nbins + 1, nnz)?;
    let bins = group
        .group("bins")
        .map_err(|error| h5_err("Failed to open bins group", error))?;
    let stored_bin_chrom = read_numeric_1d::<i32>(
        &bins
            .dataset("chrom")
            .map_err(|error| h5_err("Failed to open bins/chrom", error))?,
        "bins/chrom",
    )?;
    let bin_chrom = validate_bin_chrom(nchroms, stored_bin_chrom, nbins)?;
    validate_chrom_offsets_match_bins(&chrom_offset, &bin_chrom)?;
    let pixels = group
        .group("pixels")
        .map_err(|error| h5_err("Failed to open pixels group", error))?;
    let bin1_dataset = pixels
        .dataset("bin1_id")
        .map_err(|error| h5_err("Failed to open pixels/bin1_id", error))?;
    validate_bin1_offsets_match_pixels(&bin1_dataset, &bin1_offset, nnz)?;
    Ok(IndexData {
        chrom_offset,
        bin1_offset,
    })
}

fn validate_offsets(
    name: &str,
    offsets: &[i64],
    expected_len: usize,
    expected_last: usize,
) -> Result<()> {
    if offsets.len() != expected_len {
        return Err(DataFusionError::Plan(format!(
            "{name} has {} entries, expected {expected_len}",
            offsets.len()
        )));
    }
    if offsets.first() != Some(&0) {
        return Err(DataFusionError::Plan(format!("{name} must begin with 0")));
    }
    for (index, pair) in offsets.windows(2).enumerate() {
        if pair[0] < 0 || pair[1] < pair[0] {
            return Err(DataFusionError::Plan(format!(
                "{name} is invalid at entries {index} and {}: {} then {}",
                index + 1,
                pair[0],
                pair[1]
            )));
        }
    }
    if offsets
        .last()
        .and_then(|&value| usize::try_from(value).ok())
        != Some(expected_last)
    {
        return Err(DataFusionError::Plan(format!(
            "{name} must end at {expected_last}"
        )));
    }
    Ok(())
}

fn validate_chrom_offsets_match_bins(offsets: &[i64], bin_chrom: &[usize]) -> Result<()> {
    for (chrom_index, pair) in offsets.windows(2).enumerate() {
        let lo = usize::try_from(pair[0]).map_err(|_| {
            DataFusionError::Plan(format!(
                "indexes/chrom_offset[{chrom_index}]={} is not a valid bin boundary",
                pair[0]
            ))
        })?;
        let hi = usize::try_from(pair[1]).map_err(|_| {
            DataFusionError::Plan(format!(
                "indexes/chrom_offset[{}]={} is not a valid bin boundary",
                chrom_index + 1,
                pair[1]
            ))
        })?;
        for bin_index in lo..hi {
            let actual = bin_chrom.get(bin_index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "indexes/chrom_offset boundary {hi} exceeds the {} bins/chrom rows",
                    bin_chrom.len()
                ))
            })?;
            if *actual != chrom_index {
                return Err(DataFusionError::Plan(format!(
                    "indexes/chrom_offset assigns bins/chrom[{bin_index}]={actual} to chromosome {chrom_index}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_bin1_offsets_match_pixels(
    dataset: &hdf5_metno::Dataset,
    offsets: &[i64],
    nnz: usize,
) -> Result<()> {
    let mut expected_bin = 0;
    for lo in (0..nnz).step_by(INDEX_VALIDATION_BATCH_ROWS) {
        let hi = (lo + INDEX_VALIDATION_BATCH_ROWS).min(nnz);
        let values = read_numeric_slice::<i64>(dataset, lo, hi, "pixels/bin1_id")?;
        validate_bin1_assignment_block(offsets, lo, &values, &mut expected_bin)?;
    }
    Ok(())
}

fn validate_bin1_assignment_block(
    offsets: &[i64],
    row_start: usize,
    values: &[i64],
    expected_bin: &mut usize,
) -> Result<()> {
    for (index, &actual_bin) in values.iter().enumerate() {
        let row = row_start + index;
        while *expected_bin + 1 < offsets.len()
            && usize::try_from(offsets[*expected_bin + 1]).is_ok_and(|offset| offset <= row)
        {
            *expected_bin += 1;
        }
        if *expected_bin + 1 >= offsets.len() || actual_bin != *expected_bin as i64 {
            return Err(DataFusionError::Plan(format!(
                "indexes/bin1_offset assigns pixels/bin1_id[{row}]={actual_bin} to bin {}",
                *expected_bin
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod validation_tests {
    use super::{
        validate_bin_array_len, validate_bin_chrom, validate_bin1_assignment_block,
        validate_chrom_names, validate_chrom_offsets_match_bins, validate_offsets,
    };

    #[test]
    fn rejects_invalid_bin_chromosome_references() {
        let error = validate_bin_chrom(2, vec![0, -1], 2)
            .unwrap_err()
            .to_string();
        assert!(error.contains("bins/chrom[1]=-1"), "{error}");

        let error = validate_bin_chrom(2, vec![0, 2], 2)
            .unwrap_err()
            .to_string();
        assert!(error.contains("bins/chrom[1]=2"), "{error}");
    }

    #[test]
    fn rejects_duplicate_chromosome_names() {
        validate_chrom_names(&["chr1".to_string(), "chr2".to_string()]).unwrap();

        let error = validate_chrom_names(&["chr1".to_string(), "chr1".to_string()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("chroms/name[1]='chr1'"), "{error}");
        assert!(error.contains("chroms/name[0]"), "{error}");
    }

    #[test]
    fn rejects_mismatched_bin_array_lengths() {
        let error = validate_bin_array_len("bins/start", 1, 2)
            .unwrap_err()
            .to_string();
        assert!(error.contains("bins/start has 1 rows"), "{error}");
    }

    #[test]
    fn rejects_malformed_csr_offsets() {
        assert!(validate_offsets("index", &[0, 2, 1], 3, 1).is_err());
        assert!(validate_offsets("index", &[0, 1], 3, 1).is_err());
        assert!(validate_offsets("index", &[0, 1, 2], 3, 1).is_err());
    }

    #[test]
    fn rejects_chromosome_offsets_that_disagree_with_bin_assignments() {
        validate_chrom_offsets_match_bins(&[0, 1, 2], &[0, 1]).unwrap();
        validate_chrom_offsets_match_bins(&[0, 1, 1, 3], &[0, 2, 2]).unwrap();

        let error = validate_chrom_offsets_match_bins(&[0, 2, 2], &[0, 1])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bins/chrom[1]=1"), "{error}");

        let error = validate_chrom_offsets_match_bins(&[0, 0, 2], &[0, 1])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bins/chrom[0]=0"), "{error}");
    }

    #[test]
    fn rejects_bin1_offsets_that_disagree_with_pixel_assignments() {
        let mut expected_bin = 0;
        validate_bin1_assignment_block(&[0, 1, 2], 0, &[0, 1], &mut expected_bin).unwrap();

        let mut expected_bin = 0;
        let error = validate_bin1_assignment_block(&[0, 2, 2], 0, &[0, 1], &mut expected_bin)
            .unwrap_err()
            .to_string();
        assert!(error.contains("pixels/bin1_id[1]=1"), "{error}");

        let mut expected_bin = 0;
        validate_bin1_assignment_block(&[0, 1, 1, 3], 0, &[0, 2, 2], &mut expected_bin).unwrap();
    }
}
