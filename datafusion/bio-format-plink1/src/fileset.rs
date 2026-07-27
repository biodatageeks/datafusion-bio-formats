use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::{CompanionRule, resolve_companion, sanitize_location};
use datafusion_bio_format_core::genotype::{CoordinateSystem, SampleSelection};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_storage_type,
};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::table_provider::{PlinkReadOptions, SampleIdMode};

pub(crate) const BED_HEADER_LEN: u64 = 3;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FamIdentity {
    pub(crate) fid: String,
    pub(crate) iid: String,
}

#[derive(Clone, Debug)]
pub(crate) struct BimVariant {
    pub(crate) chrom: String,
    pub(crate) id: Option<String>,
    pub(crate) cm: f64,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) a1: String,
    pub(crate) a2: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PlinkFileset {
    pub(crate) bed_path: String,
    pub(crate) bim_path: String,
    pub(crate) fam_path: String,
    pub(crate) variants: Arc<Vec<BimVariant>>,
    pub(crate) selected_samples: SampleSelection,
    pub(crate) selected_identities: Arc<Vec<FamIdentity>>,
    pub(crate) sample_count: usize,
    pub(crate) bytes_per_variant: u64,
    pub(crate) object_storage_options: ObjectStorageOptions,
    pub(crate) companion_bytes: u64,
}

pub(crate) enum PlinkRangeReader {
    Local(tokio::fs::File),
    Remote(RemoteObject),
}

impl PlinkRangeReader {
    pub(crate) async fn open(path: &str, options: &ObjectStorageOptions) -> Result<Self> {
        match get_storage_type(path.to_string()) {
            StorageType::LOCAL => tokio::fs::File::open(local_path(path)?)
                .await
                .map(Self::Local)
                .map_err(|error| io_error("open", path, error)),
            _ => RemoteObject::open(path.to_string(), options.clone())
                .await
                .map(Self::Remote)
                .map_err(|error| external_error("open", path, error)),
        }
    }

    pub(crate) async fn read_range(
        &mut self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        let expected = usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
            DataFusionError::Execution(
                "PLINK byte range does not fit memory address space".to_string(),
            )
        })?;
        match self {
            Self::Local(file) => {
                file.seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .map_err(|error| io_error("seek", path, error))?;
                let mut bytes = vec![0; expected];
                file.read_exact(&mut bytes)
                    .await
                    .map_err(|error| io_error("read", path, error))?;
                Ok(Bytes::from(bytes))
            }
            Self::Remote(object) => object
                .read_range(range)
                .await
                .map_err(|error| external_error("read", path, error)),
        }
    }
}

impl PlinkFileset {
    pub(crate) async fn open(bed_path: String, options: &PlinkReadOptions) -> Result<Self> {
        let storage_options = options.object_storage_options.clone().unwrap_or_default();
        let bim_path = resolve_member(
            &bed_path,
            "BIM",
            options.bim_path.as_deref(),
            "bim",
            &storage_options,
        )
        .await?;
        let fam_path = resolve_member(
            &bed_path,
            "FAM",
            options.fam_path.as_deref(),
            "fam",
            &storage_options,
        )
        .await?;

        let bim_bytes =
            read_bounded(&bim_path, options.max_companion_bytes, &storage_options).await?;
        let fam_bytes =
            read_bounded(&fam_path, options.max_companion_bytes, &storage_options).await?;
        let identities = parse_fam(&fam_path, &fam_bytes, options.sample_id_mode)?;
        if identities.len() > options.max_samples {
            return Err(DataFusionError::Plan(format!(
                "FAM sample count {} exceeds configured max_samples {}",
                identities.len(),
                options.max_samples
            )));
        }
        let source_names = sample_names(&identities, options.sample_id_mode)?;
        let selected_samples = datafusion_bio_format_core::genotype::resolve_samples(
            &source_names,
            options.samples.as_deref(),
            options.missing_sample_policy,
        )?;
        let selected_identities = selected_samples
            .source_indices()
            .iter()
            .map(|&index| identities[index].clone())
            .collect();

        let variants = parse_bim(&bim_path, &bim_bytes, options.coordinate_system)?;
        if variants.len() > options.max_variants {
            return Err(DataFusionError::Plan(format!(
                "BIM variant count {} exceeds configured max_variants {}",
                variants.len(),
                options.max_variants
            )));
        }

        let sample_count = identities.len();
        let sample_count_u64 = u64::try_from(sample_count)
            .map_err(|_| DataFusionError::Plan("FAM sample count does not fit u64".to_string()))?;
        let bytes_per_variant = sample_count_u64.checked_add(3).ok_or_else(|| {
            DataFusionError::Plan("PLINK sample count arithmetic overflowed".to_string())
        })? / 4;
        let variant_count = u64::try_from(variants.len())
            .map_err(|_| DataFusionError::Plan("BIM variant count does not fit u64".to_string()))?;
        let expected_size = expected_bed_size(variant_count, sample_count_u64)?;

        validate_bed(&bed_path, expected_size, &storage_options).await?;

        Ok(Self {
            bed_path,
            bim_path,
            fam_path,
            variants: Arc::new(variants),
            selected_samples,
            selected_identities: Arc::new(selected_identities),
            sample_count,
            bytes_per_variant,
            object_storage_options: storage_options,
            companion_bytes: (bim_bytes.len() + fam_bytes.len()) as u64,
        })
    }
}

fn expected_bed_size(variant_count: u64, sample_count: u64) -> Result<u64> {
    let bytes_per_variant = sample_count.checked_add(3).ok_or_else(|| {
        DataFusionError::Plan("PLINK sample count arithmetic overflowed".to_string())
    })? / 4;
    variant_count
        .checked_mul(bytes_per_variant)
        .and_then(|value| value.checked_add(BED_HEADER_LEN))
        .ok_or_else(|| {
            DataFusionError::Plan("PLINK BED expected-length arithmetic overflowed".to_string())
        })
}

async fn resolve_member(
    bed_path: &str,
    role: &str,
    explicit: Option<&str>,
    extension: &str,
    options: &ObjectStorageOptions,
) -> Result<String> {
    resolve_companion(
        bed_path,
        role,
        explicit,
        &[CompanionRule::ReplaceExtension(extension.to_string())],
        true,
        |candidate| {
            let options = options.clone();
            async move { object_exists(&candidate, &options).await }
        },
    )
    .await?
    .ok_or_else(|| DataFusionError::Plan(format!("required {role} companion was not found")))
}

async fn object_exists(path: &str, options: &ObjectStorageOptions) -> Result<bool> {
    match get_storage_type(path.to_string()) {
        StorageType::LOCAL => Ok(Path::new(local_path(path)?).is_file()),
        _ => match RemoteObject::open(path.to_string(), options.clone()).await {
            Ok(object) => match object.size().await {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(external_error("stat", path, error)),
            },
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(external_error("open", path, error)),
        },
    }
}

async fn read_bounded(
    path: &str,
    max_bytes: usize,
    options: &ObjectStorageOptions,
) -> Result<Bytes> {
    let size = object_size(path, options).await?;
    if size > max_bytes as u64 {
        return Err(DataFusionError::Plan(format!(
            "companion {} is {size} bytes, exceeding configured max_companion_bytes {max_bytes}",
            sanitize_location(path)
        )));
    }
    if size == 0 {
        return Ok(Bytes::new());
    }
    read_range(path, 0..size, options).await
}

pub(crate) async fn object_size(path: &str, options: &ObjectStorageOptions) -> Result<u64> {
    match get_storage_type(path.to_string()) {
        StorageType::LOCAL => std::fs::metadata(local_path(path)?)
            .map(|metadata| metadata.len())
            .map_err(|error| io_error("stat", path, error)),
        _ => RemoteObject::open(path.to_string(), options.clone())
            .await
            .map_err(|error| external_error("open", path, error))?
            .size()
            .await
            .map_err(|error| external_error("stat", path, error)),
    }
}

pub(crate) async fn read_range(
    path: &str,
    range: std::ops::Range<u64>,
    options: &ObjectStorageOptions,
) -> Result<Bytes> {
    let expected = usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
        DataFusionError::Execution("PLINK byte range does not fit memory address space".to_string())
    })?;
    match get_storage_type(path.to_string()) {
        StorageType::LOCAL => {
            let mut file = tokio::fs::File::open(local_path(path)?)
                .await
                .map_err(|error| io_error("open", path, error))?;
            file.seek(std::io::SeekFrom::Start(range.start))
                .await
                .map_err(|error| io_error("seek", path, error))?;
            let mut bytes = vec![0; expected];
            file.read_exact(&mut bytes)
                .await
                .map_err(|error| io_error("read", path, error))?;
            Ok(Bytes::from(bytes))
        }
        _ => RemoteObject::open(path.to_string(), options.clone())
            .await
            .map_err(|error| external_error("open", path, error))?
            .read_range(range)
            .await
            .map_err(|error| external_error("read", path, error)),
    }
}

fn local_path(path: &str) -> Result<&str> {
    if let Some(path) = path.strip_prefix("file://") {
        if path.is_empty() {
            return Err(DataFusionError::Plan(
                "local file URL has an empty path".to_string(),
            ));
        }
        Ok(path)
    } else {
        Ok(path)
    }
}

async fn validate_bed(
    bed_path: &str,
    expected_size: u64,
    options: &ObjectStorageOptions,
) -> Result<()> {
    let observed_size = object_size(bed_path, options).await?;
    if observed_size != expected_size {
        return Err(DataFusionError::Plan(format!(
            "PLINK BED length mismatch for {}: expected {expected_size} bytes from BIM/FAM counts, observed {observed_size}",
            sanitize_location(bed_path)
        )));
    }
    if observed_size < BED_HEADER_LEN {
        return Err(DataFusionError::Plan(format!(
            "PLINK BED {} is shorter than the 3-byte header",
            sanitize_location(bed_path)
        )));
    }
    let header = read_range(bed_path, 0..BED_HEADER_LEN, options).await?;
    match header.as_ref() {
        [0x6c, 0x1b, 0x01] => Ok(()),
        [0x6c, 0x1b, 0x00] => Err(DataFusionError::Plan(
            "PLINK BED uses unsupported sample-major layout; convert it to variant-major BED"
                .to_string(),
        )),
        _ => Err(DataFusionError::Plan(format!(
            "PLINK BED {} has invalid or legacy magic; expected 6c 1b 01",
            sanitize_location(bed_path)
        ))),
    }
}

fn parse_fam(path: &str, bytes: &[u8], mode: SampleIdMode) -> Result<Vec<FamIdentity>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Plan(format!(
            "FAM {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;
    let mut identities = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 6 {
            return Err(DataFusionError::Plan(format!(
                "FAM {} line {} has {} fields; expected 6",
                sanitize_location(path),
                line_index + 1,
                fields.len()
            )));
        }
        identities.push(FamIdentity {
            fid: fields[0].to_string(),
            iid: fields[1].to_string(),
        });
    }
    if mode == SampleIdMode::Iid {
        let mut seen = std::collections::HashSet::with_capacity(identities.len());
        for identity in &identities {
            if !seen.insert(identity.iid.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "FAM {} contains duplicate IID '{}'; use sample_id_mode = fid_iid",
                    sanitize_location(path),
                    identity.iid
                )));
            }
        }
    }
    Ok(identities)
}

fn sample_names(identities: &[FamIdentity], mode: SampleIdMode) -> Result<Vec<String>> {
    let names: Vec<_> = identities
        .iter()
        .map(|identity| match mode {
            SampleIdMode::Iid => identity.iid.clone(),
            SampleIdMode::FidIid => {
                format!("{}:{}", escape_id(&identity.fid), escape_id(&identity.iid))
            }
        })
        .collect();
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    for name in &names {
        if !seen.insert(name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "FAM sample identifier remains ambiguous in {mode:?} mode: {name}"
            )));
        }
    }
    Ok(names)
}

fn escape_id(value: &str) -> String {
    value.replace('%', "%25").replace(':', "%3A")
}

fn parse_bim(path: &str, bytes: &[u8], coordinates: CoordinateSystem) -> Result<Vec<BimVariant>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Plan(format!(
            "BIM {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;
    let mut variants = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let row = line_index + 1;
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 6 {
            return Err(DataFusionError::Plan(format!(
                "BIM {} line {row} has {} fields; expected 6",
                sanitize_location(path),
                fields.len()
            )));
        }
        let cm = fields[2].parse::<f64>().map_err(|error| {
            DataFusionError::Plan(format!(
                "BIM {} line {row} has invalid centimorgan value '{}': {error}",
                sanitize_location(path),
                fields[2]
            ))
        })?;
        if !cm.is_finite() {
            return Err(DataFusionError::Plan(format!(
                "BIM {} line {row} centimorgan value must be finite",
                sanitize_location(path)
            )));
        }
        let position = fields[3].parse::<u64>().map_err(|error| {
            DataFusionError::Plan(format!(
                "BIM {} line {row} has invalid base-pair position '{}': {error}",
                sanitize_location(path),
                fields[3]
            ))
        })?;
        let site = coordinates.site(position).map_err(|error| {
            DataFusionError::Plan(format!(
                "BIM {} line {row} has invalid base-pair position '{}': {error}",
                sanitize_location(path),
                fields[3]
            ))
        })?;
        if fields[0].is_empty() || fields[4].is_empty() || fields[5].is_empty() {
            return Err(DataFusionError::Plan(format!(
                "BIM {} line {row} contains an empty required field",
                sanitize_location(path)
            )));
        }
        variants.push(BimVariant {
            chrom: fields[0].to_string(),
            id: (fields[1] != ".").then(|| fields[1].to_string()),
            cm,
            start: site.start,
            end: site.end,
            a1: fields[4].to_string(),
            a2: fields[5].to_string(),
        });
    }
    Ok(variants)
}

fn io_error(action: &str, path: &str, error: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::new(
        error.kind(),
        format!("{action} {}: {error}", sanitize_location(path)),
    )))
}

fn external_error(
    action: &str,
    path: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "{action} {}: {error}",
        sanitize_location(path)
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_checked_bed_sizes() {
        assert_eq!(expected_bed_size(4, 5).unwrap(), 11);
        assert!(expected_bed_size(1, u64::MAX).is_err());
        assert!(expected_bed_size(u64::MAX, 4).is_err());
    }
}
