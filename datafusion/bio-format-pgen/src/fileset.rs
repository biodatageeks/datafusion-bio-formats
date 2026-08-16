use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::{CompanionRule, resolve_companion, sanitize_location};
use datafusion_bio_format_core::genotype::{CoordinateSystem, SampleSelection, resolve_samples};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use flate2::read::MultiGzDecoder;

use crate::source::ObjectAccess;
use crate::table_provider::{PgenReadOptions, PsamIdMode};

pub(crate) const PGEN_SPEC_BASELINE: &str = "plink-ng 9ee41ce (2026-07-27)";
const PGEN_BLOCK_VARIANTS: usize = 1 << 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgenMode {
    Plink1,
    FixedHardcall,
    FixedDosage,
    FixedPhasedDosage,
    Variable,
    VariableExtensions,
    ExternalIndex,
    ExternalIndexExtensions,
}

impl PgenMode {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Self::Plink1),
            0x02 => Ok(Self::FixedHardcall),
            0x03 => Ok(Self::FixedDosage),
            0x04 => Ok(Self::FixedPhasedDosage),
            0x10 => Ok(Self::Variable),
            0x11 => Ok(Self::VariableExtensions),
            0x20 => Ok(Self::ExternalIndex),
            0x21 => Ok(Self::ExternalIndexExtensions),
            _ => Err(DataFusionError::Plan(format!(
                "unsupported PGEN storage mode 0x{value:02x}; specification baseline {PGEN_SPEC_BASELINE}"
            ))),
        }
    }

    pub(crate) fn byte(self) -> u8 {
        match self {
            Self::Plink1 => 0x01,
            Self::FixedHardcall => 0x02,
            Self::FixedDosage => 0x03,
            Self::FixedPhasedDosage => 0x04,
            Self::Variable => 0x10,
            Self::VariableExtensions => 0x11,
            Self::ExternalIndex => 0x20,
            Self::ExternalIndexExtensions => 0x21,
        }
    }

    fn has_external_index(self) -> bool {
        matches!(self, Self::ExternalIndex | Self::ExternalIndexExtensions)
    }

    fn has_extensions(self) -> bool {
        matches!(
            self,
            Self::VariableExtensions | Self::ExternalIndexExtensions
        )
    }

    fn is_biallelic_only(self) -> bool {
        matches!(
            self,
            Self::Plink1 | Self::FixedHardcall | Self::FixedDosage | Self::FixedPhasedDosage
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PvarVariant {
    pub(crate) chrom: String,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) id: Option<String>,
    pub(crate) reference: String,
    pub(crate) alternate: Vec<String>,
}

impl PvarVariant {
    pub(crate) fn allele_count(&self) -> usize {
        1 + self.alternate.len()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct PsamIdentity {
    pub(crate) fid: String,
    pub(crate) iid: String,
    pub(crate) sid: String,
}

#[derive(Clone, Debug)]
pub(crate) struct RecordInfo {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) record_type: u8,
    pub(crate) ld_base: Option<usize>,
}

impl RecordInfo {
    pub(crate) fn end(&self) -> u64 {
        self.offset + u64::from(self.length)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PgenFileset {
    pub(crate) pgen_path: String,
    pub(crate) pvar_path: String,
    pub(crate) psam_path: String,
    pub(crate) pgi_path: Option<String>,
    pub(crate) source: ObjectAccess,
    pub(crate) variants: Arc<Vec<PvarVariant>>,
    pub(crate) selected_samples: SampleSelection,
    pub(crate) selected_identities: Arc<Vec<PsamIdentity>>,
    pub(crate) sample_count: usize,
    pub(crate) records: Arc<Vec<RecordInfo>>,
    pub(crate) mode: PgenMode,
    pub(crate) companion_bytes: u64,
    pub(crate) header_bytes: u64,
}

impl PgenFileset {
    pub(crate) async fn open(pgen_path: String, options: &PgenReadOptions) -> Result<Self> {
        let storage_options = options.object_storage_options.clone().unwrap_or_default();
        let source = ObjectAccess::open(&pgen_path, &storage_options).await?;
        let pgen_size = source.size(&pgen_path).await?;
        if pgen_size < 3 {
            return Err(DataFusionError::Plan(format!(
                "PGEN {} is shorter than its 3-byte magic",
                sanitize_location(&pgen_path)
            )));
        }

        reject_hybrid_companions(&pgen_path, options, &storage_options).await?;
        let pvar_path = resolve_pvar(&pgen_path, options, &storage_options).await?;
        let psam_path = resolve_member(
            &pgen_path,
            "PSAM",
            options.psam_path.as_deref(),
            &[CompanionRule::ReplaceExtension("psam".to_string())],
            &storage_options,
        )
        .await?;
        let pvar_raw = ObjectAccess::open(&pvar_path, &storage_options)
            .await?
            .read_all_bounded(&pvar_path, options.max_companion_bytes)
            .await?;
        let pvar_bytes = decode_text_companion(
            &pvar_path,
            &pvar_raw,
            options.max_decompressed_companion_bytes,
        )?;
        let variants = parse_pvar(
            &pvar_path,
            &pvar_bytes,
            options.coordinate_system,
            options.max_variants,
        )?;
        if let Some((index, count)) = variants
            .iter()
            .map(PvarVariant::allele_count)
            .enumerate()
            .find(|(_, count)| *count > 65_536)
        {
            return Err(DataFusionError::Plan(format!(
                "PVAR variant {index} has {count} alleles; raw UInt16 allele output supports at most 65536"
            )));
        }

        let psam_raw = ObjectAccess::open(&psam_path, &storage_options)
            .await?
            .read_all_bounded(&psam_path, options.max_companion_bytes)
            .await?;
        let psam_bytes = decode_text_companion(
            &psam_path,
            &psam_raw,
            options.max_decompressed_companion_bytes,
        )?;
        let identities = parse_psam(&psam_path, &psam_bytes, options.max_samples)?;
        let source_names = sample_names(&identities, options.psam_id_mode);
        let selected_samples = resolve_samples(
            &source_names,
            options.samples.as_deref(),
            options.missing_sample_policy,
        )?;
        let selected_identities = selected_samples
            .source_indices()
            .iter()
            .map(|&index| identities[index].clone())
            .collect();

        let prefix = source.read_range(&pgen_path, 0..3).await?;
        if prefix[0..2] != [0x6c, 0x1b] {
            return Err(DataFusionError::Plan(format!(
                "PGEN {} has invalid magic; expected 6c 1b",
                sanitize_location(&pgen_path)
            )));
        }
        let mode = PgenMode::from_byte(prefix[2])?;
        let (
            pgi_path,
            records,
            header_bytes,
            index_companion_bytes,
            header_sample_count,
            header_variant_count,
        ) = if mode.has_external_index() {
            let pgi_path = resolve_member(
                &pgen_path,
                "PGEN index",
                options.pgi_path.as_deref(),
                &[CompanionRule::AppendSuffix(".pgi".to_string())],
                &storage_options,
            )
            .await?;
            let pgi = ObjectAccess::open(&pgi_path, &storage_options).await?;
            let (records, index_bytes_read, samples, variants) = parse_index(
                &pgen_path,
                &source,
                pgen_size,
                mode,
                Some((&pgi_path, &pgi)),
                options.max_header_bytes,
                options.max_record_bytes,
                &variants,
                identities.len(),
            )
            .await?;
            (
                Some(pgi_path),
                records,
                3,
                index_bytes_read,
                samples,
                variants,
            )
        } else {
            let (records, index_bytes_read, samples, variants) = parse_index(
                &pgen_path,
                &source,
                pgen_size,
                mode,
                None,
                options.max_header_bytes,
                options.max_record_bytes,
                &variants,
                identities.len(),
            )
            .await?;
            (None, records, 3 + index_bytes_read, 0, samples, variants)
        };

        if header_sample_count != identities.len() {
            return Err(DataFusionError::Plan(format!(
                "PGEN sample count {header_sample_count} differs from PSAM row count {}",
                identities.len()
            )));
        }
        if header_variant_count != variants.len() {
            return Err(DataFusionError::Plan(format!(
                "PGEN variant count {header_variant_count} differs from PVAR row count {}",
                variants.len()
            )));
        }

        Ok(Self {
            pgen_path,
            pvar_path,
            psam_path,
            pgi_path,
            source,
            variants: Arc::new(variants),
            selected_samples,
            selected_identities: Arc::new(selected_identities),
            sample_count: identities.len(),
            records: Arc::new(records),
            mode,
            companion_bytes: (pvar_raw.len() + psam_raw.len()) as u64 + index_companion_bytes,
            header_bytes,
        })
    }
}

async fn resolve_pvar(
    pgen_path: &str,
    options: &PgenReadOptions,
    storage_options: &ObjectStorageOptions,
) -> Result<String> {
    resolve_member(
        pgen_path,
        "PVAR",
        options.pvar_path.as_deref(),
        &[
            CompanionRule::ReplaceExtension("pvar".to_string()),
            CompanionRule::ReplaceExtension("pvar.zst".to_string()),
        ],
        storage_options,
    )
    .await
}

async fn resolve_member(
    pgen_path: &str,
    role: &str,
    explicit: Option<&str>,
    rules: &[CompanionRule],
    options: &ObjectStorageOptions,
) -> Result<String> {
    resolve_companion(pgen_path, role, explicit, rules, true, |candidate| {
        let options = options.clone();
        async move { ObjectAccess::exists(&candidate, &options).await }
    })
    .await?
    .ok_or_else(|| DataFusionError::Plan(format!("required {role} companion was not found")))
}

async fn reject_hybrid_companions(
    pgen_path: &str,
    options: &PgenReadOptions,
    storage_options: &ObjectStorageOptions,
) -> Result<()> {
    if options.pvar_path.is_some() || options.psam_path.is_some() {
        return Ok(());
    }
    let pvar = datafusion_bio_format_core::companion::companion_candidates(
        pgen_path,
        &[
            CompanionRule::ReplaceExtension("pvar".to_string()),
            CompanionRule::ReplaceExtension("pvar.zst".to_string()),
        ],
    )?;
    let psam = datafusion_bio_format_core::companion::companion_candidates(
        pgen_path,
        &[CompanionRule::ReplaceExtension("psam".to_string())],
    )?;
    let has_pvar = if ObjectAccess::exists(&pvar[0], storage_options).await? {
        true
    } else {
        ObjectAccess::exists(&pvar[1], storage_options).await?
    };
    if has_pvar || ObjectAccess::exists(&psam[0], storage_options).await? {
        return Ok(());
    }
    let bim = datafusion_bio_format_core::companion::companion_candidates(
        pgen_path,
        &[CompanionRule::ReplaceExtension("bim".to_string())],
    )?;
    let fam = datafusion_bio_format_core::companion::companion_candidates(
        pgen_path,
        &[CompanionRule::ReplaceExtension("fam".to_string())],
    )?;
    if ObjectAccess::exists(&bim[0], storage_options).await?
        && ObjectAccess::exists(&fam[0], storage_options).await?
    {
        return Err(DataFusionError::Plan(
            "unsupported hybrid PGEN/BIM/FAM fileset; provide standard PVAR/PSAM companions"
                .to_string(),
        ));
    }
    Ok(())
}

fn decode_text_companion(path: &str, bytes: &[u8], max_decoded: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        let decoder = zstd::stream::read::Decoder::new(bytes).map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to decompress text companion {} as zstd: {error}",
                sanitize_location(path)
            ))
        })?;
        decoder
            .take((max_decoded as u64).saturating_add(1))
            .read_to_end(&mut decoded)
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to decompress text companion {} as zstd: {error}",
                    sanitize_location(path)
                ))
            })?;
    } else if bytes.starts_with(&[0x1f, 0x8b]) {
        MultiGzDecoder::new(bytes)
            .take((max_decoded as u64).saturating_add(1))
            .read_to_end(&mut decoded)
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to decompress text companion {} as gzip: {error}",
                    sanitize_location(path)
                ))
            })?;
    } else {
        decoded.extend_from_slice(bytes);
    }
    if decoded.len() > max_decoded {
        return Err(DataFusionError::Plan(format!(
            "decompressed text companion {} exceeds configured limit {max_decoded}",
            sanitize_location(path)
        )));
    }
    Ok(decoded)
}

fn parse_pvar(
    path: &str,
    bytes: &[u8],
    coordinates: CoordinateSystem,
    max_variants: usize,
) -> Result<Vec<PvarVariant>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Plan(format!(
            "PVAR {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;
    let lines: Vec<_> = text.lines().collect();
    let body_start = lines
        .iter()
        .position(|line| !line.starts_with('#'))
        .unwrap_or(lines.len());
    let header_index = lines[..body_start]
        .iter()
        .rposition(|line| line.starts_with("#CHROM"));
    let (start_line, columns) = if let Some(index) = header_index {
        if index + 1 != body_start {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} #CHROM line must be the final header line",
                sanitize_location(path)
            )));
        }
        let columns = lines[index]
            .trim_start_matches('#')
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        (body_start, columns)
    } else {
        if body_start > 0 {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} header does not end with #CHROM",
                sanitize_location(path)
            )));
        }
        let first_width = lines
            .iter()
            .find(|line| !line.is_empty())
            .map(|line| line.split_whitespace().count())
            .unwrap_or(0);
        // PLINK 2 specifies BIM order for a headerless PVAR. The five-column
        // form omits CM: CHROM, ID, POS, ALT, REF.
        let columns = match first_width {
            5 => vec!["CHROM", "ID", "POS", "ALT", "REF"],
            6.. => vec!["CHROM", "ID", "CM", "POS", "ALT", "REF"],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "headerless PVAR {} must have at least five columns",
                    sanitize_location(path)
                )));
            }
        }
        .into_iter()
        .map(str::to_string)
        .collect();
        (0, columns)
    };
    let column = |name: &str| {
        columns
            .iter()
            .position(|value| value == name)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PVAR {} is missing required {name} column",
                    sanitize_location(path)
                ))
            })
    };
    let chrom_col = column("CHROM")?;
    let pos_col = column("POS")?;
    let id_col = column("ID")?;
    let ref_col = column("REF")?;
    let alt_col = column("ALT")?;
    let required_width = [chrom_col, pos_col, id_col, ref_col, alt_col]
        .into_iter()
        .max()
        .unwrap_or(0)
        + 1;

    let mut variants = Vec::new();
    for (line_index, line) in lines.iter().enumerate().skip(start_line) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < required_width {
            return Err(pvar_line_error(
                path,
                line_index,
                format!(
                    "has {} columns; at least {required_width} required",
                    fields.len()
                ),
            ));
        }
        let position = fields[pos_col].parse::<u64>().map_err(|error| {
            pvar_line_error(path, line_index, format!("has invalid POS: {error}"))
        })?;
        let site = coordinates
            .site(position)
            .map_err(|error| pvar_line_error(path, line_index, error.to_string()))?;
        let reference = fields[ref_col];
        if reference.is_empty() || reference == "." || reference.contains(',') {
            return Err(pvar_line_error(
                path,
                line_index,
                "has malformed REF allele".to_string(),
            ));
        }
        let alternate = fields[alt_col]
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        if alternate.is_empty()
            || alternate
                .iter()
                .any(|allele| allele.is_empty() || allele == ".")
        {
            return Err(pvar_line_error(
                path,
                line_index,
                "has malformed ALT allele list".to_string(),
            ));
        }
        if variants.len() >= max_variants {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} exceeds configured max_variants {max_variants}",
                sanitize_location(path)
            )));
        }
        variants.push(PvarVariant {
            chrom: fields[chrom_col].to_string(),
            start: site.start,
            end: site.end,
            id: (fields[id_col] != ".").then(|| fields[id_col].to_string()),
            reference: reference.to_string(),
            alternate,
        });
    }
    Ok(variants)
}

fn pvar_line_error(path: &str, line_index: usize, detail: String) -> DataFusionError {
    DataFusionError::Plan(format!(
        "PVAR {} line {} {detail}",
        sanitize_location(path),
        line_index + 1
    ))
}

fn parse_psam(path: &str, bytes: &[u8], max_samples: usize) -> Result<Vec<PsamIdentity>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Plan(format!(
            "PSAM {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;
    let lines: Vec<_> = text.lines().collect();
    let body_start = lines
        .iter()
        .position(|line| !line.starts_with('#'))
        .unwrap_or(lines.len());
    let header = lines[..body_start]
        .iter()
        .rfind(|line| line.starts_with("#FID") || line.starts_with("#IID"));
    let (columns, start) = if let Some(header) = header {
        (
            header
                .trim_start_matches('#')
                .split_whitespace()
                .collect::<Vec<_>>(),
            body_start,
        )
    } else {
        let first_width = lines
            .iter()
            .find(|line| !line.is_empty())
            .map(|line| line.split_whitespace().count())
            .unwrap_or(0);
        if first_width < 5 {
            return Err(DataFusionError::Plan(format!(
                "headerless PSAM {} must have at least five columns",
                sanitize_location(path)
            )));
        }
        (vec!["FID", "IID", "PAT", "MAT", "SEX"], 0)
    };
    let iid_col = columns
        .iter()
        .position(|value| *value == "IID")
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "PSAM {} is missing required IID column",
                sanitize_location(path)
            ))
        })?;
    let fid_col = columns.iter().position(|value| *value == "FID");
    let sid_col = columns.iter().position(|value| *value == "SID");
    let mut identities = Vec::new();
    let mut full_ids = HashSet::new();
    for (line_index, line) in lines.iter().enumerate().skip(start) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < columns.len() {
            return Err(DataFusionError::Plan(format!(
                "PSAM {} line {} has {} fields; expected at least {}",
                sanitize_location(path),
                line_index + 1,
                fields.len(),
                columns.len()
            )));
        }
        let iid = fields[iid_col];
        if iid == "0" || iid.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "PSAM {} line {} has invalid IID",
                sanitize_location(path),
                line_index + 1
            )));
        }
        let identity = PsamIdentity {
            fid: fid_col
                .map(|index| fields[index])
                .unwrap_or("0")
                .to_string(),
            iid: iid.to_string(),
            sid: sid_col
                .map(|index| fields[index])
                .unwrap_or("0")
                .to_string(),
        };
        if !full_ids.insert((
            identity.fid.clone(),
            identity.iid.clone(),
            identity.sid.clone(),
        )) {
            return Err(DataFusionError::Plan(format!(
                "PSAM {} line {} repeats a full FID/IID/SID identity",
                sanitize_location(path),
                line_index + 1
            )));
        }
        identities.push(identity);
        if identities.len() > max_samples {
            return Err(DataFusionError::Plan(format!(
                "PSAM {} exceeds configured max_samples {max_samples}",
                sanitize_location(path)
            )));
        }
    }
    Ok(identities)
}

fn sample_names(identities: &[PsamIdentity], mode: PsamIdMode) -> Vec<String> {
    identities
        .iter()
        .map(|identity| match mode {
            PsamIdMode::Iid => identity.iid.clone(),
            PsamIdMode::FidIid => {
                format!("{}:{}", escape_id(&identity.fid), escape_id(&identity.iid))
            }
            PsamIdMode::FidIidSid => format!(
                "{}:{}:{}",
                escape_id(&identity.fid),
                escape_id(&identity.iid),
                escape_id(&identity.sid)
            ),
        })
        .collect()
}

fn escape_id(value: &str) -> String {
    value.replace('\\', "\\\\").replace(':', "\\:")
}

#[allow(clippy::too_many_arguments)]
async fn parse_index(
    pgen_path: &str,
    pgen: &ObjectAccess,
    pgen_size: u64,
    mode: PgenMode,
    external: Option<(&str, &ObjectAccess)>,
    max_header_bytes: usize,
    max_record_bytes: u64,
    pvar: &[PvarVariant],
    psam_sample_count: usize,
) -> Result<(Vec<RecordInfo>, u64, usize, usize)> {
    if mode.is_biallelic_only()
        && let Some((index, count)) = pvar
            .iter()
            .map(PvarVariant::allele_count)
            .enumerate()
            .find(|(_, count)| *count != 2)
    {
        return Err(DataFusionError::Plan(format!(
            "PGEN storage mode 0x{:02x} is biallelic-only, but PVAR variant {index} has {count} alleles",
            mode.byte()
        )));
    }
    if mode == PgenMode::Plink1 {
        let bytes_per_variant = psam_sample_count.checked_add(3).ok_or_else(|| {
            DataFusionError::Plan("PGEN sample count arithmetic overflowed".to_string())
        })? / 4;
        let expected = 3_u64
            .checked_add(
                u64::try_from(bytes_per_variant)
                    .ok()
                    .and_then(|width| width.checked_mul(pvar.len() as u64))
                    .ok_or_else(|| {
                        DataFusionError::Plan("PGEN fixed-width length overflowed".to_string())
                    })?,
            )
            .ok_or_else(|| DataFusionError::Plan("PGEN length overflowed".to_string()))?;
        if bytes_per_variant as u64 > max_record_bytes {
            return Err(DataFusionError::Plan(format!(
                "PLINK 1-mode PGEN record width {bytes_per_variant} exceeds configured max_record_bytes {max_record_bytes}"
            )));
        }
        if expected != pgen_size {
            return Err(DataFusionError::Plan(format!(
                "PLINK 1-mode PGEN length mismatch: expected {expected}, observed {pgen_size}"
            )));
        }
        let records = (0..pvar.len())
            .map(|index| RecordInfo {
                offset: 3 + (index * bytes_per_variant) as u64,
                length: bytes_per_variant as u32,
                record_type: 0xff,
                ld_base: None,
            })
            .collect();
        return Ok((records, 0, psam_sample_count, pvar.len()));
    }

    let (header_path, header_object, expected_magic) = if let Some((path, object)) = external {
        let magic = if mode == PgenMode::ExternalIndex {
            0x30
        } else {
            0x31
        };
        (path, object, magic)
    } else {
        (pgen_path, pgen, mode.byte())
    };
    let prefix = header_object.read_range(header_path, 0..12).await?;
    if prefix[0..2] != [0x6c, 0x1b] || prefix[2] != expected_magic {
        return Err(DataFusionError::Plan(format!(
            "{} has invalid PGEN index magic; expected 6c 1b {expected_magic:02x}",
            sanitize_location(header_path)
        )));
    }
    let variant_count = read_le(&prefix, 3, 4)? as usize;
    let sample_count = read_le(&prefix, 7, 4)? as usize;
    if variant_count != pvar.len() {
        return Err(DataFusionError::Plan(format!(
            "PGEN header variant count {variant_count} differs from PVAR row count {}",
            pvar.len()
        )));
    }
    if sample_count != psam_sample_count {
        return Err(DataFusionError::Plan(format!(
            "PGEN header sample count {sample_count} differs from PSAM row count {psam_sample_count}"
        )));
    }
    let control = prefix[11];
    let type_storage = control & 0x0f;
    if type_storage > 7 {
        return Err(DataFusionError::Plan(format!(
            "PGEN header control uses reserved record-index encoding {type_storage} under {PGEN_SPEC_BASELINE}"
        )));
    }
    let allele_width = usize::from((control >> 4) & 0x03);
    let nonref_mode = control >> 6;

    if matches!(
        mode,
        PgenMode::FixedHardcall | PgenMode::FixedDosage | PgenMode::FixedPhasedDosage
    ) {
        if control & 0x3f != 0 {
            return Err(DataFusionError::Plan(format!(
                "fixed-width PGEN mode 0x{:02x} has invalid header control 0x{control:02x}",
                mode.byte()
            )));
        }
        let nonref_bytes = if nonref_mode == 3 {
            variant_count.div_ceil(8)
        } else {
            0
        };
        let header_len = 12_usize
            .checked_add(nonref_bytes)
            .ok_or_else(|| DataFusionError::Plan("PGEN header length overflowed".to_string()))?;
        if header_len > max_header_bytes {
            return Err(DataFusionError::Plan(format!(
                "PGEN header exceeds configured max_header_bytes {max_header_bytes}"
            )));
        }
        let header = header_object
            .read_range(header_path, 0..header_len as u64)
            .await?;
        if nonref_bytes > 0 {
            validate_padding(&header[12..], variant_count, "provisional-reference flags")?;
        }
        let hardcall_bytes = sample_count.div_ceil(4);
        let record_width = match mode {
            PgenMode::FixedHardcall => hardcall_bytes,
            PgenMode::FixedDosage => hardcall_bytes
                .checked_add(sample_count.checked_mul(2).ok_or_else(|| {
                    DataFusionError::Plan("PGEN dosage width overflowed".to_string())
                })?)
                .ok_or_else(|| DataFusionError::Plan("PGEN record width overflowed".to_string()))?,
            PgenMode::FixedPhasedDosage => hardcall_bytes
                .checked_add(sample_count.checked_mul(4).ok_or_else(|| {
                    DataFusionError::Plan("PGEN phased-dosage width overflowed".to_string())
                })?)
                .ok_or_else(|| DataFusionError::Plan("PGEN record width overflowed".to_string()))?,
            _ => unreachable!(),
        };
        let expected_size = (header_len as u64)
            .checked_add(
                (record_width as u64)
                    .checked_mul(variant_count as u64)
                    .ok_or_else(|| {
                        DataFusionError::Plan("PGEN expected size overflowed".to_string())
                    })?,
            )
            .ok_or_else(|| DataFusionError::Plan("PGEN expected size overflowed".to_string()))?;
        if record_width as u64 > max_record_bytes {
            return Err(DataFusionError::Plan(format!(
                "fixed-width PGEN record width {record_width} exceeds configured max_record_bytes {max_record_bytes}"
            )));
        }
        if expected_size != pgen_size {
            return Err(DataFusionError::Plan(format!(
                "fixed-width PGEN length mismatch: expected {expected_size}, observed {pgen_size}"
            )));
        }
        let record_type = match mode {
            PgenMode::FixedHardcall => 0x00,
            PgenMode::FixedDosage => 0x40,
            PgenMode::FixedPhasedDosage => 0xc0,
            _ => unreachable!(),
        };
        let records = (0..variant_count)
            .map(|index| RecordInfo {
                offset: header_len as u64 + (index * record_width) as u64,
                length: record_width as u32,
                record_type,
                ld_base: None,
            })
            .collect();
        return Ok((records, 12 + header_len as u64, sample_count, variant_count));
    }

    let block_count = variant_count.div_ceil(PGEN_BLOCK_VARIANTS);
    let offsets_bytes = block_count
        .checked_mul(8)
        .ok_or_else(|| DataFusionError::Plan("PGEN block index size overflowed".to_string()))?;
    let length_width = 1 + usize::from(type_storage & 3);
    let type_width_is_nibble = type_storage < 4;
    let mut body_len = 12_usize
        .checked_add(offsets_bytes)
        .ok_or_else(|| DataFusionError::Plan("PGEN header size overflowed".to_string()))?;
    for block in 0..block_count {
        let count = (variant_count - block * PGEN_BLOCK_VARIANTS).min(PGEN_BLOCK_VARIANTS);
        let type_bytes = if type_width_is_nibble {
            count.div_ceil(2)
        } else {
            count
        };
        body_len = body_len
            .checked_add(type_bytes)
            .and_then(|value| value.checked_add(count.checked_mul(length_width)?))
            .and_then(|value| value.checked_add(count.checked_mul(allele_width)?))
            .and_then(|value| {
                value.checked_add(if nonref_mode == 3 {
                    count.div_ceil(8)
                } else {
                    0
                })
            })
            .ok_or_else(|| DataFusionError::Plan("PGEN header size overflowed".to_string()))?;
    }
    if body_len > max_header_bytes {
        return Err(DataFusionError::Plan(format!(
            "PGEN header body {body_len} exceeds configured max_header_bytes {max_header_bytes}"
        )));
    }
    let body = header_object
        .read_range(header_path, 0..body_len as u64)
        .await?;
    let block_offsets = (0..block_count)
        .map(|index| read_le(&body, 12 + index * 8, 8))
        .collect::<Result<Vec<_>>>()?;
    if variant_count > 0 && block_offsets.first().copied().unwrap_or(0) < 3 {
        return Err(DataFusionError::Plan(
            "PGEN first variant block starts inside the file magic".to_string(),
        ));
    }

    let mut cursor = 12 + offsets_bytes;
    let mut records = Vec::with_capacity(variant_count);
    let mut allele_counts = Vec::with_capacity(variant_count);
    for block in 0..block_count {
        let block_start_index = block * PGEN_BLOCK_VARIANTS;
        let count = (variant_count - block_start_index).min(PGEN_BLOCK_VARIANTS);
        let type_bytes = if type_width_is_nibble {
            count.div_ceil(2)
        } else {
            count
        };
        let type_slice = take(&body, &mut cursor, type_bytes, "variant record types")?;
        if type_width_is_nibble && count % 2 == 1 && type_slice.last().unwrap() & 0xf0 != 0 {
            return Err(DataFusionError::Plan(format!(
                "PGEN variant block {block} has nonzero record-type padding"
            )));
        }
        let record_types = (0..count)
            .map(|index| {
                if type_width_is_nibble {
                    (type_slice[index / 2] >> ((index % 2) * 4)) & 0x0f
                } else {
                    type_slice[index]
                }
            })
            .collect::<Vec<_>>();
        let lengths = take(
            &body,
            &mut cursor,
            count * length_width,
            "variant record lengths",
        )?;
        let mut offset = block_offsets[block];
        for (index, &record_type) in record_types.iter().enumerate() {
            let length = read_le(lengths, index * length_width, length_width)?;
            let length = u32::try_from(length)
                .map_err(|_| DataFusionError::Plan("PGEN record length exceeds u32".to_string()))?;
            let absolute_index = block_start_index + index;
            if record_type & 7 == 5 {
                return Err(DataFusionError::Plan(format!(
                    "PGEN variant {absolute_index} uses reserved main-track representation 5 under {PGEN_SPEC_BASELINE}"
                )));
            }
            if length == 0 {
                return Err(DataFusionError::Plan(format!(
                    "PGEN variant {absolute_index} has an empty record"
                )));
            }
            if u64::from(length) > max_record_bytes {
                return Err(DataFusionError::Plan(format!(
                    "PGEN variant {absolute_index} record length {length} exceeds configured max_record_bytes {max_record_bytes}"
                )));
            }
            let ld_base = if record_type & 6 == 2 {
                records[block_start_index..]
                    .iter()
                    .rposition(|record: &RecordInfo| record.record_type & 6 != 2)
                    .map(|relative| block_start_index + relative)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "PGEN LD-compressed variant {absolute_index} has no base in its variant block"
                        ))
                    })?
                    .into()
            } else {
                None
            };
            records.push(RecordInfo {
                offset,
                length,
                record_type,
                ld_base,
            });
            offset = offset.checked_add(u64::from(length)).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PGEN record offset overflow at variant {absolute_index}"
                ))
            })?;
        }
        if block + 1 < block_count && offset != block_offsets[block + 1] {
            return Err(DataFusionError::Plan(format!(
                "PGEN variant block {block} lengths end at {offset}, but next block starts at {}",
                block_offsets[block + 1]
            )));
        }
        // In variable-width modes, zero means counts are supplied by the
        // accompanying PVAR; it does not imply that every row is biallelic.
        if allele_width > 0 {
            let raw = take(&body, &mut cursor, count * allele_width, "allele counts")?;
            for index in 0..count {
                // The PGEN header stores total allele count verbatim. The
                // pinned pgenlib reader compares this raw value directly with
                // the accompanying PVAR allele-index offset delta.
                allele_counts.push(read_le(raw, index * allele_width, allele_width)? as usize);
            }
        }
        if nonref_mode == 3 {
            let flags = take(
                &body,
                &mut cursor,
                count.div_ceil(8),
                "provisional-reference flags",
            )?;
            validate_padding(flags, count, "provisional-reference flags")?;
        }
    }
    if cursor != body.len() {
        return Err(DataFusionError::Plan(
            "PGEN header body parser did not consume its declared extent".to_string(),
        ));
    }
    for (index, count) in allele_counts.iter().copied().enumerate() {
        if count != pvar.get(index).map(PvarVariant::allele_count).unwrap_or(0) {
            return Err(DataFusionError::Plan(format!(
                "PGEN allele count {count} at variant {index} differs from PVAR allele count {}",
                pvar.get(index).map(PvarVariant::allele_count).unwrap_or(0)
            )));
        }
    }
    for (index, record) in records.iter().enumerate() {
        if record.end() > pgen_size {
            return Err(DataFusionError::Plan(format!(
                "PGEN record {index} range {}..{} exceeds object length {pgen_size}",
                record.offset,
                record.end()
            )));
        }
    }

    let mut header_end = body_len as u64;
    let mut extension_bytes_read = 0_u64;
    let mut footer_offset = None;
    if mode.has_extensions() {
        let known_extension_end = if external.is_some() {
            Some(header_object.size(header_path).await?)
        } else {
            block_offsets.first().copied()
        };
        if let Some(extension_end) = known_extension_end {
            if extension_end < body_len as u64 {
                return Err(DataFusionError::Plan(
                    "PGEN header extensions overlap the fixed header body".to_string(),
                ));
            }
            let extension_len = usize::try_from(extension_end - body_len as u64).map_err(|_| {
                DataFusionError::Plan("PGEN extension region does not fit usize".to_string())
            })?;
            if body_len.saturating_add(extension_len) > max_header_bytes {
                return Err(DataFusionError::Plan(format!(
                    "PGEN header with extensions exceeds configured max_header_bytes {max_header_bytes}"
                )));
            }
            let extensions = header_object
                .read_range(header_path, body_len as u64..extension_end)
                .await?;
            footer_offset = validate_extensions(&extensions, pgen_size)?;
            header_end = extension_end;
            extension_bytes_read = extension_end - body_len as u64;
        } else {
            let (parsed_footer_offset, parsed_len, bytes_read) = read_empty_embedded_extensions(
                header_object,
                header_path,
                body_len,
                pgen_size,
                max_header_bytes,
            )
            .await?;
            footer_offset = parsed_footer_offset;
            header_end = (body_len as u64)
                .checked_add(u64::try_from(parsed_len).map_err(|_| {
                    DataFusionError::Plan("PGEN extension length does not fit u64".to_string())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Plan("PGEN extension end overflowed".to_string())
                })?;
            extension_bytes_read = bytes_read;
        }
    } else if external.is_some() {
        let observed = header_object.size(header_path).await?;
        if observed != body_len as u64 {
            return Err(DataFusionError::Plan(format!(
                "PGI length {observed} does not equal expected header length {body_len}"
            )));
        }
    } else if variant_count > 0 {
        if block_offsets[0] != body_len as u64 {
            return Err(DataFusionError::Plan(format!(
                "PGEN first block offset {} does not equal header end {body_len}",
                block_offsets[0]
            )));
        }
    } else if pgen_size != body_len as u64 {
        return Err(DataFusionError::Plan(format!(
            "empty PGEN length {pgen_size} does not equal header length {body_len}"
        )));
    }
    let data_end = records
        .last()
        .map(RecordInfo::end)
        .unwrap_or_else(|| if external.is_some() { 3 } else { header_end });
    let expected_data_end = footer_offset.unwrap_or(pgen_size);
    if data_end != expected_data_end {
        return Err(DataFusionError::Plan(format!(
            "PGEN variant data ends at {data_end}, but {} begins at {expected_data_end}",
            if footer_offset.is_some() {
                "the declared footer"
            } else {
                "the object ends"
            }
        )));
    }
    Ok((
        records,
        12 + body_len as u64 + extension_bytes_read,
        sample_count,
        variant_count,
    ))
}

async fn read_empty_embedded_extensions(
    header_object: &ObjectAccess,
    header_path: &str,
    body_len: usize,
    pgen_size: u64,
    max_header_bytes: usize,
) -> Result<(Option<u64>, usize, u64)> {
    // A zero-variant embedded index has no first block offset to delimit its
    // extension region. Grow the probe only to the next parser requirement so
    // a declared footer is never downloaded as if it were header metadata.
    let body_start = body_len as u64;
    let available = pgen_size.checked_sub(body_start).ok_or_else(|| {
        DataFusionError::Plan("PGEN header extensions overlap the fixed header body".to_string())
    })?;
    let max_extension_bytes = max_header_bytes.checked_sub(body_len).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "PGEN header with extensions exceeds configured max_header_bytes {max_header_bytes}"
        ))
    })?;
    let mut bytes = Vec::new();
    let initial_len = available.min(2) as usize;
    if initial_len > max_extension_bytes {
        return Err(DataFusionError::Plan(format!(
            "PGEN header with extensions exceeds configured max_header_bytes {max_header_bytes}"
        )));
    }
    if initial_len > 0 {
        bytes.extend_from_slice(
            &header_object
                .read_range(header_path, body_start..body_start + initial_len as u64)
                .await?,
        );
    }

    loop {
        match parse_extension_layout(&bytes, pgen_size)? {
            ExtensionLayoutStatus::NeedMore(required) => {
                if required > max_extension_bytes {
                    return Err(DataFusionError::Plan(format!(
                        "PGEN header with extensions exceeds configured max_header_bytes {max_header_bytes}"
                    )));
                }
                if required as u64 > available {
                    return Err(DataFusionError::Plan(
                        "truncated PGEN header extension metadata".to_string(),
                    ));
                }
                let start = body_start + bytes.len() as u64;
                let end = body_start + required as u64;
                bytes.extend_from_slice(&header_object.read_range(header_path, start..end).await?);
            }
            ExtensionLayoutStatus::Complete {
                footer_offset,
                total_len,
            } => {
                if total_len > max_extension_bytes {
                    return Err(DataFusionError::Plan(format!(
                        "PGEN header with extensions exceeds configured max_header_bytes {max_header_bytes}"
                    )));
                }
                if total_len as u64 > available {
                    return Err(DataFusionError::Plan(
                        "truncated PGEN header extension body".to_string(),
                    ));
                }
                if total_len > bytes.len() {
                    let start = body_start + bytes.len() as u64;
                    let end = body_start + total_len as u64;
                    bytes.extend_from_slice(
                        &header_object.read_range(header_path, start..end).await?,
                    );
                }
                return Ok((footer_offset, total_len, bytes.len() as u64));
            }
        }
    }
}

fn validate_extensions(bytes: &[u8], pgen_size: u64) -> Result<Option<u64>> {
    let (footer_offset, consumed) = parse_extensions(bytes, pgen_size)?;
    if consumed != bytes.len() {
        return Err(DataFusionError::Plan(format!(
            "PGEN header extension region has {} unaccounted bytes",
            bytes.len() - consumed
        )));
    }
    Ok(footer_offset)
}

fn parse_extensions(bytes: &[u8], pgen_size: u64) -> Result<(Option<u64>, usize)> {
    let (footer_offset, cursor) = match parse_extension_layout(bytes, pgen_size)? {
        ExtensionLayoutStatus::Complete {
            footer_offset,
            total_len,
        } => (footer_offset, total_len),
        ExtensionLayoutStatus::NeedMore(_) => {
            return Err(DataFusionError::Plan(
                "truncated PGEN header extension metadata".to_string(),
            ));
        }
    };
    if cursor > bytes.len() {
        return Err(DataFusionError::Plan(
            "truncated PGEN header extension body".to_string(),
        ));
    }
    Ok((footer_offset, cursor))
}

enum ExtensionLayoutStatus {
    NeedMore(usize),
    Complete {
        footer_offset: Option<u64>,
        total_len: usize,
    },
}

fn parse_extension_layout(bytes: &[u8], pgen_size: u64) -> Result<ExtensionLayoutStatus> {
    let mut cursor = 0;
    let header_flags = match read_flag_varint_prefix(bytes, &mut cursor)? {
        Some(value) => value,
        None => return Ok(ExtensionLayoutStatus::NeedMore(cursor + 1)),
    };
    let footer_flags = match read_flag_varint_prefix(bytes, &mut cursor)? {
        Some(value) => value,
        None => return Ok(ExtensionLayoutStatus::NeedMore(cursor + 1)),
    };
    let footer_offset = if footer_flags > 0 {
        let required = cursor
            .checked_add(8)
            .ok_or_else(|| DataFusionError::Plan("PGEN extension cursor overflowed".to_string()))?;
        if required > bytes.len() {
            return Ok(ExtensionLayoutStatus::NeedMore(required));
        }
        let footer_offset = read_le(bytes, cursor, 8)?;
        cursor += 8;
        if footer_offset > pgen_size {
            return Err(DataFusionError::Plan(format!(
                "PGEN footer extension offset {footer_offset} exceeds object length {pgen_size}"
            )));
        }
        Some(footer_offset)
    } else {
        None
    };
    let mut body_bytes = 0_usize;
    for _ in 0..header_flags {
        let length = match read_varint_prefix(bytes, &mut cursor)? {
            Some(value) => value,
            None => return Ok(ExtensionLayoutStatus::NeedMore(cursor + 1)),
        };
        body_bytes = body_bytes
            .checked_add(usize::try_from(length).map_err(|_| {
                DataFusionError::Plan("PGEN header extension length does not fit usize".to_string())
            })?)
            .ok_or_else(|| DataFusionError::Plan("PGEN extension length overflowed".to_string()))?;
    }
    let total_len = cursor
        .checked_add(body_bytes)
        .ok_or_else(|| DataFusionError::Plan("PGEN extension cursor overflowed".to_string()))?;
    Ok(ExtensionLayoutStatus::Complete {
        footer_offset,
        total_len,
    })
}

fn read_flag_varint_prefix(bytes: &[u8], cursor: &mut usize) -> Result<Option<usize>> {
    let mut set_bits = 0_usize;
    for index in 0..37 {
        let Some(&byte) = bytes.get(*cursor) else {
            return Ok(None);
        };
        *cursor += 1;
        set_bits = set_bits
            .checked_add((byte & 0x7f).count_ones() as usize)
            .ok_or_else(|| DataFusionError::Plan("PGEN flag count overflowed".to_string()))?;
        if byte & 0x80 == 0 {
            if index == 36 && byte > 0x0f {
                return Err(DataFusionError::Plan(
                    "PGEN extension flags exceed 256 bits".to_string(),
                ));
            }
            return Ok(Some(set_bits));
        }
    }
    Err(DataFusionError::Plan(
        "PGEN extension flags exceed 256 bits".to_string(),
    ))
}

fn read_varint_prefix(bytes: &[u8], cursor: &mut usize) -> Result<Option<u64>> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let Some(&byte) = bytes.get(*cursor) else {
            return Ok(None);
        };
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(DataFusionError::Execution(
                "PGEN base-128 varint overflows u64".to_string(),
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(Some(value));
        }
    }
    Err(DataFusionError::Execution(
        "PGEN base-128 varint exceeds ten bytes".to_string(),
    ))
}

pub(crate) fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    if let Some(value) = read_varint_prefix(bytes, cursor)? {
        return Ok(value);
    }
    Err(DataFusionError::Execution(
        "truncated PGEN base-128 varint".to_string(),
    ))
}

pub(crate) fn read_le(bytes: &[u8], offset: usize, width: usize) -> Result<u64> {
    if width == 0 || width > 8 || offset.saturating_add(width) > bytes.len() {
        return Err(DataFusionError::Plan(format!(
            "truncated PGEN little-endian integer at byte {offset} with width {width}"
        )));
    }
    let mut value = 0_u64;
    for (shift, byte) in bytes[offset..offset + width].iter().enumerate() {
        value |= u64::from(*byte) << (shift * 8);
    }
    Ok(value)
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize, context: &str) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| DataFusionError::Plan(format!("{context} length overflowed")))?;
    let result = bytes.get(*cursor..end).ok_or_else(|| {
        DataFusionError::Plan(format!("truncated PGEN {context} at byte {cursor}"))
    })?;
    *cursor = end;
    Ok(result)
}

pub(crate) fn validate_padding(bytes: &[u8], value_count: usize, context: &str) -> Result<()> {
    let remainder = value_count % 8;
    if remainder > 0 && bytes.last().is_some_and(|byte| byte >> remainder != 0) {
        return Err(DataFusionError::Plan(format!(
            "PGEN {context} has nonzero padding bits"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }

    #[test]
    fn decodes_varint_boundaries() {
        for expected in [0, 1, 0x7f, 0x80, 0x3fff, 0x4000, u32::MAX as u64, u64::MAX] {
            let encoded = encode_varint(expected);
            let mut cursor = 0;
            assert_eq!(read_varint(&encoded, &mut cursor).unwrap(), expected);
            assert_eq!(cursor, encoded.len());
        }
    }

    #[test]
    fn rejects_overlong_and_truncated_varints() {
        let mut cursor = 0;
        assert!(read_varint(&[0x80; 10], &mut cursor).is_err());
        let mut cursor = 0;
        assert!(read_varint(&[0x80], &mut cursor).is_err());
        let mut cursor = 0;
        assert!(read_varint(&[0xff; 10], &mut cursor).is_err());
    }

    #[test]
    fn escapes_composite_sample_ids_without_collisions() {
        let identities = vec![PsamIdentity {
            fid: "a:b".to_string(),
            iid: r"c\d".to_string(),
            sid: "0".to_string(),
        }];
        assert_eq!(
            sample_names(&identities, PsamIdMode::FidIid),
            vec![r"a\:b:c\\d"]
        );
    }

    #[test]
    fn parses_bim_order_headerless_pvar_and_enforces_row_limit() {
        let bytes = b"1 v1 10 C A\n2 v2 20 G T\n";
        let variants =
            parse_pvar("cohort.pvar", bytes, CoordinateSystem::ZeroBasedHalfOpen, 2).unwrap();
        assert_eq!(variants[0].start, 9);
        assert_eq!(variants[0].id.as_deref(), Some("v1"));
        assert_eq!(variants[0].reference, "A");
        assert_eq!(variants[0].alternate, vec!["C"]);
        let error = parse_pvar("cohort.pvar", bytes, CoordinateSystem::ZeroBasedHalfOpen, 1)
            .unwrap_err()
            .to_string();
        assert!(error.contains("max_variants 1"), "{error}");
    }
}
