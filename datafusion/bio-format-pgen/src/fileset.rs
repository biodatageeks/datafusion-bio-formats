use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct RecordInfo {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) record_type: u8,
    pub(crate) ld_base: Option<usize>,
}

#[derive(Debug)]
pub(crate) enum RecordIndex {
    Fixed {
        count: usize,
        first_offset: u64,
        record_width: u32,
        record_type: u8,
    },
    Variable(VariableRecordIndex),
    #[cfg(test)]
    Explicit(Box<[RecordInfo]>),
}

#[derive(Debug)]
pub(crate) struct VariableRecordIndex {
    header: Bytes,
    blocks: Box<[VariableRecordBlock]>,
    relative_offsets: Box<[u8]>,
    ld_base_deltas: Box<[u16]>,
    variant_count: usize,
    length_width: usize,
    type_width_is_nibble: bool,
}

#[derive(Clone, Copy, Debug)]
struct VariableRecordBlock {
    first_offset: u64,
    type_offset: usize,
    length_offset: usize,
    relative_offset_start: usize,
    relative_offset_width: usize,
    count: usize,
}

impl RecordIndex {
    pub(crate) fn record(&self, index: usize) -> Result<RecordInfo> {
        match self {
            Self::Fixed {
                count,
                first_offset,
                record_width,
                record_type,
            } => {
                if index >= *count {
                    return Err(DataFusionError::Plan(format!(
                        "PGEN record index {index} is out of bounds for {count} variants"
                    )));
                }
                let offset = first_offset
                    .checked_add(
                        (index as u64)
                            .checked_mul(u64::from(*record_width))
                            .ok_or_else(|| {
                                DataFusionError::Plan("PGEN record offset overflowed".to_string())
                            })?,
                    )
                    .ok_or_else(|| {
                        DataFusionError::Plan("PGEN record offset overflowed".to_string())
                    })?;
                Ok(RecordInfo {
                    offset,
                    length: *record_width,
                    record_type: *record_type,
                    ld_base: None,
                })
            }
            Self::Variable(index_data) => index_data.record(index),
            #[cfg(test)]
            Self::Explicit(records) => records.get(index).copied().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PGEN record index {index} is out of bounds for {} variants",
                    records.len()
                ))
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn explicit(records: Vec<RecordInfo>) -> Self {
        Self::Explicit(records.into_boxed_slice())
    }
}

impl VariableRecordIndex {
    fn record(&self, index: usize) -> Result<RecordInfo> {
        if index >= self.variant_count {
            return Err(DataFusionError::Plan(format!(
                "PGEN record index {index} is out of bounds for {} variants",
                self.variant_count
            )));
        }
        let block_index = index / PGEN_BLOCK_VARIANTS;
        let block = self.blocks.get(block_index).ok_or_else(|| {
            DataFusionError::Plan(format!("PGEN record {index} has no index block"))
        })?;
        let within_block = index % PGEN_BLOCK_VARIANTS;
        if within_block >= block.count {
            return Err(DataFusionError::Plan(format!(
                "PGEN record {index} exceeds index block {block_index}"
            )));
        }
        let type_byte = *self
            .header
            .get(block.type_offset + within_block / if self.type_width_is_nibble { 2 } else { 1 })
            .ok_or_else(|| DataFusionError::Plan("truncated PGEN record type index".to_string()))?;
        let record_type = if self.type_width_is_nibble {
            (type_byte >> ((within_block % 2) * 4)) & 0x0f
        } else {
            type_byte
        };
        let length = u32::try_from(read_le(
            &self.header,
            block.length_offset + within_block * self.length_width,
            self.length_width,
        )?)
        .map_err(|_| DataFusionError::Plan("PGEN record length exceeds u32".to_string()))?;
        let relative_offset = read_le(
            &self.relative_offsets,
            block.relative_offset_start + within_block * block.relative_offset_width,
            block.relative_offset_width,
        )?;
        let offset = block
            .first_offset
            .checked_add(relative_offset)
            .ok_or_else(|| DataFusionError::Plan("PGEN record offset overflowed".to_string()))?;
        let ld_delta = usize::from(*self.ld_base_deltas.get(index).ok_or_else(|| {
            DataFusionError::Plan(format!("PGEN record {index} has no LD-base entry"))
        })?);
        let ld_base = if ld_delta == 0 {
            None
        } else {
            Some(index.checked_sub(ld_delta).ok_or_else(|| {
                DataFusionError::Plan(format!("PGEN record {index} has an invalid LD-base delta"))
            })?)
        };
        Ok(RecordInfo {
            offset,
            length,
            record_type,
            ld_base,
        })
    }
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
    pub(crate) records: Arc<RecordIndex>,
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

/// Column positions a PVAR body is parsed against, resolved once from the header.
struct PvarLayout {
    chrom: usize,
    position: usize,
    id: usize,
    reference: usize,
    alternate: usize,
    width: usize,
}

/// What stopped a chunk parser short of consuming its whole range.
enum PvarStop {
    /// `max_variants` was already reached and another variant line followed.
    Limit,
    /// A line was malformed; the payload is the detail for `pvar_line_error`.
    Malformed(String),
}

/// Bodies below this parse faster in one thread than split across several: a
/// small PVAR fits in cache and the spawn cost dominates the work.
const PARALLEL_PVAR_MIN_BYTES: usize = 1 << 20;

/// The most chunks a PVAR body is split into, regardless of core count.
const PARALLEL_PVAR_MAX_CHUNKS: usize = 16;

fn parse_pvar(
    path: &str,
    bytes: &[u8],
    coordinates: CoordinateSystem,
    max_variants: usize,
) -> Result<Vec<PvarVariant>> {
    parse_pvar_chunked(path, bytes, coordinates, max_variants, None)
}

/// `chunks` pins how many pieces the body is split into; `None` decides from the
/// body size and the host's core count. Tests pin it, because reconciling the
/// row limit across chunk boundaries only has anything to reconcile when a
/// particular boundary falls in a particular place.
fn parse_pvar_chunked(
    path: &str,
    bytes: &[u8],
    coordinates: CoordinateSystem,
    max_variants: usize,
    chunks: Option<usize>,
) -> Result<Vec<PvarVariant>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Plan(format!(
            "PVAR {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;

    // The header is walked with byte offsets, not through `lines()`, so the
    // body's start is known as a byte position and can be split for parallel
    // parsing. Line semantics match `str::lines`: split on `\n`, strip a
    // trailing `\r`.
    let mut header: Option<(usize, &str)> = None;
    let mut header_lines = 0;
    let mut body_start_byte = 0;
    let mut offset = 0;
    while offset < text.len() {
        let rest = &text[offset..];
        let (raw, next) = match rest.find('\n') {
            Some(newline) => (&rest[..newline], offset + newline + 1),
            None => (rest, text.len()),
        };
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if !line.starts_with('#') {
            break;
        }
        if line.starts_with("#CHROM") {
            header = Some((header_lines, line));
        }
        header_lines += 1;
        offset = next;
        body_start_byte = offset;
    }
    let body = &text[body_start_byte..];

    let columns = if let Some((header_index, header)) = header {
        if header_index + 1 != header_lines {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} #CHROM line must be the final header line",
                sanitize_location(path)
            )));
        }
        header
            .trim_start_matches('#')
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>()
    } else {
        if header_lines > 0 {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} header does not end with #CHROM",
                sanitize_location(path)
            )));
        }
        let first_width = body
            .lines()
            .find(|line| !line.is_empty())
            .map(|line| line.split_whitespace().count())
            .unwrap_or(0);
        // PLINK 2 specifies BIM order for a headerless PVAR. The five-column
        // form omits CM: CHROM, ID, POS, ALT, REF.
        match first_width {
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
        .collect()
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
    let (chrom, position, id, reference, alternate) = (
        column("CHROM")?,
        column("POS")?,
        column("ID")?,
        column("REF")?,
        column("ALT")?,
    );
    let layout = PvarLayout {
        chrom,
        position,
        id,
        reference,
        alternate,
        width: [chrom, position, id, reference, alternate]
            .into_iter()
            .max()
            .unwrap_or(0)
            + 1,
    };
    let layout = &layout;

    let chunks = pvar_chunk_ranges(body, chunks.unwrap_or_else(|| pvar_chunk_count(body)));
    let parsed = if chunks.len() <= 1 {
        chunks
            .iter()
            .map(|&(start, end)| {
                parse_pvar_chunk(&body[start..end], coordinates, max_variants, layout)
            })
            .collect::<Vec<_>>()
    } else {
        // Scoped threads borrow `body` and `layout` directly, so the body is
        // never copied and no runtime is involved. The work is CPU-bound and
        // joins before this returns.
        std::thread::scope(|scope| {
            let handles = chunks
                .iter()
                .map(|&(start, end)| {
                    scope.spawn(move || {
                        parse_pvar_chunk(&body[start..end], coordinates, max_variants, layout)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        DataFusionError::Plan(format!(
                            "PVAR {} chunk parser panicked",
                            sanitize_location(path)
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?
    };

    let limit_error = || {
        DataFusionError::Plan(format!(
            "PVAR {} exceeds configured max_variants {max_variants}",
            sanitize_location(path)
        ))
    };
    let mut variants = Vec::with_capacity(parsed.iter().map(|(rows, _)| rows.len()).sum());
    for ((rows, stop), &(chunk_start, _)) in parsed.into_iter().zip(&chunks) {
        // Whichever condition comes first in the file wins, exactly as it does
        // when one pass reads the whole body. The limit trips inside this
        // chunk's rows when they would carry the total past it, which is
        // earlier than any error the chunk reports, because a chunk stops at
        // its first bad line.
        if variants.len() + rows.len() > max_variants {
            return Err(limit_error());
        }
        variants.extend(rows);
        if let Some((chunk_line, stop)) = stop {
            // The chunk stopped, so a further line exists. A serial pass would
            // check the limit before reading it.
            if variants.len() >= max_variants {
                return Err(limit_error());
            }
            return Err(match stop {
                PvarStop::Limit => limit_error(),
                PvarStop::Malformed(detail) => {
                    let preceding = body[..chunk_start].bytes().filter(|&b| b == b'\n').count();
                    pvar_line_error(path, header_lines + preceding + chunk_line, detail)
                }
            });
        }
    }
    Ok(variants)
}

/// Byte ranges covering `body`, each holding whole lines.
///
/// Split points are advanced to the next newline, so a range never starts or
/// ends mid-line and each chunk can be parsed independently.
fn pvar_chunk_ranges(body: &str, target_chunks: usize) -> Vec<(usize, usize)> {
    if body.is_empty() {
        return Vec::new();
    }
    if target_chunks <= 1 {
        return vec![(0, body.len())];
    }

    let bytes = body.as_bytes();
    let mut ranges = Vec::with_capacity(target_chunks);
    let mut start = 0;
    for chunk in 1..target_chunks {
        if start >= bytes.len() {
            break;
        }
        let mut cut = (bytes.len() * chunk / target_chunks).max(start);
        while cut < bytes.len() && bytes[cut] != b'\n' {
            cut += 1;
        }
        // A newline is a char boundary and so is the byte after it, so slicing
        // here is always valid UTF-8.
        if cut < bytes.len() {
            cut += 1;
        }
        if cut > start {
            ranges.push((start, cut));
            start = cut;
        }
    }
    if start < bytes.len() {
        ranges.push((start, bytes.len()));
    }
    ranges
}

/// How many pieces to split a body into, from its size and the host's cores.
fn pvar_chunk_count(body: &str) -> usize {
    if body.len() < PARALLEL_PVAR_MIN_BYTES {
        return 1;
    }
    std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .min(PARALLEL_PVAR_MAX_CHUNKS)
}

/// Parses one chunk, reporting where it stopped in chunk-local line numbers.
fn parse_pvar_chunk(
    body: &str,
    coordinates: CoordinateSystem,
    max_variants: usize,
    layout: &PvarLayout,
) -> (Vec<PvarVariant>, Option<(usize, PvarStop)>) {
    let mut variants = Vec::new();
    for (chunk_line, line) in body.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A chunk cannot know the running total, so the exact limit is the
        // caller's job. This bounds a chunk's own allocation on a file far over
        // the limit, and changes no output: whatever this reports, the caller
        // re-derives the same answer from the counts.
        if variants.len() >= max_variants {
            return (variants, Some((chunk_line, PvarStop::Limit)));
        }
        match parse_pvar_record(line, coordinates, layout) {
            Ok(variant) => variants.push(variant),
            Err(detail) => return (variants, Some((chunk_line, PvarStop::Malformed(detail)))),
        }
    }
    (variants, None)
}

fn parse_pvar_record(
    line: &str,
    coordinates: CoordinateSystem,
    layout: &PvarLayout,
) -> std::result::Result<PvarVariant, String> {
    let fields: Vec<_> = line.split_whitespace().collect();
    if fields.len() < layout.width {
        return Err(format!(
            "has {} columns; at least {} required",
            fields.len(),
            layout.width
        ));
    }
    let position = fields[layout.position]
        .parse::<u64>()
        .map_err(|error| format!("has invalid POS: {error}"))?;
    let site = coordinates
        .site(position)
        .map_err(|error| error.to_string())?;
    let reference = fields[layout.reference];
    if reference.is_empty() || reference == "." || reference.contains(',') {
        return Err("has malformed REF allele".to_string());
    }
    let alternate = fields[layout.alternate]
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    if alternate.is_empty()
        || alternate
            .iter()
            .any(|allele| allele.is_empty() || allele == ".")
    {
        return Err("has malformed ALT allele list".to_string());
    }
    Ok(PvarVariant {
        chrom: fields[layout.chrom].to_string(),
        start: site.start,
        end: site.end,
        id: (fields[layout.id] != ".").then(|| fields[layout.id].to_string()),
        reference: reference.to_string(),
        alternate,
    })
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
    let mut lines = text.lines().enumerate();
    let mut header = None;
    let first_body = loop {
        match lines.next() {
            Some((_, line)) if line.starts_with('#') => {
                if line.starts_with("#FID") || line.starts_with("#IID") {
                    header = Some(line);
                }
            }
            body => break body,
        }
    };
    let columns = if let Some(header) = header {
        header
            .trim_start_matches('#')
            .split_whitespace()
            .collect::<Vec<_>>()
    } else {
        let first_width = text
            .lines()
            .find(|line| !line.is_empty())
            .map(|line| line.split_whitespace().count())
            .unwrap_or(0);
        if first_width < 5 {
            return Err(DataFusionError::Plan(format!(
                "headerless PSAM {} must have at least five columns",
                sanitize_location(path)
            )));
        }
        vec!["FID", "IID", "PAT", "MAT", "SEX"]
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
    for (line_index, line) in first_body.into_iter().chain(lines) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if identities.len() >= max_samples {
            return Err(DataFusionError::Plan(format!(
                "PSAM {} exceeds configured max_samples {max_samples}",
                sanitize_location(path)
            )));
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
) -> Result<(RecordIndex, u64, usize, usize)> {
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
        let record_width = u32::try_from(bytes_per_variant).map_err(|_| {
            DataFusionError::Plan("PLINK 1-mode PGEN record width exceeds u32".to_string())
        })?;
        return Ok((
            RecordIndex::Fixed {
                count: pvar.len(),
                first_offset: 3,
                record_width,
                record_type: 0xff,
            },
            0,
            psam_sample_count,
            pvar.len(),
        ));
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
        let record_width = u32::try_from(record_width).map_err(|_| {
            DataFusionError::Plan("fixed-width PGEN record width exceeds u32".to_string())
        })?;
        return Ok((
            RecordIndex::Fixed {
                count: variant_count,
                first_offset: header_len as u64,
                record_width,
                record_type,
            },
            12 + header_len as u64,
            sample_count,
            variant_count,
        ));
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
    if variant_count > 0 {
        let first_offset = block_offsets.first().copied().unwrap_or(0);
        if external.is_some() && first_offset != 3 {
            return Err(DataFusionError::Plan(format!(
                "external-index PGEN variant data must start at byte 3, observed {first_offset}"
            )));
        }
        if first_offset < 3 {
            return Err(DataFusionError::Plan(
                "PGEN first variant block starts inside the file magic".to_string(),
            ));
        }
    }

    let mut cursor = 12 + offsets_bytes;
    let mut blocks = Vec::with_capacity(block_count);
    let mut relative_offsets = Vec::new();
    let mut ld_base_deltas = Vec::with_capacity(variant_count);
    let mut record_data_end = None;
    for block in 0..block_count {
        let block_start_index = block * PGEN_BLOCK_VARIANTS;
        let count = (variant_count - block_start_index).min(PGEN_BLOCK_VARIANTS);
        let type_bytes = if type_width_is_nibble {
            count.div_ceil(2)
        } else {
            count
        };
        let type_offset = cursor;
        let type_slice = take(&body, &mut cursor, type_bytes, "variant record types")?;
        if type_width_is_nibble && count % 2 == 1 && type_slice.last().unwrap() & 0xf0 != 0 {
            return Err(DataFusionError::Plan(format!(
                "PGEN variant block {block} has nonzero record-type padding"
            )));
        }
        let length_offset = cursor;
        let lengths = take(
            &body,
            &mut cursor,
            count * length_width,
            "variant record lengths",
        )?;
        let mut offset = block_offsets[block];
        let mut last_non_ld = None;
        let mut block_relative_offsets = Vec::with_capacity(count);
        for index in 0..count {
            let record_type = if type_width_is_nibble {
                (type_slice[index / 2] >> ((index % 2) * 4)) & 0x0f
            } else {
                type_slice[index]
            };
            let length = read_le(lengths, index * length_width, length_width)?;
            let length = u32::try_from(length)
                .map_err(|_| DataFusionError::Plan("PGEN record length exceeds u32".to_string()))?;
            let absolute_index = block_start_index + index;
            if record_type & 7 == 5 {
                return Err(DataFusionError::Plan(format!(
                    "PGEN variant {absolute_index} uses reserved main-track representation 5 under {PGEN_SPEC_BASELINE}"
                )));
            }
            if record_type & 0x80 != 0 && (record_type >> 5) & 3 == 0 {
                return Err(DataFusionError::Plan(format!(
                    "PGEN variant {absolute_index} has a phased-dosage track without a dosage track"
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
            block_relative_offsets.push(offset.checked_sub(block_offsets[block]).ok_or_else(
                || DataFusionError::Plan("PGEN block-relative offset underflowed".to_string()),
            )?);
            let ld_delta = if record_type & 6 == 2 {
                let base = last_non_ld.ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "PGEN LD-compressed variant {absolute_index} has no base in its variant block"
                    ))
                })?;
                u16::try_from(index - base).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "PGEN LD-base distance overflow at variant {absolute_index}"
                    ))
                })?
            } else {
                last_non_ld = Some(index);
                0
            };
            ld_base_deltas.push(ld_delta);
            offset = offset.checked_add(u64::from(length)).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PGEN record offset overflow at variant {absolute_index}"
                ))
            })?;
            if offset > pgen_size {
                return Err(DataFusionError::Plan(format!(
                    "PGEN record {absolute_index} ends at {offset}, exceeding object length {pgen_size}"
                )));
            }
        }
        if block + 1 < block_count && offset != block_offsets[block + 1] {
            return Err(DataFusionError::Plan(format!(
                "PGEN variant block {block} lengths end at {offset}, but next block starts at {}",
                block_offsets[block + 1]
            )));
        }
        let max_relative_offset = block_relative_offsets.last().copied().unwrap_or(0);
        let relative_offset_width = ((u64::BITS - max_relative_offset.leading_zeros()) as usize)
            .div_ceil(8)
            .max(1);
        let relative_offset_start = relative_offsets.len();
        for relative_offset in block_relative_offsets {
            relative_offsets
                .extend_from_slice(&relative_offset.to_le_bytes()[..relative_offset_width]);
        }
        blocks.push(VariableRecordBlock {
            first_offset: block_offsets[block],
            type_offset,
            length_offset,
            relative_offset_start,
            relative_offset_width,
            count,
        });
        record_data_end = Some(offset);
        // In variable-width modes, zero means counts are supplied by the
        // accompanying PVAR; it does not imply that every row is biallelic.
        if allele_width > 0 {
            let raw = take(&body, &mut cursor, count * allele_width, "allele counts")?;
            for index in 0..count {
                // The PGEN header stores total allele count verbatim. The
                // pinned pgenlib reader compares this raw value directly with
                // the accompanying PVAR allele-index offset delta.
                let allele_count = read_le(raw, index * allele_width, allele_width)? as usize;
                let absolute_index = block_start_index + index;
                let pvar_allele_count = pvar
                    .get(absolute_index)
                    .map(PvarVariant::allele_count)
                    .unwrap_or(0);
                if allele_count != pvar_allele_count {
                    return Err(DataFusionError::Plan(format!(
                        "PGEN allele count {allele_count} at variant {absolute_index} differs from PVAR allele count {pvar_allele_count}"
                    )));
                }
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
    let data_end =
        record_data_end.unwrap_or_else(|| if external.is_some() { 3 } else { header_end });
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
        RecordIndex::Variable(VariableRecordIndex {
            header: body,
            blocks: blocks.into_boxed_slice(),
            relative_offsets: relative_offsets.into_boxed_slice(),
            ld_base_deltas: ld_base_deltas.into_boxed_slice(),
            variant_count,
            length_width,
            type_width_is_nibble,
        }),
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
    fn decodes_record_descriptors_from_compact_indexes() {
        let mut header = vec![0x20, 0x00];
        for length in [300_u16, 400, 500] {
            header.extend(length.to_le_bytes());
        }
        let mut packed_offsets = Vec::new();
        for offset in [0_u16, 300, 700] {
            packed_offsets.extend(offset.to_le_bytes());
        }
        let index = RecordIndex::Variable(VariableRecordIndex {
            header: Bytes::from(header),
            blocks: vec![VariableRecordBlock {
                first_offset: 1_000,
                type_offset: 0,
                length_offset: 2,
                relative_offset_start: 0,
                relative_offset_width: 2,
                count: 3,
            }]
            .into_boxed_slice(),
            relative_offsets: packed_offsets.into_boxed_slice(),
            ld_base_deltas: vec![0, 1, 0].into_boxed_slice(),
            variant_count: 3,
            length_width: 2,
            type_width_is_nibble: true,
        });
        assert_eq!(index.record(0).unwrap().offset, 1_000);
        let ld = index.record(1).unwrap();
        assert_eq!(
            (ld.offset, ld.length, ld.record_type, ld.ld_base),
            (1_300, 400, 2, Some(0))
        );
        assert_eq!(index.record(2).unwrap().offset, 1_700);

        let fixed = RecordIndex::Fixed {
            count: 3,
            first_offset: 12,
            record_width: 5,
            record_type: 0x40,
        };
        let second = fixed.record(1).unwrap();
        assert_eq!(
            (second.offset, second.length, second.record_type),
            (17, 5, 0x40)
        );
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
        let error = parse_pvar(
            "cohort.pvar",
            b"1 v1 10 C A\nmalformed\n",
            CoordinateSystem::ZeroBasedHalfOpen,
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("max_variants 1"), "{error}");
    }

    /// A body large enough to cross `PARALLEL_PVAR_MIN_BYTES` and be split.
    fn large_pvar(rows: usize, malformed_at: Option<usize>) -> (String, usize) {
        let mut text = String::from("##fileformat=PVARv1.0\n#CHROM\tPOS\tID\tREF\tALT\n");
        let header_lines = 2;
        for row in 0..rows {
            if Some(row) == malformed_at {
                text.push_str("22\tnot-a-position\tbad\tA\tC\n");
            } else {
                let position = row + 1;
                text.push_str(&format!("22\t{position}\trs{row}\tA\tC\n"));
            }
        }
        (text, header_lines)
    }

    /// `rows` valid records, split into exactly `chunks` pieces.
    fn parse_pinned(text: &str, max_variants: usize, chunks: usize) -> Result<Vec<PvarVariant>> {
        parse_pvar_chunked(
            "cohort.pvar",
            text.as_bytes(),
            CoordinateSystem::ZeroBasedHalfOpen,
            max_variants,
            Some(chunks),
        )
    }

    #[test]
    fn enforces_the_row_limit_on_a_body_with_no_bad_line() {
        // Nothing stops any chunk here, so the only thing that can enforce the
        // limit is reconciling the counts as the chunks are joined.
        let (text, _) = large_pvar(400, None);
        assert_eq!(parse_pinned(&text, 400, 8).unwrap().len(), 400);
        let error = parse_pinned(&text, 399, 8).unwrap_err().to_string();
        assert!(error.contains("max_variants 399"), "{error}");
    }

    #[test]
    fn the_row_limit_beats_a_malformed_row_reached_at_the_same_moment() {
        // 400 good rows then a bad one, with the limit at exactly 400: a serial
        // pass checks the limit before reading the bad line, so the limit wins.
        // A chunk cannot see that on its own — it only knows its own rows — so
        // this is what the reconciliation exists for.
        let (mut text, header_lines) = large_pvar(400, None);
        text.push_str("22\tnot-a-position\tbad\tA\tC\n");
        let error = parse_pinned(&text, 400, 8).unwrap_err().to_string();
        assert!(error.contains("max_variants 400"), "{error}");

        // One more allowed row and the bad line is read, so it reports instead.
        let error = parse_pinned(&text, 401, 8).unwrap_err().to_string();
        assert!(
            error.contains(&format!("line {} has invalid POS", header_lines + 401)),
            "{error}"
        );
    }

    #[test]
    fn chunk_ranges_cover_the_body_on_line_boundaries() {
        // Whatever the split points land on, the ranges must tile the body
        // exactly and never cut a line: a chunk that starts mid-line would
        // parse a fragment as a record.
        for body in [
            "".to_string(),
            "a\n".to_string(),
            "a\nbb\nccc\n".to_string(),
            "a\nbb\nccc".to_string(),
            "x\n".repeat(400_000),
            format!("{}\nlast-line-without-newline", "y\n".repeat(400_000)),
        ] {
            let ranges = pvar_chunk_ranges(&body, 8);
            if body.is_empty() {
                assert!(ranges.is_empty());
                continue;
            }
            assert_eq!(ranges.first().unwrap().0, 0);
            assert_eq!(ranges.last().unwrap().1, body.len());
            for window in ranges.windows(2) {
                assert_eq!(window[0].1, window[1].0, "ranges must be contiguous");
            }
            for &(start, end) in &ranges {
                assert!(start < end);
                assert!(start == 0 || body.as_bytes()[start - 1] == b'\n');
                assert!(end == body.len() || body.as_bytes()[end - 1] == b'\n');
            }
            // Splitting must not change how many lines there are.
            let split_lines: usize = ranges
                .iter()
                .map(|&(start, end)| body[start..end].lines().count())
                .sum();
            assert_eq!(split_lines, body.lines().count());
        }
    }

    #[test]
    fn parses_a_split_pvar_identically_to_a_single_chunk() {
        let rows = 60_000;
        let (text, _) = large_pvar(rows, None);
        assert!(
            text.len() > PARALLEL_PVAR_MIN_BYTES,
            "fixture must be large enough to be split"
        );
        let variants = parse_pvar(
            "cohort.pvar",
            text.as_bytes(),
            CoordinateSystem::ZeroBasedHalfOpen,
            usize::MAX,
        )
        .unwrap();
        // Order and content must survive being parsed out of order.
        assert_eq!(variants.len(), rows);
        for (row, variant) in variants.iter().enumerate() {
            assert_eq!(variant.chrom, "22");
            assert_eq!(variant.start, row as u64);
            assert_eq!(variant.id.as_deref(), Some(format!("rs{row}").as_str()));
            assert_eq!(variant.reference, "A");
            assert_eq!(variant.alternate, vec!["C"]);
        }
    }

    #[test]
    fn reports_the_file_line_of_a_malformed_row_in_a_split_body() {
        // The chunk that finds the bad line only knows its own line numbers, so
        // this is what catches a missing chunk offset.
        for malformed_at in [0_usize, 1, 30_000, 59_999] {
            let (text, header_lines) = large_pvar(60_000, Some(malformed_at));
            let error = parse_pvar(
                "cohort.pvar",
                text.as_bytes(),
                CoordinateSystem::ZeroBasedHalfOpen,
                usize::MAX,
            )
            .unwrap_err()
            .to_string();
            let expected = format!("line {} has invalid POS", header_lines + malformed_at + 1);
            assert!(error.contains(&expected), "{error} (expected {expected})");
        }
    }

    #[test]
    fn enforces_the_row_limit_before_a_malformed_row_in_a_split_body() {
        // A serial pass checks the limit before reading the offending line, and
        // the limit is reached in an earlier chunk than the bad line. The
        // reconciliation across chunks has to reproduce that ordering.
        let (text, _) = large_pvar(60_000, Some(50_000));
        let error = parse_pvar(
            "cohort.pvar",
            text.as_bytes(),
            CoordinateSystem::ZeroBasedHalfOpen,
            10_000,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("max_variants 10000"), "{error}");
    }

    #[test]
    fn reports_a_malformed_row_that_precedes_the_row_limit() {
        // The mirror of the test above: the bad line comes first, so it wins.
        let (text, header_lines) = large_pvar(60_000, Some(100));
        let error = parse_pvar(
            "cohort.pvar",
            text.as_bytes(),
            CoordinateSystem::ZeroBasedHalfOpen,
            50_000,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains(&format!("line {} has invalid POS", header_lines + 101)),
            "{error}"
        );
    }

    #[test]
    fn streams_psam_rows_and_enforces_limit_before_parsing_excess_rows() {
        let bytes = b"#FID IID PAT MAT SEX\nf1 i1 0 0 0\nmalformed\n";
        let error = parse_psam("cohort.psam", bytes, 1).unwrap_err().to_string();
        assert!(error.contains("max_samples 1"), "{error}");
    }
}
