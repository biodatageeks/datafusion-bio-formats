use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;

use crate::source::ObjectAccess;
use crate::table_provider::BgenReadOptions;

const FIXED_PREFIX_BYTES: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BgenLayout {
    Layout1,
    Layout2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BgenCompression {
    None,
    Zlib,
    Zstd,
}

#[derive(Debug, Clone)]
pub(crate) struct BgenHeader {
    pub(crate) variant_count: u32,
    pub(crate) sample_count: u32,
    pub(crate) first_variant_offset: u64,
    pub(crate) layout: BgenLayout,
    pub(crate) compression: BgenCompression,
    pub(crate) sample_names: Vec<String>,
    pub(crate) synthetic_sample_names: bool,
    pub(crate) object_size: u64,
    /// Bytes read from an external sample companion, if one was used.
    ///
    /// Taken from that companion's own handle, which counts what it returns, so
    /// a preliminary read cannot be left out. Bytes read from the BGEN object
    /// itself are not here: they are on the handle the caller passed in, which
    /// the provider reads once construction is done.
    pub(crate) companion_sample_bytes: u64,
    /// Requests issued against an external sample companion.
    pub(crate) companion_sample_requests: u64,
}

impl BgenHeader {
    pub(crate) async fn read(
        path: &str,
        source: &ObjectAccess,
        options: &BgenReadOptions,
    ) -> Result<Self> {
        let object_size = source.size(path).await?;
        if object_size < 24 {
            return Err(plan_error(
                path,
                "file is shorter than the minimum BGEN header",
            ));
        }
        let prefix = source.read_range(path, 0..FIXED_PREFIX_BYTES).await?;
        let offset = u32_at(&prefix, 0)? as u64;
        let header_length = u32_at(&prefix, 4)? as u64;
        let variant_count = u32_at(&prefix, 8)?;
        let sample_count = u32_at(&prefix, 12)?;
        let magic = prefix.get(16..20).ok_or_else(|| {
            plan_error(
                path,
                "BGEN header is truncated while reading the magic value",
            )
        })?;

        if header_length < 20 {
            return Err(plan_error(
                path,
                &format!("header length {header_length} is smaller than 20"),
            ));
        }
        if header_length > options.max_header_bytes as u64 {
            return Err(plan_error(
                path,
                &format!(
                    "header length {header_length} exceeds max_header_bytes {}",
                    options.max_header_bytes
                ),
            ));
        }
        if header_length > offset {
            return Err(plan_error(
                path,
                &format!("header length {header_length} exceeds first-variant offset {offset}"),
            ));
        }
        let first_variant_offset = offset
            .checked_add(4)
            .ok_or_else(|| plan_error(path, "first-variant offset arithmetic overflowed"))?;
        if first_variant_offset > object_size {
            return Err(plan_error(
                path,
                &format!(
                    "first variant offset {first_variant_offset} exceeds object size {object_size}"
                ),
            ));
        }
        if variant_count as usize > options.max_variants {
            return Err(plan_error(
                path,
                &format!(
                    "declared variant count {variant_count} exceeds max_variants {}",
                    options.max_variants
                ),
            ));
        }
        if sample_count as usize > options.max_samples {
            return Err(plan_error(
                path,
                &format!(
                    "declared sample count {sample_count} exceeds max_samples {}",
                    options.max_samples
                ),
            ));
        }
        if magic != b"bgen" && magic != [0, 0, 0, 0] {
            return Err(plan_error(
                path,
                &format!("invalid BGEN magic bytes {magic:02x?}"),
            ));
        }

        let full_header_end = 4_u64
            .checked_add(header_length)
            .ok_or_else(|| plan_error(path, "header boundary arithmetic overflowed"))?;
        let header = source.read_range(path, 0..full_header_end).await?;
        let flags_offset = usize::try_from(header_length)
            .map_err(|_| plan_error(path, "header length does not fit memory address space"))?;
        let flags = u32_at(&header, flags_offset)?;
        let reserved_mask = !((0b11_u32) | (0b1111_u32 << 2) | (1_u32 << 31));
        if flags & reserved_mask != 0 {
            return Err(plan_error(
                path,
                &format!("unsupported non-zero reserved BGEN flags: 0x{flags:08x}"),
            ));
        }
        let compression = match flags & 0b11 {
            0 => BgenCompression::None,
            1 => BgenCompression::Zlib,
            2 => BgenCompression::Zstd,
            value => {
                return Err(plan_error(
                    path,
                    &format!("unsupported BGEN compression flag {value}"),
                ));
            }
        };
        let layout = match (flags >> 2) & 0b1111 {
            1 => BgenLayout::Layout1,
            2 => BgenLayout::Layout2,
            value => {
                return Err(plan_error(
                    path,
                    &format!("unsupported BGEN layout {value}; expected Layout 1 or Layout 2"),
                ));
            }
        };
        if layout == BgenLayout::Layout1 && compression == BgenCompression::Zstd {
            return Err(plan_error(
                path,
                "Layout 1 does not support zstd-compressed probability blocks",
            ));
        }

        // Bytes read from an external sample companion are companion I/O, not
        // primary object I/O, so they are tracked separately.
        let mut companion_sample_bytes = 0_u64;
        let mut companion_sample_requests = 0_u64;
        let has_embedded_samples = flags & (1_u32 << 31) != 0;
        let (sample_names, synthetic_sample_names, _) = if has_embedded_samples {
            let sample_block_start = full_header_end;
            let prefix_end = sample_block_start
                .checked_add(8)
                .ok_or_else(|| plan_error(path, "sample block boundary arithmetic overflowed"))?;
            if prefix_end > first_variant_offset {
                return Err(plan_error(
                    path,
                    "embedded sample block overlaps variant data",
                ));
            }
            let sample_prefix = source
                .read_range(path, sample_block_start..prefix_end)
                .await?;
            let block_length = u32_at(&sample_prefix, 0)? as u64;
            if block_length < 8 {
                return Err(plan_error(
                    path,
                    "embedded sample block length is smaller than 8",
                ));
            }
            let block_end = sample_block_start
                .checked_add(block_length)
                .ok_or_else(|| plan_error(path, "sample block length arithmetic overflowed"))?;
            if block_end > first_variant_offset {
                return Err(plan_error(
                    path,
                    "embedded sample block overlaps variant data",
                ));
            }
            if block_length > options.max_sample_block_bytes as u64 {
                return Err(plan_error(
                    path,
                    &format!(
                        "sample block length {block_length} exceeds max_sample_block_bytes {}",
                        options.max_sample_block_bytes
                    ),
                ));
            }
            let block = source
                .read_range(path, sample_block_start..block_end)
                .await?;
            (
                parse_sample_block(path, &block, sample_count, options.max_string_bytes)?,
                false,
                block_length,
            )
        } else if let Some(sample_path) = &options.sample_path {
            let (names, bytes, requests) =
                read_external_samples(sample_path, sample_count, options).await?;
            companion_sample_bytes = bytes;
            companion_sample_requests = requests;
            (names, false, 0)
        } else {
            (
                (1..=sample_count)
                    .map(|index| format!("sample_{index}"))
                    .collect(),
                true,
                0,
            )
        };

        Ok(Self {
            variant_count,
            sample_count,
            first_variant_offset,
            layout,
            compression,
            sample_names,
            synthetic_sample_names,
            object_size,
            companion_sample_bytes,
            companion_sample_requests,
        })
    }
}

async fn read_external_samples(
    path: &str,
    expected_count: u32,
    options: &BgenReadOptions,
) -> Result<(Vec<String>, u64, u64)> {
    let source = ObjectAccess::open(
        path,
        &options.object_storage_options.clone().unwrap_or_default(),
    )
    .await
    .map_err(|error| {
        DataFusionError::Plan(format!(
            "open BGEN sample companion {}: {error}",
            sanitize_location(path)
        ))
    })?;
    let bytes = source
        .read_all_bounded(path, options.max_sample_block_bytes)
        .await?;
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        plan_error(
            path,
            &format!("sample companion is not valid UTF-8: {error}"),
        )
    })?;
    let nonempty: Vec<_> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .collect();
    let qctool = nonempty
        .first()
        .is_some_and(|(_, line)| line.split_whitespace().take(2).eq(["ID_1", "ID_2"]));
    let start = if qctool {
        if nonempty.len() < 2 {
            return Err(plan_error(
                path,
                "qctool sample file is missing its type row",
            ));
        }
        2
    } else {
        0
    };
    let mut names = Vec::with_capacity(nonempty.len().saturating_sub(start));
    for (line_number, line) in nonempty.into_iter().skip(start) {
        let fields: Vec<_> = line.split_whitespace().collect();
        let name = if qctool {
            fields.get(1).copied()
        } else if fields.len() == 1 {
            fields.first().copied()
        } else {
            None
        }
        .ok_or_else(|| {
            plan_error(
                path,
                &format!("invalid sample companion row {}", line_number + 1),
            )
        })?;
        if name.len() > options.max_string_bytes {
            return Err(plan_error(
                path,
                &format!(
                    "sample ID on line {} exceeds max_string_bytes",
                    line_number + 1
                ),
            ));
        }
        names.push(name.to_string());
    }
    // From the handle rather than from `bytes`: it counts the size request and
    // every read, including the one `read_all_bounded` makes to size the object.
    Ok((
        validate_sample_names(path, names, expected_count)?,
        source.bytes(),
        source.requests(),
    ))
}

fn parse_sample_block(
    path: &str,
    bytes: &[u8],
    expected_count: u32,
    max_string_bytes: usize,
) -> Result<Vec<String>> {
    let declared_length = u32_at(bytes, 0)? as usize;
    if declared_length != bytes.len() {
        return Err(plan_error(
            path,
            &format!(
                "embedded sample block length mismatch: declared {declared_length}, read {}",
                bytes.len()
            ),
        ));
    }
    let count = u32_at(bytes, 4)?;
    if count != expected_count {
        return Err(plan_error(
            path,
            &format!(
                "embedded sample count {count} differs from BGEN header count {expected_count}"
            ),
        ));
    }
    let mut cursor = 8_usize;
    let mut names = Vec::with_capacity(count as usize);
    for index in 0..count {
        let length = u16_at(bytes, cursor)? as usize;
        cursor = cursor
            .checked_add(2)
            .ok_or_else(|| plan_error(path, "sample identifier offset arithmetic overflowed"))?;
        if length > max_string_bytes {
            return Err(plan_error(
                path,
                &format!("sample identifier {index} exceeds max_string_bytes"),
            ));
        }
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| plan_error(path, "sample identifier length arithmetic overflowed"))?;
        let value = bytes.get(cursor..end).ok_or_else(|| {
            plan_error(
                path,
                &format!("embedded sample identifier {index} is truncated"),
            )
        })?;
        names.push(
            std::str::from_utf8(value)
                .map_err(|error| {
                    plan_error(
                        path,
                        &format!("sample identifier {index} is not valid UTF-8: {error}"),
                    )
                })?
                .to_string(),
        );
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(plan_error(path, "embedded sample block has trailing bytes"));
    }
    validate_sample_names(path, names, expected_count)
}

fn validate_sample_names(
    path: &str,
    names: Vec<String>,
    expected_count: u32,
) -> Result<Vec<String>> {
    if names.len() != expected_count as usize {
        return Err(plan_error(
            path,
            &format!(
                "sample companion count {} differs from BGEN header count {expected_count}",
                names.len()
            ),
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    for name in &names {
        if name.is_empty() {
            return Err(plan_error(path, "sample identifiers must not be empty"));
        }
        if !seen.insert(name.as_str()) {
            return Err(plan_error(
                path,
                &format!("duplicate BGEN sample identifier: {name}"),
            ));
        }
    }
    Ok(names)
}

pub(crate) fn u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| DataFusionError::Plan("BGEN u16 offset overflowed".to_string()))?;
    let value = bytes.get(offset..end).ok_or_else(|| {
        DataFusionError::Plan(format!("truncated little-endian u16 at byte {offset}"))
    })?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

pub(crate) fn u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| DataFusionError::Plan("BGEN u32 offset overflowed".to_string()))?;
    let value = bytes.get(offset..end).ok_or_else(|| {
        DataFusionError::Plan(format!("truncated little-endian u32 at byte {offset}"))
    })?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn plan_error(path: &str, message: &str) -> DataFusionError {
    DataFusionError::Plan(format!("BGEN {}: {message}", sanitize_location(path)))
}
