use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;

use crate::header::{BgenCompression, BgenHeader, BgenLayout};
use crate::source::ObjectAccess;
use crate::table_provider::BgenReadOptions;

const INITIAL_METADATA_WINDOW: usize = 4 * 1024;
/// Bytes fetched per catalog read so one object read covers many variant records.
const METADATA_CHUNK_BYTES: u64 = 1024 * 1024;
/// Fewest records a read-ahead must be expected to cover to be worth its bytes.
///
/// Metadata and genotype payloads are interleaved, so any read spanning several
/// records also pulls their payloads. That trade is worth making when records
/// are small, and not worth making when one payload already fills the window.
const READ_AHEAD_RECORDS: u64 = 64;
/// Records reserved before any variant has been parsed.
///
/// The declared variant count is untrusted, so it only caps this reservation
/// rather than driving it; the catalog grows normally beyond it.
const INITIAL_VARIANT_CAPACITY: u64 = 65_536;
/// Bytes a Layout 2 payload spends on its own length fields: the block length
/// and, when compressed, the declared decompressed length.
const BLOCK_FRAMING_BYTES: u64 = 8;
/// Worst-case codec expansion allowed on top of the decompressed limit.
///
/// zlib and zstd both bound their expansion on incompressible input to a small
/// fraction of the input plus a fixed header, so allowing a sixteenth plus a
/// kilobyte cannot reject a block a real encoder produced, while still bounding
/// what a malformed record can make the reader download.
const COMPRESSION_EXPANSION_DIVISOR: u64 = 16;
const COMPRESSION_EXPANSION_BYTES: u64 = 1024;

#[derive(Clone, Debug)]
pub(crate) struct BgenVariant {
    pub(crate) index: usize,
    pub(crate) id: Option<String>,
    pub(crate) rsid: Option<String>,
    pub(crate) chrom: String,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) position: u32,
    pub(crate) alleles: Vec<String>,
    pub(crate) record_offset: u64,
    pub(crate) record_size: u64,
    pub(crate) payload_offset: u64,
    pub(crate) payload_size: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct BgenCatalog {
    pub(crate) variants: Arc<Vec<BgenVariant>>,
    pub(crate) bytes_read: u64,
}

/// Sequential read-ahead buffer over the variant records of one BGEN object.
///
/// Variant metadata is small relative to the genotype block that follows it, so
/// fetching one record at a time issues an object read per variant. This buffer
/// fetches [`METADATA_CHUNK_BYTES`] at a time and serves subsequent records from
/// memory, refilling only when a request leaves the buffered window.
struct MetadataWindow<'a> {
    path: &'a str,
    source: &'a ObjectAccess,
    object_size: u64,
    buffer: Bytes,
    buffer_start: u64,
    bytes_read: u64,
    /// Size of the most recently parsed record, used to decide whether reading
    /// ahead would mostly buffer genotype payloads.
    last_record_size: u64,
}

impl<'a> MetadataWindow<'a> {
    fn new(path: &'a str, source: &'a ObjectAccess, object_size: u64) -> Self {
        Self {
            path,
            source,
            object_size,
            buffer: Bytes::new(),
            buffer_start: 0,
            bytes_read: 0,
            last_record_size: 0,
        }
    }

    /// Returns object bytes starting at `offset`, at least `wanted` long unless
    /// the object ends first.
    async fn bytes_at(&mut self, offset: u64, wanted: usize) -> Result<&[u8]> {
        let remaining = self.object_size.saturating_sub(offset);
        let target = (wanted as u64).min(remaining);
        // Refill whenever the window cannot serve the request, including when
        // `wanted` is zero but `offset` sits outside the buffer, so the slice
        // below never depends on the caller having asked for at least one byte.
        if window_slice(&self.buffer, self.buffer_start, offset)
            .is_none_or(|slice| (slice.len() as u64) < target)
        {
            let read_size = remaining.min((wanted as u64).max(self.read_ahead_bytes()));
            let end = offset.checked_add(read_size).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "BGEN {} metadata range overflowed at offset {offset}",
                    sanitize_location(self.path)
                ))
            })?;
            self.buffer = self.source.read_range(self.path, offset..end).await?;
            self.buffer_start = offset;
            self.bytes_read = self.bytes_read.saturating_add(self.buffer.len() as u64);
        }
        window_slice(&self.buffer, self.buffer_start, offset).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "BGEN {} metadata window does not cover offset {offset}",
                sanitize_location(self.path)
            ))
        })
    }

    /// Bytes to fetch beyond what the caller asked for.
    ///
    /// Read-ahead only pays off when one fetch covers many records. Once a
    /// single record is large enough that [`READ_AHEAD_RECORDS`] of them would
    /// not fit, the window stops reading ahead and skips each genotype payload
    /// instead of buffering it.
    fn read_ahead_bytes(&self) -> u64 {
        if self.last_record_size == 0 {
            return METADATA_CHUNK_BYTES;
        }
        match self.last_record_size.checked_mul(READ_AHEAD_RECORDS) {
            Some(span) if span <= METADATA_CHUNK_BYTES => METADATA_CHUNK_BYTES,
            _ => 0,
        }
    }

    /// Records how large the last parsed record was.
    fn observe_record_size(&mut self, record_size: u64) {
        self.last_record_size = record_size;
    }
}

/// Returns the buffered bytes from `offset` onwards.
///
/// `None` means the window starts after `offset` or ends before it, so the
/// caller must refill rather than slice.
fn window_slice(buffer: &[u8], buffer_start: u64, offset: u64) -> Option<&[u8]> {
    let start = offset.checked_sub(buffer_start)?;
    let start = usize::try_from(start).ok()?;
    buffer.get(start..)
}

pub(crate) async fn build_transient_catalog(
    path: &str,
    source: &ObjectAccess,
    header: &BgenHeader,
    options: &BgenReadOptions,
) -> Result<BgenCatalog> {
    // The declared variant count is untrusted, and a large object could still
    // "justify" reserving tens of millions of records before a single record
    // boundary has been validated. Start from a modest reservation and let the
    // vector grow, so a malformed header costs a few reallocations rather than
    // an allocation failure.
    let mut variants =
        Vec::with_capacity((header.variant_count as u64).min(INITIAL_VARIANT_CAPACITY) as usize);
    let mut record_offset = header.first_variant_offset;
    let mut window = MetadataWindow::new(path, source, header.object_size);

    for index in 0..header.variant_count as usize {
        let mut window_size = INITIAL_METADATA_WINDOW.min(options.max_variant_metadata_bytes);
        let variant = loop {
            let remaining = header
                .object_size
                .checked_sub(record_offset)
                .ok_or_else(|| {
                    catalog_error(
                        path,
                        index,
                        record_offset,
                        "record offset exceeds object size",
                    )
                })?;
            if remaining == 0 {
                return Err(catalog_error(
                    path,
                    index,
                    record_offset,
                    "variant catalog ended before the declared variant count",
                ));
            }
            let read_size = remaining.min(window_size as u64);
            let bytes = window.bytes_at(record_offset, window_size).await?;
            // The window serves whatever its read-ahead buffer holds, which is
            // up to METADATA_CHUNK_BYTES and unrelated to `window_size`. Parsing
            // that whole buffer would let a record walk past
            // `max_variant_metadata_bytes` without ever reporting `NeedMore`, so
            // the doubling loop below — and with it the limit — would never run.
            let bytes = &bytes[..bytes.len().min(window_size)];
            match parse_variant(path, index, record_offset, bytes, header, options) {
                Ok(variant) => break variant,
                Err(ParseVariantError::NeedMore) if read_size < remaining => {
                    if window_size >= options.max_variant_metadata_bytes {
                        return Err(catalog_error(
                            path,
                            index,
                            record_offset,
                            &format!(
                                "variant metadata exceeds max_variant_metadata_bytes {}",
                                options.max_variant_metadata_bytes
                            ),
                        ));
                    }
                    window_size = window_size
                        .saturating_mul(2)
                        .min(options.max_variant_metadata_bytes);
                }
                Err(ParseVariantError::NeedMore) => {
                    return Err(catalog_error(
                        path,
                        index,
                        record_offset,
                        "variant metadata is truncated",
                    ));
                }
                Err(ParseVariantError::Invalid(error)) => {
                    return Err(catalog_error(
                        path,
                        index,
                        record_offset,
                        &error.to_string(),
                    ));
                }
            }
        };
        window.observe_record_size(variant.record_size);
        record_offset = variant
            .record_offset
            .checked_add(variant.record_size)
            .ok_or_else(|| {
                catalog_error(
                    path,
                    index,
                    variant.record_offset,
                    "record end arithmetic overflowed",
                )
            })?;
        variants.push(variant);
    }

    if record_offset != header.object_size {
        return Err(DataFusionError::Plan(format!(
            "BGEN {} has trailing or unaccounted bytes: parsed variants end at {record_offset}, object size is {}",
            sanitize_location(path),
            header.object_size
        )));
    }

    Ok(BgenCatalog {
        variants: Arc::new(variants),
        bytes_read: window.bytes_read,
    })
}

enum ParseVariantError {
    NeedMore,
    Invalid(DataFusionError),
}

fn parse_variant(
    path: &str,
    index: usize,
    record_offset: u64,
    bytes: &[u8],
    header: &BgenHeader,
    options: &BgenReadOptions,
) -> std::result::Result<BgenVariant, ParseVariantError> {
    let mut cursor = SliceCursor::new(bytes);
    if header.layout == BgenLayout::Layout1 {
        let count = cursor.u32()?;
        if count != header.sample_count {
            return Err(ParseVariantError::Invalid(catalog_error(
                path,
                index,
                record_offset,
                &format!(
                    "Layout 1 sample count {count} differs from header count {}",
                    header.sample_count
                ),
            )));
        }
    }
    let id = cursor.string_u16(options.max_string_bytes, "variant ID")?;
    let rsid = cursor.string_u16(options.max_string_bytes, "RS identifier")?;
    let chrom = cursor.string_u16(options.max_string_bytes, "chromosome")?;
    let position = cursor.u32()?;
    let allele_count = match header.layout {
        BgenLayout::Layout1 => 2,
        BgenLayout::Layout2 => cursor.u16()? as usize,
    };
    if allele_count == 0 || allele_count > options.max_alleles {
        return Err(ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            &format!(
                "allele count {allele_count} is outside supported range 1..={}",
                options.max_alleles
            ),
        )));
    }
    let mut alleles = Vec::with_capacity(allele_count);
    for allele_index in 0..allele_count {
        let allele = cursor.string_u32(options.max_string_bytes, "allele")?;
        if allele.is_empty() {
            return Err(ParseVariantError::Invalid(catalog_error(
                path,
                index,
                record_offset,
                &format!("allele {allele_index} is empty"),
            )));
        }
        alleles.push(allele);
    }
    let payload_offset = record_offset
        .checked_add(cursor.position as u64)
        .ok_or_else(|| {
            ParseVariantError::Invalid(catalog_error(
                path,
                index,
                record_offset,
                "payload offset arithmetic overflowed",
            ))
        })?;
    let payload_size = match (header.layout, header.compression) {
        (BgenLayout::Layout1, BgenCompression::None) => u64::from(header.sample_count)
            .checked_mul(6)
            .ok_or_else(|| {
                ParseVariantError::Invalid(catalog_error(
                    path,
                    index,
                    record_offset,
                    "Layout 1 payload size arithmetic overflowed",
                ))
            })?,
        _ => {
            let compressed_or_block_size = u64::from(cursor.u32()?);
            compressed_or_block_size.checked_add(4).ok_or_else(|| {
                ParseVariantError::Invalid(catalog_error(
                    path,
                    index,
                    record_offset,
                    "payload size arithmetic overflowed",
                ))
            })?
        }
    };
    // For an uncompressed block the payload is the decompressed data, so the
    // configured limit can be applied before a range read downloads it. A
    // compressed block carries its decompressed length inside the payload, so
    // that case is still checked during decoding.
    let declared_uncompressed = match (header.layout, header.compression) {
        (BgenLayout::Layout1, BgenCompression::None) => Some(payload_size),
        // Layout 2 prefixes the block with its own four-byte length.
        (BgenLayout::Layout2, BgenCompression::None) => payload_size.checked_sub(4),
        _ => None,
    };
    // Every payload is also bounded before it is read, so a record declaring a
    // multi-gigabyte serialized block cannot make execution download it.
    //
    // Compression does not guarantee the serialized form is smaller than its
    // input, so the ceiling allows for codec expansion on top of the
    // decompressed limit rather than assuming compression shrinks the block.
    let limit = options.max_decompressed_block_bytes as u64;
    let serialized_ceiling = limit
        .saturating_add(limit / COMPRESSION_EXPANSION_DIVISOR)
        .saturating_add(COMPRESSION_EXPANSION_BYTES)
        .saturating_add(BLOCK_FRAMING_BYTES);
    if payload_size > serialized_ceiling {
        return Err(ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            &format!(
                "serialized payload of {payload_size} bytes exceeds max_decompressed_block_bytes {} plus codec expansion and framing",
                options.max_decompressed_block_bytes
            ),
        )));
    }
    if let Some(size) = declared_uncompressed
        && size > options.max_decompressed_block_bytes as u64
    {
        return Err(ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            &format!(
                "declared block of {size} bytes exceeds max_decompressed_block_bytes {}",
                options.max_decompressed_block_bytes
            ),
        )));
    }
    let metadata_size = payload_offset.checked_sub(record_offset).ok_or_else(|| {
        ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            "metadata size arithmetic underflowed",
        ))
    })?;
    let record_size = metadata_size.checked_add(payload_size).ok_or_else(|| {
        ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            "record size arithmetic overflowed",
        ))
    })?;
    let record_end = record_offset.checked_add(record_size).ok_or_else(|| {
        ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            "record end arithmetic overflowed",
        ))
    })?;
    if record_end > header.object_size {
        return Err(ParseVariantError::Invalid(catalog_error(
            path,
            index,
            record_offset,
            &format!(
                "variant block ends at {record_end}, beyond object size {}",
                header.object_size
            ),
        )));
    }
    let coordinates = options
        .coordinate_system
        .site(position.into())
        .map_err(|error| {
            ParseVariantError::Invalid(catalog_error(
                path,
                index,
                record_offset,
                &format!("invalid one-based position {position}: {error}"),
            ))
        })?;

    Ok(BgenVariant {
        index,
        id: (!id.is_empty()).then_some(id),
        rsid: (!rsid.is_empty()).then_some(rsid),
        chrom,
        start: coordinates.start,
        end: coordinates.end,
        position,
        alleles,
        record_offset,
        record_size,
        payload_offset,
        payload_size,
    })
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> std::result::Result<&'a [u8], ParseVariantError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ParseVariantError::NeedMore)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ParseVariantError::NeedMore)?;
        self.position = end;
        Ok(value)
    }

    fn u16(&mut self) -> std::result::Result<u16, ParseVariantError> {
        let value = self.take(2)?;
        Ok(u16::from_le_bytes([value[0], value[1]]))
    }

    fn u32(&mut self) -> std::result::Result<u32, ParseVariantError> {
        let value = self.take(4)?;
        Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
    }

    fn string_u16(
        &mut self,
        max_length: usize,
        role: &str,
    ) -> std::result::Result<String, ParseVariantError> {
        let length = self.u16()? as usize;
        self.string(length, max_length, role)
    }

    fn string_u32(
        &mut self,
        max_length: usize,
        role: &str,
    ) -> std::result::Result<String, ParseVariantError> {
        let length = usize::try_from(self.u32()?).map_err(|_| ParseVariantError::NeedMore)?;
        self.string(length, max_length, role)
    }

    fn string(
        &mut self,
        length: usize,
        max_length: usize,
        role: &str,
    ) -> std::result::Result<String, ParseVariantError> {
        if length > max_length {
            return Err(ParseVariantError::Invalid(DataFusionError::Plan(format!(
                "{role} length {length} exceeds configured maximum {max_length}"
            ))));
        }
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|error| {
                ParseVariantError::Invalid(DataFusionError::Plan(format!(
                    "{role} is not valid UTF-8: {error}"
                )))
            })
    }
}

fn catalog_error(path: &str, index: usize, offset: u64, message: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "BGEN {} variant {index} at byte {offset}: {message}",
        sanitize_location(path)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_slice_covers_only_the_buffered_range() {
        let buffer = [10_u8, 11, 12, 13];
        // Before the window starts.
        assert_eq!(window_slice(&buffer, 100, 99), None);
        // Exactly at the start.
        assert_eq!(window_slice(&buffer, 100, 100), Some(&buffer[..]));
        // Inside the window.
        assert_eq!(window_slice(&buffer, 100, 102), Some(&buffer[2..]));
        // One past the last byte is an empty slice, not a miss: the window still
        // describes that position.
        assert_eq!(window_slice(&buffer, 100, 104), Some(&buffer[4..]));
        // Beyond the window.
        assert_eq!(window_slice(&buffer, 100, 105), None);
    }

    #[test]
    fn read_ahead_stops_once_one_record_fills_the_window() {
        let source = ObjectAccess::Local("unused".to_string());
        let mut window = MetadataWindow::new("unused", &source, 1 << 30);
        // Nothing parsed yet: read ahead so the first fetch covers many records.
        assert_eq!(window.read_ahead_bytes(), METADATA_CHUNK_BYTES);
        // Small records: reading ahead covers at least READ_AHEAD_RECORDS.
        window.observe_record_size(METADATA_CHUNK_BYTES / READ_AHEAD_RECORDS);
        assert_eq!(window.read_ahead_bytes(), METADATA_CHUNK_BYTES);
        // One byte larger and the read-ahead would mostly buffer payloads.
        window.observe_record_size(METADATA_CHUNK_BYTES / READ_AHEAD_RECORDS + 1);
        assert_eq!(window.read_ahead_bytes(), 0);
        // A record larger than the whole window never reads ahead.
        window.observe_record_size(u64::MAX);
        assert_eq!(window.read_ahead_bytes(), 0);
    }

    #[test]
    fn window_slice_rejects_an_offset_that_does_not_fit_usize() {
        let buffer = [0_u8; 4];
        assert_eq!(window_slice(&buffer, 0, u64::MAX), None);
    }
}
