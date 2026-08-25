//! Direct-chunk fast path for 1-D chunked pixel datasets.
//!
//! libhdf5 serializes every read behind a process-wide lock, and gzip+shuffle
//! decoding happens inside that lock — which caps parallel scan speedups near
//! 2x. This module records each chunk's file address once (under the lock,
//! via `H5Dchunk_iter`), then serves reads with plain file I/O, zlib-rs
//! inflation, and byte unshuffling in Rust — no libhdf5 in the data path, so
//! partitions decode truly concurrently.
//!
//! Scope: 1-D chunked datasets whose filter pipeline is empty, `[Deflate]`,
//! or `[Shuffle, Deflate]` (what cooler/h5py write). Anything else — or any
//! disagreement with a libhdf5 reference read of the first chunk, checked at
//! index-build time — falls back to the ordinary hdf5 path.

use std::fs::File;
use std::io::Read;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use flate2::read::ZlibDecoder;
use hdf5_metno::Dataset;
use hdf5_metno::datatype::ByteOrder;
use hdf5_metno::filters::Filter;

use crate::hdf5_utils::h5_err;

/// Direct indexes are an optimization and must never consume unbounded memory
/// from an untrusted logical dataset shape. This permits millions of chunks
/// while keeping the worst-case dense index allocation below 64 MiB.
const MAX_CHUNK_INDEX_BYTES: usize = 64 * 1024 * 1024;

/// One chunk's location in the file.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChunkLoc {
    addr: u64,
    size: usize,
    filter_mask: u32,
}

/// Byte-level index of a 1-D chunked dataset.
#[derive(Debug)]
pub(crate) struct ChunkedColumn {
    pub elem_size: usize,
    pub chunk_elems: usize,
    pub n_elems: usize,
    shuffle: bool,
    deflate: bool,
    chunks: Vec<ChunkLoc>,
}

/// Positions of the filters in the stored pipeline, for `filter_mask` bits.
#[derive(Clone, Copy)]
struct FilterPositions {
    shuffle: Option<usize>,
    deflate: Option<usize>,
}

/// Build the chunk index for a dataset, or `None` when the layout is outside
/// the fast path's scope. Runs under the libhdf5 lock; call once per scan.
pub(crate) fn index_column(ds: &Dataset) -> Option<Arc<ChunkedColumn>> {
    let shape = ds.shape();
    if shape.len() != 1 {
        return None;
    }
    let n_elems = shape[0];
    let chunk = ds.chunk()?;
    if chunk.len() != 1 || chunk[0] == 0 {
        return None;
    }
    let chunk_elems = chunk[0];
    let dtype = ds.dtype().ok()?;
    // Direct conversion below is deliberately little-endian. Inspect the
    // stored datatype instead of inferring its order from probe values: zero
    // and other byte-symmetric leading values cannot distinguish byte order.
    if !supports_direct_byte_order(dtype.byte_order()) {
        return None;
    }
    let elem_size = dtype.size();
    let chunk_bytes = chunk_elems.checked_mul(elem_size)?;
    let file_size = std::fs::metadata(ds.filename()).ok()?.len();
    let filters = ds.filters();
    let mut positions = FilterPositions {
        shuffle: None,
        deflate: None,
    };
    for (index, filter) in filters.iter().enumerate() {
        match filter {
            Filter::Shuffle => positions.shuffle = Some(index),
            Filter::Deflate(_) => positions.deflate = Some(index),
            _ => return None,
        }
    }
    // Decode order is the inverse of the write pipeline: inflate first, then
    // unshuffle. That only holds when shuffle precedes deflate on write.
    if let (Some(shuffle), Some(deflate)) = (positions.shuffle, positions.deflate)
        && shuffle > deflate
    {
        return None;
    }

    let expected = n_elems.div_ceil(chunk_elems);
    if !chunk_index_within_budget(expected, file_size) {
        return None;
    }
    let mut chunks = vec![
        ChunkLoc {
            addr: u64::MAX,
            size: 0,
            filter_mask: 0,
        };
        expected
    ];
    let mut malformed = false;
    ds.chunks_visit(|info| {
        if info.offset.len() != 1 {
            malformed = true;
            return 1;
        }
        let Ok(offset) = usize::try_from(info.offset[0]) else {
            malformed = true;
            return 1;
        };
        let index = offset / chunk_elems;
        let deflate_applied = positions
            .deflate
            .is_some_and(|position| info.filter_mask & (1_u32 << position) == 0);
        let Some(size) = valid_stored_chunk_size(
            info.addr,
            info.size,
            chunk_bytes,
            deflate_applied,
            file_size,
        ) else {
            malformed = true;
            return 1;
        };
        if index >= expected {
            malformed = true;
            return 1;
        }
        chunks[index] = ChunkLoc {
            addr: info.addr,
            size,
            filter_mask: info.filter_mask,
        };
        0
    })
    .ok()?;
    // Every chunk must be allocated: a chunk never written has no address and
    // would need fill-value handling the fast path does not implement.
    if malformed
        || chunks
            .iter()
            .any(|chunk| chunk.addr == u64::MAX || chunk.size == 0)
    {
        return None;
    }
    // With both filters present, this decoder only supports chunks to which
    // the complete shuffle+deflate pipeline was applied. Reject the column up
    // front if any optional filter was skipped so execution uses libhdf5 for
    // every chunk instead of failing only when it reaches a later masked one.
    if positions.shuffle.is_some()
        && positions.deflate.is_some()
        && chunks.iter().any(|chunk| chunk.filter_mask != 0)
    {
        return None;
    }

    let column = Arc::new(ChunkedColumn {
        elem_size,
        chunk_elems,
        n_elems,
        shuffle: positions.shuffle.is_some(),
        deflate: positions.deflate.is_some(),
        chunks,
    });
    Some(column)
}

fn supports_direct_byte_order(byte_order: ByteOrder) -> bool {
    byte_order == ByteOrder::LittleEndian
}

fn chunk_index_within_budget(chunk_count: usize, file_size: u64) -> bool {
    let Some(index_bytes) = chunk_count.checked_mul(std::mem::size_of::<ChunkLoc>()) else {
        return false;
    };
    let Ok(chunk_count) = u64::try_from(chunk_count) else {
        return false;
    };
    index_bytes <= MAX_CHUNK_INDEX_BYTES && chunk_count <= file_size
}

/// Validate a stored chunk extent before retaining it in the direct index.
///
/// A deflate stream emitted by ordinary HDF5/zlib should stay close to its
/// decoded size even for incompressible input. The deliberately conservative
/// 12.5% plus 64-byte allowance exceeds standard zlib bounds while rejecting
/// attacker-controlled multi-gigabyte extents. Unfiltered chunks must have
/// exactly the decoded chunk size. Any unusual but valid stream falls back to
/// libhdf5 rather than entering the optimized path.
fn valid_stored_chunk_size(
    addr: u64,
    stored_size: u64,
    decoded_size: usize,
    deflate_applied: bool,
    file_size: u64,
) -> Option<usize> {
    let stored_limit = if deflate_applied {
        decoded_size
            .checked_add(decoded_size.div_ceil(8))?
            .checked_add(64)?
    } else {
        decoded_size
    };
    let size = usize::try_from(stored_size).ok()?;
    let end = addr.checked_add(stored_size)?;
    (size > 0 && size <= stored_limit && end <= file_size).then_some(size)
}

/// Verify the fast path against a libhdf5 reference read of the leading
/// elements. Returns false (→ caller falls back) on any disagreement.
pub(crate) fn validate_against_reference(
    column: &ChunkedColumn,
    file_path: &str,
    reference_le_bytes: &[u8],
) -> bool {
    let probe_elems = reference_le_bytes.len() / column.elem_size;
    if probe_elems == 0 {
        return true;
    }
    let Ok(mut reader) = ChunkReader::open(file_path) else {
        return false;
    };
    let mut bytes = Vec::new();
    if reader
        .read_range(column, 0, probe_elems, &mut bytes)
        .is_err()
    {
        return false;
    }
    bytes == reference_le_bytes
}

/// Sequential reader for one stream/partition. Owns its file handle and
/// scratch buffers; safe to use from any thread without shared locks.
pub(crate) struct ChunkReader {
    file: File,
    compressed: Vec<u8>,
    inflated: Vec<u8>,
    unshuffled: Vec<u8>,
}

impl ChunkReader {
    pub(crate) fn open(file_path: &str) -> Result<Self> {
        let file = File::open(file_path).map_err(|error| {
            h5_err(
                &format!("Failed to open '{file_path}' for chunk reads"),
                error,
            )
        })?;
        Ok(Self {
            file,
            compressed: Vec::new(),
            inflated: Vec::new(),
            unshuffled: Vec::new(),
        })
    }

    /// Read elements `[lo, hi)` into `out` as raw little-endian bytes.
    pub(crate) fn read_range(
        &mut self,
        column: &ChunkedColumn,
        lo: usize,
        hi: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        out.clear();
        out.reserve((hi - lo) * column.elem_size);
        let chunk_bytes = column.chunk_elems * column.elem_size;
        for chunk_index in (lo / column.chunk_elems)..=((hi - 1) / column.chunk_elems) {
            let loc = column.chunks[chunk_index];
            self.compressed.resize(loc.size, 0);
            self.file
                .seek(SeekFrom::Start(loc.addr))
                .and_then(|_| self.file.read_exact(&mut self.compressed))
                .map_err(|error| h5_err(&format!("Failed to read chunk {chunk_index}"), error))?;

            // filter_mask bit i set == pipeline filter i was skipped for this chunk
            let deflate = column.deflate && loc.filter_mask & 0b01 == 0;
            let shuffle_bit = if column.deflate { 0b10 } else { 0b01 };
            let shuffle = column.shuffle && loc.filter_mask & shuffle_bit == 0;
            if column.deflate && column.shuffle && loc.filter_mask != 0 {
                // Mixed per-chunk filter skips are vanishingly rare; decoding
                // them correctly needs pipeline-position bookkeeping we don't
                // carry, so refuse instead of risking garbage.
                return Err(DataFusionError::Internal(format!(
                    "cooler fast path: unsupported per-chunk filter mask {:#x}",
                    loc.filter_mask
                )));
            }

            let decoded: &[u8] = if deflate {
                self.inflated.clear();
                let inflation_limit = u64::try_from(chunk_bytes)
                    .ok()
                    .and_then(|size| size.checked_add(1))
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "cooler fast path: chunk {chunk_index} size limit overflow"
                        ))
                    })?;
                ZlibDecoder::new(self.compressed.as_slice())
                    .take(inflation_limit)
                    .read_to_end(&mut self.inflated)
                    .map_err(|error| {
                        h5_err(&format!("Failed to inflate chunk {chunk_index}"), error)
                    })?;
                &self.inflated
            } else {
                &self.compressed
            };
            // Filtered edge chunks are stored at full chunk size (padded).
            if decoded.len() != chunk_bytes {
                return Err(DataFusionError::Internal(format!(
                    "cooler fast path: chunk {chunk_index} decoded to {} bytes, expected {chunk_bytes}",
                    decoded.len()
                )));
            }
            let decoded: &[u8] = if shuffle {
                self.unshuffled.resize(chunk_bytes, 0);
                unshuffle(decoded, column.elem_size, &mut self.unshuffled);
                &self.unshuffled
            } else {
                decoded
            };

            let chunk_start = chunk_index * column.chunk_elems;
            let local_lo = lo.max(chunk_start) - chunk_start;
            let local_hi = (hi.min(chunk_start + column.chunk_elems)) - chunk_start;
            out.extend_from_slice(
                &decoded[local_lo * column.elem_size..local_hi * column.elem_size],
            );
        }
        Ok(())
    }
}

/// Invert HDF5's shuffle filter: input is grouped by byte plane
/// (`plane[b][i]` = byte `b` of element `i`), output is element-contiguous.
fn unshuffle(src: &[u8], elem_size: usize, dst: &mut [u8]) {
    let n = src.len() / elem_size;
    for byte in 0..elem_size {
        let plane = &src[byte * n..(byte + 1) * n];
        let mut position = byte;
        for &value in plane {
            dst[position] = value;
            position += elem_size;
        }
    }
}

pub(crate) fn bytes_to_i64(bytes: &[u8]) -> Vec<i64> {
    bytes
        .chunks_exact(8)
        .map(|b| i64::from_le_bytes(b.try_into().expect("8-byte chunk")))
        .collect()
}

pub(crate) fn bytes_to_i32(bytes: &[u8]) -> Vec<i32> {
    bytes
        .chunks_exact(4)
        .map(|b| i32::from_le_bytes(b.try_into().expect("4-byte chunk")))
        .collect()
}

pub(crate) fn bytes_to_u64(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().expect("8-byte chunk")))
        .collect()
}

pub(crate) fn bytes_to_u32(bytes: &[u8]) -> Vec<u32> {
    bytes
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().expect("4-byte chunk")))
        .collect()
}

pub(crate) fn bytes_to_f64(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|b| f64::from_le_bytes(b.try_into().expect("8-byte chunk")))
        .collect()
}

/// Per-column fast indexes for the pixels table; `None` columns fall back to
/// ordinary hdf5 reads.
#[derive(Debug, Default)]
pub(crate) struct FastPixels {
    pub bin1: Option<Arc<ChunkedColumn>>,
    pub bin2: Option<Arc<ChunkedColumn>>,
    pub count: Option<Arc<ChunkedColumn>>,
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn direct_path_requires_explicit_little_endian_metadata() {
        assert!(supports_direct_byte_order(ByteOrder::LittleEndian));
        assert!(!supports_direct_byte_order(ByteOrder::BigEndian));
        assert!(!supports_direct_byte_order(ByteOrder::Vax));
        assert!(!supports_direct_byte_order(ByteOrder::Mixed));
        assert!(!supports_direct_byte_order(ByteOrder::None));
    }

    #[test]
    fn logical_chunk_index_is_bounded_before_dense_allocation() {
        let maximum_chunks = MAX_CHUNK_INDEX_BYTES / std::mem::size_of::<ChunkLoc>();

        assert!(chunk_index_within_budget(
            maximum_chunks,
            maximum_chunks as u64
        ));
        assert!(!chunk_index_within_budget(maximum_chunks + 1, u64::MAX));
        assert!(!chunk_index_within_budget(10_000, 9_999));
        assert!(!chunk_index_within_budget(usize::MAX, u64::MAX));
    }

    #[test]
    fn oversized_or_out_of_file_stored_chunks_are_rejected_before_indexing() {
        let decoded_size: usize = 64;
        let allowed = decoded_size + decoded_size.div_ceil(8) + 64;

        assert_eq!(
            valid_stored_chunk_size(10, allowed as u64, decoded_size, true, 10_000),
            Some(allowed)
        );
        assert_eq!(
            valid_stored_chunk_size(10, (allowed + 1) as u64, decoded_size, true, 10_000),
            None
        );
        assert_eq!(
            valid_stored_chunk_size(9_990, 64, decoded_size, false, 10_000),
            None
        );
        assert_eq!(
            valid_stored_chunk_size(u64::MAX - 1, 64, decoded_size, false, u64::MAX),
            None
        );
    }

    #[test]
    fn oversized_inflated_chunk_is_bounded_and_rejected() {
        let chunk_bytes = 64;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&vec![7_u8; 4_096]).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&compressed).unwrap();
        file.flush().unwrap();

        let column = ChunkedColumn {
            elem_size: 1,
            chunk_elems: chunk_bytes,
            n_elems: chunk_bytes,
            shuffle: false,
            deflate: true,
            chunks: vec![ChunkLoc {
                addr: 0,
                size: compressed.len(),
                filter_mask: 0,
            }],
        };
        let mut reader = ChunkReader::open(file.path().to_str().unwrap()).unwrap();
        let mut out = Vec::new();

        let error = reader
            .read_range(&column, 0, chunk_bytes, &mut out)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decoded to 65 bytes, expected 64")
        );
        assert_eq!(reader.inflated.len(), chunk_bytes + 1);
        assert!(out.is_empty());
    }
}
