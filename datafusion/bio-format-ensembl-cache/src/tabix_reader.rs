//! BGZF-aware parallel reader for tabix `all_vars.gz` variation files.
//!
//! Splits a single bgzf-compressed file into N byte-range partitions,
//! each reading from a different position in the file.  This enables
//! intra-file parallelism for tabix caches where each chromosome has
//! a single `all_vars.gz` file.

use crate::errors::{Result, exec_err};
use noodles_bgzf as bgzf;
use noodles_csi::binning_index::BinningIndex;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

const IO_BUFFER_SIZE: usize = 64 * 1024;

/// Describes a byte-range partition within a bgzf file.
#[derive(Debug, Clone)]
pub(crate) struct BgzfPartition {
    /// Compressed byte offset to start reading from.
    /// Partition 0 starts at 0; others start at a bgzf block boundary.
    pub start_compressed: u64,
    /// Compressed byte offset where this partition should stop.
    /// The reader continues until the current line's compressed offset
    /// exceeds this value.
    pub end_compressed: u64,
    /// Whether to skip the first line when reading from this partition.
    ///
    /// True when the preceding BGZF block's last uncompressed byte is
    /// not `'\n'`, meaning a text line spans across the boundary.  The
    /// first bytes at `start_compressed` are then a continuation fragment
    /// already consumed by the previous partition's `read_line`.
    pub skip_first_line: bool,
}

/// Compute byte-range partitions for a bgzf file.
///
/// Scans the file to find bgzf block boundaries, then distributes
/// blocks across `num_partitions` roughly equal byte ranges.
/// Each partition starts at a valid block boundary.
pub(crate) fn compute_bgzf_partitions(
    file_path: &Path,
    num_partitions: usize,
) -> Result<Vec<BgzfPartition>> {
    let file_size = std::fs::metadata(file_path)
        .map(|m| m.len())
        .map_err(|e| exec_err(format!("Failed to stat {}: {e}", file_path.display())))?;

    if file_size == 0 || num_partitions <= 1 {
        return Ok(vec![BgzfPartition {
            start_compressed: 0,
            end_compressed: file_size,
            skip_first_line: false,
        }]);
    }

    // Scan for bgzf block start offsets by reading block headers.
    let block_offsets = scan_bgzf_block_offsets(file_path, file_size)?;

    if block_offsets.len() <= 1 {
        return Ok(vec![BgzfPartition {
            start_compressed: 0,
            end_compressed: file_size,
            skip_first_line: false,
        }]);
    }

    // Collect partition boundary offsets (each boundary is a block start).
    let target_chunk = file_size / num_partitions as u64;
    let mut boundaries = vec![0u64];
    let mut current_start = 0u64;

    for i in 1..num_partitions {
        let target_offset = target_chunk * i as u64;
        let best = block_offsets
            .iter()
            .copied()
            .filter(|&off| off > current_start)
            .min_by_key(|&off| (off as i64 - target_offset as i64).unsigned_abs())
            .unwrap_or(file_size);

        if best < file_size && best > current_start {
            boundaries.push(best);
            current_start = best;
        }
    }

    build_partitions_from_boundaries(file_path, &block_offsets, &boundaries, file_size)
}

/// Look up the compressed byte range for a chromosome in a tabix or CSI index.
///
/// Supports both `.tbi` (tabix) and `.csi` (coordinate-sorted index) files.
/// Returns `Some((start_compressed, end_compressed))` if the chromosome
/// is found in the index, `None` otherwise.
pub(crate) fn tabix_chrom_byte_range(index_path: &Path, chrom: &str) -> Result<Option<(u64, u64)>> {
    let is_csi = index_path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("csi"));

    if is_csi {
        chrom_byte_range_from_csi(index_path, chrom)
    } else {
        chrom_byte_range_from_tbi(index_path, chrom)
    }
}

fn chrom_byte_range_from_tbi(tbi_path: &Path, chrom: &str) -> Result<Option<(u64, u64)>> {
    let index = noodles_tabix::fs::read(tbi_path).map_err(|e| {
        exec_err(format!(
            "Failed reading tabix index {}: {e}",
            tbi_path.display()
        ))
    })?;

    extract_chrom_range_from_index(index.header(), index.reference_sequences(), chrom)
}

fn chrom_byte_range_from_csi(csi_path: &Path, chrom: &str) -> Result<Option<(u64, u64)>> {
    let index = noodles_csi::fs::read(csi_path).map_err(|e| {
        exec_err(format!(
            "Failed reading CSI index {}: {e}",
            csi_path.display()
        ))
    })?;

    extract_chrom_range_from_index(index.header(), index.reference_sequences(), chrom)
}

/// Extract the compressed byte range for a chromosome from an index's
/// header and reference sequences.  Works for both TBI and CSI indexes
/// since both use the same `ReferenceSequence<I>` structure.
fn extract_chrom_range_from_index<
    I: noodles_csi::binning_index::index::reference_sequence::Index,
>(
    header: Option<&noodles_csi::binning_index::index::Header>,
    ref_seqs: &[noodles_csi::binning_index::index::ReferenceSequence<I>],
    chrom: &str,
) -> Result<Option<(u64, u64)>> {
    let header = match header {
        Some(h) => h,
        None => return Ok(None),
    };

    let names: Vec<String> = header
        .reference_sequence_names()
        .iter()
        .map(|n| String::from_utf8_lossy(n.as_ref()).to_string())
        .collect();

    let tid = match names.iter().position(|n| n == chrom) {
        Some(t) => t,
        None => return Ok(None),
    };

    if tid >= ref_seqs.len() {
        return Ok(None);
    }

    let ref_seq = &ref_seqs[tid];

    // Scan bins/chunks to find the compressed byte range for this chromosome
    let mut min_offset = u64::MAX;
    let mut max_offset = 0u64;
    for bin in ref_seq.bins().values() {
        for chunk in bin.chunks() {
            min_offset = min_offset.min(chunk.start().compressed());
            max_offset = max_offset.max(chunk.end().compressed());
        }
    }

    if min_offset == u64::MAX {
        return Ok(None);
    }

    // The max chunk end from bins gives us a compressed offset guaranteed
    // to be past the last chr data.  However, in BgzfPartitionLineReader
    // the check `pos.compressed() >= end` happens BEFORE reading, so the
    // block at `max_offset` might not be read if `max_offset` IS a block
    // boundary.  Add 1 so the reader reads that block too.  The slight
    // overshoot (reading into the next block) is fine — any extra lines
    // from the next chromosome are discarded by the row-level filter.
    Ok(Some((min_offset, max_offset + 1)))
}

/// Compute byte-range partitions restricted to a specific range within
/// a bgzf file.  Used when the tabix index tells us which byte range
/// belongs to a specific chromosome.
pub(crate) fn compute_bgzf_partitions_in_range(
    file_path: &Path,
    range_start: u64,
    range_end: u64,
    num_partitions: usize,
) -> Result<Vec<BgzfPartition>> {
    let file_size = std::fs::metadata(file_path)
        .map(|m| m.len())
        .map_err(|e| exec_err(format!("Failed to stat {}: {e}", file_path.display())))?;

    let range_end = range_end.min(file_size);

    if range_start >= range_end || num_partitions <= 1 {
        return Ok(vec![BgzfPartition {
            start_compressed: range_start,
            end_compressed: range_end,
            skip_first_line: false,
        }]);
    }

    // Scan all blocks (need full list for skip-flag lookups).
    let all_offsets = scan_bgzf_block_offsets(file_path, file_size)?;
    let block_offsets: Vec<u64> = all_offsets
        .iter()
        .copied()
        .filter(|&off| off >= range_start && off < range_end)
        .collect();

    if block_offsets.len() <= 1 {
        return Ok(vec![BgzfPartition {
            start_compressed: range_start,
            end_compressed: range_end,
            skip_first_line: false,
        }]);
    }

    let range_size = range_end - range_start;
    let target_chunk = range_size / num_partitions as u64;
    let mut boundaries = vec![block_offsets[0]];
    let mut current_start = block_offsets[0];

    for i in 1..num_partitions {
        let target_offset = range_start + target_chunk * i as u64;
        let best = block_offsets
            .iter()
            .copied()
            .filter(|&off| off > current_start)
            .min_by_key(|&off| (off as i64 - target_offset as i64).unsigned_abs())
            .unwrap_or(range_end);

        if best < range_end && best > current_start {
            boundaries.push(best);
            current_start = best;
        }
    }

    // Use all_offsets so we can look up the block before any boundary.
    build_partitions_from_boundaries(file_path, &all_offsets, &boundaries, range_end)
}

/// Scan a bgzf file to find the compressed offsets of each block.
///
/// Reads each bgzf block header to extract the block size, then
/// advances to the next block.  This is fast (only reads headers,
/// not the full compressed data).
fn scan_bgzf_block_offsets(file_path: &Path, file_size: u64) -> Result<Vec<u64>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(file_path)
        .map_err(|e| exec_err(format!("Failed opening {}: {e}", file_path.display())))?;

    let mut offsets = Vec::new();
    let mut pos = 0u64;
    let mut header = [0u8; 18]; // bgzf block header is 18 bytes

    while pos + 28 <= file_size {
        // The EOF block is exactly 28 bytes; skip it
        file.seek(SeekFrom::Start(pos))
            .map_err(|e| exec_err(format!("Seek failed: {e}")))?;

        if file.read_exact(&mut header).is_err() {
            break;
        }

        // Verify bgzf magic: 1f 8b 08 04
        if header[0] != 0x1f || header[1] != 0x8b || header[2] != 0x08 || header[3] != 0x04 {
            break;
        }

        offsets.push(pos);

        // Extract BSIZE from bytes 16-17 (little-endian u16).
        // BSIZE is the total block size minus 1.
        let bsize = u16::from_le_bytes([header[16], header[17]]) as u64 + 1;
        pos += bsize;
    }

    Ok(offsets)
}

/// Build partitions from a list of boundary offsets, computing `skip_first_line`
/// for each non-first partition by checking whether the preceding BGZF block
/// ends with a newline.
fn build_partitions_from_boundaries(
    file_path: &Path,
    block_offsets: &[u64],
    boundaries: &[u64],
    file_end: u64,
) -> Result<Vec<BgzfPartition>> {
    let mut partitions = Vec::with_capacity(boundaries.len());
    for (i, &start) in boundaries.iter().enumerate() {
        let end = boundaries.get(i + 1).copied().unwrap_or(file_end);
        let skip = if i == 0 {
            false
        } else {
            // Find the BGZF block that ends right before this boundary.
            // If its last uncompressed byte is not '\n', a text line spans
            // the boundary and we must skip the continuation fragment.
            match block_offsets.iter().copied().rev().find(|&off| off < start) {
                Some(prev_block) => !block_ends_with_newline(file_path, prev_block)?,
                None => false,
            }
        };
        partitions.push(BgzfPartition {
            start_compressed: start,
            end_compressed: end,
            skip_first_line: skip,
        });
    }
    Ok(partitions)
}

/// Check whether the last uncompressed byte of a BGZF block is `'\n'`.
///
/// Used during partition construction to decide if the boundary between
/// two partitions falls at a line boundary.  If the block ends with
/// `'\n'`, the next block starts a new line and the following partition
/// should NOT skip its first line.  Otherwise the boundary is mid-line
/// and the first fragment must be discarded.
fn block_ends_with_newline(file_path: &Path, block_offset: u64) -> Result<bool> {
    use std::io::Read;

    let file = File::open(file_path)
        .map_err(|e| exec_err(format!("Failed opening {}: {e}", file_path.display())))?;
    let mut reader = bgzf::Reader::new(BufReader::with_capacity(IO_BUFFER_SIZE, file));
    let vpos = bgzf::VirtualPosition::try_from((block_offset, 0))
        .map_err(|e| exec_err(format!("Invalid virtual position: {e}")))?;
    reader
        .seek(vpos)
        .map_err(|e| exec_err(format!("BGZF seek failed: {e}")))?;

    // Read the entire block's uncompressed data and check the last byte.
    // BGZF blocks decompress to at most 65536 bytes.
    let mut last_byte = None;
    let mut buf = [0u8; 65536];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| exec_err(format!("BGZF read failed: {e}")))?;
        if n == 0 {
            break;
        }
        last_byte = Some(buf[n - 1]);
        // Stop once the reader advances past the target block.
        if reader.virtual_position().compressed() != block_offset {
            break;
        }
    }

    Ok(last_byte == Some(b'\n'))
}

/// Wraps a bgzf reader that stops after the partition's end offset.
pub(crate) struct BgzfPartitionLineReader {
    inner: bgzf::Reader<BufReader<File>>,
    end_compressed: u64,
    buf: String,
    done: bool,
}

impl BgzfPartitionLineReader {
    pub fn open(file_path: &Path, partition: &BgzfPartition) -> Result<Self> {
        let file = File::open(file_path)
            .map_err(|e| exec_err(format!("Failed opening {}: {e}", file_path.display())))?;

        let mut inner = bgzf::Reader::new(BufReader::with_capacity(IO_BUFFER_SIZE, file));

        if partition.start_compressed > 0 {
            let vpos = bgzf::VirtualPosition::try_from((partition.start_compressed, 0))
                .map_err(|e| exec_err(format!("Invalid virtual position: {e}")))?;
            inner
                .seek(vpos)
                .map_err(|e| exec_err(format!("BGZF seek failed: {e}")))?;

            if partition.skip_first_line {
                // The preceding block did not end with '\n', so the first
                // bytes at this position are a continuation of a line
                // already consumed by the previous partition.  Discard
                // through the next newline.
                let mut discard = String::new();
                inner
                    .read_line(&mut discard)
                    .map_err(|e| exec_err(format!("BGZF read_line failed: {e}")))?;
            }
        }

        Ok(Self {
            inner,
            end_compressed: partition.end_compressed,
            buf: String::new(),
            done: false,
        })
    }

    /// Read the next line. Returns `Ok(None)` when past the partition boundary
    /// or at EOF, and `Err` on I/O or BGZF decompression failures.
    pub fn next_line(&mut self) -> Result<Option<&str>> {
        loop {
            if self.done {
                return Ok(None);
            }

            // Check position BEFORE reading: if the current compressed
            // position is at or past the end boundary, this partition
            // is done.  The next partition will read from this position.
            let pos = self.inner.virtual_position();
            if pos.compressed() >= self.end_compressed {
                self.done = true;
                return Ok(None);
            }

            self.buf.clear();
            match self.inner.read_line(&mut self.buf) {
                Ok(0) => {
                    self.done = true;
                    return Ok(None);
                }
                Ok(_) => {
                    // Trim trailing newline
                    let len = self.buf.trim_end_matches('\n').trim_end_matches('\r').len();
                    self.buf.truncate(len);
                    if self.buf.is_empty() {
                        continue; // skip empty lines
                    }
                    return Ok(Some(&self.buf));
                }
                Err(e) => {
                    self.done = true;
                    return Err(exec_err(format!("BGZF read error: {e}")));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(format!(
            "{}/tests/fixtures/{}",
            env!("CARGO_MANIFEST_DIR"),
            name
        ))
    }

    #[test]
    fn compute_partitions_single() {
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 1).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].start_compressed, 0);
    }

    #[test]
    fn compute_partitions_multiple() {
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 4).unwrap();
        assert!(partitions.len() >= 2, "should have multiple partitions");
        assert_eq!(partitions[0].start_compressed, 0);
        // Last partition ends at file size
        let file_size = std::fs::metadata(&path).unwrap().len();
        assert_eq!(partitions.last().unwrap().end_compressed, file_size);
        // Partitions are contiguous (end of one = start of next)
        for w in partitions.windows(2) {
            assert_eq!(w[0].end_compressed, w[1].start_compressed);
        }
    }

    #[test]
    fn read_single_partition_all_lines() {
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let file_size = std::fs::metadata(&path).unwrap().len();
        let partition = BgzfPartition {
            start_compressed: 0,
            end_compressed: file_size,
            skip_first_line: false,
        };
        let mut reader = BgzfPartitionLineReader::open(&path, &partition).unwrap();
        let mut lines = Vec::new();
        while let Some(line) = reader.next_line().unwrap() {
            lines.push(line.to_string());
        }
        // The bgzf fixture has 49991 chr1 + 29991 chr2 = 79982 variation lines
        assert_eq!(lines.len(), 79982);
        assert!(lines[0].starts_with("1\t"));
        assert!(lines.last().unwrap().starts_with("2\t"));
    }

    #[test]
    fn multi_partition_covers_all_lines() {
        // Verify that splitting into N partitions reads every line exactly once
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 4).unwrap();

        let mut all_lines = Vec::new();
        let mut per_partition_counts = Vec::new();
        for partition in &partitions {
            let mut count = 0;
            let mut reader = BgzfPartitionLineReader::open(&path, partition).unwrap();
            while let Some(line) = reader.next_line().unwrap() {
                all_lines.push(line.to_string());
                count += 1;
            }
            per_partition_counts.push(count);
        }

        assert_eq!(
            all_lines.len(),
            79982,
            "partition counts: {per_partition_counts:?}, total: {}",
            all_lines.len()
        );
        // First line should be chr1 start=100
        assert!(all_lines[0].starts_with("1\t100\t"));
        // Last line should be chr2
        assert!(all_lines.last().unwrap().starts_with("2\t"));
    }

    #[test]
    fn tabix_chrom_byte_range_finds_chrom() {
        let tbi_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz.tbi");
        let range = tabix_chrom_byte_range(&tbi_path, "1").unwrap();
        assert!(range.is_some(), "chr1 should be in the tabix index");
        let (start, end) = range.unwrap();
        assert!(end > start, "end ({end}) should be > start ({start})");
    }

    #[test]
    fn tabix_chrom_byte_range_missing_chrom() {
        let tbi_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz.tbi");
        let range = tabix_chrom_byte_range(&tbi_path, "99").unwrap();
        assert!(range.is_none(), "chr99 should not be in the index");
    }

    #[test]
    fn range_partitions_cover_chrom_data() {
        // Partition only chr1's byte range from the tabix index.
        // Should read all chr1 lines. May include a few chr2 lines at
        // the bgzf block boundary (which the row-level filter discards).
        let data_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let tbi_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz.tbi");

        let (start, end) = tabix_chrom_byte_range(&tbi_path, "1").unwrap().unwrap();
        let partitions = compute_bgzf_partitions_in_range(&data_path, start, end, 4).unwrap();

        let mut chr1_count = 0usize;
        let mut other_count = 0usize;
        for partition in &partitions {
            let mut reader = BgzfPartitionLineReader::open(&data_path, partition).unwrap();
            while let Some(line) = reader.next_line().unwrap() {
                if line.starts_with("1\t") {
                    chr1_count += 1;
                } else {
                    other_count += 1;
                }
            }
        }

        // Must read ALL chr1 lines (49991)
        assert_eq!(chr1_count, 49991, "must cover all chr1 lines");
        // May read a few chr2 lines at the block boundary, but far
        // fewer than the full 29991 chr2 lines in the file.
        assert!(
            other_count < 1000,
            "should read very few non-chr1 lines, got {other_count}"
        );
    }

    #[test]
    fn range_partitions_preserve_order() {
        let data_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let tbi_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz.tbi");

        let (start, end) = tabix_chrom_byte_range(&tbi_path, "1").unwrap().unwrap();
        let partitions = compute_bgzf_partitions_in_range(&data_path, start, end, 4).unwrap();

        for (p_idx, partition) in partitions.iter().enumerate() {
            let mut reader = BgzfPartitionLineReader::open(&data_path, partition).unwrap();
            let mut prev_start: Option<i64> = None;
            while let Some(line) = reader.next_line().unwrap() {
                // Only check ordering for chr1 lines (boundary may spill chr2)
                if !line.starts_with("1\t") {
                    continue;
                }
                let start: i64 = line.split('\t').nth(1).unwrap().parse().unwrap();
                if let Some(prev) = prev_start {
                    assert!(
                        start >= prev,
                        "partition {p_idx}: start {start} < prev {prev}"
                    );
                }
                prev_start = Some(start);
            }
        }
    }

    #[test]
    fn multi_partition_preserves_order() {
        // Verify that within each partition, start positions are non-decreasing
        // when all lines are from the same chromosome.
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 4).unwrap();

        for (p_idx, partition) in partitions.iter().enumerate() {
            let mut reader = BgzfPartitionLineReader::open(&path, partition).unwrap();
            let mut prev_start: Option<i64> = None;
            let mut prev_chrom = String::new();
            while let Some(line) = reader.next_line().unwrap() {
                let fields: Vec<&str> = line.split('\t').collect();
                let chrom = fields[0];
                let start: i64 = fields[1].parse().unwrap();

                if chrom == prev_chrom
                    && let Some(prev) = prev_start
                {
                    assert!(
                        start >= prev,
                        "partition {p_idx}: start {start} < prev {prev} for chrom {chrom}"
                    );
                }
                prev_chrom = chrom.to_string();
                prev_start = Some(start);
            }
        }
    }

    // -----------------------------------------------------------------------
    // skip_first_line flag correctness
    // -----------------------------------------------------------------------

    #[test]
    fn first_partition_never_skips() {
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 4).unwrap();
        assert!(
            !partitions[0].skip_first_line,
            "first partition must not skip"
        );
    }

    #[test]
    fn single_partition_skip_is_false() {
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let partitions = compute_bgzf_partitions(&path, 1).unwrap();
        assert_eq!(partitions.len(), 1);
        assert!(!partitions[0].skip_first_line);
    }

    #[test]
    fn range_first_partition_never_skips() {
        let data_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let tbi_path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz.tbi");
        let (start, end) = tabix_chrom_byte_range(&tbi_path, "1").unwrap().unwrap();
        let partitions = compute_bgzf_partitions_in_range(&data_path, start, end, 4).unwrap();
        assert!(
            !partitions[0].skip_first_line,
            "first partition of a range must not skip"
        );
    }

    // -----------------------------------------------------------------------
    // block_ends_with_newline
    // -----------------------------------------------------------------------

    #[test]
    fn block_ends_with_newline_first_block() {
        // The first block of our fixture should end with '\n' because
        // variation lines are short (~100 bytes) and many fit in one
        // 64KB block, each terminated by '\n'.
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        assert!(block_ends_with_newline(&path, 0).unwrap());
    }

    #[test]
    fn block_ends_with_newline_all_boundary_blocks() {
        // For each partition boundary, verify the helper agrees with
        // the skip_first_line flag: skip iff preceding block does NOT
        // end with newline.
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let file_size = std::fs::metadata(&path).unwrap().len();
        let block_offsets = scan_bgzf_block_offsets(&path, file_size).unwrap();
        let partitions = compute_bgzf_partitions(&path, 8).unwrap();

        for (i, p) in partitions.iter().enumerate() {
            if i == 0 {
                continue;
            }
            let prev_block = block_offsets
                .iter()
                .copied()
                .rev()
                .find(|&off| off < p.start_compressed);
            if let Some(pb) = prev_block {
                let ends_nl = block_ends_with_newline(&path, pb).unwrap();
                assert_eq!(
                    p.skip_first_line, !ends_nl,
                    "partition {i}: skip_first_line={} but block at {pb} ends_with_nl={ends_nl}",
                    p.skip_first_line
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // no duplicate or lost lines
    // -----------------------------------------------------------------------

    #[test]
    fn multi_partition_no_duplicate_lines() {
        // Read all lines via single partition and via 4 partitions,
        // then compare line-by-line to ensure no duplicates or losses.
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        let file_size = std::fs::metadata(&path).unwrap().len();

        // Single partition (ground truth)
        let single = BgzfPartition {
            start_compressed: 0,
            end_compressed: file_size,
            skip_first_line: false,
        };
        let mut reader = BgzfPartitionLineReader::open(&path, &single).unwrap();
        let mut expected = Vec::new();
        while let Some(line) = reader.next_line().unwrap() {
            expected.push(line.to_string());
        }

        // Multi partition
        let partitions = compute_bgzf_partitions(&path, 4).unwrap();
        let mut actual = Vec::new();
        for partition in &partitions {
            let mut r = BgzfPartitionLineReader::open(&path, partition).unwrap();
            while let Some(line) = r.next_line().unwrap() {
                actual.push(line.to_string());
            }
        }

        assert_eq!(
            actual.len(),
            expected.len(),
            "line count mismatch: expected {}, got {}",
            expected.len(),
            actual.len()
        );
        // Spot-check first, last, and a middle line
        assert_eq!(actual[0], expected[0], "first line mismatch");
        assert_eq!(
            actual.last().unwrap(),
            expected.last().unwrap(),
            "last line mismatch"
        );
        let mid = expected.len() / 2;
        assert_eq!(actual[mid], expected[mid], "middle line mismatch");
    }

    #[test]
    fn partition_counts_stable_across_sizes() {
        // Splitting into 2, 4, 8, or 16 partitions must always yield
        // exactly 79982 lines total.
        let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
        for n in [2, 4, 8, 16] {
            let partitions = compute_bgzf_partitions(&path, n).unwrap();
            let mut total = 0usize;
            for partition in &partitions {
                let mut r = BgzfPartitionLineReader::open(&path, partition).unwrap();
                while r.next_line().unwrap().is_some() {
                    total += 1;
                }
            }
            assert_eq!(
                total, 79982,
                "with {n} partitions: expected 79982 lines, got {total}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // error propagation
    // -----------------------------------------------------------------------

    #[test]
    fn next_line_returns_result_type() {
        // Verify next_line returns Result<Option<&str>> (not Option<&str>)
        // by using the ? operator in a Result-returning test.
        fn inner() -> Result<()> {
            let path = fixture_path("variation_tabix_bgzf/variation/all_vars.gz");
            let file_size = std::fs::metadata(&path).unwrap().len();
            let partition = BgzfPartition {
                start_compressed: 0,
                end_compressed: file_size,
                skip_first_line: false,
            };
            let mut reader = BgzfPartitionLineReader::open(&path, &partition)?;
            let first = reader.next_line()?;
            assert!(first.is_some());
            Ok(())
        }
        inner().unwrap();
    }
}
