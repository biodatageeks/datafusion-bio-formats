//! Storage abstraction for local and remote Pairs files.
//!
//! Provides readers for plain text and BGZF-compressed Pairs files,
//! plus an indexed reader for tabix-based region queries.

use crate::header::{PairsHeader, parse_pairs_header};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type,
};
use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;
use flate2::read::GzDecoder;
use noodles_bgzf as bgzf;
use noodles_csi::binning_index::BinningIndex;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Local Pairs file reader supporting BGZF, GZIP, and plain text.
pub enum PairsLocalReader {
    /// BGZF-compressed reader
    BGZF(BufReader<bgzf::Reader<File>>),
    /// GZIP-compressed reader
    GZIP(Box<BufReader<GzDecoder<File>>>),
    /// Plain text reader
    PLAIN(BufReader<File>),
}

impl PairsLocalReader {
    /// Read the next line into the provided buffer.
    /// Returns the number of bytes read (0 = EOF).
    pub fn read_line(&mut self, buf: &mut String) -> std::io::Result<usize> {
        match self {
            PairsLocalReader::BGZF(r) => r.read_line(buf),
            PairsLocalReader::GZIP(r) => r.read_line(buf),
            PairsLocalReader::PLAIN(r) => r.read_line(buf),
        }
    }
}

/// Read the Pairs header from a local file (synchronous).
///
/// Detects compression by inspecting the first bytes (BGZF magic),
/// then parses the header. Safe to call from both async and sync contexts.
pub fn get_local_pairs_header(
    file_path: &str,
    _object_storage_options: &ObjectStorageOptions,
) -> std::io::Result<PairsHeader> {
    // Detect compression by reading magic bytes (avoids async runtime dependency)
    let is_bgzf = {
        use std::io::Read;
        let mut f = File::open(file_path)?;
        let mut magic = [0u8; 4];
        let n = f.read(&mut magic)?;
        // BGZF/GZIP magic: 0x1f 0x8b
        n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b
    };

    if is_bgzf {
        // Try BGZF first (superset of GZIP for our purposes)
        let file = File::open(file_path)?;
        let bgzf_reader = bgzf::Reader::new(file);
        let mut buf_reader = BufReader::new(bgzf_reader);
        parse_pairs_header(&mut buf_reader)
    } else {
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);
        parse_pairs_header(&mut buf_reader)
    }
}

/// Create a local Pairs reader with automatic compression detection.
pub async fn new_local_reader(
    file_path: &str,
    object_storage_options: ObjectStorageOptions,
) -> Result<PairsLocalReader, std::io::Error> {
    let compression = get_compression_type(
        file_path.to_string(),
        Some(CompressionType::AUTO),
        object_storage_options,
    )
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    let file = File::open(file_path)?;
    match compression {
        CompressionType::BGZF => {
            let bgzf_reader = bgzf::Reader::new(file);
            Ok(PairsLocalReader::BGZF(BufReader::new(bgzf_reader)))
        }
        CompressionType::GZIP => {
            let gz_reader = GzDecoder::new(file);
            Ok(PairsLocalReader::GZIP(Box::new(BufReader::new(gz_reader))))
        }
        _ => Ok(PairsLocalReader::PLAIN(BufReader::new(file))),
    }
}

/// A local indexed Pairs reader for tabix-based region queries.
///
/// Uses noodles' tabix `IndexedReader::Builder` to support random-access queries on
/// BGZF-compressed, tabix-indexed Pairs files. Used when an index file (.tbi/.csi/.px2)
/// is available and chr1/pos1 filters are present.
pub struct IndexedPairsReader {
    reader:
        noodles_csi::io::IndexedReader<noodles_bgzf_pairs::io::Reader<File>, noodles_tabix::Index>,
    contig_names: Vec<String>,
    #[allow(dead_code)]
    header: PairsHeader,
}

impl IndexedPairsReader {
    /// Creates a new indexed Pairs reader.
    ///
    /// # Arguments
    /// * `file_path` - Path to the BGZF-compressed Pairs file
    /// * `index_path` - Path to the TBI, CSI, or px2 index file
    pub fn new(file_path: &str, index_path: &str) -> Result<Self, std::io::Error> {
        let index = noodles_tabix::fs::read(index_path)?;

        let contig_names = index
            .header()
            .map(|h| {
                h.reference_sequence_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let reader = noodles_tabix::io::indexed_reader::Builder::default()
            .set_index(index)
            .build_from_path(Path::new(file_path))
            .map_err(std::io::Error::other)?;

        // Parse header from the BGZF file for column info
        let header = {
            let file = File::open(file_path)?;
            let bgzf_reader = bgzf::Reader::new(file);
            let mut buf_reader = BufReader::new(bgzf_reader);
            parse_pairs_header(&mut buf_reader)?
        };

        Ok(Self {
            reader,
            contig_names,
            header,
        })
    }

    /// Returns the contig names from the tabix index header.
    pub fn contig_names(&self) -> &[String] {
        &self.contig_names
    }

    /// Query records overlapping a genomic region.
    ///
    /// Returns an iterator over raw indexed records. Each record can be accessed
    /// as a string reference via `.as_ref()` to get the raw TSV line.
    pub fn query<'a>(
        &'a mut self,
        region: &'a noodles_core::Region,
    ) -> Result<
        impl Iterator<Item = Result<noodles_csi::io::indexed_records::Record, std::io::Error>> + 'a,
        std::io::Error,
    > {
        self.reader.query(region)
    }
}

/// Estimate compressed byte sizes per region from a TBI (tabix) index.
///
/// Reads the tabix index and estimates the compressed byte range for each genomic region
/// by examining the chunks in each reference's bins using VirtualPosition offsets.
pub fn estimate_sizes_from_tbi(
    index_path: &str,
    regions: &[GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
) -> Vec<RegionSizeEstimate> {
    let index = match noodles_tabix::fs::read(index_path) {
        Ok(idx) => idx,
        Err(e) => {
            log::debug!("Failed to read TBI index for size estimation: {e}");
            return regions
                .iter()
                .map(|r| RegionSizeEstimate {
                    region: r.clone(),
                    estimated_bytes: 1,
                    contig_length: None,
                    unmapped_count: 0,
                    nonempty_bin_positions: Vec::new(),
                    leaf_bin_span: 0,
                })
                .collect();
        }
    };

    let contig_name_to_idx: std::collections::HashMap<&str, usize> = contig_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    regions
        .iter()
        .map(|region| {
            let ref_idx = contig_name_to_idx.get(region.chrom.as_str()).copied();
            let estimated_bytes = ref_idx
                .and_then(|idx| {
                    index.reference_sequences().get(idx).map(|ref_seq| {
                        let mut min_offset = u64::MAX;
                        let mut max_offset = 0u64;
                        for (_bin_id, bin) in ref_seq.bins() {
                            for chunk in bin.chunks() {
                                let start = chunk.start().compressed();
                                let end = chunk.end().compressed();
                                min_offset = min_offset.min(start);
                                max_offset = max_offset.max(end);
                            }
                        }
                        max_offset.saturating_sub(min_offset)
                    })
                })
                .unwrap_or(1);

            const TBI_LEAF_FIRST: usize = 4681;
            const TBI_LEAF_LAST: usize = 37448;
            const TBI_LEAF_SPAN: u64 = 16384;

            let mut nonempty_bin_positions: Vec<u64> = ref_idx
                .and_then(|idx| index.reference_sequences().get(idx))
                .map(|ref_seq| {
                    ref_seq
                        .bins()
                        .keys()
                        .copied()
                        .filter(|&bin_id| (TBI_LEAF_FIRST..=TBI_LEAF_LAST).contains(&bin_id))
                        .map(|bin_id| ((bin_id - TBI_LEAF_FIRST) as u64) * TBI_LEAF_SPAN + 1)
                        .collect()
                })
                .unwrap_or_default();
            nonempty_bin_positions.sort_unstable();

            let contig_length = ref_idx
                .and_then(|idx| contig_lengths.get(idx).copied())
                .filter(|&len| len > 0)
                .or_else(|| {
                    nonempty_bin_positions
                        .last()
                        .map(|&max_pos| max_pos + TBI_LEAF_SPAN - 1)
                })
                .or_else(|| {
                    const LEVEL_OFFSETS: [(usize, u64); 6] = [
                        (0, 1 << 29),
                        (1, 1 << 26),
                        (9, 1 << 23),
                        (73, 1 << 20),
                        (585, 1 << 17),
                        (4681, 1 << 14),
                    ];
                    ref_idx
                        .and_then(|idx| index.reference_sequences().get(idx))
                        .and_then(|ref_seq| {
                            ref_seq
                                .bins()
                                .keys()
                                .copied()
                                .filter_map(|bin_id| {
                                    LEVEL_OFFSETS.iter().rev().find_map(|&(offset, span)| {
                                        if bin_id >= offset
                                            && bin_id
                                                < LEVEL_OFFSETS
                                                    .iter()
                                                    .find(|&&(o, _)| o > offset)
                                                    .map_or(37449, |&(o, _)| o)
                                        {
                                            let idx_in_level = (bin_id - offset) as u64;
                                            Some((idx_in_level + 1) * span)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .max()
                        })
                });

            RegionSizeEstimate {
                region: region.clone(),
                estimated_bytes,
                contig_length,
                unmapped_count: 0,
                nonempty_bin_positions,
                leaf_bin_span: TBI_LEAF_SPAN,
            }
        })
        .collect()
}
