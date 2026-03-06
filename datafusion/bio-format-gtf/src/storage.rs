use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, get_compression_type, get_remote_stream, get_remote_stream_bgzf_async,
    get_remote_stream_gz_async,
};
use flate2::read::GzDecoder;
use noodles_csi::BinningIndex;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::path::Path;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

/// A parsed GTF record with all 9 fields
pub struct GtfRecord {
    /// Sequence ID (chromosome/contig name)
    pub seqid: String,
    /// Source of the annotation
    pub source: String,
    /// Feature type
    pub ty: String,
    /// Start position (1-based, inclusive)
    pub start: u32,
    /// End position (1-based, inclusive)
    pub end: u32,
    /// Score or None if not provided
    pub score: Option<f32>,
    /// Strand direction
    pub strand: String,
    /// Phase as a string for parsing flexibility
    pub phase: String,
    /// Raw attribute string in GTF format
    pub attributes: String,
}

/// Trait for unified GTF record access
///
/// Returns borrowed `&str` references to avoid per-record allocations in hot paths.
pub trait GtfRecordTrait {
    /// Returns the reference sequence name (seqid) field
    fn reference_sequence_name(&self) -> &str;
    /// Returns the start position (1-based, inclusive)
    fn start(&self) -> u32;
    /// Returns the end position (1-based, inclusive)
    fn end(&self) -> u32;
    /// Returns the feature type
    fn ty(&self) -> &str;
    /// Returns the source of this annotation
    fn source(&self) -> &str;
    /// Returns the score, or None if score is not available
    fn score(&self) -> Option<f32>;
    /// Returns the strand ('+', '-', or '.')
    fn strand(&self) -> &str;
    /// Returns the phase (0, 1, 2) for coding features, or None if not applicable
    fn phase(&self) -> Option<u8>;
    /// Returns the raw attributes string
    fn attributes_string(&self) -> &str;
}

impl GtfRecordTrait for GtfRecord {
    fn reference_sequence_name(&self) -> &str {
        &self.seqid
    }

    fn start(&self) -> u32 {
        self.start
    }

    fn end(&self) -> u32 {
        self.end
    }

    fn ty(&self) -> &str {
        &self.ty
    }

    fn source(&self) -> &str {
        &self.source
    }

    fn score(&self) -> Option<f32> {
        self.score
    }

    fn strand(&self) -> &str {
        &self.strand
    }

    fn phase(&self) -> Option<u8> {
        self.phase.parse().ok()
    }

    fn attributes_string(&self) -> &str {
        &self.attributes
    }
}

/// Local GTF file reader supporting plain, GZIP, and BGZF compressed files
pub enum GtfLocalReader {
    /// Plain text reader
    Plain(BufReader<File>),
    /// GZIP compressed reader
    Gzip(Box<BufReader<GzDecoder<File>>>),
    /// BGZF compressed reader (block-gzipped, used with tabix indexes)
    Bgzf(Box<BufReader<noodles_bgzf::Reader<File>>>),
}

impl GtfLocalReader {
    /// Create a new GTF local reader, auto-detecting compression.
    ///
    /// For `.gz` files, checks the BGZF magic bytes to distinguish BGZF from plain GZIP.
    pub fn new(file_path: &str) -> Result<Self, Error> {
        let path = Path::new(file_path);

        if file_path.ends_with(".gz") || file_path.ends_with(".gzip") {
            // Check if this is BGZF by reading the first few bytes
            if is_bgzf(path)? {
                let file = File::open(path)?;
                let bgzf_reader = noodles_bgzf::Reader::new(file);
                Ok(GtfLocalReader::Bgzf(Box::new(BufReader::new(bgzf_reader))))
            } else {
                let file = File::open(path)?;
                let decoder = GzDecoder::new(file);
                Ok(GtfLocalReader::Gzip(Box::new(BufReader::new(decoder))))
            }
        } else {
            let file = File::open(path)?;
            Ok(GtfLocalReader::Plain(BufReader::new(file)))
        }
    }

    /// Convert into a synchronous iterator of GTF records
    pub fn into_sync_iterator(self) -> GtfSyncIterator {
        match self {
            GtfLocalReader::Plain(reader) => GtfSyncIterator {
                lines: Box::new(reader.lines()),
            },
            GtfLocalReader::Gzip(reader) => GtfSyncIterator {
                lines: Box::new((*reader).lines()),
            },
            GtfLocalReader::Bgzf(reader) => GtfSyncIterator {
                lines: Box::new((*reader).lines()),
            },
        }
    }
}

/// Async remote GTF file reader supporting plain, GZIP, and BGZF compressed streams
pub enum GtfRemoteReader {
    /// Plain text remote reader
    Plain(tokio::io::BufReader<StreamReader<FuturesBytesStream, Bytes>>),
    /// GZIP compressed remote reader
    Gzip(tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>),
    /// BGZF compressed remote reader
    Bgzf(tokio::io::BufReader<noodles_bgzf::AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
}

impl GtfRemoteReader {
    /// Create a new remote GTF reader, auto-detecting compression.
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        use datafusion_bio_format_core::object_storage::CompressionType;

        let compression =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(Error::other)?;

        match compression {
            CompressionType::BGZF => {
                let inner = get_remote_stream_bgzf_async(file_path, object_storage_options)
                    .await
                    .map_err(Error::other)?;
                Ok(GtfRemoteReader::Bgzf(tokio::io::BufReader::new(inner)))
            }
            CompressionType::GZIP => {
                let inner = get_remote_stream_gz_async(file_path, object_storage_options)
                    .await
                    .map_err(Error::other)?;
                Ok(GtfRemoteReader::Gzip(tokio::io::BufReader::new(inner)))
            }
            _ => {
                let stream = get_remote_stream(file_path, object_storage_options, None)
                    .await
                    .map_err(Error::other)?;
                Ok(GtfRemoteReader::Plain(tokio::io::BufReader::new(
                    StreamReader::new(stream),
                )))
            }
        }
    }

    /// Read a line from the remote stream.
    pub async fn read_line(&mut self, buf: &mut String) -> Result<usize, Error> {
        match self {
            GtfRemoteReader::Plain(r) => r.read_line(buf).await,
            GtfRemoteReader::Gzip(r) => r.read_line(buf).await,
            GtfRemoteReader::Bgzf(r) => r.read_line(buf).await,
        }
    }
}

/// Check if a file is BGZF by examining magic bytes.
/// BGZF files start with `1f 8b 08 04` (gzip with FEXTRA) and contain
/// the BGZF extra field `42 43` at offset 12.
fn is_bgzf(path: &Path) -> Result<bool, Error> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut buf = [0u8; 18];
    let n = file.read(&mut buf)?;
    if n < 18 {
        return Ok(false);
    }
    // Check gzip magic + FEXTRA flag
    if buf[0] != 0x1f || buf[1] != 0x8b || buf[2] != 0x08 || (buf[3] & 0x04) == 0 {
        return Ok(false);
    }
    // Check for BC extra field (BGZF signature)
    // Extra field starts at offset 12, first 2 bytes are subfield ID
    Ok(buf[12] == b'B' && buf[13] == b'C')
}

/// Synchronous iterator over GTF records
pub struct GtfSyncIterator {
    lines: Box<dyn Iterator<Item = Result<String, Error>> + Send>,
}

impl Iterator for GtfSyncIterator {
    type Item = Result<GtfRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(e) => return Some(Err(e)),
            };

            // Skip comment lines and empty lines
            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            return Some(parse_gtf_line(&line));
        }
    }
}

/// Parse a single GTF line into a GtfRecord
pub(crate) fn parse_gtf_line(line: &str) -> Result<GtfRecord, Error> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 9 {
        return Err(Error::new(
            std::io::ErrorKind::InvalidData,
            format!("GTF line has {} fields, expected 9", fields.len()),
        ));
    }

    let score = if fields[5] == "." {
        None
    } else {
        fields[5].parse().ok()
    };

    Ok(GtfRecord {
        seqid: fields[0].to_string(),
        source: fields[1].to_string(),
        ty: fields[2].to_string(),
        start: fields[3].parse().map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid start position '{}': {e}", fields[3]),
            )
        })?,
        end: fields[4].parse().map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid end position '{}': {e}", fields[4]),
            )
        })?,
        score,
        strand: fields[6].to_string(),
        phase: fields[7].to_string(),
        attributes: fields[8].to_string(),
    })
}

/// A local indexed GTF reader for region-based queries.
///
/// Uses noodles' tabix `IndexedReader::Builder` to support random-access queries on
/// BGZF-compressed, tabix-indexed GTF files. This is used when an index file (.tbi/.csi)
/// is available and genomic region filters are present.
pub struct IndexedGtfReader {
    reader:
        noodles_csi::io::IndexedReader<noodles_bgzf_gtf::io::Reader<File>, noodles_tabix::Index>,
    contig_names: Vec<String>,
}

impl IndexedGtfReader {
    /// Creates a new indexed GTF reader.
    ///
    /// # Arguments
    /// * `file_path` - Path to the BGZF-compressed GTF file (.gtf.gz)
    /// * `index_path` - Path to the TBI or CSI index file
    pub fn new(file_path: &str, index_path: &str) -> Result<Self, Error> {
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
            .map_err(Error::other)?;

        Ok(Self {
            reader,
            contig_names,
        })
    }

    /// Returns the contig names from the tabix index header.
    pub fn contig_names(&self) -> &[String] {
        &self.contig_names
    }

    /// Query records overlapping a genomic region.
    pub fn query<'a>(
        &'a mut self,
        region: &'a noodles_core::Region,
    ) -> Result<
        impl Iterator<Item = Result<noodles_csi::io::indexed_records::Record, Error>> + 'a,
        Error,
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
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
) -> Vec<datafusion_bio_format_core::partition_balancer::RegionSizeEstimate> {
    use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;

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

            // Collect non-empty leaf bin positions for data-aware splitting.
            // TBI uses the same binning scheme as BAI (min_shift=14, depth=5).
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
