//! Storage layer for reading FASTQ files from local and remote sources
//!
//! This module provides functions for creating FASTQ readers with support for:
//! - Multiple compression formats (BGZF, GZIP, uncompressed)
//! - Local and remote file sources (GCS, S3, Azure Blob Storage)
//! - Multi-threaded reading for BGZF files
//! - Byte range reading for partitioned/distributed processing

use crate::table_provider::FastqByteRange;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, get_remote_stream_with_range,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bgzf;
use noodles_bgzf::gzi;
use noodles_fastq as fastq;
use noodles_fastq::Record;
use noodles_fastq::io::Reader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, Seek, SeekFrom};
use tokio_util::io::StreamReader;

/// Creates an async BGZF-decompressing FASTQ reader for remote files
///
/// # Arguments
///
/// * `file_path` - Path to the BGZF-compressed FASTQ file (remote URL)
/// * `object_storage_options` - Configuration for accessing remote storage
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_fastq_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = fastq::r#async::io::Reader::new(inner);
    Ok(reader)
}

/// Creates a remote BGZF FASTQ reader that reads only a specific byte range
pub async fn get_remote_fastq_bgzf_reader_with_range(
    file_path: String,
    byte_range: FastqByteRange,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    Error,
> {
    // Use range reading with BGZF - comet-bio translates GZI blocks to byte offsets
    let stream = get_remote_stream_with_range(
        file_path.clone(),
        object_storage_options,
        byte_range.start,
        byte_range.end,
    )
    .await
    .map_err(std::io::Error::other)?;

    let stream_reader = StreamReader::new(stream);
    let bgzf_reader = bgzf::r#async::Reader::new(stream_reader);
    let fastq_reader = fastq::r#async::io::Reader::new(bgzf_reader);
    Ok(fastq_reader)
}

/// Creates an async FASTQ reader for uncompressed remote files
///
/// # Arguments
///
/// * `file_path` - Path to the uncompressed FASTQ file (remote URL)
/// * `object_storage_options` - Configuration for accessing remote storage
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_fastq_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Creates a remote uncompressed FASTQ reader that reads only a specific byte range
pub async fn get_remote_fastq_reader_with_range(
    file_path: String,
    byte_range: FastqByteRange,
    object_storage_options: ObjectStorageOptions,
) -> Result<fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream_with_range(
        file_path.clone(),
        object_storage_options,
        byte_range.start,
        byte_range.end,
    )
    .await
    .map_err(std::io::Error::other)?;

    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Creates an async GZIP-decompressing FASTQ reader for remote files
///
/// # Arguments
///
/// * `file_path` - Path to the GZIP-compressed FASTQ file (remote URL)
/// * `object_storage_options` - Configuration for accessing remote storage
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_fastq_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fastq::r#async::io::Reader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = fastq::r#async::io::Reader::new(stream);
    Ok(reader)
}

/// Creates a synchronous BGZF-decompressing FASTQ reader for local files
///
/// Utilizes multiple worker threads for improved decompression performance.
///
/// # Arguments
///
/// * `file_path` - Path to the BGZF-compressed FASTQ file (local filesystem)
/// * `thread_num` - Number of worker threads for BGZF decompression
///
/// # Returns
///
/// A synchronous BGZF reader wrapping a FASTQ reader
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub fn get_local_fastq_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(fastq::io::Reader::new)
}

/// Helper to find line end in buffer
fn find_line_end(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|pos| start + pos)
}

/// Synchronize BGZF reader to next valid FASTQ record boundary
fn synchronize_bgzf_fastq_reader<R: BufRead>(
    reader: &mut noodles_bgzf::IndexedReader<R>,
    end_comp: u64,
) -> Result<(), Error> {
    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(()); // EOF
        }

        // Find the first potential header line starting with '@'
        if let Some(at_pos) = buf.iter().position(|&b| b == b'@') {
            // Validate it's a real FASTQ record by checking for '+' on third line
            if let Some(l1_end) = find_line_end(buf, at_pos) {
                if let Some(l2_end) = find_line_end(buf, l1_end + 1) {
                    if let Some(l3_start) = l2_end.checked_add(1) {
                        if buf.get(l3_start) == Some(&b'+') {
                            // Valid record found, consume up to start of record
                            reader.consume(at_pos);
                            return Ok(());
                        }
                    }
                }
            }
            // False positive or incomplete buffer, consume and retry
            if let Some(end_of_at_line) = find_line_end(buf, at_pos) {
                reader.consume(end_of_at_line + 1);
            } else {
                let len = buf.len();
                reader.consume(len);
            }
        } else {
            // No '@' found, consume entire buffer
            let len = buf.len();
            reader.consume(len);
        }
    }
}

/// Partition-aware BGZF FASTQ reader with proper boundary handling
///
/// This reader ensures each record is read by exactly ONE partition:
/// - Records that START in [start, end) are owned by this partition and read completely
/// - Records that START before start are skipped (owned by previous partition)
/// - Records that START at/after end are not read (owned by next partition)
/// - Records can EXTEND past end boundary if they START before it
pub struct BgzfRangedFastqReader {
    reader: fastq::io::Reader<noodles_bgzf::IndexedReader<BufReader<File>>>,
    start_offset: u64, // Compressed offset where partition starts
    end_offset: u64,   // Compressed offset where partition ends
    state: ReaderState,
}

#[derive(Debug)]
enum ReaderState {
    /// Initial state - need to synchronize and check ownership
    Uninitialized,
    /// After sync, positioned at first record - need to check if we own it
    Synchronized {
        sync_position: u64,
        moved_during_sync: bool,
    },
    /// Normal reading state
    Reading,
    /// Reached end of partition or EOF
    Finished,
}

impl BgzfRangedFastqReader {
    /// Creates a new BGZF ranged FASTQ reader.
    ///
    /// # Arguments
    ///
    /// * `reader` - FASTQ reader wrapping a BGZF indexed reader
    /// * `start_offset` - Compressed byte offset where reading starts
    /// * `end_offset` - Compressed byte offset where reading ends
    pub fn new(
        reader: fastq::io::Reader<noodles_bgzf::IndexedReader<BufReader<File>>>,
        start_offset: u64,
        end_offset: u64,
    ) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static READER_COUNTER: AtomicU64 = AtomicU64::new(0);
        let reader_id = READER_COUNTER.fetch_add(1, Ordering::SeqCst);

        log::debug!(
            "BgzfRangedFastqReader#{} - start={}, end={}",
            reader_id,
            start_offset,
            end_offset
        );
        Self {
            reader,
            start_offset,
            end_offset,
            state: if start_offset == 0 {
                log::debug!(
                    "Reader#{} - Partition 0, starting in Reading state",
                    reader_id
                );
                ReaderState::Reading // Partition 0 starts at beginning
            } else {
                log::debug!(
                    "Reader#{} - Non-zero partition, starting in Uninitialized state",
                    reader_id
                );
                ReaderState::Uninitialized // Need to sync
            },
        }
    }

    /// Initialize reader by synchronizing to record boundary
    fn initialize(&mut self) -> Result<(), Error> {
        // Record position before synchronization to detect if we moved
        let pos_before_sync = self.reader.get_ref().virtual_position().compressed();
        log::debug!("initialize - pos_before_sync={}", pos_before_sync);

        // Synchronize to next complete FASTQ record
        synchronize_bgzf_fastq_reader(self.reader.get_mut(), self.end_offset)?;

        // Record the position after synchronization
        let sync_pos = self.reader.get_ref().virtual_position().compressed();
        let moved = sync_pos != pos_before_sync;
        log::debug!("initialize - sync_pos={}, moved={}", sync_pos, moved);

        if sync_pos >= self.end_offset {
            // Synchronization went past our range - no records for us
            self.state = ReaderState::Finished;
        } else {
            // Pass info about whether we moved during sync
            self.state = ReaderState::Synchronized {
                sync_position: sync_pos,
                moved_during_sync: moved,
            };
        }

        Ok(())
    }

    /// Read a single record while respecting partition boundaries
    /// Returns the number of bytes read (0 indicates EOF or end of range)
    pub fn read_record(&mut self, record: &mut noodles_fastq::Record) -> Result<usize, Error> {
        loop {
            match &self.state {
                ReaderState::Uninitialized => {
                    self.initialize()?;
                    continue;
                }
                ReaderState::Synchronized {
                    sync_position,
                    moved_during_sync,
                } => {
                    // We're at the first record after sync
                    // Decision: skip or own?
                    // - If we MOVED during sync: record started before our boundary → skip it
                    // - If we DIDN'T move: we landed exactly on a record start → own it

                    let sync_pos = *sync_position;
                    let moved = *moved_during_sync;
                    log::debug!(
                        "Synchronized state - sync_pos={}, start_offset={}, moved={}",
                        sync_pos,
                        self.start_offset,
                        moved
                    );

                    if moved {
                        // We had to search forward to find a record boundary
                        // This means the record started before our partition boundary
                        // Previous partition owns it
                        log::debug!(
                            "Skipping first record (moved during sync, started before boundary)"
                        );
                        let mut dummy = noodles_fastq::Record::default();
                        self.reader.read_record(&mut dummy)?;
                        self.state = ReaderState::Reading;
                        continue;
                    } else {
                        // We landed exactly on a record boundary
                        // This record starts at our partition boundary - we own it
                        log::debug!("Owning first record (landed exactly on record boundary)");
                        self.state = ReaderState::Reading;
                        continue;
                    }
                }
                ReaderState::Finished => {
                    return Ok(0);
                }
                ReaderState::Reading => {
                    // Check position BEFORE reading
                    let pos_before = self.reader.get_ref().virtual_position().compressed();

                    if pos_before >= self.end_offset {
                        self.state = ReaderState::Finished;
                        return Ok(0);
                    }

                    // Read the record - it's ours because it starts in our range
                    return self.reader.read_record(record);
                }
            }
        }
    }

    /// Read records while respecting partition boundaries
    /// Strategy:
    /// - Synchronize to first complete record (if not partition 0)
    /// - Check if synchronized record is owned by this partition
    /// - Read all records that START before end_offset (even if they extend past)
    pub fn records(&mut self) -> impl Iterator<Item = Result<noodles_fastq::Record, Error>> + '_ {
        std::iter::from_fn(move || {
            let mut record = noodles_fastq::Record::default();
            match self.read_record(&mut record) {
                Ok(0) => None,
                Ok(_) => Some(Ok(record)),
                Err(e) => Some(Err(e)),
            }
        })
    }
}

/// Read GZI index file for BGZF
pub fn read_gzi_index(gzi_path: &str) -> Result<gzi::Index, Error> {
    gzi::read(std::path::Path::new(gzi_path)).map_err(|e| {
        Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read GZI index {}: {}", gzi_path, e),
        )
    })
}

/// Create a local BGZF reader with GZI index for byte range reading
///
/// Implements proper partition boundary handling:
/// - Seeks to the BGZF block containing start_offset
/// - For non-zero start: synchronizes to next complete record, checks ownership
/// - Records starting in [start, end) are owned and read completely (even if extending past end)
/// - Records starting before start or at/after end are not read by this partition
///
/// This ensures each record is read exactly once across all partitions with no overlaps.
pub fn get_local_fastq_bgzf_reader_with_range(
    file_path: String,
    byte_range: FastqByteRange,
) -> Result<BgzfRangedFastqReader, Error> {
    // Read GZI index (file_path + ".gzi")
    let gzi_path = format!("{}.gzi", file_path);
    let index = read_gzi_index(&gzi_path)?;

    // Open file and create indexed reader
    let file = std::fs::File::open(&file_path)?;
    let mut reader = noodles_bgzf::IndexedReader::new(BufReader::new(file), index.clone());

    // Find the BGZF block that contains or comes before our start_offset
    // The GZI index gives us (compressed_offset, uncompressed_offset) pairs
    let virtual_pos = if byte_range.start == 0 {
        // Start at beginning
        0
    } else {
        // Find the block at or before our start offset
        let mut target_uncomp = 0u64;

        for (comp, uncomp) in index.as_ref().iter() {
            if *comp <= byte_range.start {
                target_uncomp = *uncomp;
            } else {
                break; // Found the block after our target
            }
        }

        // Use the uncompressed offset as the seek position
        // IndexedReader will use the GZI index to find the right block
        target_uncomp
    };

    // Seek to the virtual position
    reader.seek(SeekFrom::Start(virtual_pos))?;

    // Create partition-aware reader
    // It will handle synchronization and ownership checking internally
    Ok(BgzfRangedFastqReader::new(
        fastq::io::Reader::new(reader),
        byte_range.start, // Compressed offset
        byte_range.end,   // Compressed offset
    ))
}

/// Creates a synchronous FASTQ reader for uncompressed local files
///
/// # Arguments
///
/// * `file_path` - Path to the uncompressed FASTQ file (local filesystem)
///
/// # Returns
///
/// A buffered synchronous FASTQ reader
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub fn get_local_fastq_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fastq::io::Reader::new)
}

fn resolve_fastq_range_start(file_path: &str, byte_range: &FastqByteRange) -> Result<u64, Error> {
    if byte_range.start == 0 {
        return Ok(0);
    }

    let mut file = std::fs::File::open(file_path)?;
    file.seek(SeekFrom::Start(byte_range.start))?;
    let mut reader = BufReader::new(file);
    let mut current_offset = byte_range.start;

    // Skip to end of current line (which is likely in the middle of a record)
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    if bytes_read == 0 {
        return Ok(byte_range.end);
    }
    current_offset += bytes_read as u64;

    // FASTQ format: exactly 4 lines per record
    // Line 1: @header
    // Line 2: sequence (ACGTN...)
    // Line 3: + (separator)
    // Line 4: quality (same length as sequence!)
    //
    // Critical: Quality scores can start with '@', so we must validate all 4 lines
    // The strongest check is: len(sequence) == len(quality)

    // Allow searching beyond end_offset to find a valid boundary
    // Typical FASTQ record: ~100-500 bytes, allow up to 2000 bytes search
    let search_limit = byte_range.end + 2000;

    // Strategy: Sliding window of 4 lines, checking each window for valid FASTQ pattern
    // This avoids the complexity of seeking back after false matches
    let mut lines_buffer: Vec<(String, u64)> = Vec::new(); // (line_content, line_start_offset)

    loop {
        line.clear();
        let line_start = current_offset;
        let line_bytes = reader.read_line(&mut line)?;
        if line_bytes == 0 {
            // EOF reached
            return Ok(byte_range.end);
        }

        lines_buffer.push((line.clone(), line_start));
        current_offset += line_bytes as u64;

        // Keep only last 4 lines in buffer
        if lines_buffer.len() > 4 {
            lines_buffer.remove(0);
        }

        // Check if we have a valid FASTQ record (need exactly 4 lines)
        if lines_buffer.len() == 4 {
            let (line0, offset0) = &lines_buffer[0];
            let (line1, _) = &lines_buffer[1];
            let (line2, _) = &lines_buffer[2];
            let (line3, _) = &lines_buffer[3];

            // Validate FASTQ structure:
            // 1. Line 0 starts with '@' (header)
            // 2. Line 2 starts with '+' (separator)
            // 3. Line 1 (sequence) and Line 3 (quality) have the same length
            if line0.starts_with('@') && line2.starts_with('+') {
                let seq_len = line1.trim_end().len();
                let qual_len = line3.trim_end().len();

                // Strong validation: sequence and quality must have identical length
                if seq_len == qual_len && seq_len > 0 {
                    // This is a valid FASTQ record!
                    return Ok(*offset0);
                }
            }
        }

        // Give up if we've searched too far past the end
        if current_offset >= search_limit {
            return Ok(byte_range.end);
        }
    }
}

fn open_local_fastq_reader_at_range(
    file_path: &str,
    byte_range: &FastqByteRange,
) -> Result<(Reader<BufReader<File>>, u64), Error> {
    if byte_range.start > byte_range.end {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid byte range: start ({}) > end ({})",
                byte_range.start, byte_range.end
            ),
        ));
    }

    let actual_start = resolve_fastq_range_start(file_path, byte_range)?;

    let mut file = std::fs::File::open(file_path)?;
    file.seek(SeekFrom::Start(actual_start))?;
    let reader = BufReader::new(file);
    Ok((fastq::io::Reader::new(reader), actual_start))
}

/// Create a reader that seeks to a specific byte range and handles FASTQ record boundaries
pub fn get_local_fastq_reader_with_range(
    file_path: String,
    byte_range: FastqByteRange,
) -> Result<Reader<BufReader<LimitedRangeFile>>, Error> {
    if byte_range.start > byte_range.end {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid byte range: start ({}) > end ({})",
                byte_range.start, byte_range.end
            ),
        ));
    }

    let actual_start = resolve_fastq_range_start(&file_path, &byte_range)?;
    let mut file = std::fs::File::open(&file_path)?;

    // If there's no complete record within the requested window, return an empty reader.
    if actual_start >= byte_range.end {
        file.seek(SeekFrom::Start(byte_range.end))?;
        let limited = LimitedRangeFile::new(file, byte_range.end, byte_range.end);
        return Ok(fastq::io::Reader::new(BufReader::new(limited)));
    }

    file.seek(SeekFrom::Start(actual_start))?;
    let limited = LimitedRangeFile::new(file, actual_start, byte_range.end);
    Ok(fastq::io::Reader::new(BufReader::new(limited)))
}

/// A file wrapper that limits reading to a specific byte range.
pub struct LimitedRangeFile {
    file: File,
    start_offset: u64,
    end_offset: u64,
    current_position: u64,
}

impl LimitedRangeFile {
    fn new(file: File, start_offset: u64, end_offset: u64) -> Self {
        Self {
            file,
            start_offset,
            end_offset,
            current_position: start_offset,
        }
    }
}

impl std::io::Read for LimitedRangeFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_position >= self.end_offset {
            return Ok(0);
        }

        let remaining = (self.end_offset - self.current_position) as usize;
        if remaining == 0 {
            return Ok(0);
        }

        let bytes_to_read = std::cmp::min(buf.len(), remaining);
        let bytes_read = self.file.read(&mut buf[..bytes_to_read])?;
        self.current_position += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl std::io::Seek for LimitedRangeFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(offset) => self.start_offset + offset,
            SeekFrom::Current(offset) => (self.current_position as i64 + offset) as u64,
            SeekFrom::End(offset) => (self.end_offset as i64 + offset) as u64,
        };

        let clamped = target.clamp(self.start_offset, self.end_offset);
        let actual = self.file.seek(SeekFrom::Start(clamped))?;
        self.current_position = actual;
        Ok(actual - self.start_offset)
    }
}

/// A FASTQ reader that tracks position and stops at byte boundaries
pub struct PositionTrackingFastqReader {
    reader: Reader<BufReader<File>>,
    end_offset: u64,
    current_pos: u64,
}

impl PositionTrackingFastqReader {
    /// Creates a new position-tracking FASTQ reader for byte range reading.
    ///
    /// # Arguments
    ///
    /// * `reader` - FASTQ reader for uncompressed files
    /// * `start_pos` - Byte position where reading starts
    /// * `end_offset` - Byte offset where reading should stop
    pub fn new(reader: Reader<BufReader<File>>, start_pos: u64, end_offset: u64) -> Self {
        Self {
            reader,
            end_offset,
            current_pos: start_pos,
        }
    }

    /// Read records while tracking position and respecting end boundary
    /// Strategy: Include a record if it STARTS before the end offset, even if it extends beyond
    pub fn records(&mut self) -> impl Iterator<Item = Result<noodles_fastq::Record, Error>> + '_ {
        std::iter::from_fn(move || {
            if self.current_pos >= self.end_offset {
                return None;
            }

            let mut record = noodles_fastq::Record::default();
            match self.reader.read_record(&mut record) {
                Ok(0) => None,
                Ok(bytes_read) => {
                    let record_start = self.current_pos;
                    self.current_pos += bytes_read as u64;

                    if record_start < self.end_offset {
                        Some(Ok(record))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e)),
            }
        })
    }
}

/// Creates an async GZIP-decompressing FASTQ reader for local files
///
/// # Arguments
///
/// * `file_path` - Path to the GZIP-compressed FASTQ file (local filesystem)
///
/// # Returns
///
/// An async GZIP reader wrapping a FASTQ reader
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read
pub async fn get_local_fastq_gz_reader(
    file_path: String,
) -> Result<
    fastq::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(fastq::r#async::io::Reader::new)
}

/// An async FASTQ reader that automatically detects and handles remote file compression
///
/// This enum wraps different reader implementations based on the compression format
/// detected from the remote file. It provides a unified interface for reading FASTQ
/// data from cloud storage sources (GCS, S3, Azure Blob Storage, etc.).
pub enum FastqRemoteReader {
    /// BGZF-compressed remote FASTQ file reader
    BGZF(
        fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    /// GZIP-compressed remote FASTQ file reader
    GZIP(
        fastq::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    /// Uncompressed remote FASTQ file reader
    PLAIN(fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastqRemoteReader {
    /// Creates a new remote FASTQ reader, automatically detecting compression format
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the remote FASTQ file (URL)
    /// * `object_storage_options` - Configuration for accessing remote storage
    ///
    /// # Returns
    ///
    /// A `FastqRemoteReader` enum variant matching the detected compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the compression type cannot be detected, file cannot be accessed,
    /// or the reader cannot be created
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_range(file_path, None, object_storage_options).await
    }

    /// Create reader that can optionally read only a specific byte range
    pub async fn new_with_range(
        file_path: String,
        byte_range: Option<FastqByteRange>,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(std::io::Error::other)?;

        match compression_type {
            CompressionType::BGZF => {
                // BGZF: Use byte range directly - comet-bio translates GZI blocks to byte offsets
                let reader = if let Some(range) = byte_range {
                    // Use range reading for remote BGZF files
                    get_remote_fastq_bgzf_reader_with_range(
                        file_path,
                        range,
                        object_storage_options,
                    )
                    .await?
                } else {
                    get_remote_fastq_bgzf_reader(file_path, object_storage_options).await?
                };
                Ok(FastqRemoteReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                // Regular gzip: not splittable, must read from start
                if byte_range.is_some() && byte_range.as_ref().unwrap().start > 0 {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "GZIP files cannot be split - use BGZF format for distributed reading",
                    ));
                }
                let reader = get_remote_fastq_gz_reader(file_path, object_storage_options).await?;
                Ok(FastqRemoteReader::GZIP(reader))
            }
            CompressionType::NONE => {
                // Uncompressed: direct byte range seeking supported
                let reader = if let Some(range) = byte_range {
                    // Use range reading for remote uncompressed files
                    get_remote_fastq_reader_with_range(file_path, range, object_storage_options)
                        .await?
                } else {
                    get_remote_fastq_reader(file_path, object_storage_options).await?
                };
                Ok(FastqRemoteReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }
    /// Returns a boxed async stream of FASTQ records from the remote file
    ///
    /// # Returns
    ///
    /// A boxed stream yielding `Result<Record, Error>` for each FASTQ record in the file
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastqRemoteReader::GZIP(reader) => reader.records().boxed(),
            FastqRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

/// A FASTQ reader that automatically detects and handles local file compression
///
/// This enum wraps different reader implementations based on the compression format
/// detected from the local file. For BGZF files, it uses multi-threaded decompression
/// for improved performance.
pub enum FastqLocalReader {
    /// BGZF-compressed local FASTQ file reader with multi-threaded decompression
    BGZF(fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    /// BGZF-compressed local FASTQ file reader with byte range support
    BGZFRanged(BgzfRangedFastqReader),
    /// GZIP-compressed local FASTQ file reader
    GZIP(
        fastq::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    /// Uncompressed local FASTQ file reader
    PLAIN(Reader<BufReader<File>>),
    /// Uncompressed local FASTQ file reader with byte range support
    PlainRanged(PositionTrackingFastqReader),
}

impl FastqLocalReader {
    /// Creates a new local FASTQ reader, automatically detecting compression format
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the local FASTQ file (filesystem path)
    /// * `thread_num` - Number of worker threads for BGZF decompression (used only for BGZF files)
    /// * `object_storage_options` - Configuration for compression type detection
    ///
    /// # Returns
    ///
    /// A `FastqLocalReader` enum variant matching the detected compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the compression type cannot be detected, file cannot be opened,
    /// or the reader cannot be created
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_range(file_path, thread_num, None, object_storage_options).await
    }

    /// Create reader that can optionally read only a specific byte range
    pub async fn new_with_range(
        file_path: String,
        thread_num: usize,
        byte_range: Option<FastqByteRange>,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await
        .map_err(std::io::Error::other)?;

        match compression_type {
            CompressionType::BGZF => {
                if let Some(range) = byte_range {
                    // Use IndexedReader with GZI index for range reading
                    let reader = get_local_fastq_bgzf_reader_with_range(file_path, range)?;
                    Ok(FastqLocalReader::BGZFRanged(reader))
                } else {
                    // Use MultithreadedReader for full-file reading
                    let reader = get_local_fastq_bgzf_reader(file_path, thread_num)?;
                    Ok(FastqLocalReader::BGZF(reader))
                }
            }
            CompressionType::GZIP => {
                // Regular gzip: not splittable, must read from start
                if byte_range.is_some() && byte_range.as_ref().unwrap().start > 0 {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "GZIP files cannot be split - use BGZF format for distributed reading",
                    ));
                }
                let reader = get_local_fastq_gz_reader(file_path).await?;
                Ok(FastqLocalReader::GZIP(reader))
            }
            CompressionType::NONE => {
                // Uncompressed: direct byte range seeking
                if let Some(range) = byte_range {
                    let (reader, actual_start) =
                        open_local_fastq_reader_at_range(&file_path, &range)?;
                    let tracking_reader =
                        PositionTrackingFastqReader::new(reader, actual_start, range.end);
                    Ok(FastqLocalReader::PlainRanged(tracking_reader))
                } else {
                    let reader = get_local_fastq_reader(file_path)?;
                    Ok(FastqLocalReader::PLAIN(reader))
                }
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }

    /// Returns a boxed async stream of FASTQ records from the local file
    ///
    /// Note: For synchronous readers (BGZF and PLAIN), records are wrapped in an async stream.
    ///
    /// # Returns
    ///
    /// A boxed stream yielding `Result<Record, Error>` for each FASTQ record in the file
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::BGZFRanged(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::GZIP(reader) => reader.records().boxed(),
            FastqLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::PlainRanged(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
