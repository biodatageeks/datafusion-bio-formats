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
use noodles_fastq as fastq;
use noodles_fastq::Record;
use noodles_fastq::io::Reader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, Seek, SeekFrom};
use tokio_util::io::StreamReader;

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
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let stream_reader = StreamReader::new(stream);
    let bgzf_reader = bgzf::r#async::Reader::new(stream_reader);
    let fastq_reader = fastq::r#async::io::Reader::new(bgzf_reader);
    Ok(fastq_reader)
}

pub async fn get_remote_fastq_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

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
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

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

pub fn get_local_fastq_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(fastq::io::Reader::new);
    reader
}

pub fn get_local_fastq_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fastq::io::Reader::new);
    reader
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

pub async fn get_local_fastq_gz_reader(
    file_path: String,
) -> Result<
    fastq::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(fastq::r#async::io::Reader::new);
    reader
}

pub enum FastqRemoteReader {
    BGZF(
        fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    GZIP(
        fastq::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    PLAIN(fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastqRemoteReader {
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
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

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
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastqRemoteReader::GZIP(reader) => reader.records().boxed(),
            FastqRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub enum FastqLocalReader {
    BGZF(fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    GZIP(
        fastq::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    PLAIN(Reader<BufReader<File>>),
    PlainRanged(PositionTrackingFastqReader),
}

impl FastqLocalReader {
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        match compression_type {
            CompressionType::BGZF => {
                // For distributed reading, force single-threaded to avoid conflicts
                let effective_thread_num = if byte_range.is_some() { 1 } else { thread_num };
                let reader = get_local_fastq_bgzf_reader(file_path, effective_thread_num)?;
                Ok(FastqLocalReader::BGZF(reader))
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

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::GZIP(reader) => reader.records().boxed(),
            FastqLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::PlainRanged(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
