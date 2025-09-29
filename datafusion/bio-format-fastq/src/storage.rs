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

/// Create a reader that seeks to a specific byte range and handles FASTQ record boundaries
pub fn get_local_fastq_reader_with_range(
    file_path: String,
    byte_range: FastqByteRange,
) -> Result<Reader<BufReader<LimitedRangeFile>>, Error> {
    // Validate byte range
    if byte_range.start > byte_range.end {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid byte range: start ({}) > end ({})", byte_range.start, byte_range.end)
        ));
    }

    let mut file = std::fs::File::open(&file_path)?;

    // Seek to the start position
    let actual_start = if byte_range.start > 0 {
        file.seek(SeekFrom::Start(byte_range.start))?;

        // Find the next FASTQ record boundary (line starting with '@')
        let mut buf_reader = BufReader::new(&mut file);
        let boundary_start = find_next_fastq_record_boundary(&mut buf_reader, byte_range.start)?;

        // If boundary is beyond end offset, return empty range
        if boundary_start >= byte_range.end {
            // Create an empty range file
            file = std::fs::File::open(&file_path)?;
            let limited_file = LimitedRangeFile::new(file, boundary_start, boundary_start);
            let reader = BufReader::new(limited_file);
            return Ok(fastq::io::Reader::new(reader));
        }

        // Reopen file and seek to the actual start boundary
        file = std::fs::File::open(&file_path)?;
        file.seek(SeekFrom::Start(boundary_start))?;
        boundary_start
    } else {
        0
    };

    // Create a limited range file wrapper that stops reading at end offset
    let limited_file = LimitedRangeFile::new(file, actual_start, byte_range.end);
    let reader = BufReader::new(limited_file);
    Ok(fastq::io::Reader::new(reader))
}

/// A file wrapper that limits reading to a specific byte range
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
            return Ok(0); // EOF - no more bytes to read in this range
        }

        let remaining_bytes = (self.end_offset - self.current_position) as usize;
        let bytes_to_read = std::cmp::min(buf.len(), remaining_bytes);

        if bytes_to_read == 0 {
            return Ok(0);
        }

        let bytes_read = self.file.read(&mut buf[..bytes_to_read])?;
        self.current_position += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl std::io::Seek for LimitedRangeFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => self.start_offset + offset,
            SeekFrom::Current(offset) => (self.current_position as i64 + offset) as u64,
            SeekFrom::End(offset) => (self.end_offset as i64 + offset) as u64,
        };

        // Clamp position to valid range
        let clamped_pos = std::cmp::max(self.start_offset, std::cmp::min(new_pos, self.end_offset));

        let actual_pos = self.file.seek(SeekFrom::Start(clamped_pos))?;
        self.current_position = actual_pos;
        Ok(actual_pos - self.start_offset)
    }
}

/// Find the next FASTQ record boundary starting from current position
fn find_next_fastq_record_boundary(reader: &mut BufReader<&mut File>, start_offset: u64) -> Result<u64, Error> {
    let mut current_offset = start_offset;
    let mut line = String::new();

    // If we're at the start of file, no need to search
    if start_offset == 0 {
        return Ok(0);
    }

    // First, skip to end of current line to get to a line boundary
    line.clear();
    let bytes_read = reader.read_line(&mut line)?;
    if bytes_read == 0 {
        // EOF reached
        return Ok(current_offset);
    }
    current_offset += bytes_read as u64;

    // Now search for next FASTQ record starting with '@'
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;

        if bytes_read == 0 {
            // EOF reached, return current offset
            return Ok(current_offset);
        }

        // Check if this line starts a new FASTQ record
        if line.starts_with('@') && line.trim().len() > 1 {
            // Found a potential record start
            return Ok(current_offset);
        }

        current_offset += bytes_read as u64;

        // Safety check to avoid infinite loops
        if current_offset > start_offset + 10000 {
            // If we can't find a boundary within 10KB, just return current position
            return Ok(current_offset);
        }
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
    PlainRanged(Reader<BufReader<LimitedRangeFile>>),
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
                    // Use the new byte range reader with proper boundary synchronization
                    let reader = get_local_fastq_reader_with_range(file_path, range)?;
                    Ok(FastqLocalReader::PlainRanged(reader))
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
