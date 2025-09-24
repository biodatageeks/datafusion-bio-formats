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
use std::io::{BufReader, Error, Seek, SeekFrom};
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
) -> Result<Reader<BufReader<File>>, Error> {
    let mut file = std::fs::File::open(file_path)?;

    // Seek to the start position
    if byte_range.start > 0 {
        file.seek(SeekFrom::Start(byte_range.start))?;
    }

    let reader = BufReader::new(file);
    Ok(fastq::io::Reader::new(reader))
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
    // For now, use PLAIN reader for range reading to simplify implementation
    // TODO: Implement proper range reading with boundary synchronization
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
                    // For now, use the regular reader with range seeking
                    // TODO: Implement proper boundary synchronization
                    let reader = get_local_fastq_reader_with_range(file_path, range)?;
                    Ok(FastqLocalReader::PLAIN(reader))
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
        }
    }
}
