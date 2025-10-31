//! Storage layer for reading FASTQ files from local and remote sources
//!
//! This module provides functions for creating FASTQ readers with support for:
//! - Multiple compression formats (BGZF, GZIP, uncompressed)
//! - Local and remote file sources (GCS, S3, Azure Blob Storage)
//! - Multi-threaded reading for BGZF files

use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bgzf;
use noodles_fastq as fastq;
use noodles_fastq::Record;
use noodles_fastq::io::Reader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
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
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        match compression_type {
            CompressionType::BGZF => {
                let reader =
                    get_remote_fastq_bgzf_reader(file_path, object_storage_options).await?;
                Ok(FastqRemoteReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                let reader = get_remote_fastq_gz_reader(file_path, object_storage_options).await?;
                Ok(FastqRemoteReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader = get_remote_fastq_reader(file_path, object_storage_options).await?;
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
    /// GZIP-compressed local FASTQ file reader
    GZIP(
        fastq::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    /// Uncompressed local FASTQ file reader
    PLAIN(Reader<BufReader<File>>),
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
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_fastq_bgzf_reader(file_path, thread_num)?;
                Ok(FastqLocalReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                // GZIP is treated as BGZF for local files
                let reader = get_local_fastq_gz_reader(file_path).await?;
                Ok(FastqLocalReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader = get_local_fastq_reader(file_path)?;
                Ok(FastqLocalReader::PLAIN(reader))
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
            FastqLocalReader::GZIP(reader) => reader.records().boxed(),
            FastqLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
