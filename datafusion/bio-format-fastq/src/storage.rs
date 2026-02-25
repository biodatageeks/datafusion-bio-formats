//! Storage layer for reading FASTQ files from local and remote sources
//!
//! This module provides functions for creating FASTQ readers with support for:
//! - Multiple compression formats (BGZF, GZIP, uncompressed)
//! - Local and remote file sources (GCS, S3, Azure Blob Storage)

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
pub async fn get_remote_fastq_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Creates an async GZIP-decompressing FASTQ reader for remote files
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

/// Creates a synchronous BGZF-decompressing FASTQ reader for local files.
///
/// Uses single-threaded BGZF decompression.
/// Parallel reading is handled at the partition level by the execution plan.
pub fn get_local_fastq_bgzf_reader(
    file_path: String,
) -> Result<fastq::io::Reader<bgzf::Reader<std::fs::File>>, Error> {
    std::fs::File::open(file_path)
        .map(bgzf::Reader::new)
        .map(fastq::io::Reader::new)
}

/// Creates a synchronous FASTQ reader for uncompressed local files
pub fn get_local_fastq_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fastq::io::Reader::new)
}

/// Creates an async GZIP-decompressing FASTQ reader for local files
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
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(std::io::Error::other)?;
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
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastqRemoteReader::GZIP(reader) => reader.records().boxed(),
            FastqRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

/// A FASTQ reader that automatically detects and handles local file compression
#[allow(clippy::large_enum_variant)]
pub enum FastqLocalReader {
    /// BGZF-compressed local FASTQ file reader
    BGZF(fastq::io::Reader<bgzf::Reader<std::fs::File>>),
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
    /// * `file_path` - Path to the local FASTQ file
    /// * `object_storage_options` - Optional configuration for compression type detection.
    ///   If `None`, compression is detected from magic bytes.
    pub async fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Result<Self, Error> {
        let compression_type = match &object_storage_options {
            Some(opts) => get_compression_type(
                file_path.clone(),
                opts.compression_type.clone(),
                opts.clone(),
            )
            .await
            .map_err(std::io::Error::other)?,
            None => {
                // Detect compression from magic bytes synchronously
                let detected = crate::physical_exec::detect_compression_sync(&file_path)?;
                match detected {
                    crate::physical_exec::DetectedCompression::Bgzf => CompressionType::BGZF,
                    crate::physical_exec::DetectedCompression::Gzip => CompressionType::GZIP,
                    crate::physical_exec::DetectedCompression::None => CompressionType::NONE,
                }
            }
        };
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_fastq_bgzf_reader(file_path)?;
                Ok(FastqLocalReader::BGZF(reader))
            }
            CompressionType::GZIP => {
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
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::GZIP(reader) => reader.records().boxed(),
            FastqLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
