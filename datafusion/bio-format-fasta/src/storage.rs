use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bgzf;
use noodles_fasta as fasta;
use noodles_fasta::Record;
use noodles_fasta::io::Reader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
use tokio_util::io::StreamReader;

/// Creates an async FASTA reader for BGZF-compressed files in cloud storage.
///
/// # Arguments
///
/// * `file_path` - URI to the FASTA file in cloud storage (e.g., `gs://bucket/file.fasta.bgz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async FASTA reader capable of reading BGZF-compressed records from cloud storage.
pub async fn get_remote_fasta_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fasta::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = fasta::r#async::io::Reader::new(inner);
    Ok(reader)
}

/// Creates an async FASTA reader for uncompressed files in cloud storage.
///
/// # Arguments
///
/// * `file_path` - URI to the FASTA file in cloud storage (e.g., `s3://bucket/file.fasta`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async FASTA reader for reading uncompressed FASTA records from cloud storage.
pub async fn get_remote_fasta_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fasta::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = fasta::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Creates an async FASTA reader for GZIP-compressed files in cloud storage.
///
/// # Arguments
///
/// * `file_path` - URI to the FASTA file in cloud storage (e.g., `gs://bucket/file.fasta.gz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async FASTA reader capable of reading GZIP-compressed records from cloud storage.
pub async fn get_remote_fasta_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fasta::r#async::io::Reader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = fasta::r#async::io::Reader::new(stream);
    Ok(reader)
}

/// Creates a FASTA reader for BGZF-compressed local files.
///
/// # Arguments
///
/// * `file_path` - Path to the BGZF-compressed FASTA file
///
/// # Returns
///
/// A FASTA reader with single-threaded BGZF decompression.
/// Parallel reading is handled at the partition level.
pub fn get_local_fasta_bgzf_reader(
    file_path: String,
) -> Result<fasta::io::Reader<bgzf::Reader<std::fs::File>>, Error> {
    std::fs::File::open(file_path)
        .map(bgzf::Reader::new)
        .map(fasta::io::Reader::new)
}

/// Creates a FASTA reader for uncompressed local files.
///
/// # Arguments
///
/// * `file_path` - Path to the uncompressed FASTA file
///
/// # Returns
///
/// A buffered FASTA reader for sequential file reading.
pub fn get_local_fasta_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fasta::io::Reader::new)
}

/// Creates an async FASTA reader for GZIP-compressed local files.
///
/// # Arguments
///
/// * `file_path` - Path to the GZIP-compressed FASTA file
///
/// # Returns
///
/// An async FASTA reader with GZIP decompression support.
pub async fn get_local_fasta_gz_reader(
    file_path: String,
) -> Result<
    fasta::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(fasta::r#async::io::Reader::new)
}

/// Async FASTA reader abstraction for cloud storage with multiple compression format support.
///
/// This enum handles reading FASTA files from cloud storage (GCS, S3, Azure) with automatic
/// detection and support for BGZF, GZIP, and uncompressed formats.
///
/// # Variants
///
/// - `BGZF`: Reads BGZF-compressed FASTA files
/// - `GZIP`: Reads GZIP-compressed FASTA files
/// - `PLAIN`: Reads uncompressed FASTA files
pub enum FastaRemoteReader {
    /// BGZF-compressed reader variant
    BGZF(
        fasta::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    /// GZIP-compressed reader variant
    GZIP(
        fasta::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    /// Uncompressed reader variant
    PLAIN(fasta::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastaRemoteReader {
    /// Creates a new FASTA reader for a remote file with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - URI to the FASTA file in cloud storage
    /// * `object_storage_options` - Configuration for cloud storage access
    ///
    /// # Returns
    ///
    /// A new `FastaRemoteReader` instance with the appropriate compression handler.
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
                    get_remote_fasta_bgzf_reader(file_path, object_storage_options).await?;
                Ok(FastaRemoteReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                let reader = get_remote_fasta_gz_reader(file_path, object_storage_options).await?;
                Ok(FastaRemoteReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader = get_remote_fasta_reader(file_path, object_storage_options).await?;
                Ok(FastaRemoteReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTA reader: {:?}",
                compression_type
            ),
        }
    }

    /// Returns an async stream of FASTA records from the remote file.
    ///
    /// # Returns
    ///
    /// A boxed stream yielding `Record` items or errors.
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastaRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastaRemoteReader::GZIP(reader) => reader.records().boxed(),
            FastaRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

/// FASTA reader abstraction for local files with multiple compression format support.
///
/// This enum handles reading FASTA files from the local filesystem with automatic
/// detection and support for BGZF, GZIP, and uncompressed formats.
///
/// # Variants
///
/// - `BGZF`: Reads BGZF-compressed files with multithreaded decompression
/// - `GZIP`: Reads GZIP-compressed files asynchronously
/// - `PLAIN`: Reads uncompressed files synchronously
#[allow(clippy::large_enum_variant)]
pub enum FastaLocalReader {
    /// BGZF-compressed reader variant
    BGZF(fasta::io::Reader<bgzf::Reader<std::fs::File>>),
    /// GZIP-compressed reader variant
    GZIP(
        fasta::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    /// Uncompressed reader variant
    PLAIN(Reader<BufReader<File>>),
}

impl FastaLocalReader {
    /// Creates a new FASTA reader for a local file with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the FASTA file on the local filesystem
    /// * `object_storage_options` - Configuration (including compression type hints)
    ///
    /// # Returns
    ///
    /// A new `FastaLocalReader` instance with the appropriate compression handler.
    pub async fn new(
        file_path: String,
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
                let reader = get_local_fasta_bgzf_reader(file_path)?;
                Ok(FastaLocalReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                // GZIP is treated as BGZF for local files
                let reader = get_local_fasta_gz_reader(file_path).await?;
                Ok(FastaLocalReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader = get_local_fasta_reader(file_path)?;
                Ok(FastaLocalReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTA reader: {:?}",
                compression_type
            ),
        }
    }

    /// Returns an async stream of FASTA records from the local file.
    ///
    /// # Returns
    ///
    /// A boxed stream yielding `Record` items or errors.
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastaLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastaLocalReader::GZIP(reader) => reader.records().boxed(),
            FastaLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}

/// Synchronous local FASTA reader for BGZF and uncompressed files.
/// Used by the buffer-reuse read path to avoid per-record clone allocations.
pub enum FastaSyncReader {
    /// BGZF-compressed reader.
    Bgzf(fasta::io::Reader<bgzf::Reader<std::fs::File>>),
    /// Uncompressed reader with buffered I/O.
    Plain(fasta::io::Reader<BufReader<File>>),
}

impl FastaSyncReader {
    /// Reads a raw definition line into the buffer.
    /// Returns 0 at EOF.
    pub fn read_definition(&mut self, buf: &mut String) -> std::io::Result<usize> {
        match self {
            FastaSyncReader::Bgzf(r) => r.read_definition(buf),
            FastaSyncReader::Plain(r) => r.read_definition(buf),
        }
    }

    /// Reads a sequence into the buffer.
    /// Returns the number of bases read.
    pub fn read_sequence(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            FastaSyncReader::Bgzf(r) => r.read_sequence(buf),
            FastaSyncReader::Plain(r) => r.read_sequence(buf),
        }
    }
}

/// Opens a local FASTA file synchronously with the given compression type.
/// Supports BGZF and uncompressed formats for the buffer-reuse read path.
pub fn open_local_fasta_sync(
    file_path: &str,
    compression_type: CompressionType,
) -> std::io::Result<FastaSyncReader> {
    match compression_type {
        CompressionType::BGZF => Ok(FastaSyncReader::Bgzf(get_local_fasta_bgzf_reader(
            file_path.to_string(),
        )?)),
        CompressionType::NONE => Ok(FastaSyncReader::Plain(get_local_fasta_reader(
            file_path.to_string(),
        )?)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("Sync FASTA reader does not support compression: {compression_type:?}"),
        )),
    }
}
