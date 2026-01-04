use crate::table_provider::VcfByteRange;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion::arrow;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, get_remote_stream_with_range,
    get_storage_type,
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use log::debug;
use log::info;
use noodles_bgzf::{AsyncReader, MultithreadedReader, gzi};
use noodles_vcf as vcf;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, Seek, SeekFrom};
use std::num::NonZero;
use std::sync::Arc;
use tokio_util::io::StreamReader;
use vcf::io::Reader;
use vcf::{Header, Record};

/// Creates a remote BGZF-compressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf.bgz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for BGZF-compressed data
pub async fn get_remote_vcf_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options)
        .await
        .unwrap();
    vcf::r#async::io::Reader::new(inner)
}

/// Creates a remote GZIP-compressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf.gz`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for GZIP-compressed data
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_vcf_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    vcf::r#async::io::Reader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = vcf::r#async::io::Reader::new(stream);
    Ok(reader)
}

/// Creates a remote uncompressed VCF reader for cloud storage.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file (e.g., `s3://bucket/file.vcf`)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
///
/// An async VCF reader for uncompressed data
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the stream cannot be created
pub async fn get_remote_vcf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, std::io::Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None)
        .await
        .map_err(std::io::Error::other)?;
    let inner = StreamReader::new(stream);
    Ok(vcf::r#async::io::Reader::new(inner))
}

/// Creates a local BGZF-compressed VCF reader with multithreading support.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
/// * `thread_num` - Number of worker threads for parallel decompression
///
/// # Returns
///
/// A VCF reader with multithreaded BGZF decompression
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub fn get_local_vcf_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<Reader<MultithreadedReader<File>>, Error> {
    debug!(
        "Reading VCF file from local storage with {} threads",
        thread_num
    );
    File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(vcf::io::Reader::new)
}

/// Creates a local GZIP-compressed VCF reader.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
///
/// # Returns
///
/// An async VCF reader for GZIP-compressed data
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub async fn get_local_vcf_gz_reader(
    file_path: String,
) -> Result<
    vcf::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(vcf::r#async::io::Reader::new)
}

/// Creates a local uncompressed VCF reader.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
///
/// # Returns
///
/// An async VCF reader for uncompressed data
///
/// # Errors
///
/// Returns an error if the file cannot be opened
pub async fn get_local_vcf_reader(
    file_path: String,
) -> Result<vcf::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>, Error> {
    debug!("Reading VCF file from local storage with async reader");
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(vcf::r#async::io::Reader::new)?;
    Ok(reader)
}

/// Gets the VCF header from a local file, auto-detecting compression.
///
/// # Arguments
///
/// * `file_path` - Path to the local VCF file
/// * `thread_num` - Number of worker threads for BGZF decompression
/// * `object_storage_options` - Configuration for compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be read or the header is invalid
pub async fn get_local_vcf_header(
    file_path: String,
    thread_num: usize,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::Header, Error> {
    let compression_type = get_compression_type(
        file_path.clone(),
        object_storage_options.compression_type.clone(),
        object_storage_options.clone(),
    )
    .await
    .map_err(std::io::Error::other)?;
    let header = match compression_type {
        CompressionType::BGZF => {
            let mut reader = get_local_vcf_bgzf_reader(file_path, thread_num)?;
            reader.read_header()?
        }
        CompressionType::GZIP => {
            let mut reader = get_local_vcf_gz_reader(file_path).await?;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_local_vcf_reader(file_path).await?;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

/// Gets the VCF header from a remote file, auto-detecting compression.
///
/// # Arguments
///
/// * `file_path` - Path or URI to the remote VCF file
/// * `object_storage_options` - Configuration for cloud storage and compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the header is invalid
pub async fn get_remote_vcf_header(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::Header, Error> {
    info!(
        "Getting remote VCF header with options: {}",
        object_storage_options
    );
    let compression_type = get_compression_type(
        file_path.clone(),
        object_storage_options.clone().compression_type,
        object_storage_options.clone(),
    )
    .await
    .map_err(std::io::Error::other)?;
    let header = match compression_type {
        CompressionType::BGZF => {
            let mut reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
            reader.read_header().await?
        }
        CompressionType::GZIP => {
            let mut reader = get_remote_vcf_gz_reader(file_path, object_storage_options).await?;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_remote_vcf_reader(file_path, object_storage_options).await?;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

/// Gets the VCF header from either a local or remote file, auto-detecting storage type and compression.
///
/// # Arguments
///
/// * `file_path` - Path to the VCF file (local path or cloud URI)
/// * `object_storage_options` - Configuration for cloud storage and compression detection
///
/// # Returns
///
/// The parsed VCF header
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or the header is invalid
pub async fn get_header(
    file_path: String,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<vcf::Header, Error> {
    let storage_type = get_storage_type(file_path.clone());
    let header = match storage_type {
        StorageType::LOCAL => {
            get_local_vcf_header(file_path, 1, object_storage_options.unwrap().clone()).await?
        }
        _ => get_remote_vcf_header(file_path, object_storage_options.unwrap().clone()).await?,
    };
    Ok(header)
}

// ============================================================================
// Byte Range Reading Functions
// ============================================================================

/// Read GZI index from file
fn read_gzi_index(gzi_path: &str) -> Result<gzi::Index, Error> {
    gzi::read(std::path::Path::new(gzi_path))
        .map_err(|e| Error::other(format!("Failed to read GZI index from {}: {}", gzi_path, e)))
}

/// Create a local BGZF VCF reader that reads only a specific byte range
/// Uses GZI index to find BGZF block boundaries
pub fn get_local_vcf_bgzf_reader_with_range(
    file_path: String,
    byte_range: VcfByteRange,
) -> Result<Reader<noodles_bgzf::IndexedReader<BufReader<File>>>, Error> {
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

    // Create VCF reader
    Ok(vcf::io::Reader::new(reader))
}

/// Helper to find next variant line (skip header lines starting with '#')
fn open_local_vcf_reader_at_range(
    file_path: &str,
    range: &VcfByteRange,
) -> Result<(vcf::io::Reader<BufReader<File>>, u64), Error> {
    if range.start > range.end {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid byte range: start ({}) > end ({})",
                range.start, range.end
            ),
        ));
    }

    let file = std::fs::File::open(file_path)?;
    let mut file = BufReader::new(file);

    // Seek to start position
    file.seek(SeekFrom::Start(range.start))?;

    // Find next variant line (skip header lines starting with '#')
    let mut actual_start = range.start;
    let mut buffer = Vec::new();

    loop {
        let pos_before = file.stream_position()?;
        buffer.clear();
        let bytes_read = file.read_until(b'\n', &mut buffer)?;

        if bytes_read == 0 {
            break; // EOF
        }

        // Check if this is a variant line (doesn't start with '#')
        if !buffer.is_empty() && buffer[0] != b'#' {
            actual_start = pos_before;
            break;
        }
    }

    // Seek back to actual start
    file.seek(SeekFrom::Start(actual_start))?;

    let reader = vcf::io::Reader::new(file);
    Ok((reader, actual_start))
}

/// Create a local uncompressed VCF reader that reads only a specific byte range
pub fn get_local_vcf_reader_with_range(
    file_path: String,
    byte_range: VcfByteRange,
) -> Result<vcf::io::Reader<BufReader<File>>, Error> {
    if byte_range.start > byte_range.end {
        return Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid byte range: start ({}) > end ({})",
                byte_range.start, byte_range.end
            ),
        ));
    }

    // Handle zero-byte range
    if byte_range.start == byte_range.end {
        // Create an empty reader by seeking to end
        let file = std::fs::File::open(&file_path)?;
        let mut file = BufReader::new(file);
        file.seek(SeekFrom::Start(byte_range.end))?;
        return Ok(vcf::io::Reader::new(file));
    }

    let (reader, _actual_start) = open_local_vcf_reader_at_range(&file_path, &byte_range)?;
    Ok(reader)
}

/// Create a remote BGZF VCF reader that reads only a specific byte range
pub async fn get_remote_vcf_bgzf_reader_with_range(
    file_path: String,
    byte_range: VcfByteRange,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>, Error> {
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
    let bgzf_reader = AsyncReader::new(stream_reader);
    let vcf_reader = vcf::r#async::io::Reader::new(bgzf_reader);
    Ok(vcf_reader)
}

/// Create a remote uncompressed VCF reader that reads only a specific byte range
pub async fn get_remote_vcf_reader_with_range(
    file_path: String,
    byte_range: VcfByteRange,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream_with_range(
        file_path.clone(),
        object_storage_options,
        byte_range.start,
        byte_range.end,
    )
    .await
    .map_err(std::io::Error::other)?;

    let reader = vcf::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Unified reader for remote VCF files with multiple compression format support.
///
/// This enum handles BGZF, GZIP, and uncompressed remote VCF files from cloud storage.
/// The appropriate variant is created based on the detected compression type.
pub enum VcfRemoteReader {
    /// Reader for BGZF-compressed remote VCF files.
    BGZF(vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    /// Reader for GZIP-compressed remote VCF files.
    GZIP(
        vcf::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>,
        >,
    ),
    /// Reader for uncompressed remote VCF files.
    PLAIN(vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl VcfRemoteReader {
    /// Creates a new remote VCF reader with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path or URI to the remote VCF file
    /// * `object_storage_options` - Configuration for cloud storage access
    ///
    /// # Returns
    ///
    /// A `VcfRemoteReader` instance with the appropriate variant for the detected compression
    pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
        Self::new_with_range(file_path, None, object_storage_options).await
    }

    /// Creates a new remote VCF reader with byte range support.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path or URI to the remote VCF file
    /// * `byte_range` - Optional byte range for reading only a portion of the file
    /// * `object_storage_options` - Configuration for cloud storage access
    ///
    /// # Returns
    ///
    /// A `VcfRemoteReader` instance with the appropriate variant for the detected compression
    pub async fn new_with_range(
        file_path: String,
        byte_range: Option<VcfByteRange>,
        object_storage_options: ObjectStorageOptions,
    ) -> Self {
        info!("Creating remote VCF reader: {}", object_storage_options);
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
            object_storage_options.clone(),
        )
        .await
        .unwrap_or(CompressionType::NONE);
        match compression_type {
            CompressionType::BGZF => {
                let reader = if let Some(range) = byte_range {
                    get_remote_vcf_bgzf_reader_with_range(file_path, range, object_storage_options)
                        .await
                        .unwrap()
                } else {
                    get_remote_vcf_bgzf_reader(file_path, object_storage_options).await
                };
                VcfRemoteReader::BGZF(reader)
            }
            CompressionType::GZIP => {
                // Regular gzip: not splittable, must read from start
                if byte_range.is_some() && byte_range.as_ref().unwrap().start > 0 {
                    panic!("GZIP files cannot be split - use BGZF format for distributed reading");
                }
                let reader = get_remote_vcf_gz_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                VcfRemoteReader::GZIP(reader)
            }
            CompressionType::NONE => {
                let reader = if let Some(range) = byte_range {
                    get_remote_vcf_reader_with_range(file_path, range, object_storage_options)
                        .await
                        .unwrap()
                } else {
                    get_remote_vcf_reader(file_path, object_storage_options)
                        .await
                        .unwrap()
                };
                VcfRemoteReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.read_header().await,
            VcfRemoteReader::GZIP(reader) => reader.read_header().await,
            VcfRemoteReader::PLAIN(reader) => reader.read_header().await,
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfRemoteReader::GZIP(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfRemoteReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
        }
    }

    /// Reads VCF records as an async stream.
    ///
    /// # Returns
    ///
    /// A boxed async stream of VCF records
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.records().boxed(),
            VcfRemoteReader::GZIP(reader) => reader.records().boxed(),
            VcfRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

/// Unified reader for local VCF files with multiple compression format support.
///
/// This enum handles BGZF (multithreaded), GZIP, and uncompressed local VCF files.
/// The appropriate variant is created based on the detected compression type.
pub enum VcfLocalReader {
    /// Reader for BGZF-compressed local VCF files with multithreading support.
    BGZF(Reader<MultithreadedReader<File>>),
    /// Reader for BGZF-compressed local VCF files with byte range support.
    BGZFRanged(Reader<noodles_bgzf::IndexedReader<BufReader<File>>>),
    /// Reader for GZIP-compressed local VCF files.
    GZIP(
        vcf::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    /// Reader for uncompressed local VCF files.
    PLAIN(vcf::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>),
    /// Reader for uncompressed local VCF files with byte range support.
    PlainRanged(Reader<BufReader<File>>),
}

impl VcfLocalReader {
    /// Creates a new local VCF reader with automatic compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the local VCF file
    /// * `thread_num` - Number of worker threads for BGZF decompression
    /// * `object_storage_options` - Configuration for compression detection
    ///
    /// # Returns
    ///
    /// A `VcfLocalReader` instance with the appropriate variant for the detected compression
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Self {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
            object_storage_options.clone(),
        )
        .await
        .unwrap_or(CompressionType::NONE);
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_vcf_bgzf_reader(file_path, thread_num).unwrap();
                VcfLocalReader::BGZF(reader)
            }
            CompressionType::GZIP => {
                let reader = get_local_vcf_gz_reader(file_path).await.unwrap();
                VcfLocalReader::GZIP(reader)
            }
            CompressionType::NONE => {
                let reader = get_local_vcf_reader(file_path).await.unwrap();
                VcfLocalReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    /// Creates a new local VCF reader with byte range support.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the local VCF file
    /// * `thread_num` - Number of worker threads for BGZF decompression
    /// * `byte_range` - Optional byte range for reading only a portion of the file
    /// * `object_storage_options` - Configuration for compression detection
    ///
    /// # Returns
    ///
    /// A `VcfLocalReader` instance with the appropriate variant for the detected compression
    pub async fn new_with_range(
        file_path: String,
        thread_num: usize,
        byte_range: Option<VcfByteRange>,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
            object_storage_options.clone(),
        )
        .await
        .map_err(std::io::Error::other)?;

        match compression_type {
            CompressionType::BGZF => {
                if let Some(range) = byte_range {
                    // Use IndexedReader with GZI index for range reading
                    let reader = get_local_vcf_bgzf_reader_with_range(file_path, range)?;
                    Ok(VcfLocalReader::BGZFRanged(reader))
                } else {
                    // Use MultithreadedReader for full-file reading
                    let reader = get_local_vcf_bgzf_reader(file_path, thread_num)?;
                    Ok(VcfLocalReader::BGZF(reader))
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
                let reader = get_local_vcf_gz_reader(file_path).await?;
                Ok(VcfLocalReader::GZIP(reader))
            }
            CompressionType::NONE => {
                // Uncompressed: direct byte range seeking
                if let Some(range) = byte_range {
                    let reader = get_local_vcf_reader_with_range(file_path, range)?;
                    Ok(VcfLocalReader::PlainRanged(reader))
                } else {
                    let reader = get_local_vcf_reader(file_path).await?;
                    Ok(VcfLocalReader::PLAIN(reader))
                }
            }
            _ => unimplemented!(
                "Unsupported compression type for VCF reader: {:?}",
                compression_type
            ),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => reader.read_header(),
            VcfLocalReader::BGZFRanged(reader) => reader.read_header(),
            VcfLocalReader::GZIP(reader) => reader.read_header().await,
            VcfLocalReader::PLAIN(reader) => reader.read_header().await,
            VcfLocalReader::PlainRanged(reader) => reader.read_header(),
        }
    }

    /// Reads VCF records as a stream.
    ///
    /// # Returns
    ///
    /// A boxed stream of VCF records
    pub fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            VcfLocalReader::BGZFRanged(reader) => stream::iter(reader.records()).boxed(),
            VcfLocalReader::GZIP(reader) => reader.records().boxed(),
            VcfLocalReader::PLAIN(reader) => reader.records().boxed(),
            VcfLocalReader::PlainRanged(reader) => stream::iter(reader.records()).boxed(),
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => {
                let header = reader.read_header()?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::BGZFRanged(reader) => {
                let header = reader.read_header()?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::GZIP(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::PlainRanged(reader) => {
                let header = reader.read_header()?;
                Ok(get_info_fields(&header).await)
            }
        }
    }
}

/// Extracts INFO field metadata from a VCF header into a RecordBatch.
///
/// # Arguments
///
/// * `header` - The VCF header to extract INFO fields from
///
/// # Returns
///
/// A RecordBatch with columns: name (String), type (String), description (String)
pub async fn get_info_fields(header: &Header) -> arrow::array::RecordBatch {
    let info_fields = header.infos();
    let mut field_names = StringBuilder::new();
    let mut field_types = StringBuilder::new();
    let mut field_descriptions = StringBuilder::new();
    for (field_name, field) in info_fields {
        field_names.append_value(field_name);
        field_types.append_value(field.ty());
        field_descriptions.append_value(field.description());
    }
    // build RecordBatch
    let field_names = field_names.finish();
    let field_types = field_types.finish();
    let field_descriptions = field_descriptions.finish();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("type", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("description", arrow::datatypes::DataType::Utf8, false),
    ]);
    arrow::record_batch::RecordBatch::try_new(
        SchemaRef::from(schema.clone()),
        vec![
            Arc::new(field_names),
            Arc::new(field_types),
            Arc::new(field_descriptions),
        ],
    )
    .unwrap()
}

/// Unified reader for both local and remote VCF files.
///
/// This is the primary entry point for reading VCF files from any source (local, S3, GCS, etc).
/// It automatically detects the storage type and compression format.
pub enum VcfReader {
    /// Reader for local VCF files.
    Local(VcfLocalReader),
    /// Reader for remote VCF files.
    Remote(VcfRemoteReader),
}

impl VcfReader {
    /// Creates a new VCF reader with automatic storage type and compression detection.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the VCF file (local path or cloud URI)
    /// * `thread_num` - Optional number of worker threads for BGZF decompression
    /// * `object_storage_options` - Optional configuration for cloud storage and compression
    ///
    /// # Returns
    ///
    /// A `VcfReader` instance configured for the detected storage and compression type
    pub async fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        info!(
            "Storage type for VCF file {}: {:?}",
            file_path, storage_type
        );
        match storage_type {
            StorageType::LOCAL => VcfReader::Local(
                VcfLocalReader::new(
                    file_path,
                    thread_num.unwrap_or(1),
                    object_storage_options.unwrap(),
                )
                .await,
            ),
            _ => VcfReader::Remote(
                VcfRemoteReader::new(file_path, object_storage_options.unwrap()).await,
            ),
        }
    }

    /// Reads the VCF header.
    ///
    /// # Returns
    ///
    /// The parsed VCF header
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfReader::Local(reader) => reader.read_header().await,
            VcfReader::Remote(reader) => reader.read_header().await,
        }
    }

    /// Reads INFO field metadata from the VCF header.
    ///
    /// # Returns
    ///
    /// A RecordBatch containing field names, types, and descriptions
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfReader::Local(reader) => reader.describe().await,
            VcfReader::Remote(reader) => reader.describe().await,
        }
    }

    /// Reads VCF records as an async stream.
    ///
    /// # Returns
    ///
    /// A boxed async stream of VCF records
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfReader::Local(reader) => reader.read_records(),
            VcfReader::Remote(reader) => reader.read_records().await,
        }
    }
}
