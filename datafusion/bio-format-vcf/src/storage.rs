use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion::arrow;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, get_storage_type,
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use log::debug;
use log::info;
use noodles_bgzf::{AsyncReader, MultithreadedReader};
use noodles_vcf as vcf;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use std::sync::Arc;
use tokio::io::BufReader;
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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
) -> Result<vcf::r#async::io::Reader<BufReader<tokio::fs::File>>, Error> {
    debug!("Reading VCF file from local storage with async reader");
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(BufReader::new)
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
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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

/// Unified reader for remote VCF files with multiple compression format support.
///
/// This enum handles BGZF, GZIP, and uncompressed remote VCF files from cloud storage.
/// The appropriate variant is created based on the detected compression type.
pub enum VcfRemoteReader {
    /// Reader for BGZF-compressed remote VCF files.
    BGZF(vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    /// Reader for GZIP-compressed remote VCF files.
    GZIP(vcf::r#async::io::Reader<BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>>),
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
                let reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
                VcfRemoteReader::BGZF(reader)
            }
            CompressionType::GZIP => {
                let reader = get_remote_vcf_gz_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                VcfRemoteReader::GZIP(reader)
            }
            CompressionType::NONE => {
                let reader = get_remote_vcf_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
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
    /// Reader for GZIP-compressed local VCF files.
    GZIP(vcf::r#async::io::Reader<BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>>),
    /// Reader for uncompressed local VCF files.
    PLAIN(vcf::r#async::io::Reader<BufReader<tokio::fs::File>>),
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
            VcfLocalReader::GZIP(reader) => reader.read_header().await,
            VcfLocalReader::PLAIN(reader) => reader.read_header().await,
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
            VcfLocalReader::GZIP(reader) => reader.records().boxed(),
            VcfLocalReader::PLAIN(reader) => reader.records().boxed(),
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
            VcfLocalReader::GZIP(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
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
