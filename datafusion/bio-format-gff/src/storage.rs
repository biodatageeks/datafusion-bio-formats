use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use noodles::bgzf;
use noodles_gff as gff;
use noodles_gff::feature::RecordBuf;
use noodles_gff::feature::record_buf::Attributes;

/// Unified GFF record that can hold different fast parser types
pub enum UnifiedGffRecord {
    /// Record parsed with the optimized fast parser
    Fast(DynGffRecord),
    /// Record parsed with the SIMD-optimized parser
    Simd(DynGffRecord),
}

/// Trait for unified GFF record access
pub trait GffRecordTrait {
    /// Returns the reference sequence name (seqid) field
    fn reference_sequence_name(&self) -> String;
    /// Returns the start position (1-based, inclusive)
    fn start(&self) -> u32;
    /// Returns the end position (1-based, inclusive)
    fn end(&self) -> u32;
    /// Returns the feature type
    fn ty(&self) -> String;
    /// Returns the source of this annotation
    fn source(&self) -> String;
    /// Returns the score, or None if score is not available
    fn score(&self) -> Option<f32>;
    /// Returns the strand ('+', '-', or '.')
    fn strand(&self) -> String;
    /// Returns the phase (0, 1, 2) for coding features, or None if not applicable
    fn phase(&self) -> Option<u8>;
    /// Returns the raw attributes string
    fn attributes_string(&self) -> String;
}

/// Iterator that can return different record types
pub enum UnifiedGffIterator {
    /// Iterator using fast parser
    Fast(Box<dyn Iterator<Item = std::io::Result<DynGffRecord>> + Send>),
    /// Iterator using SIMD parser
    Simd(Box<dyn Iterator<Item = std::io::Result<DynGffRecord>> + Send>),
}

/// Dynamic GFF record that holds the actual fast record types
pub struct DynGffRecord {
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
    /// Raw attribute string
    pub attributes: String,
}

impl Iterator for UnifiedGffIterator {
    type Item = Result<UnifiedGffRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            UnifiedGffIterator::Fast(iter) => {
                iter.next().map(|result| result.map(UnifiedGffRecord::Fast))
            }
            UnifiedGffIterator::Simd(iter) => {
                iter.next().map(|result| result.map(UnifiedGffRecord::Simd))
            }
        }
    }
}

impl GffRecordTrait for UnifiedGffRecord {
    fn reference_sequence_name(&self) -> String {
        match self {
            UnifiedGffRecord::Fast(record) => record.seqid.clone(),
            UnifiedGffRecord::Simd(record) => record.seqid.clone(),
        }
    }

    fn start(&self) -> u32 {
        match self {
            UnifiedGffRecord::Fast(record) => record.start,
            UnifiedGffRecord::Simd(record) => record.start,
        }
    }

    fn end(&self) -> u32 {
        match self {
            UnifiedGffRecord::Fast(record) => record.end,
            UnifiedGffRecord::Simd(record) => record.end,
        }
    }

    fn ty(&self) -> String {
        match self {
            UnifiedGffRecord::Fast(record) => record.ty.clone(),
            UnifiedGffRecord::Simd(record) => record.ty.clone(),
        }
    }

    fn source(&self) -> String {
        match self {
            UnifiedGffRecord::Fast(record) => record.source.clone(),
            UnifiedGffRecord::Simd(record) => record.source.clone(),
        }
    }

    fn score(&self) -> Option<f32> {
        match self {
            UnifiedGffRecord::Fast(record) => record.score,
            UnifiedGffRecord::Simd(record) => record.score,
        }
    }

    fn strand(&self) -> String {
        match self {
            UnifiedGffRecord::Fast(record) => record.strand.clone(),
            UnifiedGffRecord::Simd(record) => record.strand.clone(),
        }
    }

    fn phase(&self) -> Option<u8> {
        match self {
            UnifiedGffRecord::Fast(record) => {
                // Convert phase string to u8
                record.phase.parse().ok()
            }
            UnifiedGffRecord::Simd(record) => {
                // Convert phase string to u8
                record.phase.parse().ok()
            }
        }
    }

    fn attributes_string(&self) -> String {
        match self {
            UnifiedGffRecord::Fast(record) => record.attributes.clone(),
            UnifiedGffRecord::Simd(record) => record.attributes.clone(),
        }
    }
}
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
use tokio_util::io::StreamReader;

/// Parser type selection for GFF processing
///
/// This enum allows you to choose between different parsing strategies:
///
/// # Performance Comparison
/// - `Standard`: Original noodles-gff parser (~2305 ns/record)
/// - `Fast`: Optimized parser with ~3.2x speedup (~720 ns/record)  
/// - `Simd`: SIMD-optimized parser with ~3.3x speedup (~703 ns/record)
///
/// # Attribute Parsing
/// All parser types correctly parse GFF attributes including:
/// - Simple attributes: `ID=gene1` → `String("gene1")`
/// - Array attributes: `Alias=alt1,alt2,alt3` → `Array(["alt1", "alt2", "alt3"])`
///
/// # Usage Examples
/// ```rust,no_run
/// # use datafusion_bio_format_gff::storage::{GffRemoteReader, GffParserType};
/// # use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
/// # use futures_util::StreamExt;
/// # async fn example() -> Result<(), std::io::Error> {
/// let path = "s3://bucket/file.gff".to_string();
/// let options = ObjectStorageOptions::default();
///
/// // Use default fast parser
/// let reader = GffRemoteReader::new(path.clone(), options.clone()).await?;
///
/// // Use specific parser type
/// let reader = GffRemoteReader::new_with_parser(path, options, GffParserType::Simd).await?;
///
/// // Read records with proper attribute parsing
/// let mut stream = reader.read_records();
/// while let Some(record) = stream.next().await {
///     let record = record?;
///     // All attributes are properly parsed into RecordBuf format
///     println!("Attributes: {:?}", record.attributes());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GffParserType {
    /// Standard noodles-gff parser (compatible but slower)
    Standard,
    /// Fast parser with ~3x performance improvement
    Fast,
    /// SIMD parser with ~3.3x performance improvement
    Simd,
}

impl Default for GffParserType {
    fn default() -> Self {
        Self::Fast // Default to fast parser for best balance of performance and compatibility
    }
}

impl GffParserType {
    /// Get the fastest available parser (SIMD)
    pub fn fastest() -> Self {
        Self::Simd
    }

    /// Get the most compatible parser (Standard)
    pub fn compatible() -> Self {
        Self::Standard
    }

    /// Get the recommended parser for production use (Fast)
    pub fn recommended() -> Self {
        Self::Fast
    }
}

/// Creates an async GFF reader for GZIP-compressed remote files
///
/// # Arguments
/// * `file_path` - Path to the remote GFF file (e.g., S3, GCS, Azure)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
/// An async GFF reader configured for GZIP-compressed streams
pub async fn get_remote_gff_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    gff::r#async::io::Reader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = gff::r#async::io::Reader::new(stream);
    Ok(reader)
}

/// Creates an async GFF reader for BGZF-compressed remote files
///
/// # Arguments
/// * `file_path` - Path to the remote GFF file (e.g., S3, GCS, Azure)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
/// An async GFF reader configured for BGZF-compressed streams
pub async fn get_remote_gff_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = gff::r#async::io::Reader::new(inner);
    Ok(reader)
}

/// Creates an async GFF reader for uncompressed remote files
///
/// # Arguments
/// * `file_path` - Path to the remote GFF file (e.g., S3, GCS, Azure)
/// * `object_storage_options` - Configuration for cloud storage access
///
/// # Returns
/// An async GFF reader configured for uncompressed streams
pub async fn get_remote_gff_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = gff::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Async GFF reader for remote files with multiple compression and parser type variants
///
/// Combines compression type (GZIP, BGZF, PLAIN) with parser selection (Standard, Fast, SIMD)
/// to provide optimal performance for different use cases.
pub enum GffRemoteReader {
    /// Standard parser with GZIP compression
    GZIP(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    /// Standard parser with BGZF compression
    BGZF(gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>),
    /// Standard parser with no compression
    PLAIN(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),

    /// Fast parser with GZIP compression
    GzipFast(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    /// Fast parser with BGZF compression
    BgzfFast(
        gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    /// Fast parser with no compression
    PlainFast(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),

    /// SIMD parser with GZIP compression
    GzipSimd(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    /// SIMD parser with BGZF compression
    BgzfSimd(
        gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    /// SIMD parser with no compression
    PlainSimd(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl GffRemoteReader {
    /// Creates a new remote GFF reader with default parser selection
    ///
    /// # Arguments
    /// * `file_path` - Path to the remote GFF file
    /// * `object_storage_options` - Cloud storage configuration
    ///
    /// # Returns
    /// A configured GffRemoteReader using the default parser type (Fast)
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_parser(file_path, object_storage_options, GffParserType::default()).await
    }

    /// Creates a new remote GFF reader with explicit parser type selection
    ///
    /// # Arguments
    /// * `file_path` - Path to the remote GFF file
    /// * `object_storage_options` - Cloud storage configuration
    /// * `parser_type` - Parser type to use (Standard, Fast, or SIMD)
    ///
    /// # Returns
    /// A configured GffRemoteReader with the specified parser type
    pub async fn new_with_parser(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
        parser_type: GffParserType,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        match (compression_type.clone(), parser_type) {
            // GZIP variants
            (CompressionType::GZIP, GffParserType::Standard) => {
                let reader = get_remote_gff_gz_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::GZIP(reader))
            }
            (CompressionType::GZIP, GffParserType::Fast) => {
                let reader = get_remote_gff_gz_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::GzipFast(reader))
            }
            (CompressionType::GZIP, GffParserType::Simd) => {
                let reader = get_remote_gff_gz_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::GzipSimd(reader))
            }

            // BGZF variants
            (CompressionType::BGZF, GffParserType::Standard) => {
                let reader = get_remote_gff_bgzf_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::BGZF(reader))
            }
            (CompressionType::BGZF, GffParserType::Fast) => {
                let reader = get_remote_gff_bgzf_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::BgzfFast(reader))
            }
            (CompressionType::BGZF, GffParserType::Simd) => {
                let reader = get_remote_gff_bgzf_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::BgzfSimd(reader))
            }

            // Plain variants
            (CompressionType::NONE, GffParserType::Standard) => {
                let reader = get_remote_gff_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::PLAIN(reader))
            }
            (CompressionType::NONE, GffParserType::Fast) => {
                let reader = get_remote_gff_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::PlainFast(reader))
            }
            (CompressionType::NONE, GffParserType::Simd) => {
                let reader = get_remote_gff_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::PlainSimd(reader))
            }

            _ => unimplemented!(
                "Compression type {:?} is not supported for GFF files",
                compression_type
            ),
        }
    }
    /// Returns a stream of parsed GFF records
    ///
    /// The stream uses the appropriate parser method based on the reader type:
    /// - Standard and Fast variants use `fast_record_bufs()`
    /// - SIMD variants use `simd_record_bufs()`
    ///
    /// # Returns
    /// A boxed stream of GFF record buffers
    pub fn read_records(self) -> BoxStream<'static, Result<RecordBuf, Error>> {
        match self {
            // Standard parsers - use fast methods to avoid borrowing issues
            GffRemoteReader::BGZF(reader) => reader.fast_record_bufs().boxed(),
            GffRemoteReader::GZIP(reader) => reader.fast_record_bufs().boxed(),
            GffRemoteReader::PLAIN(reader) => reader.fast_record_bufs().boxed(),

            // Fast parsers with proper attribute parsing
            GffRemoteReader::BgzfFast(reader) => reader.fast_record_bufs().boxed(),
            GffRemoteReader::GzipFast(reader) => reader.fast_record_bufs().boxed(),
            GffRemoteReader::PlainFast(reader) => reader.fast_record_bufs().boxed(),

            // SIMD parsers with proper attribute parsing
            GffRemoteReader::BgzfSimd(reader) => reader.simd_record_bufs().boxed(),
            GffRemoteReader::GzipSimd(reader) => reader.simd_record_bufs().boxed(),
            GffRemoteReader::PlainSimd(reader) => reader.simd_record_bufs().boxed(),
        }
    }

    /// Reads and returns the attributes from the first GFF record
    ///
    /// # Returns
    /// The attributes structure from the first record in the file
    pub async fn get_attributes(self) -> Attributes {
        match self {
            // Standard parsers - use fast methods to avoid borrowing issues
            GffRemoteReader::BGZF(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffRemoteReader::GZIP(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffRemoteReader::PLAIN(reader) => reader.fast_record_bufs().next().await.unwrap(),

            // Fast parsers with proper attribute parsing
            GffRemoteReader::BgzfFast(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffRemoteReader::GzipFast(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffRemoteReader::PlainFast(reader) => reader.fast_record_bufs().next().await.unwrap(),

            // SIMD parsers with proper attribute parsing
            GffRemoteReader::BgzfSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
            GffRemoteReader::GzipSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
            GffRemoteReader::PlainSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
        }
        .unwrap()
        .attributes()
        .clone()
    }
}

/// Creates a synchronous GFF reader for GZIP-compressed local files
///
/// # Arguments
/// * `file_path` - Path to the local GFF file
///
/// # Returns
/// A synchronous GFF reader configured for GZIP-compressed files
pub fn get_local_gff_gz_sync_reader(
    file_path: String,
) -> Result<gff::io::Reader<std::io::BufReader<flate2::read::GzDecoder<std::fs::File>>>, Error> {
    let file = std::fs::File::open(file_path)?;
    let decoder = flate2::read::GzDecoder::new(file);
    let reader = gff::io::Reader::new(std::io::BufReader::new(decoder));
    Ok(reader)
}

/// Creates a synchronous GFF reader for BGZF-compressed local files with multithreading
///
/// # Arguments
/// * `file_path` - Path to the local GFF file
/// * `thread_num` - Number of threads to use for decompression
///
/// # Returns
/// A synchronous GFF reader configured for BGZF-compressed files with parallel decompression
pub fn get_local_gff_bgzf_sync_reader(
    file_path: String,
    thread_num: usize,
) -> Result<gff::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let file = std::fs::File::open(file_path)?;
    let reader = bgzf::MultithreadedReader::with_worker_count(
        std::num::NonZero::new(thread_num).unwrap(),
        file,
    );
    Ok(gff::io::Reader::new(reader))
}

/// Creates a synchronous GFF reader for uncompressed local files
///
/// # Arguments
/// * `file_path` - Path to the local GFF file
///
/// # Returns
/// A synchronous GFF reader configured for uncompressed files
pub fn get_local_gff_plain_sync_reader(
    file_path: String,
) -> Result<gff::io::Reader<std::io::BufReader<std::fs::File>>, Error> {
    let file = std::fs::File::open(file_path)?;
    let reader = gff::io::Reader::new(std::io::BufReader::new(file));
    Ok(reader)
}

/// Creates an async GFF reader for GZIP-compressed local files
///
/// # Arguments
/// * `file_path` - Path to the local GZIP-compressed GFF file
///
/// # Returns
/// An async GFF reader configured for GZIP-compressed local files
pub async fn get_local_gff_gz_reader(
    file_path: String,
) -> Result<
    gff::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(gff::r#async::io::Reader::new)
}

/// Creates an async GFF reader for BGZF-compressed local files
///
/// # Arguments
/// * `file_path` - Path to the local BGZF-compressed GFF file
///
/// # Returns
/// An async GFF reader configured for BGZF-compressed local files with async I/O
pub async fn get_local_gff_bgzf_async_reader(
    file_path: String,
) -> Result<
    gff::r#async::io::Reader<
        tokio::io::BufReader<bgzf::r#async::Reader<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    let file = tokio::fs::File::open(file_path).await?;
    let buf_reader = tokio::io::BufReader::new(file);
    let bgzf_reader = bgzf::r#async::Reader::new(buf_reader);
    let reader = gff::r#async::io::Reader::new(tokio::io::BufReader::new(bgzf_reader));
    Ok(reader)
}

/// Creates an async GFF reader for uncompressed local files
///
/// # Arguments
/// * `file_path` - Path to the local uncompressed GFF file
///
/// # Returns
/// An async GFF reader configured for uncompressed local files with async I/O
pub async fn get_local_gff_async_reader(
    file_path: String,
) -> Result<gff::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>, Error> {
    let file = tokio::fs::File::open(file_path).await?;
    let reader = gff::r#async::io::Reader::new(tokio::io::BufReader::new(file));
    Ok(reader)
}

/// Synchronous GFF reader for local files with multiple compression and parser type variants
///
/// Combines compression type (GZIP, BGZF, PLAIN) with parser selection (Standard, Fast, SIMD)
/// to provide optimal performance for different use cases with blocking I/O.
pub enum GffLocalReader {
    /// Standard parser with GZIP compression
    GZIP(gff::io::Reader<BufReader<flate2::read::GzDecoder<File>>>),
    /// Standard parser with BGZF compression
    BGZF(gff::io::Reader<bgzf::MultithreadedReader<File>>),
    /// Standard parser with no compression
    PLAIN(gff::io::Reader<BufReader<File>>),

    /// Fast parser with GZIP compression
    GzipFast(gff::io::Reader<BufReader<flate2::read::GzDecoder<File>>>),
    /// Fast parser with BGZF compression
    BgzfFast(gff::io::Reader<bgzf::MultithreadedReader<File>>),
    /// Fast parser with no compression
    PlainFast(gff::io::Reader<BufReader<File>>),

    /// SIMD parser with GZIP compression
    GzipSimd(gff::io::Reader<BufReader<flate2::read::GzDecoder<File>>>),
    /// SIMD parser with BGZF compression
    BgzfSimd(gff::io::Reader<bgzf::MultithreadedReader<File>>),
    /// SIMD parser with no compression
    PlainSimd(gff::io::Reader<BufReader<File>>),
}

impl GffLocalReader {
    /// Creates a new local GFF reader with default parser selection
    ///
    /// # Arguments
    /// * `file_path` - Path to the local GFF file
    /// * `thread_num` - Number of threads for BGZF decompression
    /// * `object_storage_options` - Storage configuration (unused for local files)
    ///
    /// # Returns
    /// A configured GffLocalReader using the default parser type (Fast)
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_parser(
            file_path,
            thread_num,
            object_storage_options,
            GffParserType::default(),
        )
        .await
    }

    /// Creates a new local GFF reader with explicit parser type selection
    ///
    /// # Arguments
    /// * `file_path` - Path to the local GFF file
    /// * `thread_num` - Number of threads for BGZF decompression
    /// * `object_storage_options` - Storage configuration (unused for local files)
    /// * `parser_type` - Parser type to use (Standard, Fast, or SIMD)
    ///
    /// # Returns
    /// A configured GffLocalReader with the specified parser type
    pub async fn new_with_parser(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
        parser_type: GffParserType,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        match (compression_type.clone(), parser_type) {
            // GZIP variants - using sync readers
            (CompressionType::GZIP, GffParserType::Standard) => {
                let reader = get_local_gff_gz_sync_reader(file_path)?;
                Ok(GffLocalReader::GZIP(reader))
            }
            (CompressionType::GZIP, GffParserType::Fast) => {
                let reader = get_local_gff_gz_sync_reader(file_path)?;
                Ok(GffLocalReader::GzipFast(reader))
            }
            (CompressionType::GZIP, GffParserType::Simd) => {
                let reader = get_local_gff_gz_sync_reader(file_path)?;
                Ok(GffLocalReader::GzipSimd(reader))
            }

            // BGZF variants - using sync readers with multithreading
            (CompressionType::BGZF, GffParserType::Standard) => {
                let reader = get_local_gff_bgzf_sync_reader(file_path, thread_num)?;
                Ok(GffLocalReader::BGZF(reader))
            }
            (CompressionType::BGZF, GffParserType::Fast) => {
                let reader = get_local_gff_bgzf_sync_reader(file_path, thread_num)?;
                Ok(GffLocalReader::BgzfFast(reader))
            }
            (CompressionType::BGZF, GffParserType::Simd) => {
                let reader = get_local_gff_bgzf_sync_reader(file_path, thread_num)?;
                Ok(GffLocalReader::BgzfSimd(reader))
            }

            // Plain variants - using sync readers
            (CompressionType::NONE, GffParserType::Standard) => {
                let reader = get_local_gff_plain_sync_reader(file_path)?;
                Ok(GffLocalReader::PLAIN(reader))
            }
            (CompressionType::NONE, GffParserType::Fast) => {
                let reader = get_local_gff_plain_sync_reader(file_path)?;
                Ok(GffLocalReader::PlainFast(reader))
            }
            (CompressionType::NONE, GffParserType::Simd) => {
                let reader = get_local_gff_plain_sync_reader(file_path)?;
                Ok(GffLocalReader::PlainSimd(reader))
            }

            _ => unimplemented!(
                "Compression type {:?} is not supported for GFF files",
                compression_type
            ),
        }
    }

    /// Converts this reader into a unified iterator over GFF records
    ///
    /// # Returns
    /// A UnifiedGffIterator that yields parsed GFF records with the selected parser type
    pub fn into_sync_iterator(self) -> UnifiedGffIterator {
        match self {
            // Standard parsers - but we still use fast_records for API consistency
            GffLocalReader::BGZF(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::GZIP(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::PLAIN(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            // Fast parsers use fast_records() and convert to DynGffRecord
            GffLocalReader::BgzfFast(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::GzipFast(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::PlainFast(reader) => {
                UnifiedGffIterator::Fast(Box::new(reader.fast_records().map(|r| {
                    r.map(|fast_record| DynGffRecord {
                        seqid: fast_record.seqid.clone(),
                        source: fast_record.source.clone(),
                        ty: fast_record.ty.clone(),
                        start: fast_record.start,
                        end: fast_record.end,
                        score: fast_record.score,
                        strand: fast_record.strand.clone(),
                        phase: fast_record.phase.clone(),
                        attributes: fast_record.attributes.clone(),
                    })
                })))
            }
            // SIMD parsers use simd_records() and convert to DynGffRecord
            GffLocalReader::BgzfSimd(reader) => {
                UnifiedGffIterator::Simd(Box::new(reader.simd_records().map(|r| {
                    r.map(|simd_record| DynGffRecord {
                        seqid: simd_record.seqid.clone(),
                        source: simd_record.source.clone(),
                        ty: simd_record.ty.clone(),
                        start: simd_record.start,
                        end: simd_record.end,
                        score: simd_record.score,
                        strand: simd_record.strand.clone(),
                        phase: simd_record.phase.clone(),
                        attributes: simd_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::GzipSimd(reader) => {
                UnifiedGffIterator::Simd(Box::new(reader.simd_records().map(|r| {
                    r.map(|simd_record| DynGffRecord {
                        seqid: simd_record.seqid.clone(),
                        source: simd_record.source.clone(),
                        ty: simd_record.ty.clone(),
                        start: simd_record.start,
                        end: simd_record.end,
                        score: simd_record.score,
                        strand: simd_record.strand.clone(),
                        phase: simd_record.phase.clone(),
                        attributes: simd_record.attributes.clone(),
                    })
                })))
            }
            GffLocalReader::PlainSimd(reader) => {
                UnifiedGffIterator::Simd(Box::new(reader.simd_records().map(|r| {
                    r.map(|simd_record| DynGffRecord {
                        seqid: simd_record.seqid.clone(),
                        source: simd_record.source.clone(),
                        ty: simd_record.ty.clone(),
                        start: simd_record.start,
                        end: simd_record.end,
                        score: simd_record.score,
                        strand: simd_record.strand.clone(),
                        phase: simd_record.phase.clone(),
                        attributes: simd_record.attributes.clone(),
                    })
                })))
            }
        }
    }

    /// Reads and returns the attributes from the first GFF record
    ///
    /// # Returns
    /// The attributes structure from the first record in the file, or an error if reading fails
    pub fn get_attributes(self) -> Result<Attributes, Error> {
        match self {
            // Standard parsers use record_bufs()
            GffLocalReader::BGZF(mut reader) => reader
                .record_bufs()
                .next()
                .unwrap()
                .map(|r| r.attributes().clone())
                ,
            GffLocalReader::GZIP(mut reader) => reader
                .record_bufs()
                .next()
                .unwrap()
                .map(|r| r.attributes().clone())
                ,
            GffLocalReader::PLAIN(mut reader) => reader
                .record_bufs()
                .next()
                .unwrap()
                .map(|r| r.attributes().clone())
                ,
            // Fast parsers - we'll need to adapt this
            GffLocalReader::BgzfFast(_) => {
                unimplemented!("get_attributes not yet implemented for fast parsers")
            }
            GffLocalReader::GzipFast(_) => {
                unimplemented!("get_attributes not yet implemented for fast parsers")
            }
            GffLocalReader::PlainFast(_) => {
                unimplemented!("get_attributes not yet implemented for fast parsers")
            }
            // SIMD parsers - we'll need to adapt this
            GffLocalReader::BgzfSimd(_) => {
                unimplemented!("get_attributes not yet implemented for SIMD parsers")
            }
            GffLocalReader::GzipSimd(_) => {
                unimplemented!("get_attributes not yet implemented for SIMD parsers")
            }
            GffLocalReader::PlainSimd(_) => {
                unimplemented!("get_attributes not yet implemented for SIMD parsers")
            }
        }
    }
}
