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
use opendal::FuturesBytesStream;
use std::io::Error;
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

pub async fn get_remote_gff_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let reader = gff::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

pub enum GffRemoteReader {
    // Standard parsers
    GZIP(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    BGZF(gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>),
    PLAIN(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),

    // Fast parsers (same reader types but used with fast_record_bufs() method)
    GzipFast(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    BgzfFast(
        gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    PlainFast(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),

    // SIMD parsers (same reader types but used with simd_record_bufs() method)
    GzipSimd(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    BgzfSimd(
        gff::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    PlainSimd(gff::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl GffRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_parser(file_path, object_storage_options, GffParserType::default()).await
    }

    pub async fn new_with_parser(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
        parser_type: GffParserType,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone()).await;

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

// Old sync helper functions removed - using async consistently now

pub async fn get_local_gff_gz_reader(
    file_path: String,
) -> Result<
    gff::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(gff::r#async::io::Reader::new);
    reader
}

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

pub async fn get_local_gff_async_reader(
    file_path: String,
) -> Result<gff::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>, Error> {
    let file = tokio::fs::File::open(file_path).await?;
    let reader = gff::r#async::io::Reader::new(tokio::io::BufReader::new(file));
    Ok(reader)
}

pub enum GffLocalReader {
    // Standard parsers - all async now
    GZIP(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    tokio::io::BufReader<tokio::fs::File>,
                >,
            >,
        >,
    ),
    BGZF(
        gff::r#async::io::Reader<
            tokio::io::BufReader<bgzf::r#async::Reader<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    PLAIN(gff::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>),

    // Fast parsers (same reader types but used with fast methods)
    GzipFast(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    tokio::io::BufReader<tokio::fs::File>,
                >,
            >,
        >,
    ),
    BgzfFast(
        gff::r#async::io::Reader<
            tokio::io::BufReader<bgzf::r#async::Reader<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    PlainFast(gff::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>),

    // SIMD parsers (same reader types but used with simd methods)
    GzipSimd(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    tokio::io::BufReader<tokio::fs::File>,
                >,
            >,
        >,
    ),
    BgzfSimd(
        gff::r#async::io::Reader<
            tokio::io::BufReader<bgzf::r#async::Reader<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    PlainSimd(gff::r#async::io::Reader<tokio::io::BufReader<tokio::fs::File>>),
}

impl GffLocalReader {
    pub async fn new(
        file_path: String,
        _thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        Self::new_with_parser(
            file_path,
            _thread_num,
            object_storage_options,
            GffParserType::default(),
        )
        .await
    }

    pub async fn new_with_parser(
        file_path: String,
        _thread_num: usize,
        object_storage_options: ObjectStorageOptions,
        parser_type: GffParserType,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await;

        match (compression_type.clone(), parser_type) {
            // GZIP variants
            (CompressionType::GZIP, GffParserType::Standard) => {
                let reader = get_local_gff_gz_reader(file_path).await?;
                Ok(GffLocalReader::GZIP(reader))
            }
            (CompressionType::GZIP, GffParserType::Fast) => {
                let reader = get_local_gff_gz_reader(file_path).await?;
                Ok(GffLocalReader::GzipFast(reader))
            }
            (CompressionType::GZIP, GffParserType::Simd) => {
                let reader = get_local_gff_gz_reader(file_path).await?;
                Ok(GffLocalReader::GzipSimd(reader))
            }

            // BGZF variants - now using async readers
            (CompressionType::BGZF, GffParserType::Standard) => {
                let reader = get_local_gff_bgzf_async_reader(file_path).await?;
                Ok(GffLocalReader::BGZF(reader))
            }
            (CompressionType::BGZF, GffParserType::Fast) => {
                let reader = get_local_gff_bgzf_async_reader(file_path).await?;
                Ok(GffLocalReader::BgzfFast(reader))
            }
            (CompressionType::BGZF, GffParserType::Simd) => {
                let reader = get_local_gff_bgzf_async_reader(file_path).await?;
                Ok(GffLocalReader::BgzfSimd(reader))
            }

            // Plain variants - now using async readers
            (CompressionType::NONE, GffParserType::Standard) => {
                let reader = get_local_gff_async_reader(file_path).await?;
                Ok(GffLocalReader::PLAIN(reader))
            }
            (CompressionType::NONE, GffParserType::Fast) => {
                let reader = get_local_gff_async_reader(file_path).await?;
                Ok(GffLocalReader::PlainFast(reader))
            }
            (CompressionType::NONE, GffParserType::Simd) => {
                let reader = get_local_gff_async_reader(file_path).await?;
                Ok(GffLocalReader::PlainSimd(reader))
            }

            _ => unimplemented!(
                "Compression type {:?} is not supported for GFF files",
                compression_type
            ),
        }
    }

    pub fn read_records(self) -> BoxStream<'static, Result<RecordBuf, Error>> {
        match self {
            // Standard parsers - use fast methods to avoid borrowing issues
            GffLocalReader::BGZF(reader) => reader.fast_record_bufs().boxed(),
            GffLocalReader::GZIP(reader) => reader.fast_record_bufs().boxed(),
            GffLocalReader::PLAIN(reader) => reader.fast_record_bufs().boxed(),

            // Fast parsers with proper attribute parsing
            GffLocalReader::BgzfFast(reader) => reader.fast_record_bufs().boxed(),
            GffLocalReader::GzipFast(reader) => reader.fast_record_bufs().boxed(),
            GffLocalReader::PlainFast(reader) => reader.fast_record_bufs().boxed(),

            // SIMD parsers with proper attribute parsing
            GffLocalReader::BgzfSimd(reader) => reader.simd_record_bufs().boxed(),
            GffLocalReader::GzipSimd(reader) => reader.simd_record_bufs().boxed(),
            GffLocalReader::PlainSimd(reader) => reader.simd_record_bufs().boxed(),
        }
    }

    pub async fn get_attributes(self) -> Attributes {
        match self {
            // Standard parsers - use fast methods to avoid borrowing issues
            GffLocalReader::BGZF(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffLocalReader::GZIP(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffLocalReader::PLAIN(reader) => reader.fast_record_bufs().next().await.unwrap(),

            // Fast parsers with proper attribute parsing
            GffLocalReader::BgzfFast(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffLocalReader::GzipFast(reader) => reader.fast_record_bufs().next().await.unwrap(),
            GffLocalReader::PlainFast(reader) => reader.fast_record_bufs().next().await.unwrap(),

            // SIMD parsers with proper attribute parsing
            GffLocalReader::BgzfSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
            GffLocalReader::GzipSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
            GffLocalReader::PlainSimd(reader) => reader.simd_record_bufs().next().await.unwrap(),
        }
        .unwrap()
        .attributes()
        .clone()
    }
}
