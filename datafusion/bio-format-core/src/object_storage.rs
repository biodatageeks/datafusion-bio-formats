use crate::companion::sanitize_location;
use async_compression::tokio::bufread::GzipDecoder;
use futures::StreamExt;
use log;
use log::debug;
use noodles_bgzf as bgzf;
use noodles_bgzf::r#async::io::Reader as AsyncReader;
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Azblob, Gcs, Http, S3};
use opendal::{FuturesBytesStream, Operator};
use std::env;
use std::fmt::Display;
use std::ops::Range;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use url::Url;

/// Configuration options for object storage operations
#[derive(Clone, Debug)]
pub struct ObjectStorageOptions {
    /// Chunk size in MB for reading data
    pub chunk_size: Option<usize>,
    /// Number of concurrent fetch operations
    pub concurrent_fetches: Option<usize>,
    /// Allow anonymous access to cloud storage
    pub allow_anonymous: bool,
    /// Enable request payer for S3
    pub enable_request_payer: bool,
    /// Maximum number of retry attempts
    pub max_retries: Option<usize>,
    /// Timeout in seconds for operations
    pub timeout: Option<usize>,
    /// Type of compression to use
    pub compression_type: Option<CompressionType>,
}

impl Display for ObjectStorageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObjectStorageOptions {{ chunk_size: {:?}, concurrent_fetches: {:?}, allow_anonymous: {}, enable_request_payer: {}, max_retries: {:?}, timeout: {:?}, compression_type: {:?} }}",
            self.chunk_size,
            self.concurrent_fetches,
            self.allow_anonymous,
            self.enable_request_payer,
            self.max_retries,
            self.timeout,
            self.compression_type
        )
    }
}

/// Type of compression used for data files
#[derive(Clone, Debug, PartialEq)]
pub enum CompressionType {
    /// Standard GZIP compression
    GZIP,
    /// BGZF (Block GZIP Format) compression for parallel reading
    BGZF,
    /// No compression
    NONE,
    /// Automatically detect compression type
    AUTO,
}

impl CompressionType {
    /// Creates a CompressionType from a string representation
    ///
    /// # Arguments
    ///
    /// * `compression_type` - String representing the compression type ("gz", "bgz", "none", "auto")
    ///
    /// # Panics
    ///
    /// Panics if the compression type string is not recognized. Prefer
    /// [`Self::try_from_string`] for anything derived from user input.
    pub fn from_string(compression_type: String) -> Self {
        match Self::try_from_string(&compression_type) {
            Some(parsed) => parsed,
            None => panic!("Invalid compression type: {compression_type}"),
        }
    }

    /// Creates a CompressionType from a string, or `None` if unrecognized.
    pub fn try_from_string(compression_type: &str) -> Option<Self> {
        match compression_type.to_lowercase().as_str() {
            "gz" => Some(CompressionType::GZIP),
            "bgz" => Some(CompressionType::BGZF),
            "none" => Some(CompressionType::NONE),
            "auto" => Some(CompressionType::AUTO),
            _ => None,
        }
    }
}

impl Default for ObjectStorageOptions {
    fn default() -> Self {
        ObjectStorageOptions {
            chunk_size: Some(8),                           // Default chunk size in MB
            concurrent_fetches: Some(1),                   // Default concurrent fetches
            allow_anonymous: true,                         // Default to allowing anonymous access
            enable_request_payer: false,                   // Default to not enabling request payer
            max_retries: Some(5),                          // Default max retries
            timeout: Some(300),                            // Default timeout in seconds
            compression_type: Some(CompressionType::AUTO), // Default compression type
        }
    }
}
/// Type of storage backend for data files
#[derive(Debug)]
pub enum StorageType {
    /// Google Cloud Storage
    GCS,
    /// Amazon S3
    S3,
    /// Azure Blob Storage
    AZBLOB,
    /// HTTP/HTTPS endpoint
    HTTP,
    /// Local filesystem
    LOCAL,
}

impl StorageType {
    /// Creates a StorageType from a URL prefix
    ///
    /// # Arguments
    ///
    /// * `object_storage_type` - URL scheme prefix ("gs", "s3", "abfs", "local", "file", "http", "https")
    ///
    /// # Panics
    ///
    /// Panics if the storage type prefix is not recognized. Prefer
    /// [`Self::try_from_prefix`] for anything derived from user input.
    pub fn from_prefix(object_storage_type: String) -> Self {
        match Self::try_from_prefix(&object_storage_type) {
            Some(parsed) => parsed,
            None => panic!("Invalid object storage type: {object_storage_type}"),
        }
    }

    /// Creates a StorageType from a URL prefix, or `None` if unrecognized.
    pub fn try_from_prefix(object_storage_type: &str) -> Option<Self> {
        match object_storage_type.to_lowercase().as_str() {
            "gs" => Some(StorageType::GCS),
            "s3" => Some(StorageType::S3),
            "abfs" => Some(StorageType::AZBLOB),
            "local" | "file" => Some(StorageType::LOCAL),
            "http" | "https" => Some(StorageType::HTTP),
            _ => None,
        }
    }
}

fn get_file_path(file_path: String) -> String {
    //extract the file path from the file path
    let file_path = file_path
        .split("://")
        .last()
        .unwrap()
        .split('/')
        .skip(1)
        .collect::<Vec<&str>>()
        .join("/");
    //return the file path
    file_path.to_string()
}

/// Detects the compression type of a file by examining its header
///
/// # Arguments
///
/// * `file_path` - Path to the file (local or remote URL)
/// * `compression_type` - Optional compression type hint; if AUTO or None, detection will be performed
/// * `object_storage_options` - Configuration options for accessing remote files
///
/// # Returns
///
/// The detected compression type (GZIP, BGZF, or NONE)
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or read
pub async fn get_compression_type(
    file_path: String,
    compression_type: Option<CompressionType>,
    object_storage_options: ObjectStorageOptions,
) -> Result<CompressionType, opendal::Error> {
    debug!(
        "get_compression_type called with file_path: {file_path}, compression_type: {compression_type:?}"
    );
    if let Some(ct) = compression_type
        && ct != CompressionType::AUTO
    {
        return Ok(ct);
    }

    let storage_type = get_storage_type(file_path.clone());
    let buffer = if matches!(storage_type, StorageType::LOCAL) {
        let local_path = file_path.strip_prefix("file://").unwrap_or(&file_path);
        // For local files, read directly
        // A missing or unreadable path is a normal error for a caller that
        // passed one, not a reason to take the process down; the signature
        // already carries it.
        let mut file = tokio::fs::File::open(local_path).await.map_err(|error| {
            opendal::Error::new(
                opendal::ErrorKind::NotFound,
                format!(
                    "cannot open {} to detect its compression",
                    sanitize_location(local_path)
                ),
            )
            .set_source(error)
        })?;
        let mut buffer = vec![0; 18];
        let n = file.read(&mut buffer).await.map_err(|error| {
            opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                format!(
                    "cannot read {} to detect its compression",
                    sanitize_location(local_path)
                ),
            )
            .set_source(error)
        })?;
        buffer.truncate(n);
        buffer
    } else {
        // For remote files, read only the minimum bytes needed for compression detection (18 bytes)
        match get_remote_stream(file_path.clone(), object_storage_options.clone(), Some(18)).await {
            Ok(mut stream) => {
                let mut buffer = Vec::with_capacity(18);
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            buffer.extend_from_slice(&chunk);
                            if buffer.len() >= 18 {
                                break;
                            }
                        }
                        // A failed read is not an absence of compression. Using
                        // the bytes that did arrive would guess from a truncated
                        // magic number and hand the caller a plain reader for a
                        // BGZF file, which fails later and further away.
                        Err(error) => {
                            return Err(opendal::Error::new(
                                opendal::ErrorKind::Unexpected,
                                format!(
                                    "cannot read {} to detect its compression",
                                    sanitize_location(&file_path)
                                ),
                            )
                            .set_source(error));
                        }
                    }
                }
                buffer
            }
            // Reporting an unreadable object as uncompressed sends the caller on
            // to open it as plain text, so the real failure — a bad credential,
            // a missing object — surfaces later as a parse error against the
            // wrong reader. The local path already propagates this.
            Err(error) => {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    format!(
                        "cannot open {} to detect its compression",
                        sanitize_location(&file_path)
                    ),
                )
                .set_source(error));
            }
        }
    };

    if buffer.len() < 4 {
        return Ok(CompressionType::NONE);
    }

    // GZIP magic number: 0x1f 0x8b
    if buffer.len() >= 2 && buffer[0] == 0x1f && buffer[1] == 0x8b {
        // FLG byte is at index 3
        if buffer.len() >= 10 && (buffer[3] & 0x04) != 0 {
            if buffer.len() < 12 {
                return Ok(CompressionType::GZIP); // Not enough data for BGZF check
            }
            // XLEN is at index 10, little-endian
            let xlen = u16::from_le_bytes([buffer[10], buffer[11]]);
            if buffer.len() >= 12 + xlen as usize {
                // BGZF subfield identifier is 'B' 'C'
                let mut i = 12;
                while i < 12 + xlen as usize {
                    let si1 = buffer[i];
                    let si2 = buffer[i + 1];
                    let slen = u16::from_le_bytes([buffer[i + 2], buffer[i + 3]]);
                    if si1 == b'B' && si2 == b'C' && slen == 2 {
                        return Ok(CompressionType::BGZF);
                    }
                    i += (slen + 4) as usize;
                }
            }
        }
        return Ok(CompressionType::GZIP);
    }

    Ok(CompressionType::NONE)
}

/// Creates a BGZF-decompressing async reader for a remote file
///
/// # Arguments
///
/// * `file_path` - Path to the BGZF-compressed file (local or remote URL)
/// * `object_storage_options` - Configuration options for accessing the file
///
/// # Returns
///
/// An async reader that decompresses BGZF data on the fly
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if stream creation fails
pub async fn get_remote_stream_bgzf_async(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream = StreamReader::new(
        get_remote_stream(file_path.clone(), object_storage_options, None).await?,
    );
    Ok(bgzf::r#async::io::Reader::new(remote_stream))
}

/// Opens a BGZF reader over one sequential request, without a size preflight.
///
/// [`get_remote_stream_bgzf_async`] streams the whole object through the
/// configured reader chunking, which asks the backend for the object length and
/// so issues a HEAD. A pre-signed URL that authorizes GET and range requests but
/// not HEAD therefore fails there, even though every later read the caller makes
/// would have succeeded. Reading a bounded prefix — a header — needs no length,
/// so it goes through a single request instead. The caller is responsible for
/// bounding how much it consumes.
pub async fn get_remote_stream_bgzf_single_request(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let object = RemoteObject::open(file_path, object_storage_options).await?;
    let remote_stream = StreamReader::new(object.stream_single_request().await?);
    Ok(bgzf::r#async::io::Reader::new(remote_stream))
}

/// Opens a BGZF reader over the whole object, retrying without the size
/// preflight when the backend refuses it.
///
/// Unlike [`get_remote_stream_bgzf_single_request`], this keeps the chunked,
/// concurrent reader on the common path — a full scan of a large object depends
/// on it — and gives up that concurrency only for the URLs that cannot support
/// it. See [`RemoteObject::stream_with_size_preflight_fallback`].
pub async fn get_remote_stream_bgzf_head_tolerant(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let object = RemoteObject::open(file_path, object_storage_options).await?;
    let remote_stream = StreamReader::new(object.stream_with_size_preflight_fallback().await?);
    Ok(bgzf::r#async::io::Reader::new(remote_stream))
}

/// Whether an error means the backend refused to report the object's size,
/// rather than that the object cannot be read.
///
/// A pre-signed URL scoped to GET and range requests answers a HEAD with 403,
/// and a backend with no size capability reports the request as unsupported.
/// Both leave every actual read authorized, so the caller can retry without the
/// preflight. Every other kind — a missing object, a bad config, a rate limit —
/// would fail the retry too, and retrying would only hide the first error
/// behind the second.
fn is_refused_size_preflight(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        opendal::ErrorKind::PermissionDenied | opendal::ErrorKind::Unsupported
    )
}

/// Builds a gzip decoder that decodes **all** members of a multi-member
/// (concatenated / block) gzip stream, not just the first one.
///
/// `async_compression`'s `GzipDecoder` stops after the first gzip member by
/// default; for block-gzip files (e.g. produced by pigz, bgzip-as-gzip, fastp)
/// this silently drops every member after the first, and crashes with
/// `UnexpectedEof` when the first member's bytes end in the middle of a record.
/// Enabling `multiple_members(true)` makes it consume every member.
pub fn gzip_multi_member_decoder<R: tokio::io::AsyncBufRead>(inner: R) -> GzipDecoder<R> {
    let mut decoder = GzipDecoder::new(inner);
    decoder.multiple_members(true);
    decoder
}

/// Creates a GZIP-decompressing async reader for a remote file
///
/// # Arguments
///
/// * `file_path` - Path to the GZIP-compressed file (local or remote URL)
/// * `object_storage_options` - Configuration options for accessing the file
///
/// # Returns
///
/// An async reader that decompresses GZIP data on the fly
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if stream creation fails
pub async fn get_remote_stream_gz_async(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, bytes::Bytes>>,
    opendal::Error,
> {
    let remote_stream = StreamReader::new(
        get_remote_stream(file_path.clone(), object_storage_options, None).await?,
    );
    Ok(gzip_multi_member_decoder(remote_stream))
}

/// Determines the storage type from a file path or URL
///
/// # Arguments
///
/// * `file_path` - File path or URL to analyze
///
/// # Returns
///
/// The detected storage type (GCS, S3, AZBLOB, HTTP, or LOCAL)
pub fn get_storage_type(file_path: String) -> StorageType {
    //extract the file system prefix from the file path
    let file_system_prefix = file_path.split("://").next();
    let file_system_prefix = if file_path == file_system_prefix.unwrap() {
        None
    } else {
        file_system_prefix
    };
    match file_system_prefix {
        Some(prefix)
            if prefix.to_lowercase().starts_with("http") & is_azure_blob_url(&file_path) =>
        {
            StorageType::AZBLOB
        }
        // A path this crate does not recognize is treated as a local one, so an
        // unsupported scheme surfaces as a normal "cannot open" error naming the
        // path. Panicking here would take the process down over a string a user
        // typed into a query.
        Some(prefix) => StorageType::try_from_prefix(prefix).unwrap_or(StorageType::LOCAL),
        None => StorageType::LOCAL,
    }
}

fn get_bucket_name(file_path: String) -> String {
    //extract the bucket name from the file path
    let bucket_name = file_path
        .split("://")
        .last()
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    //return the bucket name
    bucket_name.to_string()
}

#[derive(Debug)]
struct BlobInfo {
    account: String,
    container: String,
    endpoint: String,
    relative_path: String,
}

fn extract_account_and_container(url_str: &str) -> BlobInfo {
    // 1) Parse with `url::Url`
    let url = Url::parse(url_str).unwrap();
    let scheme = url.scheme();
    let host = url.host_str().ok_or("URL is missing a host").unwrap();
    // If there’s an explicit port (e.g. emulator), include it; otherwise, empty.
    let port = match url.port() {
        Some(p) => format!("{p}"),
        None => String::new(),
    };
    let mut segments = url
        .path_segments()
        .ok_or("Unable to split path segments")
        .unwrap();
    let (account, container) = if host.ends_with(".blob.core.windows.net") {
        // For “real Azure”, the account is the subdomain before ".blob.core.windows.net"
        let account = host.trim_end_matches(".blob.core.windows.net").to_string();

        // The first path segment is the container
        let container = segments
            .next()
            .ok_or("URL is missing container segment")
            .unwrap()
            .to_string();

        (account, container)
    } else {
        // For emulator style (e.g. "127.0.0.1:10000/devstoreaccount1/dataset/"),
        // the first path segment is the account, the second is the container.

        let account = segments
            .next()
            .ok_or("URL is missing account segment")
            .unwrap()
            .to_string();
        let container = segments
            .next()
            .ok_or("URL is missing container segment")
            .unwrap()
            .to_string();

        (account, container)
    };
    let endpoint = if !host.ends_with(".blob.core.windows.net") {
        // For Azure Blob Storage, the endpoint is the full URL without the path
        format!("{scheme}://{host}:{port}/{account}")
    } else {
        format!("{scheme}://{host}:{port}")
    };
    let remaining: Vec<&str> = segments.collect();
    // Join by "/" (no leading slash). If empty, relative_path = ""
    let relative_path = if remaining.is_empty() {
        String::new()
    } else {
        remaining.join("/")
    };
    BlobInfo {
        account,
        container,
        endpoint,
        relative_path,
    }
}

fn is_azure_blob_url(url_str: &str) -> bool {
    if let Ok(url) = Url::parse(url_str)
        && let Some(host) = url.host_str()
    {
        // Check if the host ends with the Azure Blob Storage domain
        if host.ends_with(".blob.core.windows.net") {
            // Ensure the path has at least two segments: container and blob
            if let Some(segments) = url.path_segments() {
                let segments: Vec<_> = segments.collect();
                return segments.len() >= 2;
            }
        } else if let Ok(endpoint) = env::var("AZURE_ENDPOINT_URL")
            && !endpoint.is_empty()
            && url.as_str().starts_with(&endpoint)
        {
            // FIXME: This is a workaround for the Azure Blob Storage emulator.
            return true;
        }
    }
    false
}
/// Creates a byte stream for reading from a file (local or remote)
///
/// # Arguments
///
/// * `file_path` - Path to the file (local path or remote URL)
/// * `object_storage_options` - Configuration options for accessing remote files
/// * `byte_limit` - Optional limit on number of bytes to read
///
/// # Returns
///
/// A byte stream for reading file contents
///
/// # Errors
///
/// Returns an error if the file cannot be accessed or if the storage backend is not supported
pub async fn get_remote_stream(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
    byte_limit: Option<usize>,
) -> Result<FuturesBytesStream, opendal::Error> {
    let object = RemoteObject::open(file_path, object_storage_options).await?;
    match byte_limit {
        Some(limit) => object.stream_range(0..limit as u64).await,
        None => object.stream().await,
    }
}

/// Rebuilds the scheme/host/port prefix of an HTTP object URL.
///
/// `Url::host_str` returns an IPv6 literal without its brackets, so
/// reassembling from it would turn `http://[::1]:8080` into `http://::1:8080`,
/// where the final colon no longer separates a port. `Host`'s Display keeps
/// them.
fn http_endpoint(url: &Url) -> Result<String, opendal::Error> {
    let host = match url.host().ok_or_else(|| {
        opendal::Error::new(
            opendal::ErrorKind::ConfigInvalid,
            "HTTP object URL has no host",
        )
    })? {
        url::Host::Ipv6(address) => format!("[{address}]"),
        other => other.to_string(),
    };
    Ok(match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    })
}

/// Credentials carried in an HTTP object URL's userinfo, percent-decoded.
///
/// Returns `None` when the URL has no username, which is the common case.
/// A username with no password is still returned, since some servers accept it.
fn http_credentials(url: &Url) -> Option<(String, Option<String>)> {
    let username = url.username();
    if username.is_empty() {
        return None;
    }
    Some((percent_decode(username), url.password().map(percent_decode)))
}

/// Decodes the `%XX` escapes a URL's userinfo carries.
///
/// Credentials routinely contain characters that must be escaped there — `@`
/// and `:` above all — so passing the raw field through would authenticate with
/// the wrong secret. Anything that is not a valid escape, or that does not
/// decode to UTF-8, is returned unchanged rather than rejected: this is a
/// credential, not a parse target.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = (index + 2 < bytes.len()).then(|| {
            std::str::from_utf8(&bytes[index + 1..index + 3])
                .ok()
                .and_then(|digits| u8::from_str_radix(digits, 16).ok())
        });
        match (bytes[index], hex.flatten()) {
            (b'%', Some(byte)) => {
                decoded.push(byte);
                index += 3;
            }
            (byte, _) => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).unwrap_or_else(|_| value.to_string())
}

/// A remotely stored immutable object with bounded read primitives.
#[derive(Clone, Debug)]
pub struct RemoteObject {
    operator: Operator,
    path: String,
    chunk_size: Option<usize>,
    concurrent_fetches: usize,
}

impl RemoteObject {
    /// Opens an object using the configured OpenDAL storage backend.
    pub async fn open(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, opendal::Error> {
        let storage_type = get_storage_type(file_path.clone());
        let bucket_name = get_bucket_name(file_path.clone());
        let relative_file_path = get_file_path(file_path.clone());
        let chunk_size = object_storage_options.clone().chunk_size.unwrap_or(64);
        let concurrent_fetches = object_storage_options
            .clone()
            .concurrent_fetches
            .unwrap_or(8);
        let allow_anonymous = object_storage_options.allow_anonymous;
        let enable_request_payer = object_storage_options.enable_request_payer;
        let max_retries = object_storage_options.max_retries.unwrap_or(5);
        let timeout = object_storage_options.timeout.unwrap_or(300);

        let (operator, path, reader_chunk_size, reader_concurrency) = match storage_type {
            StorageType::S3 => {
                log::info!(
                    "Using S3 storage type with parameters: \
                bucket_name: {bucket_name}, \
                allow_anonymous: {allow_anonymous}, \
                enable_request_payer: {enable_request_payer}, \
                max_retries: {max_retries}, \
                timeout: {timeout}"
                );
                let mut builder = S3::default()
                    .region(
                        &env::var("AWS_REGION").unwrap_or(
                            env::var("AWS_DEFAULT_REGION").unwrap_or(
                                S3::detect_region("https://s3.amazonaws.com", bucket_name.as_str())
                                    .await
                                    .unwrap_or("us-east-1".to_string()),
                            ),
                        ),
                    )
                    .bucket(bucket_name.as_str())
                    .endpoint(&env::var("AWS_ENDPOINT_URL").unwrap_or_default());
                if allow_anonymous {
                    builder = builder.disable_ec2_metadata().allow_anonymous();
                };
                if enable_request_payer {
                    builder = builder.enable_request_payer();
                }
                let operator = Operator::new(builder)?
                    .layer(
                        TimeoutLayer::new()
                            .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                    ) // 5 minutes
                    .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                    .layer(LoggingLayer::default())
                    .finish();
                (operator, relative_file_path, None, 1)
            }
            //FIXME: Currently, Azure Blob Storage does not support anonymous access
            StorageType::AZBLOB => {
                let blob_info = extract_account_and_container(&file_path);
                log::info!(
                    "Using Azure Blob Storage type with parameters: \
                account_name: {}, \
                container_name: {}, \
                endpoint: {}, \
                chunk_size: {}, \
                concurrent_fetches: {}, \
                allow_anonymous: {}, \
                max_retries: {}, \
                timeout: {}",
                    blob_info.account,
                    blob_info.container.clone(),
                    blob_info.endpoint,
                    chunk_size,
                    concurrent_fetches,
                    allow_anonymous,
                    max_retries,
                    timeout,
                );

                let builder = Azblob::default()
                    .root("/")
                    .container(&blob_info.container)
                    .endpoint(&blob_info.endpoint)
                    .account_name(&env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default())
                    .account_key(&env::var("AZURE_STORAGE_KEY").unwrap_or_default());
                let operator = Operator::new(builder)?
                    .layer(
                        TimeoutLayer::new()
                            .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                    ) // 5 minutes
                    .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                    .layer(LoggingLayer::default())
                    .finish();
                (
                    operator,
                    blob_info.relative_path,
                    Some(chunk_size * 1024 * 1024),
                    1,
                )
            }
            StorageType::HTTP => {
                let url = Url::parse(&file_path).map_err(|error| {
                    opendal::Error::new(
                        opendal::ErrorKind::ConfigInvalid,
                        "invalid HTTP object URL",
                    )
                    .set_source(error)
                })?;
                let endpoint = http_endpoint(&url)?;
                let mut path = url.path().trim_start_matches('/').to_string();
                if let Some(query) = url.query() {
                    path.push('?');
                    path.push_str(query);
                }
                // Userinfo in the URL is credentials, and rebuilding the
                // endpoint from scheme/host/port alone silently dropped them, so
                // every request went out unauthenticated against a server that
                // required them.
                let mut builder = Http::default().endpoint(&endpoint);
                if let Some((username, password)) = http_credentials(&url) {
                    builder = builder.username(&username);
                    if let Some(password) = password {
                        builder = builder.password(&password);
                    }
                }
                let operator = Operator::new(builder)?
                    .layer(
                        TimeoutLayer::new()
                            .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                    )
                    .layer(RetryLayer::new().with_max_times(max_retries))
                    .layer(LoggingLayer::default())
                    .finish();
                (
                    operator,
                    path,
                    Some(chunk_size * 1024 * 1024),
                    concurrent_fetches,
                )
            }

            StorageType::GCS => {
                log::info!(
                    "Using GCS storage type with parameters: \
                bucket_name: {bucket_name}, \
                chunk_size: {chunk_size}, \
                concurrent_fetches: {concurrent_fetches}, \
                allow_anonymous: {allow_anonymous}, \
                max_retries: {max_retries}, \
                timeout: {timeout}",
                );
                let mut builder = Gcs::default().bucket(bucket_name.as_str());
                if allow_anonymous {
                    builder = builder.disable_vm_metadata().allow_anonymous();
                } else if let Ok(service_account_key) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                    builder = builder.credential_path(service_account_key.as_str());
                } else {
                    log::warn!(
                        "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set. Using default credentials."
                    );
                };
                let operator = Operator::new(builder)?
                    .layer(
                        TimeoutLayer::new()
                            .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                    ) // 5 minutes
                    .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
                    .layer(LoggingLayer::default())
                    .finish();
                (
                    operator,
                    relative_file_path,
                    Some(chunk_size * 1024 * 1024),
                    concurrent_fetches,
                )
            }
            StorageType::LOCAL => {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unsupported,
                    "RemoteObject requires a non-local path",
                ));
            }
        };

        Ok(Self {
            operator,
            path,
            chunk_size: reader_chunk_size,
            concurrent_fetches: reader_concurrency,
        })
    }

    /// Returns the object size in bytes.
    pub async fn size(&self) -> Result<u64, opendal::Error> {
        self.operator
            .stat(&self.path)
            .await
            .map(|metadata| metadata.content_length())
    }

    /// Returns the object's length and a validator identifying this version of
    /// it, without reading its contents.
    ///
    /// A cache keyed on an object's bytes cannot be consulted until those bytes
    /// have been fetched, which defeats the cache. This is what a caller keys on
    /// instead: the length plus whichever validator the backend publishes — an
    /// entity tag where there is one, otherwise a modification time. A backend
    /// offering neither degrades to length alone, which is weaker but no worse
    /// than having no cache at all.
    pub async fn identity(&self) -> Result<(u64, String), opendal::Error> {
        let metadata = self.operator.stat(&self.path).await?;
        let length = metadata.content_length();
        let mut validator = format!("len={length}");
        if let Some(etag) = metadata.etag() {
            validator.push_str(";etag=");
            validator.push_str(etag);
        }
        if let Some(modified) = metadata.last_modified() {
            validator.push_str(";modified=");
            validator.push_str(&modified.to_string());
        }
        Ok((length, validator))
    }

    /// Reads the entire object into memory.
    ///
    /// This is intended for bounded companion metadata such as CSI indexes.
    pub async fn read_all(&self) -> Result<bytes::Bytes, opendal::Error> {
        self.operator
            .read(&self.path)
            .await
            .map(|buffer| buffer.to_bytes())
    }

    /// Reads a half-open byte range into memory.
    pub async fn read_range(&self, range: Range<u64>) -> Result<bytes::Bytes, opendal::Error> {
        self.operator
            .read_with(&self.path)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
    }

    /// Streams the complete object.
    pub async fn stream(&self) -> Result<FuturesBytesStream, opendal::Error> {
        let reader = match self.chunk_size {
            Some(chunk_size) => {
                self.operator
                    .reader_with(&self.path)
                    .chunk(chunk_size)
                    .concurrent(self.concurrent_fetches)
                    .await?
            }
            None => {
                self.operator
                    .reader_with(&self.path)
                    .concurrent(self.concurrent_fetches)
                    .await?
            }
        };
        reader.into_bytes_stream(..).await
    }

    /// Streams the complete object through one sequential backend request.
    ///
    /// Unlike [`Self::stream`], this does not apply the configured reader chunk
    /// size. It therefore avoids a size/HEAD preflight on backends that need the
    /// object length to split a complete-object read into ranges. This is useful
    /// for bounded companion metadata when the caller enforces its own byte
    /// ceiling while consuming the stream.
    pub async fn stream_single_request(&self) -> Result<FuturesBytesStream, opendal::Error> {
        let reader = self.operator.reader_with(&self.path).concurrent(1).await?;
        reader.into_bytes_stream(..).await
    }

    /// Streams the complete object, falling back to a single sequential request
    /// when the backend refuses the size preflight.
    ///
    /// [`Self::stream`] splits a complete-object read into concurrent ranges,
    /// which needs the object length and so issues a HEAD. That concurrency is
    /// what makes a full scan of a large object bearable, so it stays the
    /// default; only a backend that refuses the preflight — a pre-signed URL
    /// authorizing GET and range requests but not HEAD — drops to
    /// [`Self::stream_single_request`], whose sequential read needs no length.
    /// Nothing has been consumed when the preflight fails, so the retry starts
    /// from the beginning of the object.
    pub async fn stream_with_size_preflight_fallback(
        &self,
    ) -> Result<FuturesBytesStream, opendal::Error> {
        match self.stream().await {
            Ok(stream) => Ok(stream),
            Err(error) if is_refused_size_preflight(&error) => self.stream_single_request().await,
            Err(error) => Err(error),
        }
    }

    /// Streams a half-open byte range.
    pub async fn stream_range(
        &self,
        range: Range<u64>,
    ) -> Result<FuturesBytesStream, opendal::Error> {
        let reader = match self.chunk_size {
            Some(chunk_size) => {
                self.operator
                    .reader_with(&self.path)
                    .chunk(chunk_size)
                    .concurrent(self.concurrent_fetches)
                    .await?
            }
            None => {
                self.operator
                    .reader_with(&self.path)
                    .concurrent(self.concurrent_fetches)
                    .await?
            }
        };
        reader.into_bytes_stream(range).await
    }

    /// Streams a range with a hard ceiling on each sequential backend read.
    ///
    /// The configured chunk size is honored when it is smaller than
    /// `max_chunk_size`; otherwise it is capped. Only one chunk is fetched at a
    /// time, so callers can stream an arbitrarily large logical range without
    /// materializing it or multiplying the byte ceiling by reader concurrency.
    pub async fn stream_range_bounded(
        &self,
        range: Range<u64>,
        max_chunk_size: usize,
    ) -> Result<FuturesBytesStream, opendal::Error> {
        if max_chunk_size == 0 {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConfigInvalid,
                "bounded range stream chunk size must be greater than zero",
            ));
        }

        let chunk_size = self
            .chunk_size
            .unwrap_or(max_chunk_size)
            .min(max_chunk_size);
        let reader = self
            .operator
            .reader_with(&self.path)
            .chunk(chunk_size)
            .concurrent(1)
            .await?;
        reader.into_bytes_stream(range).await
    }
}

#[cfg(test)]
mod multimember_tests {
    use super::gzip_multi_member_decoder;
    use tokio::io::AsyncReadExt;

    fn gz(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    #[tokio::test]
    async fn decodes_all_concatenated_gzip_members() {
        // two gzip members concatenated -> "hello" + "world"
        let mut data = gz(b"hello");
        data.extend(gz(b"world"));

        let cursor = std::io::Cursor::new(data);
        let mut decoder = gzip_multi_member_decoder(tokio::io::BufReader::new(cursor));
        let mut out = String::new();
        decoder.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "helloworld");
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn userinfo_becomes_credentials() {
        // Rebuilding the endpoint from scheme/host/port drops userinfo, so the
        // credentials have to be carried over explicitly.
        let url = Url::parse("https://user:pass@example.test/f.bcf").unwrap();
        assert_eq!(
            http_credentials(&url),
            Some(("user".to_string(), Some("pass".to_string())))
        );
        // Credentials routinely contain escaped characters.
        let url = Url::parse("https://a%40b.test:p%3Ass%40word@example.test/f.bcf").unwrap();
        assert_eq!(
            http_credentials(&url),
            Some(("a@b.test".to_string(), Some("p:ss@word".to_string())))
        );
        // A username with no password is still credentials.
        let url = Url::parse("https://token@example.test/f.bcf").unwrap();
        assert_eq!(http_credentials(&url), Some(("token".to_string(), None)));
        // The common case carries none.
        let url = Url::parse("https://example.test/f.bcf").unwrap();
        assert_eq!(http_credentials(&url), None);
    }

    #[test]
    fn a_malformed_escape_is_left_alone() {
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("a%2"), "a%2");
    }

    #[test]
    fn an_ipv6_endpoint_keeps_its_brackets() {
        // Without them the endpoint reads as `http://::1:8081`, whose final
        // colon no longer separates a port.
        let url = Url::parse("http://[::1]:8081/example.bcf").unwrap();
        assert_eq!(http_endpoint(&url).unwrap(), "http://[::1]:8081");
    }

    #[test]
    fn named_and_ipv4_endpoints_are_unchanged() {
        for (input, expected) in [
            ("https://example.test/file.bcf", "https://example.test"),
            ("http://127.0.0.1:8080/file.bcf", "http://127.0.0.1:8080"),
            // A scheme's default port normalizes away, which is correct.
            ("https://example.test:443/f.bcf", "https://example.test"),
        ] {
            let url = Url::parse(input).unwrap();
            assert_eq!(http_endpoint(&url).unwrap(), expected, "{input}");
        }
    }
}

#[cfg(test)]
mod preflight_tests {
    use super::*;

    #[test]
    fn a_refused_size_preflight_is_recognized() {
        // A signed URL that authorizes GET and range requests but not HEAD
        // answers the preflight with 403, and a backend that cannot report a
        // size at all reports it as unsupported. Neither says the object is
        // unreadable, so both are worth retrying without the preflight.
        for kind in [
            opendal::ErrorKind::PermissionDenied,
            opendal::ErrorKind::Unsupported,
        ] {
            assert!(
                is_refused_size_preflight(&opendal::Error::new(kind, "denied")),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_genuine_read_failure_is_not_a_refused_preflight() {
        // Retrying these without a preflight would only fail again, more
        // slowly, and would mask the real error behind the second one.
        for kind in [
            opendal::ErrorKind::NotFound,
            opendal::ErrorKind::ConfigInvalid,
            opendal::ErrorKind::Unexpected,
            opendal::ErrorKind::RateLimited,
        ] {
            assert!(
                !is_refused_size_preflight(&opendal::Error::new(kind, "failed")),
                "{kind:?}"
            );
        }
    }
}
