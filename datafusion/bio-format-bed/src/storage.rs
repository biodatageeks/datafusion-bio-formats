use crate::async_reader;
use crate::record::{line_error, prepare_line};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, gzip_multi_member_decoder,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use log::{debug, info};
use noodles_bed;
use noodles_bed::Record;
use noodles_bgzf as bgzf;
use noodles_bgzf::io::Reader as BgzfReader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, Cursor, Error};
use tokio_util::io::StreamReader;

/// Creates a remote BGZF-compressed BED reader from cloud storage
///
/// # Arguments
///
/// * `file_path` - Remote file path (GCS, S3, or Azure URL)
/// * `object_storage_options` - Cloud storage configuration
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
pub async fn get_remote_bed_bgzf_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_reader::Reader<bgzf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, N>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = async_reader::Reader::new(inner);
    Ok(reader)
}

/// Creates a remote GZIP-compressed BED reader from cloud storage
///
/// # Arguments
///
/// * `file_path` - Remote file path (GCS, S3, or Azure URL)
/// * `object_storage_options` - Cloud storage configuration
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
pub async fn get_remote_bed_gz_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_reader::Reader<
        tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>,
        N,
    >,
    Error,
> {
    // get_remote_stream_gz_async is multi-member aware (sets multiple_members(true)).
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = async_reader::Reader::new(stream);
    Ok(reader)
}

/// Creates a remote uncompressed BED reader from cloud storage
///
/// # Arguments
///
/// * `file_path` - Remote file path (GCS, S3, or Azure URL)
/// * `object_storage_options` - Cloud storage configuration
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
pub async fn get_remote_bed_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<async_reader::Reader<StreamReader<FuturesBytesStream, Bytes>, N>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = async_reader::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

/// Creates a local BGZF-compressed BED reader with parallel decompression
///
/// # Arguments
///
/// * `file_path` - Local file path
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
///
/// # Errors
///
/// Returns error if file cannot be opened
pub fn get_local_bed_bgzf_reader<const N: usize>(
    file_path: String,
) -> Result<noodles_bed::io::Reader<N, BgzfReader<File>>, Error> {
    debug!("Reading BED file from local storage");
    File::open(file_path.strip_prefix("file://").unwrap_or(&file_path))
        .map(BgzfReader::new)
        .map(noodles_bed::io::Reader::new)
}

/// Creates a local GZIP-compressed BED reader
///
/// # Arguments
///
/// * `file_path` - Local file path
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
///
/// # Errors
///
/// Returns error if file cannot be opened
pub async fn get_local_bed_gz_reader<const N: usize>(
    file_path: String,
) -> Result<
    async_reader::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        N,
    >,
    Error,
> {
    tokio::fs::File::open(file_path.strip_prefix("file://").unwrap_or(&file_path))
        .await
        .map(tokio::io::BufReader::new)
        .map(gzip_multi_member_decoder)
        .map(tokio::io::BufReader::new)
        .map(async_reader::Reader::new)
}

/// Creates a local uncompressed BED reader
///
/// # Arguments
///
/// * `file_path` - Local file path
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
///
/// # Errors
///
/// Returns error if file cannot be opened
pub fn get_local_bed_reader<const N: usize>(
    file_path: String,
) -> Result<noodles_bed::io::Reader<N, std::io::BufReader<File>>, Error> {
    debug!("Reading BED file from local storage with sync reader");
    File::open(file_path.strip_prefix("file://").unwrap_or(&file_path))
        .map(std::io::BufReader::new)
        .map(noodles_bed::io::Reader::new)
}

/// Remote BED reader supporting multiple compression formats
///
/// This enum wraps different reader implementations for BGZF, GZIP, and uncompressed
/// BED files from cloud storage backends.
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
#[allow(clippy::large_enum_variant)]
pub enum BedRemoteReader<const N: usize> {
    /// BGZF-compressed BED reader
    BGZF(
        async_reader::Reader<bgzf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, N>,
    ),
    /// GZIP-compressed BED reader
    GZIP(
        async_reader::Reader<
            tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>,
            N,
        >,
    ),
    /// Uncompressed BED reader
    PLAIN(async_reader::Reader<StreamReader<FuturesBytesStream, Bytes>, N>),
}

/// Macro to generate BedRemoteReader implementations for different column counts
macro_rules! impl_bed_remote_reader {
    ($($n:expr),*) => {
        $(
            impl BedRemoteReader<$n> {
                /// Creates a new remote BED reader, auto-detecting compression format
                pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Result<Self, Error> {
                    info!("Creating remote BED reader: {}", object_storage_options);
                    let compression_type = get_compression_type(
                        file_path.clone(),
                        object_storage_options.clone().compression_type,
                        object_storage_options.clone(),
                    )
                    .await
                    .map_err(Error::other)?;
                    match compression_type {
                        CompressionType::BGZF => {
                            let reader = get_remote_bed_bgzf_reader::<$n>(file_path, object_storage_options).await?;
                            Ok(BedRemoteReader::BGZF(reader))
                        }
                        CompressionType::GZIP => {
                            let reader = get_remote_bed_gz_reader::<$n>(file_path, object_storage_options).await?;
                            Ok(BedRemoteReader::GZIP(reader))
                        }
                        CompressionType::NONE => {
                            let reader = get_remote_bed_reader::<$n>(file_path, object_storage_options).await?;
                            Ok(BedRemoteReader::PLAIN(reader))
                        }

                        _ => Err(Error::new(std::io::ErrorKind::InvalidInput, "unsupported BED compression")),
                    }
                }

                /// Returns a stream of BED records from the remote reader
                pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record<$n>, Error>> {
                    match self {
                        BedRemoteReader::BGZF(reader) => reader.records().boxed(),
                        BedRemoteReader::GZIP(reader) => reader.records().boxed(),
                        BedRemoteReader::PLAIN(reader) => reader.records().boxed(),
                    }
                }

                /// Returns a stream of lines from the remote reader
                pub async fn lines(&mut self) -> BoxStream<'_, Result<String, Error>> {
                    match self {
                        BedRemoteReader::BGZF(reader) => reader.lines().boxed(),
                        BedRemoteReader::GZIP(reader) => reader.lines().boxed(),
                        BedRemoteReader::PLAIN(reader) => reader.lines().boxed(),
                    }
                }
            }
        )*
    };
}
//
// // Generate implementations for N = 3, 4, 5, 6
impl_bed_remote_reader!(3, 4, 5, 6);

/// Local BED reader supporting multiple compression formats
///
/// This enum wraps different reader implementations for BGZF and uncompressed
/// BED files from local storage.
///
/// # Type Parameters
///
/// * `N` - Number of BED columns (3-6)
pub enum BedLocalReader<const N: usize> {
    /// BGZF-compressed BED reader
    BGZF(noodles_bed::io::Reader<N, BgzfReader<File>>),
    /// GZIP-compressed BED reader (multi-member aware)
    GZIP(
        Box<
            async_reader::Reader<
                tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
                N,
            >,
        >,
    ),
    /// Uncompressed BED reader
    PLAIN(noodles_bed::io::Reader<N, std::io::BufReader<File>>),
}

// Both synchronous backends use the same framing and errors as the async reader.
macro_rules! sync_records {
    ($reader:expr, $n:expr) => {
        try_stream! {
            let mut buf = Vec::new();
            let mut line_number = 0;
            loop {
                buf.clear();
                line_number += 1;
                let n = $reader.get_mut().read_until(b'\n', &mut buf)
                    .map_err(|e| line_error(line_number, e))?;
                if n == 0 {
                    break;
                }
                if !prepare_line(&mut buf, $n).map_err(|e| line_error(line_number, e))? {
                    continue;
                }
                let mut record = Record::<$n>::default();
                let mut parser = noodles_bed::io::Reader::<$n, _>::new(Cursor::new(&buf));
                parser.read_record(&mut record).map_err(|e| line_error(line_number, e))?;
                yield record;
            }
        }
        .boxed()
    };
}

/// Macro to generate BedLocalReader implementations for different column counts
macro_rules! impl_bed_local_reader {
    ($($n:expr),*) => {
        $(
            impl BedLocalReader<$n> {
                /// Creates a new local BED reader, auto-detecting compression format
                pub async fn new(file_path: String) -> Result<Self, Error> {
                    Self::with_options(file_path, ObjectStorageOptions::default()).await
                }

                /// Creates a local reader honoring an explicit compression setting.
                pub async fn with_options(file_path: String, options: ObjectStorageOptions) -> Result<Self, Error> {
                    info!("Creating local BED reader: {}", file_path);
                    let compression_type = get_compression_type(
                        file_path.clone(),
                        options.compression_type.clone(),
                        options,
                    )
                    .await
                    .map_err(std::io::Error::other)?;
                    match compression_type {
                        CompressionType::BGZF => {
                            let reader = get_local_bed_bgzf_reader::<$n>(file_path)?;
                            Ok(BedLocalReader::BGZF(reader))
                        }
                        CompressionType::GZIP => {
                            let reader = get_local_bed_gz_reader::<$n>(file_path).await?;
                            Ok(BedLocalReader::GZIP(Box::new(reader)))
                        }
                        CompressionType::NONE => {
                            let reader = get_local_bed_reader::<$n>(file_path)?;
                            Ok(BedLocalReader::PLAIN(reader))
                        }
                        _ => Err(Error::new(std::io::ErrorKind::InvalidInput, "unsupported BED compression")),
                    }
                }

                /// Returns a stream of BED records from the local reader
                pub fn read_records(&mut self) -> impl Stream<Item = Result<Record<$n>, Error>> + '_ {
                    match self {
                        BedLocalReader::BGZF(reader) => sync_records!(reader, $n),
                        BedLocalReader::GZIP(reader) => reader.records().boxed(),
                        BedLocalReader::PLAIN(reader) => sync_records!(reader, $n),
                    }
                }
            }
        )*
    };
}

// Generate implementations for N = 3, 4, 5, 6
impl_bed_local_reader!(3, 4, 5, 6);

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Read};

    struct FailingRead;
    impl Read for FailingRead {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("injected sync read failure"))
        }
    }

    #[tokio::test]
    async fn sync_io_failure_is_yielded_once_and_terminates() {
        let input = Cursor::new(b"chr1\t0\t5\n").chain(FailingRead);
        let mut reader = noodles_bed::io::Reader::<3, _>::new(std::io::BufReader::new(input));
        let mut records = sync_records!(reader, 3);
        assert!(records.next().await.unwrap().is_ok());
        let error: io::Error = records.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("BED line 2"));
        assert!(error.to_string().contains("injected sync read failure"));
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn sync_widths_and_small_buffers_match_async_parsing() {
        for size in [1, 2, 7, 64] {
            let input = &b"# comment\r\nchr1\t0\t5\r\nchr1\t5\t8\tname\nchr1\t9"[..];
            let mut reader = noodles_bed::io::Reader::<4, _>::new(
                std::io::BufReader::with_capacity(size, input),
            );
            let mut records = sync_records!(reader, 4);
            assert!(records.next().await.unwrap().unwrap().name().is_none());
            assert_eq!(
                records
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .name()
                    .unwrap()
                    .to_string(),
                "name"
            );
            let error: io::Error = records.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("BED line 4"));
            assert!(records.next().await.is_none());
        }
    }
}
