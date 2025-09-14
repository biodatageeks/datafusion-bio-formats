use crate::async_reader;
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use log::{debug, error, info};
use noodles::bgzf;
use noodles_bed;
use noodles_bed::Record;
use noodles_bgzf::MultithreadedReader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use tokio_util::io::StreamReader;

pub async fn get_remote_bed_bgzf_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_reader::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>, N>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = async_reader::Reader::new(inner);
    Ok(reader)
}

pub async fn get_remote_fastq_gz_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    async_reader::Reader<
        tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>,
        N,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = async_reader::Reader::new(stream);
    Ok(reader)
}

pub async fn get_remote_bed_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<async_reader::Reader<StreamReader<FuturesBytesStream, Bytes>, N>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = async_reader::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

pub fn get_local_bed_bgzf_reader<const N: usize>(
    file_path: String,
    thread_num: usize,
) -> Result<noodles_bed::io::Reader<N, MultithreadedReader<File>>, Error> {
    debug!(
        "Reading VCF file from local storage with {} threads",
        thread_num
    );
    let reader = File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(noodles_bed::io::Reader::new);
    reader
}

pub async fn get_local_bed_gz_reader<const N: usize>(
    file_path: String,
) -> Result<
    async_reader::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        N,
    >,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(async_reader::Reader::new);
    reader
}

pub fn get_local_bed_reader<const N: usize>(
    file_path: String,
) -> Result<noodles_bed::io::Reader<N, std::io::BufReader<File>>, Error> {
    debug!("Reading BED file from local storage with sync reader");
    let reader = File::open(file_path)
        .map(|f| std::io::BufReader::new(f))
        .map(noodles_bed::io::Reader::new);
    reader
}

pub enum BedRemoteReader<const N: usize> {
    BGZF(async_reader::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>, N>),
    GZIP(
        async_reader::Reader<
            tokio::io::BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>,
            N,
        >,
    ),
    PLAIN(async_reader::Reader<StreamReader<FuturesBytesStream, Bytes>, N>),
}

macro_rules! impl_bed_remote_reader {
    ($($n:expr),*) => {
        $(
            impl BedRemoteReader<$n> {
                pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
                    info!("Creating remote BED reader: {}", object_storage_options);
                    let compression_type = get_compression_type(
                        file_path.clone(),
                        object_storage_options.clone().compression_type,
                        object_storage_options.clone(),
                    )
                    .await
                    .unwrap_or(CompressionType::NONE);
                    match compression_type {
                        CompressionType::BGZF => {
                            let reader = get_remote_bed_bgzf_reader::<$n>(file_path, object_storage_options).await.unwrap();
                            BedRemoteReader::BGZF(reader)
                        }
                        CompressionType::NONE => {
                            let reader = get_remote_bed_reader::<$n>(file_path, object_storage_options).await.unwrap();
                            BedRemoteReader::PLAIN(reader)
                        }

                        _ => panic!("Compression type not supported."),
                    }
                }

                pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record<$n>, Error>> {
                    match self {
                        BedRemoteReader::BGZF(reader) => reader.records().boxed(),
                        BedRemoteReader::GZIP(reader) => reader.records().boxed(),
                        BedRemoteReader::PLAIN(reader) => reader.records().boxed(),
                    }
                }

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

pub enum BedLocalReader<const N: usize> {
    BGZF(noodles_bed::io::Reader<N, MultithreadedReader<File>>),
    PLAIN(noodles_bed::io::Reader<N, std::io::BufReader<File>>),
}

macro_rules! impl_bed_local_reader {
    ($($n:expr),*) => {
        $(
            impl BedLocalReader<$n> {
                pub async fn new(file_path: String, thread_num: usize) -> Result<Self, Error> {
                    info!("Creating local BED reader: {}", file_path);
                    let compression_type = get_compression_type(
                        file_path.clone(),
                        None,
                        ObjectStorageOptions::default(),
                    )
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                    match compression_type {
                        CompressionType::BGZF => {
                            let reader = get_local_bed_bgzf_reader::<$n>(file_path, thread_num)?;
                            Ok(BedLocalReader::BGZF(reader))
                        }
                        CompressionType::NONE => {
                            let reader = get_local_bed_reader::<$n>(file_path)?;
                            Ok(BedLocalReader::PLAIN(reader))
                        }
                        _ => panic!("Compression type not supported."),
                    }
                }

                pub fn read_records(&mut self) -> impl Stream<Item = Result<Record<$n>, Error>> + '_ {
                    match self {
                        BedLocalReader::BGZF(reader) => {
                            try_stream! {
                                loop{
                                    let mut record = noodles_bed::Record::<$n>::default();
                                    match reader.read_record(&mut record) {
                                        Ok(0) => break, // EOF
                                        Ok(_) => yield record,
                                        _ => error!("Error reading record from BED file"),
                                    }
                                }
                            }.boxed()
                        },
                        BedLocalReader::PLAIN(reader) => {
                            try_stream! {
                                loop{
                                    let mut record = noodles_bed::Record::<$n>::default();
                                    match reader.read_record(&mut record) {
                                        Ok(0) => break, // EOF
                                        Ok(_) => yield record,
                                        _ => error!("Error reading record from BED file"),
                                    }
                                }
                            }.boxed()
                        },
                    }
                }
            }
        )*
    };
}

// Generate implementations for N = 3, 4, 5, 6
impl_bed_local_reader!(3, 4, 5, 6);
