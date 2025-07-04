use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, stream};
use log::error;
use noodles::bgzf;
use noodles_fasta as fasta;
use noodles_fasta::AsyncReader;
use noodles_fasta::Record;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
use tokio_util::io::StreamReader;

pub async fn get_remote_fasta_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fasta::AsyncReader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>, Error>
{
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = AsyncReader::new(inner);
    Ok(reader)
}

pub async fn get_remote_fasta_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let reader = AsyncReader::new(StreamReader::new(stream));
    Ok(reader)
}

pub async fn get_remote_fasta_gz_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    AsyncReader<
        tokio::io::BufReader<
            async_compression::tokio::bufread::GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>,
        >,
    >,
    Error,
> {
    let stream = tokio::io::BufReader::new(
        get_remote_stream_gz_async(file_path.clone(), object_storage_options).await?,
    );
    let reader = AsyncReader::new(stream);
    Ok(reader)
}

pub fn get_local_fasta_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<fasta::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(fasta::Reader::new);
    reader
}

pub fn get_local_fastq_reader(file_path: String) -> Result<fasta::Reader<BufReader<File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fasta::Reader::new);
    reader
}

pub async fn get_local_fastq_gz_reader(
    file_path: String,
) -> Result<
    AsyncReader<tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>>,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(AsyncReader::new);
    reader
}

pub enum FastaRemoteReader {
    BGZF(AsyncReader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>),
    GZIP(
        AsyncReader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    PLAIN(AsyncReader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastaRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
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
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }
    // pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
    //     match self {
    //         FastaRemoteReader::BGZF(reader) => {
    //             try_stream! {
    //               loop{
    //                    let mut buf = Vec::new();
    //                     match reader.read_sequence(&mut buf).await? {
    //                         Ok(0) => break, // EOF
    //                         Ok(_) => yield buf,
    //                         _ => error!("Error reading record from BED file"),
    //                     }
    //                 }
    //             }.boxed()
    //         },
    //         FastaRemoteReader::GZIP(reader) => reader.records().boxed(),
    //         FastaRemoteReader::PLAIN(reader) => reader.records().boxed(),
    //     }
    // }
}

pub enum FastaLocalReader {
    BGZF(fasta::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    GZIP(AsyncReader<tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>>),
    PLAIN(fasta::Reader<BufReader<File>>),
}

impl FastaLocalReader {
    pub async fn new(file_path: String, thread_num: usize) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_fasta_bgzf_reader(file_path, thread_num)?;
                Ok(FastaLocalReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                // GZIP is treated as BGZF for local files
                let reader = get_local_fastq_gz_reader(file_path).await?;
                Ok(FastaLocalReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader = get_local_fastq_reader(file_path)?;
                Ok(FastaLocalReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }

    // pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
    //     match self {
    //         FastaLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
    //         FastaLocalReader::GZIP(reader) => reader.records().boxed(),
    //         FastaLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
    //     }
    // }
}
