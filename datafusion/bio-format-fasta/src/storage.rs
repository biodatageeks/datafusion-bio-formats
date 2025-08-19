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

pub async fn get_remote_fasta_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fasta::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let reader = fasta::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

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

pub fn get_local_fasta_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<fasta::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(fasta::io::Reader::new);
    reader
}

pub fn get_local_fasta_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fasta::io::Reader::new);
    reader
}

pub async fn get_local_fasta_gz_reader(
    file_path: String,
) -> Result<
    fasta::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(fasta::r#async::io::Reader::new);
    reader
}

pub enum FastaRemoteReader {
    BGZF(
        fasta::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    GZIP(
        fasta::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    StreamReader<FuturesBytesStream, Bytes>,
                >,
            >,
        >,
    ),
    PLAIN(fasta::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastaRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone()).await;
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
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastaRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastaRemoteReader::GZIP(reader) => reader.records().boxed(),
            FastaRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub enum FastaLocalReader {
    BGZF(fasta::io::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    GZIP(
        fasta::r#async::io::Reader<
            tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
        >,
    ),
    PLAIN(Reader<BufReader<File>>),
}

impl FastaLocalReader {
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await;
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_fasta_bgzf_reader(file_path, thread_num)?;
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

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastaLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastaLocalReader::GZIP(reader) => reader.records().boxed(),
            FastaLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
