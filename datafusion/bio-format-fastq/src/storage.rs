use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bgzf;
use noodles_fastq::Record;
use std::fs::File;
use std::io::{BufReader, Error};

use noodles_fastq as fastq;
use noodles_fastq::io::Reader;
use opendal::FuturesBytesStream;
use tokio_util::io::StreamReader;

pub async fn get_remote_fastq_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    Error,
> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let reader = fastq::r#async::io::Reader::new(inner);
    Ok(reader)
}

pub async fn get_remote_fastq_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let reader = fastq::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

pub fn get_local_fastq_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(fastq::io::Reader::new);
    reader
}

pub fn get_local_fastq_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(fastq::io::Reader::new);
    reader
}

pub enum FastqRemoteReader {
    BGZF(
        fastq::r#async::io::Reader<bgzf::r#async::Reader<StreamReader<FuturesBytesStream, Bytes>>>,
    ),
    PLAIN(fastq::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl FastqRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        match compression_type {
            CompressionType::BGZF => {
                let reader =
                    get_remote_fastq_bgzf_reader(file_path, object_storage_options).await?;
                Ok(FastqRemoteReader::BGZF(reader))
            }
            CompressionType::NONE => {
                let reader = get_remote_fastq_reader(file_path, object_storage_options).await?;
                Ok(FastqRemoteReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqRemoteReader::BGZF(reader) => reader.records().boxed(),
            FastqRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub enum FastqLocalReader {
    BGZF(fastq::io::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    PLAIN(Reader<BufReader<File>>),
}

impl FastqLocalReader {
    pub fn new(file_path: String, thread_num: usize) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        match compression_type {
            CompressionType::BGZF => {
                let reader = get_local_fastq_bgzf_reader(file_path, thread_num)?;
                Ok(FastqLocalReader::BGZF(reader))
            }
            CompressionType::NONE => {
                let reader = get_local_fastq_reader(file_path)?;
                Ok(FastqLocalReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for FASTQ reader: {:?}",
                compression_type
            ),
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            FastqLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            FastqLocalReader::PLAIN(reader) => stream::iter(reader.records()).boxed(),
        }
    }
}
