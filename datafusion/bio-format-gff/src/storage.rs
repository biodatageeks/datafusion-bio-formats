use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bgzf;
use noodles_gff as gff;
use noodles_gff::feature::RecordBuf;
use noodles_gff::feature::record_buf::Attributes;
use noodles_gff::io::Reader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
use tokio_util::io::StreamReader;

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
}

impl GffRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        match compression_type {
            CompressionType::GZIP => {
                let reader = get_remote_gff_gz_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::GZIP(reader))
            }
            CompressionType::BGZF => {
                let reader = get_remote_gff_bgzf_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::BGZF(reader))
            }
            CompressionType::NONE => {
                let reader = get_remote_gff_reader(file_path, object_storage_options).await?;
                Ok(GffRemoteReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Compression type {:?} is not supported for GFF files",
                compression_type
            ),
        }
    }
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<RecordBuf, Error>> {
        match self {
            GffRemoteReader::BGZF(reader) => reader.record_bufs().boxed(),
            GffRemoteReader::GZIP(reader) => reader.record_bufs().boxed(),
            GffRemoteReader::PLAIN(reader) => reader.record_bufs().boxed(),
        }
    }
    pub async fn get_attributes(&mut self) -> Attributes {
        match self {
            GffRemoteReader::BGZF(reader) => reader.record_bufs().next().await.unwrap(),
            GffRemoteReader::GZIP(reader) => reader.record_bufs().next().await.unwrap(),
            GffRemoteReader::PLAIN(reader) => reader.record_bufs().next().await.unwrap(),
        }
        .unwrap()
        .attributes()
        .clone()
    }
}

pub fn get_local_gff_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<gff::io::Reader<bgzf::MultithreadedReader<std::fs::File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(|f| {
            bgzf::MultithreadedReader::with_worker_count(
                std::num::NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(gff::io::Reader::new);
    reader
}

pub fn get_local_gff_reader(file_path: String) -> Result<Reader<BufReader<File>>, Error> {
    let reader = std::fs::File::open(file_path)
        .map(BufReader::new)
        .map(gff::io::Reader::new);
    reader
}

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

pub enum GffLocalReader {
    GZIP(
        gff::r#async::io::Reader<
            tokio::io::BufReader<
                async_compression::tokio::bufread::GzipDecoder<
                    tokio::io::BufReader<tokio::fs::File>,
                >,
            >,
        >,
    ),
    BGZF(gff::io::Reader<bgzf::MultithreadedReader<std::fs::File>>),
    PLAIN(Reader<BufReader<File>>),
}

impl GffLocalReader {
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        let compression_type =
            get_compression_type(file_path.clone(), object_storage_options.compression_type);
        match compression_type {
            CompressionType::GZIP => {
                let reader = get_local_gff_gz_reader(file_path).await?;
                Ok(GffLocalReader::GZIP(reader))
            }
            CompressionType::BGZF => {
                let reader = get_local_gff_bgzf_reader(file_path, thread_num)?;
                Ok(GffLocalReader::BGZF(reader))
            }
            CompressionType::NONE => {
                let reader = get_local_gff_reader(file_path)?;
                Ok(GffLocalReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Compression type {:?} is not supported for GFF files",
                compression_type
            ),
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<RecordBuf, Error>> {
        match self {
            GffLocalReader::BGZF(reader) => stream::iter(reader.record_bufs()).boxed(),
            GffLocalReader::GZIP(reader) => reader.record_bufs().boxed(),
            GffLocalReader::PLAIN(reader) => stream::iter(reader.record_bufs()).boxed(),
        }
    }

    pub async fn get_attributes(&mut self) -> Attributes {
        match self {
            GffLocalReader::BGZF(reader) => reader.record_bufs().next().unwrap(),
            GffLocalReader::GZIP(reader) => reader.record_bufs().next().await.unwrap(),
            GffLocalReader::PLAIN(reader) => reader.record_bufs().next().unwrap(),
        }
        .unwrap()
        .attributes()
        .clone()
    }
}
