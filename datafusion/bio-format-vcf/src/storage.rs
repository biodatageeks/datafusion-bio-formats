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

pub async fn get_remote_vcf_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>> {
    let inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options)
        .await
        .unwrap();
    let reader = vcf::r#async::io::Reader::new(inner);
    reader
}

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

pub fn get_local_vcf_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<Reader<MultithreadedReader<File>>, Error> {
    debug!(
        "Reading VCF file from local storage with {} threads using parallel BGZF",
        thread_num
    );

    // Ensure we have at least 1 thread and cap at reasonable maximum
    let worker_count = std::cmp::min(std::cmp::max(thread_num, 1), 32);
    debug!(
        "Using {} worker threads for BGZF decompression",
        worker_count
    );

    File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(worker_count).unwrap(),
                f,
            )
        })
        .map(vcf::io::Reader::new)
}

pub async fn get_local_vcf_gz_reader(
    file_path: String,
) -> Result<
    vcf::r#async::io::Reader<
        tokio::io::BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>,
    >,
    Error,
> {
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(tokio::io::BufReader::new)
        .map(GzipDecoder::new)
        .map(tokio::io::BufReader::new)
        .map(vcf::r#async::io::Reader::new);
    reader
}

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

pub enum VcfRemoteReader {
    BGZF(
        vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>,
        Arc<Header>,
    ),
    GZIP(
        vcf::r#async::io::Reader<BufReader<GzipDecoder<StreamReader<FuturesBytesStream, Bytes>>>>,
        Arc<Header>,
    ),
    PLAIN(
        vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>,
        Arc<Header>,
    ),
}

impl VcfRemoteReader {
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
                let mut reader =
                    get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
                let header = Arc::new(reader.read_header().await.unwrap());
                VcfRemoteReader::BGZF(reader, header)
            }
            CompressionType::GZIP => {
                let mut reader = get_remote_vcf_gz_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                let header = Arc::new(reader.read_header().await.unwrap());
                VcfRemoteReader::GZIP(reader, header)
            }
            CompressionType::NONE => {
                let mut reader = get_remote_vcf_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                let header = Arc::new(reader.read_header().await.unwrap());
                VcfRemoteReader::PLAIN(reader, header)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfRemoteReader::BGZF(_, header) => Ok((**header).clone()),
            VcfRemoteReader::GZIP(_, header) => Ok((**header).clone()),
            VcfRemoteReader::PLAIN(_, header) => Ok((**header).clone()),
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfRemoteReader::BGZF(_, header) => Ok(get_info_fields(header).await),
            VcfRemoteReader::GZIP(_, header) => Ok(get_info_fields(header).await),
            VcfRemoteReader::PLAIN(_, header) => Ok(get_info_fields(header).await),
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfRemoteReader::BGZF(reader, _) => Box::pin(reader.records()),
            VcfRemoteReader::GZIP(reader, _) => Box::pin(reader.records()),
            VcfRemoteReader::PLAIN(reader, _) => Box::pin(reader.records()),
        }
    }
}

pub enum VcfLocalReader {
    BGZF(Reader<MultithreadedReader<File>>, Arc<Header>),
    GZIP(
        vcf::r#async::io::Reader<BufReader<GzipDecoder<tokio::io::BufReader<tokio::fs::File>>>>,
        Arc<Header>,
    ),
    PLAIN(
        vcf::r#async::io::Reader<BufReader<tokio::fs::File>>,
        Arc<Header>,
    ),
}

impl VcfLocalReader {
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

        info!(
            "Creating VcfLocalReader with compression: {:?}, threads: {}",
            compression_type, thread_num
        );

        match compression_type {
            CompressionType::BGZF => {
                let mut reader = get_local_vcf_bgzf_reader(file_path, thread_num).unwrap();
                let header = Arc::new(reader.read_header().unwrap());
                info!(
                    "Successfully created parallel BGZF VCF reader with {} threads",
                    thread_num
                );
                VcfLocalReader::BGZF(reader, header)
            }
            CompressionType::GZIP => {
                let mut reader = get_local_vcf_gz_reader(file_path).await.unwrap();
                let header = Arc::new(reader.read_header().await.unwrap());
                VcfLocalReader::GZIP(reader, header)
            }
            CompressionType::NONE => {
                let mut reader = get_local_vcf_reader(file_path).await.unwrap();
                let header = Arc::new(reader.read_header().await.unwrap());
                VcfLocalReader::PLAIN(reader, header)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfLocalReader::BGZF(_, header) => Ok((**header).clone()),
            VcfLocalReader::GZIP(_, header) => Ok((**header).clone()),
            VcfLocalReader::PLAIN(_, header) => Ok((**header).clone()),
        }
    }

    pub fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfLocalReader::BGZF(reader, _) => {
                let records = reader.records().map(|r| r.map_err(Error::from));
                Box::pin(stream::iter(records))
            }
            VcfLocalReader::GZIP(reader, _) => Box::pin(reader.records()),
            VcfLocalReader::PLAIN(reader, _) => Box::pin(reader.records()),
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfLocalReader::BGZF(_, header) => Ok(get_info_fields(header).await),
            VcfLocalReader::GZIP(_, header) => Ok(get_info_fields(header).await),
            VcfLocalReader::PLAIN(_, header) => Ok(get_info_fields(header).await),
        }
    }
}

pub async fn get_info_fields(header: &Header) -> arrow::array::RecordBatch {
    let info_fields = header.infos();
    let mut field_names = StringBuilder::new();
    let mut field_types = StringBuilder::new();
    let mut field_descriptions = StringBuilder::new();
    for (field_name, field) in info_fields {
        field_names.append_value(field_name);
        field_types.append_value(field.ty().to_string());
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
    let record_batch = arrow::record_batch::RecordBatch::try_new(
        SchemaRef::from(schema.clone()),
        vec![
            Arc::new(field_names),
            Arc::new(field_types),
            Arc::new(field_descriptions),
        ],
    )
    .unwrap();
    record_batch
}

pub enum VcfReader {
    Local(VcfLocalReader),
    Remote(VcfRemoteReader),
}

impl VcfReader {
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

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfReader::Local(reader) => reader.read_header().await,
            VcfReader::Remote(reader) => reader.read_header().await,
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfReader::Local(reader) => reader.describe().await,
            VcfReader::Remote(reader) => reader.describe().await,
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfReader::Local(reader) => reader.read_records(),
            VcfReader::Remote(reader) => reader.read_records().await,
        }
    }
}
