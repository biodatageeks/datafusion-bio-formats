use noodles::bgzf;
use noodles_bgzf::AsyncReader;
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Gcs, S3};
use opendal::{FuturesBytesStream, Operator};
use tokio_util::io::StreamReader;
pub enum CompressionType {
    GZIP,
    BGZF,
    NONE,
}

impl CompressionType {
    pub fn from_string(compression_type: String) -> Self {
        match compression_type.to_lowercase().as_str() {
            "gz" => CompressionType::GZIP,
            "bgz" => CompressionType::BGZF,
            "none" => CompressionType::NONE,
            _ => panic!("Invalid compression type"),
        }
    }
}

pub enum StorageType {
    GCS,
    S3,
    AZBLOB,
    LOCAL,
}

impl StorageType {
    pub fn from_string(object_storage_type: String) -> Self {
        match object_storage_type.as_str() {
            "gs" => StorageType::GCS,
            "s3" => StorageType::S3,
            "abfs" => StorageType::AZBLOB,
            "local" => StorageType::LOCAL,
            "file" => StorageType::LOCAL,
            _ => panic!("Invalid object storage type"),
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

pub fn get_compression_type(file_path: String) -> CompressionType {
    //extract the file extension from path
    if file_path.to_lowercase().ends_with(".vcf") {
        return CompressionType::NONE;
    }
    let file_extension = file_path.split('.').last().unwrap();
    //return the compression type
    CompressionType::from_string(file_extension.to_string())
}

pub async fn get_remote_stream_bgzf(
    file_path: String,
    chunk_size: usize,
    concurrent_fetches: usize,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream = StreamReader::new(
        get_remote_stream(file_path.clone(), chunk_size, concurrent_fetches).await?,
    );
    Ok(bgzf::r#async::Reader::new(remote_stream))
}

pub fn get_storage_type(file_path: String) -> StorageType {
    //extract the file system prefix from the file path
    let file_system_prefix = file_path.split("://").next();
    let file_system_prefix = if file_path == file_system_prefix.unwrap() {
        None
    } else {
        file_system_prefix
    };
    match file_system_prefix {
        Some(prefix) => StorageType::from_string(prefix.to_string()),
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

pub async fn get_remote_stream(
    file_path: String,
    chunk_size: usize,
    concurrent_fetches: usize,
) -> Result<FuturesBytesStream, opendal::Error> {
    let storage_type = get_storage_type(file_path.clone());
    let bucket_name = get_bucket_name(file_path.clone());
    let file_path = get_file_path(file_path.clone());

    match storage_type {
        StorageType::S3 => {
            let builder = S3::default()
                .region(
                    &S3::detect_region("https://s3.amazonaws.com", bucket_name.as_str())
                        .await
                        .unwrap(),
                )
                .bucket(bucket_name.as_str())
                .disable_ec2_metadata()
                .allow_anonymous();

            let operator = Operator::new(builder)?
                .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(300))) // 5 minutes
                .layer(RetryLayer::new().with_max_times(5)) // Retry up to 5 times
                .layer(LoggingLayer::default())
                .finish();

            // Reduce chunk size and increase concurrency for better reliability
            let adjusted_chunk_size = chunk_size.min(8 * 1024 * 1024); // Max 8MB chunks
            let adjusted_concurrency = concurrent_fetches.max(4); // Min 4 concurrent fetches

            operator
                .reader_with(file_path.as_str())
                .chunk(adjusted_chunk_size)
                .concurrent(adjusted_concurrency)
                .await?
                .into_bytes_stream(..)
                .await
        }
        StorageType::GCS => {
            let builder = Gcs::default()
                .bucket(bucket_name.as_str())
                .disable_vm_metadata()
                .allow_anonymous();
            let operator = Operator::new(builder)?
                .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(120)))
                .layer(RetryLayer::new().with_max_times(3))
                .layer(LoggingLayer::default())
                .finish();
            operator
                .reader_with(file_path.as_str())
                .chunk(chunk_size * 1024 * 1024)
                .concurrent(concurrent_fetches)
                .await?
                .into_bytes_stream(..)
                .await
        }
        _ => panic!("Invalid object storage type"),
    }
}
