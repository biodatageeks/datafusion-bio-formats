use log;
use noodles::bgzf;
use noodles_bgzf::AsyncReader;
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Gcs, S3};
use opendal::{FuturesBytesStream, Operator};
use std::env;
use tokio_util::io::StreamReader;

#[derive(Clone, Debug)]
pub struct ObjectStorageOptions {
    pub chunk_size: Option<usize>,
    pub concurrent_fetches: Option<usize>,
    pub allow_anonymous: bool,
    pub enable_request_payer: bool,
    pub max_retries: Option<usize>,
    pub timeout: Option<usize>,
}

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
    object_storage_options: ObjectStorageOptions,
) -> Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream =
        StreamReader::new(get_remote_stream(file_path.clone(), object_storage_options).await?);
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
    object_storage_options: ObjectStorageOptions,
) -> Result<FuturesBytesStream, opendal::Error> {
    let storage_type = get_storage_type(file_path.clone());
    let bucket_name = get_bucket_name(file_path.clone());
    let file_path = get_file_path(file_path.clone());
    let chunk_size = object_storage_options.clone().chunk_size.unwrap_or(64);
    let concurrent_fetches = object_storage_options
        .clone()
        .concurrent_fetches
        .unwrap_or(8);
    let allow_anonymous = object_storage_options.allow_anonymous;
    let enable_request_payer = object_storage_options.enable_request_payer;
    let max_retries = object_storage_options.max_retries.unwrap_or(5);
    let timeout = object_storage_options.timeout.unwrap_or(300);

    match storage_type {
        StorageType::S3 => {
            log::info!(
                "Using S3 storage type with parameters: \
                bucket_name: {}, \
                allow_anonymous: {}, \
                enable_request_payer: {}, \
                max_retries: {}, \
                timeout: {}",
                bucket_name,
                allow_anonymous,
                enable_request_payer,
                max_retries,
                timeout
            );
            let mut builder = S3::default()
                .region(
                    &env::var("AWS_REGION").unwrap_or(
                        S3::detect_region("https://s3.amazonaws.com", bucket_name.as_str())
                            .await
                            .unwrap_or("us-east-1".to_string()),
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

            //FIXME: disable because of AWS S3 bug
            // Reduce chunk size and increase concurrency for better reliability
            // let adjusted_chunk_size = chunk_size.min(8 * 1024 * 1024); // Max 8MB chunks
            // let adjusted_concurrency = concurrent_fetches.max(4); // Min 4 concurrent fetches

            operator
                .reader_with(file_path.as_str())
                .concurrent(1)
                .await?
                .into_bytes_stream(..)
                .await
        }
        StorageType::GCS => {
            log::info!(
                "Using GCS storage type with parameters: \
                bucket_name: {}, \
                chunk_size: {}, \
                concurrent_fetches: {}, \
                allow_anonymous: {}, \
                max_retries: {}, \
                timeout: {}",
                bucket_name,
                chunk_size,
                concurrent_fetches,
                allow_anonymous,
                max_retries,
                timeout,
            );
            let mut builder = Gcs::default().bucket(bucket_name.as_str());
            if allow_anonymous {
                builder = builder.disable_vm_metadata().allow_anonymous();
            };
            let operator = Operator::new(builder)?
                .layer(
                    TimeoutLayer::new()
                        .with_io_timeout(std::time::Duration::from_secs(timeout as u64)),
                ) // 5 minutes
                .layer(RetryLayer::new().with_max_times(max_retries)) // Retry up to 5 times
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
