use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles_bam as bam;
use noodles_bam::Record;
use noodles_bam::io::Reader;
use noodles_bgzf as bgzf;
use noodles_bgzf::MultithreadedReader;
use noodles_sam::header::ReferenceSequences;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use tokio_util::io::StreamReader;

/// Creates a remote BAM reader for cloud storage (GCS, S3, Azure).
///
/// # Arguments
///
/// * `file_path` - Path to the BAM file on cloud storage
/// * `object_storage_options` - Configuration for object storage access
///
/// # Returns
///
/// A remote BAM async reader that can be used to stream records
pub async fn get_remote_bam_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    bam::r#async::io::Reader<bgzf::AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>>,
    Error,
> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = bam::r#async::io::Reader::from(bgzf::AsyncReader::new(StreamReader::new(stream)));
    Ok(reader)
}

/// Creates a local BAM reader with multithreaded decompression.
///
/// # Arguments
///
/// * `file_path` - Path to the local BAM file
/// * `thread_num` - Number of threads to use for BGZF decompression
///
/// # Returns
///
/// A local BAM reader with multithreaded BGZF decompression capability
pub async fn get_local_bam_reader(
    file_path: String,
    thread_num: usize,
) -> Result<Reader<MultithreadedReader<File>>, Error> {
    File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(bam::io::Reader::from)
}

/// An enum representing either a local or remote BAM file reader.
///
/// This type abstracts over the different reader implementations needed for local
/// files with multithreaded decompression and remote files accessed via cloud storage.
pub enum BamReader {
    /// Local BAM reader with multithreaded BGZF decompression
    Local(Reader<MultithreadedReader<File>>),
    /// Remote BAM reader for cloud storage access
    Remote(
        Box<
            bam::r#async::io::Reader<
                noodles_bgzf::AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
            >,
        >,
    ),
}

impl BamReader {
    /// Creates a new BAM reader for either local or remote files.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the BAM file (local or remote URL)
    /// * `thread_num` - Optional number of threads for local BGZF decompression
    /// * `object_storage_options` - Optional cloud storage configuration
    ///
    /// # Returns
    ///
    /// A BamReader variant appropriate for the storage type detected from the file path
    pub async fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        match storage_type {
            StorageType::LOCAL => {
                let thread_num = thread_num.unwrap_or(1);
                let reader = get_local_bam_reader(file_path, thread_num).await.unwrap();
                BamReader::Local(reader)
            }
            StorageType::AZBLOB | StorageType::GCS | StorageType::S3 => {
                let object_storage_options = object_storage_options
                    .expect("ObjectStorageOptions must be provided for remote storage");
                let reader = get_remote_bam_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                BamReader::Remote(Box::new(reader))
            }
            _ => panic!("Unsupported storage type for BAM file: {:?}", storage_type),
        }
    }
    /// Reads BAM records from the file as an async stream.
    ///
    /// # Returns
    ///
    /// A boxed future stream yielding BAM records or IO errors
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            BamReader::Local(reader) => {
                // reader.read_header().unwrap();
                stream::iter(reader.records()).boxed()
            }
            BamReader::Remote(reader) => {
                // reader.read_header().await.unwrap();
                reader.as_mut().records().boxed()
            }
        }
    }
    /// Reads the BAM file header and returns the reference sequences.
    ///
    /// # Returns
    ///
    /// Reference sequences from the BAM header
    pub async fn read_sequences(&mut self) -> ReferenceSequences {
        match self {
            BamReader::Local(reader) => {
                let header = reader.read_header().unwrap();
                header.reference_sequences().clone()
            }
            BamReader::Remote(reader) => {
                let header = reader.as_mut().read_header().await.unwrap();
                header.reference_sequences().clone()
            }
        }
    }
}
