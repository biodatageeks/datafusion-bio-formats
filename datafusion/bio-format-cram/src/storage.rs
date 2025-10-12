use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles_cram as cram;
use noodles_cram::io::Reader;
use noodles_fasta as fasta;
use noodles_sam::Header;
use noodles_sam::alignment::RecordBuf;
use noodles_sam::header::ReferenceSequences;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{self, BufReader};
use tokio_util::io::StreamReader;

/// Reference sequence repository for CRAM decoding
pub enum ReferenceSequenceRepository {
    /// Use embedded reference from CRAM file
    Embedded,
    /// Use external FASTA file (local path)
    External(fasta::Repository),
    /// No reference available (limited functionality)
    None,
}

impl ReferenceSequenceRepository {
    /// Load reference from external FASTA file
    pub fn from_fasta_path(_path: &str) -> io::Result<Self> {
        // TODO: Implement external FASTA reference support
        // For now, return Embedded mode which will use the reference embedded in the CRAM file
        Ok(Self::Embedded)
    }
}

pub async fn get_remote_cram_reader(
    file_path: String,
    reference_path: Option<String>,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    (
        cram::r#async::io::Reader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
        ReferenceSequenceRepository,
    ),
    io::Error,
> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = cram::r#async::io::Reader::new(StreamReader::new(stream));

    // Load reference if provided
    let reference_repo = if let Some(ref_path) = reference_path {
        ReferenceSequenceRepository::from_fasta_path(&ref_path)?
    } else {
        // Try to use embedded reference
        ReferenceSequenceRepository::Embedded
    };

    Ok((reader, reference_repo))
}

pub async fn get_local_cram_reader(
    file_path: String,
    reference_path: Option<String>,
) -> Result<(Reader<BufReader<File>>, ReferenceSequenceRepository), io::Error> {
    let file = File::open(&file_path)?;
    let reader = cram::io::Reader::new(BufReader::new(file));

    // Load reference if provided
    let reference_repo = if let Some(ref_path) = reference_path {
        ReferenceSequenceRepository::from_fasta_path(&ref_path)?
    } else {
        // Try to use embedded reference
        ReferenceSequenceRepository::Embedded
    };

    Ok((reader, reference_repo))
}

pub enum CramReader {
    Local(Reader<BufReader<File>>, ReferenceSequenceRepository, Header),
    Remote(
        cram::r#async::io::Reader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
        ReferenceSequenceRepository,
        Header,
    ),
}

impl CramReader {
    pub async fn new(
        file_path: String,
        reference_path: Option<String>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        match storage_type {
            StorageType::LOCAL => {
                let (mut reader, reference_repo) = get_local_cram_reader(file_path, reference_path)
                    .await
                    .unwrap();
                let header = reader.read_header().unwrap();
                CramReader::Local(reader, reference_repo, header)
            }
            StorageType::AZBLOB | StorageType::GCS | StorageType::S3 => {
                let object_storage_options = object_storage_options
                    .expect("ObjectStorageOptions must be provided for remote storage");
                let (mut reader, reference_repo) =
                    get_remote_cram_reader(file_path, reference_path, object_storage_options)
                        .await
                        .unwrap();
                let header = reader.read_header().await.unwrap();
                CramReader::Remote(reader, reference_repo, header)
            }
            _ => panic!("Unsupported storage type for CRAM file: {:?}", storage_type),
        }
    }

    pub async fn read_records<'a>(&'a mut self) -> BoxStream<'a, Result<RecordBuf, io::Error>> {
        match self {
            CramReader::Local(reader, _reference_repo, header) => {
                stream::iter(reader.records(header)).boxed()
            }
            CramReader::Remote(reader, _reference_repo, header) => reader.records(header).boxed(),
        }
    }

    pub fn get_header(&self) -> &Header {
        match self {
            CramReader::Local(_, _, header) => header,
            CramReader::Remote(_, _, header) => header,
        }
    }

    pub fn get_sequences(&self) -> &ReferenceSequences {
        match self {
            CramReader::Local(_, _, header) => header.reference_sequences(),
            CramReader::Remote(_, _, header) => header.reference_sequences(),
        }
    }
}
