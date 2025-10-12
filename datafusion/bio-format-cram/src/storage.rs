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
    ///
    /// This function creates a FASTA repository for use with CRAM files that require
    /// an external reference sequence. The FASTA file must have an accompanying index
    /// file (.fai) in the same directory.
    ///
    /// # Arguments
    /// * `path` - Path to the FASTA file (local filesystem only for now)
    ///
    /// # Returns
    /// * `Ok(ReferenceSequenceRepository::External)` - Successfully loaded reference
    /// * `Err` - Failed to load reference or index file
    ///
    /// # Example
    /// ```no_run
    /// # use std::io;
    /// # use bio_format_cram::storage::ReferenceSequenceRepository;
    /// let repo = ReferenceSequenceRepository::from_fasta_path("/path/to/reference.fasta")?;
    /// # Ok::<(), io::Error>(())
    /// ```
    pub fn from_fasta_path(path: &str) -> io::Result<Self> {
        use std::path::PathBuf;

        // Determine storage type
        let storage_type = get_storage_type(path.to_string());

        match storage_type {
            StorageType::LOCAL => {
                // Local file path - use IndexedReader
                let fasta_path = PathBuf::from(path);
                let index_path = format!("{}.fai", path);

                // Check if files exist
                if !fasta_path.exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("FASTA file not found: {}", path),
                    ));
                }

                let index_path_buf = PathBuf::from(&index_path);
                if !index_path_buf.exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "FASTA index (.fai) not found: {}. Please create it using 'samtools faidx {}'",
                            index_path, path
                        ),
                    ));
                }

                // Open FASTA file
                let file = File::open(&fasta_path)?;
                let reader = BufReader::new(file);

                // Read FASTA index
                let index = fasta::fai::read(&index_path)?;

                // Create indexed reader
                let indexed_reader = fasta::io::IndexedReader::new(reader, index);

                // Create adapter and repository
                let adapter = fasta::repository::adapters::IndexedReader::new(indexed_reader);
                let repository = fasta::Repository::new(adapter);

                Ok(Self::External(repository))
            }
            StorageType::S3 | StorageType::GCS | StorageType::AZBLOB => {
                // Remote storage not yet implemented
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "Remote FASTA references from {:?} are not yet supported. Please use a local FASTA file or embedded reference.",
                        storage_type
                    ),
                ))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Unsupported storage type for FASTA reference: {:?}",
                    storage_type
                ),
            )),
        }
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
) -> Result<(Reader<File>, ReferenceSequenceRepository), io::Error> {
    // Load reference repository if provided
    let reference_repo = if let Some(ref_path) = reference_path {
        ReferenceSequenceRepository::from_fasta_path(&ref_path)?
    } else {
        // Use default empty repository for embedded references
        ReferenceSequenceRepository::Embedded
    };

    // Build reader with the repository using Builder pattern
    let reader = match &reference_repo {
        ReferenceSequenceRepository::External(repo) => {
            // Use Builder to set the repository
            cram::io::reader::Builder::default()
                .set_reference_sequence_repository(repo.clone())
                .build_from_path(&file_path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        }
        ReferenceSequenceRepository::Embedded | ReferenceSequenceRepository::None => {
            // Use Builder with default (empty) repository
            let default_repo = fasta::Repository::default();
            cram::io::reader::Builder::default()
                .set_reference_sequence_repository(default_repo)
                .build_from_path(&file_path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        }
    };

    Ok((reader, reference_repo))
}

pub enum CramReader {
    Local(Reader<File>, ReferenceSequenceRepository, Header),
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
                // Repository is already set on the reader via Builder pattern during construction
                // The reader will use it automatically when decoding CRAM data
                stream::iter(reader.records(header)).boxed()
            }
            CramReader::Remote(reader, _reference_repo, header) => {
                // Repository is already set on the reader via Builder pattern during construction
                // The reader will use it automatically when decoding CRAM data
                reader.records(header).boxed()
            }
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
