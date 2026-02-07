use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use datafusion_bio_format_core::record_filter::RecordFieldAccessor;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles_bam as bam;
use noodles_bam::Record;
use noodles_bam::io::Reader;
use noodles_bgzf::r#async::io::Reader as AsyncBgzfReader;
use noodles_bgzf::io::MultithreadedReader;
use noodles_sam as sam;
use noodles_sam::alignment::RecordBuf;
use noodles_sam::header::ReferenceSequences;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufReader, Error};
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
    bam::r#async::io::Reader<AsyncBgzfReader<StreamReader<FuturesBytesStream, bytes::Bytes>>>,
    Error,
> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
    let reader = bam::r#async::io::Reader::from(AsyncBgzfReader::new(StreamReader::new(stream)));
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
        .map(|f| MultithreadedReader::with_worker_count(NonZero::new(thread_num).unwrap(), f))
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
        bam::r#async::io::Reader<AsyncBgzfReader<StreamReader<FuturesBytesStream, bytes::Bytes>>>,
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
                BamReader::Remote(reader)
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
                reader.records().boxed()
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
                let header = reader.read_header().await.unwrap();
                header.reference_sequences().clone()
            }
        }
    }

    /// Reads the BAM file header and returns the full header.
    ///
    /// Must be called before `read_records()` since it consumes the header
    /// from the stream. This is the same constraint as `read_sequences()`.
    ///
    /// # Returns
    ///
    /// The full SAM header from the BAM file
    pub async fn read_header(&mut self) -> sam::Header {
        match self {
            BamReader::Local(reader) => reader.read_header().unwrap(),
            BamReader::Remote(reader) => reader.read_header().await.unwrap(),
        }
    }
}

/// Checks if a file path refers to a SAM file based on file extension.
///
/// # Arguments
///
/// * `path` - File path to check
///
/// # Returns
///
/// `true` if the path ends with `.sam` (case-insensitive)
pub fn is_sam_file(path: &str) -> bool {
    path.to_lowercase().ends_with(".sam")
}

/// Creates a local SAM reader.
///
/// # Arguments
///
/// * `file_path` - Path to the local SAM file
///
/// # Returns
///
/// A SAM reader wrapping a buffered file reader
pub fn get_local_sam_reader(file_path: String) -> Result<sam::io::Reader<BufReader<File>>, Error> {
    File::open(file_path)
        .map(BufReader::new)
        .map(sam::io::Reader::new)
}

/// An enum representing a SAM file reader.
///
/// SAM files are text-based alignment files. Currently only local reading
/// is supported since SAM files are not typically used with cloud storage.
pub enum SamReader {
    /// Local SAM reader with buffered I/O
    Local(sam::io::Reader<BufReader<File>>, sam::Header),
}

impl SamReader {
    /// Creates a new SAM reader for a local file.
    ///
    /// Reads the SAM header during construction.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the local SAM file
    ///
    /// # Returns
    ///
    /// A SamReader instance with the header already parsed
    pub fn new(file_path: String) -> Self {
        let mut reader = get_local_sam_reader(file_path).unwrap();
        let header = reader.read_header().unwrap();
        SamReader::Local(reader, header)
    }

    /// Returns the reference sequences from the SAM file header.
    ///
    /// # Returns
    ///
    /// Reference sequences from the SAM header
    pub fn read_sequences(&self) -> ReferenceSequences {
        match self {
            SamReader::Local(_, header) => header.reference_sequences().clone(),
        }
    }

    /// Returns a reference to the full SAM header.
    ///
    /// # Returns
    ///
    /// Reference to the SAM header (already parsed during construction)
    pub fn get_header(&self) -> &sam::Header {
        match self {
            SamReader::Local(_, header) => header,
        }
    }

    /// Reads SAM records as an async stream.
    ///
    /// Wraps the synchronous SAM record iterator as a futures stream
    /// for compatibility with the async processing pipeline.
    ///
    /// # Returns
    ///
    /// A boxed stream yielding SAM RecordBuf entries or IO errors
    pub fn read_records(&mut self) -> BoxStream<'_, Result<RecordBuf, Error>> {
        match self {
            SamReader::Local(reader, header) => stream::iter(reader.record_bufs(header)).boxed(),
        }
    }
}

/// A local indexed BAM reader for region-based queries.
///
/// Uses noodles' `IndexedReader::Builder` to support random-access queries using BAI indexes.
/// This is used when an index file is available and genomic region filters are present.
pub struct IndexedBamReader {
    reader: bam::io::IndexedReader<noodles_bgzf::io::Reader<File>>,
    header: sam::Header,
}

impl IndexedBamReader {
    /// Creates a new indexed BAM reader.
    ///
    /// # Arguments
    /// * `file_path` - Path to the BAM file
    /// * `index_path` - Path to the BAI index file
    pub fn new(file_path: &str, index_path: &str) -> Result<Self, Error> {
        let mut reader = bam::io::indexed_reader::Builder::default()
            .set_index(bam::bai::fs::read(index_path)?)
            .build_from_path(file_path)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;
        let header = reader.read_header()?;
        Ok(Self { reader, header })
    }

    /// Returns a reference to the SAM header.
    pub fn header(&self) -> &sam::Header {
        &self.header
    }

    /// Returns reference sequence names from the header.
    pub fn reference_names(&self) -> Vec<String> {
        self.header
            .reference_sequences()
            .keys()
            .map(|k| k.to_string())
            .collect()
    }

    /// Query records overlapping a genomic region.
    ///
    /// Returns an iterator of BAM records overlapping the specified region.
    /// The region string uses noodles format: "chr1:1000-2000" (1-based, closed).
    pub fn query(
        &mut self,
        region: &noodles_core::Region,
    ) -> Result<impl Iterator<Item = Result<Record, Error>> + '_, Error> {
        self.reader.query(&self.header, region)
    }
}

/// Estimate compressed byte sizes per region from a BAI index.
///
/// Reads the BAI index and estimates the compressed byte range for each genomic region
/// by examining the chunks in each reference's bins. Uses `VirtualPosition.compressed()`
/// to find the min/max compressed offsets per reference.
///
/// # Arguments
/// * `index_path` - Path to the BAI index file
/// * `regions` - Genomic regions to estimate sizes for
/// * `reference_names` - Reference sequence names from the header (in order)
/// * `reference_lengths` - Optional reference sequence lengths (for sub-region splitting)
pub fn estimate_sizes_from_bai(
    index_path: &str,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    reference_names: &[String],
    reference_lengths: &[u64],
) -> Vec<datafusion_bio_format_core::partition_balancer::RegionSizeEstimate> {
    use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;

    let index = match bam::bai::fs::read(index_path) {
        Ok(idx) => idx,
        Err(e) => {
            log::debug!("Failed to read BAI index for size estimation: {}", e);
            // Return uniform estimates as fallback
            return regions
                .iter()
                .map(|r| RegionSizeEstimate {
                    region: r.clone(),
                    estimated_bytes: 1,
                    contig_length: None,
                })
                .collect();
        }
    };

    let ref_name_to_idx: std::collections::HashMap<&str, usize> = reference_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    regions
        .iter()
        .map(|region| {
            let ref_idx = ref_name_to_idx.get(region.chrom.as_str()).copied();
            let estimated_bytes = ref_idx
                .and_then(|idx| {
                    index.reference_sequences().get(idx).map(|ref_seq| {
                        let mut min_offset = u64::MAX;
                        let mut max_offset = 0u64;
                        for (_bin_id, bin) in ref_seq.bins() {
                            for chunk in bin.chunks() {
                                let start = chunk.start().compressed();
                                let end = chunk.end().compressed();
                                min_offset = min_offset.min(start);
                                max_offset = max_offset.max(end);
                            }
                        }
                        max_offset.saturating_sub(min_offset)
                    })
                })
                .unwrap_or(1);

            let contig_length = ref_idx
                .and_then(|idx| reference_lengths.get(idx).copied())
                .filter(|&len| len > 0);

            RegionSizeEstimate {
                region: region.clone(),
                estimated_bytes,
                contig_length,
            }
        })
        .collect()
}

/// Record field accessor for BAM records, used for record-level filter evaluation.
///
/// This struct holds pre-extracted field values from a BAM record
/// for efficient access during filter evaluation.
pub struct BamRecordFields {
    /// Chromosome name
    pub chrom: Option<String>,
    /// Start position (in the output coordinate system)
    pub start: Option<u32>,
    /// End position (in the output coordinate system)
    pub end: Option<u32>,
    /// Mapping quality
    pub mapping_quality: Option<u32>,
    /// SAM flags
    pub flags: u32,
}

impl RecordFieldAccessor for BamRecordFields {
    fn get_string_field(&self, name: &str) -> Option<String> {
        match name {
            "chrom" => self.chrom.clone(),
            _ => None,
        }
    }

    fn get_u32_field(&self, name: &str) -> Option<u32> {
        match name {
            "start" => self.start,
            "end" => self.end,
            "mapping_quality" => self.mapping_quality,
            "flags" => Some(self.flags),
            _ => None,
        }
    }

    fn get_f32_field(&self, _name: &str) -> Option<f32> {
        None
    }

    fn get_f64_field(&self, _name: &str) -> Option<f64> {
        None
    }
}
