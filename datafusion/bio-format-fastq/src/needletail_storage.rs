use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use needletail::{parse_fastx_file, parser::SequenceRecord};
use noodles_bgzf as bgzf;
use std::fs::File;
use std::io::{BufReader, Cursor, Error, Read};
use tokio_util::io::StreamReader;

pub struct NeedletailRecord {
    pub name: String,
    pub description: Option<String>,
    pub sequence: String,
    pub quality_scores: String,
}

impl NeedletailRecord {
    #[inline]
    fn from_needletail_record(record: SequenceRecord<'_>) -> Result<Self, Error> {
        // Use unsafe string conversion for performance (needletail guarantees valid UTF-8)
        let full_name = unsafe { std::str::from_utf8_unchecked(record.id()) };

        // Fast string parsing without intermediate allocations
        let (name, description) = if let Some(space_pos) = full_name.find(' ') {
            let name_part = unsafe { full_name.get_unchecked(..space_pos) };
            let desc_part = unsafe { full_name.get_unchecked(space_pos + 1..) }.trim();
            let description = if desc_part.is_empty() {
                None
            } else {
                Some(desc_part.to_owned())
            };
            (name_part.to_owned(), description)
        } else {
            (full_name.to_owned(), None)
        };

        // Use unsafe string conversion for sequence (needletail guarantees valid UTF-8)
        let seq_bytes = record.seq();
        let sequence = unsafe { std::str::from_utf8_unchecked(&seq_bytes) }.to_owned();

        // Use unsafe string conversion for quality scores
        let quality_scores = if let Some(qual) = record.qual() {
            unsafe { std::str::from_utf8_unchecked(&qual) }.to_owned()
        } else {
            String::new()
        };

        Ok(NeedletailRecord {
            name,
            description,
            sequence,
            quality_scores,
        })
    }
}

pub enum NeedletailRemoteReader {
    PLAIN(Box<dyn Read + Send + Sync>),
    GZIP(Box<dyn Read + Send + Sync>),
    BGZF(Box<dyn Read + Send + Sync>),
}

impl NeedletailRemoteReader {
    pub async fn new(
        file_path: String,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        log::info!("Creating needletail remote reader for: {}", file_path);
        let compression_type =
            get_compression_type(file_path.clone(), None, object_storage_options.clone())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let stream = get_remote_stream(file_path.clone(), object_storage_options, None).await?;
        let mut bytes = Vec::new();
        let mut stream_reader = StreamReader::new(stream);

        // Read entire stream into memory for needletail
        tokio::io::copy(&mut stream_reader, &mut bytes).await?;

        match compression_type {
            CompressionType::BGZF => {
                let reader: Box<dyn Read + Send + Sync> = Box::new(Cursor::new(bytes));
                Ok(NeedletailRemoteReader::BGZF(reader))
            }
            CompressionType::GZIP => {
                let reader: Box<dyn Read + Send + Sync> = Box::new(Cursor::new(bytes));
                Ok(NeedletailRemoteReader::GZIP(reader))
            }
            CompressionType::NONE => {
                let reader: Box<dyn Read + Send + Sync> = Box::new(Cursor::new(bytes));
                Ok(NeedletailRemoteReader::PLAIN(reader))
            }
            _ => unimplemented!(
                "Unsupported compression type for needletail FASTQ reader: {:?}",
                compression_type
            ),
        }
    }

    pub fn read_records(&mut self) -> BoxStream<'_, Result<NeedletailRecord, Error>> {
        match self {
            NeedletailRemoteReader::PLAIN(reader) => {
                let mut data = Vec::new();
                match reader.read_to_end(&mut data) {
                    Ok(_) => Self::parse_fastx_from_bytes(data),
                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                }
            }
            NeedletailRemoteReader::GZIP(reader) => {
                let mut data = Vec::new();
                match reader.read_to_end(&mut data) {
                    Ok(_) => Self::parse_fastx_from_bytes(data),
                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                }
            }
            NeedletailRemoteReader::BGZF(reader) => {
                let mut data = Vec::new();
                match reader.read_to_end(&mut data) {
                    Ok(_) => Self::parse_fastx_from_bytes(data),
                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                }
            }
        }
    }

    fn parse_fastx_from_bytes(
        data: Vec<u8>,
    ) -> BoxStream<'static, Result<NeedletailRecord, Error>> {
        let mut records = Vec::new();

        // Create a temporary file to work with needletail's file-based API
        match std::fs::write("/tmp/needletail_temp.fastq", &data) {
            Ok(()) => {
                match parse_fastx_file("/tmp/needletail_temp.fastq") {
                    Ok(mut reader) => {
                        while let Some(record_result) = reader.next() {
                            match record_result {
                                Ok(rec) => match NeedletailRecord::from_needletail_record(rec) {
                                    Ok(nr) => records.push(Ok(nr)),
                                    Err(e) => records.push(Err(e)),
                                },
                                Err(e) => records.push(Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                ))),
                            }
                        }
                    }
                    Err(e) => records.push(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))),
                }
                // Clean up temp file
                let _ = std::fs::remove_file("/tmp/needletail_temp.fastq");
            }
            Err(e) => records.push(Err(e)),
        }

        stream::iter(records).boxed()
    }
}

pub enum NeedletailLocalReader {
    PLAIN(String),       // Store file path for plain files
    GZIP(String),        // Store file path for gzip files
    BGZF(String, usize), // Store file path and thread count for BGZF files
}

// Wrapper for multi-threaded BGZF reader with needletail
pub struct NeedletailBgzfReader {
    reader: bgzf::MultithreadedReader<File>,
}

fn get_needletail_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<NeedletailBgzfReader, Error> {
    let file = File::open(file_path)?;
    let reader = bgzf::MultithreadedReader::with_worker_count(
        std::num::NonZero::new(thread_num).unwrap(),
        file,
    );
    Ok(NeedletailBgzfReader { reader })
}

impl NeedletailLocalReader {
    pub async fn new(
        file_path: String,
        thread_num: usize,
        object_storage_options: ObjectStorageOptions,
    ) -> Result<Self, Error> {
        log::info!(
            "Creating needletail local reader for: {} with {} threads",
            file_path,
            thread_num
        );
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.compression_type.clone(),
            object_storage_options.clone(),
        )
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        match compression_type {
            CompressionType::BGZF => {
                // Store path and thread count for lazy BGZF reader creation
                Ok(NeedletailLocalReader::BGZF(file_path, thread_num))
            }
            CompressionType::GZIP => Ok(NeedletailLocalReader::GZIP(file_path)),
            CompressionType::NONE => Ok(NeedletailLocalReader::PLAIN(file_path)),
            _ => unimplemented!(
                "Unsupported compression type for needletail FASTQ reader: {:?}",
                compression_type
            ),
        }
    }

    pub fn read_records_sync(&self) -> impl Iterator<Item = Result<NeedletailRecord, Error>> + '_ {
        match self {
            NeedletailLocalReader::PLAIN(path) => {
                log::debug!("Starting needletail sync read of plain file: {}", path);
                NeedletailSyncIterator::new_from_path(path.clone())
            }
            NeedletailLocalReader::GZIP(path) => {
                log::debug!("Starting needletail sync read of gzip file: {}", path);
                NeedletailSyncIterator::new_from_path(path.clone())
            }
            NeedletailLocalReader::BGZF(path, thread_count) => {
                log::debug!(
                    "Starting needletail sync read of BGZF file: {} with {} threads",
                    path,
                    thread_count
                );
                NeedletailSyncIterator::new_from_bgzf_path(path.clone(), *thread_count)
            }
        }
    }

    // Keep async version for compatibility but mark as deprecated
    #[deprecated(note = "Use read_records_sync for better performance with local files")]
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<NeedletailRecord, Error>> {
        let records: Vec<_> = self.read_records_sync().collect();
        stream::iter(records).boxed()
    }

    fn parse_fastx_from_bytes(
        data: Vec<u8>,
    ) -> BoxStream<'static, Result<NeedletailRecord, Error>> {
        let mut records = Vec::new();

        // Create a temporary file to work with needletail's file-based API
        match std::fs::write("/tmp/needletail_temp.fastq", &data) {
            Ok(()) => {
                match parse_fastx_file("/tmp/needletail_temp.fastq") {
                    Ok(mut reader) => {
                        while let Some(record_result) = reader.next() {
                            match record_result {
                                Ok(rec) => match NeedletailRecord::from_needletail_record(rec) {
                                    Ok(nr) => records.push(Ok(nr)),
                                    Err(e) => records.push(Err(e)),
                                },
                                Err(e) => records.push(Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                ))),
                            }
                        }
                    }
                    Err(e) => records.push(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))),
                }
                // Clean up temp file
                let _ = std::fs::remove_file("/tmp/needletail_temp.fastq");
            }
            Err(e) => records.push(Err(e)),
        }

        stream::iter(records).boxed()
    }
}

// Multi-threaded BGZF reader for needletail
pub struct NeedletailBgzfMultiThreadedReader {
    file_path: String,
    thread_num: usize,
}

impl NeedletailBgzfMultiThreadedReader {
    pub fn new(file_path: String, thread_num: usize) -> Self {
        log::info!(
            "Creating needletail multi-threaded BGZF reader for: {} with {} threads",
            file_path,
            thread_num
        );
        Self {
            file_path,
            thread_num,
        }
    }

    pub async fn read_records(&self) -> BoxStream<'_, Result<NeedletailRecord, Error>> {
        let mut records = Vec::new();

        // Use needletail's file-based API directly for multi-threaded BGZF reading
        match parse_fastx_file(&self.file_path) {
            Ok(mut reader) => {
                while let Some(record_result) = reader.next() {
                    match record_result {
                        Ok(rec) => match NeedletailRecord::from_needletail_record(rec) {
                            Ok(nr) => records.push(Ok(nr)),
                            Err(e) => records.push(Err(e)),
                        },
                        Err(e) => records.push(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        ))),
                    }
                }
            }
            Err(e) => records.push(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }

        stream::iter(records).boxed()
    }
}

// Iterator that handles both file paths and BGZF readers
pub enum NeedletailSyncIterator {
    FromPath(Box<dyn Iterator<Item = Result<NeedletailRecord, Error>> + Send>),
    FromBgzf(Box<dyn Iterator<Item = Result<NeedletailRecord, Error>> + Send>),
}

impl NeedletailSyncIterator {
    pub fn new_from_path(file_path: String) -> Self {
        match parse_fastx_file(&file_path) {
            Ok(mut reader) => {
                let iter = std::iter::from_fn(move || {
                    reader
                        .next()
                        .map(|record_result| Self::convert_record(record_result))
                });
                Self::FromPath(Box::new(iter))
            }
            Err(e) => {
                let error = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                let iter = std::iter::once(Err(error));
                Self::FromPath(Box::new(iter))
            }
        }
    }

    pub fn new_from_bgzf_path(file_path: String, thread_count: usize) -> Self {
        match get_needletail_bgzf_reader(file_path, thread_count) {
            Ok(bgzf_reader) => {
                // Take ownership of the BGZF reader
                let buf_reader = BufReader::new(bgzf_reader.reader);

                match needletail::parser::parse_fastx_reader(buf_reader) {
                    Ok(mut reader) => {
                        let iter = std::iter::from_fn(move || {
                            reader
                                .next()
                                .map(|record_result| Self::convert_record(record_result))
                        });
                        Self::FromBgzf(Box::new(iter))
                    }
                    Err(e) => {
                        let error = std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to create needletail reader from BGZF: {}", e),
                        );
                        let iter = std::iter::once(Err(error));
                        Self::FromBgzf(Box::new(iter))
                    }
                }
            }
            Err(e) => {
                let iter = std::iter::once(Err(e));
                Self::FromBgzf(Box::new(iter))
            }
        }
    }

    #[inline]
    fn convert_record(
        record_result: Result<SequenceRecord, needletail::errors::ParseError>,
    ) -> Result<NeedletailRecord, Error> {
        match record_result {
            Ok(rec) => {
                // Inline optimized conversion to avoid function call overhead
                let full_name = unsafe { std::str::from_utf8_unchecked(rec.id()) };
                let (name, description) = if let Some(space_pos) = full_name.find(' ') {
                    let name_part = unsafe { full_name.get_unchecked(..space_pos) };
                    let desc_part = unsafe { full_name.get_unchecked(space_pos + 1..) }.trim();
                    (
                        name_part.to_string(),
                        if desc_part.is_empty() {
                            None
                        } else {
                            Some(desc_part.to_string())
                        },
                    )
                } else {
                    (full_name.to_string(), None)
                };

                let seq_bytes = rec.seq();
                let sequence = unsafe { std::str::from_utf8_unchecked(&seq_bytes) }.to_string();

                let quality_scores = if let Some(qual) = rec.qual() {
                    unsafe { std::str::from_utf8_unchecked(&qual) }.to_string()
                } else {
                    String::new()
                };

                Ok(NeedletailRecord {
                    name,
                    description,
                    sequence,
                    quality_scores,
                })
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("needletail parse error: {}", e),
            )),
        }
    }
}

impl Iterator for NeedletailSyncIterator {
    type Item = Result<NeedletailRecord, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::FromPath(iter) => iter.next(),
            Self::FromBgzf(iter) => iter.next(),
        }
    }
}
