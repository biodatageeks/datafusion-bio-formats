//! Writer for FASTQ files with compression support
//!
//! This module provides writers for FASTQ files with support for:
//! - Uncompressed (plain) FASTQ
//! - GZIP compression
//! - BGZF compression (default, recommended)

use datafusion::common::{DataFusionError, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use noodles_bgzf as bgzf;
use noodles_fastq as fastq;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Compression type for FASTQ output files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FastqCompressionType {
    /// No compression (plain text FASTQ)
    Plain,
    /// Standard GZIP compression
    Gzip,
    /// BGZF compression (block-gzipped, allows random access)
    /// This is the default and recommended compression format
    #[default]
    Bgzf,
}

impl FastqCompressionType {
    /// Determines compression type from file extension
    ///
    /// # Arguments
    ///
    /// * `path` - File path to analyze
    ///
    /// # Returns
    ///
    /// The detected compression type based on extension:
    /// - `.bgz` or `.bgzf` -> BGZF
    /// - `.gz` (but not `.bgz`) -> GZIP
    /// - Otherwise -> Plain
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.ends_with(".bgz") || path_str.ends_with(".bgzf") {
            FastqCompressionType::Bgzf
        } else if path_str.ends_with(".gz") {
            FastqCompressionType::Gzip
        } else {
            FastqCompressionType::Plain
        }
    }
}

/// A unified writer for FASTQ files supporting multiple compression formats
///
/// This enum provides a single interface for writing FASTQ data regardless
/// of the underlying compression format. Use `FastqCompressionType::from_path()`
/// to automatically detect the appropriate format from the file extension.
pub enum FastqLocalWriter {
    /// Writer for uncompressed FASTQ files
    Plain(fastq::io::Writer<BufWriter<File>>),
    /// Writer for GZIP-compressed FASTQ files
    Gzip(fastq::io::Writer<GzEncoder<BufWriter<File>>>),
    /// Writer for BGZF-compressed FASTQ files (recommended)
    Bgzf(fastq::io::Writer<bgzf::Writer<BufWriter<File>>>),
}

impl FastqLocalWriter {
    /// Creates a new FASTQ writer for the given path with automatic compression detection
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    ///
    /// # Returns
    ///
    /// A new `FastqLocalWriter` configured for the appropriate compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let compression = FastqCompressionType::from_path(&path);
        Self::with_compression(path, compression)
    }

    /// Creates a new FASTQ writer with explicit compression type
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    /// * `compression` - The compression format to use
    ///
    /// # Returns
    ///
    /// A new `FastqLocalWriter` configured for the specified compression format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn with_compression<P: AsRef<Path>>(
        path: P,
        compression: FastqCompressionType,
    ) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create output file: {}", e))
        })?;
        let buf_writer = BufWriter::new(file);

        match compression {
            FastqCompressionType::Plain => {
                let writer = fastq::io::Writer::new(buf_writer);
                Ok(FastqLocalWriter::Plain(writer))
            }
            FastqCompressionType::Gzip => {
                let encoder = GzEncoder::new(buf_writer, Compression::default());
                let writer = fastq::io::Writer::new(encoder);
                Ok(FastqLocalWriter::Gzip(writer))
            }
            FastqCompressionType::Bgzf => {
                let bgzf_writer = bgzf::Writer::new(buf_writer);
                let writer = fastq::io::Writer::new(bgzf_writer);
                Ok(FastqLocalWriter::Bgzf(writer))
            }
        }
    }

    /// Writes a single FASTQ record to the file
    ///
    /// # Arguments
    ///
    /// * `record` - The FASTQ record to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails
    pub fn write_record(&mut self, record: &fastq::Record) -> Result<()> {
        match self {
            FastqLocalWriter::Plain(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTQ record: {}", e))
            }),
            FastqLocalWriter::Gzip(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTQ record: {}", e))
            }),
            FastqLocalWriter::Bgzf(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTQ record: {}", e))
            }),
        }
    }

    /// Writes multiple FASTQ records to the file
    ///
    /// # Arguments
    ///
    /// * `records` - Slice of FASTQ records to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing any record fails
    pub fn write_records(&mut self, records: &[fastq::Record]) -> Result<()> {
        for record in records {
            self.write_record(record)?;
        }
        Ok(())
    }

    /// Flushes the writer, ensuring all data is written to the file
    ///
    /// This should be called before the writer is dropped to ensure
    /// all data is properly written, especially for compressed formats.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails
    pub fn flush(&mut self) -> Result<()> {
        match self {
            FastqLocalWriter::Plain(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {}", e))),
            FastqLocalWriter::Gzip(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {}", e))),
            FastqLocalWriter::Bgzf(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {}", e))),
        }
    }

    /// Finishes writing and closes the file
    ///
    /// For compressed formats, this properly finalizes the compression stream.
    /// This consumes the writer.
    ///
    /// # Errors
    ///
    /// Returns an error if finishing fails
    pub fn finish(self) -> Result<()> {
        match self {
            FastqLocalWriter::Plain(writer) => {
                let mut buf_writer = writer.into_inner();
                buf_writer.flush().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to flush writer: {}", e))
                })
            }
            FastqLocalWriter::Gzip(writer) => {
                let encoder = writer.into_inner();
                encoder.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish GZIP stream: {}", e))
                })?;
                Ok(())
            }
            FastqLocalWriter::Bgzf(writer) => {
                let bgzf_writer = writer.into_inner();
                bgzf_writer.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish BGZF stream: {}", e))
                })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_fastq::record::Definition;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_compression_type_from_path() {
        assert_eq!(
            FastqCompressionType::from_path("test.fastq"),
            FastqCompressionType::Plain
        );
        assert_eq!(
            FastqCompressionType::from_path("test.fastq.gz"),
            FastqCompressionType::Gzip
        );
        assert_eq!(
            FastqCompressionType::from_path("test.fastq.bgz"),
            FastqCompressionType::Bgzf
        );
        assert_eq!(
            FastqCompressionType::from_path("test.fastq.bgzf"),
            FastqCompressionType::Bgzf
        );
        assert_eq!(
            FastqCompressionType::from_path("TEST.FASTQ.GZ"),
            FastqCompressionType::Gzip
        );
    }

    #[test]
    fn test_write_plain_fastq() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fastq").unwrap();
        let path = temp_file.path();

        let record = fastq::Record::new(
            Definition::new("seq1", "description"),
            b"ACGT".to_vec(),
            b"IIII".to_vec(),
        );

        {
            let mut writer = FastqLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        // Read back and verify
        let mut content = String::new();
        let mut file = File::open(path).unwrap();
        file.read_to_string(&mut content).unwrap();

        assert!(content.contains("@seq1"));
        assert!(content.contains("ACGT"));
        assert!(content.contains("IIII"));

        Ok(())
    }

    #[test]
    fn test_write_gzip_fastq() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fastq.gz").unwrap();
        let path = temp_file.path();

        let record = fastq::Record::new(
            Definition::new("seq1", ""),
            b"ACGT".to_vec(),
            b"IIII".to_vec(),
        );

        {
            let mut writer = FastqLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        // Verify file exists and is not empty
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        Ok(())
    }

    #[test]
    fn test_write_bgzf_fastq() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fastq.bgz").unwrap();
        let path = temp_file.path();

        let record = fastq::Record::new(
            Definition::new("seq1", ""),
            b"ACGT".to_vec(),
            b"IIII".to_vec(),
        );

        {
            let mut writer = FastqLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        // Verify file exists and is not empty
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        Ok(())
    }
}
