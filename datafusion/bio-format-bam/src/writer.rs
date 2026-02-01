//! Writer for BAM/SAM files with compression support
//!
//! This module provides writers for BAM/SAM files with support for:
//! - Plain SAM (uncompressed)
//! - BGZF-compressed BAM (default, recommended)

use datafusion::common::{DataFusionError, Result};
use noodles_bam as bam;
use noodles_bgzf::io::Writer as BgzfWriter;
use noodles_sam as sam;
use noodles_sam::alignment::io::Write as AlignmentWrite;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

/// Compression type for BAM output files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BamCompressionType {
    /// Plain SAM format (uncompressed text)
    Plain,
    /// BGZF-compressed BAM format (binary, default)
    #[default]
    Bgzf,
}

impl BamCompressionType {
    /// Determines compression type from file extension
    ///
    /// # Arguments
    ///
    /// * `path` - File path to analyze
    ///
    /// # Returns
    ///
    /// The detected compression type based on extension:
    /// - `.sam` -> Plain SAM
    /// - `.bam` (or anything else) -> BGZF BAM (default)
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.ends_with(".sam") {
            BamCompressionType::Plain
        } else {
            // Default to BAM for .bam or any other extension
            BamCompressionType::Bgzf
        }
    }
}

/// A unified writer for BAM/SAM files supporting multiple compression formats
///
/// This enum provides a single interface for writing alignment data regardless
/// of the underlying format. Use `BamCompressionType::from_path()` to automatically
/// detect the appropriate format from the file extension.
pub enum BamLocalWriter {
    /// Writer for uncompressed SAM files
    Sam(sam::io::Writer<BufWriter<File>>),
    /// Writer for BGZF-compressed BAM files (recommended, BGZF handled internally by BAM writer)
    Bam(bam::io::Writer<BgzfWriter<BufWriter<File>>>),
}

impl BamLocalWriter {
    /// Creates a new BAM/SAM writer for the given path with automatic compression detection
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    ///
    /// # Returns
    ///
    /// A new `BamLocalWriter` configured for the appropriate format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let compression = BamCompressionType::from_path(&path);
        Self::with_compression(path, compression)
    }

    /// Creates a new BAM/SAM writer with explicit compression type
    ///
    /// # Arguments
    ///
    /// * `path` - The output file path
    /// * `compression` - The compression format to use
    ///
    /// # Returns
    ///
    /// A new `BamLocalWriter` configured for the specified format
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created
    pub fn with_compression<P: AsRef<Path>>(
        path: P,
        compression: BamCompressionType,
    ) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create output file: {}", e))
        })?;
        let buf_writer = BufWriter::new(file);

        match compression {
            BamCompressionType::Plain => Ok(BamLocalWriter::Sam(sam::io::Writer::new(buf_writer))),
            BamCompressionType::Bgzf => {
                // BAM writer handles BGZF compression internally
                Ok(BamLocalWriter::Bam(bam::io::Writer::new(buf_writer)))
            }
        }
    }

    /// Writes the SAM/BAM header to the file
    ///
    /// This must be called before writing any records.
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails
    pub fn write_header(&mut self, header: &sam::Header) -> Result<()> {
        match self {
            BamLocalWriter::Sam(writer) => writer.write_header(header).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write SAM header: {}", e))
            }),
            BamLocalWriter::Bam(writer) => writer.write_header(header).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write BAM header: {}", e))
            }),
        }
    }

    /// Writes a single alignment record to the file
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for BAM encoding)
    /// * `record` - The alignment record to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails
    pub fn write_record(
        &mut self,
        header: &sam::Header,
        record: &sam::alignment::RecordBuf,
    ) -> Result<()> {
        match self {
            BamLocalWriter::Sam(writer) => {
                writer.write_alignment_record(header, record).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to write SAM record: {}", e))
                })
            }
            BamLocalWriter::Bam(writer) => {
                writer.write_alignment_record(header, record).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to write BAM record: {}", e))
                })
            }
        }
    }

    /// Writes multiple alignment records to the file
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for BAM encoding)
    /// * `records` - Slice of alignment records to write
    ///
    /// # Errors
    ///
    /// Returns an error if writing any record fails
    pub fn write_records(
        &mut self,
        header: &sam::Header,
        records: &[sam::alignment::RecordBuf],
    ) -> Result<()> {
        for record in records {
            self.write_record(header, record)?;
        }
        Ok(())
    }

    /// Finishes writing and closes the file
    ///
    /// For compressed formats, this properly finalizes the compression stream.
    /// This consumes the writer.
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for BAM finish)
    ///
    /// # Errors
    ///
    /// Returns an error if finishing fails
    pub fn finish(mut self, header: &sam::Header) -> Result<()> {
        match self {
            BamLocalWriter::Sam(ref mut writer) => {
                // SAM writer needs header for finish
                writer.finish(header).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish SAM stream: {}", e))
                })?;
                Ok(())
            }
            BamLocalWriter::Bam(ref mut writer) => {
                // BAM writer needs header for finish
                writer.finish(header).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish BAM stream: {}", e))
                })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_compression_type_from_path() {
        assert_eq!(
            BamCompressionType::from_path("test.sam"),
            BamCompressionType::Plain
        );
        assert_eq!(
            BamCompressionType::from_path("test.bam"),
            BamCompressionType::Bgzf
        );
        assert_eq!(
            BamCompressionType::from_path("test.SAM"),
            BamCompressionType::Plain
        );
        assert_eq!(
            BamCompressionType::from_path("test.BAM"),
            BamCompressionType::Bgzf
        );
    }

    #[test]
    fn test_create_sam_writer() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".sam").unwrap();
        let path = temp_file.path();

        let writer = BamLocalWriter::new(path)?;
        match writer {
            BamLocalWriter::Sam(_) => Ok(()),
            _ => Err(DataFusionError::Execution(
                "Expected SAM writer".to_string(),
            )),
        }
    }

    #[test]
    fn test_create_bam_writer() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".bam").unwrap();
        let path = temp_file.path();

        let writer = BamLocalWriter::new(path)?;
        match writer {
            BamLocalWriter::Bam(_) => Ok(()),
            _ => Err(DataFusionError::Execution(
                "Expected BAM writer".to_string(),
            )),
        }
    }

    #[test]
    fn test_write_empty_header() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".sam").unwrap();
        let path = temp_file.path();

        let header = sam::Header::default();
        let mut writer = BamLocalWriter::new(path)?;
        writer.write_header(&header)?;
        writer.finish(&header)?;

        Ok(())
    }
}
