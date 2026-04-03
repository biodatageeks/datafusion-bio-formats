//! Writer for FASTA files with compression support
//!
//! This module provides writers for FASTA files with support for:
//! - Uncompressed (plain) FASTA
//! - GZIP compression
//! - BGZF compression (default, recommended)

use datafusion::common::{DataFusionError, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use noodles_bgzf as bgzf;
use noodles_fasta as fasta;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Compression type for FASTA output files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FastaCompressionType {
    /// No compression (plain text FASTA)
    Plain,
    /// Standard GZIP compression
    Gzip,
    /// BGZF compression (block-gzipped, allows random access)
    /// This is the default and recommended compression format
    #[default]
    Bgzf,
}

impl FastaCompressionType {
    /// Determines compression type from file extension
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
            FastaCompressionType::Bgzf
        } else if path_str.ends_with(".gz") {
            FastaCompressionType::Gzip
        } else {
            FastaCompressionType::Plain
        }
    }
}

/// A unified writer for FASTA files supporting multiple compression formats
pub enum FastaLocalWriter {
    /// Writer for uncompressed FASTA files
    Plain(fasta::io::Writer<BufWriter<File>>),
    /// Writer for GZIP-compressed FASTA files
    Gzip(fasta::io::Writer<GzEncoder<BufWriter<File>>>),
    /// Writer for BGZF-compressed FASTA files (recommended)
    Bgzf(fasta::io::Writer<bgzf::Writer<BufWriter<File>>>),
}

impl FastaLocalWriter {
    /// Creates a new FASTA writer for the given path with automatic compression detection
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let compression = FastaCompressionType::from_path(&path);
        Self::with_compression(path, compression)
    }

    /// Creates a new FASTA writer with explicit compression type
    pub fn with_compression<P: AsRef<Path>>(
        path: P,
        compression: FastaCompressionType,
    ) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create output file: {e}"))
        })?;
        let buf_writer = BufWriter::new(file);

        match compression {
            FastaCompressionType::Plain => {
                let writer = fasta::io::Writer::new(buf_writer);
                Ok(FastaLocalWriter::Plain(writer))
            }
            FastaCompressionType::Gzip => {
                let encoder = GzEncoder::new(buf_writer, Compression::default());
                let writer = fasta::io::Writer::new(encoder);
                Ok(FastaLocalWriter::Gzip(writer))
            }
            FastaCompressionType::Bgzf => {
                let bgzf_writer = bgzf::Writer::new(buf_writer);
                let writer = fasta::io::Writer::new(bgzf_writer);
                Ok(FastaLocalWriter::Bgzf(writer))
            }
        }
    }

    /// Writes a single FASTA record to the file
    pub fn write_record(&mut self, record: &fasta::Record) -> Result<()> {
        match self {
            FastaLocalWriter::Plain(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTA record: {e}"))
            }),
            FastaLocalWriter::Gzip(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTA record: {e}"))
            }),
            FastaLocalWriter::Bgzf(writer) => writer.write_record(record).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write FASTA record: {e}"))
            }),
        }
    }

    /// Writes multiple FASTA records to the file
    pub fn write_records(&mut self, records: &[fasta::Record]) -> Result<()> {
        for record in records {
            self.write_record(record)?;
        }
        Ok(())
    }

    /// Flushes the writer, ensuring all data is written to the file
    pub fn flush(&mut self) -> Result<()> {
        match self {
            FastaLocalWriter::Plain(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
            FastaLocalWriter::Gzip(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
            FastaLocalWriter::Bgzf(writer) => writer
                .get_mut()
                .flush()
                .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}"))),
        }
    }

    /// Finishes writing and closes the file
    ///
    /// For compressed formats, this properly finalizes the compression stream.
    /// This consumes the writer.
    pub fn finish(self) -> Result<()> {
        match self {
            FastaLocalWriter::Plain(writer) => {
                let mut buf_writer = writer.into_inner();
                buf_writer
                    .flush()
                    .map_err(|e| DataFusionError::Execution(format!("Failed to flush writer: {e}")))
            }
            FastaLocalWriter::Gzip(writer) => {
                let encoder = writer.into_inner();
                encoder.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish GZIP stream: {e}"))
                })?;
                Ok(())
            }
            FastaLocalWriter::Bgzf(writer) => {
                let bgzf_writer = writer.into_inner();
                bgzf_writer.finish().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to finish BGZF stream: {e}"))
                })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_fasta::record::{Definition, Sequence};
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_compression_type_from_path() {
        assert_eq!(
            FastaCompressionType::from_path("test.fasta"),
            FastaCompressionType::Plain
        );
        assert_eq!(
            FastaCompressionType::from_path("test.fa"),
            FastaCompressionType::Plain
        );
        assert_eq!(
            FastaCompressionType::from_path("test.fasta.gz"),
            FastaCompressionType::Gzip
        );
        assert_eq!(
            FastaCompressionType::from_path("test.fasta.bgz"),
            FastaCompressionType::Bgzf
        );
        assert_eq!(
            FastaCompressionType::from_path("test.fasta.bgzf"),
            FastaCompressionType::Bgzf
        );
        assert_eq!(
            FastaCompressionType::from_path("TEST.FASTA.GZ"),
            FastaCompressionType::Gzip
        );
    }

    #[test]
    fn test_write_plain_fasta() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fasta").unwrap();
        let path = temp_file.path();

        let definition = Definition::new("seq1", None);
        let sequence = Sequence::from(b"ACGT".to_vec());
        let record = fasta::Record::new(definition, sequence);

        {
            let mut writer = FastaLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        let mut content = String::new();
        let mut file = File::open(path).unwrap();
        file.read_to_string(&mut content).unwrap();

        assert!(content.contains(">seq1"));
        assert!(content.contains("ACGT"));

        Ok(())
    }

    #[test]
    fn test_write_gzip_fasta() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fasta.gz").unwrap();
        let path = temp_file.path();

        let definition = Definition::new("seq1", None);
        let sequence = Sequence::from(b"ACGT".to_vec());
        let record = fasta::Record::new(definition, sequence);

        {
            let mut writer = FastaLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        Ok(())
    }

    #[test]
    fn test_write_bgzf_fasta() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".fasta.bgz").unwrap();
        let path = temp_file.path();

        let definition = Definition::new("seq1", None);
        let sequence = Sequence::from(b"ACGT".to_vec());
        let record = fasta::Record::new(definition, sequence);

        {
            let mut writer = FastaLocalWriter::new(path)?;
            writer.write_record(&record)?;
            writer.finish()?;
        }

        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);

        Ok(())
    }
}
