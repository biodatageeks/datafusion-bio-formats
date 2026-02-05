//! Writer for CRAM files with reference sequence support
//!
//! This module provides writers for CRAM files with support for:
//! - CRAM format with external reference sequences
//! - Reference sequence validation and indexing

use datafusion::common::{DataFusionError, Result};
use noodles_cram as cram;
use noodles_fasta as fasta;
use noodles_sam as sam;
use noodles_sam::alignment::io::Write as AlignmentWrite;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Writer for CRAM files with reference sequence support
///
/// CRAM files require a reference sequence for compression. This writer
/// handles reference sequence loading and validation.
pub struct CramLocalWriter {
    writer: cram::io::Writer<BufWriter<File>>,
    reference_path: Option<PathBuf>,
}

impl CramLocalWriter {
    /// Creates a new CRAM writer with reference sequence
    ///
    /// # Arguments
    ///
    /// * `path` - The output CRAM file path
    /// * `reference_path` - Optional path to reference FASTA file (with .fai index)
    ///
    /// # Returns
    ///
    /// A new `CramLocalWriter` configured for CRAM output
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be created
    /// - The reference path is provided but invalid
    /// - The reference index (.fai) is missing
    pub fn new<P: AsRef<Path>>(path: P, reference_path: Option<P>) -> Result<Self> {
        let reference_path = reference_path.map(|p| p.as_ref().to_path_buf());

        // Validate reference path if provided
        if let Some(ref ref_path) = reference_path {
            if !ref_path.exists() {
                return Err(DataFusionError::Execution(format!(
                    "Reference FASTA file not found: {}",
                    ref_path.display()
                )));
            }

            // Check for .fai index
            let fai_path = format!("{}.fai", ref_path.display());
            if !Path::new(&fai_path).exists() {
                return Err(DataFusionError::Execution(format!(
                    "Reference index (.fai) not found: {}. Please create it with: samtools faidx {}",
                    fai_path,
                    ref_path.display()
                )));
            }
        }

        let file = File::create(path.as_ref()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create CRAM file: {}", e))
        })?;
        let buf_writer = BufWriter::new(file);

        // Build writer with reference repository if available
        let writer = if let Some(ref ref_path) = reference_path {
            let fasta_file = File::open(ref_path).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open reference FASTA {}: {}",
                    ref_path.display(),
                    e
                ))
            })?;
            let fasta_reader = std::io::BufReader::new(fasta_file);

            let fai_path = format!("{}.fai", ref_path.display());
            let index_file = File::open(&fai_path).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open reference index {}: {}",
                    fai_path, e
                ))
            })?;
            let mut index_reader = fasta::fai::io::Reader::new(std::io::BufReader::new(index_file));
            let index = index_reader.read_index().map_err(|e| {
                DataFusionError::Execution(format!("Failed to read FASTA index: {}", e))
            })?;

            let indexed_reader = fasta::io::IndexedReader::new(fasta_reader, index);
            let adapter = fasta::repository::adapters::IndexedReader::new(indexed_reader);
            let repository = fasta::Repository::new(adapter);

            cram::io::writer::Builder::default()
                .set_reference_sequence_repository(repository)
                .build_from_writer(buf_writer)
        } else {
            cram::io::writer::Builder::default().build_from_writer(buf_writer)
        };

        Ok(Self {
            writer,
            reference_path,
        })
    }

    /// Returns the reference path if set
    pub fn reference_path(&self) -> Option<&Path> {
        self.reference_path.as_deref()
    }

    /// Writes the SAM header to the CRAM file
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
        self.writer
            .write_header(header)
            .map_err(|e| DataFusionError::Execution(format!("Failed to write CRAM header: {}", e)))
    }

    /// Writes a single alignment record to the file
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for CRAM encoding)
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
        self.writer
            .write_alignment_record(header, record)
            .map_err(|e| DataFusionError::Execution(format!("Failed to write CRAM record: {}", e)))
    }

    /// Writes multiple alignment records to the file
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for CRAM encoding)
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
    /// This properly finalizes the CRAM stream and consumes the writer.
    ///
    /// # Arguments
    ///
    /// * `header` - The SAM header (needed for CRAM finish)
    ///
    /// # Errors
    ///
    /// Returns an error if finishing fails
    pub fn finish(mut self, header: &sam::Header) -> Result<()> {
        self.writer.finish(header).map_err(|e| {
            DataFusionError::Execution(format!("Failed to finish CRAM stream: {}", e))
        })?;
        self.writer.get_mut().flush().map_err(|e| {
            DataFusionError::Execution(format!("Failed to flush CRAM output: {}", e))
        })?;
        Ok(())
    }
}

/// Validates that a reference FASTA file exists and has an index
///
/// # Arguments
///
/// * `reference_path` - Path to the reference FASTA file
///
/// # Returns
///
/// Ok(()) if the reference and index exist, error otherwise
pub fn validate_reference_file<P: AsRef<Path>>(reference_path: P) -> Result<()> {
    let ref_path = reference_path.as_ref();

    if !ref_path.exists() {
        return Err(DataFusionError::Execution(format!(
            "Reference FASTA file not found: {}",
            ref_path.display()
        )));
    }

    let fai_path = format!("{}.fai", ref_path.display());
    if !Path::new(&fai_path).exists() {
        return Err(DataFusionError::Execution(format!(
            "Reference index (.fai) not found: {}. Create it with: samtools faidx {}",
            fai_path,
            ref_path.display()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_validate_reference_file_missing() {
        let result = validate_reference_file("/nonexistent/path/ref.fasta");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_cram_writer_without_reference() {
        let temp_file = NamedTempFile::with_suffix(".cram").unwrap();
        let path = temp_file.path();

        let writer = CramLocalWriter::new(path, Option::<&Path>::None);
        assert!(writer.is_ok());
    }

    #[test]
    fn test_write_empty_header() -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".cram").unwrap();
        let path = temp_file.path();

        let header = sam::Header::default();
        let mut writer = CramLocalWriter::new(path, Option::<&Path>::None)?;
        writer.write_header(&header)?;
        writer.finish(&header)?;

        Ok(())
    }

    /// Noodles-level roundtrip test: read CRAM → write CRAM → read back
    ///
    /// This test isolates noodles reader/writer compatibility by:
    /// 1. Reading records from a real CRAM file using noodles directly
    /// 2. Writing them to a new CRAM file using noodles directly
    /// 3. Attempting to read the written file back with noodles
    #[test]
    fn test_noodles_cram_roundtrip_with_reference() {
        let cram_path = "/Users/mwiewior/research/git/polars-bio/tests/data/io/cram/external_ref/test_chr20.cram";
        let ref_path =
            "/Users/mwiewior/research/git/polars-bio/tests/data/io/cram/external_ref/chr20.fa";

        if !Path::new(cram_path).exists() || !Path::new(ref_path).exists() {
            eprintln!("Skipping roundtrip test: test data not available");
            return;
        }

        // Build reference repository
        let fasta_file = File::open(ref_path).unwrap();
        let fasta_reader = std::io::BufReader::new(fasta_file);
        let fai_path = format!("{}.fai", ref_path);
        let index_file = File::open(&fai_path).unwrap();
        let mut index_reader = fasta::fai::io::Reader::new(std::io::BufReader::new(index_file));
        let index = index_reader.read_index().unwrap();
        let indexed_reader = fasta::io::IndexedReader::new(fasta_reader, index);
        let adapter = fasta::repository::adapters::IndexedReader::new(indexed_reader);
        let repository = fasta::Repository::new(adapter);

        // --- Step 1: Read original CRAM ---
        let mut reader = cram::io::reader::Builder::default()
            .set_reference_sequence_repository(repository.clone())
            .build_from_path(cram_path)
            .unwrap();
        let header = reader.read_header().unwrap();

        let mut records: Vec<sam::alignment::RecordBuf> = Vec::new();
        for result in reader.records(&header) {
            match result {
                Ok(record) => records.push(record),
                Err(e) => {
                    eprintln!("Error reading record: {}", e);
                    break;
                }
            }
        }
        let num_original = records.len();
        eprintln!(
            "Read {} records from original CRAM, header has {} ref seqs",
            num_original,
            header.reference_sequences().len()
        );
        assert!(num_original > 0, "Should read at least one record");

        // --- Step 2: Write to new CRAM ---
        let temp_file = NamedTempFile::with_suffix(".cram").unwrap();
        let output_path = temp_file.path().to_path_buf();

        let mut writer = cram::io::writer::Builder::default()
            .set_reference_sequence_repository(repository.clone())
            .build_from_writer(BufWriter::new(File::create(&output_path).unwrap()));

        writer.write_header(&header).unwrap();
        for record in &records {
            writer.write_alignment_record(&header, record).unwrap();
        }
        writer.finish(&header).unwrap();
        // Explicit flush of BufWriter
        writer.get_mut().flush().unwrap();

        let output_size = std::fs::metadata(&output_path).unwrap().len();
        eprintln!(
            "Wrote {} records to {}, file size: {} bytes",
            num_original,
            output_path.display(),
            output_size
        );

        // --- Step 3: Read back the written CRAM ---
        let mut reader2 = cram::io::reader::Builder::default()
            .set_reference_sequence_repository(repository.clone())
            .build_from_path(&output_path)
            .unwrap();
        let header2 = reader2.read_header().unwrap();
        eprintln!(
            "Read-back header has {} ref seqs",
            header2.reference_sequences().len()
        );

        let mut count = 0;
        for result in reader2.records(&header2) {
            match result {
                Ok(_) => count += 1,
                Err(e) => {
                    eprintln!(
                        "Error reading back record {} of {}: {}",
                        count + 1,
                        num_original,
                        e
                    );
                    panic!(
                        "Failed to read back record {} of {}: {}",
                        count + 1,
                        num_original,
                        e
                    );
                }
            }
        }

        assert_eq!(
            count, num_original,
            "Should read back the same number of records"
        );
        eprintln!("Roundtrip successful: {} records", count);
    }
}
