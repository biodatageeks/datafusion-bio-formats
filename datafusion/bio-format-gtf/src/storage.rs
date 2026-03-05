use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::path::Path;

/// A parsed GTF record with all 9 fields
pub struct GtfRecord {
    /// Sequence ID (chromosome/contig name)
    pub seqid: String,
    /// Source of the annotation
    pub source: String,
    /// Feature type
    pub ty: String,
    /// Start position (1-based, inclusive)
    pub start: u32,
    /// End position (1-based, inclusive)
    pub end: u32,
    /// Score or None if not provided
    pub score: Option<f32>,
    /// Strand direction
    pub strand: String,
    /// Phase as a string for parsing flexibility
    pub phase: String,
    /// Raw attribute string in GTF format
    pub attributes: String,
}

/// Trait for unified GTF record access
pub trait GtfRecordTrait {
    /// Returns the reference sequence name (seqid) field
    fn reference_sequence_name(&self) -> String;
    /// Returns the start position (1-based, inclusive)
    fn start(&self) -> u32;
    /// Returns the end position (1-based, inclusive)
    fn end(&self) -> u32;
    /// Returns the feature type
    fn ty(&self) -> String;
    /// Returns the source of this annotation
    fn source(&self) -> String;
    /// Returns the score, or None if score is not available
    fn score(&self) -> Option<f32>;
    /// Returns the strand ('+', '-', or '.')
    fn strand(&self) -> String;
    /// Returns the phase (0, 1, 2) for coding features, or None if not applicable
    fn phase(&self) -> Option<u8>;
    /// Returns the raw attributes string
    fn attributes_string(&self) -> String;
}

impl GtfRecordTrait for GtfRecord {
    fn reference_sequence_name(&self) -> String {
        self.seqid.clone()
    }

    fn start(&self) -> u32 {
        self.start
    }

    fn end(&self) -> u32 {
        self.end
    }

    fn ty(&self) -> String {
        self.ty.clone()
    }

    fn source(&self) -> String {
        self.source.clone()
    }

    fn score(&self) -> Option<f32> {
        self.score
    }

    fn strand(&self) -> String {
        self.strand.clone()
    }

    fn phase(&self) -> Option<u8> {
        self.phase.parse().ok()
    }

    fn attributes_string(&self) -> String {
        self.attributes.clone()
    }
}

/// Local GTF file reader supporting plain and GZIP compressed files
pub enum GtfLocalReader {
    /// Plain text reader
    Plain(BufReader<File>),
    /// GZIP compressed reader
    Gzip(Box<BufReader<GzDecoder<File>>>),
}

impl GtfLocalReader {
    /// Create a new GTF local reader, auto-detecting compression from the file extension
    pub fn new(file_path: &str) -> Result<Self, Error> {
        let path = Path::new(file_path);
        let file = File::open(path)?;

        if file_path.ends_with(".gz") || file_path.ends_with(".gzip") {
            let decoder = GzDecoder::new(file);
            Ok(GtfLocalReader::Gzip(Box::new(BufReader::new(decoder))))
        } else {
            Ok(GtfLocalReader::Plain(BufReader::new(file)))
        }
    }

    /// Convert into a synchronous iterator of GTF records
    pub fn into_sync_iterator(self) -> GtfSyncIterator {
        match self {
            GtfLocalReader::Plain(reader) => GtfSyncIterator {
                lines: Box::new(reader.lines()),
            },
            GtfLocalReader::Gzip(reader) => GtfSyncIterator {
                lines: Box::new((*reader).lines()),
            },
        }
    }
}

/// Synchronous iterator over GTF records
pub struct GtfSyncIterator {
    lines: Box<dyn Iterator<Item = Result<String, Error>> + Send>,
}

impl Iterator for GtfSyncIterator {
    type Item = Result<GtfRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(e) => return Some(Err(e)),
            };

            // Skip comment lines and empty lines
            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            return Some(parse_gtf_line(&line));
        }
    }
}

/// Parse a single GTF line into a GtfRecord
fn parse_gtf_line(line: &str) -> Result<GtfRecord, Error> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() < 9 {
        return Err(Error::new(
            std::io::ErrorKind::InvalidData,
            format!("GTF line has {} fields, expected 9", fields.len()),
        ));
    }

    let score = if fields[5] == "." {
        None
    } else {
        fields[5].parse().ok()
    };

    Ok(GtfRecord {
        seqid: fields[0].to_string(),
        source: fields[1].to_string(),
        ty: fields[2].to_string(),
        start: fields[3].parse().map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid start position '{}': {e}", fields[3]),
            )
        })?,
        end: fields[4].parse().map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid end position '{}': {e}", fields[4]),
            )
        })?,
        score,
        strand: fields[6].to_string(),
        phase: fields[7].to_string(),
        attributes: fields[8].to_string(),
    })
}
