use datafusion_bio_format_core::object_storage::{
    CompressionType, get_compression_type, get_remote_stream_sync,
};
use noodles_bgzf as bgzf;
use noodles_fasta::{self as fasta, Record};
use std::fs::File;
use std::io::{BufReader, Error, Read};

pub enum FastaReader {
    Local(FastaLocalReader),
    Remote(FastaRemoteReader),
}

impl FastaReader {
    pub fn new(file_path: String, is_remote: bool) -> Result<Self, Error> {
        if is_remote {
            Ok(Self::Remote(FastaRemoteReader::new(&file_path)?))
        } else {
            Ok(Self::Local(FastaLocalReader::new(file_path, 1)?))
        }
    }

    pub fn records(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<Record, std::io::Error>> + Send + '_> {
        match self {
            Self::Local(reader) => reader.records(),
            Self::Remote(reader) => reader.records(),
        }
    }
}

pub enum FastaLocalReader {
    BGZF(fasta::io::Reader<bgzf::Reader<File>>),
    GZIP(fasta::io::Reader<BufReader<flate2::read::GzDecoder<File>>>),
    PLAIN(fasta::io::Reader<BufReader<File>>),
}

impl FastaLocalReader {
    pub fn new(file_path: String, _thread_num: usize) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        let file = File::open(file_path)?;

        match compression_type {
            CompressionType::BGZF => {
                let reader = bgzf::Reader::new(file);
                Ok(Self::BGZF(fasta::io::Reader::new(reader)))
            }
            CompressionType::GZIP => {
                let reader = flate2::read::GzDecoder::new(file);
                Ok(Self::GZIP(fasta::io::Reader::new(BufReader::new(reader))))
            }
            CompressionType::NONE => Ok(Self::PLAIN(fasta::io::Reader::new(BufReader::new(file)))),
            _ => unimplemented!(),
        }
    }

    pub fn records(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<Record, std::io::Error>> + Send + '_> {
        match self {
            Self::BGZF(reader) => Box::new(reader.records()),
            Self::GZIP(reader) => Box::new(reader.records()),
            Self::PLAIN(reader) => Box::new(reader.records()),
        }
    }
}

pub enum FastaRemoteReader {
    BGZF(fasta::io::Reader<bgzf::Reader<Box<dyn Read + Send + Sync>>>),
    GZIP(fasta::io::Reader<BufReader<flate2::read::GzDecoder<Box<dyn Read + Send + Sync>>>>),
    PLAIN(fasta::io::Reader<BufReader<Box<dyn Read + Send + Sync>>>),
}

impl FastaRemoteReader {
    pub fn new(file_path: &str) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.to_string(), None);
        let stream = get_remote_stream_sync(file_path)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

        match compression_type {
            CompressionType::BGZF => {
                let reader = bgzf::Reader::new(stream);
                Ok(Self::BGZF(fasta::io::Reader::new(reader)))
            }
            CompressionType::GZIP => {
                let reader = flate2::read::GzDecoder::new(stream);
                Ok(Self::GZIP(fasta::io::Reader::new(BufReader::new(reader))))
            }
            CompressionType::NONE => {
                Ok(Self::PLAIN(fasta::io::Reader::new(BufReader::new(stream))))
            }
            _ => unimplemented!(),
        }
    }

    pub fn records(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<Record, std::io::Error>> + Send + '_> {
        match self {
            Self::BGZF(reader) => Box::new(reader.records()),
            Self::GZIP(reader) => Box::new(reader.records()),
            Self::PLAIN(reader) => Box::new(reader.records()),
        }
    }
}
