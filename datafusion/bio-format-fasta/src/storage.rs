use datafusion_bio_format_core::object_storage::{CompressionType, get_compression_type};
use noodles_bgzf as bgzf;
use noodles_fasta::{self as fasta, Record};
use std::fs::File;
use std::io::{BufReader, Error};

pub enum FastaLocalReader {
    BGZF(fasta::Reader<bgzf::MultithreadedReader<File>>),
    GZIP(fasta::Reader<BufReader<flate2::read::GzDecoder<File>>>),
    PLAIN(fasta::Reader<BufReader<File>>),
}

impl FastaLocalReader {
    pub fn new(file_path: String, thread_num: usize) -> Result<Self, Error> {
        let compression_type = get_compression_type(file_path.clone(), None);
        let file = File::open(file_path)?;

        match compression_type {
            CompressionType::BGZF => {
                let reader = bgzf::MultithreadedReader::with_worker_count(
                    std::num::NonZero::new(thread_num).unwrap(),
                    file,
                );
                Ok(Self::BGZF(fasta::Reader::new(reader)))
            }
            CompressionType::GZIP => {
                let reader = flate2::read::GzDecoder::new(file);
                Ok(Self::GZIP(fasta::Reader::new(BufReader::new(reader))))
            }
            CompressionType::NONE => Ok(Self::PLAIN(fasta::Reader::new(BufReader::new(file)))),
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
