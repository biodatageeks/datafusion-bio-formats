pub mod bgzf_parallel_reader;
pub mod needletail_storage;
pub mod parser;
mod physical_exec;
pub mod storage;
pub mod table_provider;

pub use bgzf_parallel_reader::BgzfFastqTableProvider;
pub use parser::FastqParser;
