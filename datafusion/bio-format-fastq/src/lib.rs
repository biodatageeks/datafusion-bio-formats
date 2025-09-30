pub mod bgzf_parallel_reader;
mod physical_exec;
pub mod storage;
pub mod table_provider;

pub use bgzf_parallel_reader::{BgzfFastqTableProvider, get_bgzf_partition_bounds, synchronize_reader};
