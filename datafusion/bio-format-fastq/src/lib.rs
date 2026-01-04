//! FASTQ file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading FASTQ files,
//! a standard text-based format for storing nucleotide sequences and their corresponding
//! quality scores from DNA sequencing.
//!
//! # Features
//!
//! - Direct SQL queries on FASTQ files via DataFusion
//! - Support for compressed files (GZIP, BGZF)
//! - Parallel reading of BGZF-compressed files
//! - Cloud storage support (GCS, S3, Azure)
//! - Memory-efficient streaming for large files
//!
//! # Schema
//!
//! FASTQ files are read into tables with the following columns:
//!
//! | Column | Type | Description |
//! |--------|------|-------------|
//! | name | String | Sequence identifier |
//! | description | String | Sequence description (optional) |
//! | sequence | String | Nucleotide sequence |
//! | quality_scores | String | Quality scores (Phred+33 encoded) |
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_fastq::BgzfFastqTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a FASTQ file as a table
//! let table = BgzfFastqTableProvider::try_new("data/sample.fastq.gz")?;
//! ctx.register_table("sequences", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT name, sequence FROM sequences LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// BGZF parallel reader implementation for multi-threaded FASTQ parsing
pub mod bgzf_parallel_reader;
mod physical_exec;
/// Storage layer for reading FASTQ files from local and remote sources
pub mod storage;
/// DataFusion table provider implementation for FASTQ files
pub mod table_provider;

pub use bgzf_parallel_reader::{
    BgzfFastqTableProvider, get_bgzf_partition_bounds, synchronize_reader,
};
