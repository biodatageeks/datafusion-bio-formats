//! FASTQ file format support for Apache DataFusion
//!
//! This crate provides a DataFusion table provider for reading and writing FASTQ files,
//! a standard text-based format for storing nucleotide sequences and their corresponding
//! quality scores from DNA sequencing.
//!
//! # Features
//!
//! - Direct SQL queries on FASTQ files via DataFusion
//! - Support for compressed files (GZIP, BGZF)
//! - Parallel reading of BGZF-compressed files (via GZI index)
//! - Parallel reading of uncompressed files (via byte-range partitioning)
//! - Cloud storage support (GCS, S3, Azure)
//! - Memory-efficient streaming for large files
//! - Write support (INSERT OVERWRITE)
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
//! use datafusion_bio_format_fastq::FastqTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a FASTQ file as a table (local file, no cloud storage options needed)
//! let table = FastqTableProvider::new("data/sample.fastq.bgz".to_string(), None)?;
//! ctx.register_table("sequences", Arc::new(table))?;
//!
//! // Query with SQL — parallelism is automatic based on target_partitions
//! let df = ctx.sql("SELECT name, sequence FROM sequences LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub(crate) mod physical_exec;
/// Serializer for converting Arrow RecordBatches to FASTQ records
pub mod serializer;
/// Storage layer for reading FASTQ files from local and remote sources
pub mod storage;
/// DataFusion table provider implementation for FASTQ files
pub mod table_provider;
mod write_exec;
/// Writer for FASTQ files with compression support
pub mod writer;

pub use table_provider::FastqTableProvider;
pub use write_exec::FastqWriteExec;
pub use writer::{FastqCompressionType, FastqLocalWriter};
