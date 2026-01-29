//! BAM (Binary Alignment Map) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading BAM files,
//! the binary compressed version of SAM (Sequence Alignment/Map) format.
//!
//! # Features
//!
//! - Direct SQL queries on BAM files via DataFusion
//! - BGZF compression support (native to BAM)
//! - Cloud storage support (GCS, S3, Azure)
//! - Support for indexed BAM files (BAI)
//! - Memory-efficient streaming
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_bam::table_provider::BamTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a BAM file as a table
//! let table = BamTableProvider::new("data/alignments.bam".to_string(), None, None, true, None)?;
//! ctx.register_table("alignments", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT name, chrom, start FROM alignments LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// Physical execution plan for BAM file scanning.
mod physical_exec;
/// BAM file storage and I/O operations.
pub mod storage;
/// DataFusion table provider implementation for BAM files.
pub mod table_provider;
/// Registry of common BAM alignment tags with type information.
mod tag_registry;
