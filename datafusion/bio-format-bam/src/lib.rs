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
//! ctx.register_table("alignments", Arc::new(table.clone()))?;
//!
//! // Discover and describe available columns by sampling records
//! let schema_info = table.describe(&ctx, Some(100)).await?;
//! schema_info.show().await?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT name, chrom, start FROM alignments LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// BAM/SAM header builder for constructing headers from Arrow schemas.
pub mod header_builder;
/// Physical execution plan for BAM file scanning.
mod physical_exec;
/// Serializer for converting Arrow RecordBatches to BAM records.
pub mod serializer;
/// BAM file storage and I/O operations.
pub mod storage;
/// DataFusion table provider implementation for BAM files.
pub mod table_provider;
/// Write execution plan for BAM/SAM files.
pub mod write_exec;
/// Writer for BAM/SAM files with compression support.
pub mod writer;
