//! BED (Browser Extensible Data) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading BED files,
//! a tab-delimited format for representing genomic regions and annotations.
//!
//! # Features
//!
//! - Direct SQL queries on BED files via DataFusion
//! - Support for BED3, BED6, BED12, and custom formats
//! - Cloud storage support (GCS, S3, Azure)
//! - Efficient interval queries
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_bed::table_provider::{BedTableProvider, BEDFields};
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a BED file as a table
//! let table = BedTableProvider::new("data/genes.bed".to_string(), BEDFields::BED3, None, None)?;
//! ctx.register_table("genes", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT chrom, start, end FROM genes LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

mod async_reader;
mod physical_exec;
/// Storage and reader implementations for BED files
///
/// This module provides readers for accessing BED file data from various storage backends
/// including local files and cloud storage (GCS, S3, Azure).
pub mod storage;
/// DataFusion table provider for BED file format
///
/// This module implements the DataFusion `TableProvider` trait to enable SQL queries
/// over BED files. It defines the schema and query execution plan for BED data.
pub mod table_provider;
