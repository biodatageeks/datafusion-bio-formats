//! GFF (General Feature Format) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading GFF/GFF3 files,
//! a standard tab-delimited format for representing genomic features and annotations.
//!
//! # Features
//!
//! - Direct SQL queries on GFF files via DataFusion
//! - Attribute parsing support
//! - Cloud storage support (GCS, S3, Azure)
//! - GZIP compression support
//! - Efficient annotation queries
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_gff::table_provider::GffTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a GFF file as a table
//! let table = GffTableProvider::new("data/annotations.gff3".to_string(), None, None, None, true)?;
//! ctx.register_table("annotations", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT seqid, ty, start, end FROM annotations LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// Filter expression evaluation and pushdown support
mod filter_utils;

/// Physical execution plan for GFF file queries
mod physical_exec;

/// Storage abstraction for local and remote GFF files with multiple parser types
///
/// Supports both local and remote (S3, GCS, Azure) GFF file reading with parser
/// type selection for performance tuning (Standard, Fast, SIMD).
pub mod storage;

/// Apache DataFusion table provider for GFF format
///
/// Implements the DataFusion TableProvider trait for seamless SQL query support
/// on GFF files with schema projection and filter pushdown capabilities.
pub mod table_provider;
