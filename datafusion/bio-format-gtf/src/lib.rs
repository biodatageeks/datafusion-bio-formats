//! GTF (Gene Transfer Format) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading GTF files,
//! a standard tab-delimited format for representing gene annotations (used by GENCODE/Ensembl).
//!
//! GTF uses the same 9-column structure as GFF3 but with different attribute syntax:
//! GTF uses `key "value";` while GFF3 uses `key=value;`.
//!
//! # Features
//!
//! - Direct SQL queries on GTF files via DataFusion
//! - GTF-style attribute parsing (`key "value"` format)
//! - GZIP and BGZF compression support
//! - Tabix index support for region-based queries with partition balancing
//! - Efficient annotation queries
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_gtf::table_provider::GtfTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a GTF file as a table
//! let table = GtfTableProvider::new("data/annotations.gtf".to_string(), None, true)?;
//! ctx.register_table("annotations", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT chrom, type, start, \"end\" FROM annotations LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// Filter expression evaluation and pushdown support
mod filter_utils;

/// Physical execution plan for GTF file queries
mod physical_exec;

/// Storage abstraction for local GTF files
///
/// Supports plain text, GZIP, and BGZF compressed GTF file reading,
/// as well as tabix-indexed region queries.
pub mod storage;

/// Apache DataFusion table provider for GTF format
///
/// Implements the DataFusion TableProvider trait for seamless SQL query support
/// on GTF files with schema projection, filter pushdown, and tabix index capabilities.
pub mod table_provider;
