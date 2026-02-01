//! CRAM file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading CRAM files,
//! a highly compressed columnar file format for storing biological sequences aligned to
//! a reference genome.
//!
//! # Features
//!
//! - Direct SQL queries on CRAM files via DataFusion
//! - High compression with reference-based encoding
//! - Cloud storage support (GCS, S3, Azure)
//! - Support for CRAM indexes (CRAI)
//! - Memory-efficient streaming
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_cram::table_provider::CramTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a CRAM file as a table
//! let table = CramTableProvider::new("data/alignments.cram".to_string(), None, None, true, None)?;
//! ctx.register_table("alignments", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT name, chrom, start FROM alignments LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Important Notes
//!
//! - This crate uses a forked version of noodles from biodatageeks/noodles for enhanced CRAM support
//! - CRAM files require a reference genome for full sequence reconstruction

#![warn(missing_docs)]

/// Internal implementation of the physical execution plan for CRAM file reads.
///
/// This module handles the actual reading of CRAM file records and
/// conversion to Apache Arrow RecordBatch format for DataFusion queries.
mod physical_exec;

/// Storage and I/O operations for reading CRAM files.
///
/// Provides functions to read CRAM files from local filesystem or cloud storage,
/// with support for reference sequences and both synchronous and asynchronous I/O.
pub mod storage;

/// DataFusion table provider implementation for CRAM files.
///
/// Implements the TableProvider trait to allow registering CRAM files as
/// queryable tables within DataFusion contexts.
pub mod table_provider;

/// Writer for CRAM files with reference sequence support.
///
/// Provides functionality to write CRAM files with optional reference compression.
pub mod writer;

/// Physical execution plan for writing CRAM files.
///
/// Implements the ExecutionPlan trait for writing DataFusion query results to CRAM files.
pub mod write_exec;

/// Serializer for converting Arrow RecordBatches to CRAM records.
///
/// Handles conversion of DataFusion Arrow data to noodles CRAM record format.
pub mod serializer;

/// Header builder for constructing CRAM headers from Arrow schemas.
///
/// Reconstructs SAM/CRAM headers from Arrow schema metadata for round-trip operations.
pub mod header_builder;
