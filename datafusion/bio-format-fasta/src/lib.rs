//! FASTA file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading FASTA files,
//! a simple text-based format for representing nucleotide or protein sequences.
//!
//! # Features
//!
//! - Direct SQL queries on FASTA files via DataFusion
//! - GZIP compression support
//! - Cloud storage support (GCS, S3, Azure)
//! - Efficient sequence queries
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a FASTA file as a table
//! let table = FastaTableProvider::new("data/sequences.fasta".to_string(), None, None)?;
//! ctx.register_table("sequences", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT name, sequence FROM sequences LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Important Notes
//!
//! - This crate uses a forked version of noodles from biodatageeks/noodles for enhanced FASTA support

#![warn(missing_docs)]

/// Physical execution plan for FASTA table scans.
///
/// This module contains the `FastaExec` struct which implements DataFusion's `ExecutionPlan` trait
/// for executing queries against FASTA files. It handles both local and remote (cloud) file access.
pub mod physical_exec;

/// FASTA file reader implementations.
///
/// This module provides utilities for reading FASTA files from local filesystems and cloud storage.
/// It supports various compression formats including BGZF, GZIP, and uncompressed files.
pub mod storage;

/// DataFusion table provider for FASTA files.
///
/// This module contains the `FastaTableProvider` struct which implements DataFusion's `TableProvider` trait
/// to enable querying FASTA files using SQL.
pub mod table_provider;
