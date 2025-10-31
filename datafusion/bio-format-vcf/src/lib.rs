//! VCF (Variant Call Format) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading VCF files,
//! the standard format for storing genetic variants discovered through DNA sequencing.
//!
//! # Features
//!
//! - Direct SQL queries on VCF files via DataFusion
//! - Support for compressed files (GZIP, BGZF)
//! - Parallel reading of BGZF-compressed files
//! - Cloud storage support (GCS, S3, Azure)
//! - Case-sensitive INFO and FORMAT field handling
//! - Projection pushdown optimization
//!
//! # Schema
//!
//! VCF files are read into tables with core variant columns plus dynamically generated
//! INFO and FORMAT columns based on the VCF header:
//!
//! **Core Columns:** chrom, pos, id, ref, alt, qual, filter
//!
//! **INFO Columns:** Dynamically created (e.g., info_af, info_dp)
//!
//! **FORMAT/Sample Columns:** Created per sample (e.g., sample1_gt, sample1_dp)
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a VCF file as a table
//! let table = BgzfVcfTableProvider::try_new("data/variants.vcf.gz")?;
//! ctx.register_table("variants", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("
//!     SELECT chrom, pos, ref, alt
//!     FROM variants
//!     WHERE chrom = 'chr1'
//!     LIMIT 10
//! ").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Important Notes
//!
//! - This crate uses a forked version of noodles from biodatageeks/noodles for enhanced VCF support
//! - INFO and FORMAT field names are case-sensitive per VCF specification

#![warn(missing_docs)]

extern crate core;

/// Parallel BGZF VCF reader implementation.
///
/// This module provides an optimized table provider for reading BGZF-compressed VCF files
/// with parallel execution using BGZF block partition boundaries. It's designed for high-performance
/// querying of large genomic variant datasets.
pub mod bgzf_parallel_reader;
/// Physical execution plan implementation for VCF queries.
mod physical_exec;
/// Storage layer and file I/O utilities for VCF format.
///
/// This module handles reading VCF files from various sources (local, S3, GCS, Azure)
/// and supports different compression formats (BGZF, GZIP, uncompressed).
pub mod storage;
/// DataFusion table provider implementation for VCF files.
///
/// This module provides the primary API for registering VCF files as queryable
/// DataFusion tables and handling schema determination from VCF headers.
pub mod table_provider;
