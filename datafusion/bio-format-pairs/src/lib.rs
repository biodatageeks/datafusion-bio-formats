//! Pairs (Hi-C) file format support for Apache DataFusion
//!
//! This crate provides DataFusion table provider implementations for reading Pairs files,
//! a tab-delimited format for Hi-C chromosome conformation capture contact data from
//! the 4D Nucleome project.
//!
//! # Features
//!
//! - Direct SQL queries on Pairs files via DataFusion
//! - BGZF-compressed and plain text file support
//! - Tabix index support for efficient region queries on chr1/pos1
//! - Pairix (.px2) index detection (treated as tabix-compatible)
//! - Filter pushdown for chr1/pos1 (index) and chr2/pos2 (residual)
//! - Cloud storage support (GCS, S3, Azure)
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_pairs::table_provider::PairsTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//!
//! // Register a Pairs file as a table
//! let table = PairsTableProvider::new("data/contacts.pairs.gz".to_string(), None, true)?;
//! ctx.register_table("contacts", Arc::new(table))?;
//!
//! // Query with SQL
//! let df = ctx.sql("SELECT chr1, pos1, chr2, pos2 FROM contacts LIMIT 10").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

/// Pairs file header parsing
pub mod header;

/// Pairs-specific filter utilities for index and residual pushdown
mod filter_utils;

/// Physical execution plan for Pairs file queries
mod physical_exec;

/// Storage abstraction for local and remote Pairs files
pub mod storage;

/// Apache DataFusion table provider for Pairs format
pub mod table_provider;
