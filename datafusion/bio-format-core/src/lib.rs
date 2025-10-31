//! Core utilities for DataFusion bioinformatics table providers
//!
//! This crate provides shared infrastructure for implementing Apache DataFusion table providers
//! for various bioinformatics file formats. It includes:
//!
//! - **Object Storage Integration**: Support for reading from local files and cloud storage
//!   providers (GCS, S3, Azure Blob Storage) via OpenDAL
//! - **Compression Support**: Automatic detection and handling of GZIP and BGZF compression
//! - **Table Utilities**: Helper functions and types for implementing DataFusion table providers
//! - **Streaming I/O**: Efficient streaming readers optimized for large biological data files
//!
//! ## Usage
//!
//! This crate is primarily used as a dependency by format-specific crates in the
//! datafusion-bio-format family. Most users will interact with format-specific crates rather
//! than using this core crate directly.
//!
//! ### Example: Compression Detection
//!
//! ```rust,no_run
//! use datafusion_bio_format_core::object_storage::{get_compression_type, CompressionType, ObjectStorageOptions};
//!
//! # async fn example() -> Result<(), opendal::Error> {
//! // Detect compression type of a file
//! let compression = get_compression_type(
//!     "data/sample.fastq.gz".to_string(),
//!     Some(CompressionType::AUTO),
//!     ObjectStorageOptions::default()
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`object_storage`]: Cloud and local storage integration via OpenDAL
//! - [`table_utils`]: Utilities for implementing DataFusion table providers

#![warn(missing_docs)]

/// Object storage integration for cloud and local file access
pub mod object_storage;
/// Table utilities for building DataFusion table providers
pub mod table_utils;

#[cfg(test)]
mod tests {
    mod object_storage;
}
