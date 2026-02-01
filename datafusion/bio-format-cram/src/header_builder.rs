//! CRAM header builder for constructing headers from Arrow schemas
//!
//! This module provides functionality for building SAM headers from Arrow schemas,
//! enabling round-trip CRAM read/write operations. Header information is reconstructed from:
//! - Schema-level metadata: file format version, reference sequences, read groups,
//!   program info, comments, and reference path (stored as JSON)
//!
//! When metadata is not available, sensible defaults are generated.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use noodles_sam as sam;
use noodles_sam::header::record::value::Map;
use noodles_sam::header::record::value::map::{
    Program, ReadGroup, ReferenceSequence, header::Version,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

/// Metadata key for CRAM file format version
pub const CRAM_FILE_FORMAT_VERSION_KEY: &str = "bio.cram.file_format_version";
/// Metadata key for reference sequences (JSON array)
pub const CRAM_REFERENCE_SEQUENCES_KEY: &str = "bio.cram.reference_sequences";
/// Metadata key for read groups (JSON array)
pub const CRAM_READ_GROUPS_KEY: &str = "bio.cram.read_groups";
/// Metadata key for program info (JSON array)
pub const CRAM_PROGRAM_INFO_KEY: &str = "bio.cram.program_info";
/// Metadata key for comments (JSON array)
pub const CRAM_COMMENTS_KEY: &str = "bio.cram.comments";
/// Metadata key for reference FASTA path
pub const CRAM_REFERENCE_PATH_KEY: &str = "bio.cram.reference_path";
/// Metadata key for reference MD5 checksum
pub const CRAM_REFERENCE_MD5_KEY: &str = "bio.cram.reference_md5";

/// Reference sequence metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceSequenceMetadata {
    /// Reference sequence name
    pub name: String,
    /// Reference sequence length
    pub length: usize,
}

/// Read group metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadGroupMetadata {
    /// Read group ID (required)
    pub id: String,
    /// Sample name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<String>,
    /// Platform
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Library
    #[serde(skip_serializing_if = "Option::is_none")]
    pub library: Option<String>,
    /// Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Program info metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramMetadata {
    /// Program ID (required)
    pub id: String,
    /// Program name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Program version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Command line
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_line: Option<String>,
}

/// Builds a SAM header from an Arrow schema for CRAM files
///
/// Reconstructs SAM header from schema metadata:
/// - File format version from `bio.cram.file_format_version` (defaults to "1.6")
/// - Reference sequences from `bio.cram.reference_sequences` (JSON array)
/// - Read groups from `bio.cram.read_groups` (JSON array)
/// - Program info from `bio.cram.program_info` (JSON array)
/// - Comments from `bio.cram.comments` (JSON array)
/// - Reference path from `bio.cram.reference_path` (optional)
///
/// # Arguments
///
/// * `schema` - The Arrow schema containing metadata
/// * `tag_fields` - List of tag field names (currently not used in header)
///
/// # Returns
///
/// A noodles SAM Header
pub fn build_cram_header(schema: &SchemaRef, _tag_fields: &[String]) -> Result<sam::Header> {
    let mut builder = sam::Header::builder();

    let schema_metadata = schema.metadata();

    // Build header (@HD) line
    let file_format_version = schema_metadata
        .get(CRAM_FILE_FORMAT_VERSION_KEY)
        .map(|s| s.as_str())
        .unwrap_or("1.6");

    // Set header with version
    let version: Version = file_format_version
        .parse()
        .unwrap_or_else(|_| Version::new(1, 6));
    let header_map = Map::<sam::header::record::value::map::Header>::new(version);
    builder = builder.set_header(header_map);

    // Add reference sequences (@SQ)
    if let Some(ref_seqs_json) = schema_metadata.get(CRAM_REFERENCE_SEQUENCES_KEY) {
        if let Some(ref_seqs) = from_json_string::<Vec<ReferenceSequenceMetadata>>(ref_seqs_json) {
            for ref_seq in ref_seqs {
                let length = NonZeroUsize::new(ref_seq.length).ok_or_else(|| {
                    DataFusionError::Execution(
                        "Reference sequence length cannot be zero".to_string(),
                    )
                })?;
                let reference_sequence = Map::<ReferenceSequence>::new(length);
                builder = builder.add_reference_sequence(ref_seq.name, reference_sequence);
            }
        }
    }

    // Add read groups (@RG)
    if let Some(read_groups_json) = schema_metadata.get(CRAM_READ_GROUPS_KEY) {
        if let Some(read_groups) = from_json_string::<Vec<ReadGroupMetadata>>(read_groups_json) {
            for rg in read_groups {
                let rg_map = Map::<ReadGroup>::default();
                builder = builder.add_read_group(rg.id, rg_map);
            }
        }
    }

    // Add program info (@PG)
    if let Some(programs_json) = schema_metadata.get(CRAM_PROGRAM_INFO_KEY) {
        if let Some(programs) = from_json_string::<Vec<ProgramMetadata>>(programs_json) {
            for pg in programs {
                let pg_map = Map::<Program>::default();
                builder = builder.add_program(pg.id, pg_map);
            }
        }
    }

    // Add comments (@CO)
    if let Some(comments_json) = schema_metadata.get(CRAM_COMMENTS_KEY) {
        if let Some(comments) = from_json_string::<Vec<String>>(comments_json) {
            for comment in comments {
                builder = builder.add_comment(comment);
            }
        }
    }

    Ok(builder.build())
}

/// Deserializes a JSON string to a typed value
fn from_json_string<T: serde::de::DeserializeOwned>(json: &str) -> Option<T> {
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_build_cram_header_basic() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("chrom", DataType::Utf8, true),
            Field::new("start", DataType::UInt32, true),
        ]));

        let header = build_cram_header(&schema, &[])?;

        // Check default values
        assert_eq!(header.header().unwrap().version().to_string(), "1.6");

        Ok(())
    }

    #[test]
    fn test_build_cram_header_with_metadata() -> Result<()> {
        let mut metadata = HashMap::new();
        metadata.insert(CRAM_FILE_FORMAT_VERSION_KEY.to_string(), "1.6".to_string());

        let ref_seqs = vec![
            ReferenceSequenceMetadata {
                name: "chr1".to_string(),
                length: 249250621,
            },
            ReferenceSequenceMetadata {
                name: "chr2".to_string(),
                length: 242193529,
            },
        ];
        metadata.insert(
            CRAM_REFERENCE_SEQUENCES_KEY.to_string(),
            serde_json::to_string(&ref_seqs).unwrap(),
        );

        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("chrom", DataType::Utf8, true),
                Field::new("start", DataType::UInt32, true),
            ],
            metadata,
        ));

        let header = build_cram_header(&schema, &[])?;

        // Check values
        assert_eq!(header.header().unwrap().version().to_string(), "1.6");
        assert_eq!(header.reference_sequences().len(), 2);

        Ok(())
    }

    #[test]
    fn test_build_cram_header_with_reference_path() -> Result<()> {
        let mut metadata = HashMap::new();
        metadata.insert(
            CRAM_REFERENCE_PATH_KEY.to_string(),
            "/path/to/reference.fasta".to_string(),
        );

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("name", DataType::Utf8, true)],
            metadata,
        ));

        let header = build_cram_header(&schema, &[])?;

        // Reference path is stored in metadata but not in header itself
        assert!(header.header().is_some());

        Ok(())
    }

    #[test]
    fn test_build_cram_header_with_comments() -> Result<()> {
        let mut metadata = HashMap::new();

        let comments = vec!["This is a test".to_string(), "Another comment".to_string()];
        metadata.insert(
            CRAM_COMMENTS_KEY.to_string(),
            serde_json::to_string(&comments).unwrap(),
        );

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("name", DataType::Utf8, true)],
            metadata,
        ));

        let header = build_cram_header(&schema, &[])?;

        // Check comments
        assert_eq!(header.comments().len(), 2);

        Ok(())
    }
}
