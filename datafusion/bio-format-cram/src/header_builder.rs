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
use datafusion_bio_format_core::{
    BAM_COMMENTS_KEY, BAM_FILE_FORMAT_VERSION_KEY, BAM_PROGRAM_INFO_KEY, BAM_READ_GROUPS_KEY,
    BAM_REFERENCE_SEQUENCES_KEY, BAM_SORT_ORDER_KEY, ProgramMetadata, ReadGroupMetadata,
    ReferenceSequenceMetadata, from_json_string,
};
use noodles_sam as sam;
use noodles_sam::header::record::value::Map;
use noodles_sam::header::record::value::map::{
    Program, ReadGroup, ReferenceSequence, header::Version,
};
use std::num::NonZeroUsize;

/// Metadata key for CRAM file format version (CRAM-specific override)
pub const CRAM_FILE_FORMAT_VERSION_KEY: &str = "bio.cram.file_format_version";
/// Metadata key for reference FASTA path (CRAM-specific)
pub const CRAM_REFERENCE_PATH_KEY: &str = "bio.cram.reference_path";
/// Metadata key for reference MD5 checksum (CRAM-specific)
pub const CRAM_REFERENCE_MD5_KEY: &str = "bio.cram.reference_md5";

/// Builds a SAM header from an Arrow schema for CRAM files
///
/// Reconstructs SAM header from schema metadata:
/// - File format version from `bio.cram.file_format_version` or `bio.bam.file_format_version` (defaults to "1.6")
/// - Reference sequences from `bio.bam.reference_sequences` (JSON array)
/// - Read groups from `bio.bam.read_groups` (JSON array)
/// - Program info from `bio.bam.program_info` (JSON array)
/// - Comments from `bio.bam.comments` (JSON array)
/// - Reference path from `bio.cram.reference_path` (CRAM-specific, optional)
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
    // Try CRAM-specific version first, then fall back to BAM version
    let file_format_version = schema_metadata
        .get(CRAM_FILE_FORMAT_VERSION_KEY)
        .or_else(|| schema_metadata.get(BAM_FILE_FORMAT_VERSION_KEY))
        .map(|s| s.as_str())
        .unwrap_or("1.6");

    // Set header with version
    let version: Version = file_format_version
        .parse()
        .unwrap_or_else(|_| Version::new(1, 6));
    let mut header_map = Map::<sam::header::record::value::map::Header>::new(version);

    // Set sort order if available (using other_fields)
    if let Some(sort_order_str) = schema_metadata.get(BAM_SORT_ORDER_KEY) {
        use noodles_sam::header::record::value::map::header::tag;
        header_map
            .other_fields_mut()
            .insert(tag::SORT_ORDER, sort_order_str.to_string().into());
    }

    builder = builder.set_header(header_map);

    // Add reference sequences (@SQ) - uses BAM metadata key
    if let Some(ref_seqs_json) = schema_metadata.get(BAM_REFERENCE_SEQUENCES_KEY) {
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

    // Add read groups (@RG) - uses BAM metadata key
    if let Some(read_groups_json) = schema_metadata.get(BAM_READ_GROUPS_KEY) {
        if let Some(read_groups) = from_json_string::<Vec<ReadGroupMetadata>>(read_groups_json) {
            for rg in read_groups {
                // Create read group map with available fields
                let mut rg_map = Map::<ReadGroup>::default();

                // Set optional fields using other_fields
                use noodles_sam::header::record::value::map::read_group::tag;
                if let Some(sample) = rg.sample {
                    rg_map.other_fields_mut().insert(tag::SAMPLE, sample.into());
                }
                if let Some(platform) = rg.platform {
                    rg_map
                        .other_fields_mut()
                        .insert(tag::PLATFORM, platform.into());
                }
                if let Some(library) = rg.library {
                    rg_map
                        .other_fields_mut()
                        .insert(tag::LIBRARY, library.into());
                }
                if let Some(description) = rg.description {
                    rg_map
                        .other_fields_mut()
                        .insert(tag::DESCRIPTION, description.into());
                }

                builder = builder.add_read_group(rg.id, rg_map);
            }
        }
    }

    // Add program info (@PG) - uses BAM metadata key
    if let Some(programs_json) = schema_metadata.get(BAM_PROGRAM_INFO_KEY) {
        if let Some(programs) = from_json_string::<Vec<ProgramMetadata>>(programs_json) {
            for pg in programs {
                // Create program map with available fields
                let mut pg_map = Map::<Program>::default();

                // Set optional fields using other_fields
                use noodles_sam::header::record::value::map::program::tag;
                if let Some(name) = pg.name {
                    pg_map.other_fields_mut().insert(tag::NAME, name.into());
                }
                if let Some(version) = pg.version {
                    pg_map
                        .other_fields_mut()
                        .insert(tag::VERSION, version.into());
                }
                if let Some(command_line) = pg.command_line {
                    pg_map
                        .other_fields_mut()
                        .insert(tag::COMMAND_LINE, command_line.into());
                }

                builder = builder.add_program(pg.id, pg_map);
            }
        }
    }

    // Add comments (@CO) - uses BAM metadata key
    if let Some(comments_json) = schema_metadata.get(BAM_COMMENTS_KEY) {
        if let Some(comments) = from_json_string::<Vec<String>>(comments_json) {
            for comment in comments {
                builder = builder.add_comment(comment);
            }
        }
    }

    Ok(builder.build())
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
            BAM_REFERENCE_SEQUENCES_KEY.to_string(),
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
            BAM_COMMENTS_KEY.to_string(),
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
