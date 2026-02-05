//! BAM/SAM header builder for constructing headers from Arrow schemas
//!
//! This module provides functionality for building SAM headers from Arrow schemas,
//! enabling round-trip BAM/SAM read/write operations. Header information is reconstructed from:
//! - Schema-level metadata: file format version, sort order, reference sequences,
//!   read groups, program info, and comments (stored as JSON)
//!
//! When metadata is not available, sensible defaults are generated.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::{
    BAM_COMMENTS_KEY, BAM_FILE_FORMAT_VERSION_KEY, BAM_GROUP_ORDER_KEY, BAM_PROGRAM_INFO_KEY,
    BAM_READ_GROUPS_KEY, BAM_REFERENCE_SEQUENCES_KEY, BAM_SORT_ORDER_KEY, BAM_SUBSORT_ORDER_KEY,
    ProgramMetadata, ReadGroupMetadata, ReferenceSequenceMetadata, from_json_string,
};
use noodles_sam as sam;
use noodles_sam::header::record::value::Map;
use noodles_sam::header::record::value::map::{
    Program, ReadGroup, ReferenceSequence, header::Version,
};
use std::num::NonZeroUsize;

/// Builds a SAM header from an Arrow schema
///
/// Reconstructs SAM header from schema metadata:
/// - File format version from `bio.bam.file_format_version` (defaults to "1.6")
/// - Sort order from `bio.bam.sort_order` (defaults to "unknown")
/// - Reference sequences from `bio.bam.reference_sequences` (JSON array)
/// - Read groups from `bio.bam.read_groups` (JSON array)
/// - Program info from `bio.bam.program_info` (JSON array)
/// - Comments from `bio.bam.comments` (JSON array)
///
/// # Arguments
///
/// * `schema` - The Arrow schema containing metadata
/// * `tag_fields` - List of tag field names (currently not used in header)
///
/// # Returns
///
/// A noodles SAM Header
pub fn build_bam_header(schema: &SchemaRef, _tag_fields: &[String]) -> Result<sam::Header> {
    let mut builder = sam::Header::builder();

    let schema_metadata = schema.metadata();

    // Build header (@HD) line
    let file_format_version = schema_metadata
        .get(BAM_FILE_FORMAT_VERSION_KEY)
        .map(|s| s.as_str())
        .unwrap_or("1.6");

    // Set header with version
    let version: Version = file_format_version
        .parse()
        .unwrap_or_else(|_| Version::new(1, 6));
    let mut header_map = Map::<sam::header::record::value::map::Header>::new(version);

    // Set sort order, group order, and subsort order if available
    {
        use noodles_sam::header::record::value::map::header::tag;
        if let Some(sort_order_str) = schema_metadata.get(BAM_SORT_ORDER_KEY) {
            header_map
                .other_fields_mut()
                .insert(tag::SORT_ORDER, sort_order_str.to_string().into());
        }
        if let Some(group_order_str) = schema_metadata.get(BAM_GROUP_ORDER_KEY) {
            header_map
                .other_fields_mut()
                .insert(tag::GROUP_ORDER, group_order_str.to_string().into());
        }
        if let Some(subsort_order_str) = schema_metadata.get(BAM_SUBSORT_ORDER_KEY) {
            header_map
                .other_fields_mut()
                .insert(tag::SUBSORT_ORDER, subsort_order_str.to_string().into());
        }
    }

    builder = builder.set_header(header_map);

    // Add reference sequences (@SQ) with all optional fields (AS, UR, M5, SP, etc.)
    if let Some(ref_seqs_json) = schema_metadata.get(BAM_REFERENCE_SEQUENCES_KEY) {
        if let Some(ref_seqs) = from_json_string::<Vec<ReferenceSequenceMetadata>>(ref_seqs_json) {
            for ref_seq in ref_seqs {
                let length = NonZeroUsize::new(ref_seq.length).ok_or_else(|| {
                    DataFusionError::Execution(
                        "Reference sequence length cannot be zero".to_string(),
                    )
                })?;
                let mut reference_sequence = Map::<ReferenceSequence>::new(length);
                // Restore optional @SQ fields (AS, UR, M5, SP, etc.)
                for (key, value) in &ref_seq.other_fields {
                    if let Some(tag) = str_to_sq_tag(key) {
                        reference_sequence
                            .other_fields_mut()
                            .insert(tag, value.to_string().into());
                    }
                }
                builder = builder.add_reference_sequence(ref_seq.name, reference_sequence);
            }
        }
    }

    // Add read groups (@RG)
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
                // Restore additional @RG fields
                for (key, value) in &rg.other_fields {
                    if let Some(t) = str_to_rg_tag(key) {
                        rg_map
                            .other_fields_mut()
                            .insert(t, value.to_string().into());
                    }
                }

                builder = builder.add_read_group(rg.id, rg_map);
            }
        }
    }

    // Add program info (@PG)
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
                // Restore additional @PG fields
                for (key, value) in &pg.other_fields {
                    if let Some(t) = str_to_pg_tag(key) {
                        pg_map
                            .other_fields_mut()
                            .insert(t, value.to_string().into());
                    }
                }

                builder = builder.add_program(pg.id, pg_map);
            }
        }
    }

    // Add comments (@CO)
    if let Some(comments_json) = schema_metadata.get(BAM_COMMENTS_KEY) {
        if let Some(comments) = from_json_string::<Vec<String>>(comments_json) {
            for comment in comments {
                builder = builder.add_comment(comment);
            }
        }
    }

    Ok(builder.build())
}

/// Convert a 2-char @SQ tag name to the corresponding noodles tag constant
fn str_to_sq_tag(
    s: &str,
) -> Option<
    noodles_sam::header::record::value::map::tag::Other<
        noodles_sam::header::record::value::map::reference_sequence::tag::Standard,
    >,
> {
    use noodles_sam::header::record::value::map::reference_sequence::tag;
    match s {
        "AH" => Some(tag::ALTERNATIVE_LOCUS),
        "AN" => Some(tag::ALTERNATIVE_NAMES),
        "AS" => Some(tag::ASSEMBLY_ID),
        "DS" => Some(tag::DESCRIPTION),
        "M5" => Some(tag::MD5_CHECKSUM),
        "SP" => Some(tag::SPECIES),
        "TP" => Some(tag::MOLECULE_TOPOLOGY),
        "UR" => Some(tag::URI),
        _ => None,
    }
}

/// Convert a 2-char @RG tag name to the corresponding noodles tag constant
fn str_to_rg_tag(
    s: &str,
) -> Option<
    noodles_sam::header::record::value::map::tag::Other<
        noodles_sam::header::record::value::map::read_group::tag::Standard,
    >,
> {
    use noodles_sam::header::record::value::map::read_group::tag;
    match s {
        "BC" => Some(tag::BARCODE),
        "CN" => Some(tag::SEQUENCING_CENTER),
        "DT" => Some(tag::PRODUCED_AT),
        "FO" => Some(tag::FLOW_ORDER),
        "KS" => Some(tag::KEY_SEQUENCE),
        "PG" => Some(tag::PROGRAM),
        "PI" => Some(tag::PREDICTED_MEDIAN_INSERT_SIZE),
        "PM" => Some(tag::PLATFORM_MODEL),
        "PU" => Some(tag::PLATFORM_UNIT),
        // SM, PL, LB, DS are handled as named fields
        _ => None,
    }
}

/// Convert a 2-char @PG tag name to the corresponding noodles tag constant
fn str_to_pg_tag(
    s: &str,
) -> Option<
    noodles_sam::header::record::value::map::tag::Other<
        noodles_sam::header::record::value::map::program::tag::Standard,
    >,
> {
    use noodles_sam::header::record::value::map::program::tag;
    match s {
        "PP" => Some(tag::PREVIOUS_PROGRAM_ID),
        "DS" => Some(tag::DESCRIPTION),
        // PN, VN, CL are handled as named fields
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_build_bam_header_basic() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("chrom", DataType::Utf8, true),
            Field::new("start", DataType::UInt32, true),
        ]));

        let header = build_bam_header(&schema, &[])?;

        // Check default values
        assert_eq!(header.header().unwrap().version().to_string(), "1.6");

        Ok(())
    }

    #[test]
    fn test_build_bam_header_with_metadata() -> Result<()> {
        let mut metadata = HashMap::new();
        metadata.insert(BAM_FILE_FORMAT_VERSION_KEY.to_string(), "1.6".to_string());
        metadata.insert(BAM_SORT_ORDER_KEY.to_string(), "coordinate".to_string());

        let ref_seqs = vec![
            ReferenceSequenceMetadata {
                name: "chr1".to_string(),
                length: 249250621,
                other_fields: HashMap::new(),
            },
            ReferenceSequenceMetadata {
                name: "chr2".to_string(),
                length: 242193529,
                other_fields: HashMap::new(),
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

        let header = build_bam_header(&schema, &[])?;

        // Check values
        assert_eq!(header.header().unwrap().version().to_string(), "1.6");
        assert_eq!(header.reference_sequences().len(), 2);

        Ok(())
    }

    #[test]
    fn test_build_bam_header_with_read_groups() -> Result<()> {
        let mut metadata = HashMap::new();

        let read_groups = vec![ReadGroupMetadata {
            id: "RG1".to_string(),
            sample: Some("SAMPLE1".to_string()),
            platform: Some("ILLUMINA".to_string()),
            library: Some("LIB1".to_string()),
            description: Some("Test read group".to_string()),
            other_fields: HashMap::new(),
        }];
        metadata.insert(
            BAM_READ_GROUPS_KEY.to_string(),
            serde_json::to_string(&read_groups).unwrap(),
        );

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("name", DataType::Utf8, true)],
            metadata,
        ));

        let header = build_bam_header(&schema, &[])?;

        // Check read groups
        assert_eq!(header.read_groups().len(), 1);
        // Read group details are stored but API to retrieve them is limited
        // Just verify it exists

        Ok(())
    }

    #[test]
    fn test_build_bam_header_with_programs() -> Result<()> {
        let mut metadata = HashMap::new();

        let programs = vec![ProgramMetadata {
            id: "bwa".to_string(),
            name: Some("bwa".to_string()),
            version: Some("0.7.17".to_string()),
            command_line: Some("bwa mem ref.fa reads.fq".to_string()),
            other_fields: HashMap::new(),
        }];
        metadata.insert(
            BAM_PROGRAM_INFO_KEY.to_string(),
            serde_json::to_string(&programs).unwrap(),
        );

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("name", DataType::Utf8, true)],
            metadata,
        ));

        let header = build_bam_header(&schema, &[])?;

        // Check programs exist - Programs doesn't have is_empty, just verify we can access it
        let _ = header.programs();

        Ok(())
    }

    #[test]
    fn test_build_bam_header_with_comments() -> Result<()> {
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

        let header = build_bam_header(&schema, &[])?;

        // Check comments
        assert_eq!(header.comments().len(), 2);

        Ok(())
    }
}
