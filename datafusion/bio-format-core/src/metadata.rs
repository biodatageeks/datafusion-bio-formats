//! Generic bioinformatics metadata handling for Arrow schemas
//!
//! This module provides standardized metadata key constants and serialization
//! utilities for storing bioinformatics file headers in Arrow schema metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Generic Metadata Keys (All Formats)
// ============================================================================

/// Coordinate system: "true" = 0-based half-open [start, end), "false" = 1-based closed [start, end]
pub const COORDINATE_SYSTEM_METADATA_KEY: &str = "bio.coordinate_system_zero_based";

/// File format version (e.g., "VCFv4.3", "BAM/1.6")
pub const BIO_FILE_FORMAT_VERSION_KEY: &str = "bio.file_format_version";

/// Compression type (e.g., "GZIP", "BGZF", "NONE")
pub const BIO_COMPRESSION_TYPE_KEY: &str = "bio.compression_type";

/// Source file URI
pub const BIO_SOURCE_URI_KEY: &str = "bio.source_uri";

// ============================================================================
// VCF-Specific Metadata Keys
// ============================================================================

// Schema-level metadata

/// VCF file format version (e.g., "VCFv4.3") stored in schema metadata
pub const VCF_FILE_FORMAT_KEY: &str = "bio.vcf.file_format";

/// VCF FILTER definitions stored as JSON array of FilterMetadata
pub const VCF_FILTERS_KEY: &str = "bio.vcf.filters";

/// VCF CONTIG definitions stored as JSON array of ContigMetadata
pub const VCF_CONTIGS_KEY: &str = "bio.vcf.contigs";

/// VCF ALT allele definitions stored as JSON array of AltAlleleMetadata
pub const VCF_ALTERNATIVE_ALLELES_KEY: &str = "bio.vcf.alternative_alleles";

/// VCF sample names stored as JSON array of strings
pub const VCF_SAMPLE_NAMES_KEY: &str = "bio.vcf.samples";

// Field-level metadata

/// VCF field description stored in field metadata
pub const VCF_FIELD_DESCRIPTION_KEY: &str = "bio.vcf.field.description";

/// VCF field type (Integer, Float, String, Flag) stored in field metadata
pub const VCF_FIELD_TYPE_KEY: &str = "bio.vcf.field.type";

/// VCF field number (1, A, R, G, .) stored in field metadata
pub const VCF_FIELD_NUMBER_KEY: &str = "bio.vcf.field.number";

/// VCF field type category (INFO or FORMAT) stored in field metadata
pub const VCF_FIELD_FIELD_TYPE_KEY: &str = "bio.vcf.field.field_type";

/// VCF FORMAT field ID for multi-sample columns (e.g., "GT" for Sample1_GT)
pub const VCF_FIELD_FORMAT_ID_KEY: &str = "bio.vcf.field.format_id";

// ============================================================================
// BAM-Specific Metadata Keys
// ============================================================================

/// BAM file format version (e.g., "1.6") stored in schema metadata
pub const BAM_FILE_FORMAT_VERSION_KEY: &str = "bio.bam.file_format_version";

/// BAM reference sequences (contigs) stored as JSON array
pub const BAM_REFERENCE_SEQUENCES_KEY: &str = "bio.bam.reference_sequences";

/// BAM read groups (@RG) stored as JSON array
pub const BAM_READ_GROUPS_KEY: &str = "bio.bam.read_groups";

/// BAM program info (@PG) stored as JSON array
pub const BAM_PROGRAM_INFO_KEY: &str = "bio.bam.program_info";

/// BAM comments (@CO) stored as JSON array
pub const BAM_COMMENTS_KEY: &str = "bio.bam.comments";

/// BAM optional tag name (e.g., "NM", "MD") stored in field metadata
pub const BAM_TAG_TAG_KEY: &str = "bio.bam.tag.tag";

/// BAM optional tag SAM type (e.g., "i", "Z", "f") stored in field metadata
pub const BAM_TAG_TYPE_KEY: &str = "bio.bam.tag.type";

/// BAM optional tag description stored in field metadata
pub const BAM_TAG_DESCRIPTION_KEY: &str = "bio.bam.tag.description";

// ============================================================================
// GFF-Specific Metadata Keys (For Future Use)
// ============================================================================

/// GFF version (e.g., "3") stored in schema metadata
pub const GFF_VERSION_KEY: &str = "bio.gff.version";

/// GFF directives (##gff-version, ##genome-build, etc.) stored as JSON object
pub const GFF_DIRECTIVES_KEY: &str = "bio.gff.directives";

/// GFF sequence-region directives stored as JSON array
pub const GFF_SEQUENCE_REGIONS_KEY: &str = "bio.gff.sequence_regions";

// ============================================================================
// BED-Specific Metadata Keys (For Future Use)
// ============================================================================

/// BED variant type (BED3, BED6, BED12, etc.) stored in schema metadata
pub const BED_VARIANT_KEY: &str = "bio.bed.variant";

/// BED track metadata (name, description, color, etc.) stored as JSON object
pub const BED_TRACK_METADATA_KEY: &str = "bio.bed.track_metadata";

/// BED browser directives stored as JSON array
pub const BED_BROWSER_LINES_KEY: &str = "bio.bed.browser_lines";

// ============================================================================
// Shared Metadata Structures
// ============================================================================

/// Generic filter metadata (used by VCF, potentially others)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterMetadata {
    /// Filter ID (e.g., "PASS", "LowQual")
    pub id: String,
    /// Filter description
    pub description: String,
}

/// Generic contig/reference sequence metadata
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContigMetadata {
    /// Contig/chromosome ID (e.g., "chr1", "1")
    pub id: String,
    /// Contig length in base pairs (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    /// Additional metadata key-value pairs
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
}

/// Alternative allele metadata (VCF-specific)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AltAlleleMetadata {
    /// ALT allele ID (e.g., "DEL", "INS", "DUP")
    pub id: String,
    /// ALT allele description
    pub description: String,
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Serialize a value to JSON string, returning empty string on failure
pub fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::new())
}

/// Deserialize from JSON string, returning None on failure
pub fn from_json_string<'a, T: Deserialize<'a>>(json: &'a str) -> Option<T> {
    serde_json::from_str(json).ok()
}
