use datafusion::arrow::datatypes::{DataType, Field};
use noodles_sam::alignment::record::data::field::Value;
use noodles_sam::alignment::record::data::field::value::Array;
use std::collections::HashMap;
use std::sync::Arc;

/// Definition of a BAM alignment tag with type information
pub struct TagDefinition {
    /// SAM specification type code (e.g., 'i' for integer, 'Z' for string)
    pub sam_type: char,
    /// Arrow data type for this tag
    pub arrow_type: DataType,
    /// Human-readable description of the tag's purpose
    pub description: String,
}

/// Returns a comprehensive registry of SAM/BAM alignment tags
///
/// Contains 75 tags organized into categories:
/// - Alignment scoring: NM, MD, AS, XS, MQ, H0, H1, H2
/// - Read groups: RG, LB, PU, PG
/// - Single-cell: CB, UB, UR, CR, CY, UY
/// - Barcoding & molecular IDs: BC, BZ, MI, OX, QT, QX, RX
/// - Base modifications: ML, MM, MN
/// - Quality: BQ, OQ, E2, PQ, Q2, U2, UQ
/// - Pairing: MC, R2, SA, CC, CP
/// - Original: OC, OP, OA
/// - Platform: FI, TC, FS, FZ
/// - Color space: CM, CQ, CS
/// - Annotations: CO, CT, PT, TS
/// - Other: NH, HI, IH, SM, AM, X0, X1, XA, XN, XM, XO, XG, XT
/// - BAM-specific: CG
/// - Reserved: GC, GQ, GS, MF, RT, S2, SQ
pub fn get_known_tags() -> HashMap<String, TagDefinition> {
    let mut tags = HashMap::new();

    // Alignment scoring tags
    tags.insert(
        "NM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Edit distance to the reference".to_string(),
        },
    );
    tags.insert(
        "MD".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "String for mismatching positions".to_string(),
        },
    );
    tags.insert(
        "AS".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Alignment score".to_string(),
        },
    );
    tags.insert(
        "XS".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Suboptimal alignment score".to_string(),
        },
    );
    tags.insert(
        "MQ".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Mapping quality of the mate/next segment".to_string(),
        },
    );

    // Read group tags
    tags.insert(
        "RG".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Read group".to_string(),
        },
    );
    tags.insert(
        "LB".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Library".to_string(),
        },
    );
    tags.insert(
        "PU".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Platform unit".to_string(),
        },
    );
    tags.insert(
        "PG".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Program".to_string(),
        },
    );

    // Single-cell barcode tags
    tags.insert(
        "CB".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Cell barcode sequence (corrected)".to_string(),
        },
    );
    tags.insert(
        "UB".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Unique molecular identifier (UMI) barcode sequence (corrected)"
                .to_string(),
        },
    );
    tags.insert(
        "UR".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "UMI barcode sequence (uncorrected)".to_string(),
        },
    );
    tags.insert(
        "CR".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Cell barcode sequence (uncorrected)".to_string(),
        },
    );
    tags.insert(
        "CY".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Cell barcode quality scores".to_string(),
        },
    );
    tags.insert(
        "UY".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "UMI barcode quality scores".to_string(),
        },
    );

    // Quality tags
    tags.insert(
        "BQ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Base quality (offset by 33)".to_string(),
        },
    );
    tags.insert(
        "OQ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Original quality scores".to_string(),
        },
    );

    // Pairing tags
    tags.insert(
        "MC".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "CIGAR string for mate/next segment".to_string(),
        },
    );
    tags.insert(
        "R2".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Sequence of mate/next segment in template".to_string(),
        },
    );
    tags.insert(
        "SA".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Chimeric/split alignment information".to_string(),
        },
    );
    tags.insert(
        "CC".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reference name of the next hit".to_string(),
        },
    );
    tags.insert(
        "CP".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Leftmost coordinate of the next hit".to_string(),
        },
    );

    // Original tags
    tags.insert(
        "OC".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Original CIGAR".to_string(),
        },
    );
    tags.insert(
        "OP".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Original mapping position".to_string(),
        },
    );
    tags.insert(
        "OA".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Original alignment".to_string(),
        },
    );

    // Platform tags
    tags.insert(
        "FI".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Flow ion/cell identification".to_string(),
        },
    );
    tags.insert(
        "TC".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Complete read count".to_string(),
        },
    );

    // Other common tags
    tags.insert(
        "NH".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of reported alignments".to_string(),
        },
    );
    tags.insert(
        "HI".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Hit index".to_string(),
        },
    );
    tags.insert(
        "IH".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of hits".to_string(),
        },
    );
    tags.insert(
        "SM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Template-independent mapping quality".to_string(),
        },
    );
    tags.insert(
        "AM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Smallest template-independent mapping quality in the template"
                .to_string(),
        },
    );
    tags.insert(
        "X0".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of best hits".to_string(),
        },
    );
    tags.insert(
        "X1".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of suboptimal hits".to_string(),
        },
    );
    tags.insert(
        "XA".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Alternative hits".to_string(),
        },
    );
    tags.insert(
        "XN".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of ambiguous bases in the reference".to_string(),
        },
    );
    tags.insert(
        "XM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of mismatches in the alignment".to_string(),
        },
    );
    tags.insert(
        "XO".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of gap opens".to_string(),
        },
    );
    tags.insert(
        "XG".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of gap extensions".to_string(),
        },
    );
    tags.insert(
        "XT".to_string(),
        TagDefinition {
            sam_type: 'A',
            arrow_type: DataType::Utf8,
            description: "Type: Unique/Repeat/N/Mate-sw".to_string(),
        },
    );

    // Barcoding and molecular identifiers
    tags.insert(
        "BC".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Barcode sequence identifying the sample".to_string(),
        },
    );
    tags.insert(
        "BZ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Phred quality of the unique molecular barcode bases in the OX tag"
                .to_string(),
        },
    );
    tags.insert(
        "MI".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Molecular identifier (string uniquely identifying the source molecule)"
                .to_string(),
        },
    );
    tags.insert(
        "OX".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Original unique molecular barcode bases".to_string(),
        },
    );
    tags.insert(
        "QT".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Phred quality of the sample barcode sequence in the BC tag".to_string(),
        },
    );
    tags.insert(
        "QX".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Quality score of the unique molecular identifier in the RX tag"
                .to_string(),
        },
    );
    tags.insert(
        "RX".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Sequence bases of the (possibly corrected) unique molecular identifier"
                .to_string(),
        },
    );

    // Base modifications
    tags.insert(
        "ML".to_string(),
        TagDefinition {
            sam_type: 'B',
            arrow_type: DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            description: "Base modification probabilities".to_string(),
        },
    );
    tags.insert(
        "MM".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Base modifications / methylation".to_string(),
        },
    );
    tags.insert(
        "MN".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Length of sequence at the time MM and ML were produced".to_string(),
        },
    );

    // Color space sequencing
    tags.insert(
        "CM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Edit distance between the color sequence and the color reference"
                .to_string(),
        },
    );
    tags.insert(
        "CQ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Color read base qualities".to_string(),
        },
    );
    tags.insert(
        "CS".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Color read sequence".to_string(),
        },
    );

    // Quality and probability scores
    tags.insert(
        "E2".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "The 2nd most likely base calls".to_string(),
        },
    );
    tags.insert(
        "PQ".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Phred likelihood of the template".to_string(),
        },
    );
    tags.insert(
        "Q2".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Phred quality of the mate/next segment sequence in the R2 tag"
                .to_string(),
        },
    );
    tags.insert(
        "U2".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description:
                "Phred probability of the 2nd call being wrong conditional on the best being wrong"
                    .to_string(),
        },
    );
    tags.insert(
        "UQ".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Phred likelihood of the segment, conditional on mapping being correct"
                .to_string(),
        },
    );

    // Alignment hits
    tags.insert(
        "H0".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of perfect hits".to_string(),
        },
    );
    tags.insert(
        "H1".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of 1-difference hits".to_string(),
        },
    );
    tags.insert(
        "H2".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of 2-difference hits".to_string(),
        },
    );

    // Flow and platform specific
    tags.insert(
        "FS".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Segment suffix".to_string(),
        },
    );
    tags.insert(
        "FZ".to_string(),
        TagDefinition {
            sam_type: 'B',
            arrow_type: DataType::List(Arc::new(Field::new("item", DataType::UInt16, true))),
            description: "Flow signal intensities".to_string(),
        },
    );

    // Annotations
    tags.insert(
        "CO".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Free-text comments".to_string(),
        },
    );
    tags.insert(
        "CT".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Complete read annotation tag (consensus annotation dummy features)"
                .to_string(),
        },
    );
    tags.insert(
        "PT".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Read annotations for parts of the padded read sequence".to_string(),
        },
    );
    tags.insert(
        "TS".to_string(),
        TagDefinition {
            sam_type: 'A',
            arrow_type: DataType::Utf8,
            description: "Transcript strand".to_string(),
        },
    );

    // 10X Genomics Cell Ranger tags
    tags.insert(
        "RE".to_string(),
        TagDefinition {
            sam_type: 'A',
            arrow_type: DataType::Utf8,
            description: "10X: read type indicator (I=intronic, N=intergenic, E=exonic)"
                .to_string(),
        },
    );
    tags.insert(
        "xf".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "10X: extra alignment flags".to_string(),
        },
    );
    tags.insert(
        "nM".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of mismatches per (nM:i tag, lowercase)".to_string(),
        },
    );
    tags.insert(
        "ts".to_string(),
        TagDefinition {
            sam_type: 'i',
            arrow_type: DataType::Int32,
            description: "Number of trimmed bases at transcript start".to_string(),
        },
    );

    // BAM-specific
    tags.insert(
        "CG".to_string(),
        TagDefinition {
            sam_type: 'B',
            arrow_type: DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            description:
                "BAM-only: CIGAR in BAM's binary encoding if it consists of >65535 operators"
                    .to_string(),
        },
    );

    // Reserved for backwards compatibility
    tags.insert(
        "GC".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "GQ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "GS".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "MF".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "RT".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "S2".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );
    tags.insert(
        "SQ".to_string(),
        TagDefinition {
            sam_type: 'Z',
            arrow_type: DataType::Utf8,
            description: "Reserved for backwards compatibility reasons".to_string(),
        },
    );

    tags
}

/// Convert SAM tag type character to Arrow DataType
#[allow(dead_code)]
pub fn sam_tag_type_to_arrow_type(sam_type: char) -> DataType {
    match sam_type {
        'A' => DataType::Utf8,    // Character
        'i' => DataType::Int32,   // Integer
        'f' => DataType::Float32, // Float
        'Z' => DataType::Utf8,    // String
        'H' => DataType::Utf8,    // Hex string
        _ => DataType::Utf8,      // Default to string for unknown types
    }
}

/// Infer SAM type and Arrow type from a noodles Value
/// Used for runtime type validation and schema discovery
pub fn infer_type_from_noodles_value(value: &Value) -> (char, DataType) {
    match value {
        Value::Character(_) => ('A', DataType::Utf8),
        Value::Int8(_)
        | Value::UInt8(_)
        | Value::Int16(_)
        | Value::UInt16(_)
        | Value::Int32(_)
        | Value::UInt32(_) => ('i', DataType::Int32),
        Value::Float(_) => ('f', DataType::Float32),
        Value::String(_) => ('Z', DataType::Utf8),
        Value::Hex(_) => ('H', DataType::Utf8),
        Value::Array(arr) => match arr {
            Array::Int8(_)
            | Array::UInt8(_)
            | Array::Int16(_)
            | Array::UInt16(_)
            | Array::Int32(_)
            | Array::UInt32(_) => (
                'B',
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            ),
            Array::Float(_) => (
                'B',
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_tags_coverage() {
        let tags = get_known_tags();

        // Test some common alignment tags
        assert!(tags.contains_key("NM"));
        assert!(tags.contains_key("MD"));
        assert!(tags.contains_key("AS"));

        // Verify types
        assert_eq!(tags["NM"].sam_type, 'i');
        assert_eq!(tags["NM"].arrow_type, DataType::Int32);

        assert_eq!(tags["MD"].sam_type, 'Z');
        assert_eq!(tags["MD"].arrow_type, DataType::Utf8);

        // Test single-cell tags
        assert!(tags.contains_key("CB"));
        assert!(tags.contains_key("UB"));

        // Should have ~75 tags (40 original + 30 from SAM spec + 10X tags)
        assert!(tags.len() >= 75);
    }

    #[test]
    fn test_type_mapping() {
        assert_eq!(sam_tag_type_to_arrow_type('i'), DataType::Int32);
        assert_eq!(sam_tag_type_to_arrow_type('Z'), DataType::Utf8);
        assert_eq!(sam_tag_type_to_arrow_type('A'), DataType::Utf8);
        assert_eq!(sam_tag_type_to_arrow_type('f'), DataType::Float32);
        assert_eq!(sam_tag_type_to_arrow_type('H'), DataType::Utf8);
    }

    #[test]
    fn test_tag_descriptions() {
        let tags = get_known_tags();

        // Verify descriptions are present
        assert!(!tags["NM"].description.is_empty());
        assert!(!tags["AS"].description.is_empty());
        assert!(!tags["CB"].description.is_empty());
    }
}
