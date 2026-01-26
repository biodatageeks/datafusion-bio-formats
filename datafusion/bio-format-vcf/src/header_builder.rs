//! VCF header builder for constructing VCF headers from Arrow schemas
//!
//! This module provides functionality for building VCF header lines from Arrow schemas,
//! enabling round-trip VCF read/write operations. When field metadata is available
//! (from reading a VCF file), it preserves original descriptions, types, and numbers.

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use std::collections::HashSet;

/// Index of the CHROM column in VCF schema
pub const CHROM_IDX: usize = 0;
/// Index of the START (POS) column in VCF schema
pub const START_IDX: usize = 1;
/// Index of the END column in VCF schema
pub const END_IDX: usize = 2;
/// Index of the ID column in VCF schema
pub const ID_IDX: usize = 3;
/// Index of the REF column in VCF schema
pub const REF_IDX: usize = 4;
/// Index of the ALT column in VCF schema
pub const ALT_IDX: usize = 5;
/// Index of the QUAL column in VCF schema
pub const QUAL_IDX: usize = 6;
/// Index of the FILTER column in VCF schema
pub const FILTER_IDX: usize = 7;
/// Number of core VCF columns (before INFO fields)
pub const CORE_FIELD_COUNT: usize = 8;

/// Builds VCF header lines from an Arrow schema with INFO and FORMAT field definitions
///
/// If the schema fields contain VCF metadata (vcf_description, vcf_type, vcf_number),
/// those values are used. Otherwise, defaults are generated from the Arrow types.
///
/// # Arguments
///
/// * `schema` - The Arrow schema containing field definitions
/// * `info_fields` - List of INFO field names to include
/// * `format_fields` - List of FORMAT field names per sample
/// * `sample_names` - List of sample names from the original VCF
///
/// # Returns
///
/// A vector of VCF header lines (without the column header line)
pub fn build_vcf_header_lines(
    schema: &SchemaRef,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
) -> Result<Vec<String>> {
    let mut lines = Vec::new();

    // File format line
    lines.push("##fileformat=VCFv4.3".to_string());

    // Add INFO field definitions
    for info_name in info_fields {
        // Look up field by name to support any column order
        if let Ok(field_idx) = schema.index_of(info_name) {
            let field = schema.field(field_idx);
            let (vcf_type, number, description) = get_info_field_metadata(field, info_name);

            lines.push(format!(
                "##INFO=<ID={},Number={},Type={},Description=\"{}\">",
                info_name, number, vcf_type, description
            ));
        }
    }

    // Add FORMAT field definitions
    let unique_format_fields: HashSet<_> = format_fields.iter().collect();

    for format_name in unique_format_fields {
        // Find the first occurrence to get the type (try both naming conventions)
        if let Some(field) = find_format_field(schema, format_name, sample_names) {
            let (vcf_type, number, description) = get_format_field_metadata(field, format_name);

            lines.push(format!(
                "##FORMAT=<ID={},Number={},Type={},Description=\"{}\">",
                format_name, number, vcf_type, description
            ));
        }
    }

    Ok(lines)
}

/// Extracts VCF metadata from an INFO field, using stored metadata if available
fn get_info_field_metadata(field: &Field, field_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // Try to get stored VCF metadata
    let vcf_type = metadata
        .get("vcf_type")
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_type(field.data_type()).to_string());

    let number = metadata
        .get("vcf_number")
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(field.data_type()).to_string());

    let description = metadata
        .get("vcf_description")
        .cloned()
        .unwrap_or_else(|| format!("{} field", field_name));

    (vcf_type, number, description)
}

/// Extracts VCF metadata from a FORMAT field, using stored metadata if available
fn get_format_field_metadata(field: &Field, format_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // Try to get stored VCF metadata
    let vcf_type = metadata.get("vcf_type").cloned().unwrap_or_else(|| {
        // GT is always a string
        if format_name == "GT" {
            "String".to_string()
        } else {
            arrow_type_to_vcf_type(field.data_type()).to_string()
        }
    });

    let number = metadata
        .get("vcf_number")
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(field.data_type()).to_string());

    let description = metadata
        .get("vcf_description")
        .cloned()
        .unwrap_or_else(|| format!("{} format field", format_name));

    (vcf_type, number, description)
}

/// Finds a FORMAT field in the schema by name (handles both single and multi-sample naming)
fn find_format_field<'a>(
    schema: &'a SchemaRef,
    format_name: &str,
    sample_names: &[String],
) -> Option<&'a Field> {
    // First try direct name lookup (single sample case)
    if let Ok(idx) = schema.index_of(format_name) {
        return Some(schema.field(idx));
    }

    // Try multi-sample naming convention: {sample}_{format}
    for sample_name in sample_names {
        let column_name = format!("{}_{}", sample_name, format_name);
        if let Ok(idx) = schema.index_of(&column_name) {
            return Some(schema.field(idx));
        }
    }

    None
}

/// Builds the VCF column header line
///
/// # Arguments
///
/// * `sample_names` - List of sample names
///
/// # Returns
///
/// The column header line (starting with #CHROM)
pub fn build_vcf_column_header(sample_names: &[String]) -> String {
    let mut header = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO".to_string();

    if !sample_names.is_empty() {
        header.push_str("\tFORMAT");
        for sample in sample_names {
            header.push('\t');
            header.push_str(sample);
        }
    }

    header
}

/// Converts Arrow DataType to VCF type string
fn arrow_type_to_vcf_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int32 => "Integer",
        DataType::Float32 | DataType::Float64 => "Float",
        DataType::Boolean => "Flag",
        DataType::Utf8 | DataType::LargeUtf8 => "String",
        DataType::List(inner) => match inner.data_type() {
            DataType::Int32 => "Integer",
            DataType::Float32 | DataType::Float64 => "Float",
            _ => "String",
        },
        _ => "String",
    }
}

/// Converts Arrow DataType to VCF Number string
fn arrow_type_to_vcf_number(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "0", // Flag type
        DataType::List(_) => ".", // Variable length
        _ => "1",                 // Single value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_build_vcf_header_lines_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        assert!(lines.iter().any(|l| l.contains("##fileformat=")));
        assert!(lines.iter().any(|l| l.contains("##INFO=<ID=DP")));
    }

    #[test]
    fn test_build_vcf_header_lines_with_metadata() {
        // Create field with VCF metadata
        let mut dp_metadata = HashMap::new();
        dp_metadata.insert("vcf_description".to_string(), "Read Depth".to_string());
        dp_metadata.insert("vcf_number".to_string(), "1".to_string());
        dp_metadata.insert("vcf_type".to_string(), "Integer".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true).with_metadata(dp_metadata),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        // Should use the original description from metadata
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Description=\"Read Depth\""))
        );
    }

    #[test]
    fn test_build_vcf_column_header_no_samples() {
        let header = build_vcf_column_header(&[]);
        assert_eq!(header, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
    }

    #[test]
    fn test_build_vcf_column_header_with_samples() {
        let header = build_vcf_column_header(&["SAMPLE1".to_string(), "SAMPLE2".to_string()]);
        assert!(header.contains("FORMAT"));
        assert!(header.contains("SAMPLE1"));
        assert!(header.contains("SAMPLE2"));
    }

    #[test]
    fn test_find_format_field_single_sample() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("GT", DataType::Utf8, true),
        ]));

        let field = find_format_field(&schema, "GT", &["SAMPLE1".to_string()]);
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GT");
    }

    #[test]
    fn test_find_format_field_multi_sample() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("SAMPLE1_GT", DataType::Utf8, true),
            Field::new("SAMPLE2_GT", DataType::Utf8, true),
        ]));

        let field = find_format_field(
            &schema,
            "GT",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "SAMPLE1_GT");
    }
}
