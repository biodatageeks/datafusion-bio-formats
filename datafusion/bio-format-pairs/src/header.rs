//! Pairs file header parsing and schema derivation.
//!
//! The Pairs format uses `#`-prefixed header lines before the data. Key directives:
//! - `## pairs format v1.0` — format version
//! - `#columns: readID chr1 pos1 chr2 pos2 strand1 strand2` — column definitions
//! - `#chromsize: chr1 249250621` — chromosome sizes
//! - `#sorted: chr1-chr2-pos1-pos2` — sort order
//! - `#shape: upper triangle` — matrix shape
//! - `#genome_assembly: hg38` — reference genome

use datafusion::arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};
use std::io::BufRead;

/// Chromosome size entry from `#chromsize:` header lines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChromSize {
    /// Chromosome name
    pub name: String,
    /// Chromosome length in base pairs
    pub length: u64,
}

/// Parsed Pairs file header.
#[derive(Debug, Clone)]
pub struct PairsHeader {
    /// Format version from `## pairs format vX.Y`
    pub format_version: String,
    /// Column names from `#columns:` line
    pub columns: Vec<String>,
    /// Chromosome sizes from `#chromsize:` lines
    pub chromsizes: Vec<ChromSize>,
    /// Sort order from `#sorted:` line
    pub sorted: Option<String>,
    /// Matrix shape from `#shape:` line
    pub shape: Option<String>,
    /// Genome assembly from `#genome_assembly:` line
    pub genome_assembly: Option<String>,
    /// Processing commands from `#command:` lines
    pub commands: Vec<String>,
}

impl Default for PairsHeader {
    fn default() -> Self {
        Self {
            format_version: "1.0".to_string(),
            columns: vec![
                "readID".to_string(),
                "chr1".to_string(),
                "pos1".to_string(),
                "chr2".to_string(),
                "pos2".to_string(),
                "strand1".to_string(),
                "strand2".to_string(),
            ],
            chromsizes: Vec::new(),
            sorted: None,
            shape: None,
            genome_assembly: None,
            commands: Vec::new(),
        }
    }
}

/// Parse the Pairs file header from a buffered reader.
///
/// Reads all `#`-prefixed lines until the first data line (or EOF).
/// The reader is left positioned at the first data line.
pub fn parse_pairs_header(reader: &mut impl BufRead) -> std::io::Result<PairsHeader> {
    let mut header = PairsHeader {
        format_version: String::new(),
        columns: Vec::new(),
        chromsizes: Vec::new(),
        sorted: None,
        shape: None,
        genome_assembly: None,
        commands: Vec::new(),
    };

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break; // EOF
        }

        let trimmed = line.trim();

        // Format version line: ## pairs format v1.0
        if trimmed.starts_with("## pairs format") {
            if let Some(version) = trimmed.strip_prefix("## pairs format v") {
                header.format_version = version.trim().to_string();
            } else if let Some(version) = trimmed.strip_prefix("## pairs format ") {
                header.format_version = version.trim().to_string();
            }
            continue;
        }

        // Other header lines start with #
        if !trimmed.starts_with('#') {
            break; // First data line — stop
        }

        // Remove leading # and parse directive
        let directive = &trimmed[1..];

        if let Some(cols) = directive.strip_prefix("columns:") {
            header.columns = cols.split_whitespace().map(|s| s.to_string()).collect();
        } else if let Some(cs) = directive.strip_prefix("chromsize:") {
            let parts: Vec<&str> = cs.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(length) = parts[1].parse::<u64>() {
                    header.chromsizes.push(ChromSize {
                        name: parts[0].to_string(),
                        length,
                    });
                }
            }
        } else if let Some(sorted) = directive.strip_prefix("sorted:") {
            header.sorted = Some(sorted.trim().to_string());
        } else if let Some(shape) = directive.strip_prefix("shape:") {
            header.shape = Some(shape.trim().to_string());
        } else if let Some(genome) = directive.strip_prefix("genome_assembly:") {
            header.genome_assembly = Some(genome.trim().to_string());
        } else if let Some(cmd) = directive.strip_prefix("command:") {
            header.commands.push(cmd.trim().to_string());
        }
        // Ignore unknown directives
    }

    // If no columns line was found, use default 7-column layout
    if header.columns.is_empty() {
        header.columns = vec![
            "readID".to_string(),
            "chr1".to_string(),
            "pos1".to_string(),
            "chr2".to_string(),
            "pos2".to_string(),
            "strand1".to_string(),
            "strand2".to_string(),
        ];
    }

    // Default version if not found
    if header.format_version.is_empty() {
        header.format_version = "1.0".to_string();
    }

    Ok(header)
}

/// Map Pairs column names to Arrow field definitions.
///
/// Standard column types:
/// - `readID` → Utf8 (not null)
/// - `chr1`, `chr2` → Utf8 (not null)
/// - `pos1`, `pos2` → UInt32 (not null)
/// - `strand1`, `strand2` → Utf8 (not null)
/// - `frag1`, `frag2`, `mapq1`, `mapq2` → UInt32 (nullable)
/// - Unknown columns → Utf8 (nullable)
pub fn columns_to_arrow_fields(columns: &[String]) -> Vec<Field> {
    columns
        .iter()
        .map(|col| match col.as_str() {
            "readID" => Field::new("readID", DataType::Utf8, false),
            "chr1" => Field::new("chr1", DataType::Utf8, false),
            "chr2" => Field::new("chr2", DataType::Utf8, false),
            "pos1" => Field::new("pos1", DataType::UInt32, false),
            "pos2" => Field::new("pos2", DataType::UInt32, false),
            "strand1" => Field::new("strand1", DataType::Utf8, false),
            "strand2" => Field::new("strand2", DataType::Utf8, false),
            "frag1" => Field::new("frag1", DataType::UInt32, true),
            "frag2" => Field::new("frag2", DataType::UInt32, true),
            "mapq1" => Field::new("mapq1", DataType::UInt32, true),
            "mapq2" => Field::new("mapq2", DataType::UInt32, true),
            other => Field::new(other, DataType::Utf8, true),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_parse_full_header() {
        let header_text = "\
## pairs format v1.0
#sorted: chr1-chr2-pos1-pos2
#shape: upper triangle
#genome_assembly: hg38
#chromsize: chr1 249250621
#chromsize: chr2 243199373
#columns: readID chr1 pos1 chr2 pos2 strand1 strand2
#command: pairtools parse input.sam
";
        let mut cursor = Cursor::new(header_text);
        let header = parse_pairs_header(&mut cursor).unwrap();

        assert_eq!(header.format_version, "1.0");
        assert_eq!(
            header.columns,
            vec![
                "readID", "chr1", "pos1", "chr2", "pos2", "strand1", "strand2"
            ]
        );
        assert_eq!(header.chromsizes.len(), 2);
        assert_eq!(header.chromsizes[0].name, "chr1");
        assert_eq!(header.chromsizes[0].length, 249250621);
        assert_eq!(header.sorted.as_deref(), Some("chr1-chr2-pos1-pos2"));
        assert_eq!(header.shape.as_deref(), Some("upper triangle"));
        assert_eq!(header.genome_assembly.as_deref(), Some("hg38"));
        assert_eq!(header.commands.len(), 1);
    }

    #[test]
    fn test_parse_minimal_header() {
        let header_text =
            "## pairs format v1.0\n#columns: readID chr1 pos1 chr2 pos2 strand1 strand2\n";
        let mut cursor = Cursor::new(header_text);
        let header = parse_pairs_header(&mut cursor).unwrap();

        assert_eq!(header.format_version, "1.0");
        assert_eq!(header.columns.len(), 7);
        assert!(header.chromsizes.is_empty());
        assert!(header.sorted.is_none());
    }

    #[test]
    fn test_parse_no_columns_line() {
        let header_text = "## pairs format v1.0\n";
        let mut cursor = Cursor::new(header_text);
        let header = parse_pairs_header(&mut cursor).unwrap();

        // Should use default 7-column layout
        assert_eq!(header.columns.len(), 7);
        assert_eq!(header.columns[0], "readID");
    }

    #[test]
    fn test_parse_extra_columns() {
        let header_text = "## pairs format v1.0\n#columns: readID chr1 pos1 chr2 pos2 strand1 strand2 frag1 frag2 mapq1 mapq2\n";
        let mut cursor = Cursor::new(header_text);
        let header = parse_pairs_header(&mut cursor).unwrap();

        assert_eq!(header.columns.len(), 11);
        assert_eq!(header.columns[7], "frag1");
        assert_eq!(header.columns[10], "mapq2");
    }

    #[test]
    fn test_columns_to_arrow_fields_standard() {
        let columns: Vec<String> = vec![
            "readID", "chr1", "pos1", "chr2", "pos2", "strand1", "strand2",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let fields = columns_to_arrow_fields(&columns);
        assert_eq!(fields.len(), 7);
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert!(!fields[0].is_nullable()); // readID not null
        assert_eq!(fields[2].data_type(), &DataType::UInt32);
        assert!(!fields[2].is_nullable()); // pos1 not null
    }

    #[test]
    fn test_columns_to_arrow_fields_extended() {
        let columns: Vec<String> = vec![
            "readID",
            "chr1",
            "pos1",
            "chr2",
            "pos2",
            "strand1",
            "strand2",
            "frag1",
            "mapq1",
            "custom_field",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let fields = columns_to_arrow_fields(&columns);
        assert_eq!(fields.len(), 10);
        assert_eq!(fields[7].data_type(), &DataType::UInt32); // frag1
        assert!(fields[7].is_nullable()); // frag1 nullable
        assert_eq!(fields[9].data_type(), &DataType::Utf8); // custom_field
        assert!(fields[9].is_nullable()); // custom_field nullable
    }
}
