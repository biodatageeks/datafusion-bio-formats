//! Serializer for converting Arrow RecordBatches to BAM records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to BAM/SAM format for writing to files.

use datafusion::arrow::array::{
    Array, Float32Array, Int32Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array,
};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::{BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY};
use noodles_sam as sam;
use noodles_sam::alignment::RecordBuf;
use noodles_sam::alignment::record_buf::{
    Cigar, QualityScores, Sequence, data::field::Value as TagValue,
};
use std::collections::HashMap;

/// Converts an Arrow RecordBatch to a vector of BAM records.
///
/// The RecordBatch must have columns matching BAM schema names. Columns are
/// looked up by name, so the order in the batch does not matter.
///
/// # Arguments
///
/// * `batch` - The Arrow RecordBatch to convert
/// * `header` - The SAM header (needed for reference sequence lookups)
/// * `tag_fields` - Names of alignment tag fields to include
/// * `coordinate_system_zero_based` - If true, coordinates are 0-based (need +1 for BAM POS)
///
/// # Returns
///
/// A vector of BAM records that can be written to a file
///
/// # Errors
///
/// Returns an error if required columns are missing or have wrong types
pub fn batch_to_bam_records(
    batch: &RecordBatch,
    header: &sam::Header,
    tag_fields: &[String],
    coordinate_system_zero_based: bool,
) -> Result<Vec<RecordBuf>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Build reference sequence name -> ID mapping
    let ref_map = build_reference_map(header);

    // Look up core columns by name
    let names = get_string_column_by_name(batch, "name")?;
    let chroms = get_string_column_by_name(batch, "chrom")?;
    let starts = get_u32_column_by_name(batch, "start")?;
    let flags = get_u32_column_by_name(batch, "flags")?;
    let cigars = get_string_column_by_name(batch, "cigar")?;
    let mapping_qualities = get_u32_column_by_name(batch, "mapping_quality")?;
    let mate_chroms = get_string_column_by_name(batch, "mate_chrom")?;
    let mate_starts = get_u32_column_by_name(batch, "mate_start")?;
    let sequences = get_string_column_by_name(batch, "sequence")?;
    let quality_scores = get_string_column_by_name(batch, "quality_scores")?;

    // Build tag column map
    let tag_columns = build_tag_column_map(batch, tag_fields);

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let record = build_single_record(
            row,
            &names,
            &chroms,
            &starts,
            &flags,
            &cigars,
            &mapping_qualities,
            &mate_chroms,
            &mate_starts,
            &sequences,
            &quality_scores,
            &ref_map,
            batch,
            tag_fields,
            &tag_columns,
            coordinate_system_zero_based,
        )?;
        records.push(record);
    }

    Ok(records)
}

/// Builds a single BAM record from Arrow array values
#[allow(clippy::too_many_arguments)]
fn build_single_record(
    row: usize,
    names: &StringArray,
    chroms: &StringArray,
    starts: &UInt32Array,
    flags: &UInt32Array,
    cigars: &StringArray,
    mapping_qualities: &UInt32Array,
    mate_chroms: &StringArray,
    mate_starts: &UInt32Array,
    sequences: &StringArray,
    quality_scores: &StringArray,
    ref_map: &HashMap<String, usize>,
    batch: &RecordBatch,
    tag_fields: &[String],
    tag_columns: &HashMap<String, usize>,
    coordinate_system_zero_based: bool,
) -> Result<RecordBuf> {
    // Create a new mutable BAM record
    let mut record = RecordBuf::default();

    // 1. QNAME (read name)
    let name = if names.is_null(row) {
        None
    } else {
        Some(names.value(row).as_bytes().to_vec().into())
    };
    *record.name_mut() = name;

    // 2. FLAG
    let flag_value = flags.value(row);
    *record.flags_mut() = sam::alignment::record::Flags::from(flag_value as u16);

    // 3. RNAME (reference sequence ID)
    let ref_id = if chroms.is_null(row) {
        None
    } else {
        let chrom_name = chroms.value(row);
        ref_map.get(chrom_name).copied()
    };
    *record.reference_sequence_id_mut() = ref_id;

    // 4. POS (alignment position)
    use noodles_core::Position as CorePosition;
    let alignment_start = if starts.is_null(row) {
        None
    } else {
        let pos_value = starts.value(row);
        // Convert 0-based to 1-based if needed
        let pos_1based = if coordinate_system_zero_based {
            pos_value + 1
        } else {
            pos_value
        };
        // BAM uses 1-based positions, try_from expects usize
        CorePosition::try_from(pos_1based as usize).ok()
    };
    *record.alignment_start_mut() = alignment_start;

    // 5. MAPQ (mapping quality)
    let mapq = if mapping_qualities.is_null(row) {
        sam::alignment::record::MappingQuality::new(255) // Unknown
    } else {
        sam::alignment::record::MappingQuality::new(mapping_qualities.value(row) as u8)
    };
    *record.mapping_quality_mut() = mapq;

    // 6. CIGAR
    let cigar_str = cigars.value(row);
    let cigar = parse_cigar_string(cigar_str)?;
    *record.cigar_mut() = cigar;

    // 7. RNEXT (mate reference sequence ID)
    let mate_ref_id = if mate_chroms.is_null(row) {
        None
    } else {
        let mate_chrom_name = mate_chroms.value(row);
        if mate_chrom_name == "=" {
            // Same as RNAME
            ref_id
        } else {
            ref_map.get(mate_chrom_name).copied()
        }
    };
    *record.mate_reference_sequence_id_mut() = mate_ref_id;

    // 8. PNEXT (mate position)
    let mate_alignment_start = if mate_starts.is_null(row) {
        None
    } else {
        let mate_pos_value = mate_starts.value(row);
        // Convert 0-based to 1-based if needed
        let mate_pos_1based = if coordinate_system_zero_based {
            mate_pos_value + 1
        } else {
            mate_pos_value
        };
        CorePosition::try_from(mate_pos_1based as usize).ok()
    };
    *record.mate_alignment_start_mut() = mate_alignment_start;

    // 9. TLEN (template length) - not stored in our schema, default to 0
    *record.template_length_mut() = 0;

    // 10. SEQ (sequence)
    let seq_str = sequences.value(row);
    let sequence = if seq_str == "*" || seq_str.is_empty() {
        Sequence::default()
    } else {
        // Convert sequence string to bytes
        Sequence::from(seq_str.as_bytes().to_vec())
    };
    *record.sequence_mut() = sequence;

    // 11. QUAL (quality scores)
    let qual_str = quality_scores.value(row);
    let quality_scores_vec = decode_quality_scores(qual_str)?;
    *record.quality_scores_mut() = QualityScores::from(quality_scores_vec);

    // 12. Tags (optional)
    let data = build_tags(row, batch, tag_fields, tag_columns)?;
    *record.data_mut() = data;

    Ok(record)
}

/// Builds reference sequence name to ID mapping from header
fn build_reference_map(header: &sam::Header) -> HashMap<String, usize> {
    header
        .reference_sequences()
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (name.to_string(), i))
        .collect()
}

/// Parses a CIGAR string into noodles CIGAR operations
///
/// Supports: M, I, D, N, S, H, P, =, X
fn parse_cigar_string(cigar_str: &str) -> Result<Cigar> {
    if cigar_str == "*" || cigar_str.is_empty() {
        return Ok(Cigar::default());
    }

    // Use record::Cigar to parse the string
    let cigar_bytes = cigar_str.as_bytes();
    let record_cigar = sam::record::Cigar::new(cigar_bytes);

    // Collect operations from the iterator
    let ops: std::result::Result<Vec<_>, _> = record_cigar.iter().collect();
    let ops = ops.map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse CIGAR '{}': {}", cigar_str, e))
    })?;

    Ok(Cigar::from(ops))
}

/// Decodes Phred+33 quality scores from ASCII string to u8 values
fn decode_quality_scores(qual_str: &str) -> Result<Vec<u8>> {
    if qual_str == "*" || qual_str.is_empty() {
        return Ok(Vec::new());
    }

    // Phred+33: ASCII char - 33 = quality score
    Ok(qual_str.bytes().map(|b| b.saturating_sub(33)).collect())
}

/// Builds alignment tags (optional fields) from tag columns
fn build_tags(
    row: usize,
    batch: &RecordBatch,
    tag_fields: &[String],
    tag_columns: &HashMap<String, usize>,
) -> Result<sam::alignment::record_buf::Data> {
    use sam::alignment::record::data::field::Tag;
    let mut data = sam::alignment::record_buf::Data::default();

    for tag_name in tag_fields {
        let col_idx = match tag_columns.get(tag_name) {
            Some(&idx) => idx,
            None => continue, // Column not in batch, skip
        };

        let column = batch.column(col_idx);
        if column.is_null(row) {
            continue;
        }

        // Get SAM type from field metadata
        let schema = batch.schema();
        let field = schema.field(col_idx);
        let sam_type = field
            .metadata()
            .get(BAM_TAG_TYPE_KEY)
            .and_then(|s| s.chars().next())
            .unwrap_or('Z'); // Default to string

        // Parse tag (2-char code)
        if tag_name.len() != 2 {
            continue; // Invalid tag name
        }
        let tag_bytes = tag_name.as_bytes();
        let tag = Tag::new(tag_bytes[0], tag_bytes[1]);

        // Convert Arrow value to SAM tag value
        if let Some(value) = arrow_to_sam_tag_value(column.as_ref(), row, sam_type)? {
            data.insert(tag, value);
        }
    }

    Ok(data)
}

/// Converts an Arrow array value to a SAM tag value
fn arrow_to_sam_tag_value(
    array: &dyn Array,
    row: usize,
    sam_type: char,
) -> Result<Option<TagValue>> {
    if array.is_null(row) {
        return Ok(None);
    }

    match sam_type {
        // Integer types
        'i' | 'c' | 's' | 'C' | 'S' | 'I' => {
            if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                Ok(Some(TagValue::from(arr.value(row))))
            } else if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                Ok(Some(TagValue::from(arr.value(row) as i32)))
            } else {
                Err(DataFusionError::Execution(
                    "Tag value type mismatch for integer".to_string(),
                ))
            }
        }
        // Float
        'f' => {
            if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                Ok(Some(TagValue::from(arr.value(row))))
            } else {
                Err(DataFusionError::Execution(
                    "Tag value type mismatch for float".to_string(),
                ))
            }
        }
        // String
        'Z' | 'H' => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                Ok(Some(TagValue::from(arr.value(row))))
            } else {
                Err(DataFusionError::Execution(
                    "Tag value type mismatch for string".to_string(),
                ))
            }
        }
        // Character
        'A' => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                let s = arr.value(row);
                if let Some(ch) = s.chars().next() {
                    Ok(Some(TagValue::from(ch as u8)))
                } else {
                    Ok(None)
                }
            } else {
                Err(DataFusionError::Execution(
                    "Tag value type mismatch for character".to_string(),
                ))
            }
        }
        // Array (Int or Float)
        'B' => {
            if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
                let list = arr.value(row);

                // Check element type
                if list.len() > 0 {
                    if let Some(int_arr) = list.as_any().downcast_ref::<Int32Array>() {
                        // Integer array
                        let values: Vec<i32> =
                            (0..int_arr.len()).map(|i| int_arr.value(i)).collect();
                        Ok(Some(TagValue::from(values)))
                    } else if let Some(float_arr) = list.as_any().downcast_ref::<Float32Array>() {
                        // Float array
                        let values: Vec<f32> =
                            (0..float_arr.len()).map(|i| float_arr.value(i)).collect();
                        Ok(Some(TagValue::from(values)))
                    } else if let Some(uint8_arr) = list.as_any().downcast_ref::<UInt8Array>() {
                        // UInt8 array
                        let values: Vec<u8> =
                            (0..uint8_arr.len()).map(|i| uint8_arr.value(i)).collect();
                        Ok(Some(TagValue::from(values)))
                    } else {
                        Err(DataFusionError::Execution(
                            "Unsupported array element type for tag".to_string(),
                        ))
                    }
                } else {
                    // Empty array
                    Ok(None)
                }
            } else {
                Err(DataFusionError::Execution(
                    "Tag value type mismatch for array".to_string(),
                ))
            }
        }
        _ => {
            // Unknown type, try string as fallback
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                Ok(Some(TagValue::from(arr.value(row))))
            } else {
                Ok(None)
            }
        }
    }
}

/// Gets a string column from the batch by name
fn get_string_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{}' not found in batch", name))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{}' must be String type", name)))
}

/// Gets a u32 column from the batch by name
fn get_u32_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{}' not found in batch", name))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{}' must be UInt32 type", name)))
}

/// Builds a map from tag name to column index
fn build_tag_column_map(batch: &RecordBatch, tag_fields: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    let schema = batch.schema();
    for tag_name in tag_fields {
        if let Ok(idx) = schema.index_of(tag_name) {
            // Verify this is actually a tag field by checking metadata
            let field = schema.field(idx);
            if field.metadata().contains_key(BAM_TAG_TAG_KEY) {
                map.insert(tag_name.clone(), idx);
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use noodles_sam::header::record::value::Map;
    use noodles_sam::header::record::value::map::ReferenceSequence;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    #[test]
    fn test_parse_cigar_string() -> Result<()> {
        use sam::alignment::record::Cigar as CigarTrait;

        let cigar = parse_cigar_string("10M5I3D")?;
        assert!(CigarTrait::len(&cigar) > 0);

        let empty = parse_cigar_string("*")?;
        assert_eq!(CigarTrait::len(&empty), 0);

        Ok(())
    }

    #[test]
    fn test_decode_quality_scores() -> Result<()> {
        // Phred+33 encoding: ! = 0, " = 1, etc.
        let scores = decode_quality_scores("!!\"#")?;
        assert_eq!(scores, vec![0, 0, 1, 2]);

        let empty = decode_quality_scores("*")?;
        assert!(empty.is_empty());

        Ok(())
    }

    #[test]
    fn test_build_reference_map() {
        use sam::header::record::value::Map;
        use sam::header::record::value::map::ReferenceSequence;
        use std::num::NonZeroUsize;

        let header = sam::Header::builder()
            .add_reference_sequence(
                "chr1",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(249250621).unwrap()),
            )
            .add_reference_sequence(
                "chr2",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(242193529).unwrap()),
            )
            .build();

        let ref_map = build_reference_map(&header);
        assert_eq!(ref_map.get("chr1"), Some(&0));
        assert_eq!(ref_map.get("chr2"), Some(&1));
    }

    #[test]
    fn test_batch_to_bam_records_basic() -> Result<()> {
        // Create a minimal schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("chrom", DataType::Utf8, true),
            Field::new("start", DataType::UInt32, true),
            Field::new("flags", DataType::UInt32, false),
            Field::new("cigar", DataType::Utf8, false),
            Field::new("mapping_quality", DataType::UInt32, false),
            Field::new("mate_chrom", DataType::Utf8, true),
            Field::new("mate_start", DataType::UInt32, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ]));

        // Create test data
        let names = StringArray::from(vec![Some("read1")]);
        let chroms = StringArray::from(vec![Some("chr1")]);
        let starts = UInt32Array::from(vec![Some(100u32)]);
        let flags = UInt32Array::from(vec![0u32]);
        let cigars = StringArray::from(vec!["10M"]);
        let mapping_qualities = UInt32Array::from(vec![60u32]);
        let mate_chroms = StringArray::from(vec![Option::<&str>::None]);
        let mate_starts = UInt32Array::from(vec![Option::<u32>::None]);
        let sequences = StringArray::from(vec!["ACGTACGTAC"]);
        let quality_scores = StringArray::from(vec!["!!!!!!!!!!"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(names),
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(flags),
                Arc::new(cigars),
                Arc::new(mapping_qualities),
                Arc::new(mate_chroms),
                Arc::new(mate_starts),
                Arc::new(sequences),
                Arc::new(quality_scores),
            ],
        )
        .unwrap();

        // Create header with reference sequences
        let header = sam::Header::builder()
            .add_reference_sequence(
                "chr1",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(249250621).unwrap()),
            )
            .build();

        let records = batch_to_bam_records(&batch, &header, &[], true)?;
        assert_eq!(records.len(), 1);

        Ok(())
    }
}
