use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Int32Array, Int32Builder, ListBuilder,
    NullArray, StringArray, StringBuilder, UInt8Builder, UInt16Builder, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::DataFusionError;
use noodles_sam::alignment::record::cigar::op::{Kind as OpKind, Op};
use std::fmt::Write;
use std::io;
use std::sync::Arc;

/// Create a properly typed array filled with null values
///
/// # Arguments
/// * `data_type` - The Arrow data type for the array
/// * `length` - Number of null values to create
///
/// # Returns
/// An ArrayRef of the specified type filled with null values
fn new_null_array(data_type: &DataType, length: usize) -> ArrayRef {
    match data_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for _ in 0..length {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
        DataType::List(field) => match field.data_type() {
            DataType::Int32 => {
                let mut builder = ListBuilder::new(Int32Builder::new());
                for _ in 0..length {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            DataType::Float32 => {
                let mut builder = ListBuilder::new(Float32Builder::new());
                for _ in 0..length {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for _ in 0..length {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            DataType::UInt8 => {
                let mut builder = ListBuilder::new(UInt8Builder::new());
                for _ in 0..length {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            DataType::UInt16 => {
                let mut builder = ListBuilder::new(UInt16Builder::new());
                for _ in 0..length {
                    builder.append_null();
                }
                Arc::new(builder.finish())
            }
            _ => Arc::new(NullArray::new(length)),
        },
        _ => Arc::new(NullArray::new(length)),
    }
}

/// Container for alignment record field data
pub struct RecordFields<'a> {
    /// Read/query template names
    pub name: &'a [Option<String>],
    /// Reference sequence names (chromosomes)
    pub chrom: &'a [Option<String>],
    /// Alignment start positions
    pub start: &'a [Option<u32>],
    /// Alignment end positions
    pub end: &'a [Option<u32>],
    /// SAM flags
    pub flag: &'a [u32],
    /// CIGAR strings
    pub cigar: &'a [String],
    /// Mapping quality scores (255 = unavailable per SAM spec, but always present)
    pub mapping_quality: &'a [u32],
    /// Mate/next segment reference sequence names
    pub mate_chrom: &'a [Option<String>],
    /// Mate/next segment alignment start positions
    pub mate_start: &'a [Option<u32>],
    /// Read sequences (pre-built Arrow array)
    pub sequence: ArrayRef,
    /// Base quality scores (pre-built Arrow array)
    pub quality_scores: ArrayRef,
    /// Template length (TLEN)
    pub template_length: &'a [i32],
}

/// Build a RecordBatch from alignment record fields
///
/// This function creates Arrow arrays from the record fields and combines them
/// with optional tag arrays. It handles projection if specified.
///
/// # Arguments
/// * `schema` - The Arrow schema for the batch
/// * `fields` - Container with pointers to all core field vectors
/// * `tag_arrays` - Optional vector of tag column arrays
/// * `projection` - Optional column indices to include in output
/// * `record_count` - Number of records in this batch (used for empty projection / null arrays)
pub fn build_record_batch(
    schema: SchemaRef,
    fields: RecordFields,
    tag_arrays: Option<&Vec<ArrayRef>>,
    projection: Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    let name = fields.name;
    let chrom = fields.chrom;
    let start = fields.start;
    let end = fields.end;
    let flag = fields.flag;
    let cigar = fields.cigar;
    let mapping_quality = fields.mapping_quality;
    let mate_chrom = fields.mate_chrom;
    let mate_start = fields.mate_start;
    let sequence_array = fields.sequence;
    let quality_scores_array = fields.quality_scores;
    let template_length = fields.template_length;

    // Helper closures for lazy array construction — each array is built only when needed
    let make_name =
        || Arc::new(StringArray::from_iter(name.iter().map(|s| s.as_deref()))) as Arc<dyn Array>;
    let make_chrom =
        || Arc::new(StringArray::from_iter(chrom.iter().map(|s| s.as_deref()))) as Arc<dyn Array>;
    let make_start = || Arc::new(UInt32Array::from_iter(start.iter().copied())) as Arc<dyn Array>;
    let make_end = || Arc::new(UInt32Array::from_iter(end.iter().copied())) as Arc<dyn Array>;
    let make_flag =
        || Arc::new(UInt32Array::from_iter_values(flag.iter().copied())) as Arc<dyn Array>;
    let make_cigar = || {
        Arc::new(StringArray::from_iter_values(
            cigar.iter().map(|s| s.as_str()),
        )) as Arc<dyn Array>
    };
    let make_mapq = || {
        Arc::new(UInt32Array::from_iter_values(
            mapping_quality.iter().copied(),
        )) as Arc<dyn Array>
    };
    let make_mate_chrom = || {
        Arc::new(StringArray::from_iter(
            mate_chrom.iter().map(|s| s.as_deref()),
        )) as Arc<dyn Array>
    };
    let make_mate_start =
        || Arc::new(UInt32Array::from_iter(mate_start.iter().copied())) as Arc<dyn Array>;
    let make_tlen = || {
        Arc::new(Int32Array::from_iter_values(
            template_length.iter().copied(),
        )) as Arc<dyn Array>
    };

    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                make_name(),
                make_chrom(),
                make_start(),
                make_end(),
                make_flag(),
                make_cigar(),
                make_mapq(),
                make_mate_chrom(),
                make_mate_start(),
                sequence_array,
                quality_scores_array,
                make_tlen(),
            ];
            // Add tag arrays if present
            if let Some(tags) = tag_arrays {
                arrays.extend_from_slice(tags);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            if proj_ids.is_empty() {
                // For empty projections (COUNT(*)), return an empty vector
                // The schema should already be empty from the table provider
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(make_name()),
                        1 => arrays.push(make_chrom()),
                        2 => arrays.push(make_start()),
                        3 => arrays.push(make_end()),
                        4 => arrays.push(make_flag()),
                        5 => arrays.push(make_cigar()),
                        6 => arrays.push(make_mapq()),
                        7 => arrays.push(make_mate_chrom()),
                        8 => arrays.push(make_mate_start()),
                        9 => arrays.push(sequence_array.clone()),
                        10 => arrays.push(quality_scores_array.clone()),
                        11 => arrays.push(make_tlen()),
                        _ => {
                            // Tag fields start at index 12
                            let tag_idx = i - 12;
                            if let Some(tags) = tag_arrays {
                                if tag_idx < tags.len() {
                                    arrays.push(tags[tag_idx].clone());
                                } else {
                                    // Tag index out of bounds - create properly typed null array
                                    let field = &schema.fields()[i];
                                    arrays.push(new_null_array(field.data_type(), record_count));
                                }
                            } else {
                                // No tag arrays provided - create properly typed null array
                                let field = &schema.fields()[i];
                                arrays.push(new_null_array(field.data_type(), record_count));
                            }
                        }
                    }
                }
            }
            arrays
        }
    };
    // For empty projections (COUNT(*)), we need to specify row count explicitly
    if arrays.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(record_count));
        RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    } else {
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    }
}

/// Convert a CIGAR operation to string representation
///
/// Converts noodles CIGAR operations to standard SAM format strings.
/// For example: 10M, 5I, 3D, etc.
pub fn cigar_op_to_string(op: Op) -> String {
    let kind = match op.kind() {
        OpKind::Match => 'M',
        OpKind::Insertion => 'I',
        OpKind::Deletion => 'D',
        OpKind::Skip => 'N',
        OpKind::SoftClip => 'S',
        OpKind::HardClip => 'H',
        OpKind::Pad => 'P',
        OpKind::SequenceMatch => '=',
        OpKind::SequenceMismatch => 'X',
    };
    format!("{}{}", op.len(), kind)
}

/// Convert a CIGAR operation kind to its SAM character representation
#[inline]
fn cigar_op_char(kind: OpKind) -> char {
    match kind {
        OpKind::Match => 'M',
        OpKind::Insertion => 'I',
        OpKind::Deletion => 'D',
        OpKind::Skip => 'N',
        OpKind::SoftClip => 'S',
        OpKind::HardClip => 'H',
        OpKind::Pad => 'P',
        OpKind::SequenceMatch => '=',
        OpKind::SequenceMismatch => 'X',
    }
}

/// Format CIGAR operations from an iterator of owned `Op` values into a reusable buffer.
///
/// Clears the buffer first, then writes each operation as `<len><kind_char>`.
/// The buffer retains its allocation across calls, avoiding per-record heap allocations.
pub fn format_cigar_ops(ops: impl Iterator<Item = Op>, buf: &mut String) {
    buf.clear();
    for op in ops {
        let _ = write!(buf, "{}{}", op.len(), cigar_op_char(op.kind()));
    }
}

/// Format CIGAR operations from an iterator of `io::Result<Op>` values (BAM records).
///
/// Same as `format_cigar_ops` but unwraps `Result` values, as used by BAM lazy records.
pub fn format_cigar_ops_unwrap(ops: impl Iterator<Item = io::Result<Op>>, buf: &mut String) {
    buf.clear();
    for op in ops {
        let op = op.unwrap();
        let _ = write!(buf, "{}{}", op.len(), cigar_op_char(op.kind()));
    }
}

/// Get chromosome name from BAM reference sequence ID
///
/// Returns the chromosome name exactly as it appears in the BAM header.
/// BAM uses `io::Result<usize>` for reference sequence IDs.
pub fn get_chrom_by_seq_id_bam(rid: Option<io::Result<usize>>, names: &[String]) -> Option<String> {
    match rid {
        Some(rid) => {
            let idx = rid.unwrap();
            let chrom_name = names
                .get(idx)
                .expect("reference_sequence_id() should be in bounds");
            Some(chrom_name.to_string())
        }
        _ => None,
    }
}

/// Get chromosome name from CRAM reference sequence ID
///
/// Returns the chromosome name exactly as it appears in the CRAM header.
/// CRAM uses `Option<usize>` directly for reference sequence IDs.
pub fn get_chrom_by_seq_id_cram(rid: Option<usize>, names: &[String]) -> Option<String> {
    match rid {
        Some(rid) => {
            let chrom_name = names
                .get(rid)
                .expect("reference_sequence_id() should be in bounds");
            Some(chrom_name.to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_sam::alignment::record::cigar::op::Kind;

    #[test]
    fn test_cigar_op_to_string() {
        assert_eq!(cigar_op_to_string(Op::new(Kind::Match, 10)), "10M");
        assert_eq!(cigar_op_to_string(Op::new(Kind::Insertion, 5)), "5I");
        assert_eq!(cigar_op_to_string(Op::new(Kind::Deletion, 3)), "3D");
        assert_eq!(cigar_op_to_string(Op::new(Kind::Skip, 100)), "100N");
        assert_eq!(cigar_op_to_string(Op::new(Kind::SoftClip, 7)), "7S");
        assert_eq!(cigar_op_to_string(Op::new(Kind::HardClip, 2)), "2H");
    }

    #[test]
    fn test_format_cigar_ops() {
        let ops = vec![
            Op::new(Kind::Match, 10),
            Op::new(Kind::Insertion, 5),
            Op::new(Kind::Deletion, 3),
        ];
        let mut buf = String::new();
        format_cigar_ops(ops.into_iter(), &mut buf);
        assert_eq!(buf, "10M5I3D");
    }

    #[test]
    fn test_format_cigar_ops_single() {
        let ops = vec![Op::new(Kind::Match, 150)];
        let mut buf = String::new();
        format_cigar_ops(ops.into_iter(), &mut buf);
        assert_eq!(buf, "150M");
    }

    #[test]
    fn test_format_cigar_ops_empty() {
        let ops: Vec<Op> = vec![];
        let mut buf = String::new();
        format_cigar_ops(ops.into_iter(), &mut buf);
        assert_eq!(buf, "");
    }

    #[test]
    fn test_format_cigar_ops_unwrap() {
        let ops: Vec<std::io::Result<Op>> = vec![
            Ok(Op::new(Kind::SoftClip, 7)),
            Ok(Op::new(Kind::Match, 100)),
            Ok(Op::new(Kind::SoftClip, 3)),
        ];
        let mut buf = String::new();
        format_cigar_ops_unwrap(ops.into_iter(), &mut buf);
        assert_eq!(buf, "7S100M3S");
    }

    #[test]
    fn test_format_cigar_ops_buffer_reuse() {
        let mut buf = String::new();

        format_cigar_ops(vec![Op::new(Kind::Match, 50)].into_iter(), &mut buf);
        assert_eq!(buf, "50M");

        // Second call reuses the buffer (clears it first)
        format_cigar_ops(
            vec![Op::new(Kind::HardClip, 2), Op::new(Kind::Match, 100)].into_iter(),
            &mut buf,
        );
        assert_eq!(buf, "2H100M");
    }

    #[test]
    fn test_get_chrom_by_seq_id_cram() {
        let names = vec!["chr1".to_string(), "chr2".to_string(), "chrM".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_cram(Some(0), &names),
            Some("chr1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_cram(Some(2), &names),
            Some("chrM".to_string())
        );
        assert_eq!(get_chrom_by_seq_id_cram(None, &names), None);
    }

    #[test]
    fn test_get_chrom_by_seq_id_cram_preserves_original_names() {
        let names = vec!["1".to_string(), "X".to_string(), "MT".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_cram(Some(0), &names),
            Some("1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_cram(Some(1), &names),
            Some("X".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_cram(Some(2), &names),
            Some("MT".to_string())
        );
    }

    #[test]
    fn test_get_chrom_by_seq_id_bam() {
        let names = vec!["chr1".to_string(), "chr2".to_string(), "chrM".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(0)), &names),
            Some("chr1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(2)), &names),
            Some("chrM".to_string())
        );
        assert_eq!(get_chrom_by_seq_id_bam(None, &names), None);
    }

    #[test]
    fn test_get_chrom_by_seq_id_bam_preserves_original_names() {
        let names = vec!["1".to_string(), "X".to_string(), "MT".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(0)), &names),
            Some("1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(1)), &names),
            Some("X".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(2)), &names),
            Some("MT".to_string())
        );
    }
}
