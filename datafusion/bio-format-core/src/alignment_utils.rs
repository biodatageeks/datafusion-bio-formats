use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, NullArray,
    StringArray, StringBuilder, UInt8Builder, UInt16Builder, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use log::debug;
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
    /// Mapping quality scores
    pub mapping_quality: &'a [Option<u32>],
    /// Mate/next segment reference sequence names
    pub mate_chrom: &'a [Option<String>],
    /// Mate/next segment alignment start positions
    pub mate_start: &'a [Option<u32>],
    /// Read sequences
    pub sequence: &'a [String],
    /// Base quality scores
    pub quality_scores: &'a [String],
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
pub fn build_record_batch(
    schema: SchemaRef,
    fields: RecordFields,
    tag_arrays: Option<&Vec<ArrayRef>>,
    projection: Option<Vec<usize>>,
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
    let sequence = fields.sequence;
    let quality_scores = fields.quality_scores;

    let name_array =
        Arc::new(StringArray::from_iter(name.iter().map(|s| s.as_deref()))) as Arc<dyn Array>;
    let chrom_array =
        Arc::new(StringArray::from_iter(chrom.iter().map(|s| s.as_deref()))) as Arc<dyn Array>;
    let start_array = Arc::new(UInt32Array::from_iter(start.iter().copied())) as Arc<dyn Array>;
    let end_array = Arc::new(UInt32Array::from_iter(end.iter().copied())) as Arc<dyn Array>;
    let flag_array =
        Arc::new(UInt32Array::from_iter_values(flag.iter().copied())) as Arc<dyn Array>;
    let cigar_array = Arc::new(StringArray::from_iter_values(
        cigar.iter().map(|s| s.as_str()),
    )) as Arc<dyn Array>;
    let mapping_quality_array =
        Arc::new(UInt32Array::from_iter(mapping_quality.iter().copied())) as Arc<dyn Array>;
    let mate_chrom_array = Arc::new(StringArray::from_iter(
        mate_chrom.iter().map(|s| s.as_deref()),
    )) as Arc<dyn Array>;
    let mate_start_array =
        Arc::new(UInt32Array::from_iter(mate_start.iter().copied())) as Arc<dyn Array>;
    let sequence_array = Arc::new(StringArray::from_iter_values(
        sequence.iter().map(|s| s.as_str()),
    )) as Arc<dyn Array>;
    let quality_scores_array = Arc::new(StringArray::from_iter_values(
        quality_scores.iter().map(|s| s.as_str()),
    )) as Arc<dyn Array>;

    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                name_array,
                chrom_array,
                start_array,
                end_array,
                flag_array,
                cigar_array,
                mapping_quality_array,
                mate_chrom_array,
                mate_start_array,
                sequence_array,
                quality_scores_array,
            ];
            // Add tag arrays if present
            if let Some(tags) = tag_arrays {
                arrays.extend_from_slice(tags);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(name.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(name_array.len())) as Arc<dyn Array>);
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(name_array.clone()),
                        1 => arrays.push(chrom_array.clone()),
                        2 => arrays.push(start_array.clone()),
                        3 => arrays.push(end_array.clone()),
                        4 => arrays.push(flag_array.clone()),
                        5 => arrays.push(cigar_array.clone()),
                        6 => arrays.push(mapping_quality_array.clone()),
                        7 => arrays.push(mate_chrom_array.clone()),
                        8 => arrays.push(mate_start_array.clone()),
                        9 => arrays.push(sequence_array.clone()),
                        10 => arrays.push(quality_scores_array.clone()),
                        _ => {
                            // Tag fields start at index 11
                            let tag_idx = i - 11;
                            if let Some(tags) = tag_arrays {
                                if tag_idx < tags.len() {
                                    arrays.push(tags[tag_idx].clone());
                                } else {
                                    // Tag index out of bounds - create properly typed null array
                                    let field = &schema.fields()[i];
                                    arrays
                                        .push(new_null_array(field.data_type(), name_array.len()));
                                }
                            } else {
                                // No tag arrays provided - create properly typed null array
                                let field = &schema.fields()[i];
                                arrays.push(new_null_array(field.data_type(), name_array.len()));
                            }
                        }
                    }
                }
            }
            arrays
        }
    };
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
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
