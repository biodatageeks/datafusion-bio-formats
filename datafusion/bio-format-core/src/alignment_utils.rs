use datafusion::arrow::array::{Array, ArrayRef, NullArray, StringArray, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use log::debug;
use noodles_sam::alignment::record::cigar::op::{Kind as OpKind, Op};
use std::io;
use std::sync::Arc;

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

    let name_array = Arc::new(StringArray::from(name.to_vec())) as Arc<dyn Array>;
    let chrom_array = Arc::new(StringArray::from(chrom.to_vec())) as Arc<dyn Array>;
    let start_array = Arc::new(UInt32Array::from(start.to_vec())) as Arc<dyn Array>;
    let end_array = Arc::new(UInt32Array::from(end.to_vec())) as Arc<dyn Array>;
    let flag_array = Arc::new(UInt32Array::from(flag.to_vec())) as Arc<dyn Array>;
    let cigar_array = Arc::new(StringArray::from(cigar.to_vec())) as Arc<dyn Array>;
    let mapping_quality_array =
        Arc::new(UInt32Array::from(mapping_quality.to_vec())) as Arc<dyn Array>;
    let mate_chrom_array = Arc::new(StringArray::from(mate_chrom.to_vec())) as Arc<dyn Array>;
    let mate_start_array = Arc::new(UInt32Array::from(mate_start.to_vec())) as Arc<dyn Array>;
    let sequence_array = Arc::new(StringArray::from(sequence.to_vec())) as Arc<dyn Array>;
    let quality_scores_array =
        Arc::new(StringArray::from(quality_scores.to_vec())) as Arc<dyn Array>;

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
                                    arrays.push(Arc::new(NullArray::new(name_array.len()))
                                        as Arc<dyn Array>);
                                }
                            } else {
                                arrays
                                    .push(Arc::new(NullArray::new(name_array.len()))
                                        as Arc<dyn Array>);
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

/// Get chromosome name from BAM reference sequence ID
///
/// BAM uses `io::Result<usize>` for reference sequence IDs
pub fn get_chrom_by_seq_id_bam(rid: Option<io::Result<usize>>, names: &[String]) -> Option<String> {
    match rid {
        Some(rid) => {
            let idx = rid.unwrap();
            let chrom_name = names
                .get(idx)
                .expect("reference_sequence_id() should be in bounds");
            let mut chrom_name = chrom_name.to_string().to_lowercase();
            if !chrom_name.starts_with("chr") {
                chrom_name = format!("chr{}", chrom_name);
            }
            Some(chrom_name)
        }
        _ => None,
    }
}

/// Get chromosome name from CRAM reference sequence ID
///
/// CRAM uses `Option<usize>` directly for reference sequence IDs
pub fn get_chrom_by_seq_id_cram(rid: Option<usize>, names: &[String]) -> Option<String> {
    match rid {
        Some(rid) => {
            let chrom_name = names
                .get(rid)
                .expect("reference_sequence_id() should be in bounds");
            let mut chrom_name = chrom_name.to_string().to_lowercase();
            if !chrom_name.starts_with("chr") {
                chrom_name = format!("chr{}", chrom_name);
            }
            Some(chrom_name)
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
    fn test_get_chrom_by_seq_id_cram() {
        let names = vec!["1".to_string(), "2".to_string(), "X".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_cram(Some(0), &names),
            Some("chr1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_cram(Some(2), &names),
            Some("chrx".to_string())
        );
        assert_eq!(get_chrom_by_seq_id_cram(None, &names), None);
    }

    #[test]
    fn test_get_chrom_by_seq_id_bam() {
        let names = vec!["1".to_string(), "2".to_string(), "X".to_string()];

        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(0)), &names),
            Some("chr1".to_string())
        );
        assert_eq!(
            get_chrom_by_seq_id_bam(Some(Ok(2)), &names),
            Some("chrx".to_string())
        );
        assert_eq!(get_chrom_by_seq_id_bam(None, &names), None);
    }
}
