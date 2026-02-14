use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Int32Array, Int32Builder, ListBuilder,
    NullArray, StringArray, StringBuilder, UInt8Builder, UInt16Builder, UInt32Array, UInt32Builder,
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
    /// Reference sequence names (chromosomes) — borrowed from names array
    pub chrom: &'a [Option<&'a str>],
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
    /// Mate/next segment reference sequence names — borrowed from names array
    pub mate_chrom: &'a [Option<&'a str>],
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
    projection: &Option<Vec<usize>>,
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
    let make_chrom = || Arc::new(StringArray::from_iter(chrom.iter().copied())) as Arc<dyn Array>;
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
    let make_mate_chrom =
        || Arc::new(StringArray::from_iter(mate_chrom.iter().copied())) as Arc<dyn Array>;
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
                for &i in proj_ids.iter() {
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

/// Build a RecordBatch from pre-built Arrow arrays (builder-based path).
///
/// Takes arrays produced by `CoreBatchBuilders::finish()` and assembles them
/// into a RecordBatch, handling projection and tag arrays.
///
/// # Arguments
/// * `schema` - The Arrow schema for the batch
/// * `core_arrays` - 12-element array of Option<ArrayRef> from builder finish()
/// * `tag_arrays` - Optional vector of tag column arrays
/// * `projection` - Optional column indices to include in output
/// * `record_count` - Number of records in this batch (used for empty projection / null arrays)
pub fn build_record_batch_from_builders(
    schema: SchemaRef,
    core_arrays: [Option<ArrayRef>; 12],
    tag_arrays: Option<&Vec<ArrayRef>>,
    projection: &Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    let arrays = match projection {
        None => {
            // No projection — all 12 core arrays must be present
            let mut arrays: Vec<ArrayRef> =
                core_arrays.into_iter().map(|opt| opt.unwrap()).collect();
            if let Some(tags) = tag_arrays {
                arrays.extend_from_slice(tags);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(proj_ids.len());
            for &i in proj_ids.iter() {
                if i < 12 {
                    if let Some(arr) = &core_arrays[i] {
                        arrays.push(arr.clone());
                    }
                } else {
                    // Tag fields start at index 12
                    let tag_idx = i - 12;
                    if let Some(tags) = tag_arrays {
                        if tag_idx < tags.len() {
                            arrays.push(tags[tag_idx].clone());
                        } else {
                            let field = &schema.fields()[i];
                            arrays.push(new_null_array(field.data_type(), record_count));
                        }
                    } else {
                        let field = &schema.fields()[i];
                        arrays.push(new_null_array(field.data_type(), record_count));
                    }
                }
            }
            arrays
        }
    };

    if arrays.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(record_count));
        RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    } else {
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
    }
}

/// Container for core alignment field builders (Arrow builder-based path).
///
/// Replaces the 12 Vec/StringBuilder field declarations with Option<Builder> fields.
/// Each field is `None` when not projected, avoiding unnecessary allocations.
/// Builders write directly into Arrow buffers, eliminating double-buffering.
pub struct CoreBatchBuilders {
    name: Option<StringBuilder>,
    chrom: Option<StringBuilder>,
    start: Option<UInt32Builder>,
    end: Option<UInt32Builder>,
    flag: Option<UInt32Builder>,
    cigar: Option<StringBuilder>,
    mapq: Option<UInt32Builder>,
    mate_chrom: Option<StringBuilder>,
    mate_start: Option<UInt32Builder>,
    sequence: Option<StringBuilder>,
    quality: Option<StringBuilder>,
    tlen: Option<Int32Builder>,
}

/// Projection flags for alignment fields.
///
/// Determines which of the 13 core fields are needed based on the projection.
pub struct ProjectionFlags {
    /// Whether read name (index 0) is projected
    pub name: bool,
    /// Whether chromosome (index 1) is projected
    pub chrom: bool,
    /// Whether alignment start (index 2) is projected
    pub start: bool,
    /// Whether alignment end (index 3) is projected
    pub end: bool,
    /// Whether SAM flags (index 4) are projected
    pub flags: bool,
    /// Whether CIGAR string (index 5) is projected
    pub cigar: bool,
    /// Whether mapping quality (index 6) is projected
    pub mapq: bool,
    /// Whether mate chromosome (index 7) is projected
    pub mate_chrom: bool,
    /// Whether mate start position (index 8) is projected
    pub mate_start: bool,
    /// Whether sequence (index 9) is projected
    pub sequence: bool,
    /// Whether quality scores (index 10) are projected
    pub quality: bool,
    /// Whether template length (index 11) is projected
    pub tlen: bool,
    /// Whether any tag field (index >= 12) is projected
    pub any_tag: bool,
}

impl ProjectionFlags {
    /// Create projection flags from an optional projection vector.
    pub fn new(projection: &Option<Vec<usize>>) -> Self {
        let needs = |idx: usize| projection.as_ref().is_none_or(|p| p.contains(&idx));
        Self {
            name: needs(0),
            chrom: needs(1),
            start: needs(2),
            end: needs(3),
            flags: needs(4),
            cigar: needs(5),
            mapq: needs(6),
            mate_chrom: needs(7),
            mate_start: needs(8),
            sequence: needs(9),
            quality: needs(10),
            tlen: needs(11),
            any_tag: projection
                .as_ref()
                .is_none_or(|p| p.iter().any(|&i| i >= 12)),
        }
    }
}

impl CoreBatchBuilders {
    /// Create builders for projected fields, `None` for unprojected ones.
    pub fn new(flags: &ProjectionFlags, batch_size: usize) -> Self {
        Self {
            name: flags
                .name
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 20)),
            chrom: flags
                .chrom
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 10)),
            start: flags
                .start
                .then(|| UInt32Builder::with_capacity(batch_size)),
            end: flags.end.then(|| UInt32Builder::with_capacity(batch_size)),
            flag: flags
                .flags
                .then(|| UInt32Builder::with_capacity(batch_size)),
            cigar: flags
                .cigar
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 20)),
            mapq: flags.mapq.then(|| UInt32Builder::with_capacity(batch_size)),
            mate_chrom: flags
                .mate_chrom
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 10)),
            mate_start: flags
                .mate_start
                .then(|| UInt32Builder::with_capacity(batch_size)),
            sequence: flags
                .sequence
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 150)),
            quality: flags
                .quality
                .then(|| StringBuilder::with_capacity(batch_size, batch_size * 150)),
            tlen: flags.tlen.then(|| Int32Builder::with_capacity(batch_size)),
        }
    }

    /// Append a read name value (nullable).
    #[inline]
    pub fn append_name(&mut self, value: Option<&str>) {
        if let Some(b) = &mut self.name {
            b.append_option(value);
        }
    }

    /// Append a chromosome name (nullable).
    #[inline]
    pub fn append_chrom(&mut self, value: Option<&str>) {
        if let Some(b) = &mut self.chrom {
            b.append_option(value);
        }
    }

    /// Append an alignment start position (nullable).
    #[inline]
    pub fn append_start(&mut self, value: Option<u32>) {
        if let Some(b) = &mut self.start {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    /// Append an alignment end position (nullable).
    #[inline]
    pub fn append_end(&mut self, value: Option<u32>) {
        if let Some(b) = &mut self.end {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    /// Append SAM flags (non-nullable).
    #[inline]
    pub fn append_flag(&mut self, value: u32) {
        if let Some(b) = &mut self.flag {
            b.append_value(value);
        }
    }

    /// Append a CIGAR string (non-nullable).
    #[inline]
    pub fn append_cigar(&mut self, value: &str) {
        if let Some(b) = &mut self.cigar {
            b.append_value(value);
        }
    }

    /// Append mapping quality (non-nullable, 255 for unavailable).
    #[inline]
    pub fn append_mapq(&mut self, value: u32) {
        if let Some(b) = &mut self.mapq {
            b.append_value(value);
        }
    }

    /// Append a mate chromosome name (nullable).
    #[inline]
    pub fn append_mate_chrom(&mut self, value: Option<&str>) {
        if let Some(b) = &mut self.mate_chrom {
            b.append_option(value);
        }
    }

    /// Append a mate start position (nullable).
    #[inline]
    pub fn append_mate_start(&mut self, value: Option<u32>) {
        if let Some(b) = &mut self.mate_start {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    /// Append a read sequence (non-nullable).
    #[inline]
    pub fn append_sequence(&mut self, value: &str) {
        if let Some(b) = &mut self.sequence {
            b.append_value(value);
        }
    }

    /// Append base quality scores (non-nullable).
    #[inline]
    pub fn append_quality(&mut self, value: &str) {
        if let Some(b) = &mut self.quality {
            b.append_value(value);
        }
    }

    /// Append template length (non-nullable).
    #[inline]
    pub fn append_tlen(&mut self, value: i32) {
        if let Some(b) = &mut self.tlen {
            b.append_value(value);
        }
    }

    /// Finalize all active builders into arrays.
    /// After calling this, the builders are reset and ready for the next batch.
    pub fn finish(&mut self) -> [Option<ArrayRef>; 12] {
        [
            self.name.as_mut().map(|b| Arc::new(b.finish()) as ArrayRef),
            self.chrom
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.start
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.end.as_mut().map(|b| Arc::new(b.finish()) as ArrayRef),
            self.flag.as_mut().map(|b| Arc::new(b.finish()) as ArrayRef),
            self.cigar
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.mapq.as_mut().map(|b| Arc::new(b.finish()) as ArrayRef),
            self.mate_chrom
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.mate_start
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.sequence
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.quality
                .as_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef),
            self.tlen.as_mut().map(|b| Arc::new(b.finish()) as ArrayRef),
        ]
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

/// Get chromosome index from BAM reference sequence ID
///
/// Returns the index into the reference names array.
/// BAM uses `io::Result<usize>` for reference sequence IDs.
pub fn get_chrom_idx_bam(rid: Option<io::Result<usize>>) -> Option<usize> {
    rid.map(|rid| rid.unwrap())
}

/// Get chromosome index from CRAM reference sequence ID
///
/// Returns the index into the reference names array.
/// CRAM uses `Option<usize>` directly for reference sequence IDs.
pub fn get_chrom_idx_cram(rid: Option<usize>) -> Option<usize> {
    rid
}

/// Look up a chromosome name by index in the names array.
#[inline]
pub fn chrom_name_by_idx(idx: Option<usize>, names: &[String]) -> Option<&str> {
    idx.map(|i| names[i].as_str())
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
    fn test_get_chrom_idx_cram() {
        let names = vec!["chr1".to_string(), "chr2".to_string(), "chrM".to_string()];

        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_cram(Some(0)), &names),
            Some("chr1")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_cram(Some(2)), &names),
            Some("chrM")
        );
        assert_eq!(chrom_name_by_idx(get_chrom_idx_cram(None), &names), None);
    }

    #[test]
    fn test_get_chrom_idx_cram_preserves_original_names() {
        let names = vec!["1".to_string(), "X".to_string(), "MT".to_string()];

        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_cram(Some(0)), &names),
            Some("1")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_cram(Some(1)), &names),
            Some("X")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_cram(Some(2)), &names),
            Some("MT")
        );
    }

    #[test]
    fn test_get_chrom_idx_bam() {
        let names = vec!["chr1".to_string(), "chr2".to_string(), "chrM".to_string()];

        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_bam(Some(Ok(0))), &names),
            Some("chr1")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_bam(Some(Ok(2))), &names),
            Some("chrM")
        );
        assert_eq!(chrom_name_by_idx(get_chrom_idx_bam(None), &names), None);
    }

    #[test]
    fn test_get_chrom_idx_bam_preserves_original_names() {
        let names = vec!["1".to_string(), "X".to_string(), "MT".to_string()];

        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_bam(Some(Ok(0))), &names),
            Some("1")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_bam(Some(Ok(1))), &names),
            Some("X")
        );
        assert_eq!(
            chrom_name_by_idx(get_chrom_idx_bam(Some(Ok(2))), &names),
            Some("MT")
        );
    }
}
