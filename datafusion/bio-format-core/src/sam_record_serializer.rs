use crate::BAM_TAG_TAG_KEY;
use crate::alignment_utils::decode_binary_cigar_to_ops;
use crate::sam_tag_io::build_tag_data;
use datafusion::arrow::array::{
    Array, BinaryArray, Int32Array, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use noodles_sam as sam;
use noodles_sam::alignment::RecordBuf;
use noodles_sam::alignment::record_buf::{Cigar, QualityScores, Sequence};
use std::collections::HashMap;

/// Converts an Arrow `RecordBatch` to alignment records for BAM/CRAM writers.
pub fn batch_to_alignment_records(
    batch: &RecordBatch,
    header: &sam::Header,
    tag_fields: &[String],
    coordinate_system_zero_based: bool,
) -> Result<Vec<RecordBuf>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let ref_map = build_reference_map(header);
    let names = get_string_column_by_name(batch, "name")?;
    let chroms = get_string_column_by_name(batch, "chrom")?;
    let starts = get_u32_column_by_name(batch, "start")?;
    let flags = get_u32_column_by_name(batch, "flags")?;
    let cigar_col = get_cigar_column(batch)?;
    let mapping_qualities = get_u32_column_by_name(batch, "mapping_quality")?;
    let mate_chroms = get_string_column_by_name(batch, "mate_chrom")?;
    let mate_starts = get_u32_column_by_name(batch, "mate_start")?;
    let sequences = get_string_column_by_name(batch, "sequence")?;
    let quality_scores = get_string_column_by_name(batch, "quality_scores")?;
    let template_lengths = get_i32_column_by_name(batch, "template_length")?;
    let tag_columns = build_tag_column_map(batch, tag_fields);

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let record = build_single_record(
            row,
            names,
            chroms,
            starts,
            flags,
            &cigar_col,
            mapping_qualities,
            mate_chroms,
            mate_starts,
            sequences,
            quality_scores,
            template_lengths,
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

enum CigarColumn<'a> {
    String(&'a StringArray),
    Binary(&'a BinaryArray),
}

fn get_cigar_column(batch: &RecordBatch) -> Result<CigarColumn<'_>> {
    let idx = batch.schema().index_of("cigar").map_err(|_| {
        DataFusionError::Execution("Required column 'cigar' not found in batch".to_string())
    })?;
    let col = batch.column(idx);

    match col.data_type() {
        DataType::Binary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                DataFusionError::Execution("Column 'cigar' downcast to BinaryArray failed".into())
            })?;
            Ok(CigarColumn::Binary(arr))
        }
        _ => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution("Column 'cigar' must be String or Binary type".into())
            })?;
            Ok(CigarColumn::String(arr))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_single_record(
    row: usize,
    names: &StringArray,
    chroms: &StringArray,
    starts: &UInt32Array,
    flags: &UInt32Array,
    cigar_col: &CigarColumn<'_>,
    mapping_qualities: &UInt32Array,
    mate_chroms: &StringArray,
    mate_starts: &UInt32Array,
    sequences: &StringArray,
    quality_scores: &StringArray,
    template_lengths: &Int32Array,
    ref_map: &HashMap<String, usize>,
    batch: &RecordBatch,
    tag_fields: &[String],
    tag_columns: &HashMap<String, usize>,
    coordinate_system_zero_based: bool,
) -> Result<RecordBuf> {
    let mut record = RecordBuf::default();

    let name = if names.is_null(row) || names.value(row) == "*" {
        None
    } else {
        Some(names.value(row).as_bytes().to_vec().into())
    };
    *record.name_mut() = name;

    let flag_value = flags.value(row);
    if flag_value > u32::from(u16::MAX) {
        return Err(DataFusionError::Execution(format!(
            "Flag value {flag_value} does not fit into 16-bit SAM flags"
        )));
    }
    *record.flags_mut() = sam::alignment::record::Flags::from(flag_value as u16);

    let ref_id = if chroms.is_null(row) {
        None
    } else {
        let chrom_name = chroms.value(row);
        ref_map.get(chrom_name).copied()
    };
    *record.reference_sequence_id_mut() = ref_id;

    use noodles_core::Position as CorePosition;
    let alignment_start = if starts.is_null(row) {
        None
    } else {
        let pos_value = starts.value(row);
        let pos_1based = if coordinate_system_zero_based {
            pos_value + 1
        } else {
            pos_value
        };
        CorePosition::try_from(pos_1based as usize).ok()
    };
    *record.alignment_start_mut() = alignment_start;

    let mapq = sam::alignment::record::MappingQuality::new(mapping_qualities.value(row) as u8);
    *record.mapping_quality_mut() = mapq;

    let cigar = match cigar_col {
        CigarColumn::String(arr) => parse_cigar_string(arr.value(row))?,
        CigarColumn::Binary(arr) => {
            let bytes = arr.value(row);
            let ops = decode_binary_cigar_to_ops(bytes).map_err(|e| {
                DataFusionError::Execution(format!("Failed to decode binary CIGAR: {e}"))
            })?;
            Cigar::from(ops)
        }
    };
    *record.cigar_mut() = cigar;

    let mate_ref_id = if mate_chroms.is_null(row) {
        None
    } else {
        let mate_chrom_name = mate_chroms.value(row);
        if mate_chrom_name == "=" {
            ref_id
        } else {
            ref_map.get(mate_chrom_name).copied()
        }
    };
    *record.mate_reference_sequence_id_mut() = mate_ref_id;

    let mate_alignment_start = if mate_starts.is_null(row) {
        None
    } else {
        let mate_pos_value = mate_starts.value(row);
        let mate_pos_1based = if coordinate_system_zero_based {
            mate_pos_value + 1
        } else {
            mate_pos_value
        };
        CorePosition::try_from(mate_pos_1based as usize).ok()
    };
    *record.mate_alignment_start_mut() = mate_alignment_start;

    *record.template_length_mut() = template_lengths.value(row);

    let seq_str = sequences.value(row);
    let sequence = if seq_str == "*" || seq_str.is_empty() {
        Sequence::default()
    } else {
        Sequence::from(seq_str.as_bytes().to_vec())
    };
    *record.sequence_mut() = sequence;

    let qual_str = quality_scores.value(row);
    let quality_scores_vec = decode_quality_scores(qual_str)?;
    *record.quality_scores_mut() = QualityScores::from(quality_scores_vec);

    let data = build_tag_data(row, batch, tag_fields, tag_columns)?;
    *record.data_mut() = data;

    Ok(record)
}

fn build_reference_map(header: &sam::Header) -> HashMap<String, usize> {
    header
        .reference_sequences()
        .iter()
        .enumerate()
        .map(|(idx, (name, _))| (name.to_string(), idx))
        .collect()
}

fn parse_cigar_string(cigar_str: &str) -> Result<Cigar> {
    if cigar_str == "*" || cigar_str.is_empty() {
        return Ok(Cigar::default());
    }

    let record_cigar = sam::record::Cigar::new(cigar_str.as_bytes());
    let ops: std::result::Result<Vec<_>, _> = record_cigar.iter().collect();
    let ops = ops.map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse CIGAR '{cigar_str}': {e}"))
    })?;

    Ok(Cigar::from(ops))
}

fn decode_quality_scores(qual_str: &str) -> Result<Vec<u8>> {
    if qual_str == "*" || qual_str.is_empty() {
        return Ok(Vec::new());
    }

    Ok(qual_str
        .bytes()
        .map(|byte| byte.saturating_sub(33))
        .collect())
}

fn get_string_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be String type")))
}

fn get_u32_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be UInt32 type")))
}

fn get_i32_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be Int32 type")))
}

fn build_tag_column_map(batch: &RecordBatch, tag_fields: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    let schema = batch.schema();

    for tag_name in tag_fields {
        if let Ok(idx) = schema.index_of(tag_name) {
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
        let scores = decode_quality_scores("!!\"#")?;
        assert_eq!(scores, vec![0, 0, 1, 2]);

        let empty = decode_quality_scores("*")?;
        assert!(empty.is_empty());

        Ok(())
    }

    #[test]
    fn test_build_reference_map() {
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
    fn test_batch_to_alignment_records_basic() -> Result<()> {
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
            Field::new("template_length", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("read1")])),
                Arc::new(StringArray::from(vec![Some("chr1")])),
                Arc::new(UInt32Array::from(vec![Some(100u32)])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec!["10M"])),
                Arc::new(UInt32Array::from(vec![60u32])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(UInt32Array::from(vec![Option::<u32>::None])),
                Arc::new(StringArray::from(vec!["ACGTACGTAC"])),
                Arc::new(StringArray::from(vec!["!!!!!!!!!!"])),
                Arc::new(Int32Array::from(vec![0i32])),
            ],
        )
        .unwrap();

        let header = sam::Header::builder()
            .add_reference_sequence(
                "chr1",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(249250621).unwrap()),
            )
            .build();

        let records = batch_to_alignment_records(&batch, &header, &[], true)?;
        assert_eq!(records.len(), 1);

        Ok(())
    }

    #[test]
    fn test_batch_to_alignment_records_rejects_flag_overflow() {
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
            Field::new("template_length", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("read1")])),
                Arc::new(StringArray::from(vec![Some("chr1")])),
                Arc::new(UInt32Array::from(vec![Some(100u32)])),
                Arc::new(UInt32Array::from(vec![u32::from(u16::MAX) + 1])),
                Arc::new(StringArray::from(vec!["10M"])),
                Arc::new(UInt32Array::from(vec![60u32])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(UInt32Array::from(vec![Option::<u32>::None])),
                Arc::new(StringArray::from(vec!["ACGTACGTAC"])),
                Arc::new(StringArray::from(vec!["!!!!!!!!!!"])),
                Arc::new(Int32Array::from(vec![0i32])),
            ],
        )
        .unwrap();

        let header = sam::Header::builder()
            .add_reference_sequence(
                "chr1",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(249250621).unwrap()),
            )
            .build();

        let error = batch_to_alignment_records(&batch, &header, &[], true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not fit into 16-bit SAM flags"),
            "unexpected error: {error}"
        );
    }
}
