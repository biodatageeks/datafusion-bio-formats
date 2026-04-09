//! Serializer for converting Arrow RecordBatches to BAM records.

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion_bio_format_core::sam_record_serializer::batch_to_alignment_records;
use noodles_sam as sam;
use noodles_sam::alignment::RecordBuf;

/// Converts an Arrow RecordBatch to a vector of BAM records.
pub fn batch_to_bam_records(
    batch: &RecordBatch,
    header: &sam::Header,
    tag_fields: &[String],
    coordinate_system_zero_based: bool,
) -> Result<Vec<RecordBuf>> {
    batch_to_alignment_records(batch, header, tag_fields, coordinate_system_zero_based)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use noodles_sam::header::record::value::Map;
    use noodles_sam::header::record::value::map::ReferenceSequence;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    #[test]
    fn test_batch_to_bam_records_basic() -> Result<()> {
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

        let records = batch_to_bam_records(&batch, &header, &[], true)?;
        assert_eq!(records.len(), 1);

        Ok(())
    }

    #[test]
    fn test_batch_to_bam_records_rejects_flag_overflow() {
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

        let error = batch_to_bam_records(&batch, &header, &[], true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not fit into 16-bit SAM flags"),
            "unexpected error: {error}"
        );
    }
}
