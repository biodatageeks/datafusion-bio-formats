use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, ListBuilder, StringArray, StructArray, UInt8Builder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genotype::{
    GenotypeBatchSizer, GenotypeMetric, GenotypeScanMetrics,
};
use datafusion_bio_format_core::range_planning::ByteRange;

use crate::fileset::{BED_HEADER_LEN, PlinkFileset, PlinkRangeReader};

#[derive(Clone, Debug)]
pub(crate) struct PlinkReadRange {
    pub(crate) range: ByteRange,
    pub(crate) variants: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct PlinkPartition {
    pub(crate) variants: Vec<usize>,
    pub(crate) ranges: Vec<PlinkReadRange>,
}

#[derive(Debug)]
struct DecodedRow {
    variant_index: usize,
    genotypes: Option<Vec<Option<u8>>>,
}

/// Physical execution plan for a PLINK 1 scan.
pub struct PlinkExec {
    pub(crate) fileset: Arc<PlinkFileset>,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) partitions: Arc<Vec<PlinkPartition>>,
    pub(crate) batch_soft_byte_limit: usize,
    pub(crate) metrics: Arc<GenotypeScanMetrics>,
    pub(crate) cache: Arc<PlanProperties>,
}

impl PlinkExec {
    /// Returns a snapshot of PLINK genotype planning and execution counters.
    pub fn metrics_snapshot(&self) -> [(GenotypeMetric, u64); 18] {
        self.metrics.snapshot()
    }
}

impl Debug for PlinkExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlinkExec")
            .field("bed_path", &self.fileset.bed_path)
            .field("projection", &self.projection)
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for PlinkExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            formatter,
            "PlinkExec: projection=[{columns}], partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for PlinkExec {
    fn name(&self) -> &str {
        "PlinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let assignment = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "PLINK partition {partition} is out of bounds for {} partitions",
                self.partitions.len()
            ))
        })?;
        let fileset = self.fileset.clone();
        let schema = self.schema.clone();
        let metrics = self.metrics.clone();
        let max_rows = context.session_config().batch_size();
        let soft_bytes = self.batch_soft_byte_limit;
        let genotypes_projected = schema.index_of("genotypes").is_ok();

        let stream_schema = schema.clone();
        let stream = try_stream! {
            let estimated_row_bytes = if genotypes_projected {
                fileset.selected_samples.source_indices().len().saturating_mul(2)
            } else {
                0
            };
            let mut sizer = GenotypeBatchSizer::new(max_rows, soft_bytes)?;
            let mut rows = Vec::with_capacity(max_rows);

            if assignment.ranges.is_empty() {
                for variant_index in assignment.variants {
                    if sizer.should_flush_before(estimated_row_bytes) {
                        let row_count = rows.len();
                        let batch = build_batch(&fileset, schema.clone(), &rows)?;
                        record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                        yield batch;
                        rows.clear();
                        sizer.reset();
                    }
                    rows.push(DecodedRow {
                        variant_index,
                        genotypes: genotypes_projected.then(Vec::new),
                    });
                    sizer.push_row(estimated_row_bytes);
                }
            } else {
                let mut reader =
                    PlinkRangeReader::open(&fileset.bed_path, &fileset.object_storage_options)
                        .await?;
                for planned in assignment.ranges {
                    let payload = reader
                    .read_range(
                        &fileset.bed_path,
                        planned.range.start..planned.range.end,
                    )
                    .await?;
                    metrics.add(GenotypeMetric::RangeRequests, 1);
                    metrics.add(GenotypeMetric::CoalescedRanges, 1);
                    metrics.add(GenotypeMetric::PrimaryBytesRead, payload.len() as u64);

                    for variant_index in planned.variants {
                        if sizer.should_flush_before(estimated_row_bytes) {
                            let row_count = rows.len();
                            let batch = build_batch(&fileset, schema.clone(), &rows)?;
                            record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                            yield batch;
                            rows.clear();
                            sizer.reset();
                        }
                        let variant_payload =
                            variant_payload(&payload, planned.range, variant_index, fileset.bytes_per_variant)?;
                        validate_padding(variant_payload, fileset.sample_count, variant_index)?;
                        let genotypes = decode_selected_samples(
                            variant_payload,
                            fileset.selected_samples.source_indices(),
                        );
                        metrics.add(
                            GenotypeMetric::SamplesDecoded,
                            genotypes.len() as u64,
                        );
                        metrics.add(
                            GenotypeMetric::SampleValuesSkipped,
                            fileset.sample_count.saturating_sub(genotypes.len()) as u64,
                        );
                        rows.push(DecodedRow {
                            variant_index,
                            genotypes: Some(genotypes),
                        });
                        sizer.push_row(estimated_row_bytes);
                    }
                }
            }

            if !rows.is_empty() {
                let row_count = rows.len();
                let batch = build_batch(&fileset, schema.clone(), &rows)?;
                record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

fn record_batch_metrics(metrics: &GenotypeScanMetrics, rows: usize, genotype_bytes: usize) {
    metrics.add(GenotypeMetric::Batches, 1);
    metrics.add(GenotypeMetric::BatchRows, rows as u64);
    metrics.add(GenotypeMetric::EmittedVariants, rows as u64);
    metrics.add(GenotypeMetric::GenotypeBytes, genotype_bytes as u64);
}

fn variant_payload(
    bytes: &[u8],
    range: ByteRange,
    variant_index: usize,
    bytes_per_variant: u64,
) -> Result<&[u8]> {
    let index = u64::try_from(variant_index).map_err(|_| {
        DataFusionError::Execution("PLINK variant index does not fit u64".to_string())
    })?;
    let absolute_start = index
        .checked_mul(bytes_per_variant)
        .and_then(|offset| offset.checked_add(BED_HEADER_LEN))
        .ok_or_else(|| {
            DataFusionError::Execution("PLINK variant offset arithmetic overflowed".to_string())
        })?;
    let relative_start = absolute_start
        .checked_sub(range.start)
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "PLINK variant {variant_index} is not contained in planned range"
            ))
        })?;
    let width = usize::try_from(bytes_per_variant).map_err(|_| {
        DataFusionError::Execution("PLINK variant width does not fit usize".to_string())
    })?;
    let relative_end = relative_start.checked_add(width).ok_or_else(|| {
        DataFusionError::Execution("PLINK payload slice arithmetic overflowed".to_string())
    })?;
    bytes.get(relative_start..relative_end).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "PLINK BED range is truncated while reading variant {variant_index}"
        ))
    })
}

fn validate_padding(payload: &[u8], sample_count: usize, variant_index: usize) -> Result<()> {
    let used_slots = sample_count % 4;
    if used_slots == 0 {
        return Ok(());
    }
    let Some(&last) = payload.last() else {
        return Err(DataFusionError::Execution(format!(
            "PLINK BED variant {variant_index} has no payload"
        )));
    };
    let used_bits = used_slots * 2;
    let unused_mask = !((1_u8 << used_bits) - 1);
    if last & unused_mask != 0 {
        return Err(DataFusionError::Execution(format!(
            "PLINK BED variant {variant_index} has non-zero unused padding bits"
        )));
    }
    Ok(())
}

fn decode_selected_samples(payload: &[u8], sample_indices: &[usize]) -> Vec<Option<u8>> {
    sample_indices
        .iter()
        .map(|&sample_index| {
            let byte = payload[sample_index / 4];
            match (byte >> ((sample_index % 4) * 2)) & 0b11 {
                0b00 => Some(2),
                0b10 => Some(1),
                0b11 => Some(0),
                0b01 => None,
                _ => unreachable!("a two-bit value has four states"),
            }
        })
        .collect()
}

fn build_batch(
    fileset: &PlinkFileset,
    schema: SchemaRef,
    rows: &[DecodedRow],
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, Vec::new(), &options)
            .map_err(DataFusionError::from);
    }

    let arrays = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "chrom" => Ok(Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].chrom.as_str()),
            )) as ArrayRef),
            "start" => Ok(Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].start),
            )) as ArrayRef),
            "end" => Ok(Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].end),
            )) as ArrayRef),
            "id" => Ok(Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].id.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            "cm" => Ok(Arc::new(Float64Array::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].cm),
            )) as ArrayRef),
            "a1" => Ok(Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].a1.as_str()),
            )) as ArrayRef),
            "a2" => Ok(Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].a2.as_str()),
            )) as ArrayRef),
            "genotypes" => build_genotypes(field.data_type(), rows),
            name => Err(DataFusionError::Execution(format!(
                "unsupported PLINK projected field {name}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn build_genotypes(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(DataFusionError::Execution(
            "PLINK genotypes field is not a struct".to_string(),
        ));
    };
    let mut builder = ListBuilder::new(UInt8Builder::new());
    for row in rows {
        let genotypes = row.genotypes.as_deref().ok_or_else(|| {
            DataFusionError::Execution(
                "PLINK genotype projection was planned without decoded values".to_string(),
            )
        })?;
        for value in genotypes {
            builder.values().append_option(*value);
        }
        builder.append(true);
    }
    let gt = Arc::new(builder.finish()) as ArrayRef;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![gt],
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_a1_dosage_and_missingness_in_requested_order() {
        // sample codes in source order: 00, 10, 11, 01
        let payload = [0b01_11_10_00];
        assert_eq!(
            decode_selected_samples(&payload, &[3, 0, 2, 1]),
            vec![None, Some(2), Some(0), Some(1)]
        );
    }

    #[test]
    fn rejects_nonzero_high_padding_slots() {
        assert!(validate_padding(&[0b00_00_00_11], 1, 7).is_ok());
        let error = validate_padding(&[0b01_00_00_11], 1, 7)
            .unwrap_err()
            .to_string();
        assert!(error.contains("variant 7"));
    }
}
