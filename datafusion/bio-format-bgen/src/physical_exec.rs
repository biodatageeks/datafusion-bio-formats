use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, ListArray, ListBuilder, StringArray,
    StringBuilder, StructArray, UInt8Array, UInt64Array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genotype::{
    GenotypeBatchSizer, GenotypeMetric, GenotypeScanMetrics,
};
use datafusion_bio_format_core::range_planning::ByteRange;

use crate::buffers::{BufferLayout, GenotypeBuffers, TakenBuffers};
use crate::decode::{DecodeScratch, decode_variant};
use crate::table_provider::{BgenFileset, BgenOutputMode};

#[derive(Clone, Debug)]
pub(crate) struct BgenReadRange {
    pub(crate) range: ByteRange,
    pub(crate) variants: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct BgenPartition {
    pub(crate) variants: Vec<usize>,
    pub(crate) ranges: Vec<BgenReadRange>,
}

/// One emitted row's per-variant metadata.
///
/// The genotypes themselves are not here: the decoder writes them straight into
/// the batch's [`GenotypeBuffers`], so a row carries only what the block header
/// says about it.
#[derive(Debug)]
struct DecodedRow {
    variant_index: usize,
    phased: bool,
    bits: u8,
}

/// Physical execution plan for a BGEN scan.
pub struct BgenExec {
    pub(crate) fileset: Arc<BgenFileset>,
    pub(crate) schema: SchemaRef,
    pub(crate) partitions: Arc<Vec<BgenPartition>>,
    pub(crate) metrics: Arc<GenotypeScanMetrics>,
    pub(crate) batch_soft_byte_limit: usize,
    pub(crate) cache: Arc<PlanProperties>,
}

impl BgenExec {
    /// Returns a snapshot of genotype planning and execution counters.
    pub fn metrics_snapshot(&self) -> [(GenotypeMetric, u64); 18] {
        self.metrics.snapshot()
    }
}

impl Debug for BgenExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BgenExec")
            .field("path", &self.fileset.path)
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for BgenExec {
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
            "BgenExec: projection=[{columns}], partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for BgenExec {
    fn name(&self) -> &str {
        "BgenExec"
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
                "BGEN partition {partition} is out of bounds for {} partitions",
                self.partitions.len()
            ))
        })?;
        let fileset = self.fileset.clone();
        let schema = self.schema.clone();
        let stream_schema = schema.clone();
        let metrics = self.metrics.clone();
        let max_rows = context.session_config().batch_size();
        let soft_bytes = self.batch_soft_byte_limit;
        let genotype_projected = schema.index_of("genotypes").is_ok();

        let stream = try_stream! {
            let mut sizer = GenotypeBatchSizer::new(max_rows, soft_bytes)?;
            let mut rows = Vec::with_capacity(max_rows);
            // Decoding buffers live for the whole partition so per-variant work
            // reuses one decompressor and one set of probability buffers.
            let mut scratch = DecodeScratch::new();
            // The decoder appends into these, and a finished batch moves them
            // into its Arrow arrays.
            let mut buffers = GenotypeBuffers::new(
                match (fileset.options.output_mode, fileset.probability_shape) {
                    (BgenOutputMode::Dosage, _) => BufferLayout::Dosage,
                    (BgenOutputMode::Probability, Some(shape)) => {
                        BufferLayout::FixedProbability(shape.width)
                    }
                    (BgenOutputMode::Probability, None) => BufferLayout::NestedProbability,
                },
            );

            if assignment.ranges.is_empty() {
                for variant_index in assignment.variants {
                    let estimated_row_bytes = 0;
                    if sizer.should_flush_before(estimated_row_bytes) {
                        let row_count = rows.len();
                        let batch = build_batch(&fileset, schema.clone(), &rows, buffers.take())?;
                        record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                        yield batch;
                        rows.clear();
                        sizer.reset();
                    }
                    // No payload was read, so the row has no samples and no
                    // header-derived values; it still closes a variant, because
                    // the batch's row count comes from the variant offsets.
                    buffers.finish_variant()?;
                    rows.push(DecodedRow {
                        variant_index,
                        phased: false,
                        bits: 0,
                    });
                    sizer.push_row(estimated_row_bytes);
                }
            } else {
                for planned in assignment.ranges {
                    let bytes = fileset
                        .source
                        .read_range(
                            &fileset.path,
                            planned.range.start..planned.range.end,
                        )
                        .await?;
                    metrics.add(GenotypeMetric::RangeRequests, 1);
                    metrics.add(GenotypeMetric::CoalescedRanges, 1);
                    metrics.add(GenotypeMetric::PrimaryBytesRead, bytes.len() as u64);
                    // A coalesced range bridges the metadata between consecutive
                    // payloads, so its length is what was downloaded, not what
                    // was compressed genotype data. Counting the range would
                    // report the bridged metadata as compressed bytes and skew
                    // any compression ratio derived from these counters.
                    let compressed_bytes: u64 = planned
                        .variants
                        .iter()
                        .map(|&index| fileset.catalog.variants[index].payload_size)
                        .sum();
                    metrics.add(GenotypeMetric::CompressedBytes, compressed_bytes);

                    for variant_index in planned.variants {
                        let variant = &fileset.catalog.variants[variant_index];
                        let relative_start = variant
                            .payload_offset
                            .checked_sub(planned.range.start)
                            .and_then(|value| usize::try_from(value).ok())
                            .ok_or_else(|| DataFusionError::Execution(
                                "BGEN payload offset is outside its planned range".to_string(),
                            ))?;
                        let payload_size =
                            usize::try_from(variant.payload_size).map_err(|_| {
                                DataFusionError::Execution(
                                    "BGEN payload size does not fit usize".to_string(),
                                )
                            })?;
                        let relative_end = relative_start
                            .checked_add(payload_size)
                            .ok_or_else(|| DataFusionError::Execution(
                                "BGEN payload slice overflowed".to_string(),
                            ))?;
                        let payload = bytes.get(relative_start..relative_end).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "BGEN variant {variant_index} is outside its planned byte range"
                            ))
                        })?;
                        // `phased` and `bits` come from the block header, so a
                        // projection that wants only those must not reconstruct
                        // per-sample genotypes: the arrays would be built and
                        // then discarded, and a wide cohort would accumulate a
                        // batch of them.
                        let decode_samples = if genotype_projected {
                            fileset.selected_samples.source_indices()
                        } else {
                            &[]
                        };
                        let mark = buffers.mark();
                        let decoded = match decode_variant(
                            &fileset.path,
                            variant,
                            &fileset.header,
                            payload,
                            decode_samples,
                            &fileset.options,
                            &mut scratch,
                            &mut buffers,
                        ) {
                            Ok(decoded) => decoded,
                            Err(error) => {
                                // A failed variant leaves a partial row behind;
                                // drop it so the buffers stay a valid Arrow
                                // prefix rather than a torn row.
                                buffers.rollback(mark);
                                Err(error)?
                            }
                        };
                        buffers.finish_variant()?;
                        let estimated_row_bytes = buffers.bytes_since(mark);
                        metrics.add(
                            GenotypeMetric::DecompressedBytes,
                            decoded.decompressed_bytes as u64,
                        );
                        metrics.add(
                            GenotypeMetric::SamplesDecoded,
                            decode_samples.len() as u64,
                        );
                        metrics.add(
                            GenotypeMetric::SampleValuesSkipped,
                            (fileset.header.sample_count as usize)
                                .saturating_sub(decode_samples.len())
                                as u64,
                        );
                        rows.push(DecodedRow {
                            variant_index,
                            phased: decoded.phased,
                            bits: decoded.bits,
                        });
                        sizer.push_row(estimated_row_bytes);
                        // The row is already in the buffers, so the flush
                        // decision comes after it rather than before.
                        if sizer.should_flush_after() {
                            let row_count = rows.len();
                            let batch = build_batch(&fileset, schema.clone(), &rows, buffers.take())?;
                            record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                            yield batch;
                            rows.clear();
                            sizer.reset();
                        }
                    }
                }
            }
            if !rows.is_empty() {
                let row_count = rows.len();
                let batch = build_batch(&fileset, schema.clone(), &rows, buffers.take())?;
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

fn build_batch(
    fileset: &BgenFileset,
    schema: SchemaRef,
    rows: &[DecodedRow],
    buffers: TakenBuffers,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, Vec::new(), &options)
            .map_err(DataFusionError::from);
    }
    // The genotype buffers can only be moved into one array, and a projection
    // names each column at most once.
    let mut buffers = Some(buffers);
    let arrays = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "chrom" => Ok(Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| fileset.catalog.variants[row.variant_index].chrom.as_str()),
            )) as ArrayRef),
            "start" => Ok(Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|row| fileset.catalog.variants[row.variant_index].start),
            )) as ArrayRef),
            "end" => Ok(Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|row| fileset.catalog.variants[row.variant_index].end),
            )) as ArrayRef),
            "id" => Ok(Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| fileset.catalog.variants[row.variant_index].id.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            "rsid" => Ok(Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| fileset.catalog.variants[row.variant_index].rsid.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            "alleles" => build_alleles(fileset, rows),
            "phased" => Ok(Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.phased).collect::<Vec<_>>(),
            )) as ArrayRef),
            "bits" => Ok(Arc::new(UInt8Array::from_iter_values(
                rows.iter().map(|row| row.bits),
            )) as ArrayRef),
            "genotypes" => build_genotypes(
                field.data_type(),
                buffers.take().ok_or_else(|| {
                    DataFusionError::Execution(
                        "BGEN genotype buffers were already consumed by this batch".to_string(),
                    )
                })?,
                fileset.options.output_mode,
            ),
            name => Err(DataFusionError::Execution(format!(
                "unsupported BGEN projected field {name}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn build_alleles(fileset: &BgenFileset, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        false,
    )));
    for row in rows {
        for allele in &fileset.catalog.variants[row.variant_index].alleles {
            builder.values().append_value(allele);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

/// Returns the fixed state width the schema requires, if any.
fn fixed_probability_width(data_type: &DataType) -> Option<i32> {
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    let DataType::List(sample) = fields.first()?.data_type() else {
        return None;
    };
    match sample.data_type() {
        DataType::FixedSizeList(_, width) => Some(*width),
        _ => None,
    }
}

fn build_genotypes(
    data_type: &DataType,
    buffers: TakenBuffers,
    mode: BgenOutputMode,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(DataFusionError::Execution(
            "BGEN genotypes field is not a struct".to_string(),
        ));
    };
    // The decoder wrote Arrow's own layout as it went, so every buffer moves
    // into its array here. Nothing in this function copies a probability.
    let TakenBuffers {
        values,
        sample_offsets,
        nulls,
        variant_offsets,
        ploidy,
        ploidy_offsets,
    } = buffers;
    let genotype_values: ArrayRef = match mode {
        BgenOutputMode::Probability => {
            let width = fixed_probability_width(data_type);
            let state_field = Arc::new(Field::new("state", DataType::Float32, false));
            let sample_field = Arc::new(Field::new(
                "sample",
                match width {
                    Some(width) => DataType::FixedSizeList(state_field.clone(), width),
                    None => DataType::List(state_field.clone()),
                },
                true,
            ));
            let states = Arc::new(Float32Array::from(values)) as ArrayRef;
            let samples: ArrayRef = match width {
                Some(width) => Arc::new(FixedSizeListArray::try_new(
                    state_field,
                    width,
                    states,
                    nulls,
                )?),
                None => Arc::new(ListArray::try_new(
                    state_field,
                    OffsetBuffer::new(ScalarBuffer::from(sample_offsets)),
                    states,
                    nulls,
                )?),
            };
            Arc::new(ListArray::try_new(
                sample_field,
                OffsetBuffer::new(ScalarBuffer::from(variant_offsets)),
                samples,
                None,
            )?)
        }
        BgenOutputMode::Dosage => {
            // A dosage is one value per sample, so the validity belongs to the
            // values array rather than to a per-sample list.
            let dosages = Arc::new(Float32Array::new(ScalarBuffer::from(values), nulls));
            Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                OffsetBuffer::new(ScalarBuffer::from(variant_offsets)),
                dosages,
                None,
            )?)
        }
    };
    // Declared ploidy is never null.
    let ploidy = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt8, false)),
        OffsetBuffer::new(ScalarBuffer::from(ploidy_offsets)),
        Arc::new(UInt8Array::from(ploidy)),
        None,
    )?);
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![genotype_values, ploidy],
        None,
    )?))
}
