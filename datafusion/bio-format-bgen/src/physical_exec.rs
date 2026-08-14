use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float32Builder, ListArray, ListBuilder, StringArray,
    StringBuilder, StructArray, UInt8Array, UInt8Builder, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
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

use crate::decode::{DecodeScratch, DecodedGenotypes, DecodedValues, decode_variant};
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

#[derive(Debug)]
struct DecodedRow {
    variant_index: usize,
    genotypes: Option<DecodedGenotypes>,
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
        let payload_derived_projected = genotype_projected
            || schema.index_of("phased").is_ok()
            || schema.index_of("bits").is_ok();

        let stream = try_stream! {
            let mut sizer = GenotypeBatchSizer::new(max_rows, soft_bytes)?;
            let mut rows = Vec::with_capacity(max_rows);
            // Decoding buffers live for the whole partition so per-variant work
            // reuses one decompressor and one set of probability buffers.
            let mut scratch = DecodeScratch::new();

            if assignment.ranges.is_empty() {
                for variant_index in assignment.variants {
                    let estimated_row_bytes = 0;
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
                        genotypes: if genotype_projected {
                            Some(empty_genotypes(fileset.options.output_mode))
                        } else {
                            None
                        },
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
                    metrics.add(GenotypeMetric::CompressedBytes, bytes.len() as u64);

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
                        let decoded = decode_variant(
                            &fileset.path,
                            variant,
                            &fileset.header,
                            payload,
                            decode_samples,
                            &fileset.options,
                            &mut scratch,
                        )?;
                        let estimated_row_bytes = if genotype_projected {
                            decoded.estimated_arrow_bytes()
                        } else {
                            0
                        };
                        if sizer.should_flush_before(estimated_row_bytes) {
                            let row_count = rows.len();
                            let batch = build_batch(&fileset, schema.clone(), &rows)?;
                            record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                            yield batch;
                            rows.clear();
                            sizer.reset();
                        }
                        metrics.add(
                            GenotypeMetric::DecompressedBytes,
                            decoded.decompressed_bytes as u64,
                        );
                        metrics.add(
                            GenotypeMetric::SamplesDecoded,
                            fileset.selected_samples.source_indices().len() as u64,
                        );
                        metrics.add(
                            GenotypeMetric::SampleValuesSkipped,
                            (fileset.header.sample_count as usize)
                                .saturating_sub(fileset.selected_samples.source_indices().len())
                                as u64,
                        );
                        rows.push(DecodedRow {
                            variant_index,
                            genotypes: payload_derived_projected.then_some(decoded),
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

fn empty_genotypes(mode: BgenOutputMode) -> DecodedGenotypes {
    DecodedGenotypes {
        phased: false,
        bits: 0,
        ploidy: Vec::new(),
        values: match mode {
            BgenOutputMode::Probability => {
                DecodedValues::Probabilities(crate::decode::ProbabilityValues::default())
            }
            BgenOutputMode::Dosage => DecodedValues::Dosages(Vec::new()),
        },
        decompressed_bytes: 0,
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
                rows.iter()
                    .map(|row| {
                        row.genotypes
                            .as_ref()
                            .map(|decoded| decoded.phased)
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            "bits" => Ok(
                Arc::new(UInt8Array::from_iter_values(rows.iter().map(|row| {
                    row.genotypes
                        .as_ref()
                        .map(|decoded| decoded.bits)
                        .unwrap_or(0)
                }))) as ArrayRef,
            ),
            "genotypes" => build_genotypes(field.data_type(), rows, fileset.options.output_mode),
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

fn build_genotypes(
    data_type: &DataType,
    rows: &[DecodedRow],
    mode: BgenOutputMode,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(DataFusionError::Execution(
            "BGEN genotypes field is not a struct".to_string(),
        ));
    };
    let values: ArrayRef = match mode {
        BgenOutputMode::Probability => {
            let state_field = Arc::new(Field::new("state", DataType::Float32, false));
            let sample_field = Arc::new(Field::new(
                "sample",
                DataType::List(state_field.clone()),
                true,
            ));
            // The decoder already produced Arrow's own layout, so the states,
            // sample offsets, and sample validity move into the arrays without
            // being appended value by value.
            let mut states: Vec<f32> = Vec::new();
            let mut sample_offsets: Vec<i32> = vec![0];
            let mut sample_valid: Vec<bool> = Vec::new();
            let mut variant_offsets: Vec<i32> = vec![0];
            for row in rows {
                let decoded = row.genotypes.as_ref().ok_or_else(|| {
                    DataFusionError::Execution(
                        "BGEN genotype projection has no decoded payload".to_string(),
                    )
                })?;
                let DecodedValues::Probabilities(samples) = &decoded.values else {
                    return Err(DataFusionError::Execution(
                        "BGEN decoded dosage in probability mode".to_string(),
                    ));
                };
                let base = i32::try_from(states.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "BGEN probability offsets exceed the 32-bit Arrow list limit".to_string(),
                    )
                })?;
                states.extend_from_slice(&samples.values);
                // Each per-variant offset and the running base can both fit in
                // i32 while their sum does not, so the sum is what must be
                // checked; an unchecked add would wrap into a non-monotonic
                // Arrow offset in release builds.
                for offset in samples.offsets.iter().skip(1) {
                    sample_offsets.push(offset.checked_add(base).ok_or_else(|| {
                        DataFusionError::Execution(
                            "BGEN probability offsets exceed the 32-bit Arrow list limit"
                                .to_string(),
                        )
                    })?);
                }
                sample_valid.extend_from_slice(&samples.valid);
                variant_offsets.push(i32::try_from(sample_valid.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "BGEN sample offsets exceed the 32-bit Arrow list limit".to_string(),
                    )
                })?);
            }
            let states = Arc::new(Float32Array::from(states)) as ArrayRef;
            let samples = ListArray::try_new(
                state_field,
                OffsetBuffer::new(ScalarBuffer::from(sample_offsets)),
                states,
                Some(NullBuffer::from(sample_valid)),
            )?;
            Arc::new(ListArray::try_new(
                sample_field,
                OffsetBuffer::new(ScalarBuffer::from(variant_offsets)),
                Arc::new(samples) as ArrayRef,
                None,
            )?)
        }
        BgenOutputMode::Dosage => {
            let mut builder = ListBuilder::new(Float32Builder::new())
                .with_field(Arc::new(Field::new("item", DataType::Float32, true)));
            for row in rows {
                let decoded = row.genotypes.as_ref().ok_or_else(|| {
                    DataFusionError::Execution(
                        "BGEN genotype projection has no decoded payload".to_string(),
                    )
                })?;
                let DecodedValues::Dosages(samples) = &decoded.values else {
                    return Err(DataFusionError::Execution(
                        "BGEN decoded probabilities in dosage mode".to_string(),
                    ));
                };
                for dosage in samples {
                    builder.values().append_option(*dosage);
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
    };
    let mut ploidy = ListBuilder::new(UInt8Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::UInt8,
        false,
    )));
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("BGEN genotype projection has no decoded ploidy".to_string())
        })?;
        // Declared ploidy is never null, so the whole row is one bulk append.
        ploidy.values().append_slice(&decoded.ploidy);
        ploidy.append(true);
    }
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![values, Arc::new(ploidy.finish())],
        None,
    )?))
}
