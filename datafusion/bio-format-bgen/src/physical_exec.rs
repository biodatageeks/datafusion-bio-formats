use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Builder, ListBuilder, StringArray, StringBuilder, StructArray,
    UInt8Array, UInt8Builder, UInt64Array,
};
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

use crate::decode::{DecodedGenotypes, DecodedValues, decode_variant};
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
                        let decoded = decode_variant(
                            &fileset.path,
                            variant,
                            &fileset.header,
                            payload,
                            fileset.selected_samples.source_indices(),
                            &fileset.options,
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
            BgenOutputMode::Probability => DecodedValues::Probabilities(Vec::new()),
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
            let samples = ListBuilder::new(Float32Builder::new()).with_field(state_field);
            let mut outer = ListBuilder::new(samples).with_field(sample_field);
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
                for sample in samples {
                    if let Some(probabilities) = sample {
                        for probability in probabilities {
                            outer.values().values().append_value(*probability);
                        }
                        outer.values().append(true);
                    } else {
                        outer.values().append(false);
                    }
                }
                outer.append(true);
            }
            Arc::new(outer.finish())
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
        for value in &decoded.ploidy {
            ploidy.values().append_value(*value);
        }
        ploidy.append(true);
    }
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![values, Arc::new(ploidy.finish())],
        None,
    )?))
}
