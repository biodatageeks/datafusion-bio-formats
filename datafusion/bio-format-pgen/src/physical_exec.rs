use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::try_stream;
use bytes::Bytes;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, ListBuilder, StringArray, StringBuilder, StructArray,
    UInt16Builder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genotype::{
    GenotypeBatchSizer, GenotypeMetric, GenotypeScanMetrics,
};
use datafusion_bio_format_core::range_planning::ByteRange;

use crate::decode::{DecodedRecord, decode_main_track, decode_record_and_main};
use crate::fileset::PgenFileset;

#[derive(Clone, Debug)]
pub(crate) struct PgenPartition {
    pub(crate) owned: Vec<usize>,
    pub(crate) required: Vec<usize>,
    pub(crate) ranges: Vec<ByteRange>,
}

#[derive(Debug)]
struct DecodedRow {
    variant_index: usize,
    genotypes: Option<DecodedRecord>,
}

/// Physical execution plan for a PGEN scan.
pub struct PgenExec {
    pub(crate) fileset: Arc<PgenFileset>,
    pub(crate) schema: SchemaRef,
    pub(crate) partitions: Arc<Vec<PgenPartition>>,
    pub(crate) genotype_fields: Arc<Vec<String>>,
    pub(crate) metrics: Arc<GenotypeScanMetrics>,
    pub(crate) batch_soft_byte_limit: usize,
    pub(crate) cache: Arc<PlanProperties>,
}

impl PgenExec {
    /// Returns a snapshot of PGEN planning and execution counters.
    pub fn metrics_snapshot(&self) -> [(GenotypeMetric, u64); 18] {
        self.metrics.snapshot()
    }
}

impl Debug for PgenExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgenExec")
            .field("pgen_path", &self.fileset.pgen_path)
            .field("partitions", &self.partitions.len())
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for PgenExec {
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
            "PgenExec: projection=[{columns}], partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for PgenExec {
    fn name(&self) -> &str {
        "PgenExec"
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
                "PGEN partition {partition} is out of bounds for {} partitions",
                self.partitions.len()
            ))
        })?;
        let fileset = self.fileset.clone();
        let schema = self.schema.clone();
        let metrics = self.metrics.clone();
        let max_rows = context.session_config().batch_size();
        let soft_bytes = self.batch_soft_byte_limit;
        let genotype_fields = self.genotype_fields.clone();
        let genotypes_projected = schema.index_of("genotypes").is_ok();

        let stream_schema = schema.clone();
        let stream = try_stream! {
            let selected_sample_count = fileset.selected_samples.source_indices().len();
            let estimated_row_bytes = if genotypes_projected {
                estimate_genotype_bytes(selected_sample_count, &genotype_fields)
            } else {
                0
            };
            let mut sizer = GenotypeBatchSizer::new(max_rows, soft_bytes)?;
            let mut rows = Vec::with_capacity(max_rows);

            if assignment.ranges.is_empty() {
                for variant_index in assignment.owned {
                    if sizer.should_flush_before(estimated_row_bytes) {
                        let row_count = rows.len();
                        let batch = build_batch(&fileset, schema.clone(), &genotype_fields, &rows)?;
                        record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                        yield batch;
                        rows.clear();
                        sizer.reset();
                    }
                    rows.push(DecodedRow {
                        variant_index,
                        genotypes: genotypes_projected.then(|| DecodedRecord {
                            gt: Vec::new(),
                            phased: Vec::new(),
                            ds: Vec::new(),
                            hds: Vec::new(),
                        }),
                    });
                    sizer.push_row(estimated_row_bytes);
                }
            } else {
                let owned = assignment.owned.iter().copied().collect::<HashSet<_>>();
                let retained_bases = assignment
                    .required
                    .iter()
                    .filter_map(|&index| fileset.records[index].ld_base)
                    .collect::<HashSet<_>>();
                let mut main_tracks: HashMap<usize, Vec<u8>> =
                    HashMap::with_capacity(retained_bases.len());
                let mut required_position = 0;
                for range in assignment.ranges {
                    let bytes = fileset
                        .source
                        .read_range(&fileset.pgen_path, range.start..range.end)
                        .await?;
                    metrics.add(GenotypeMetric::RangeRequests, 1);
                    metrics.add(GenotypeMetric::CoalescedRanges, 1);
                    metrics.add(GenotypeMetric::PrimaryBytesRead, bytes.len() as u64);

                    while let Some(&variant_index) = assignment.required.get(required_position) {
                        let record = &fileset.records[variant_index];
                        if record.offset >= range.end {
                            break;
                        }
                        if record.offset < range.start || record.end() > range.end {
                            Err(DataFusionError::Execution(format!(
                                "PGEN variant {variant_index} range {}..{} is not contained in planned range {}..{}",
                                record.offset,
                                record.end(),
                                range.start,
                                range.end
                            )))?;
                        }
                        let payload =
                            record_payload(range, &bytes, record.offset, record.end(), variant_index)?;
                    let base_track = record
                        .ld_base
                        .map(|base| {
                            main_tracks.get(&base).cloned().ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "PGEN variant {variant_index} dependency base {base} was not decoded first"
                                ))
                            })
                        })
                        .transpose()?;
                    let base = base_track.as_deref();
                    if !owned.contains(&variant_index) {
                        let main = decode_main_track(
                            payload,
                            fileset.mode,
                            record.record_type,
                            variant_index,
                            fileset.sample_count,
                            base,
                        )?;
                        main_tracks.insert(variant_index, main);
                        metrics.add(GenotypeMetric::DependencyRecords, 1);
                        required_position += 1;
                        continue;
                    }

                    if sizer.should_flush_before(estimated_row_bytes) {
                        let row_count = rows.len();
                        let batch = build_batch(&fileset, schema.clone(), &genotype_fields, &rows)?;
                        record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                        yield batch;
                        rows.clear();
                        sizer.reset();
                    }
                    let (decoded, main) = decode_record_and_main(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        fileset.variants[variant_index].allele_count(),
                        fileset.selected_samples.source_indices(),
                        base,
                    )?;
                    if retained_bases.contains(&variant_index) {
                        main_tracks.insert(variant_index, main);
                    }
                    metrics.add(
                        GenotypeMetric::SamplesDecoded,
                        selected_sample_count as u64,
                    );
                    metrics.add(
                        GenotypeMetric::SampleValuesSkipped,
                        fileset.sample_count.saturating_sub(selected_sample_count) as u64,
                    );
                    rows.push(DecodedRow {
                        variant_index,
                        genotypes: Some(decoded),
                    });
                    sizer.push_row(estimated_row_bytes);
                    required_position += 1;
                    }
                }
                if required_position != assignment.required.len() {
                    Err(DataFusionError::Execution(format!(
                        "{} required PGEN records were not covered by planned ranges",
                        assignment.required.len() - required_position
                    )))?;
                }
            }

            if !rows.is_empty() {
                let row_count = rows.len();
                let batch = build_batch(&fileset, schema.clone(), &genotype_fields, &rows)?;
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

fn record_payload(
    range: ByteRange,
    bytes: &Bytes,
    start: u64,
    end: u64,
    variant_index: usize,
) -> Result<&[u8]> {
    let relative_start = usize::try_from(start - range.start).map_err(|_| {
        DataFusionError::Execution("PGEN record start does not fit usize".to_string())
    })?;
    let relative_end = usize::try_from(end - range.start).map_err(|_| {
        DataFusionError::Execution("PGEN record end does not fit usize".to_string())
    })?;
    bytes.get(relative_start..relative_end).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "PGEN variant {variant_index} slice is outside its loaded range"
        ))
    })
}

fn estimate_genotype_bytes(samples: usize, fields: &[String]) -> usize {
    fields.iter().fold(0_usize, |bytes, field| {
        bytes.saturating_add(match field.as_str() {
            "GT" => samples.saturating_mul(5),
            "PHASED" => samples.saturating_mul(2),
            "DS" => samples.saturating_mul(5),
            "HDS" => samples.saturating_mul(9),
            _ => 0,
        })
    })
}

fn record_batch_metrics(metrics: &GenotypeScanMetrics, rows: usize, genotype_bytes: usize) {
    metrics.add(GenotypeMetric::Batches, 1);
    metrics.add(GenotypeMetric::BatchRows, rows as u64);
    metrics.add(GenotypeMetric::EmittedVariants, rows as u64);
    metrics.add(GenotypeMetric::GenotypeBytes, genotype_bytes as u64);
}

fn build_batch(
    fileset: &PgenFileset,
    schema: SchemaRef,
    genotype_fields: &[String],
    rows: &[DecodedRow],
) -> Result<RecordBatch> {
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
            "ref" => Ok(Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| fileset.variants[row.variant_index].reference.as_str()),
            )) as ArrayRef),
            "alt" => build_alt_array(fileset, field.data_type(), rows),
            "genotypes" => build_genotype_array(field.data_type(), genotype_fields, rows),
            name => Err(DataFusionError::Execution(format!(
                "unsupported projected PGEN column {name}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .map_err(DataFusionError::from)
}

fn build_alt_array(
    fileset: &PgenFileset,
    data_type: &DataType,
    rows: &[DecodedRow],
) -> Result<ArrayRef> {
    let DataType::List(allele_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN alt field is not a list".to_string(),
        ));
    };
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(allele_field.clone());
    for row in rows {
        for allele in &fileset.variants[row.variant_index].alternate {
            builder.values().append_value(allele);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn build_genotype_array(
    data_type: &DataType,
    genotype_fields: &[String],
    rows: &[DecodedRow],
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN genotypes field is not a struct".to_string(),
        ));
    };
    let arrays = genotype_fields
        .iter()
        .zip(fields)
        .map(|(name, field)| match name.as_str() {
            "GT" => build_gt_array(field.data_type(), rows),
            "PHASED" => build_phased_array(field.data_type(), rows),
            "DS" => build_ds_array(field.data_type(), rows),
            "HDS" => build_hds_array(field.data_type(), rows),
            _ => Err(DataFusionError::Execution(format!(
                "unsupported PGEN genotype child {name}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::new(
        Fields::from(fields.iter().cloned().collect::<Vec<_>>()),
        arrays,
        None,
    )))
}

fn build_gt_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN GT field is not a list".to_string(),
        ));
    };
    let DataType::List(allele_field) = sample_field.data_type() else {
        return Err(DataFusionError::Execution(
            "PGEN GT sample field is not a list".to_string(),
        ));
    };
    let samples = ListBuilder::new(UInt16Builder::new()).with_field(allele_field.clone());
    let mut builder = ListBuilder::new(samples).with_field(sample_field.clone());
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        for call in &decoded.gt {
            if let Some(call) = call {
                builder.values().values().append_slice(call);
                builder.values().append(true);
            } else {
                builder.values().append(false);
            }
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn build_phased_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN PHASED field is not a list".to_string(),
        ));
    };
    let mut builder = ListBuilder::new(BooleanBuilder::new()).with_field(sample_field.clone());
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        for value in &decoded.phased {
            builder.values().append_option(*value);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn build_ds_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN DS field is not a list".to_string(),
        ));
    };
    let mut builder = ListBuilder::new(Float32Builder::new()).with_field(sample_field.clone());
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        for value in &decoded.ds {
            builder.values().append_option(*value);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn build_hds_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN HDS field is not a list".to_string(),
        ));
    };
    let DataType::List(haplotype_field) = sample_field.data_type() else {
        return Err(DataFusionError::Execution(
            "PGEN HDS sample field is not a list".to_string(),
        ));
    };
    let samples = ListBuilder::new(Float32Builder::new()).with_field(haplotype_field.clone());
    let mut builder = ListBuilder::new(samples).with_field(sample_field.clone());
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        for value in &decoded.hds {
            if let Some(value) = value {
                builder.values().values().append_slice(value);
                builder.values().append(true);
            } else {
                builder.values().append(false);
            }
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}
