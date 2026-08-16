use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_stream::try_stream;
use bytes::Bytes;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeListArray, FixedSizeListBuilder, Float32Builder, ListArray,
    ListBuilder, StringArray, StringBuilder, StructArray, UInt16Array, UInt16Builder, UInt64Array,
};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genotype::{
    GenotypeBatchSizer, GenotypeMetric, GenotypeScanMetrics,
};
use datafusion_bio_format_core::range_planning::ByteRange;

use crate::decode::{
    DecodedRecord, GenotypeProjection, GtDecodeWorkspace, decode_biallelic_gt_into,
    decode_dense_biallelic_gt, decode_main_track, decode_record_and_main,
    supports_biallelic_gt_fast_path,
};
use crate::fileset::{PgenFileset, PgenMode};

#[derive(Clone, Debug)]
pub(crate) struct PgenPartition {
    pub(crate) selection: Arc<Vec<usize>>,
    pub(crate) owned: Range<usize>,
    pub(crate) dependencies: Vec<usize>,
    pub(crate) ranges: Vec<ByteRange>,
}

impl PgenPartition {
    pub(crate) fn owned(&self) -> &[usize] {
        &self.selection[self.owned.clone()]
    }

    pub(crate) fn required(&self) -> impl Iterator<Item = usize> + '_ {
        MergedIndices {
            owned: self.owned(),
            dependencies: &self.dependencies,
            owned_position: 0,
            dependency_position: 0,
        }
    }
}

struct MergedIndices<'a> {
    owned: &'a [usize],
    dependencies: &'a [usize],
    owned_position: usize,
    dependency_position: usize,
}

impl Iterator for MergedIndices<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.owned.get(self.owned_position).copied(),
            self.dependencies.get(self.dependency_position).copied(),
        ) {
            (Some(owned), Some(dependency)) if owned <= dependency => {
                self.owned_position += 1;
                if owned == dependency {
                    self.dependency_position += 1;
                }
                Some(owned)
            }
            (Some(_), Some(dependency)) => {
                self.dependency_position += 1;
                Some(dependency)
            }
            (Some(owned), None) => {
                self.owned_position += 1;
                Some(owned)
            }
            (None, Some(dependency)) => {
                self.dependency_position += 1;
                Some(dependency)
            }
            (None, None) => None,
        }
    }
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
        let genotype_projection = GenotypeProjection::from_fields(&genotype_fields);
        let genotypes_projected = schema.index_of("genotypes").is_ok();

        if genotypes_projected
            && !assignment.ranges.is_empty()
            && genotype_fields.as_slice() == ["GT"]
        {
            return execute_gt_only(assignment, fileset, schema, metrics, max_rows, soft_bytes);
        }

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
                for &variant_index in assignment.owned() {
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
                            ds_stored: Vec::new(),
                            hds: Vec::new(),
                        }),
                    });
                    sizer.push_row(estimated_row_bytes);
                }
            } else {
                let owned = assignment.owned();
                let mut retained_bases = HashSet::new();
                for index in assignment.required() {
                    if let Some(base) = fileset.records.record(index)?.ld_base {
                        retained_bases.insert(base);
                    }
                }
                // The index assigns every LD record to the most recent eligible
                // non-LD record. Required records are processed in source order,
                // so one current base is sufficient and older bases can be
                // dropped as soon as a newer retained base is decoded.
                let mut ld_base_index = None;
                let mut ld_base = Vec::with_capacity(fileset.sample_count);
                let mut required = assignment.required().peekable();
                for range in assignment.ranges.iter().copied() {
                    let bytes = fileset
                        .source
                        .read_range(&fileset.pgen_path, range.start..range.end)
                        .await?;
                    metrics.add(GenotypeMetric::RangeRequests, 1);
                    metrics.add(GenotypeMetric::PrimaryBytesRead, bytes.len() as u64);

                    while let Some(&variant_index) = required.peek() {
                        let record = fileset.records.record(variant_index)?;
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
                        let base = record
                            .ld_base
                            .map(|base_index| {
                                if ld_base_index != Some(base_index) {
                                    return Err(DataFusionError::Execution(format!(
                                        "PGEN variant {variant_index} dependency base {base_index} was not decoded first"
                                    )));
                                }
                                Ok(ld_base.as_slice())
                            })
                            .transpose()?;
                        if owned.binary_search(&variant_index).is_err() {
                            let main = decode_main_track(
                                payload,
                                fileset.mode,
                                record.record_type,
                                variant_index,
                                fileset.sample_count,
                                base,
                            )?;
                            if retained_bases.contains(&variant_index) {
                                ld_base = main;
                                ld_base_index = Some(variant_index);
                            }
                            metrics.add(GenotypeMetric::DependencyRecords, 1);
                            required.next();
                            continue;
                        }

                        if sizer.should_flush_before(estimated_row_bytes) {
                            let row_count = rows.len();
                            let batch =
                                build_batch(&fileset, schema.clone(), &genotype_fields, &rows)?;
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
                            genotype_projection,
                            fileset.selected_samples.source_indices(),
                            base,
                        )?;
                        if retained_bases.contains(&variant_index) {
                            ld_base = main;
                            ld_base_index = Some(variant_index);
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
                        required.next();
                    }
                }
                if let Some(next) = required.next() {
                    Err(DataFusionError::Execution(format!(
                        "required PGEN record {next} was not covered by planned ranges"
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

struct GtBatchBuilder {
    variant_indices: Vec<usize>,
    allele_values: Vec<u16>,
    sample_validity: PackedValidityBuilder,
    selected_sample_count: usize,
    row_capacity: usize,
    sample_field: FieldRef,
    allele_field: FieldRef,
}

impl GtBatchBuilder {
    fn new(schema: &SchemaRef, row_capacity: usize, selected_sample_count: usize) -> Result<Self> {
        let genotype_field = schema.field_with_name("genotypes")?;
        let DataType::Struct(children) = genotype_field.data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN genotypes field is not a struct".to_string(),
            ));
        };
        if children.len() != 1 || children[0].name() != "GT" {
            return Err(DataFusionError::Execution(
                "PGEN GT fast path requires an exact GT-only genotype projection".to_string(),
            ));
        }
        let DataType::List(sample_field) = children[0].data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN GT field is not a list".to_string(),
            ));
        };
        let DataType::FixedSizeList(allele_field, 2) = sample_field.data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN GT sample field is not a fixed-size allele pair".to_string(),
            ));
        };
        Ok(Self {
            variant_indices: Vec::with_capacity(row_capacity),
            allele_values: Vec::with_capacity(
                row_capacity
                    .saturating_mul(selected_sample_count)
                    .saturating_mul(2),
            ),
            sample_validity: PackedValidityBuilder::new(
                row_capacity.saturating_mul(selected_sample_count),
            ),
            selected_sample_count,
            row_capacity,
            sample_field: sample_field.clone(),
            allele_field: allele_field.clone(),
        })
    }

    fn append(&mut self, variant_index: usize, calls: &[Option<[u16; 2]>]) {
        for call in calls {
            if let Some(call) = call {
                self.allele_values.extend_from_slice(call);
                self.sample_validity.append(true);
            } else {
                self.allele_values.extend_from_slice(&[0, 0]);
                self.sample_validity.append(false);
            }
        }
        self.variant_indices.push(variant_index);
    }

    fn append_codes(&mut self, variant_index: usize, codes: &[u8]) -> Result<()> {
        for &code in codes {
            let (left, right, valid) = match code {
                0 => (0, 0, true),
                1 => (0, 1, true),
                2 => (1, 1, true),
                3 => (0, 0, false),
                4 => (1, 0, true),
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid internal biallelic GT code {code}"
                    )));
                }
            };
            self.append_values(left, right, valid);
        }
        self.finish_variant(variant_index);
        Ok(())
    }

    #[inline]
    fn append_values(&mut self, left: u16, right: u16, valid: bool) {
        self.allele_values.push(left);
        self.allele_values.push(right);
        self.sample_validity.append(valid);
    }

    #[inline]
    fn append_chunk(&mut self, alleles: &[u16], validity: u8, sample_count: usize) {
        self.allele_values.extend_from_slice(alleles);
        self.sample_validity.append_packed(validity, sample_count);
    }

    fn finish_variant(&mut self, variant_index: usize) {
        self.variant_indices.push(variant_index);
    }

    fn len(&self) -> usize {
        self.variant_indices.len()
    }

    fn is_empty(&self) -> bool {
        self.variant_indices.is_empty()
    }

    fn finish(&mut self, fileset: &PgenFileset, schema: SchemaRef) -> Result<RecordBatch> {
        let validity = self.sample_validity.finish();
        let allele_values = std::mem::replace(
            &mut self.allele_values,
            Vec::with_capacity(
                self.row_capacity
                    .saturating_mul(self.selected_sample_count)
                    .saturating_mul(2),
            ),
        );
        self.sample_validity = PackedValidityBuilder::new(
            self.row_capacity.saturating_mul(self.selected_sample_count),
        );
        let alleles = Arc::new(UInt16Array::from(allele_values)) as ArrayRef;
        let samples = Arc::new(FixedSizeListArray::new(
            self.allele_field.clone(),
            2,
            alleles,
            validity,
        )) as ArrayRef;
        let gt = Arc::new(ListArray::new(
            self.sample_field.clone(),
            OffsetBuffer::from_repeated_length(self.selected_sample_count, self.len()),
            samples,
            None,
        )) as ArrayRef;
        let batch = build_gt_batch(fileset, schema, &self.variant_indices, gt)?;
        self.variant_indices.clear();
        Ok(batch)
    }
}

struct PackedValidityBuilder {
    bytes: Vec<u8>,
    len: usize,
    null_count: usize,
    capacity: usize,
}

impl PackedValidityBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.div_ceil(8)),
            len: 0,
            null_count: 0,
            capacity,
        }
    }

    #[inline]
    fn append(&mut self, valid: bool) {
        let shift = self.len % 8;
        if shift == 0 {
            self.bytes.push(0);
        }
        if valid {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << shift;
        } else {
            self.null_count += 1;
        }
        self.len += 1;
    }

    #[inline]
    fn append_packed(&mut self, bits: u8, count: usize) {
        debug_assert!(count <= 8);
        if count == 4 && self.len.is_multiple_of(4) {
            let bits = bits & 0x0f;
            if self.len.is_multiple_of(8) {
                self.bytes.push(bits);
            } else {
                let last = self.bytes.len() - 1;
                self.bytes[last] |= bits << 4;
            }
            self.len += 4;
            self.null_count += 4 - bits.count_ones() as usize;
            return;
        }
        let mask = if count == 8 {
            u8::MAX
        } else {
            (1_u8 << count) - 1
        };
        let bits = bits & mask;
        let byte_index = self.len / 8;
        let shift = self.len % 8;
        let new_len = self.len + count;
        self.bytes.resize(new_len.div_ceil(8), 0);
        self.bytes[byte_index] |= bits << shift;
        if shift + count > 8 {
            self.bytes[byte_index + 1] |= bits >> (8 - shift);
        }
        self.len = new_len;
        self.null_count += count - bits.count_ones() as usize;
    }

    fn finish(&mut self) -> Option<NullBuffer> {
        let bytes = std::mem::replace(
            &mut self.bytes,
            Vec::with_capacity(self.capacity.div_ceil(8)),
        );
        let len = std::mem::take(&mut self.len);
        let null_count = std::mem::take(&mut self.null_count);
        (null_count != 0).then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from(bytes), 0, len)))
    }
}

fn execute_gt_only(
    assignment: PgenPartition,
    fileset: Arc<PgenFileset>,
    schema: SchemaRef,
    metrics: Arc<GenotypeScanMetrics>,
    max_rows: usize,
    soft_bytes: usize,
) -> Result<SendableRecordBatchStream> {
    let stream_schema = schema.clone();
    let stream = try_stream! {
        let selected_samples = fileset.selected_samples.source_indices();
        let selected_sample_count = selected_samples.len();
        let estimated_row_bytes = estimate_genotype_bytes(selected_sample_count, &["GT".to_string()]);
        let mut sizer = GenotypeBatchSizer::new(max_rows, soft_bytes)?;
        let partition_row_capacity = initial_batch_row_capacity(
            max_rows,
            assignment.owned().len(),
            soft_bytes,
            estimated_row_bytes,
        );
        let mut batch =
            GtBatchBuilder::new(&schema, partition_row_capacity, selected_sample_count)?;
        let mut workspace = GtDecodeWorkspace::new(fileset.sample_count, selected_samples)?;

        let owned = assignment.owned();
        let mut retained_bases = HashSet::new();
        for index in assignment.required() {
            if let Some(base) = fileset.records.record(index)?.ld_base {
                retained_bases.insert(base);
            }
        }
        let mut ld_base_index = None;
        let mut ld_base = Vec::with_capacity(fileset.sample_count);
        let mut required = assignment.required().peekable();
        let genotype_projection = GenotypeProjection::gt_only();

        for range in assignment.ranges.iter().copied() {
            let bytes = fileset
                .source
                .read_range(&fileset.pgen_path, range.start..range.end)
                .await?;
            metrics.add(GenotypeMetric::RangeRequests, 1);
            metrics.add(GenotypeMetric::PrimaryBytesRead, bytes.len() as u64);

            while let Some(&variant_index) = required.peek() {
                let record = fileset.records.record(variant_index)?;
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
                let base = record
                    .ld_base
                    .map(|base_index| {
                        if ld_base_index != Some(base_index) {
                            return Err(DataFusionError::Execution(format!(
                                "PGEN variant {variant_index} dependency base {base_index} was not decoded first"
                            )));
                        }
                        Ok(ld_base.as_slice())
                    })
                    .transpose()?;

                if owned.binary_search(&variant_index).is_err() {
                    let main = decode_main_track(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        base,
                    )?;
                    if retained_bases.contains(&variant_index) {
                        ld_base = main;
                        ld_base_index = Some(variant_index);
                    }
                    metrics.add(GenotypeMetric::DependencyRecords, 1);
                    required.next();
                    continue;
                }

                if sizer.should_flush_before(estimated_row_bytes) {
                    let row_count = batch.len();
                    let finished = batch.finish(&fileset, schema.clone())?;
                    record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                    yield finished;
                    sizer.reset();
                }

                let allele_count = fileset.variants[variant_index].allele_count();
                let retain_main = retained_bases.contains(&variant_index);
                let direct_dense = workspace.has_identity_selection()
                    && !retain_main
                    && supports_biallelic_gt_fast_path(record.record_type, allele_count)
                    && (fileset.mode == PgenMode::Plink1 || record.record_type & 7 == 0);
                if direct_dense {
                    decode_dense_biallelic_gt(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        |alleles, validity, samples| {
                            batch.append_chunk(alleles, validity, samples)
                        },
                    )?;
                    batch.finish_variant(variant_index);
                } else if supports_biallelic_gt_fast_path(record.record_type, allele_count) {
                    decode_biallelic_gt_into(
                        &mut workspace,
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        selected_samples,
                        base,
                        retain_main,
                    )?;
                    batch.append_codes(variant_index, workspace.selected_codes())?;
                    if retained_bases.contains(&variant_index) {
                        workspace.swap_main_track(&mut ld_base);
                        ld_base_index = Some(variant_index);
                    }
                } else {
                    let (decoded, main) = decode_record_and_main(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        allele_count,
                        genotype_projection,
                        selected_samples,
                        base,
                    )?;
                    batch.append(variant_index, &decoded.gt);
                    if retained_bases.contains(&variant_index) {
                        ld_base = main;
                        ld_base_index = Some(variant_index);
                    }
                }
                metrics.add(GenotypeMetric::SamplesDecoded, selected_sample_count as u64);
                metrics.add(
                    GenotypeMetric::SampleValuesSkipped,
                    fileset.sample_count.saturating_sub(selected_sample_count) as u64,
                );
                sizer.push_row(estimated_row_bytes);
                required.next();
            }
        }

        if let Some(next) = required.next() {
            Err(DataFusionError::Execution(format!(
                "required PGEN record {next} was not covered by planned ranges"
            )))?;
        }
        if !batch.is_empty() {
            let row_count = batch.len();
            let finished = batch.finish(&fileset, schema.clone())?;
            record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
            yield finished;
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        stream_schema,
        stream,
    )))
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
    // Conservative soft-budget estimates include values, validity, list offsets,
    // and alignment overhead. They deliberately exceed the dense Arrow payload.
    fields.iter().fold(0_usize, |bytes, field| {
        bytes.saturating_add(match field.as_str() {
            "GT" => samples.saturating_mul(5),
            "PHASED" => samples.saturating_mul(2),
            "DS" => samples.saturating_mul(5),
            "DS_STORED" => samples.saturating_mul(5),
            "HDS" => samples.saturating_mul(9),
            _ => 0,
        })
    })
}

fn initial_batch_row_capacity(
    max_rows: usize,
    partition_rows: usize,
    soft_bytes: usize,
    estimated_row_bytes: usize,
) -> usize {
    let byte_limited_rows = if estimated_row_bytes == 0 {
        max_rows
    } else {
        (soft_bytes / estimated_row_bytes).max(1)
    };
    max_rows.min(partition_rows).min(byte_limited_rows).max(1)
}

fn record_batch_metrics(metrics: &GenotypeScanMetrics, rows: usize, genotype_bytes: usize) {
    metrics.add(GenotypeMetric::Batches, 1);
    metrics.add(GenotypeMetric::BatchRows, rows as u64);
    metrics.add(GenotypeMetric::EmittedVariants, rows as u64);
    metrics.add(GenotypeMetric::GenotypeBytes, genotype_bytes as u64);
}

fn build_gt_batch(
    fileset: &PgenFileset,
    schema: SchemaRef,
    variant_indices: &[usize],
    gt: ArrayRef,
) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "chrom" => Ok(Arc::new(StringArray::from_iter_values(
                variant_indices
                    .iter()
                    .map(|&index| fileset.variants[index].chrom.as_str()),
            )) as ArrayRef),
            "start" => Ok(Arc::new(UInt64Array::from_iter_values(
                variant_indices
                    .iter()
                    .map(|&index| fileset.variants[index].start),
            )) as ArrayRef),
            "end" => Ok(Arc::new(UInt64Array::from_iter_values(
                variant_indices
                    .iter()
                    .map(|&index| fileset.variants[index].end),
            )) as ArrayRef),
            "id" => Ok(Arc::new(StringArray::from(
                variant_indices
                    .iter()
                    .map(|&index| fileset.variants[index].id.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            "ref" => Ok(Arc::new(StringArray::from_iter_values(
                variant_indices
                    .iter()
                    .map(|&index| fileset.variants[index].reference.as_str()),
            )) as ArrayRef),
            "alt" => build_alt_index_array(fileset, field.data_type(), variant_indices),
            "genotypes" => {
                let DataType::Struct(fields) = field.data_type() else {
                    return Err(DataFusionError::Execution(
                        "PGEN genotypes field is not a struct".to_string(),
                    ));
                };
                Ok(Arc::new(StructArray::new(fields.clone(), vec![gt.clone()], None)) as ArrayRef)
            }
            name => Err(DataFusionError::Execution(format!(
                "unsupported projected PGEN column {name}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(variant_indices.len())),
    )
    .map_err(DataFusionError::from)
}

fn build_alt_index_array(
    fileset: &PgenFileset,
    data_type: &DataType,
    variant_indices: &[usize],
) -> Result<ArrayRef> {
    let DataType::List(allele_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN alt field is not a list".to_string(),
        ));
    };
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(allele_field.clone());
    for &variant_index in variant_indices {
        for allele in &fileset.variants[variant_index].alternate {
            builder.values().append_value(allele);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
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
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(rows.len(), None)));
    }
    let arrays = genotype_fields
        .iter()
        .zip(fields)
        .map(|(name, field)| match name.as_str() {
            "GT" => build_gt_array(field.data_type(), rows),
            "PHASED" => build_phased_array(field.data_type(), rows),
            "DS" => build_ds_array(field.data_type(), rows),
            "DS_STORED" => build_ds_stored_array(field.data_type(), rows),
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
    let DataType::FixedSizeList(allele_field, 2) = sample_field.data_type() else {
        return Err(DataFusionError::Execution(
            "PGEN GT sample field is not a fixed-size allele pair".to_string(),
        ));
    };
    let samples =
        FixedSizeListBuilder::new(UInt16Builder::new(), 2).with_field(allele_field.clone());
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
                builder.values().values().append_slice(&[0, 0]);
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
    build_float_sample_array(data_type, rows, "DS", |decoded| &decoded.ds)
}

fn build_ds_stored_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    build_float_sample_array(data_type, rows, "DS_STORED", |decoded| &decoded.ds_stored)
}

fn build_float_sample_array<'a>(
    data_type: &DataType,
    rows: &'a [DecodedRow],
    name: &str,
    values: impl Fn(&'a DecodedRecord) -> &'a [Option<f32>],
) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(format!(
            "PGEN {name} field is not a list"
        )));
    };
    let mut builder = ListBuilder::new(Float32Builder::new()).with_field(sample_field.clone());
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        for value in values(decoded) {
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
    let DataType::FixedSizeList(haplotype_field, 2) = sample_field.data_type() else {
        return Err(DataFusionError::Execution(
            "PGEN HDS sample field is not a fixed-size haplotype pair".to_string(),
        ));
    };
    let samples =
        FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(haplotype_field.clone());
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
                builder.values().values().append_slice(&[0.0, 0.0]);
                builder.values().append(false);
            }
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::{estimate_genotype_bytes, initial_batch_row_capacity};

    #[test]
    fn bounds_gt_builder_capacity_by_the_soft_byte_budget() {
        let estimated_row_bytes = estimate_genotype_bytes(100_000, &["GT".to_string()]);
        assert_eq!(estimated_row_bytes, 500_000);
        assert_eq!(
            initial_batch_row_capacity(8192, 8192, 64 * 1024 * 1024, estimated_row_bytes),
            134
        );
        assert_eq!(
            initial_batch_row_capacity(8192, 8192, 1, estimated_row_bytes),
            1
        );
        assert_eq!(
            initial_batch_row_capacity(8192, 3, 64 * 1024 * 1024, estimated_row_bytes),
            3
        );
    }
}
