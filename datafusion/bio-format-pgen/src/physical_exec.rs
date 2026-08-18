use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeListArray, FixedSizeListBuilder, Float32Array,
    Float32Builder, Int8Array, ListArray, ListBuilder, StringArray, StringBuilder, StructArray,
    UInt16Array, UInt16Builder, UInt64Array,
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
    DENSE_ALT_COUNT_QUADS, DENSE_ALT_COUNT_QUADS_PLINK1, DENSE_DOSAGE_QUADS,
    DENSE_DOSAGE_QUADS_PLINK1, DENSE_DOSAGE_VALIDITY, DENSE_DOSAGE_VALIDITY_PLINK1, DecodedRecord,
    GenotypeProjection, GtDecodeWorkspace, decode_biallelic_gt_into, decode_common_difflist_into,
    decode_dense_biallelic_gt, decode_main_track_and_validate, decode_record_and_main,
    supports_biallelic_gt_fast_path, supports_common_difflist_fast_path, validated_dense_hardcalls,
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

        // GT and DS both reduce to one value per sample per variant, so both
        // can be decoded straight into their Arrow buffers by the shared fast
        // path instead of going through the generic per-variant intermediates.
        if genotypes_projected
            && !assignment.ranges.is_empty()
            && matches!(genotype_fields.as_slice(), [field]
                if field == "GT" || field == "DS" || field == "ALT_COUNT")
        {
            return execute_single_field(
                genotype_fields[0].clone(),
                assignment,
                fileset,
                schema,
                metrics,
                max_rows,
                soft_bytes,
            );
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
                let mut range_reader = fileset.source.range_reader(&fileset.pgen_path).await?;
                for range in assignment.ranges.iter().copied() {
                    let bytes = range_reader.read_range(range.start..range.end).await?;
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
                            record_payload(range, bytes, record.offset, record.end(), variant_index)?;
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
                            let main = decode_main_track_and_validate(
                                payload,
                                fileset.mode,
                                record.record_type,
                                variant_index,
                                fileset.sample_count,
                                fileset.variants[variant_index].allele_count(),
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

/// Batch builder for a `DS`-only projection.
///
/// Mirrors `GtBatchBuilder`: the decoder writes dosages straight into the Arrow
/// values buffer and a packed validity bitmap, so a `DS` scan never allocates
/// the per-variant `Vec<Option<f32>>` the generic path builds. That vector is
/// eight bytes per genotype cell and one allocation per variant, which on a
/// whole chromosome is the dominant single-core cost.
struct DsBatchBuilder {
    variant_indices: Vec<usize>,
    dosages: Vec<f32>,
    sample_validity: PackedValidityBuilder,
    selected_sample_count: usize,
    row_capacity: usize,
    sample_field: FieldRef,
}

impl DsBatchBuilder {
    fn new(schema: &SchemaRef, row_capacity: usize, selected_sample_count: usize) -> Result<Self> {
        let genotype_field = schema.field_with_name("genotypes")?;
        let DataType::Struct(children) = genotype_field.data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN genotypes field is not a struct".to_string(),
            ));
        };
        if children.len() != 1 || children[0].name() != "DS" {
            return Err(DataFusionError::Execution(
                "PGEN DS fast path requires an exact DS-only genotype projection".to_string(),
            ));
        }
        let DataType::List(sample_field) = children[0].data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN DS field is not a list".to_string(),
            ));
        };
        let cells = row_capacity.saturating_mul(selected_sample_count);
        Ok(Self {
            variant_indices: Vec::with_capacity(row_capacity),
            dosages: Vec::with_capacity(cells),
            sample_validity: PackedValidityBuilder::new(cells),
            selected_sample_count,
            row_capacity,
            sample_field: sample_field.clone(),
        })
    }

    /// Appends decoded dosages. These must come from the record's dosage
    /// track when it has one — deriving them from hardcalls would silently
    /// replace a fractional dosage such as 0.125 with an allele count.
    fn append(&mut self, variant_index: usize, dosages: &[Option<f32>]) {
        for dosage in dosages {
            match dosage {
                Some(dosage) => {
                    self.dosages.push(*dosage);
                    self.sample_validity.append(true);
                }
                None => {
                    self.dosages.push(0.0);
                    self.sample_validity.append(false);
                }
            }
        }
        self.variant_indices.push(variant_index);
    }

    fn append_codes(&mut self, variant_index: usize, codes: &[u8]) -> Result<()> {
        // Codes 0..=4 map to a dosage through a table so the loop stays branch
        // free, and validity is accumulated a byte at a time rather than a bit
        // at a time. This is the path almost every record takes: plink2 writes
        // LD-compressed records, which are not eligible for the dense decode,
        // so this runs once per genotype cell across a whole scan.
        if let Some(code) = codes.iter().copied().find(|&code| code > 4) {
            return Err(DataFusionError::Execution(format!(
                "invalid internal biallelic GT code {code}"
            )));
        }

        // Written as a slice-to-slice loop over plain arithmetic rather than a
        // table lookup and `Vec::push`, so LLVM can vectorize it: the per-push
        // capacity check and the indexed table both block that. `alt_dosage`
        // is branchless, so this compiles to a NEON/SSE2 widening loop.
        let start = self.dosages.len();
        self.dosages.resize(start + codes.len(), 0.0);
        let output = &mut self.dosages[start..];
        for (slot, &code) in output.iter_mut().zip(codes) {
            *slot = f32::from(alt_count_from_code(code));
        }
        let present = !codes.contains(&3);

        if present {
            // A variant with no missing call is the common case in a dense
            // callset, and skips the bitmap bookkeeping entirely.
            self.sample_validity.append_all_valid(codes.len());
        } else {
            for chunk in codes.chunks(8) {
                let mut bits = 0_u8;
                for (index, &code) in chunk.iter().enumerate() {
                    if code != 3 {
                        bits |= 1 << index;
                    }
                }
                self.sample_validity.append_packed(bits, chunk.len());
            }
        }
        self.finish_variant(variant_index);
        Ok(())
    }

    /// Appends one whole variant from a common category plus its sparse patches.
    ///
    /// The fill is a single `resize`, so a sample is written once and never read
    /// back; `append_codes` on the same record writes a code per sample and then
    /// reads every one of them again. Patches touch only the samples that differ
    /// from the common category.
    fn append_common_difflist(&mut self, common: u8, patches: &[(usize, u8)], sample_count: usize) {
        let start = self.dosages.len();
        self.dosages
            .resize(start + sample_count, dosage_from_code(common));
        let output = &mut self.dosages[start..];
        for &(sample, value) in patches {
            output[sample] = dosage_from_code(value);
        }
        append_common_difflist_validity(&mut self.sample_validity, common, patches, sample_count);
    }

    /// Appends one whole variant straight from its packed two-bit hardcalls.
    ///
    /// Walking the bytes here rather than through a per-quad callback keeps the
    /// inner loop free of indirect calls so it can be unrolled, and indexes a
    /// 4 KiB dosage table instead of the ~80 KiB allele table the GT path uses.
    fn append_dense(&mut self, packed: &[u8], sample_count: usize, mode: PgenMode) {
        let (quads, validity_of) = if mode == PgenMode::Plink1 {
            (&DENSE_DOSAGE_QUADS_PLINK1, &DENSE_DOSAGE_VALIDITY_PLINK1)
        } else {
            (&DENSE_DOSAGE_QUADS, &DENSE_DOSAGE_VALIDITY)
        };
        let full_bytes = sample_count / 4;
        self.dosages.reserve(sample_count);

        // A variant with no missing call is the common case in a dense
        // callset, and lets the whole validity run be appended at once instead
        // of a nibble at a time.
        let mut all_present = true;
        for &byte in &packed[..full_bytes] {
            self.dosages.extend_from_slice(&quads[usize::from(byte)]);
            all_present &= validity_of[usize::from(byte)] == 0x0f;
        }
        let remainder = sample_count % 4;
        if remainder != 0 {
            let byte = packed[full_bytes];
            self.dosages
                .extend_from_slice(&quads[usize::from(byte)][..remainder]);
        }

        if all_present && remainder == 0 {
            self.sample_validity.append_all_valid(sample_count);
        } else {
            for &byte in &packed[..full_bytes] {
                self.sample_validity
                    .append_packed(validity_of[usize::from(byte)], 4);
            }
            if remainder != 0 {
                let byte = packed[full_bytes];
                self.sample_validity
                    .append_packed(validity_of[usize::from(byte)], remainder);
            }
        }
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
        let cells = self.row_capacity.saturating_mul(self.selected_sample_count);
        let validity = self.sample_validity.finish();
        let dosages = std::mem::replace(&mut self.dosages, Vec::with_capacity(cells));
        self.sample_validity = PackedValidityBuilder::new(cells);
        let values = Arc::new(Float32Array::new(dosages.into(), validity)) as ArrayRef;
        let ds = Arc::new(ListArray::new(
            self.sample_field.clone(),
            OffsetBuffer::from_repeated_length(self.selected_sample_count, self.len()),
            values,
            None,
        )) as ArrayRef;
        let batch = build_gt_batch(fileset, schema, &self.variant_indices, ds)?;
        self.variant_indices.clear();
        Ok(batch)
    }
}

/// Batch builder for an `ALT_COUNT`-only projection.
///
/// Emits the hardcall ALT allele count as `Int8`, one byte per genotype cell
/// rather than the four `DS` needs. On a whole chromosome that is 2.53 GB of
/// output instead of 10.13 GB, and output width is the single largest term in
/// this scan's cost: PLINK 2's own reader takes 0.827 s to produce the `int8`
/// matrix and 1.853 s for the `float32` one from the same records.
struct AltCountBatchBuilder {
    variant_indices: Vec<usize>,
    counts: Vec<i8>,
    sample_validity: PackedValidityBuilder,
    selected_sample_count: usize,
    row_capacity: usize,
    sample_field: FieldRef,
}

impl AltCountBatchBuilder {
    fn new(schema: &SchemaRef, row_capacity: usize, selected_sample_count: usize) -> Result<Self> {
        let genotype_field = schema.field_with_name("genotypes")?;
        let DataType::Struct(children) = genotype_field.data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN genotypes field is not a struct".to_string(),
            ));
        };
        if children.len() != 1 || children[0].name() != "ALT_COUNT" {
            return Err(DataFusionError::Execution(
                "PGEN ALT_COUNT fast path requires an exact ALT_COUNT-only projection".to_string(),
            ));
        }
        let DataType::List(sample_field) = children[0].data_type() else {
            return Err(DataFusionError::Execution(
                "PGEN ALT_COUNT field is not a list".to_string(),
            ));
        };
        let cells = row_capacity.saturating_mul(selected_sample_count);
        Ok(Self {
            variant_indices: Vec::with_capacity(row_capacity),
            counts: Vec::with_capacity(cells),
            sample_validity: PackedValidityBuilder::new(cells),
            selected_sample_count,
            row_capacity,
            sample_field: sample_field.clone(),
        })
    }

    fn append(&mut self, variant_index: usize, calls: &[Option<[u16; 2]>]) {
        for call in calls {
            match call {
                Some(call) => {
                    let count = u8::from(call[0] == 1) + u8::from(call[1] == 1);
                    self.counts.push(count as i8);
                    self.sample_validity.append(true);
                }
                None => {
                    self.counts.push(0);
                    self.sample_validity.append(false);
                }
            }
        }
        self.variant_indices.push(variant_index);
    }

    fn append_codes(&mut self, variant_index: usize, codes: &[u8]) -> Result<()> {
        if let Some(code) = codes.iter().copied().find(|&code| code > 4) {
            return Err(DataFusionError::Execution(format!(
                "invalid internal biallelic GT code {code}"
            )));
        }
        // Same shape as the DS path: a slice-to-slice loop over branchless
        // arithmetic, which vectorizes. int8 output means one lane per byte,
        // so this is the widest of the expansion loops.
        let start = self.counts.len();
        self.counts.resize(start + codes.len(), 0);
        let output = &mut self.counts[start..];
        for (slot, &code) in output.iter_mut().zip(codes) {
            *slot = alt_count_from_code(code);
        }
        let present = !codes.contains(&3);
        if present {
            self.sample_validity.append_all_valid(codes.len());
        } else {
            for chunk in codes.chunks(8) {
                let mut bits = 0_u8;
                for (index, &code) in chunk.iter().enumerate() {
                    if code != 3 {
                        bits |= 1 << index;
                    }
                }
                self.sample_validity.append_packed(bits, chunk.len());
            }
        }
        self.finish_variant(variant_index);
        Ok(())
    }

    /// The `ALT_COUNT` counterpart of `DsBatchBuilder::append_common_difflist`.
    fn append_common_difflist(&mut self, common: u8, patches: &[(usize, u8)], sample_count: usize) {
        let start = self.counts.len();
        self.counts
            .resize(start + sample_count, alt_count_from_code(common));
        let output = &mut self.counts[start..];
        for &(sample, value) in patches {
            output[sample] = alt_count_from_code(value);
        }
        append_common_difflist_validity(&mut self.sample_validity, common, patches, sample_count);
    }

    fn append_dense(&mut self, packed: &[u8], sample_count: usize, mode: PgenMode) {
        let (quads, validity_of) = if mode == PgenMode::Plink1 {
            (&DENSE_ALT_COUNT_QUADS_PLINK1, &DENSE_DOSAGE_VALIDITY_PLINK1)
        } else {
            (&DENSE_ALT_COUNT_QUADS, &DENSE_DOSAGE_VALIDITY)
        };
        let full_bytes = sample_count / 4;
        self.counts.reserve(sample_count);
        let mut all_present = true;
        for &byte in &packed[..full_bytes] {
            self.counts.extend_from_slice(&quads[usize::from(byte)]);
            all_present &= validity_of[usize::from(byte)] == 0x0f;
        }
        let remainder = sample_count % 4;
        if remainder != 0 {
            let byte = packed[full_bytes];
            self.counts
                .extend_from_slice(&quads[usize::from(byte)][..remainder]);
        }
        if all_present && remainder == 0 {
            self.sample_validity.append_all_valid(sample_count);
        } else {
            for &byte in &packed[..full_bytes] {
                self.sample_validity
                    .append_packed(validity_of[usize::from(byte)], 4);
            }
            if remainder != 0 {
                let byte = packed[full_bytes];
                self.sample_validity
                    .append_packed(validity_of[usize::from(byte)], remainder);
            }
        }
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
        let cells = self.row_capacity.saturating_mul(self.selected_sample_count);
        let validity = self.sample_validity.finish();
        let counts = std::mem::replace(&mut self.counts, Vec::with_capacity(cells));
        self.sample_validity = PackedValidityBuilder::new(cells);
        let values = Arc::new(Int8Array::new(counts.into(), validity)) as ArrayRef;
        let alt_count = Arc::new(ListArray::new(
            self.sample_field.clone(),
            OffsetBuffer::from_repeated_length(self.selected_sample_count, self.len()),
            values,
            None,
        )) as ArrayRef;
        let batch = build_gt_batch(fileset, schema, &self.variant_indices, alt_count)?;
        self.variant_indices.clear();
        Ok(batch)
    }
}

/// The single-field fast path builds either `GT` or `DS`; the surrounding scan
/// loop is identical, so it is shared rather than duplicated.
enum FastFieldBuilder {
    Gt(GtBatchBuilder),
    Ds(DsBatchBuilder),
    AltCount(AltCountBatchBuilder),
}

impl FastFieldBuilder {
    fn new(
        field: &str,
        schema: &SchemaRef,
        row_capacity: usize,
        selected_sample_count: usize,
    ) -> Result<Self> {
        match field {
            "GT" => Ok(Self::Gt(GtBatchBuilder::new(
                schema,
                row_capacity,
                selected_sample_count,
            )?)),
            "DS" => Ok(Self::Ds(DsBatchBuilder::new(
                schema,
                row_capacity,
                selected_sample_count,
            )?)),
            "ALT_COUNT" => Ok(Self::AltCount(AltCountBatchBuilder::new(
                schema,
                row_capacity,
                selected_sample_count,
            )?)),
            other => Err(DataFusionError::Execution(format!(
                "PGEN fast path does not support genotype field {other}"
            ))),
        }
    }

    fn append(&mut self, variant_index: usize, decoded: &DecodedRecord) {
        match self {
            Self::Gt(builder) => builder.append(variant_index, &decoded.gt),
            Self::Ds(builder) => builder.append(variant_index, &decoded.ds),
            Self::AltCount(builder) => builder.append(variant_index, &decoded.gt),
        }
    }

    /// Whether hardcall phase orientation affects this field's output.
    ///
    /// `GT` distinguishes `(0,1)` from `(1,0)`; `DS` sums them, so a dosage
    /// scan can validate the phase track without applying it.
    fn needs_phase(&self) -> bool {
        matches!(self, Self::Gt(_))
    }

    /// The projection the generic fallback must decode for this field.
    fn projection(&self) -> GenotypeProjection {
        match self {
            Self::Gt(_) => GenotypeProjection::gt_only(),
            Self::Ds(_) => GenotypeProjection::from_fields(&["DS".to_string()]),
            Self::AltCount(_) => GenotypeProjection::alt_count_only(),
        }
    }

    fn append_codes(&mut self, variant_index: usize, codes: &[u8]) -> Result<()> {
        match self {
            Self::Gt(builder) => builder.append_codes(variant_index, codes),
            Self::Ds(builder) => builder.append_codes(variant_index, codes),
            Self::AltCount(builder) => builder.append_codes(variant_index, codes),
        }
    }

    /// Appends one common-value + difflist record to this builder.
    ///
    /// Gated by `needs_phase`: the fused decode validates the record's phase
    /// track but discards its orientation, which `GT` needs.
    fn append_common_difflist(
        &mut self,
        common: u8,
        patches: &[(usize, u8)],
        sample_count: usize,
    ) -> Result<()> {
        match self {
            Self::Ds(builder) => {
                builder.append_common_difflist(common, patches, sample_count);
                Ok(())
            }
            Self::AltCount(builder) => {
                builder.append_common_difflist(common, patches, sample_count);
                Ok(())
            }
            Self::Gt(_) => Err(DataFusionError::Execution(
                "PGEN GT projection cannot use the fused common-value decode".to_string(),
            )),
        }
    }

    /// Decodes one dense biallelic record directly into this builder.
    ///
    /// `GT` needs the phase pattern, so it goes through the quad callback.
    /// `DS` does not, so it validates the record once and then walks the
    /// two-bit codes inline against a compact dosage table.
    fn append_dense_record(
        &mut self,
        payload: &[u8],
        mode: PgenMode,
        record_type: u8,
        variant_index: usize,
        sample_count: usize,
    ) -> Result<()> {
        match self {
            Self::Gt(builder) => decode_dense_biallelic_gt(
                payload,
                mode,
                record_type,
                variant_index,
                sample_count,
                |alleles, validity, samples| builder.append_chunk(alleles, validity, samples),
            ),
            Self::AltCount(builder) => {
                let packed = validated_dense_hardcalls(
                    payload,
                    mode,
                    record_type,
                    variant_index,
                    sample_count,
                )?;
                builder.append_dense(packed, sample_count, mode);
                Ok(())
            }
            Self::Ds(builder) => {
                let packed = validated_dense_hardcalls(
                    payload,
                    mode,
                    record_type,
                    variant_index,
                    sample_count,
                )?;
                builder.append_dense(packed, sample_count, mode);
                Ok(())
            }
        }
    }

    fn finish_variant(&mut self, variant_index: usize) {
        match self {
            Self::Gt(builder) => builder.finish_variant(variant_index),
            Self::Ds(builder) => builder.finish_variant(variant_index),
            Self::AltCount(builder) => builder.finish_variant(variant_index),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Gt(builder) => builder.len(),
            Self::Ds(builder) => builder.len(),
            Self::AltCount(builder) => builder.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Gt(builder) => builder.is_empty(),
            Self::Ds(builder) => builder.is_empty(),
            Self::AltCount(builder) => builder.is_empty(),
        }
    }

    fn finish(&mut self, fileset: &PgenFileset, schema: SchemaRef) -> Result<RecordBatch> {
        match self {
            Self::Gt(builder) => builder.finish(fileset, schema),
            Self::Ds(builder) => builder.finish(fileset, schema),
            Self::AltCount(builder) => builder.finish(fileset, schema),
        }
    }
}

/// The internal biallelic GT code for a missing call.
const MISSING_CODE: u8 = 3;

/// ALT allele count for an internal biallelic GT code.
///
/// Codes are 0=`(0,0)`, 1=`(0,1)`, 2=`(1,1)`, 3=missing, 4=`(1,0)`, so the
/// count is 0, 1, 2, 0, 1. Expressed as arithmetic rather than a lookup table
/// because a table index blocks vectorization of the surrounding loop, and
/// this runs once per genotype cell.
#[inline]
pub(crate) fn alt_count_from_code(code: u8) -> i8 {
    (code - 3 * u8::from(code >= 3)) as i8
}

/// ALT dosage for an internal biallelic GT code.
///
/// A missing call yields `0.0`; the validity bitmap is what records it, and the
/// value is never read back.
#[inline]
fn dosage_from_code(code: u8) -> f32 {
    f32::from(alt_count_from_code(code))
}

/// Validity for a common category plus its sparse patches.
///
/// Appends one run for the whole variant and then touches only the samples whose
/// validity differs from the common category's, so the bitmap work is
/// proportional to the difflist rather than to the sample count.
fn append_common_difflist_validity(
    validity: &mut PackedValidityBuilder,
    common: u8,
    patches: &[(usize, u8)],
    sample_count: usize,
) {
    let start = validity.len;
    if common == MISSING_CODE {
        validity.append_all_invalid(sample_count);
        for &(sample, value) in patches {
            if value != MISSING_CODE {
                validity.set_valid(start + sample);
            }
        }
    } else {
        validity.append_all_valid(sample_count);
        for &(sample, value) in patches {
            if value == MISSING_CODE {
                validity.set_invalid(start + sample);
            }
        }
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

    /// Appends `count` valid entries at once.
    ///
    /// Used when a whole variant has no missing call, which avoids touching
    /// the bitmap once per sample for the common dense case.
    fn append_all_valid(&mut self, count: usize) {
        let shift = self.len % 8;
        let mut remaining = count;
        if shift != 0 {
            let head = (8 - shift).min(remaining);
            let mask = if head == 8 {
                u8::MAX
            } else {
                ((1_u8 << head) - 1) << shift
            };
            let last = self.bytes.len() - 1;
            self.bytes[last] |= mask;
            self.len += head;
            remaining -= head;
        }
        let whole = remaining / 8;
        self.bytes.resize(self.bytes.len() + whole, u8::MAX);
        self.len += whole * 8;
        remaining -= whole * 8;
        if remaining != 0 {
            self.bytes.push((1_u8 << remaining) - 1);
            self.len += remaining;
        }
    }

    /// Appends `count` null entries at once.
    ///
    /// The mirror of `append_all_valid`, for a variant whose common category is
    /// the missing one.
    fn append_all_invalid(&mut self, count: usize) {
        let len = self.len + count;
        self.bytes.resize(len.div_ceil(8), 0);
        self.len = len;
        self.null_count += count;
    }

    /// Marks an already-appended entry valid.
    #[inline]
    fn set_valid(&mut self, index: usize) {
        debug_assert!(index < self.len);
        let mask = 1_u8 << (index % 8);
        let byte = &mut self.bytes[index / 8];
        if *byte & mask == 0 {
            *byte |= mask;
            self.null_count -= 1;
        }
    }

    /// Marks an already-appended entry null.
    #[inline]
    fn set_invalid(&mut self, index: usize) {
        debug_assert!(index < self.len);
        let mask = 1_u8 << (index % 8);
        let byte = &mut self.bytes[index / 8];
        if *byte & mask != 0 {
            *byte &= !mask;
            self.null_count += 1;
        }
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

fn execute_single_field(
    field: String,
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
            FastFieldBuilder::new(&field, &schema, partition_row_capacity, selected_sample_count)?;
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
        let genotype_projection = batch.projection();
        let mut range_reader = fileset.source.range_reader(&fileset.pgen_path).await?;

        for range in assignment.ranges.iter().copied() {
            let bytes = range_reader.read_range(range.start..range.end).await?;
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
                    record_payload(range, bytes, record.offset, record.end(), variant_index)?;
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
                    let main = decode_main_track_and_validate(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                        fileset.variants[variant_index].allele_count(),
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
                // Most of a plink2 fileset is a common category plus a sparse
                // difflist, which a field needing no phase orientation can build
                // in one pass. A record that a later LD record uses as its base
                // is excluded: that needs the full main track anyway.
                let fused_common_difflist = !batch.needs_phase()
                    && workspace.has_identity_selection()
                    && !retain_main
                    && supports_biallelic_gt_fast_path(record.record_type, allele_count)
                    && supports_common_difflist_fast_path(fileset.mode, record.record_type);
                if direct_dense {
                    batch.append_dense_record(
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                    )?;
                    batch.finish_variant(variant_index);
                } else if fused_common_difflist {
                    let common = decode_common_difflist_into(
                        &mut workspace,
                        payload,
                        fileset.mode,
                        record.record_type,
                        variant_index,
                        fileset.sample_count,
                    )?;
                    batch.append_common_difflist(
                        common,
                        workspace.patches(),
                        fileset.sample_count,
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
                        batch.needs_phase(),
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
                    batch.append(variant_index, &decoded);
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

pub(crate) fn record_payload(
    range: ByteRange,
    bytes: &[u8],
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
            "ALT_COUNT" => build_alt_count_array(field.data_type(), rows),
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

/// Builds the `ALT_COUNT` child: the hardcall ALT allele count as `Int8`.
///
/// One byte per genotype cell rather than the four `DS` uses. Derived from the
/// hardcall calls, not the dosage track, so a fractional stored dosage is never
/// silently rounded into a count.
fn build_alt_count_array(data_type: &DataType, rows: &[DecodedRow]) -> Result<ArrayRef> {
    let DataType::List(sample_field) = data_type else {
        return Err(DataFusionError::Execution(
            "PGEN ALT_COUNT field is not a list".to_string(),
        ));
    };
    let sample_count = match rows.first() {
        Some(row) => {
            let decoded = row.genotypes.as_ref().ok_or_else(|| {
                DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
            })?;
            decoded.gt.len()
        }
        None => 0,
    };
    let total = rows.len().saturating_mul(sample_count);
    let mut counts: Vec<i8> = Vec::with_capacity(total);
    let mut validity = PackedValidityBuilder::new(total);
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        if decoded.gt.len() != sample_count {
            return Err(DataFusionError::Execution(format!(
                "PGEN ALT_COUNT row has {} samples; expected {sample_count}",
                decoded.gt.len()
            )));
        }
        for call in &decoded.gt {
            match call {
                Some(call) => {
                    counts.push((u8::from(call[0] == 1) + u8::from(call[1] == 1)) as i8);
                    validity.append(true);
                }
                None => {
                    counts.push(0);
                    validity.append(false);
                }
            }
        }
    }
    let nulls = validity.finish();
    let values = Arc::new(Int8Array::new(counts.into(), nulls)) as ArrayRef;
    Ok(Arc::new(ListArray::new(
        sample_field.clone(),
        OffsetBuffer::from_repeated_length(sample_count, rows.len()),
        values,
        None,
    )))
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
    // Every emitted row carries the same selected-sample count, so the values
    // buffer size is known up front and the offsets are a repeated length.
    // Building the values and validity buffers directly avoids a per-cell
    // `append_option`, whose capacity check and bitmap bookkeeping dominated
    // this function: at whole-chromosome scale it runs billions of times.
    let sample_count = match rows.first() {
        Some(row) => {
            let decoded = row.genotypes.as_ref().ok_or_else(|| {
                DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
            })?;
            values(decoded).len()
        }
        None => 0,
    };
    let total = rows.len().saturating_mul(sample_count);
    let mut sample_values: Vec<f32> = Vec::with_capacity(total);
    let mut sample_validity = PackedValidityBuilder::new(total);
    for row in rows {
        let decoded = row.genotypes.as_ref().ok_or_else(|| {
            DataFusionError::Execution("PGEN genotype row was not decoded".to_string())
        })?;
        let row_values = values(decoded);
        if row_values.len() != sample_count {
            return Err(DataFusionError::Execution(format!(
                "PGEN {name} row has {} samples; expected {sample_count}",
                row_values.len()
            )));
        }
        for value in row_values {
            match value {
                Some(value) => {
                    sample_values.push(*value);
                    sample_validity.append(true);
                }
                None => {
                    sample_values.push(0.0);
                    sample_validity.append(false);
                }
            }
        }
    }
    let nulls = sample_validity.finish();
    let values_array = Arc::new(Float32Array::new(sample_values.into(), nulls)) as ArrayRef;
    Ok(Arc::new(ListArray::new(
        sample_field.clone(),
        OffsetBuffer::from_repeated_length(sample_count, rows.len()),
        values_array,
        None,
    )))
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
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::{
        AltCountBatchBuilder, DsBatchBuilder, PackedValidityBuilder, estimate_genotype_bytes,
        initial_batch_row_capacity,
    };

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

    fn single_field_schema(field: &str, value_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "genotypes",
            DataType::Struct(
                vec![Field::new(
                    field,
                    DataType::List(Arc::new(Field::new("sample", value_type, true))),
                    false,
                )]
                .into(),
            ),
            false,
        )]))
    }

    /// The per-sample codes a common category and its patches stand for.
    fn expand(common: u8, patches: &[(usize, u8)], sample_count: usize) -> Vec<u8> {
        let mut codes = vec![common; sample_count];
        for &(sample, value) in patches {
            codes[sample] = value;
        }
        codes
    }

    struct Case {
        common: u8,
        patches: Vec<(usize, u8)>,
        samples: usize,
    }

    /// Cases chosen to exercise every way the fused fill and the two-pass
    /// expansion could disagree: each common category, patches to and from the
    /// missing category, an empty difflist, and sample counts that do and do not
    /// end on a validity-byte boundary.
    fn common_difflist_cases() -> Vec<Case> {
        [
            (0, vec![], 16),
            (0, vec![(0, 2), (5, 1), (15, 2)], 16),
            (0, vec![(3, 3)], 16),
            (0, vec![(2, 1), (9, 3), (10, 3)], 13),
            (2, vec![], 13),
            (2, vec![(0, 3), (12, 0)], 13),
            (3, vec![], 16),
            (3, vec![(1, 1), (4, 2), (11, 0)], 13),
            (3, vec![(0, 3)], 5),
            (0, vec![(0, 1)], 1),
        ]
        .into_iter()
        .map(|(common, patches, samples)| Case {
            common,
            patches,
            samples,
        })
        .collect()
    }

    /// Arrow reads the bitmap, its length and its null count, so all three have
    /// to match — a bitmap that agrees while the null count drifts still yields a
    /// wrong array.
    fn assert_same_validity(
        fused: &PackedValidityBuilder,
        expanded: &PackedValidityBuilder,
        case: &str,
    ) {
        assert_eq!(fused.bytes, expanded.bytes, "{case}");
        assert_eq!(fused.len, expanded.len, "{case}");
        assert_eq!(fused.null_count, expanded.null_count, "{case}");
    }

    #[test]
    fn fused_dosage_fill_matches_the_two_pass_expansion() {
        let schema = single_field_schema("DS", DataType::Float32);
        for case in common_difflist_cases() {
            let Case {
                common,
                patches,
                samples,
            } = case;
            let mut fused = DsBatchBuilder::new(&schema, 4, samples).unwrap();
            let mut expanded = DsBatchBuilder::new(&schema, 4, samples).unwrap();
            fused.append_common_difflist(common, &patches, samples);
            fused.finish_variant(0);
            expanded
                .append_codes(0, &expand(common, &patches, samples))
                .unwrap();

            let case = format!("common {common}, patches {patches:?}, {samples} samples");
            assert_eq!(fused.dosages, expanded.dosages, "{case}");
            assert_same_validity(&fused.sample_validity, &expanded.sample_validity, &case);
        }
    }

    #[test]
    fn fused_allele_count_fill_matches_the_two_pass_expansion() {
        let schema = single_field_schema("ALT_COUNT", DataType::Int8);
        for case in common_difflist_cases() {
            let Case {
                common,
                patches,
                samples,
            } = case;
            let mut fused = AltCountBatchBuilder::new(&schema, 4, samples).unwrap();
            let mut expanded = AltCountBatchBuilder::new(&schema, 4, samples).unwrap();
            fused.append_common_difflist(common, &patches, samples);
            fused.finish_variant(0);
            expanded
                .append_codes(0, &expand(common, &patches, samples))
                .unwrap();

            let case = format!("common {common}, patches {patches:?}, {samples} samples");
            assert_eq!(fused.counts, expanded.counts, "{case}");
            assert_same_validity(&fused.sample_validity, &expanded.sample_validity, &case);
        }
    }

    /// Variants are appended back to back, so a fused fill has to land on
    /// whatever bit offset the previous ones left the validity bitmap at.
    #[test]
    fn fused_fill_matches_the_two_pass_expansion_across_consecutive_variants() {
        let samples = 13;
        let schema = single_field_schema("DS", DataType::Float32);
        let mut fused = DsBatchBuilder::new(&schema, 16, samples).unwrap();
        let mut expanded = DsBatchBuilder::new(&schema, 16, samples).unwrap();
        for (variant, case) in common_difflist_cases().into_iter().enumerate() {
            let patches = case
                .patches
                .into_iter()
                .filter(|&(sample, _)| sample < samples)
                .collect::<Vec<_>>();
            fused.append_common_difflist(case.common, &patches, samples);
            fused.finish_variant(variant);
            expanded
                .append_codes(variant, &expand(case.common, &patches, samples))
                .unwrap();
        }
        assert_eq!(fused.dosages, expanded.dosages);
        assert_same_validity(
            &fused.sample_validity,
            &expanded.sample_validity,
            "consecutive variants",
        );
    }
}
