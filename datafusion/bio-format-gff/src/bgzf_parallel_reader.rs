use crate::filter_utils::{can_push_down_filter, evaluate_filters_against_record};
use crate::storage::GffRecordTrait;
use std::any::Any;
use std::io::{self, BufRead, Read, Seek};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use datafusion_bio_format_core::table_utils::{Attribute, OptionalField, builders_to_arrays};
use noodles_bgzf::{IndexedReader, gzi};
use noodles_gff as gff;
use std::path::PathBuf;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct BgzfGffTableProvider {
    path: PathBuf,
    schema: SchemaRef,
    attr_fields: Option<Vec<String>>,
}

impl BgzfGffTableProvider {
    pub fn try_new(path: impl Into<PathBuf>, attr_fields: Option<Vec<String>>) -> io::Result<Self> {
        let schema = determine_schema_on_demand(attr_fields.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Schema error: {}", e)))?;

        Ok(Self {
            path: path.into(),
            schema,
            attr_fields,
        })
    }
}

/// Determine schema based on attribute fields (same logic as main table provider)
fn determine_schema_on_demand(attr_fields: Option<Vec<String>>) -> Result<SchemaRef> {
    // Always include 8 static GFF fields
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::UInt32, true),
    ];

    match attr_fields {
        None => {
            // Mode 1: Default - return nested attributes for maximum flexibility
            fields.push(Field::new(
                "attributes",
                DataType::List(FieldRef::from(Box::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false), // tag must be non-null
                        Field::new("value", DataType::Utf8, true), // value may be null
                    ])),
                    true,
                )))),
                true,
            ));
            debug!(
                "BGZF GFF Schema Mode 1 (Default): 8 static fields + nested attributes = 9 columns total"
            );
        }
        Some(attrs) => {
            // Mode 2: Projection - add requested attributes as individual columns
            for attr_name in &attrs {
                fields.push(Field::new(
                    attr_name,
                    DataType::Utf8, // All GFF attributes are strings
                    true,           // Attributes can be null
                ));
            }
            debug!(
                "BGZF GFF Schema Mode 2 (Projection): 8 static fields + {} attribute columns = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Get partition bounds for parallel BGZF processing
fn get_bgzf_partition_bounds(index: &gzi::Index, thread_num: usize) -> Vec<(u64, u64)> {
    let mut block_offsets: Vec<(u64, u64)> = index.as_ref().iter().map(|(c, u)| (*c, *u)).collect();
    block_offsets.insert(0, (0, 0));

    let num_blocks = block_offsets.len();
    let num_partitions = thread_num.min(num_blocks);

    if num_partitions == 0 {
        return vec![(0, u64::MAX)];
    }

    let mut ranges = Vec::with_capacity(num_partitions);
    let mut current_block_idx = 0;

    for i in 0..num_partitions {
        if current_block_idx >= num_blocks {
            break;
        }

        let (_, start_uncomp) = block_offsets[current_block_idx];

        let remainder = num_blocks % num_partitions;
        let blocks_in_partition = num_blocks / num_partitions + if i < remainder { 1 } else { 0 };
        let next_partition_start_block_idx = current_block_idx + blocks_in_partition;

        let end_comp = if next_partition_start_block_idx >= num_blocks {
            u64::MAX
        } else {
            block_offsets[next_partition_start_block_idx].0
        };

        ranges.push((start_uncomp, end_comp));
        current_block_idx = next_partition_start_block_idx;
    }
    ranges
}

#[async_trait]
impl datafusion::catalog::TableProvider for BgzfGffTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        debug!(
            "BgzfGffTableProvider::supports_filters_pushdown with {} filters",
            filters.len()
        );

        let pushdown_support = filters
            .iter()
            .map(|expr| {
                if can_push_down_filter(expr, &self.schema) {
                    debug!("BGZF Filter can be pushed down: {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    debug!("BGZF Filter cannot be pushed down: {:?}", expr);
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(pushdown_support)
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "BgzfGffTableProvider::scan with projection: {:?}, filters: {:?}",
            projection, filters
        );
        let mut index_path = self.path.as_os_str().to_owned();
        index_path.push(".gzi");

        let index = gzi::fs::read(index_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to read GZI index: {}", e)))?;

        let partitions = get_bgzf_partition_bounds(&index, state.config().target_partitions());
        debug!("BGZF GFF Partitions: {:?}", partitions);

        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        // Filter the provided filters to only include those that can be pushed down
        let pushable_filters: Vec<Expr> = filters
            .iter()
            .filter(|expr| can_push_down_filter(expr, &self.schema))
            .cloned()
            .collect();

        debug!(
            "BgzfGffTableProvider::scan - {} filters can be pushed down out of {} total",
            pushable_filters.len(),
            filters.len()
        );

        let exec = BgzfGffExec::new(
            self.path.clone(),
            partitions,
            projected_schema,
            projection.cloned(),
            pushable_filters,
            index,
            self.attr_fields.clone(),
            limit,
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct BgzfGffExec {
    path: PathBuf,
    partitions: Vec<(u64, u64)>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    index: gzi::Index,
    attr_fields: Option<Vec<String>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl BgzfGffExec {
    fn new(
        path: PathBuf,
        partitions: Vec<(u64, u64)>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        index: gzi::Index,
        attr_fields: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            path,
            partitions,
            schema,
            projection,
            filters,
            index,
            attr_fields,
            limit,
            properties,
        }
    }
}

impl DisplayAs for BgzfGffExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BgzfGffExec: path={:?}, partitions={}",
                    self.path,
                    self.partitions.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "BgzfGffExec")
            }
        }
    }
}

impl ExecutionPlan for BgzfGffExec {
    fn name(&self) -> &'static str {
        "BgzfGffExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (start_uncomp, end_comp) = self.partitions[partition];
        let path = self.path.clone();
        let schema_ref = self.schema.clone();
        let schema_for_adapter = schema_ref.clone();
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let index = self.index.clone();
        let attr_fields = self.attr_fields.clone();
        let limit = self.limit;

        debug!(
            "Executing BGZF GFF partition {} with range: {}-{}",
            partition, start_uncomp, end_comp
        );

        // Lightweight record used only for filter evaluation
        struct BgzfLineRecord {
            chrom: String,
            start: u32,
            end: u32,
            ty: String,
            source: String,
            score: Option<f32>,
            strand: String,
            phase: Option<u8>,
            attributes: String,
        }

        impl GffRecordTrait for BgzfLineRecord {
            fn reference_sequence_name(&self) -> String {
                self.chrom.clone()
            }
            fn start(&self) -> u32 {
                self.start
            }
            fn end(&self) -> u32 {
                self.end
            }
            fn ty(&self) -> String {
                self.ty.clone()
            }
            fn source(&self) -> String {
                self.source.clone()
            }
            fn score(&self) -> Option<f32> {
                self.score
            }
            fn strand(&self) -> String {
                self.strand.clone()
            }
            fn phase(&self) -> Option<u8> {
                self.phase
            }
            fn attributes_string(&self) -> String {
                self.attributes.clone()
            }
        }

        let stream = async_stream::try_stream! {
            let reader = std::fs::File::open(&path)
                .map_err(|e| ArrowError::IoError(format!("Failed to open file: {}", e), e))?;

            let mut indexed_reader = IndexedReader::new(reader, index.clone());

            // For Fast parser with BGZF, we need to be more careful about seeking
            // For now, let's test without seeking to debug the parser
            if start_uncomp > 0 {
                let virtual_pos = noodles_bgzf::VirtualPosition::from(start_uncomp);
                indexed_reader.seek(std::io::SeekFrom::Start(virtual_pos.compressed()))
                    .map_err(|e| ArrowError::IoError(format!("Failed to seek: {}", e), e))?;
            }

            let batch_size = 8192;
            let mut record_count = 0;
            let mut total_records = 0;

            // Initialize builders based on schema mode
            let unnest_enable = attr_fields.is_some();
            let mut attribute_builders = if unnest_enable {
                let mut builders = (Vec::new(), Vec::new(), Vec::new());
                if let Some(ref attrs) = attr_fields {
                    for attr_name in attrs {
                        let field = OptionalField::new(&DataType::Utf8, batch_size)
                            .map_err(|e| ArrowError::ComputeError(format!("Failed to create field builder: {}", e)))?;
                        builders.0.push(attr_name.clone());
                        builders.1.push(DataType::Utf8);
                        builders.2.push(field);
                    }
                }
                Some(builders)
            } else {
                None
            };

            // Nested attributes builder
            let mut nested_builder = if !unnest_enable {
                Some(OptionalField::new(
                    &DataType::List(FieldRef::new(Field::new(
                        "attribute",
                        DataType::Struct(Fields::from(vec![
                            Field::new("tag", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        true,
                    ))),
                    batch_size,
                ).map_err(|e| ArrowError::ComputeError(format!("Failed to create nested builder: {}", e)))?)
            } else {
                None
            };

            // Build only the columns that are needed based on projection
            let projection_indices = match &projection {
                Some(p) => p.clone(),
                None => (0..schema_ref.fields().len()).collect(), // All columns if no projection
            };

            // Schema projection is now working correctly

            // Standard field builders - only create what we need
            let mut chrom_builder = if projection_indices.contains(&0) {
                Some(datafusion::arrow::array::StringBuilder::new())
            } else { None };
            let mut start_builder = if projection_indices.contains(&1) {
                Some(datafusion::arrow::array::UInt32Builder::new())
            } else { None };
            let mut end_builder = if projection_indices.contains(&2) {
                Some(datafusion::arrow::array::UInt32Builder::new())
            } else { None };
            let mut type_builder = if projection_indices.contains(&3) {
                Some(datafusion::arrow::array::StringBuilder::new())
            } else { None };
            let mut source_builder = if projection_indices.contains(&4) {
                Some(datafusion::arrow::array::StringBuilder::new())
            } else { None };
            let mut score_builder = if projection_indices.contains(&5) {
                Some(datafusion::arrow::array::Float32Builder::new())
            } else { None };
            let mut strand_builder = if projection_indices.contains(&6) {
                Some(datafusion::arrow::array::StringBuilder::new())
            } else { None };
            let mut phase_builder = if projection_indices.contains(&7) {
                Some(datafusion::arrow::array::UInt32Builder::new())
            } else { None };

            // Switch to boundary-aware reading similar to FASTQ parallel reader
            let mut reader = IndexedReader::new(std::io::BufReader::new(std::fs::File::open(&path)
                .map_err(|e| ArrowError::IoError(format!("Failed to reopen file: {}", e), e))?), index.clone());

            // Seek by uncompressed offset to partition start and align to next full line
            reader
                .seek(std::io::SeekFrom::Start(start_uncomp))
                .map_err(|e| ArrowError::IoError(format!("Failed to seek to start_uncomp: {}", e), e))?;
            if start_uncomp > 0 {
                synchronize_gff_reader(&mut reader, start_uncomp, end_comp)
                    .map_err(|e| ArrowError::IoError(format!("Failed to synchronize reader: {}", e), e))?;
            }

            let mut gff_reader = gff::io::Reader::new(reader);

            // Special case: empty projection (COUNT(*))
            if projection_indices.is_empty() {
                let mut num_rows = 0usize;
                loop {
                    if let Some(limit_val) = limit {
                        if num_rows >= limit_val { break; }
                    }
                    if gff_reader.get_ref().virtual_position().compressed() >= end_comp {
                        break;
                    }
                    // Read a line; skip comments and empty lines
                    match read_next_line(gff_reader.get_mut()) {
                        Ok(Some(line)) => {
                            if line.is_empty() || line[0] == b'#' { continue; }
                            // Parse columns minimally for filter evaluation
                            let mut cols = line.split(|&b| b == b'\t');
                            let seqid = cols.next().unwrap_or_default();
                            let source_v = cols.next().unwrap_or_default();
                            let ty_v = cols.next().unwrap_or_default();
                            let start_v = cols.next().unwrap_or_default();
                            let end_v = cols.next().unwrap_or_default();
                            let score_v = cols.next().unwrap_or_default();
                            let strand_v = cols.next().unwrap_or_default();
                            let phase_v = cols.next().unwrap_or_default();
                            let attrs_v = cols.next().unwrap_or_default();

                            let start_parsed = std::str::from_utf8(start_v).unwrap_or("0").parse::<u32>().unwrap_or(0);
                            let end_parsed = std::str::from_utf8(end_v).unwrap_or("0").parse::<u32>().unwrap_or(0);
                            let score_parsed = {
                                let s = std::str::from_utf8(score_v).unwrap_or(".");
                                if s == "." || s.is_empty() { None } else { s.parse::<f32>().ok() }
                            };
                            let phase_parsed: Option<u8> = {
                                let p = std::str::from_utf8(phase_v).unwrap_or(".");
                                if p == "." || p.is_empty() { None } else { p.parse::<u8>().ok() }
                            };

                            let attrs_str = std::str::from_utf8(attrs_v).unwrap_or("");
                            let rec = BgzfLineRecord {
                                chrom: std::str::from_utf8(seqid).unwrap_or("").to_string(),
                                start: start_parsed,
                                end: end_parsed,
                                ty: std::str::from_utf8(ty_v).unwrap_or("").to_string(),
                                source: std::str::from_utf8(source_v).unwrap_or("").to_string(),
                                score: score_parsed,
                                strand: std::str::from_utf8(strand_v).unwrap_or("").to_string(),
                                phase: phase_parsed,
                                attributes: attrs_str.to_string(),
                            };

                            if evaluate_filters_against_record(&rec, &filters, attrs_str) {
                                num_rows += 1;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => { Err(ArrowError::ExternalError(Box::new(e)))?; unreachable!() }
                    }
                }
                let batch = datafusion::arrow::record_batch::RecordBatch::try_new_with_options(
                    schema_ref.clone(),
                    vec![],
                    &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(num_rows)),
                ).map_err(|e| ArrowError::ComputeError(format!("Failed to build COUNT(*) batch: {}", e)))?;
                yield batch;
            } else {
                // Build batches until end of partition
                loop {
                    if let Some(limit_val) = limit { if total_records >= limit_val { break; } }
                    if gff_reader.get_ref().virtual_position().compressed() >= end_comp { break; }

                    // Read next line
                    let maybe_line = read_next_line(gff_reader.get_mut())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    let line = match maybe_line { Some(l) => l, None => break };
                    if line.is_empty() || line[0] == b'#' { continue; }

                    // Parse GFF columns
                    let mut cols = line.split(|&b| b == b'\t');
                    let seqid = cols.next().unwrap_or_default();
                    let source_v = cols.next().unwrap_or_default();
                    let ty_v = cols.next().unwrap_or_default();
                    let start_v = cols.next().unwrap_or_default();
                    let end_v = cols.next().unwrap_or_default();
                    let score_v = cols.next().unwrap_or_default();
                    let strand_v = cols.next().unwrap_or_default();
                    let phase_v = cols.next().unwrap_or_default();
                    let attrs_v = cols.next().unwrap_or_default();

                    // Before appending, evaluate filters using parsed values
                    let start_parsed = std::str::from_utf8(start_v).unwrap_or("0").parse::<u32>().unwrap_or(0);
                    let end_parsed = std::str::from_utf8(end_v).unwrap_or("0").parse::<u32>().unwrap_or(0);
                    let score_parsed = {
                        let s = std::str::from_utf8(score_v).unwrap_or(".");
                        if s == "." || s.is_empty() { None } else { s.parse::<f32>().ok() }
                    };
                    let phase_parsed: Option<u8> = {
                        let p = std::str::from_utf8(phase_v).unwrap_or(".");
                        if p == "." || p.is_empty() { None } else { p.parse::<u8>().ok() }
                    };
                    let attrs_str = std::str::from_utf8(attrs_v).unwrap_or("");
                    let rec = BgzfLineRecord {
                        chrom: std::str::from_utf8(seqid).unwrap_or("").to_string(),
                        start: start_parsed,
                        end: end_parsed,
                        ty: std::str::from_utf8(ty_v).unwrap_or("").to_string(),
                        source: std::str::from_utf8(source_v).unwrap_or("").to_string(),
                        score: score_parsed,
                        strand: std::str::from_utf8(strand_v).unwrap_or("").to_string(),
                        phase: phase_parsed,
                        attributes: attrs_str.to_string(),
                    };

                    if !evaluate_filters_against_record(&rec, &filters, attrs_str) {
                        continue; // skip non-matching records
                    }

                    // Append projected standard fields
                    if let Some(b) = &mut chrom_builder { b.append_value(&rec.chrom); }
                    if let Some(b) = &mut start_builder { b.append_value(rec.start); }
                    if let Some(b) = &mut end_builder { b.append_value(rec.end); }
                    if let Some(b) = &mut type_builder { b.append_value(&rec.ty); }
                    if let Some(b) = &mut source_builder { b.append_value(&rec.source); }
                    if let Some(b) = &mut score_builder { if let Some(f) = rec.score { b.append_value(f); } else { b.append_null(); } }
                    if let Some(b) = &mut strand_builder { b.append_value(&rec.strand); }
                    if let Some(b) = &mut phase_builder { if let Some(u) = rec.phase { b.append_value(u as u32); } else { b.append_null(); } }

                    if let Some(ref mut builders) = attribute_builders {
                        process_unnested_attributes(&rec.attributes, builders);
                    } else if let Some(ref mut builder) = nested_builder {
                        let attributes = process_nested_attributes(&rec.attributes);
                        builder.append_array_struct(attributes)
                            .map_err(|e| ArrowError::ComputeError(format!("Failed to append attributes: {}", e)))?;
                    }

                    record_count += 1;
                    total_records += 1;

                    if record_count >= batch_size {
                        let batch = create_batch_projected(
                            &schema_ref,
                            &projection_indices,
                            record_count,
                            &mut chrom_builder,
                            &mut start_builder,
                            &mut end_builder,
                            &mut type_builder,
                            &mut source_builder,
                            &mut score_builder,
                            &mut strand_builder,
                            &mut phase_builder,
                            &mut attribute_builders,
                            &mut nested_builder,
                        )?;
                        yield batch;
                        record_count = 0;
                    }
                }
            }

            // Emit final batch if there are remaining records
            if record_count > 0 {
                let batch = create_batch_projected(
                    &schema_ref,
                    &projection_indices,
                    record_count,
                    &mut chrom_builder,
                    &mut start_builder,
                    &mut end_builder,
                    &mut type_builder,
                    &mut source_builder,
                    &mut score_builder,
                    &mut strand_builder,
                    &mut phase_builder,
                    &mut attribute_builders,
                    &mut nested_builder,
                )?;
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            stream,
        )))
    }
}

// Align GFF reader to the start of the next full line to avoid partial records
fn synchronize_gff_reader<R: BufRead + Seek + Read>(
    reader: &mut IndexedReader<R>,
    start_uncomp: u64,
    end_comp: u64,
) -> io::Result<()> {
    if start_uncomp == 0 {
        return Ok(());
    }
    if start_uncomp > 0 {
        let prev = start_uncomp - 1;
        reader.seek(std::io::SeekFrom::Start(prev))?;
        let mut prev_buf = [0u8; 1];
        let got = reader.read(&mut prev_buf[..])?;
        reader.seek(std::io::SeekFrom::Start(start_uncomp))?;
        if got == 1 && prev_buf[0] == b'\n' {
            return Ok(());
        }
    }
    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            reader.consume(pos + 1);
            return Ok(());
        } else {
            let len = buf.len();
            reader.consume(len);
        }
    }
}

// Read the next full line (excluding trailing newline). Returns None on EOF.
fn read_next_line<R: BufRead>(reader: &mut IndexedReader<R>) -> io::Result<Option<Vec<u8>>> {
    let mut out = Vec::with_capacity(256);
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return if out.is_empty() {
                Ok(None)
            } else {
                Ok(Some(out))
            };
        }
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            out.extend_from_slice(&buf[..pos]);
            reader.consume(pos + 1);
            return Ok(Some(out));
        } else {
            out.extend_from_slice(buf);
            let len = buf.len();
            reader.consume(len);
        }
    }
}

/// Process attributes for unnested mode (specific attribute columns)
fn process_unnested_attributes(
    attributes_str: &str,
    builders: &mut (
        Vec<String>,
        Vec<datafusion::arrow::datatypes::DataType>,
        Vec<OptionalField>,
    ),
) {
    // Create lookup map
    let mut attr_map = std::collections::HashMap::new();

    if !attributes_str.is_empty() && attributes_str != "." {
        for pair in attributes_str.split(';') {
            if pair.is_empty() {
                continue;
            }
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];

                if builders.0.contains(&key.to_string()) {
                    let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                        &value[1..value.len() - 1]
                    } else {
                        value
                    };
                    attr_map.insert(key.to_string(), decoded_value.to_string());
                }
            }
        }
    }

    // Fill builders
    for (i, attr_name) in builders.0.iter().enumerate() {
        if let Some(value) = attr_map.get(attr_name) {
            let _ = builders.2[i].append_string(value);
        } else {
            let _ = builders.2[i].append_null();
        }
    }
}

/// Process attributes for nested mode (preserve original order)
fn process_nested_attributes(attributes_str: &str) -> Vec<Attribute> {
    let mut attributes = Vec::new();

    if !attributes_str.is_empty() && attributes_str != "." {
        for pair in attributes_str.split(';') {
            if pair.is_empty() {
                continue;
            }
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];

                let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                    value[1..value.len() - 1].to_string()
                } else {
                    value.to_string()
                };

                attributes.push(Attribute {
                    tag: key.to_string(),
                    value: Some(decoded_value),
                });
            }
        }
    }

    attributes
}

/// Create Arrow RecordBatch from builders
fn create_batch(
    schema: &SchemaRef,
    chrom_builder: &mut datafusion::arrow::array::StringBuilder,
    start_builder: &mut datafusion::arrow::array::UInt32Builder,
    end_builder: &mut datafusion::arrow::array::UInt32Builder,
    type_builder: &mut datafusion::arrow::array::StringBuilder,
    source_builder: &mut datafusion::arrow::array::StringBuilder,
    score_builder: &mut datafusion::arrow::array::Float32Builder,
    strand_builder: &mut datafusion::arrow::array::StringBuilder,
    phase_builder: &mut datafusion::arrow::array::UInt32Builder,
    attribute_builders: &mut Option<(
        Vec<String>,
        Vec<datafusion::arrow::datatypes::DataType>,
        Vec<OptionalField>,
    )>,
    nested_builder: &mut Option<OptionalField>,
) -> Result<RecordBatch, ArrowError> {
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(chrom_builder.finish()),
        Arc::new(start_builder.finish()),
        Arc::new(end_builder.finish()),
        Arc::new(type_builder.finish()),
        Arc::new(source_builder.finish()),
        Arc::new(score_builder.finish()),
        Arc::new(strand_builder.finish()),
        Arc::new(phase_builder.finish()),
    ];

    // Add attribute columns
    if let Some(builders) = attribute_builders {
        let attr_arrays = builders_to_arrays(&mut builders.2);
        columns.extend(attr_arrays.into_iter().map(|arr| arr as Arc<dyn Array>));
    } else if let Some(builder) = nested_builder {
        columns.push(builder.finish()?);
    }

    RecordBatch::try_new(schema.clone(), columns)
}

/// Create Arrow RecordBatch from builders with projection support
fn create_batch_projected(
    schema: &SchemaRef,
    projection_indices: &[usize],
    row_count: usize,
    chrom_builder: &mut Option<datafusion::arrow::array::StringBuilder>,
    start_builder: &mut Option<datafusion::arrow::array::UInt32Builder>,
    end_builder: &mut Option<datafusion::arrow::array::UInt32Builder>,
    type_builder: &mut Option<datafusion::arrow::array::StringBuilder>,
    source_builder: &mut Option<datafusion::arrow::array::StringBuilder>,
    score_builder: &mut Option<datafusion::arrow::array::Float32Builder>,
    strand_builder: &mut Option<datafusion::arrow::array::StringBuilder>,
    phase_builder: &mut Option<datafusion::arrow::array::UInt32Builder>,
    attribute_builders: &mut Option<(
        Vec<String>,
        Vec<datafusion::arrow::datatypes::DataType>,
        Vec<OptionalField>,
    )>,
    nested_builder: &mut Option<OptionalField>,
) -> Result<RecordBatch, ArrowError> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    // Special case: empty projection (e.g., COUNT(*)) should yield a 0-column batch
    // with the correct number of rows so DataFusion can sum them.
    if projection_indices.is_empty() {
        return datafusion::arrow::record_batch::RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(row_count)),
        );
    }

    // Add columns in the order they appear in the projected schema
    // The projection_indices refer to the original schema, but we need to build
    // columns in the order they appear in the projected schema
    for &col_idx in projection_indices {
        match col_idx {
            0 => {
                if let Some(builder) = chrom_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            1 => {
                if let Some(builder) = start_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            2 => {
                if let Some(builder) = end_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            3 => {
                if let Some(builder) = type_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            4 => {
                if let Some(builder) = source_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            5 => {
                if let Some(builder) = score_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            6 => {
                if let Some(builder) = strand_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            7 => {
                if let Some(builder) = phase_builder {
                    columns.push(Arc::new(builder.finish()));
                }
            }
            8 => {
                // Index 8 can be either nested attributes (mode 1) or first attribute column (mode 2)
                if let Some(builder) = nested_builder {
                    // Mode 1: Nested attributes
                    columns.push(builder.finish()?);
                } else if let Some(builders) = attribute_builders {
                    // Mode 2: First attribute column (gene_id)
                    let attr_idx = col_idx - 8; // attr_idx = 0 for gene_id
                    if attr_idx < builders.2.len() {
                        columns.push(builders.2[attr_idx].finish()?);
                    }
                }
            }
            _ => {
                // Attribute columns (mode 2) - index 9 and beyond
                if let Some(builders) = attribute_builders {
                    let attr_idx = col_idx - 8; // Attribute columns start after the 8 standard fields
                    if attr_idx < builders.2.len() {
                        columns.push(builders.2[attr_idx].finish()?);
                    }
                }
            }
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    // no imports needed

    #[test]
    fn test_create_batch_projected_empty_projection_sets_row_count() {
        // In COUNT(*) scans the execution plan passes a projected (empty) schema
        // and expects an empty batch with only row_count populated.
        let schema = Arc::new(Schema::empty());

        // Empty projection simulates COUNT(*) scans
        let projection: Vec<usize> = vec![];
        let mut chrom_builder: Option<datafusion::arrow::array::StringBuilder> = None;
        let mut start_builder: Option<datafusion::arrow::array::UInt32Builder> = None;
        let mut end_builder: Option<datafusion::arrow::array::UInt32Builder> = None;
        let mut type_builder: Option<datafusion::arrow::array::StringBuilder> = None;
        let mut source_builder: Option<datafusion::arrow::array::StringBuilder> = None;
        let mut score_builder: Option<datafusion::arrow::array::Float32Builder> = None;
        let mut strand_builder: Option<datafusion::arrow::array::StringBuilder> = None;
        let mut phase_builder: Option<datafusion::arrow::array::UInt32Builder> = None;
        let mut attribute_builders: Option<(Vec<String>, Vec<DataType>, Vec<OptionalField>)> = None;
        let mut nested_builder: Option<OptionalField> = None;

        let rows = 7usize;
        let batch = create_batch_projected(
            &schema,
            &projection,
            rows,
            &mut chrom_builder,
            &mut start_builder,
            &mut end_builder,
            &mut type_builder,
            &mut source_builder,
            &mut score_builder,
            &mut strand_builder,
            &mut phase_builder,
            &mut attribute_builders,
            &mut nested_builder,
        )
        .expect("should build empty-projection batch");

        // For empty projection, schema for the batch must be empty
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), rows);

        // Creating a normal projected batch (non-empty projection) should produce columns
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
        ]));
        let mut chrom_builder = Some(datafusion::arrow::array::StringBuilder::new());
        chrom_builder.as_mut().unwrap().append_value("chr1");
        let mut start_builder = Some(datafusion::arrow::array::UInt32Builder::new());
        start_builder.as_mut().unwrap().append_value(1);

        let batch2 = create_batch_projected(
            &schema2,
            &vec![0, 1],
            1,
            &mut chrom_builder,
            &mut start_builder,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
        )
        .expect("should build non-empty projected batch");
        assert_eq!(batch2.num_columns(), 2);
        assert_eq!(batch2.num_rows(), 1);
    }
}
