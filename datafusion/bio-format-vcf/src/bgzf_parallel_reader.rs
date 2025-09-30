use std::any::Any;
use std::io::{self, BufRead, Seek};
use std::sync::Arc;
use std::thread;

use crate::table_provider::{info_to_arrow_type, is_nullable};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use datafusion_bio_format_core::table_utils::{OptionalField, builders_to_arrays};
use futures::channel::mpsc;
use log::debug;
use noodles_bgzf::{IndexedReader, gzi};
use noodles_vcf as vcf;
use noodles_vcf::header::Formats;
use noodles_vcf::variant::Record;
use noodles_vcf::variant::record::info::field::{Value, value::Array as ValueArray};
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids};
use std::path::PathBuf;

#[cfg(test)]
use tempfile::tempdir;

#[derive(Debug, Clone)]
pub struct BgzfVcfTableProvider {
    path: PathBuf,
    schema: SchemaRef,
    all_info_fields: Vec<String>,
    all_format_fields: Vec<String>,
}

impl BgzfVcfTableProvider {
    pub fn try_new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        // Read VCF header to determine schema using BGZF decompression
        let file = std::fs::File::open(&path)?;
        let bgzf_reader = noodles_bgzf::Reader::new(std::io::BufReader::new(file));
        let mut reader = vcf::io::Reader::new(bgzf_reader);
        let header = reader.read_header()?;

        let mut fields = vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
        ];

        let header_infos = header.infos();
        let header_formats = header.formats();

        let mut all_info_fields = Vec::new();
        for (tag, info) in header_infos.iter() {
            let dtype = info_to_arrow_type(&header_infos, tag);
            let nullable = is_nullable(&info.ty());
            fields.push(Field::new(tag, dtype, nullable));
            all_info_fields.push(tag.to_string());
        }

        let mut all_format_fields = Vec::new();
        for (tag, _format) in header_formats.iter() {
            let dtype = format_to_arrow_type(&header_formats, tag);
            fields.push(Field::new(format!("format_{}", tag), dtype, true));
            all_format_fields.push(tag.to_string());
        }

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            path,
            schema,
            all_info_fields,
            all_format_fields,
        })
    }
}

fn format_to_arrow_type(formats: &Formats, field: &str) -> DataType {
    let format = formats.get(field).unwrap();
    match format.ty() {
        noodles_vcf::header::record::value::map::format::Type::Integer => DataType::Int32,
        noodles_vcf::header::record::value::map::format::Type::Float => DataType::Float32,
        noodles_vcf::header::record::value::map::format::Type::Character => DataType::Utf8,
        noodles_vcf::header::record::value::map::format::Type::String => DataType::Utf8,
    }
}

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

        let (start_comp, _) = block_offsets[current_block_idx];

        let remainder = num_blocks % num_partitions;
        let blocks_in_partition = num_blocks / num_partitions + if i < remainder { 1 } else { 0 };
        let next_partition_start_block_idx = current_block_idx + blocks_in_partition;

        let end_comp = if next_partition_start_block_idx >= num_blocks {
            u64::MAX
        } else {
            block_offsets[next_partition_start_block_idx].0
        };

        ranges.push((start_comp, end_comp));
        current_block_idx = next_partition_start_block_idx;
    }
    ranges
}

#[async_trait]
impl datafusion::catalog::TableProvider for BgzfVcfTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut index_path = self.path.as_os_str().to_owned();
        index_path.push(".gzi");

        let index = gzi::fs::read(index_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to read GZI index: {}", e)))?;

        let partitions = get_bgzf_partition_bounds(&index, state.config().target_partitions());
        debug!("VCF Partitions: {:?}", partitions);

        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let exec = BgzfVcfExec::new(
            self.path.clone(),
            partitions,
            projected_schema,
            projection.cloned(),
            index,
            limit,
            self.all_info_fields.clone(),
            self.all_format_fields.clone(),
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct BgzfVcfExec {
    path: PathBuf,
    partitions: Vec<(u64, u64)>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    index: gzi::Index,
    limit: Option<usize>,
    all_info_fields: Vec<String>,
    properties: PlanProperties,
}

impl BgzfVcfExec {
    fn new(
        path: PathBuf,
        partitions: Vec<(u64, u64)>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        index: gzi::Index,
        limit: Option<usize>,
        all_info_fields: Vec<String>,
        all_format_fields: Vec<String>,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            path,
            partitions,
            schema,
            projection,
            index,
            limit,
            all_info_fields,
            properties,
        }
    }
}

impl DisplayAs for BgzfVcfExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BgzfVcfExec")
    }
}

fn find_line_end(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|pos| start + pos)
}

fn synchronize_vcf_reader<R: BufRead>(
    reader: &mut IndexedReader<R>,
    end_comp: u64,
) -> io::Result<()> {
    // Step 1: advance to the start of the next line (in case we are mid-line)
    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(()); // EOF
        }

        // If the current buffer begins at a newline, we are at line start
        if buf[0] == b'\n' || buf[0] == b'\r' {
            reader.consume(1);
            continue;
        }

        // If the current buffer looks like the beginning of a line, proceed
        if buf[0] == b'#' || buf[0].is_ascii_alphanumeric() {
            break;
        }

        // Otherwise, consume until end of current line
        if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            reader.consume(nl + 1);
        } else {
            // No newline in buffer; consume all and try again
            let len = buf.len();
            reader.consume(len);
        }
    }

    // Step 2: skip header lines (those starting with '#')
    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }

        if buf[0] == b'#' {
            // Consume until the end of this header line
            if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                reader.consume(nl + 1);
            } else {
                let len = buf.len();
                reader.consume(len);
            }
        } else {
            // At the start of a data line
            return Ok(());
        }
    }
}

fn get_variant_end(record: &dyn Record, header: &vcf::Header) -> u32 {
    let ref_len = record.reference_bases().len();
    let alt_len = record.alternate_bases().len();

    // Check if all are single base ACTG
    if ref_len == 1
        && alt_len == 1
        && record
            .reference_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c == b'A' || c == b'C' || c == b'G' || c == b'T')
        && record
            .alternate_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c.eq("A") || c.eq("C") || c.eq("G") || c.eq("T"))
    {
        record.variant_start().unwrap().unwrap().get() as u32
    } else {
        record.variant_end(header).unwrap().get() as u32
    }
}

fn load_infos(
    record: Box<dyn Record>,
    header: &vcf::Header,
    info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) -> Result<(), ArrowError> {
    for i in 0..info_builders.2.len() {
        let name = &info_builders.0[i];
        let data_type = &info_builders.1[i];
        let builder = &mut info_builders.2[i];
        let info = record.info();
        let value = info.get(header, name);

        match value {
            Some(Ok(v)) => match v {
                Some(Value::Integer(v)) => {
                    builder.append_int(v)?;
                }
                Some(Value::Array(ValueArray::Integer(values))) => {
                    builder
                        .append_array_int(values.iter().map(|v| v.unwrap().unwrap()).collect())?;
                }
                Some(Value::Array(ValueArray::Float(values))) => {
                    builder
                        .append_array_float(values.iter().map(|v| v.unwrap().unwrap()).collect())?;
                }
                Some(Value::Float(v)) => {
                    builder.append_float(v)?;
                }
                Some(Value::String(v)) => {
                    builder.append_string(&v)?;
                }
                Some(Value::Array(ValueArray::String(values))) => {
                    builder.append_array_string(
                        values
                            .iter()
                            .map(|v| v.unwrap().unwrap().to_string())
                            .collect(),
                    )?;
                }
                Some(Value::Flag) => {
                    builder.append_boolean(true)?;
                }
                None => {
                    if data_type == &DataType::Boolean {
                        builder.append_boolean(false)?;
                    } else {
                        builder.append_null()?;
                    }
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(
                        "Unsupported value type".to_string(),
                    ));
                }
            },
            _ => {
                if data_type == &DataType::Boolean {
                    builder.append_boolean(false)?;
                } else {
                    builder.append_null()?;
                }
            }
        }
    }
    Ok(())
}

#[async_trait]
impl ExecutionPlan for BgzfVcfExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "BgzfVcfExec"
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
        let (start_uncomp, end_comp) = self.partitions[partition];
        let path = self.path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let index = self.index.clone();
        let limit = self.limit;
        let batch_size = context.session_config().batch_size();
        let all_info_fields = self.all_info_fields.clone();

        let (mut tx, rx) = mpsc::channel::<(Result<RecordBatch, ArrowError>, usize)>(2);

        let _handle = thread::spawn(move || {
            let read_and_send = || -> Result<(), ArrowError> {
                // Read the VCF header from the start of the file (independent of partition)
                let header = {
                    let header_file = std::fs::File::open(&path)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    let bgzf_reader =
                        noodles_bgzf::Reader::new(std::io::BufReader::new(header_file));
                    let mut hdr_reader = vcf::io::Reader::new(bgzf_reader);
                    hdr_reader
                        .read_header()
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                };

                // Create an indexed reader for this partition
                let file = std::fs::File::open(path)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                let mut reader = IndexedReader::new(std::io::BufReader::new(file), index);

                reader
                    .seek(std::io::SeekFrom::Start(start_uncomp))
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                // Always synchronize to the next record line (skip headers), even for partition 0
                synchronize_vcf_reader(&mut reader, end_comp)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                // Wrap in a VCF reader without reading header again
                let mut vcf_reader = vcf::io::Reader::new(reader);

                let mut total_records = 0;
                let header_infos = header.infos();

                // Determine which fields are needed based on projection
                let needs_chrom = projection.as_ref().map_or(true, |proj| proj.contains(&0));
                let needs_start = projection.as_ref().map_or(true, |proj| proj.contains(&1));
                let needs_end = projection.as_ref().map_or(true, |proj| proj.contains(&2));
                let needs_id = projection.as_ref().map_or(true, |proj| proj.contains(&3));
                let needs_ref = projection.as_ref().map_or(true, |proj| proj.contains(&4));
                let needs_alt = projection.as_ref().map_or(true, |proj| proj.contains(&5));
                let needs_qual = projection.as_ref().map_or(true, |proj| proj.contains(&6));
                let needs_filter = projection.as_ref().map_or(true, |proj| proj.contains(&7));

                // Set up info builders
                let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
                    (Vec::new(), Vec::new(), Vec::new());

                for field_name in all_info_fields {
                    let data_type = info_to_arrow_type(&header_infos, &field_name);
                    let field = OptionalField::new(&data_type, batch_size)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    info_builders.0.push(field_name);
                    info_builders.1.push(data_type);
                    info_builders.2.push(field);
                }

                // Handle empty projection (COUNT queries)
                if let Some(proj) = &projection {
                    if proj.is_empty() {
                        let mut num_rows = 0;
                        let mut record = vcf::Record::default();
                        loop {
                            if let Some(limit) = limit {
                                if num_rows >= limit {
                                    break;
                                }
                            }
                            if vcf_reader.get_ref().virtual_position().compressed() >= end_comp {
                                break;
                            }
                            match vcf_reader.read_record(&mut record) {
                                Ok(0) => break, // EOF
                                Ok(_) => num_rows += 1,
                                Err(e) => return Err(ArrowError::ExternalError(Box::new(e))),
                            }
                        }
                        let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(num_rows));
                        let batch = RecordBatch::try_new_with_options(schema, vec![], &options)?;
                        tx.try_send((Ok(batch), num_rows)).ok();
                        return Ok(());
                    }
                }

                loop {
                    if let Some(limit) = limit {
                        if total_records >= limit {
                            break;
                        }
                    }

                    // Create vectors for batch data
                    let mut chroms = if needs_chrom {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut poss = if needs_start {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut pose = if needs_end {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut ids = if needs_id {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut refs = if needs_ref {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut alts = if needs_alt {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut quals = if needs_qual {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };
                    let mut filters = if needs_filter {
                        Vec::with_capacity(batch_size)
                    } else {
                        Vec::new()
                    };

                    let mut count = 0;
                    let mut record = vcf::Record::default();

                    while count < batch_size {
                        if let Some(limit) = limit {
                            if total_records >= limit {
                                break;
                            }
                        }
                        if vcf_reader.get_ref().virtual_position().compressed() >= end_comp {
                            break;
                        }

                        match vcf_reader.read_record(&mut record) {
                            Ok(0) => break, // EOF
                            Ok(_) => {}     // Continue processing
                            Err(e) => return Err(ArrowError::ExternalError(Box::new(e))),
                        }

                        // Only parse and store fields that are needed
                        if needs_chrom {
                            chroms.push(record.reference_sequence_name().to_string());
                        }
                        if needs_start {
                            poss.push(
                                record
                                    .variant_start()
                                    .unwrap()
                                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                                    .get() as u32,
                            );
                        }
                        if needs_end {
                            pose.push(get_variant_end(&record, &header));
                        }
                        if needs_id {
                            ids.push(
                                record
                                    .ids()
                                    .iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(";"),
                            );
                        }
                        if needs_ref {
                            refs.push(record.reference_bases().to_string());
                        }
                        if needs_alt {
                            alts.push(
                                record
                                    .alternate_bases()
                                    .iter()
                                    .map(|v| v.unwrap_or(".").to_string())
                                    .collect::<Vec<String>>()
                                    .join("|"),
                            );
                        }
                        if needs_qual {
                            quals.push(
                                record
                                    .quality_score()
                                    .transpose()
                                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                                    .map(|v| v as f64),
                            );
                        }
                        if needs_filter {
                            filters.push(
                                record
                                    .filters()
                                    .iter(&header)
                                    .map(|v| v.unwrap_or(".").to_string())
                                    .collect::<Vec<String>>()
                                    .join(";"),
                            );
                        }

                        // Load info fields if needed
                        if !info_builders.0.is_empty() {
                            load_infos(Box::new(record.clone()), &header, &mut info_builders)?;
                        }

                        count += 1;
                        total_records += 1;
                    }

                    if count == 0 {
                        break;
                    }

                    // Build record batch using the existing logic
                    let batch = crate::physical_exec::build_record_batch(
                        schema.clone(),
                        &chroms,
                        &poss,
                        &pose,
                        &ids,
                        &refs,
                        &alts,
                        &quals,
                        &filters,
                        Some(&builders_to_arrays(&mut info_builders.2)),
                        projection.clone(),
                    )
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    let num_rows = batch.num_rows();
                    // Block until the send succeeds
                    while let Err(e) = tx.try_send((Ok(batch.clone()), num_rows)) {
                        if e.is_disconnected() {
                            // Receiver is gone, exit
                            return Ok(());
                        }
                        // Channel is full, yield and retry
                        std::thread::yield_now();
                    }
                }
                Ok(())
            };

            if let Err(e) = read_and_send() {
                let _ = tx.try_send((Err(e), 0));
            }
        });

        use futures::StreamExt;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            rx.map(move |(item, count)| {
                debug!("VCF Partition {}: processed {} rows", partition, count);
                item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(pairs: &[(u64, u64)]) -> gzi::Index {
        // gzi::Index implements From<Vec<(u64,u64)>> in noodles-bgzf
        // If this ever changes, adapt by constructing via default and extend.
        gzi::Index::from(pairs.to_vec())
    }

    #[test]
    fn partitions_do_not_overlap_and_are_ordered() {
        // Simulate 6 BGZF blocks at compressed offsets with dummy uncompressed positions
        let idx = make_index(&[(100, 0), (200, 0), (300, 0), (400, 0), (500, 0), (600, 0)]);
        let parts = get_bgzf_partition_bounds(&idx, 3);
        assert_eq!(parts.len(), 3);
        // Check ordering and non-overlap
        for w in parts.windows(2) {
            assert!(w[0].0 <= w[0].1, "partition start <= end");
            assert!(w[1].0 <= w[1].1, "partition start <= end");
            assert!(w[0].1 <= w[1].0, "non-overlapping and ordered");
        }
        // First starts at 0 (we insert (0,0) sentinel)
        assert_eq!(parts.first().unwrap().0, 0);
        // Last ends at MAX since we use open-ended end for final partition
        assert_eq!(parts.last().unwrap().1, u64::MAX);
    }

    #[test]
    fn partitions_when_threads_exceed_blocks() {
        let idx = make_index(&[(128, 0), (256, 0)]);
        let parts = get_bgzf_partition_bounds(&idx, 8);
        // We should not create more partitions than blocks+sentinel
        assert_eq!(parts.len(), 3.min(8).min(idx.as_ref().len() + 1));
    }

    #[test]
    fn single_partition_when_no_blocks() {
        let empty = make_index(&[]);
        let parts = get_bgzf_partition_bounds(&empty, 4);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], (0, u64::MAX));
    }
}
