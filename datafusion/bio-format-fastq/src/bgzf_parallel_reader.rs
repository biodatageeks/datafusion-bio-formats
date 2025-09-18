use std::any::Any;
use std::io::{self, BufRead, Seek};
use std::sync::Arc;
use std::thread;

use crate::parser::FastqParser;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch, StringBuilder};
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
use futures::channel::mpsc;
use noodles_bgzf::{IndexedReader, gzi};
use noodles_fastq as fastq;
use std::path::PathBuf;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct BgzfFastqTableProvider {
    path: PathBuf,
    schema: SchemaRef,
    parser: FastqParser,
}

impl BgzfFastqTableProvider {
    pub fn new(path: impl Into<PathBuf>) -> datafusion::common::Result<Self> {
        Self::new_with_parser(path, FastqParser::default())
    }

    pub fn new_with_parser(
        path: impl Into<PathBuf>,
        parser: FastqParser,
    ) -> datafusion::common::Result<Self> {
        let path = path.into();
        log::info!(
            "Creating BGZF FASTQ table provider with {} parser for file: {:?}",
            parser,
            path
        );

        if parser == FastqParser::Needletail {
            log::warn!(
                "Note: BGZF table provider with needletail parser falls back to noodles for indexed BGZF reading"
            );
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ]));
        Ok(Self {
            path,
            schema,
            parser,
        })
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
impl datafusion::catalog::TableProvider for BgzfFastqTableProvider {
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
        debug!("Partitions: {:?}", partitions);
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let exec = BgzfFastqExec::new(
            self.path.clone(),
            partitions,
            projected_schema,
            projection.cloned(),
            index,
            limit,
            self.parser,
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct BgzfFastqExec {
    path: PathBuf,
    partitions: Vec<(u64, u64)>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    index: gzi::Index,
    limit: Option<usize>,
    parser: FastqParser,
    properties: PlanProperties,
}

impl BgzfFastqExec {
    fn new(
        path: PathBuf,
        partitions: Vec<(u64, u64)>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        index: gzi::Index,
        limit: Option<usize>,
        parser: FastqParser,
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
            parser,
            properties,
        }
    }
}

impl DisplayAs for BgzfFastqExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BgzfFastqExec")
    }
}

fn find_line_end(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|pos| start + pos)
}

fn synchronize_reader<R: BufRead>(reader: &mut IndexedReader<R>, end_comp: u64) -> io::Result<()> {
    // DO NOT perform an initial read_until, as it can discard a valid header
    // if the initial seek lands exactly on the start of a line.
    // The loop below is capable of handling any starting position.

    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(()); // EOF
        }

        // Find the first potential header line starting with '@'.
        if let Some(at_pos) = buf.iter().position(|&b| b == b'@') {
            // We have a potential header. To validate it, we must find a '+' on the third line after it.
            // This must all be done by inspecting the current buffer without consuming it.
            if let Some(l1_end) = find_line_end(buf, at_pos) {
                if let Some(l2_end) = find_line_end(buf, l1_end + 1) {
                    if let Some(l3_start) = l2_end.checked_add(1) {
                        if buf.get(l3_start) == Some(&b'+') {
                            // Validation successful. This is a real record start.
                            // Consume the stream up to the start of this valid record.
                            reader.consume(at_pos);
                            return Ok(());
                        }
                    }
                }
            }
            // If we get here, the validation failed because the buffer wasn't big enough
            // to contain the full 4-line record, OR it was a false positive.
            // We must consume data to get more context.
            // Consume up to and including the false-positive '@' line and try again.
            if let Some(end_of_at_line) = find_line_end(buf, at_pos) {
                reader.consume(end_of_at_line + 1);
            } else {
                // The buffer ends mid-line, consume it all.
                let len = buf.len();
                reader.consume(len);
            }
        } else {
            // No '@' found in the entire buffer. Consume it all and get a new one.
            let len = buf.len();
            reader.consume(len);
        }
    }
}

#[async_trait]
impl ExecutionPlan for BgzfFastqExec {
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
        "BgzfFastqExec"
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
        log::info!(
            "Executing BGZF FASTQ partition {} with {} parser",
            partition,
            self.parser
        );
        let (start_uncomp, end_comp) = self.partitions[partition];
        let path = self.path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let index = self.index.clone();
        let limit = self.limit;
        let batch_size = context.session_config().batch_size();

        let (mut tx, rx) = mpsc::channel::<(Result<RecordBatch, ArrowError>, usize)>(2);

        let _handle = thread::spawn(move || {
            let read_and_send = || -> Result<(), ArrowError> {
                let file = std::fs::File::open(path)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                let mut reader = IndexedReader::new(std::io::BufReader::new(file), index);

                reader
                    .seek(std::io::SeekFrom::Start(start_uncomp))
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                if start_uncomp > 0 {
                    synchronize_reader(&mut reader, end_comp)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                }

                let mut fastq_reader = fastq::io::Reader::new(reader);
                let mut record = fastq::Record::default();
                let mut total_records = 0;

                if let Some(proj) = &projection {
                    if proj.is_empty() {
                        let mut num_rows = 0;
                        loop {
                            if let Some(limit) = limit {
                                if num_rows >= limit {
                                    break;
                                }
                            }
                            if fastq_reader.get_ref().virtual_position().compressed() >= end_comp {
                                break;
                            }
                            match fastq_reader.read_record(&mut record) {
                                Ok(0) => break,
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

                    let proj_indices = projection.as_ref();
                    let mut names = proj_indices
                        .map_or(true, |p| p.contains(&0))
                        .then(StringBuilder::new);
                    let mut descriptions = proj_indices
                        .map_or(true, |p| p.contains(&1))
                        .then(StringBuilder::new);
                    let mut sequences = proj_indices
                        .map_or(true, |p| p.contains(&2))
                        .then(StringBuilder::new);
                    let mut quality_scores = proj_indices
                        .map_or(true, |p| p.contains(&3))
                        .then(StringBuilder::new);

                    let mut count = 0;
                    while count < batch_size {
                        if let Some(limit) = limit {
                            if total_records >= limit {
                                break;
                            }
                        }

                        if fastq_reader.get_ref().virtual_position().compressed() >= end_comp {
                            break;
                        }

                        match fastq_reader.read_record(&mut record) {
                            Ok(0) => break, // End of stream
                            Ok(_) => {
                                if let Some(b) = &mut names {
                                    b.append_value(std::str::from_utf8(record.name()).unwrap());
                                }
                                if let Some(b) = &mut descriptions {
                                    if record.description().is_empty() {
                                        b.append_null();
                                    } else {
                                        b.append_value(
                                            std::str::from_utf8(record.description()).unwrap(),
                                        );
                                    }
                                }
                                if let Some(b) = &mut sequences {
                                    b.append_value(std::str::from_utf8(record.sequence()).unwrap());
                                }
                                if let Some(b) = &mut quality_scores {
                                    b.append_value(
                                        std::str::from_utf8(record.quality_scores()).unwrap(),
                                    );
                                }
                                count += 1;
                                total_records += 1;
                            }
                            Err(e) => return Err(ArrowError::ExternalError(Box::new(e))),
                        }
                    }

                    if count == 0 {
                        break;
                    }

                    let mut arrays: Vec<Arc<dyn Array>> = vec![];
                    if let Some(proj) = &projection {
                        for &col_idx in proj.iter() {
                            match col_idx {
                                0 => arrays.push(Arc::new(names.as_mut().unwrap().finish())),
                                1 => arrays.push(Arc::new(descriptions.as_mut().unwrap().finish())),
                                2 => arrays.push(Arc::new(sequences.as_mut().unwrap().finish())),
                                3 => {
                                    arrays.push(Arc::new(quality_scores.as_mut().unwrap().finish()))
                                }
                                _ => unreachable!(),
                            }
                        }
                    } else {
                        arrays.push(Arc::new(names.unwrap().finish()));
                        arrays.push(Arc::new(descriptions.unwrap().finish()));
                        arrays.push(Arc::new(sequences.unwrap().finish()));
                        arrays.push(Arc::new(quality_scores.unwrap().finish()));
                    }

                    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
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
                debug!("Partition {}: processed {} rows", partition, count);
                item.map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        )))
    }
}
