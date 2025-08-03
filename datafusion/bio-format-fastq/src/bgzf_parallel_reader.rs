use std::any::Any;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    common::Result,
    datasource::TableType,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
    },
};
use futures::stream::{self};
use noodles_bgzf::{IndexedReader, gzi};
use noodles_fastq as fastq;

#[derive(Debug, Clone)]
pub struct BgzfFastqTableProvider {
    path: PathBuf,
    schema: SchemaRef,
}

impl BgzfFastqTableProvider {
    pub fn try_new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ]));
        Ok(Self {
            path: path.into(),
            schema,
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut index_path = self.path.as_os_str().to_owned();
        index_path.push(".gzi");

        let index = gzi::fs::read(index_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to read GZI index: {}", e)))?;

        let partitions = get_bgzf_partition_bounds(&index, state.config().target_partitions());

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
    properties: PlanProperties,
}

impl BgzfFastqExec {
    fn new(
        path: PathBuf,
        partitions: Vec<(u64, u64)>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        index: gzi::Index,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions.len()),
            datafusion::physical_plan::ExecutionMode::Bounded,
        );
        Self {
            path,
            partitions,
            schema,
            projection,
            index,
            properties,
        }
    }
}

impl DisplayAs for BgzfFastqExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BgzfFastqExec")
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (start_uncomp, end_comp) = self.partitions[partition];
        let path = self.path.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let index = self.index.clone();

        let stream = stream::once(async move {
            let batch = tokio::task::spawn_blocking(move || {
                let file = File::open(path)?;
                let buf_reader = BufReader::new(file);
                let mut reader = IndexedReader::new(buf_reader, index);

                reader.seek(SeekFrom::Start(start_uncomp))?;

                if start_uncomp > 0 {
                    skip_to_next_fastq_record(&mut reader)?;
                }

                let mut fastq_reader = fastq::io::Reader::new(reader);
                let mut record = fastq::Record::default();

                if let Some(proj) = &projection {
                    if proj.is_empty() {
                        let mut num_rows = 0;
                        loop {
                            match fastq_reader.read_record(&mut record) {
                                Ok(0) => break,
                                Ok(_) => num_rows += 1,
                                Err(_) => break,
                            }
                            if fastq_reader.get_ref().virtual_position().compressed() >= end_comp {
                                break;
                            }
                        }
                        let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(num_rows));
                        return RecordBatch::try_new_with_options(schema, vec![], &options)
                            .map_err(|e| DataFusionError::ArrowError(e, None));
                    }
                }

                let proj_indices = projection.as_ref();
                let mut names = proj_indices
                    .map_or(true, |p| p.contains(&0))
                    .then(StringBuilder::new);
                let mut sequences = proj_indices
                    .map_or(true, |p| p.contains(&1))
                    .then(StringBuilder::new);
                let mut quality_scores = proj_indices
                    .map_or(true, |p| p.contains(&2))
                    .then(StringBuilder::new);

                loop {
                    // First, read a record.
                    match fastq_reader.read_record(&mut record) {
                        Ok(0) => break, // End of stream
                        Ok(_) => {
                            // If successful, process it.
                            if let Some(b) = &mut names {
                                b.append_value(std::str::from_utf8(record.name()).unwrap());
                            }
                            if let Some(b) = &mut sequences {
                                b.append_value(std::str::from_utf8(record.sequence()).unwrap());
                            }
                            if let Some(b) = &mut quality_scores {
                                b.append_value(
                                    std::str::from_utf8(record.quality_scores()).unwrap(),
                                );
                            }
                        }
                        Err(_) => break, // Stop on error
                    }

                    // AFTER processing, check if the read crossed the boundary.
                    if fastq_reader.get_ref().virtual_position().compressed() >= end_comp {
                        break;
                    }
                }

                let mut arrays: Vec<Arc<dyn Array>> = vec![];
                if let Some(proj) = &projection {
                    for &col_idx in proj.iter() {
                        match col_idx {
                            0 => arrays.push(Arc::new(names.take().unwrap().finish())),
                            1 => arrays.push(Arc::new(sequences.take().unwrap().finish())),
                            2 => arrays.push(Arc::new(quality_scores.take().unwrap().finish())),
                            _ => unreachable!(),
                        }
                    }
                } else {
                    arrays.push(Arc::new(names.unwrap().finish()));
                    arrays.push(Arc::new(sequences.unwrap().finish()));
                    arrays.push(Arc::new(quality_scores.unwrap().finish()));
                }

                RecordBatch::try_new(schema, arrays)
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            })
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))??;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn is_valid_record_start(buf: &[u8]) -> bool {
    if !buf.starts_with(b"@") {
        return false;
    }

    let mut lines = buf.splitn(5, |&b| b == b'\n');
    if let (Some(l1), Some(l2), Some(l3), Some(l4)) =
        (lines.next(), lines.next(), lines.next(), lines.next())
    {
        if l1.starts_with(b"@") && l3.starts_with(b"+") {
            let seq_len = if l2.ends_with(b"\r") {
                l2.len() - 1
            } else {
                l2.len()
            };
            // Quality scores line can be shorter than sequence line if it's the last line of the file
            let qual_len = if l4.ends_with(b"\r") {
                l4.len() - 1
            } else {
                l4.len()
            };
            return seq_len >= qual_len;
        }
    }

    false
}

fn skip_to_next_fastq_record<R: BufRead>(reader: &mut R) -> io::Result<()> {
    let buf = reader.fill_buf()?;
    if buf.is_empty() || is_valid_record_start(buf) {
        // Current position is already a valid start or EOF, do nothing.
        return Ok(());
    }

    // The current position is inside a record, so we must find the next one.
    // First, consume the rest of the current line.
    if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
        reader.consume(pos + 1);
    } else {
        // No newline in buffer, consume all.
        let len = buf.len();
        reader.consume(len);
    }

    // Now, search for the start of a valid record.
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            // End of file
            return Ok(());
        }

        if is_valid_record_start(buf) {
            return Ok(());
        }

        // Not a valid start, consume this line and try the next.
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            reader.consume(pos + 1);
        } else {
            let len = buf.len();
            reader.consume(len);
        }
    }
}
