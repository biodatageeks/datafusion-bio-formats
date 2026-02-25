use crate::storage::{FastqLocalReader, FastqRemoteReader};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};

use futures::channel::mpsc;
use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};
use noodles_bgzf::{IndexedReader, gzi};
use noodles_fastq as fastq;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::thread;

/// Number of RecordBatch items buffered in the mpsc channel between the reader thread
/// and the async stream consumer. A small buffer (2) keeps memory usage low while still
/// allowing the reader thread to stay ahead of the consumer by one batch.
const PARTITION_CHANNEL_BUFFER: usize = 2;

/// Strategy for partitioning a FASTQ file across multiple execution partitions.
///
/// The strategy is automatically selected in [`FastqTableProvider::scan()`] based on
/// file type, compression format, and available index files:
///
/// - **Local BGZF with GZI index** → [`Bgzf`](Self::Bgzf) (N partitions via block ranges)
/// - **Local BGZF without GZI index** → [`Sequential`](Self::Sequential) (fallback)
/// - **Local uncompressed** → [`ByteRange`](Self::ByteRange) (N partitions via byte offsets)
/// - **Local GZIP** → [`Sequential`](Self::Sequential) (cannot seek into gzip stream)
/// - **Remote (any format)** → [`Sequential`](Self::Sequential) (byte-range seeks not supported)
///
/// N is determined by `SessionConfig::target_partitions()`, capped by block count or file size.
#[derive(Debug, Clone)]
pub(crate) enum FastqPartitionStrategy {
    /// BGZF-compressed file with GZI index — each partition reads a range of BGZF blocks.
    ///
    /// Selected when a local `.bgz`/`.bgzf` file has a companion `.gzi` index.
    /// Partition boundaries are computed from the GZI block offsets.
    Bgzf {
        partitions: Vec<(u64, u64)>,
        index: gzi::Index,
    },
    /// Uncompressed file — each partition reads a byte range.
    ///
    /// Selected when a local file has no compression (detected via magic bytes).
    /// The file is divided into equal-sized byte ranges, and each partition
    /// synchronizes to the next FASTQ record boundary before reading.
    ByteRange { partitions: Vec<(u64, u64)> },
    /// Sequential single-partition read (GZIP, remote, or fallback).
    ///
    /// Used when parallel reading is not possible: GZIP-compressed files (no random
    /// access), remote files (cloud storage), BGZF without a GZI index, or when
    /// `target_partitions` is 1.
    Sequential,
}

/// Compression detected from magic bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetectedCompression {
    Bgzf,
    Gzip,
    None,
}

/// Detect compression by reading the first 18 bytes of a local file synchronously.
pub(crate) fn detect_compression_sync(file_path: &str) -> io::Result<DetectedCompression> {
    let mut file = std::fs::File::open(file_path)?;
    let mut buf = [0u8; 18];
    let n = file.read(&mut buf)?;

    if n >= 18
        && buf[0] == 0x1f
        && buf[1] == 0x8b
        && buf[2] == 0x08
        && buf[3] & 0x04 != 0
        && buf[12] == 0x42
        && buf[13] == 0x43
    {
        Ok(DetectedCompression::Bgzf)
    } else if n >= 2 && buf[0] == 0x1f && buf[1] == 0x8b {
        Ok(DetectedCompression::Gzip)
    } else {
        Ok(DetectedCompression::None)
    }
}

/// Determine the partition strategy for a local file.
///
/// Uses synchronous I/O only — safe to call from async context via `scan()`.
pub(crate) fn detect_local_strategy(
    file_path: &str,
    target_partitions: usize,
) -> io::Result<FastqPartitionStrategy> {
    let compression = detect_compression_sync(file_path)?;

    match compression {
        DetectedCompression::Bgzf => {
            // Try to read GZI index
            let gzi_path = format!("{file_path}.gzi");
            match gzi::fs::read(&gzi_path) {
                Ok(index) => {
                    let partitions = get_bgzf_partition_bounds(&index, target_partitions);
                    Ok(FastqPartitionStrategy::Bgzf { partitions, index })
                }
                Err(_) => {
                    // No GZI index — fall back to sequential
                    Ok(FastqPartitionStrategy::Sequential)
                }
            }
        }
        DetectedCompression::Gzip => Ok(FastqPartitionStrategy::Sequential),
        DetectedCompression::None => {
            let file_size = std::fs::metadata(file_path)?.len();
            if file_size == 0 || target_partitions <= 1 {
                return Ok(FastqPartitionStrategy::Sequential);
            }
            let chunk_size = file_size / target_partitions as u64;
            if chunk_size == 0 {
                return Ok(FastqPartitionStrategy::Sequential);
            }
            let mut partitions = Vec::with_capacity(target_partitions);
            for i in 0..target_partitions {
                let start = i as u64 * chunk_size;
                let end = if i == target_partitions - 1 {
                    file_size
                } else {
                    (i as u64 + 1) * chunk_size
                };
                partitions.push((start, end));
            }
            Ok(FastqPartitionStrategy::ByteRange { partitions })
        }
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

fn find_line_end(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|pos| start + pos)
}

fn synchronize_bgzf_reader<R: BufRead>(
    reader: &mut IndexedReader<R>,
    end_comp: u64,
) -> io::Result<()> {
    loop {
        if reader.virtual_position().compressed() >= end_comp {
            return Ok(());
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }

        if let Some(at_pos) = buf.iter().position(|&b| b == b'@') {
            if let Some(l1_end) = find_line_end(buf, at_pos) {
                if let Some(l2_end) = find_line_end(buf, l1_end + 1) {
                    if let Some(l3_start) = l2_end.checked_add(1) {
                        if buf.get(l3_start) == Some(&b'+') {
                            reader.consume(at_pos);
                            return Ok(());
                        }
                    }
                }
            }
            if let Some(end_of_at_line) = find_line_end(buf, at_pos) {
                reader.consume(end_of_at_line + 1);
            } else {
                let len = buf.len();
                reader.consume(len);
            }
        } else {
            let len = buf.len();
            reader.consume(len);
        }
    }
}

/// Synchronize a plain (uncompressed) BufRead reader to a FASTQ record boundary.
fn synchronize_plain_reader<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(());
        }

        if let Some(at_pos) = buf.iter().position(|&b| b == b'@') {
            if let Some(l1_end) = find_line_end(buf, at_pos) {
                if let Some(l2_end) = find_line_end(buf, l1_end + 1) {
                    if let Some(l3_start) = l2_end.checked_add(1) {
                        if buf.get(l3_start) == Some(&b'+') {
                            reader.consume(at_pos);
                            return Ok(());
                        }
                    }
                }
            }
            if let Some(end_of_at_line) = find_line_end(buf, at_pos) {
                reader.consume(end_of_at_line + 1);
            } else {
                let len = buf.len();
                reader.consume(len);
            }
        } else {
            let len = buf.len();
            reader.consume(len);
        }
    }
}

#[allow(dead_code)]
pub struct FastqExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) strategy: FastqPartitionStrategy,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
}

impl Debug for FastqExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastqExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for FastqExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let proj_str = match &self.projection {
            Some(_) => {
                let col_names: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                col_names.join(", ")
            }
            None => "*".to_string(),
        };
        write!(f, "FastqExec: projection=[{proj_str}]")
    }
}

impl ExecutionPlan for FastqExec {
    fn name(&self) -> &str {
        "FastqExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let proj_cols = match &self.projection {
            Some(_) => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };
        info!(
            "{}: executing partition={} with projection=[{}]",
            self.name(),
            partition,
            proj_cols
        );
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        match &self.strategy {
            FastqPartitionStrategy::Bgzf {
                partitions, index, ..
            } => {
                let (start_uncomp, end_comp) = partitions[partition];
                execute_bgzf_partition(
                    self.file_path.clone(),
                    schema,
                    self.projection.clone(),
                    index.clone(),
                    start_uncomp,
                    end_comp,
                    self.limit,
                    batch_size,
                    partition,
                )
            }
            FastqPartitionStrategy::ByteRange { partitions } => {
                let (start_byte, end_byte) = partitions[partition];
                execute_byte_range_partition(
                    self.file_path.clone(),
                    schema,
                    self.projection.clone(),
                    start_byte,
                    end_byte,
                    self.limit,
                    batch_size,
                    partition,
                )
            }
            FastqPartitionStrategy::Sequential => {
                let fut = get_stream(
                    self.file_path.clone(),
                    schema.clone(),
                    batch_size,
                    self.projection.clone(),
                    self.object_storage_options.clone(),
                );
                let stream = futures::stream::once(fut).try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }
}

/// Read FASTQ records from a reader, build Arrow RecordBatches, and send them
/// through an mpsc channel. This is the shared core loop used by both BGZF and
/// byte-range partition strategies.
///
/// # Arguments
///
/// * `fastq_reader` - A FASTQ reader already positioned at the first record to read
/// * `is_past_end` - Closure that returns `true` when the reader has passed the partition boundary
/// * `schema` - Arrow schema for the output batches
/// * `projection` - Optional column projection indices
/// * `limit` - Optional row limit
/// * `batch_size` - Maximum rows per RecordBatch
/// * `tx` - Channel sender for dispatching completed batches
fn read_and_send_batches<R: BufRead>(
    fastq_reader: &mut fastq::io::Reader<R>,
    is_past_end: &mut dyn FnMut(&mut fastq::io::Reader<R>) -> bool,
    schema: &SchemaRef,
    projection: &Option<Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
    tx: &mut mpsc::Sender<(Result<RecordBatch, ArrowError>, usize)>,
) -> Result<(), ArrowError> {
    let mut record = fastq::Record::default();
    let mut total_records = 0;

    // Fast path for empty projection (COUNT(*) queries)
    if let Some(proj) = projection {
        if proj.is_empty() {
            let mut num_rows = 0;
            loop {
                if limit.is_some_and(|l| num_rows >= l) {
                    break;
                }
                if is_past_end(fastq_reader) {
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
            let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?;
            tx.try_send((Ok(batch), num_rows)).ok();
            return Ok(());
        }
    }

    loop {
        if limit.is_some_and(|l| total_records >= l) {
            break;
        }

        let proj_indices = projection.as_ref();
        let mut names = proj_indices
            .is_none_or(|p| p.contains(&0))
            .then(StringBuilder::new);
        let mut descriptions = proj_indices
            .is_none_or(|p| p.contains(&1))
            .then(StringBuilder::new);
        let mut sequences = proj_indices
            .is_none_or(|p| p.contains(&2))
            .then(StringBuilder::new);
        let mut quality_scores = proj_indices
            .is_none_or(|p| p.contains(&3))
            .then(StringBuilder::new);

        let mut count = 0;
        while count < batch_size {
            if limit.is_some_and(|l| total_records >= l) {
                break;
            }

            if is_past_end(fastq_reader) {
                break;
            }

            match fastq_reader.read_record(&mut record) {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(b) = &mut names {
                        b.append_value(std::str::from_utf8(record.name()).unwrap());
                    }
                    if let Some(b) = &mut descriptions {
                        if record.description().is_empty() {
                            b.append_null();
                        } else {
                            b.append_value(std::str::from_utf8(record.description()).unwrap());
                        }
                    }
                    if let Some(b) = &mut sequences {
                        b.append_value(std::str::from_utf8(record.sequence()).unwrap());
                    }
                    if let Some(b) = &mut quality_scores {
                        b.append_value(std::str::from_utf8(record.quality_scores()).unwrap());
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
        if let Some(proj) = projection {
            for &col_idx in proj.iter() {
                match col_idx {
                    0 => arrays.push(Arc::new(names.as_mut().unwrap().finish())),
                    1 => arrays.push(Arc::new(descriptions.as_mut().unwrap().finish())),
                    2 => arrays.push(Arc::new(sequences.as_mut().unwrap().finish())),
                    3 => arrays.push(Arc::new(quality_scores.as_mut().unwrap().finish())),
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
        let mut msg = (Ok(batch), num_rows);
        loop {
            match tx.try_send(msg) {
                Ok(()) => break,
                Err(e) if e.is_disconnected() => return Ok(()),
                Err(e) => {
                    msg = e.into_inner();
                    thread::yield_now();
                }
            }
        }
    }
    Ok(())
}

/// Execute a BGZF partition using a background thread + mpsc channel.
#[allow(clippy::too_many_arguments)]
fn execute_bgzf_partition(
    path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    index: gzi::Index,
    start_uncomp: u64,
    end_comp: u64,
    limit: Option<usize>,
    batch_size: usize,
    partition: usize,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    let (mut tx, rx) =
        mpsc::channel::<(Result<RecordBatch, ArrowError>, usize)>(PARTITION_CHANNEL_BUFFER);

    let schema_clone = schema.clone();
    let _handle = thread::spawn(move || {
        let read_and_send = || -> Result<(), ArrowError> {
            let file =
                std::fs::File::open(&path).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            let mut reader = IndexedReader::new(BufReader::new(file), index);

            reader
                .seek(SeekFrom::Start(start_uncomp))
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            if start_uncomp > 0 {
                synchronize_bgzf_reader(&mut reader, end_comp)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            }

            let mut fastq_reader = fastq::io::Reader::new(reader);
            let mut is_past_end =
                |r: &mut fastq::io::Reader<IndexedReader<BufReader<std::fs::File>>>| {
                    r.get_ref().virtual_position().compressed() >= end_comp
                };

            read_and_send_batches(
                &mut fastq_reader,
                &mut is_past_end,
                &schema_clone,
                &projection,
                limit,
                batch_size,
                &mut tx,
            )
        };

        if let Err(e) = read_and_send() {
            let _ = tx.try_send((Err(e), 0));
        }
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        rx.map(move |(item, count)| {
            debug!("Partition {partition}: processed {count} rows");
            item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }),
    )))
}

/// Execute a byte-range partition of an uncompressed FASTQ file.
#[allow(clippy::too_many_arguments)]
fn execute_byte_range_partition(
    path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    start_byte: u64,
    end_byte: u64,
    limit: Option<usize>,
    batch_size: usize,
    partition: usize,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    let (mut tx, rx) =
        mpsc::channel::<(Result<RecordBatch, ArrowError>, usize)>(PARTITION_CHANNEL_BUFFER);

    let schema_clone = schema.clone();
    let _handle = thread::spawn(move || {
        let mut read_and_send = || -> Result<(), ArrowError> {
            let file =
                std::fs::File::open(&path).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            let mut reader = BufReader::new(file);

            reader
                .seek(SeekFrom::Start(start_byte))
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            if start_byte > 0 {
                synchronize_plain_reader(&mut reader)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            }

            let mut fastq_reader = fastq::io::Reader::new(reader);
            let mut is_past_end = |r: &mut fastq::io::Reader<BufReader<std::fs::File>>| {
                r.get_mut()
                    .stream_position()
                    .map(|pos| pos >= end_byte)
                    .unwrap_or(true)
            };

            read_and_send_batches(
                &mut fastq_reader,
                &mut is_past_end,
                &schema_clone,
                &projection,
                limit,
                batch_size,
                &mut tx,
            )
        };

        if let Err(e) = read_and_send() {
            let _ = tx.try_send((Err(e), 0));
        }
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        rx.map(move |(item, count)| {
            debug!("Partition {partition}: processed {count} rows");
            item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }),
    )))
}

/// Build a RecordBatch from optional StringBuilder columns.
///
/// Each builder corresponds to a FASTQ field (name=0, description=1, sequence=2,
/// quality_scores=3). Builders are `Some` when the column is projected, `None` otherwise.
/// `row_count` is used for empty-projection batches (COUNT(*) queries) where no arrays
/// are produced but the batch must report the correct number of rows.
fn build_batch_from_builders(
    schema: &SchemaRef,
    projection: &Option<Vec<usize>>,
    names: &mut Option<StringBuilder>,
    descriptions: &mut Option<StringBuilder>,
    sequences: &mut Option<StringBuilder>,
    quality_scores: &mut Option<StringBuilder>,
    row_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    if let Some(proj) = projection {
        if proj.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(row_count));
            return Ok(RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &options,
            )?);
        }
        for &col_idx in proj {
            match col_idx {
                0 => arrays.push(Arc::new(names.as_mut().unwrap().finish())),
                1 => arrays.push(Arc::new(descriptions.as_mut().unwrap().finish())),
                2 => arrays.push(Arc::new(sequences.as_mut().unwrap().finish())),
                3 => arrays.push(Arc::new(quality_scores.as_mut().unwrap().finish())),
                _ => unreachable!(),
            }
        }
    } else {
        arrays.push(Arc::new(names.as_mut().unwrap().finish()));
        arrays.push(Arc::new(descriptions.as_mut().unwrap().finish()));
        arrays.push(Arc::new(sequences.as_mut().unwrap().finish()));
        arrays.push(Arc::new(quality_scores.as_mut().unwrap().finish()));
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

async fn get_remote_fastq_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader =
        FastqRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    let stream = try_stream! {
        let proj_indices = projection.as_ref();
        let mut records = reader.read_records().await;
        let mut record_num = 0usize;
        let mut batch_num = 0usize;

        loop {
            let mut names = proj_indices.is_none_or(|p| p.contains(&0)).then(StringBuilder::new);
            let mut descriptions = proj_indices.is_none_or(|p| p.contains(&1)).then(StringBuilder::new);
            let mut sequences = proj_indices.is_none_or(|p| p.contains(&2)).then(StringBuilder::new);
            let mut quality_scores = proj_indices.is_none_or(|p| p.contains(&3)).then(StringBuilder::new);

            let mut count = 0;
            while count < batch_size {
                match records.next().await {
                    Some(Ok(record)) => {
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
                        record_num += 1;
                    }
                    Some(Err(e)) => Err(DataFusionError::External(Box::new(e)))?,
                    None => break,
                }
            }

            if count == 0 {
                break;
            }

            let batch = build_batch_from_builders(
                &schema, &projection,
                &mut names, &mut descriptions, &mut sequences, &mut quality_scores,
                count,
            )?;
            batch_num += 1;
            debug!("Batch {batch_num}: {count} records (total: {record_num})");
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_fastq(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader = FastqLocalReader::new(file_path, object_storage_options).await?;

    let stream = try_stream! {
        let proj_indices = projection.as_ref();
        let mut records = reader.read_records().await;
        let mut record_num = 0usize;
        let mut batch_num = 0usize;

        loop {
            let mut names = proj_indices.is_none_or(|p| p.contains(&0)).then(StringBuilder::new);
            let mut descriptions = proj_indices.is_none_or(|p| p.contains(&1)).then(StringBuilder::new);
            let mut sequences = proj_indices.is_none_or(|p| p.contains(&2)).then(StringBuilder::new);
            let mut quality_scores = proj_indices.is_none_or(|p| p.contains(&3)).then(StringBuilder::new);

            let mut count = 0;
            while count < batch_size {
                match records.next().await {
                    Some(Ok(record)) => {
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
                        record_num += 1;
                    }
                    Some(Err(e)) => Err(DataFusionError::External(Box::new(e)))?,
                    None => break,
                }
            }

            if count == 0 {
                break;
            }

            let batch = build_batch_from_builders(
                &schema, &projection,
                &mut names, &mut descriptions, &mut sequences, &mut quality_scores,
                count,
            )?;
            batch_num += 1;
            debug!("Batch {batch_num}: {count} records (total: {record_num})");
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let store_type = get_storage_type(file_path.clone());

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_fastq(
                file_path,
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_fastq_stream(
                file_path,
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}
