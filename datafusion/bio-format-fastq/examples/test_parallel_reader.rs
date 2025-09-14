// //! DataFusion TableProvider for parallel BGZF-compressed FASTQ files
// //!
// //! Supports controlling parallelism via DataFusion's `target_partitions` configuration.
//
// use crate::bgzf::Reader;
// use crate::bgzf::Reader;
// use std::io::Read;
// use std::{fs::File, io, sync::Arc};
// use std::fmt::{Debug, Formatter};
// use std::path::PathBuf;
// use datafusion::arrow::array::{RecordBatch, StringArray};
// use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
//
// use datafusion::catalog::{Session, TableProvider};
// use datafusion::execution::context::SessionState;
// use datafusion::logical_expr::{Expr, TableType};
// use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream};
//
// use noodles_bgzf::{self as bgzf, gzi, VirtualPosition};
// use noodles_fastq as fastq;
//
// /// A TableProvider that reads BGZF-compressed FASTQ files in parallel.
// ///
// /// Parallelism is controlled by `SessionConfig.target_partitions`.
// pub struct BgzfFastqTable {
//     path: PathBuf,
//     schema: SchemaRef,
// }
//
// impl BgzfFastqTable {
//     /// Create a new provider for the given BGZF FASTQ file.
//     pub fn try_new(path: impl Into<PathBuf>) -> io::Result<Self> {
//         let schema = Schema::new(vec![
//             Field::new("id", DataType::Utf8, false),
//             Field::new("sequence", DataType::Utf8, false),
//             Field::new("quality", DataType::Utf8, false),
//         ]);
//         Ok(Self { path: path.into(), schema: Arc::new(schema) })
//     }
// }
//
// impl Debug for BgzfFastqTable {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// impl TableProvider for BgzfFastqTable {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
//
//     fn schema(&self) -> SchemaRef {
//         Arc::clone(&self.schema)
//     }
//
//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }
//
//     fn scan(
//         &self,
//         state: &SessionState,
//         _projection: &Option<Vec<usize>>,
//         _filters: &[Expr],
//         _limit: Option<usize>,
//     ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
//         // Discover BGZF block offsets by scanning the file
//         let file = File::open(&self.path)?;
//         let mut bgzf_reader = bgzf::Reader::new(file);
//
//         let blocks: Vec<(u64, u64)> = match bgzf::indexed_reader::read(&mut bgzf_reader) {
//             Ok(index) => index.iter().map(|blk| (blk.start(), blk.end())).collect(),
//             Err(_) => {
//                 // Fallback: manually split based on virtual positions of each BGZF block
//                 let mut positions = vec![];
//                 let mut offset = 0;
//                 while let Ok(Some(_)) = bgzf_reader.read_block() {
//                     let start = offset;
//                     offset = bgzf_reader.virtual_position().compressed();
//                     positions.push((start, offset));
//                 }
//                 positions
//             }
//         };
//
//         // Determine desired parallelism
//         let target = state.config().options_mut().with_target_partitions(blocks.len())
//         let chunk_size = (blocks.len() + target - 1) / target;
//
//         // Group blocks into partitions
//         let partitions: Vec<Vec<(u64, u64)>> = blocks.chunks(chunk_size)
//             .map(|chunk| chunk.to_vec())
//             .collect();
//
//         let exec = BgzfFastqExec::new(self.path.clone(), partitions, Arc::clone(&self.schema));
//         Ok(Arc::new(exec))
//     }
// }
//
// /// Execution plan reading BGZF FASTQ partitions.
// struct BgzfFastqExec {
//     path: PathBuf,
//     partitions: Vec<Vec<(u64, u64)>>,
//     schema: SchemaRef,
// }
//
// impl BgzfFastqExec {
//     pub fn new(path: PathBuf, partitions: Vec<Vec<(u64, u64)>>, schema: SchemaRef) -> Self {
//         Self { path, partitions, schema }
//     }
// }
//
// impl Debug for BgzfFastqExec {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// impl DisplayAs for BgzfFastqExec {
//     fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// #[async_trait::async_trait]
// impl ExecutionPlan for BgzfFastqExec {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
//
//     fn schema(&self) -> SchemaRef {
//         Arc::clone(&self.schema)
//     }
//
//
//     fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
//         vec![]
//     }
//
//     fn with_new_children(
//         self: Arc<Self>,
//         _children: Vec<Arc<dyn ExecutionPlan>>,
//     ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
//         Ok(self)
//     }

// async fn execute(
//             &self,
//             partition: usize,
//             _context: Arc<SessionState>,
//         ) -> datafusion::error::Result<SendableRecordBatchStream> {
//             let ranges = &self.partitions[partition];
//             let mut ids = Vec::new();
//             let mut seqs = Vec::new();
//             let mut quals = Vec::new();
//             let mut record = fastq::Record::default();
//
//             // Each sub-range is processed in parallel within this partition
//             ranges.iter().for_each(|&(start, end)| {
//                 let file = File::open(&self.path).unwrap();
//                 let mut reader = bgzf::Reader::new(file);
//                 reader.seek(VirtualPosition::from(start)).unwrap();
//                 let mut bgzf_slice = bgzf::Reader::new(reader.take(end - start));
//                 let mut fq = fastq::r#async::io::Reader::new(&mut bgzf_slice);
//
//                 while let Ok(Some(_)) = fq.read(&mut record) {
//                     ids.push(record.id().to_string());
//                     seqs.push(record.sequence().to_string());
//                     quals.push(record.quality_scores().to_string());
//                 }
//             });
//
//             // Build Arrow arrays and a single RecordBatch
//             let batch = RecordBatch::try_new(
//                 Arc::clone(&self.schema),
//                 vec![
//                     Arc::new(StringArray::from(ids)),
//                     Arc::new(StringArray::from(seqs)),
//                     Arc::new(StringArray::from(quals)),
//                 ],
//             )?;
//
//             Ok(Box::pin(datafusion::physical_plan::memory::MemoryStream::try_new(
//                 vec![batch],
//                 Arc::clone(&self.schema),
//                 None,
//             )?))
//         }
//
//     fn name(&self) -> &str {
//         todo!()
//     }
//
//     fn properties(&self) -> &PlanProperties {
//         todo!()
//     }
// }

use noodles::bgzf;
use noodles_bgzf::gzi;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, SeekFrom};

use noodles_bgzf::gzi::Index;
use noodles_bgzf::virtual_position::VirtualPosition;
use noodles_fastq as fastq;
use std::io::Seek;

const BLOCK_SIZE: usize = 64 * 1024; // 64 KiB typical BGZF block

// Index of BGZF block boundaries as virtual positions
// (compressed_offset, uncompressed_offset_within_block)
type BlockIndex = Vec<VirtualPosition>;

fn get_bgzf_block_offsets(index: Index, thread_num: usize) -> Vec<(u64, u64)> {
    let mut blocks = index.as_ref().to_vec();
    blocks.insert(0, (0, 0)); // Add a dummy block for the start
    let total_blocks = blocks.len();
    if total_blocks == 0 {
        return Vec::new();
    }

    // Determine how many blocks per thread (rounding up)
    let chunk_size = (total_blocks + thread_num - 1) / thread_num;
    let mut ranges = Vec::with_capacity(thread_num.min(total_blocks));

    for chunk_idx in 0..thread_num {
        let start_idx = chunk_idx * chunk_size;
        if start_idx >= total_blocks {
            break;
        }
        let end_idx = ((chunk_idx + 1) * chunk_size).min(total_blocks) - 1;
        let (_, start_uncomp) = blocks[start_idx];
        let (end_comp, _) = blocks[end_idx];
        ranges.push((start_uncomp, end_comp));
    }

    ranges
}

fn skip_to_next_fastq_record<R: BufRead>(reader: &mut R) -> io::Result<()> {
    // When reading a parallelized BGZF stream, a partition may not start at a
    // FASTQ record boundary. This function seeks to the next valid record.
    //
    // A FASTQ record has four lines:
    // 1. ID line, starting with `@`
    // 2. Sequence
    // 3. Separator, starting with `+`
    // 4. Quality scores (same length as sequence)
    //
    // This function first seeks to the next newline to align with a line
    // boundary, then iterates through lines until it finds one that starts
    // with `@` and is followed by what appears to be a valid FASTQ record.

    // First, align to the next line boundary.
    let buf = reader.fill_buf()?;
    if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
        reader.consume(pos + 1);
    } else {
        // No newline in buffer, consume all.
        let len = buf.len();
        reader.consume(len);
        // If we're at EOF, the next fill_buf will be empty.
    }

    // Now, find a valid record start.
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            // EOF
            return Ok(());
        }

        if buf.starts_with(b"@") {
            // This line starts with '@', a potential record. Let's validate.
            let mut lines = buf.splitn(5, |&b| b == b'\n');
            let l1 = lines.next();
            let l2 = lines.next();
            let l3 = lines.next();
            let l4 = lines.next();

            if let (Some(line1), Some(line2), Some(line3), Some(line4)) = (l1, l2, l3, l4) {
                if line1.starts_with(b"@") && line3.starts_with(b"+") {
                    let seq_len = if line2.ends_with(b"\r") {
                        line2.len() - 1
                    } else {
                        line2.len()
                    };
                    let qual_len = if line4.ends_with(b"\r") {
                        line4.len() - 1
                    } else {
                        line4.len()
                    };

                    if seq_len == qual_len {
                        // This looks like a valid FASTQ record.
                        // The reader is now positioned at the start of it.
                        return Ok(());
                    }
                }
            }
        }

        // If we're here, it's not a valid record start. Consume this line and try the next.
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            reader.consume(pos + 1);
        } else {
            let len = buf.len();
            reader.consume(len);
        }
    }
}

fn process_blocks(
    file_path: String,
    index: Index,
    block_offsets: &[(u64, u64)],
    partition_id: usize,
) -> Vec<(u64, u64)> {
    let results = Vec::new();

    let file = File::open(file_path).expect("Failed to open file");
    let buf_reader = BufReader::new(file);
    let mut reader = bgzf::IndexedReader::new(buf_reader, index);
    let st_uncomp = block_offsets[partition_id].0;
    println!("{}", st_uncomp);
    let _ = reader.seek(SeekFrom::Start(st_uncomp));
    if st_uncomp > 0 {
        // Ensure we start reading from the beginning of the block
        let _ = skip_to_next_fastq_record(&mut reader);
    }
    let mut record = fastq::Record::default();

    let last_block_offset = block_offsets[partition_id].1;
    let mut fq_reader = fastq::io::Reader::new(reader);
    let mut processed_records: u64 = 0;
    let mut failed_records: u64 = 0;
    loop {
        // get virtual position before reading the record
        let comp_offset = fq_reader.get_ref().virtual_position().compressed();
        match fq_reader.read_record(&mut record) {
            Ok(0) => break,
            Ok(_) => {
                // reached block boundary
                if comp_offset > last_block_offset {
                    break;
                }
                // `record` is now populated
                // println!("{}", vp.compressed());
                processed_records += 1;
            }
            Err(err) => {
                failed_records += 1;
                // eprintln!("Error reading record at compressed offset {}: {}", comp_offset, err);
                // Handle the error as needed, e.g., log it or skip the record
            }
        }
    }
    println!("Records processed: {}", processed_records);
    println!("Records failed: {}", failed_records);
    results
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let index_path = "/tmp/sample_parallel.fastq.bgz.gzi".to_string();
    let file_path = "/tmp/sample_parallel.fastq.bgz".to_string();

    let index: gzi::Index = gzi::fs::read(&index_path)?;
    let blocks = index.as_ref(); // &[(compressed, uncompressed)]
    let total_blocks = blocks.len();
    let num_threads = 4;
    let blocks = get_bgzf_block_offsets(index.clone(), num_threads);
    println!("{:?}", blocks);
    println!("Total blocks in GZI index: {}", total_blocks);
    for (compressed, uncompressed) in index.as_ref() {
        println!(
            "block at compressed byte {} maps to uncompressed byte {}",
            compressed, uncompressed
        );
    }
    process_blocks(file_path.clone(), index.clone(), &blocks, 0);
    process_blocks(file_path.clone(), index.clone(), &blocks, 1);
    process_blocks(file_path.clone(), index.clone(), &blocks, 2);
    process_blocks(file_path.clone(), index.clone(), &blocks, 3);
    Ok(())
}
