//! Physical execution plan for cooler pixel scans.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use hdf5_metno::{Dataset, File};

use crate::collection::{BinData, CountType};
use crate::fast_chunk::{
    ChunkReader, ChunkedColumn, FastPixels, bytes_to_f64, bytes_to_i32, bytes_to_i64, bytes_to_u32,
    bytes_to_u64,
};
use crate::hdf5_utils::{h5_err, read_numeric_slice};

// Large batches amortize per-read overhead (reader setup, chunk-cache probes,
// array construction); 128Ki rows of the widest joined batch stay ~5 MB.
pub(crate) const COOLER_BATCH_ROWS: usize = 131_072;

/// Physical plan streaming pixel row ranges as record batches. Each partition
/// owns a list of disjoint row ranges (pruning can produce several per
/// partition).
pub struct CoolerExec {
    pub(crate) file_path: String,
    pub(crate) group_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) partitions: Vec<Vec<(usize, usize)>>,
    pub(crate) count_type: CountType,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) bins: Option<Arc<BinData>>,
    pub(crate) fast: Arc<FastPixels>,
    pub(crate) cache: Arc<PlanProperties>,
}

impl Debug for CoolerExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoolerExec")
            .field("group_path", &self.group_path)
            .field("partitions", &self.partitions)
            .finish()
    }
}

impl DisplayAs for CoolerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let projection = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "CoolerExec: projection=[{projection}], group={}, partitions={}, rows={}",
            self.group_path,
            self.partitions.len(),
            self.partitions
                .iter()
                .flatten()
                .map(|(lo, hi)| hi - lo)
                .sum::<usize>()
        )
    }
}

impl ExecutionPlan for CoolerExec {
    fn name(&self) -> &str {
        "CoolerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
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
        let ranges = self.partitions.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "CoolerExec partition {partition} is out of range for {} partitions",
                self.partitions.len()
            ))
        })?;
        let stream = futures_util::stream::iter(CoolerPixelStream {
            file_path: self.file_path.clone(),
            group_path: self.group_path.clone(),
            schema: self.schema.clone(),
            ranges: ranges.clone().into_iter(),
            cursor: 0,
            row_hi: 0,
            count_type: self.count_type,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            bins: self.bins.clone(),
            fast: self.fast.clone(),
            sources: None,
            chunk_reader: None,
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Where one pixel column's values come from: the direct-chunk fast path or
/// an ordinary (lock-serialized) hdf5 dataset read.
enum ColumnSource {
    Fast(Arc<ChunkedColumn>),
    H5(Dataset),
}

/// Decoded `count` values in their stored width.
enum CountValues {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float64(Vec<f64>),
}

/// Lazily resolved per-column sources for the projection's needs.
struct PixelSources {
    bin1: Option<ColumnSource>,
    bin2: Option<ColumnSource>,
    count: Option<ColumnSource>,
}

/// Synchronous iterator yielding fixed-size record batches across a
/// partition's disjoint pixel row ranges. HDF5 reads happen per batch; nothing
/// larger than one batch (plus the shared bins table) is ever materialized.
struct CoolerPixelStream {
    file_path: String,
    group_path: String,
    schema: SchemaRef,
    ranges: std::vec::IntoIter<(usize, usize)>,
    cursor: usize,
    row_hi: usize,
    count_type: CountType,
    coordinate_system_zero_based: bool,
    bins: Option<Arc<BinData>>,
    fast: Arc<FastPixels>,
    sources: Option<PixelSources>,
    chunk_reader: Option<ChunkReader>,
}

impl CoolerPixelStream {
    fn needs(&self, side: char) -> bool {
        self.schema.fields().iter().any(|field| {
            let name = field.name();
            match side {
                '1' => matches!(
                    name.as_str(),
                    "chrom1" | "start1" | "end1" | "weight1" | "bin1_id"
                ),
                '2' => matches!(
                    name.as_str(),
                    "chrom2" | "start2" | "end2" | "weight2" | "bin2_id"
                ),
                _ => name == "count",
            }
        })
    }

    fn open_sources(&mut self) -> Result<()> {
        if self.sources.is_some() {
            return Ok(());
        }
        let needed = [
            (self.needs('1'), "bin1_id", self.fast.bin1.clone()),
            (self.needs('2'), "bin2_id", self.fast.bin2.clone()),
            (self.needs('c'), "count", self.fast.count.clone()),
        ];
        // The hdf5 file/group is only opened when some needed column lacks a
        // fast index; all-fast streams never touch libhdf5 at execution time.
        let mut h5_pixels = None;
        let mut resolve = |wanted: bool,
                           name: &str,
                           fast: Option<Arc<ChunkedColumn>>|
         -> Result<Option<ColumnSource>> {
            if !wanted {
                return Ok(None);
            }
            if let Some(column) = fast {
                return Ok(Some(ColumnSource::Fast(column)));
            }
            if h5_pixels.is_none() {
                let file = File::open(&self.file_path).map_err(|error| {
                    h5_err(
                        &format!("Failed to open cooler file '{}'", self.file_path),
                        error,
                    )
                })?;
                let pixels = file
                    .group(&self.group_path)
                    .and_then(|group| group.group("pixels"))
                    .map_err(|error| h5_err("Failed to open pixels group", error))?;
                h5_pixels = Some(pixels);
            }
            let dataset = h5_pixels
                .as_ref()
                .expect("pixels group opened")
                .dataset(name)
                .map_err(|error| h5_err(&format!("Failed to open pixels/{name}"), error))?;
            Ok(Some(ColumnSource::H5(dataset)))
        };
        let [bin1, bin2, count] = needed;
        self.sources = Some(PixelSources {
            bin1: resolve(bin1.0, bin1.1, bin1.2)?,
            bin2: resolve(bin2.0, bin2.1, bin2.2)?,
            count: resolve(count.0, count.1, count.2)?,
        });
        Ok(())
    }

    fn build_batch(&mut self, lo: usize, hi: usize) -> Result<RecordBatch> {
        self.open_sources()?;
        let sources = self.sources.as_ref().expect("sources resolved");
        let mut scratch: Vec<u8> = Vec::new();
        let bin1 = read_id_column(
            &sources.bin1,
            &mut self.chunk_reader,
            &self.file_path,
            lo,
            hi,
            "pixels/bin1_id",
            &mut scratch,
        )?;
        let bin2 = read_id_column(
            &sources.bin2,
            &mut self.chunk_reader,
            &self.file_path,
            lo,
            hi,
            "pixels/bin2_id",
            &mut scratch,
        )?;
        let count = match &sources.count {
            Some(ColumnSource::Fast(column)) => {
                let bytes = fast_read(
                    &mut self.chunk_reader,
                    &self.file_path,
                    column,
                    lo,
                    hi,
                    &mut scratch,
                )?;
                match self.count_type {
                    CountType::Float64 => CountValues::Float64(bytes_to_f64(bytes)),
                    CountType::Int64 => CountValues::Int64(bytes_to_i64(bytes)),
                    CountType::Int32 => CountValues::Int32(bytes_to_i32(bytes)),
                    CountType::UInt64 => CountValues::UInt64(bytes_to_u64(bytes)),
                    CountType::UInt32 => CountValues::UInt32(bytes_to_u32(bytes)),
                }
            }
            Some(ColumnSource::H5(ds)) => match self.count_type {
                CountType::Float64 => {
                    CountValues::Float64(read_numeric_slice::<f64>(ds, lo, hi, "pixels/count")?)
                }
                CountType::Int64 => {
                    CountValues::Int64(read_numeric_slice::<i64>(ds, lo, hi, "pixels/count")?)
                }
                CountType::Int32 => {
                    CountValues::Int32(read_numeric_slice::<i32>(ds, lo, hi, "pixels/count")?)
                }
                CountType::UInt64 => {
                    CountValues::UInt64(read_numeric_slice::<u64>(ds, lo, hi, "pixels/count")?)
                }
                CountType::UInt32 => {
                    CountValues::UInt32(read_numeric_slice::<u32>(ds, lo, hi, "pixels/count")?)
                }
            },
            None => CountValues::Int32(Vec::new()),
        };

        let start_offset: u32 = if self.coordinate_system_zero_based {
            0
        } else {
            1
        };
        let bins = self.bins.as_deref();
        let bin1_indexes = bins
            .map(|bins| validate_bin_references(&bin1, bins.nbins, "pixels/bin1_id", lo))
            .transpose()?;
        let bin2_indexes = bins
            .map(|bins| validate_bin_references(&bin2, bins.nbins, "pixels/bin2_id", lo))
            .transpose()?;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let array: ArrayRef = match field.name().as_str() {
                "bin1_id" => Arc::new(Int64Array::from_iter_values(bin1.iter().copied())),
                "bin2_id" => Arc::new(Int64Array::from_iter_values(bin2.iter().copied())),
                "count" => match &count {
                    CountValues::Float64(values) => {
                        Arc::new(Float64Array::from_iter_values(values.iter().copied()))
                    }
                    CountValues::Int64(values) => {
                        Arc::new(Int64Array::from_iter_values(values.iter().copied()))
                    }
                    CountValues::Int32(values) => {
                        Arc::new(Int32Array::from_iter_values(values.iter().copied()))
                    }
                    CountValues::UInt64(values) => {
                        Arc::new(UInt64Array::from_iter_values(values.iter().copied()))
                    }
                    CountValues::UInt32(values) => {
                        Arc::new(UInt32Array::from_iter_values(values.iter().copied()))
                    }
                },
                name => {
                    let bins = bins.ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "CoolerExec column {name} requires the bins table, which was not loaded"
                        ))
                    })?;
                    let indexes = if name.ends_with('1') {
                        bin1_indexes.as_deref()
                    } else {
                        bin2_indexes.as_deref()
                    }
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "CoolerExec column {name} requires validated bin references"
                        ))
                    })?;
                    joined_column(name, indexes, bins, start_offset)?
                }
            };
            arrays.push(array);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(hi - lo));
        RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn fast_read<'a>(
    reader: &mut Option<ChunkReader>,
    file_path: &str,
    column: &ChunkedColumn,
    lo: usize,
    hi: usize,
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    if reader.is_none() {
        *reader = Some(ChunkReader::open(file_path)?);
    }
    reader
        .as_mut()
        .expect("chunk reader opened")
        .read_range(column, lo, hi, scratch)?;
    Ok(scratch.as_slice())
}

fn read_id_column(
    source: &Option<ColumnSource>,
    reader: &mut Option<ChunkReader>,
    file_path: &str,
    lo: usize,
    hi: usize,
    what: &str,
    scratch: &mut Vec<u8>,
) -> Result<Vec<i64>> {
    match source {
        Some(ColumnSource::Fast(column)) => Ok(bytes_to_i64(fast_read(
            reader, file_path, column, lo, hi, scratch,
        )?)),
        Some(ColumnSource::H5(ds)) => read_numeric_slice::<i64>(ds, lo, hi, what),
        None => Ok(Vec::new()),
    }
}

fn validate_bin_references(
    ids: &[i64],
    nbins: usize,
    name: &str,
    row_offset: usize,
) -> Result<Vec<usize>> {
    ids.iter()
        .enumerate()
        .map(|(index, &value)| {
            usize::try_from(value)
                .ok()
                .filter(|&value| value < nbins)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "{name}[{}]={value} does not reference one of the {nbins} bins",
                        row_offset + index
                    ))
                })
        })
        .collect()
}

fn joined_column(
    name: &str,
    indexes: &[usize],
    bins: &BinData,
    start_offset: u32,
) -> Result<ArrayRef> {
    let lookup = |&index: &usize, table: &[u32]| -> u32 { table[index] };
    let array: ArrayRef = match name {
        "chrom1" | "chrom2" => Arc::new(StringArray::from_iter_values(
            indexes
                .iter()
                .map(|&index| bins.chrom_names[bins.chrom_idx[index]].as_str()),
        )),
        "start1" | "start2" => Arc::new(UInt32Array::from_iter_values(
            indexes
                .iter()
                .map(|id| {
                    lookup(id, &bins.start)
                        .checked_add(start_offset)
                        .ok_or_else(|| {
                            DataFusionError::Plan(
                                "A 1-based cooler start coordinate exceeds the UInt32 range"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        "end1" | "end2" => Arc::new(UInt32Array::from_iter_values(
            indexes.iter().map(|index| lookup(index, &bins.end)),
        )),
        "weight1" | "weight2" => {
            let weights = bins.weight.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "CoolerExec weight column requires bins/weight, which was not loaded"
                        .to_string(),
                )
            })?;
            Arc::new(Float64Array::from_iter_values(
                indexes.iter().map(|&index| weights[index]),
            ))
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "CoolerExec does not know how to build column {other}"
            )));
        }
    };
    Ok(array)
}

impl Iterator for CoolerPixelStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor >= self.row_hi {
            let (lo, hi) = self.ranges.next()?;
            self.cursor = lo;
            self.row_hi = hi;
        }
        let lo = self.cursor;
        let hi = (lo + COOLER_BATCH_ROWS).min(self.row_hi);
        self.cursor = hi;
        if self.schema.fields().is_empty() {
            // count(*) fast path: row counts come from the row range alone,
            // with no HDF5 pixel reads at all.
            let options = RecordBatchOptions::new().with_row_count(Some(hi - lo));
            return Some(
                RecordBatch::try_new_with_options(self.schema.clone(), Vec::new(), &options)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
            );
        }
        Some(self.build_batch(lo, hi))
    }
}

#[cfg(test)]
mod tests {
    use super::validate_bin_references;

    #[test]
    fn invalid_pixel_bin_references_return_contextual_errors() {
        let negative = validate_bin_references(&[-1], 2, "pixels/bin1_id", 41)
            .unwrap_err()
            .to_string();
        assert!(negative.contains("pixels/bin1_id[41]=-1"), "{negative}");

        let out_of_range = validate_bin_references(&[2], 2, "pixels/bin2_id", 7)
            .unwrap_err()
            .to_string();
        assert!(
            out_of_range.contains("pixels/bin2_id[7]=2"),
            "{out_of_range}"
        );
    }
}
