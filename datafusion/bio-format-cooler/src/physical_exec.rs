//! Physical execution plan for cooler pixel scans.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use hdf5_metno::{Dataset, File};

use crate::collection::BinData;
use crate::hdf5_utils::{h5_err, read_numeric_slice};

pub(crate) const COOLER_BATCH_ROWS: usize = 8192;

/// Physical plan streaming pixel row ranges as record batches. Each partition
/// owns a list of disjoint row ranges (pruning can produce several per
/// partition).
pub struct CoolerExec {
    pub(crate) file_path: String,
    pub(crate) group_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) partitions: Vec<Vec<(usize, usize)>>,
    pub(crate) count_is_float: bool,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) bins: Option<Arc<BinData>>,
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
            count_is_float: self.count_is_float,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            bins: self.bins.clone(),
            datasets: None,
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Lazily opened handles to the pixel datasets a projection needs.
struct PixelDatasets {
    bin1: Option<Dataset>,
    bin2: Option<Dataset>,
    count: Option<Dataset>,
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
    count_is_float: bool,
    coordinate_system_zero_based: bool,
    bins: Option<Arc<BinData>>,
    datasets: Option<PixelDatasets>,
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

    fn open_datasets(&mut self) -> Result<()> {
        if self.datasets.is_some() {
            return Ok(());
        }
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
        let open = |name: &str| {
            pixels
                .dataset(name)
                .map_err(|error| h5_err(&format!("Failed to open pixels/{name}"), error))
        };
        self.datasets = Some(PixelDatasets {
            bin1: if self.needs('1') {
                Some(open("bin1_id")?)
            } else {
                None
            },
            bin2: if self.needs('2') {
                Some(open("bin2_id")?)
            } else {
                None
            },
            count: if self.needs('c') {
                Some(open("count")?)
            } else {
                None
            },
        });
        Ok(())
    }

    fn build_batch(&mut self, lo: usize, hi: usize) -> Result<RecordBatch> {
        self.open_datasets()?;
        let datasets = self.datasets.as_ref().expect("datasets opened");
        let bin1 = match &datasets.bin1 {
            Some(ds) => read_numeric_slice::<i64>(ds, lo, hi, "pixels/bin1_id")?,
            None => Vec::new(),
        };
        let bin2 = match &datasets.bin2 {
            Some(ds) => read_numeric_slice::<i64>(ds, lo, hi, "pixels/bin2_id")?,
            None => Vec::new(),
        };
        let (count_int, count_float) = match &datasets.count {
            Some(ds) if self.count_is_float => (
                Vec::new(),
                read_numeric_slice::<f64>(ds, lo, hi, "pixels/count")?,
            ),
            Some(ds) => (
                read_numeric_slice::<i32>(ds, lo, hi, "pixels/count")?,
                Vec::new(),
            ),
            None => (Vec::new(), Vec::new()),
        };

        let start_offset: u32 = if self.coordinate_system_zero_based {
            0
        } else {
            1
        };
        let bins = self.bins.as_deref();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let array: ArrayRef = match field.name().as_str() {
                "bin1_id" => Arc::new(Int64Array::from_iter_values(bin1.iter().copied())),
                "bin2_id" => Arc::new(Int64Array::from_iter_values(bin2.iter().copied())),
                "count" if self.count_is_float => {
                    Arc::new(Float64Array::from_iter_values(count_float.iter().copied()))
                }
                "count" => Arc::new(Int32Array::from_iter_values(count_int.iter().copied())),
                name => {
                    let bins = bins.ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "CoolerExec column {name} requires the bins table, which was not loaded"
                        ))
                    })?;
                    let ids = if name.ends_with('1') { &bin1 } else { &bin2 };
                    joined_column(name, ids, bins, start_offset)?
                }
            };
            arrays.push(array);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(hi - lo));
        RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn joined_column(name: &str, ids: &[i64], bins: &BinData, start_offset: u32) -> Result<ArrayRef> {
    let lookup = |id: &i64, table: &[i32]| -> i32 { table[*id as usize] };
    let array: ArrayRef = match name {
        "chrom1" | "chrom2" => {
            Arc::new(StringArray::from_iter_values(ids.iter().map(|id| {
                bins.chrom_names[bins.chrom_idx[*id as usize] as usize].as_str()
            })))
        }
        "start1" | "start2" => Arc::new(UInt32Array::from_iter_values(
            ids.iter()
                .map(|id| lookup(id, &bins.start) as u32 + start_offset),
        )),
        "end1" | "end2" => Arc::new(UInt32Array::from_iter_values(
            ids.iter().map(|id| lookup(id, &bins.end) as u32),
        )),
        "weight1" | "weight2" => {
            let weights = bins.weight.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "CoolerExec weight column requires bins/weight, which was not loaded"
                        .to_string(),
                )
            })?;
            Arc::new(Float64Array::from_iter_values(
                ids.iter().map(|id| weights[*id as usize]),
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
