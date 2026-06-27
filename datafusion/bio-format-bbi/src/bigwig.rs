//! BigWig DataFusion table provider.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bigtools::BigWigRead;
use bigtools::utils::reopen::ReopenableFile;
use datafusion::arrow::array::{ArrayRef, Float32Array, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::genomic_filter::is_genomic_coordinate_filter;
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;

use crate::common::{
    BbiScanRegion, build_batch, normalize_local_path, plan_bbi_scan_regions, project_schema,
    projected_indices, projection_display, region_display, to_external_error,
};

/// Table provider for local BigWig files.
#[derive(Clone, Debug)]
pub struct BigWigTableProvider {
    file_path: String,
    schema: SchemaRef,
    chroms: Vec<(String, u32)>,
    coordinate_system_zero_based: bool,
}

impl BigWigTableProvider {
    /// Create a BigWig provider for a local file path.
    pub fn new(file_path: String, coordinate_system_zero_based: bool) -> Result<Self> {
        let file_path = normalize_local_path(&file_path, "BigWig")?;
        let reader = BigWigRead::open_file(&file_path).map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to open BigWig file '{file_path}': {error}"
            ))))
        })?;
        let chroms = reader
            .chroms()
            .iter()
            .map(|chrom| (chrom.name.clone(), chrom.length))
            .collect::<Vec<_>>();
        let schema = bigwig_schema(coordinate_system_zero_based);
        Ok(Self {
            file_path,
            schema,
            chroms,
            coordinate_system_zero_based,
        })
    }
}

#[async_trait]
impl TableProvider for BigWigTableProvider {
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
        Ok(filters
            .iter()
            .map(|expr| {
                if is_genomic_coordinate_filter(expr)
                    || can_push_down_record_filter(expr, &self.schema)
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // `_limit` is intentionally ignored: BBI scans have no cheap row cap, so
        // DataFusion applies the LIMIT in a `GlobalLimitExec` above this node.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection);
        let regions =
            plan_bbi_scan_regions(filters, &self.chroms, self.coordinate_system_zero_based);
        Ok(Arc::new(BigWigExec {
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            regions,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

/// Physical execution plan for BigWig scans.
pub struct BigWigExec {
    file_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    regions: Vec<BbiScanRegion>,
    coordinate_system_zero_based: bool,
    cache: Arc<PlanProperties>,
}

impl Debug for BigWigExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigWigExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BigWigExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BigWigExec: projection=[{}], regions=[{}]",
            projection_display(&self.schema),
            region_display(&self.regions)
        )
    }
}

impl ExecutionPlan for BigWigExec {
    fn name(&self) -> &str {
        "BigWigExec"
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let reader = BigWigRead::open_file(&self.file_path).map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to open BigWig file '{}': {error}",
                self.file_path
            ))))
        })?;
        // Emit one RecordBatch per scan region (one per chromosome when there is
        // no genomic filter) so memory stays bounded by a single region instead
        // of materializing the whole file up front.
        let stream = futures_util::stream::iter(BigWigRegionStream {
            reader,
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            regions: self.regions.clone().into_iter(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Lazily yields one [`RecordBatch`] per scan region from an open BigWig reader.
struct BigWigRegionStream {
    reader: BigWigRead<ReopenableFile>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    regions: std::vec::IntoIter<BbiScanRegion>,
    coordinate_system_zero_based: bool,
}

impl BigWigRegionStream {
    fn build_region_batch(&mut self, region: &BbiScanRegion) -> Result<RecordBatch> {
        let iter = self
            .reader
            .get_interval(&region.chrom, region.start, region.end)
            .map_err(to_external_error)?;

        let mut rows = Vec::new();
        for value in iter {
            let value = value.map_err(to_external_error)?;
            let start = if self.coordinate_system_zero_based {
                value.start
            } else {
                value.start + 1
            };
            rows.push((start, value.end, value.value));
        }

        let row_count = rows.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for index in projected_indices(self.projection.as_deref(), 4) {
            match index {
                0 => arrays.push(Arc::new(StringArray::from_iter_values(
                    std::iter::repeat_n(region.chrom.as_str(), row_count),
                ))),
                1 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|row| row.0),
                ))),
                2 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|row| row.1),
                ))),
                3 => arrays.push(Arc::new(Float32Array::from_iter_values(
                    rows.iter().map(|row| row.2),
                ))),
                _ => unreachable!("BigWig projection contains invalid column index"),
            }
        }

        build_batch(self.schema.clone(), arrays, row_count)
    }
}

impl Iterator for BigWigRegionStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let region = self.regions.next()?;
        Some(self.build_region_batch(&region))
    }
}

fn bigwig_schema(coordinate_system_zero_based: bool) -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("value", DataType::Float32, false),
        ],
        metadata,
    ))
}
