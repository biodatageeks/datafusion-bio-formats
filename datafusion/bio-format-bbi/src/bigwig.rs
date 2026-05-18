//! BigWig DataFusion table provider.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bigtools::BigWigRead;
use datafusion::arrow::array::{
    ArrayRef, Float32Array, RecordBatch, RecordBatchOptions, StringArray, UInt32Array,
};
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
use datafusion_bio_format_core::genomic_filter::{
    GenomicRegion, extract_genomic_regions, is_genomic_coordinate_filter,
};
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;

/// Native BBI interval query region in 0-based half-open coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BbiScanRegion {
    pub(crate) chrom: String,
    pub(crate) start: u32,
    pub(crate) end: u32,
}

/// Table provider for local BigWig files.
#[derive(Clone, Debug)]
pub struct BigWigTableProvider {
    file_path: String,
    schema: SchemaRef,
    full_schema: SchemaRef,
    chroms: Vec<(String, u32)>,
    coordinate_system_zero_based: bool,
}

impl BigWigTableProvider {
    /// Create a BigWig provider for a local file path.
    pub fn new(file_path: String, coordinate_system_zero_based: bool) -> Result<Self> {
        reject_unsupported_scheme(&file_path, "BigWig")?;
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
        let full_schema = bigwig_schema(coordinate_system_zero_based);
        Ok(Self {
            file_path,
            schema: full_schema.clone(),
            full_schema,
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
                    || can_push_down_record_filter(expr, &self.full_schema)
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.full_schema, projection);
        let regions =
            plan_bbi_scan_regions(filters, &self.chroms, self.coordinate_system_zero_based);
        Ok(Arc::new(BigWigExec {
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            regions,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
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
    cache: PlanProperties,
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

    fn properties(&self) -> &PlanProperties {
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
        let batch = read_bigwig_batch(
            &self.file_path,
            &self.schema,
            self.projection.as_deref(),
            &self.regions,
            self.coordinate_system_zero_based,
        )?;
        let stream = futures_util::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
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

fn read_bigwig_batch(
    file_path: &str,
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    regions: &[BbiScanRegion],
    coordinate_system_zero_based: bool,
) -> Result<RecordBatch> {
    let mut reader = BigWigRead::open_file(file_path).map_err(|error| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to open BigWig file '{file_path}': {error}"
        ))))
    })?;

    let mut rows = Vec::new();
    for region in regions {
        let iter = reader
            .get_interval(&region.chrom, region.start, region.end)
            .map_err(to_external_error)?;
        for value in iter {
            let value = value.map_err(to_external_error)?;
            let start = if coordinate_system_zero_based {
                value.start
            } else {
                value.start + 1
            };
            rows.push((region.chrom.clone(), start, value.end, value.value));
        }
    }

    let row_count = rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for index in projected_indices(projection, 4) {
        match index {
            0 => arrays.push(Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.0.as_str()),
            ))),
            1 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.1),
            ))),
            2 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.2),
            ))),
            3 => arrays.push(Arc::new(Float32Array::from_iter_values(
                rows.iter().map(|row| row.3),
            ))),
            _ => unreachable!("BigWig projection contains invalid column index"),
        }
    }

    build_batch(schema.clone(), arrays, row_count)
}

pub(crate) fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) => Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|&index| schema.field(index).clone())
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        )),
        None => schema.clone(),
    }
}

pub(crate) fn projected_indices(projection: Option<&[usize]>, width: usize) -> Vec<usize> {
    projection
        .map(|indices| indices.to_vec())
        .unwrap_or_else(|| (0..width).collect())
}

pub(crate) fn build_batch(
    schema: SchemaRef,
    arrays: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(schema, arrays, &options)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

pub(crate) fn to_external_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(crate) fn projection_display(schema: &SchemaRef) -> String {
    if schema.fields().is_empty() {
        String::new()
    } else {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub(crate) fn plan_bbi_scan_regions(
    filters: &[Expr],
    chroms: &[(String, u32)],
    coordinate_system_zero_based: bool,
) -> Vec<BbiScanRegion> {
    let analysis = extract_genomic_regions(filters, coordinate_system_zero_based);
    if analysis.unsatisfiable {
        return Vec::new();
    }

    let source_regions = if analysis.regions.is_empty() {
        chroms
            .iter()
            .map(|(chrom, _)| GenomicRegion {
                chrom: chrom.clone(),
                start: None,
                end: None,
                unmapped_tail: false,
            })
            .collect::<Vec<_>>()
    } else {
        analysis.regions
    };

    source_regions
        .into_iter()
        .filter_map(|region| convert_genomic_region_to_bbi(region, chroms))
        .collect()
}

fn convert_genomic_region_to_bbi(
    region: GenomicRegion,
    chroms: &[(String, u32)],
) -> Option<BbiScanRegion> {
    let length = chroms
        .iter()
        .find_map(|(chrom, length)| (chrom == &region.chrom).then_some(*length))?;
    let start = region
        .start
        .map(|start| start.saturating_sub(1).min(length as u64) as u32)
        .unwrap_or(0);
    let end = region
        .end
        .map(|end| end.min(length as u64) as u32)
        .unwrap_or(length);

    (start < end).then_some(BbiScanRegion {
        chrom: region.chrom,
        start,
        end,
    })
}

pub(crate) fn region_display(regions: &[BbiScanRegion]) -> String {
    regions
        .iter()
        .map(|region| format!("{}:{}-{}", region.chrom, region.start, region.end))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn reject_unsupported_scheme(path: &str, format_name: &str) -> Result<()> {
    if path.contains("://") && !path.starts_with("file://") {
        return Err(DataFusionError::NotImplemented(format!(
            "{format_name} only supports local filesystem paths in this version"
        )));
    }
    Ok(())
}
