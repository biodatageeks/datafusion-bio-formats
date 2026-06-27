//! BigBed DataFusion table provider.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bigtools::BigBedRead;
use bigtools::bed::autosql::parse::{FieldType, parse_autosql};
use bigtools::utils::reopen::ReopenableFile;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
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
use datafusion_bio_format_core::genomic_filter::is_genomic_coordinate_filter;
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;

use crate::common::{
    BbiScanRegion, build_batch, normalize_local_path, plan_bbi_scan_regions, project_schema,
    projected_indices, projection_display, region_display, to_external_error,
};

/// BigBed schema discovery mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BigBedSchemaMode {
    /// Use supported autoSQL fields and fall back to `rest` when unavailable.
    Auto,
    /// Always expose the raw trailing fields in `rest`.
    Rest,
}

#[derive(Clone, Debug)]
enum BigBedExtraColumn {
    Utf8 { name: String, rest_index: usize },
    Int64 { name: String, rest_index: usize },
    UInt64 { name: String, rest_index: usize },
    Float64 { name: String, rest_index: usize },
    Rest,
}

impl BigBedExtraColumn {
    fn name(&self) -> &str {
        match self {
            BigBedExtraColumn::Utf8 { name, .. }
            | BigBedExtraColumn::Int64 { name, .. }
            | BigBedExtraColumn::UInt64 { name, .. }
            | BigBedExtraColumn::Float64 { name, .. } => name,
            BigBedExtraColumn::Rest => "rest",
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            BigBedExtraColumn::Utf8 { .. } | BigBedExtraColumn::Rest => DataType::Utf8,
            BigBedExtraColumn::Int64 { .. } => DataType::Int64,
            BigBedExtraColumn::UInt64 { .. } => DataType::UInt64,
            BigBedExtraColumn::Float64 { .. } => DataType::Float64,
        }
    }

    /// Whether this column reads from the split per-field view (`true`) or the
    /// whole trailing `rest` string (`false`).
    fn needs_split_fields(&self) -> bool {
        !matches!(self, BigBedExtraColumn::Rest)
    }
}

/// Table provider for local BigBed files.
#[derive(Clone, Debug)]
pub struct BigBedTableProvider {
    file_path: String,
    schema: SchemaRef,
    chroms: Vec<(String, u32)>,
    extra_columns: Vec<BigBedExtraColumn>,
    coordinate_system_zero_based: bool,
}

impl BigBedTableProvider {
    /// Create a BigBed provider for a local file path.
    pub fn new(
        file_path: String,
        coordinate_system_zero_based: bool,
        schema_mode: BigBedSchemaMode,
    ) -> Result<Self> {
        let file_path = normalize_local_path(&file_path, "BigBed")?;
        let mut reader = BigBedRead::open_file(&file_path).map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to open BigBed file '{file_path}': {error}"
            ))))
        })?;
        let chroms = reader
            .chroms()
            .iter()
            .map(|chrom| (chrom.name.clone(), chrom.length))
            .collect::<Vec<_>>();
        let extra_columns = discover_extra_columns(&mut reader, schema_mode);
        let schema = bigbed_schema(coordinate_system_zero_based, &extra_columns);
        Ok(Self {
            file_path,
            schema,
            chroms,
            extra_columns,
            coordinate_system_zero_based,
        })
    }
}

#[async_trait]
impl TableProvider for BigBedTableProvider {
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
        Ok(Arc::new(BigBedExec {
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            regions,
            extra_columns: self.extra_columns.clone(),
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

/// Physical execution plan for BigBed scans.
pub struct BigBedExec {
    file_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    regions: Vec<BbiScanRegion>,
    extra_columns: Vec<BigBedExtraColumn>,
    coordinate_system_zero_based: bool,
    cache: Arc<PlanProperties>,
}

impl Debug for BigBedExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigBedExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BigBedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BigBedExec: projection=[{}], regions=[{}]",
            projection_display(&self.schema),
            region_display(&self.regions)
        )
    }
}

impl ExecutionPlan for BigBedExec {
    fn name(&self) -> &str {
        "BigBedExec"
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
        let reader = BigBedRead::open_file(&self.file_path).map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to open BigBed file '{}': {error}",
                self.file_path
            ))))
        })?;
        // Emit one RecordBatch per scan region (one per chromosome when there is
        // no genomic filter) so memory stays bounded by a single region instead
        // of materializing the whole file up front.
        let needs_split_fields = self
            .extra_columns
            .iter()
            .any(BigBedExtraColumn::needs_split_fields);
        let stream = futures_util::stream::iter(BigBedRegionStream {
            reader,
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            regions: self.regions.clone().into_iter(),
            extra_columns: self.extra_columns.clone(),
            needs_split_fields,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

#[derive(Clone, Debug)]
struct BigBedRow {
    chrom: String,
    start: u32,
    end: u32,
    /// Whole trailing field string; populated only when a `rest` column is used.
    rest: String,
    /// Per-field split of `rest`; populated only when a typed column is used.
    fields: Vec<String>,
}

/// Lazily yields one [`RecordBatch`] per scan region from an open BigBed reader.
struct BigBedRegionStream {
    reader: BigBedRead<ReopenableFile>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    regions: std::vec::IntoIter<BbiScanRegion>,
    extra_columns: Vec<BigBedExtraColumn>,
    needs_split_fields: bool,
    coordinate_system_zero_based: bool,
}

impl BigBedRegionStream {
    fn build_region_batch(&mut self, region: &BbiScanRegion) -> Result<RecordBatch> {
        let iter = self
            .reader
            .get_interval(&region.chrom, region.start, region.end)
            .map_err(to_external_error)?;

        let mut rows = Vec::new();
        for entry in iter {
            let entry = entry.map_err(to_external_error)?;
            let start = if self.coordinate_system_zero_based {
                entry.start
            } else {
                entry.start + 1
            };
            // Only allocate the split-field view or the `rest` string when a
            // column actually consumes it; the other is left empty.
            let fields = if self.needs_split_fields {
                entry.rest.split('\t').map(ToString::to_string).collect()
            } else {
                Vec::new()
            };
            rows.push(BigBedRow {
                chrom: region.chrom.clone(),
                start,
                end: entry.end,
                rest: entry.rest,
                fields,
            });
        }

        let row_count = rows.len();
        let full_width = 3 + self.extra_columns.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for index in projected_indices(self.projection.as_deref(), full_width) {
            match index {
                0 => arrays.push(Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.chrom.as_str()),
                ))),
                1 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|row| row.start),
                ))),
                2 => arrays.push(Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|row| row.end),
                ))),
                index => arrays.push(build_extra_array(&rows, &self.extra_columns[index - 3])?),
            }
        }

        build_batch(self.schema.clone(), arrays, row_count)
    }
}

impl Iterator for BigBedRegionStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let region = self.regions.next()?;
        Some(self.build_region_batch(&region))
    }
}

fn build_extra_array(rows: &[BigBedRow], column: &BigBedExtraColumn) -> Result<ArrayRef> {
    match column {
        BigBedExtraColumn::Utf8 { rest_index, .. } => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.fields.get(*rest_index).map(String::as_str))
                .collect::<Vec<_>>(),
        ))),
        BigBedExtraColumn::Rest => Ok(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.rest.as_str()),
        ))),
        BigBedExtraColumn::Int64 { rest_index, name } => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| parse_optional::<i64>(row.fields.get(*rest_index), name))
                .collect::<Result<Vec<_>>>()?,
        ))),
        BigBedExtraColumn::UInt64 { rest_index, name } => Ok(Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| parse_optional::<u64>(row.fields.get(*rest_index), name))
                .collect::<Result<Vec<_>>>()?,
        ))),
        BigBedExtraColumn::Float64 { rest_index, name } => Ok(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| parse_optional::<f64>(row.fields.get(*rest_index), name))
                .collect::<Result<Vec<_>>>()?,
        ))),
    }
}

fn parse_optional<T: std::str::FromStr>(
    value: Option<&String>,
    column_name: &str,
) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<T>().map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to parse BigBed column '{column_name}' value '{value}': {error}"
                ))
            })
        })
        .transpose()
}

fn bigbed_schema(
    coordinate_system_zero_based: bool,
    extra_columns: &[BigBedExtraColumn],
) -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
    ];
    fields.extend(
        extra_columns
            .iter()
            .map(|column| Field::new(column.name(), column.data_type(), true)),
    );
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

fn discover_extra_columns(
    reader: &mut BigBedRead<ReopenableFile>,
    schema_mode: BigBedSchemaMode,
) -> Vec<BigBedExtraColumn> {
    if schema_mode == BigBedSchemaMode::Rest {
        return vec![BigBedExtraColumn::Rest];
    }
    let Some(autosql) = reader.autosql().ok().flatten() else {
        return vec![BigBedExtraColumn::Rest];
    };
    let Ok(declarations) = parse_autosql(&autosql) else {
        return vec![BigBedExtraColumn::Rest];
    };
    // BigBed files declare a single autoSQL table; take the first declaration.
    let Some(declaration) = declarations.into_iter().next() else {
        return vec![BigBedExtraColumn::Rest];
    };
    if declaration.fields.len() <= 3 {
        return Vec::new();
    }

    let mut columns = Vec::new();
    for (rest_index, field) in declaration.fields.into_iter().skip(3).enumerate() {
        // Fixed-size array fields (e.g. `int[3]`) are stored as a single raw
        // token. Keep just that column as text rather than downgrading the
        // entire typed schema to a single `rest` column.
        if field.field_size.is_some() {
            columns.push(BigBedExtraColumn::Utf8 {
                name: field.name,
                rest_index,
            });
            continue;
        }
        let column = match field.field_type {
            FieldType::String
            | FieldType::Lstring
            | FieldType::Char
            | FieldType::Enum(_)
            | FieldType::Set(_) => BigBedExtraColumn::Utf8 {
                name: field.name,
                rest_index,
            },
            FieldType::Int | FieldType::Short | FieldType::Byte | FieldType::Bigint => {
                BigBedExtraColumn::Int64 {
                    name: field.name,
                    rest_index,
                }
            }
            FieldType::Uint | FieldType::Ushort | FieldType::Ubyte => BigBedExtraColumn::UInt64 {
                name: field.name,
                rest_index,
            },
            FieldType::Float | FieldType::Double => BigBedExtraColumn::Float64 {
                name: field.name,
                rest_index,
            },
            // Nested declarations have no flat column representation; keep the
            // raw token as text instead of discarding the whole typed schema.
            FieldType::Declaration(_, _) => BigBedExtraColumn::Utf8 {
                name: field.name,
                rest_index,
            },
        };
        columns.push(column);
    }
    columns
}
