//! BigBed DataFusion table provider.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bigtools::bed::autosql::parse::{FieldType, parse_autosql};
use bigtools::utils::reopen::ReopenableFile;
use bigtools::{BigBedIntervalIter, BigBedRead};
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
    BBI_BATCH_ROWS, BBI_EMPTY_PROJECTION_BATCH_ROWS, BbiBlockIndex, BbiScanRegion, bbi_block_index,
    build_batch, normalize_local_path, partition_estimated_bytes, plan_bbi_partitions,
    plan_bbi_scan_regions, project_schema, projected_indices, projection_display, region_display,
    to_external_error,
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
    block_index: Option<BbiBlockIndex>,
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
        let block_index = match reader.data_blocks() {
            Ok(blocks) => Some(bbi_block_index(reader.chroms(), &blocks)),
            Err(error) if error.is_data_block_traversal_limit_exceeded() => None,
            Err(error) => return Err(to_external_error(error)),
        };
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
            block_index,
            extra_columns,
            coordinate_system_zero_based,
        })
    }
}

#[async_trait]
impl TableProvider for BigBedTableProvider {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // `_limit` is intentionally ignored: BBI scans have no cheap row cap, so
        // DataFusion applies the LIMIT in a `GlobalLimitExec` above this node.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection);
        let scan_plan =
            plan_bbi_scan_regions(filters, &self.chroms, self.coordinate_system_zero_based);
        let partitions = plan_bbi_partitions(
            scan_plan.regions,
            state.config().target_partitions(),
            !scan_plan.has_explicit_region,
            self.block_index.as_ref(),
        );
        let partition_estimated_bytes =
            partition_estimated_bytes(&partitions, self.block_index.as_ref());
        let partition_count = partitions.len();
        Ok(Arc::new(BigBedExec {
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            partitions,
            partition_estimated_bytes,
            block_layout_available: self.block_index.is_some(),
            extra_columns: self.extra_columns.clone(),
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partition_count),
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
    partitions: Vec<Vec<BbiScanRegion>>,
    partition_estimated_bytes: Vec<u64>,
    block_layout_available: bool,
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
            "BigBedExec: projection=[{}], partitions={}, block_layout_available={}, estimated_data_bytes={:?}, regions=[{}]",
            projection_display(&self.schema),
            self.partitions.len(),
            self.block_layout_available,
            self.partition_estimated_bytes,
            self.partitions
                .iter()
                .map(|regions| region_display(regions))
                .collect::<Vec<_>>()
                .join(" | ")
        )
    }
}

impl ExecutionPlan for BigBedExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> datafusion::common::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn name(&self) -> &str {
        "BigBedExec"
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
        // The stream opens a reader per region and pulls intervals in fixed-size
        // chunks, so peak memory stays bounded by one batch (never a whole
        // chromosome) and LIMIT/COUNT queries can stop early.
        let needs_split_fields = self
            .extra_columns
            .iter()
            .any(BigBedExtraColumn::needs_split_fields);
        let regions = self.partitions.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "BigBedExec partition {partition} is out of range for {} partitions",
                self.partitions.len()
            ))
        })?;
        let stream = futures_util::stream::iter(BigBedRegionStream {
            file_path: self.file_path.clone(),
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            batch_rows: if self.schema.fields().is_empty() {
                BBI_EMPTY_PROJECTION_BATCH_ROWS
            } else {
                BBI_BATCH_ROWS
            },
            regions: regions.clone().into_iter(),
            extra_columns: self.extra_columns.clone(),
            needs_split_fields,
            coordinate_system_zero_based: self.coordinate_system_zero_based,
            current: None,
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

/// An open interval iterator for the region currently being streamed.
struct CurrentRegion {
    chrom: String,
    iter: BigBedIntervalIter<ReopenableFile, BigBedRead<ReopenableFile>>,
    ownership_start: Option<u32>,
    ownership_end: Option<u32>,
}

/// Lazily yields fixed-size [`RecordBatch`]es across all scan regions, opening a
/// fresh reader per region and never buffering more than one batch at a time.
struct BigBedRegionStream {
    file_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    batch_rows: usize,
    regions: std::vec::IntoIter<BbiScanRegion>,
    extra_columns: Vec<BigBedExtraColumn>,
    needs_split_fields: bool,
    coordinate_system_zero_based: bool,
    current: Option<CurrentRegion>,
}

impl BigBedRegionStream {
    /// Open the next region's interval iterator, or return `None` when all
    /// regions are exhausted.
    fn open_next_region(&mut self) -> Option<Result<CurrentRegion>> {
        let region = self.regions.next()?;
        let reader = match BigBedRead::open_file(&self.file_path) {
            Ok(reader) => reader,
            Err(error) => {
                return Some(Err(DataFusionError::External(Box::new(
                    std::io::Error::other(format!(
                        "Failed to open BigBed file '{}': {error}",
                        self.file_path
                    )),
                ))));
            }
        };
        match reader.get_interval_move(&region.chrom, region.start, region.end) {
            Ok(iter) => Some(Ok(CurrentRegion {
                chrom: region.chrom,
                iter,
                ownership_start: region.ownership_start,
                ownership_end: region.ownership_end,
            })),
            Err(error) => Some(Err(to_external_error(error))),
        }
    }

    fn build_batch(&self, rows: &[BigBedRow]) -> Result<RecordBatch> {
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
                index => arrays.push(build_extra_array(rows, &self.extra_columns[index - 3])?),
            }
        }
        build_batch(self.schema.clone(), arrays, row_count)
    }
}

impl Iterator for BigBedRegionStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                match self.open_next_region()? {
                    Ok(region) => self.current = Some(region),
                    Err(error) => return Some(Err(error)),
                }
            }

            let current = self.current.as_mut().expect("current region is set");
            let ownership_start = current.ownership_start;
            let ownership_end = current.ownership_end;
            let empty_projection = self.schema.fields().is_empty();
            let mut rows: Vec<BigBedRow> = if empty_projection {
                Vec::new()
            } else {
                Vec::with_capacity(self.batch_rows)
            };
            let mut row_count = 0;
            let mut region_done = false;
            for entry in current.iter.by_ref() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(to_external_error(error))),
                };
                if let Some(ownership_end) = ownership_end
                    && entry.start >= ownership_end
                {
                    region_done = true;
                    break;
                }
                if let Some(ownership_start) = ownership_start
                    && entry.start < ownership_start
                {
                    continue;
                }
                row_count += 1;
                if !empty_projection {
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
                        chrom: current.chrom.clone(),
                        start,
                        end: entry.end,
                        rest: entry.rest,
                        fields,
                    });
                }
                if row_count >= self.batch_rows {
                    break;
                }
            }

            if region_done {
                self.current = None;
            }

            if row_count == 0 {
                // Region fully drained; advance to the next one.
                self.current = None;
                continue;
            }

            return Some(if empty_projection {
                build_batch(self.schema.clone(), Vec::new(), row_count)
            } else {
                self.build_batch(&rows)
            });
        }
    }
}

/// Build an Arrow array for one autoSQL-derived extra column.
///
/// A row whose trailing field is absent (the record has fewer tab-separated
/// fields than the autoSQL declaration) yields a null rather than an error:
/// BED allows optional trailing fields, the columns are declared nullable, and
/// pushdown is `Inexact` so DataFusion re-applies predicates above the scan.
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
