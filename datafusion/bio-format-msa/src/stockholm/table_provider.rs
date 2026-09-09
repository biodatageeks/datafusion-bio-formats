//! DataFusion table provider for Stockholm files.

use crate::stockholm::physical_exec::{PartitionRange, StockholmExec};
use crate::stockholm::reader::has_alignment_content;
use crate::storage::{is_local, local_path, resolve_compression};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use log::debug;
use std::any::Any;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

/// Sentinel accepted in `gs_fields` that keeps the full `gs` bag column
/// alongside promoted fields (mirrors GFF's `"attributes"` sentinel).
pub const GS_BAG_SENTINEL: &str = "gs";

/// What a column of the Stockholm table holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    /// `#=GF ID`, else `#=GF AC`, else the alignment ordinal.
    AlignmentId,
    /// Sequence name.
    Name,
    /// Aligned sequence.
    Sequence,
    /// All `#=GS` lines of the row as `List<Struct<tag, value>>`.
    GsBag,
    /// All `#=GR` lines of the row as `List<Struct<tag, value>>`.
    GrBag,
    /// The first `#=GS` value with this feature name, promoted to a column.
    PromotedGs(String),
}

/// Arrow type of the `gs` / `gr` annotation bags: the same shape as GFF `attributes`.
pub fn annotation_bag_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(annotation_struct_fields()),
        true,
    )))
}

pub(crate) fn annotation_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ])
}

/// Builds the column layout for the given `gs_fields` request.
pub fn column_layout(gs_fields: Option<&[String]>) -> Vec<ColumnKind> {
    let mut cols = vec![
        ColumnKind::AlignmentId,
        ColumnKind::Name,
        ColumnKind::Sequence,
    ];
    match gs_fields {
        None => cols.push(ColumnKind::GsBag),
        Some(fields) => {
            for f in fields {
                if f == GS_BAG_SENTINEL {
                    cols.push(ColumnKind::GsBag);
                } else {
                    cols.push(ColumnKind::PromotedGs(f.clone()));
                }
            }
        }
    }
    cols.push(ColumnKind::GrBag);
    cols
}

fn field_for(kind: &ColumnKind) -> Field {
    match kind {
        ColumnKind::AlignmentId => Field::new("alignment_id", DataType::Utf8, false),
        ColumnKind::Name => Field::new("name", DataType::Utf8, false),
        ColumnKind::Sequence => Field::new("sequence", DataType::LargeUtf8, false),
        ColumnKind::GsBag => Field::new("gs", annotation_bag_type(), true),
        ColumnKind::GrBag => Field::new("gr", annotation_bag_type(), true),
        ColumnKind::PromotedGs(name) => Field::new(name, DataType::Utf8, true),
    }
}

/// DataFusion table provider for Stockholm (`.sto` / `.stk`) files.
#[derive(Clone, Debug)]
pub struct StockholmTableProvider {
    file_path: String,
    object_storage_options: Option<ObjectStorageOptions>,
    columns: Vec<ColumnKind>,
    schema: SchemaRef,
}

impl StockholmTableProvider {
    /// Creates a provider for `file_path`. `gs_fields` promotes the named
    /// `#=GS` features to top-level columns; include `"gs"` to also keep the bag.
    pub fn new(
        file_path: String,
        object_storage_options: Option<ObjectStorageOptions>,
        gs_fields: Option<Vec<String>>,
    ) -> datafusion::common::Result<Self> {
        let columns = column_layout(gs_fields.as_deref());
        let schema = Arc::new(Schema::new(
            columns.iter().map(field_for).collect::<Vec<_>>(),
        ));
        Ok(Self {
            file_path,
            object_storage_options,
            columns,
            schema,
        })
    }

    /// Splits a local uncompressed multi-alignment file into at most
    /// `target_partitions` byte ranges aligned to `//` boundaries.
    async fn plan_partitions(
        &self,
        target_partitions: usize,
    ) -> datafusion::common::Result<Vec<PartitionRange>> {
        let whole = vec![PartitionRange {
            range: None,
            first_ordinal: 0,
        }];
        if target_partitions <= 1 || !is_local(&self.file_path) {
            return Ok(whole);
        }
        let opts = self.object_storage_options.clone().unwrap_or_default();
        let compression = resolve_compression(&self.file_path, &opts)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{}: {e}", self.file_path)))?;
        if compression != CompressionType::NONE {
            return Ok(whole);
        }
        let path = self.file_path.clone();
        let boundaries = tokio::task::spawn_blocking(move || alignment_boundaries(&path))
            .await
            .map_err(|e| DataFusionError::Execution(format!("partition scan failed: {e}")))?
            .map_err(|e| DataFusionError::Execution(format!("{}: {e}", self.file_path)))?;
        if boundaries.len() <= 1 {
            return Ok(whole);
        }
        Ok(group_alignments(&boundaries, target_partitions))
    }
}

/// Byte offsets `(start, end)` of every alignment in a local file, found by
/// scanning for `//` lines. Trailing unterminated content becomes a final range.
///
/// One sequential pass: whether the run since the last `//` holds anything the
/// reader would call an alignment is tracked as the lines go by, so an
/// unterminated final alignment — a supported case — costs neither a second
/// read of the file nor a buffer proportional to it.
fn alignment_boundaries(path: &str) -> std::io::Result<Vec<(u64, u64)>> {
    let path = local_path(path);
    let mut reader = BufReader::with_capacity(1 << 20, std::fs::File::open(path)?);
    let mut out = Vec::new();
    let mut line = Vec::new();
    let mut offset: u64 = 0;
    let mut start: u64 = 0;
    let mut tail_has_content = false;
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        offset += n as u64;
        let body = line.strip_suffix(b"\n").unwrap_or(&line);
        let body = body.strip_suffix(b"\r").unwrap_or(body);
        if body.iter().all(|b| b.is_ascii_whitespace() || *b == b'/') && body.trim_ascii() == b"//"
        {
            out.push((start, offset));
            start = offset;
            tail_has_content = false;
        } else if !tail_has_content {
            // Invalid UTF-8 is content: the reader will raise on it rather
            // than skip it, so it must not be mistaken for a blank tail.
            tail_has_content = match std::str::from_utf8(body) {
                Ok(text) => has_alignment_content(text),
                Err(_) => true,
            };
        }
    }
    if offset > start && tail_has_content {
        out.push((start, offset));
    }
    Ok(out)
}

/// Groups consecutive alignments into at most `n` byte ranges of roughly equal size.
fn group_alignments(boundaries: &[(u64, u64)], n: usize) -> Vec<PartitionRange> {
    let total: u64 = boundaries.iter().map(|(s, e)| e - s).sum();
    let n = n.min(boundaries.len()).max(1);
    let target = total.div_ceil(n as u64).max(1);
    let mut out = Vec::with_capacity(n);
    let mut i = 0;
    while i < boundaries.len() {
        let first_ordinal = i as u64;
        let start = boundaries[i].0;
        let mut end = boundaries[i].1;
        let remaining_bins = n - out.len();
        i += 1;
        // Always leave at least one alignment per remaining bin.
        while i < boundaries.len() && end - start < target && boundaries.len() - i >= remaining_bins
        {
            end = boundaries[i].1;
            i += 1;
        }
        out.push(PartitionRange {
            range: Some(start..end),
            first_ordinal,
        });
    }
    out
}

#[async_trait]
impl TableProvider for StockholmTableProvider {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let (schema, columns): (SchemaRef, Vec<ColumnKind>) = match projection {
            // count(*): a zero-column schema; batches carry only a row count.
            Some(idx) if idx.is_empty() => (Arc::new(Schema::empty()), vec![]),
            Some(idx) => (
                Arc::new(Schema::new(
                    idx.iter()
                        .map(|&i| self.schema.field(i).clone())
                        .collect::<Vec<_>>(),
                )),
                idx.iter().map(|&i| self.columns[i].clone()).collect(),
            ),
            None => (self.schema.clone(), self.columns.clone()),
        };
        let partitions = self
            .plan_partitions(state.config().target_partitions())
            .await?;
        // A per-partition stop is only the global stop when there is one
        // partition; applying `n` in each of `p` partitions would return up to
        // `n * p` rows. `Some(0)` is the exception — zero per partition is zero
        // overall — and DataFusion enforces the real limit above the scan
        // either way, so dropping the hint only forgoes an early exit.
        let limit = match limit {
            Some(0) => Some(0),
            other if partitions.len() <= 1 => other,
            _ => None,
        };
        debug!(
            "StockholmTableProvider::scan {} partitions={} projection={:?}",
            self.file_path,
            partitions.len(),
            projection
        );
        Ok(Arc::new(StockholmExec::new(
            self.file_path.clone(),
            self.object_storage_options.clone().unwrap_or_default(),
            schema.clone(),
            columns,
            projection.map(|p| p.is_empty()).unwrap_or(false),
            limit,
            partitions.clone(),
            Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions.len()),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        )))
    }
}
