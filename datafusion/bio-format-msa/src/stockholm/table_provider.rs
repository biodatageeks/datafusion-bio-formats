//! DataFusion table provider for Stockholm files.

use crate::stockholm::physical_exec::{PartitionRange, StockholmExec};
use crate::stockholm::reader::{STOCKHOLM_HEADER_PREFIX, has_alignment_content, is_terminator};
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

/// One `//`-delimited run of a local file: its byte range, and how many
/// alignments the reader will emit from it.
#[derive(Clone, Copy, Debug)]
struct Run {
    start: u64,
    end: u64,
    alignments: u64,
}

/// The `//`-delimited runs of a local file. Trailing unterminated content
/// becomes a final run.
///
/// One sequential pass, and it counts alignments rather than runs: the reader
/// also starts a new alignment at a `# STOCKHOLM 1.0` that follows a missing
/// terminator, so a run can hold more than one. Counting runs instead would
/// seed a later partition with too small an ordinal and duplicate the fallback
/// identifiers. Tracking the count as the lines go by also means an
/// unterminated final alignment — a supported case — costs neither a second
/// read of the file nor a buffer proportional to it.
fn alignment_boundaries(path: &str) -> std::io::Result<Vec<Run>> {
    let path = local_path(path);
    let mut reader = BufReader::with_capacity(1 << 20, std::fs::File::open(path)?);
    let mut out: Vec<Run> = Vec::new();
    let mut line = Vec::new();
    let mut offset: u64 = 0;
    let mut start: u64 = 0;
    let mut alignments: u64 = 0;
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        offset += n as u64;
        let body = line.strip_suffix(b"\n").unwrap_or(&line);
        let body = body.strip_suffix(b"\r").unwrap_or(body);
        // Invalid UTF-8 is never a terminator, and counts as content: the
        // reader raises on it rather than skipping it, so it must not be
        // mistaken for a blank tail.
        let text = match std::str::from_utf8(body) {
            Ok(text) => text,
            Err(_) => {
                alignments = alignments.max(1);
                continue;
            }
        };
        if is_terminator(text) {
            if alignments > 0 {
                out.push(Run {
                    start,
                    end: offset,
                    alignments,
                });
            }
            start = offset;
            alignments = 0;
        } else if has_alignment_content(text) {
            if text.trim_start().starts_with(STOCKHOLM_HEADER_PREFIX) {
                // Each header opens an alignment, terminator or not.
                alignments += 1;
            } else if alignments == 0 {
                // Data before any header: an alignment that omits its own.
                alignments = 1;
            }
        }
    }
    if offset > start && alignments > 0 {
        out.push(Run {
            start,
            end: offset,
            alignments,
        });
    }
    Ok(out)
}

/// Groups consecutive runs into at most `n` byte ranges of roughly equal size.
///
/// `first_ordinal` counts the alignments before each range, not the runs, so a
/// run holding several alignments advances it by all of them.
fn group_alignments(runs: &[Run], n: usize) -> Vec<PartitionRange> {
    let total: u64 = runs.iter().map(|r| r.end - r.start).sum();
    let n = n.min(runs.len()).max(1);
    let target = total.div_ceil(n as u64).max(1);
    let mut out = Vec::with_capacity(n);
    let mut i = 0;
    let mut ordinal: u64 = 0;
    while i < runs.len() {
        let first_ordinal = ordinal;
        let start = runs[i].start;
        let mut end = runs[i].end;
        let remaining_bins = n - out.len();
        ordinal += runs[i].alignments;
        i += 1;
        // Always leave at least one run per remaining bin.
        while i < runs.len() && end - start < target && runs.len() - i >= remaining_bins {
            end = runs[i].end;
            ordinal += runs[i].alignments;
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
        // A limit of zero asks for nothing, so answer before discovering
        // partitions: that would sniff the compression and scan the whole input
        // for `//` boundaries, which both opens the file and reads it.
        let (partitions, limit) = if limit == Some(0) {
            (
                vec![PartitionRange {
                    range: None,
                    first_ordinal: 0,
                }],
                Some(0),
            )
        } else {
            let partitions = self
                .plan_partitions(state.config().target_partitions())
                .await?;
            // A per-partition stop is only the global stop when there is one
            // partition; applying `n` in each of `p` partitions would return up
            // to `n * p` rows. DataFusion enforces the real limit above the
            // scan either way, so dropping the hint only forgoes an early exit.
            let limit = if partitions.len() <= 1 { limit } else { None };
            (partitions, limit)
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
