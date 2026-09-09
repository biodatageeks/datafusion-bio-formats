//! A2M and A3M: FASTA-shaped alignment formats.
//!
//! Both formats are parsed identically. The reader is a verbatim passthrough:
//! it never folds case, rewrites `.`/`-`, pads rows or validates the alignment,
//! because A3M (and dotless A2M, which is what Easel itself writes) is ragged by
//! design. The only format-specific behaviour is skipping `#` lines that hh-suite
//! may emit before the first `>` record.

use crate::storage::{LineSource, open_lines, read_line};
use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, LargeStringBuilder, RecordBatch, RecordBatchOptions, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures_util::TryStreamExt;
use log::debug;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Which FASTA-like alignment flavour a file is declared to be.
///
/// A2M and A3M are byte-identical to parse; the flavour only names the table
/// in plans and error messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsaFlavor {
    /// UCSC/SAM A2M (`.a2m`), dotted or dotless.
    A2m,
    /// hh-suite A3M (`.a3m`).
    A3m,
}

impl MsaFlavor {
    /// Upper-case short name (`A2M` / `A3M`).
    pub fn as_str(&self) -> &'static str {
        match self {
            MsaFlavor::A2m => "A2M",
            MsaFlavor::A3m => "A3M",
        }
    }
}

impl fmt::Display for MsaFlavor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The FASTA schema: `name`, `description`, `sequence`.
pub fn fasta_like_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::LargeUtf8, false),
    ]))
}

/// DataFusion table provider for A2M / A3M files.
#[derive(Clone, Debug)]
pub struct FastaLikeTableProvider {
    file_path: String,
    flavor: MsaFlavor,
    schema: SchemaRef,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl FastaLikeTableProvider {
    /// Creates a provider for `file_path` (local path or object-store URI).
    pub fn new(
        file_path: String,
        flavor: MsaFlavor,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            file_path,
            flavor,
            schema: fasta_like_schema(),
            object_storage_options,
        })
    }
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        // count(*): a zero-column schema; batches carry only a row count.
        Some(indices) if indices.is_empty() => Arc::new(Schema::empty()),
        Some(indices) => Arc::new(Schema::new(
            indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        )),
        None => schema.clone(),
    }
}

#[async_trait]
impl TableProvider for FastaLikeTableProvider {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection);
        Ok(Arc::new(FastaLikeExec {
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            file_path: self.file_path.clone(),
            flavor: self.flavor,
            schema,
            projection: projection.cloned(),
            limit,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}

/// Physical plan reading one A2M / A3M file as a single partition.
pub struct FastaLikeExec {
    file_path: String,
    flavor: MsaFlavor,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
    cache: Arc<PlanProperties>,
}

impl Debug for FastaLikeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastaLikeExec")
            .field("flavor", &self.flavor)
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for FastaLikeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let cols: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        write!(
            f,
            "{}Exec: file={}, projection=[{}]",
            self.flavor,
            self.file_path,
            cols.join(", ")
        )
    }
}

impl ExecutionPlan for FastaLikeExec {
    fn name(&self) -> &str {
        "FastaLikeExec"
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        debug!(
            "{} {}: executing partition={} projection={:?} limit={:?}",
            self.flavor, self.file_path, partition, self.projection, self.limit
        );
        let batch_size = context.session_config().batch_size();
        let fut = record_batches(
            self.file_path.clone(),
            self.schema.clone(),
            self.projection.clone(),
            self.limit,
            batch_size,
            self.object_storage_options.clone().unwrap_or_default(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Column indices into [`fasta_like_schema`].
const COL_NAME: usize = 0;
const COL_DESCRIPTION: usize = 1;
const COL_SEQUENCE: usize = 2;

/// Column builders for one batch of FASTA-like records.
///
/// Only the projected columns are built. `sequence` in particular is by far the
/// largest of the three, so a `count(*)` or a `SELECT name` never accumulates
/// alignment text it is about to discard.
struct Builders {
    /// Source column indices to emit, in output order; empty for `count(*)`.
    projection: Vec<usize>,
    name: Option<StringBuilder>,
    description: Option<StringBuilder>,
    sequence: Option<LargeStringBuilder>,
    rows: usize,
}

impl Builders {
    fn new(projection: &Option<Vec<usize>>) -> Self {
        let projection = projection
            .clone()
            .unwrap_or_else(|| vec![COL_NAME, COL_DESCRIPTION, COL_SEQUENCE]);
        let wants = |col: usize| projection.contains(&col);
        Self {
            name: wants(COL_NAME).then(StringBuilder::new),
            description: wants(COL_DESCRIPTION).then(StringBuilder::new),
            sequence: wants(COL_SEQUENCE).then(LargeStringBuilder::new),
            projection,
            rows: 0,
        }
    }

    /// Whether the caller needs to decode sequence bytes at all.
    fn wants_sequence(&self) -> bool {
        self.sequence.is_some()
    }

    fn push(&mut self, name: &str, description: Option<&str>, sequence: &str) {
        if let Some(b) = self.name.as_mut() {
            b.append_value(name);
        }
        if let Some(b) = self.description.as_mut() {
            b.append_option(description);
        }
        if let Some(b) = self.sequence.as_mut() {
            b.append_value(sequence);
        }
        self.rows += 1;
    }

    fn finish(&mut self, schema: &SchemaRef) -> datafusion::common::Result<RecordBatch> {
        let rows = self.rows;
        self.rows = 0;
        let arrays: Vec<ArrayRef> = self
            .projection
            .iter()
            .map(|&col| match col {
                COL_NAME => Arc::new(self.name.as_mut().expect("projected").finish()) as ArrayRef,
                COL_DESCRIPTION => {
                    Arc::new(self.description.as_mut().expect("projected").finish()) as ArrayRef
                }
                COL_SEQUENCE => {
                    Arc::new(self.sequence.as_mut().expect("projected").finish()) as ArrayRef
                }
                other => unreachable!("column {other} is not in the FASTA-like schema"),
            })
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("error building batch: {e}")))
    }
}

/// Splits a `>` header (without the `>`) into name and optional description
/// on the first whitespace character only. Commas are not separators.
fn split_header(header: &str) -> (&str, Option<&str>) {
    match header.find([' ', '\t']) {
        Some(i) => {
            let desc = header[i..].trim_start_matches([' ', '\t']);
            (&header[..i], (!desc.is_empty()).then_some(desc))
        }
        None => (header, None),
    }
}

async fn record_batches(
    file_path: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
    opts: ObjectStorageOptions,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    // A pushed-down `LIMIT 0` asks for nothing, so do not open the input at all.
    if limit == Some(0) {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )));
    }
    let mut src: LineSource = open_lines(&file_path, &opts, None)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to open {file_path}: {e}")))?;
    let out_schema = schema.clone();
    let stream = try_stream! {
        let mut builders = Builders::new(&projection);
        // Sequence lines are still read — they delimit records — but they are
        // only accumulated and decoded when the column is actually projected.
        let want_sequence = builders.wants_sequence();
        let mut line: Vec<u8> = Vec::new();
        let mut line_no: u64 = 0;
        let mut seen_record = false;
        let mut header: Option<String> = None;
        let mut sequence: Vec<u8> = Vec::new();
        let mut emitted: usize = 0;
        let mut done = false;

        loop {
            let more = read_line(&mut src, &mut line)
                .await
                .map_err(|e| DataFusionError::Execution(format!("{file_path}:{line_no}: read error: {e}")))?;
            let at_eof = !more;
            let starts_record = !at_eof && line.first() == Some(&b'>');

            if at_eof || starts_record {
                if let Some(h) = header.take() {
                    let (name, description) = split_header(&h);
                    let seq = if want_sequence {
                        std::str::from_utf8(&sequence).map_err(|e| {
                            DataFusionError::Execution(format!("{file_path}:{line_no}: sequence is not UTF-8: {e}"))
                        })?
                    } else {
                        ""
                    };
                    builders.push(name, description, seq);
                    sequence.clear();
                    emitted += 1;
                    if builders.rows >= batch_size {
                        yield builders.finish(&out_schema)?;
                    }
                    if limit.is_some_and(|l| emitted >= l) {
                        done = true;
                    }
                }
                if at_eof || done {
                    break;
                }
            }

            line_no += 1;
            if starts_record {
                seen_record = true;
                let h = std::str::from_utf8(&line[1..]).map_err(|e| {
                    DataFusionError::Execution(format!("{file_path}:{line_no}: header is not UTF-8: {e}"))
                })?;
                header = Some(h.trim_end().to_string());
            } else if line.is_empty() {
                continue;
            } else if !seen_record {
                if line[0] == b'#' {
                    // hh-suite may write "#A3M#" and other "#" lines before the first record.
                    continue;
                }
                Err(DataFusionError::Execution(format!(
                    "{file_path}:{line_no}: expected a '>' record header before sequence data"
                )))?;
            } else if want_sequence {
                sequence.extend_from_slice(&line);
            }
        }

        if builders.rows > 0 {
            yield builders.finish(&out_schema)?;
        }
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
