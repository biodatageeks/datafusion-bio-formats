//! Physical execution plan for Stockholm scans.

use crate::stockholm::reader::{SequenceRecord, StockholmReader};
use crate::stockholm::table_provider::{ColumnKind, annotation_struct_fields};
use crate::storage::open_lines;
use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, LargeStringBuilder, ListBuilder, RecordBatch, RecordBatchOptions, StringBuilder,
    StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use futures_util::TryStreamExt;
use log::debug;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

/// One scan partition: a byte range (or the whole input) and the ordinal of
/// the first alignment it contains.
#[derive(Clone, Debug)]
pub struct PartitionRange {
    /// Byte window, `None` for the whole input.
    pub range: Option<Range<u64>>,
    /// Ordinal of the first alignment in the window.
    pub first_ordinal: u64,
}

/// Physical plan reading a Stockholm file in one or more partitions.
pub struct StockholmExec {
    file_path: String,
    opts: ObjectStorageOptions,
    schema: SchemaRef,
    columns: Vec<ColumnKind>,
    empty_projection: bool,
    limit: Option<usize>,
    partitions: Vec<PartitionRange>,
    cache: Arc<PlanProperties>,
}

impl StockholmExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_path: String,
        opts: ObjectStorageOptions,
        schema: SchemaRef,
        columns: Vec<ColumnKind>,
        empty_projection: bool,
        limit: Option<usize>,
        partitions: Vec<PartitionRange>,
        cache: Arc<PlanProperties>,
    ) -> Self {
        Self {
            file_path,
            opts,
            schema,
            columns,
            empty_projection,
            limit,
            partitions,
            cache,
        }
    }
}

impl Debug for StockholmExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StockholmExec")
            .field("columns", &self.columns)
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for StockholmExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let cols: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        write!(
            f,
            "StockholmExec: file={}, partitions={}, projection=[{}]",
            self.file_path,
            self.partitions.len(),
            cols.join(", ")
        )
    }
}

impl ExecutionPlan for StockholmExec {
    fn name(&self) -> &str {
        "StockholmExec"
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
        let part = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "StockholmExec: partition {partition} out of {}",
                self.partitions.len()
            ))
        })?;
        debug!(
            "StockholmExec {}: partition={} range={:?} first_ordinal={} limit={:?}",
            self.file_path, partition, part.range, part.first_ordinal, self.limit
        );
        let fut = record_batches(
            self.file_path.clone(),
            self.opts.clone(),
            self.schema.clone(),
            self.columns.clone(),
            self.empty_projection,
            self.limit,
            context.session_config().batch_size(),
            part,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

enum ColumnBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Bag(ListBuilder<StructBuilder>),
}

fn bag_builder() -> ListBuilder<StructBuilder> {
    let fields = annotation_struct_fields();
    ListBuilder::new(StructBuilder::from_fields(fields.clone(), 0)).with_field(Field::new(
        "item",
        DataType::Struct(fields),
        true,
    ))
}

fn append_bag(b: &mut ListBuilder<StructBuilder>, items: &[(String, String)]) {
    if items.is_empty() {
        b.append_null();
        return;
    }
    let st = b.values();
    for (tag, value) in items {
        st.field_builder::<StringBuilder>(0)
            .expect("tag builder")
            .append_value(tag);
        st.field_builder::<StringBuilder>(1)
            .expect("value builder")
            .append_option((!value.is_empty()).then_some(value.as_str()));
        st.append(true);
    }
    b.append(true);
}

/// Builders for the projected columns of one batch.
struct RowBuilders {
    kinds: Vec<ColumnKind>,
    builders: Vec<ColumnBuilder>,
    rows: usize,
}

impl RowBuilders {
    fn new(kinds: &[ColumnKind]) -> Self {
        let builders = kinds
            .iter()
            .map(|k| match k {
                ColumnKind::Sequence => ColumnBuilder::LargeUtf8(LargeStringBuilder::new()),
                ColumnKind::GsBag | ColumnKind::GrBag => ColumnBuilder::Bag(bag_builder()),
                _ => ColumnBuilder::Utf8(StringBuilder::new()),
            })
            .collect();
        Self {
            kinds: kinds.to_vec(),
            builders,
            rows: 0,
        }
    }

    fn push(&mut self, alignment_id: &str, rec: &SequenceRecord) {
        for (kind, builder) in self.kinds.iter().zip(self.builders.iter_mut()) {
            match (kind, builder) {
                (ColumnKind::AlignmentId, ColumnBuilder::Utf8(b)) => b.append_value(alignment_id),
                (ColumnKind::Name, ColumnBuilder::Utf8(b)) => b.append_value(&rec.name),
                (ColumnKind::Sequence, ColumnBuilder::LargeUtf8(b)) => {
                    b.append_value(&rec.sequence)
                }
                (ColumnKind::GsBag, ColumnBuilder::Bag(b)) => append_bag(b, &rec.gs),
                (ColumnKind::GrBag, ColumnBuilder::Bag(b)) => append_bag(b, &rec.gr),
                (ColumnKind::PromotedGs(feature), ColumnBuilder::Utf8(b)) => b.append_option(
                    rec.gs
                        .iter()
                        .find(|(k, _)| k == feature)
                        .map(|(_, v)| v.as_str()),
                ),
                _ => unreachable!("column kind / builder mismatch"),
            }
        }
        self.rows += 1;
    }

    fn finish(
        &mut self,
        schema: &SchemaRef,
        empty_projection: bool,
    ) -> datafusion::common::Result<RecordBatch> {
        let rows = self.rows;
        self.rows = 0;
        let arrays: Vec<ArrayRef> = if empty_projection {
            vec![]
        } else {
            self.builders
                .iter_mut()
                .map(|b| match b {
                    ColumnBuilder::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
                    ColumnBuilder::LargeUtf8(b) => Arc::new(b.finish()) as ArrayRef,
                    ColumnBuilder::Bag(b) => Arc::new(b.finish()) as ArrayRef,
                })
                .collect()
        };
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("error building batch: {e}")))
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_batches(
    file_path: String,
    opts: ObjectStorageOptions,
    schema: SchemaRef,
    columns: Vec<ColumnKind>,
    empty_projection: bool,
    limit: Option<usize>,
    batch_size: usize,
    part: PartitionRange,
) -> datafusion::common::Result<SendableRecordBatchStream> {
    // A pushed-down `LIMIT 0` asks for nothing, so do not open the input at all.
    if limit == Some(0) {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )));
    }
    let src = open_lines(&file_path, &opts, part.range.clone())
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to open {file_path}: {e}")))?;
    let collect_sequences = columns.contains(&ColumnKind::Sequence);
    // Only a partition that opens at byte 0 sees the input's compulsory header.
    let at_input_start = part.range.as_ref().is_none_or(|r| r.start == 0);
    let mut reader = StockholmReader::new_at(
        src,
        file_path,
        part.first_ordinal,
        at_input_start,
        collect_sequences,
    );
    let out_schema = schema.clone();
    let stream = try_stream! {
        let mut builders = RowBuilders::new(&columns);
        let mut emitted = 0usize;
        'alignments: while let Some(alignment) = reader.next_alignment().await? {
            let id = alignment.id();
            for rec in &alignment.sequences {
                builders.push(&id, rec);
                emitted += 1;
                if builders.rows >= batch_size {
                    yield builders.finish(&out_schema, empty_projection)?;
                }
                if limit.is_some_and(|l| emitted >= l) {
                    break 'alignments;
                }
            }
        }
        if builders.rows > 0 {
            yield builders.finish(&out_schema, empty_projection)?;
        }
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
