//! Projection-aware GenBank record and feature scans.
mod batch;
mod location;
mod reader;
mod storage;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::{
    ExecutionPlan,
    stream::RecordBatchStreamAdapter,
    streaming::{PartitionStream, StreamingTableExec},
};
use datafusion_bio_format_core::{line_source::read_line, object_storage::ObjectStorageOptions};
use reader::Parser;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Filter {
    column: String,
    value: String,
}

/// Read one row per LOCUS or per FEATURES entry.
#[derive(Clone, Debug)]
pub struct GenBankTableProvider {
    path: String,
    opts: ObjectStorageOptions,
    schema: SchemaRef,
    features: bool,
    zero: bool,
}

impl GenBankTableProvider {
    /// Construct the schema without opening the file.
    pub fn new(
        path: String,
        opts: Option<ObjectStorageOptions>,
        view: &str,
        qualifier_fields: Option<&[String]>,
        zero_based: bool,
    ) -> Result<Self> {
        let features = match view {
            "features" => true,
            "records" => false,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unknown GenBank view {view:?}"
                )));
            }
        };
        Ok(Self {
            path,
            opts: opts.unwrap_or_default(),
            schema: batch::schema(features, qualifier_fields, zero_based)?,
            features,
            zero: zero_based,
        })
    }
    fn filter(&self, expr: &Expr) -> Option<Filter> {
        let Expr::BinaryExpr(b) = expr else {
            return None;
        };
        if b.op != Operator::Eq {
            return None;
        }
        let (col, lit) = match (b.left.as_ref(), b.right.as_ref()) {
            (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c)) => {
                (c, v)
            }
            _ => return None,
        };
        let allowed = if self.features {
            matches!(col.name.as_str(), "type" | "record_id")
        } else {
            col.name == "name"
        };
        if !allowed {
            return None;
        }
        match lit {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => Some(Filter {
                column: col.name.clone(),
                value: v.clone(),
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl TableProvider for GenBankTableProvider {
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
            .map(|f| {
                if self.filter(f).is_some() {
                    TableProviderFilterPushDown::Exact
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
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema(),
        };
        let filters = filters
            .iter()
            .map(|f| {
                self.filter(f).ok_or_else(|| {
                    DataFusionError::Internal(format!("unsupported GenBank scan filter {f}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let n = if limit.is_some() {
            1
        } else {
            state.config().target_partitions()
        };
        let slices = storage::partitions(&self.path, &self.opts, n)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{}: {e}", self.path)))?;
        let partitions = slices
            .into_iter()
            .map(|slice| {
                Arc::new(Partition {
                    provider: self.clone(),
                    schema: schema.clone(),
                    filters: filters.clone(),
                    slice,
                    limit,
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            [],
            false,
            limit,
        )?))
    }
}

#[derive(Clone, Debug)]
struct Partition {
    provider: GenBankTableProvider,
    schema: SchemaRef,
    filters: Vec<Filter>,
    slice: storage::Slice,
    limit: Option<usize>,
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let Self {
            provider,
            schema,
            filters,
            slice,
            limit,
        } = self.clone();
        let output = schema.clone();
        let stream = try_stream! {
            if limit == Some(0) { return; }
            let path = &provider.path;
            let mut src = slice.open(path, &provider.opts).await
                .map_err(|e| DataFusionError::Execution(format!("{path}: {e}")))?;
            let mut parser = Parser::new(&schema, provider.features, provider.zero, filters);
            let mut batch = batch::Batch::new(schema);
            let mut buf = Vec::new();
            let mut line = 0u64;
            let mut emitted = 0usize;

            while read_line(&mut src, &mut buf).await
                .map_err(|e| DataFusionError::Execution(format!("{path} near line {line}: {e}")))? {
                line += 1;
                let text = std::str::from_utf8(&buf)
                    .map_err(|e| DataFusionError::Execution(format!("{path} line {line}: {e}")))?;
                parser.line(text, &mut batch).map_err(|e| DataFusionError::Execution(
                    format!("{path} LOCUS {} partition line {line}: {e}", parser.name())
                ))?;
                let reached_limit = limit.is_some_and(|n| batch.rows + emitted >= n);
                if reached_limit || batch.rows >= ctx.session_config().batch_size() || batch.bytes >= 8 * 1024 * 1024 {
                    emitted += batch.rows;
                    yield batch.finish()?;
                    if reached_limit { return; }
                }
            }
            if !parser.complete() {
                Err(DataFusionError::Execution(format!(
                    "{path} LOCUS {} partition line {line}: missing // terminator", parser.name()
                )))?;
            }
            if batch.rows > 0 {
                yield batch.finish()?;
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(output, stream))
    }
}
