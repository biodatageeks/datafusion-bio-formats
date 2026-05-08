use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

pub(crate) fn build_record_batch(schema: SchemaRef, arrays: Vec<ArrayRef>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(Arc::clone(&schema), arrays)?)
}
