use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;

pub(crate) fn build_record_batch(
    schema: SchemaRef,
    arrays: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch> {
    if arrays.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        return Ok(RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            arrays,
            &options,
        )?);
    }

    Ok(RecordBatch::try_new(Arc::clone(&schema), arrays)?)
}
