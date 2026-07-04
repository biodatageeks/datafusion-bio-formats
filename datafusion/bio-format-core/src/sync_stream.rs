use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

/// Build a [`SendableRecordBatchStream`] that pulls batches **synchronously** on
/// the consuming task's thread — no dedicated reader thread, no channel.
///
/// Each `poll_next` invokes `next_batch` exactly once. Because the closure does
/// its (blocking) decompression/parse work inline, that work runs on the same
/// tokio worker that consumes the partition. A scan therefore uses exactly
/// `target_partitions` OS threads instead of ~2× (issue #212).
///
/// `next_batch` returns `Some(Ok(batch))` for each batch, `Some(Err(_))` to
/// surface an error (after which it should return `None`), and `None` when the
/// partition is exhausted.
pub fn sync_batch_stream<F>(schema: SchemaRef, next_batch: F) -> SendableRecordBatchStream
where
    F: FnMut() -> Option<Result<RecordBatch, DataFusionError>> + Send + 'static,
{
    let stream = futures::stream::unfold(next_batch, |mut next_batch| async move {
        let item = next_batch()?;
        Some((item, next_batch))
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn yields_batches_then_stops() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let s = schema.clone();
        let mut remaining = vec![3, 2, 1];
        let stream = sync_batch_stream(schema, move || {
            remaining.pop().map(|v| {
                Ok(
                    RecordBatch::try_new(s.clone(), vec![Arc::new(Int32Array::from(vec![v]))])
                        .unwrap(),
                )
            })
        });
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 3);
        let total: i32 = batches
            .iter()
            .map(|b| b.as_ref().unwrap().num_rows() as i32)
            .sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn propagates_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut sent = false;
        let stream = sync_batch_stream(schema, move || {
            if sent {
                None
            } else {
                sent = true;
                Some(Err(DataFusionError::Execution("boom".into())))
            }
        });
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);
        assert!(batches[0].is_err());
    }
}
