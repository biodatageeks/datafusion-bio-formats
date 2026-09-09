//! Alignment-level (`#=GF` / `#=GC`) annotations in long format.

use crate::stockholm::reader::StockholmReader;
use crate::storage::open_lines;
use datafusion::arrow::array::{
    ArrayRef, LargeStringBuilder, RecordBatch, StringBuilder, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use std::sync::Arc;

/// Schema of [`read_stockholm_annotations`]: one row per annotation line.
pub fn annotations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("alignment_id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("feature", DataType::Utf8, false),
        Field::new("value", DataType::LargeUtf8, false),
        Field::new("n_sequences", DataType::UInt32, false),
        Field::new("alignment_length", DataType::UInt32, false),
    ]))
}

/// Reads every `#=GF` and `#=GC` line of every alignment in `file_path`
/// without materialising sequences. Repeated features are preserved in file
/// order; `#=GC` values are concatenated across interleaved blocks.
pub async fn read_stockholm_annotations(
    file_path: String,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::common::Result<RecordBatch> {
    let opts = object_storage_options.unwrap_or_default();
    let src = open_lines(&file_path, &opts, None)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to open {file_path}: {e}")))?;
    let mut reader = StockholmReader::new(src, file_path, false);

    let mut alignment_id = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut feature = StringBuilder::new();
    let mut value = LargeStringBuilder::new();
    let mut n_sequences = UInt32Builder::new();
    let mut alignment_length = UInt32Builder::new();

    while let Some(alignment) = reader.next_alignment().await? {
        let id = alignment.id();
        let n = alignment.n_sequences() as u32;
        let len = alignment.alignment_length() as u32;
        for annotation in &alignment.annotations {
            alignment_id.append_value(&id);
            kind.append_value(annotation.kind.as_str());
            feature.append_value(&annotation.feature);
            value.append_value(&annotation.value);
            n_sequences.append_value(n);
            alignment_length.append_value(len);
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(alignment_id.finish()),
        Arc::new(kind.finish()),
        Arc::new(feature.finish()),
        Arc::new(value.finish()),
        Arc::new(n_sequences.finish()),
        Arc::new(alignment_length.finish()),
    ];
    RecordBatch::try_new(annotations_schema(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("error building batch: {e}")))
}
