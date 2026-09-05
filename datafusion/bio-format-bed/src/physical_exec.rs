use crate::record::invalid_data;
use crate::storage::{BedLocalReader, BedRemoteReader};
use crate::table_provider::BEDFields;
use async_stream::try_stream;
use datafusion::arrow::array::{
    ArrayRef, RecordBatch, RecordBatchOptions, StringArray, UInt16Array, UInt32Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::companion::sanitize_location;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use futures::{Stream, StreamExt};
use noodles_bed::Record;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;

/// A single-partition streaming BED scan.
#[derive(Debug)]
pub struct BedExec {
    pub(crate) file_path: String,
    pub(crate) bed_fields: BEDFields,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: Arc<PlanProperties>,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    pub(crate) coordinate_system_zero_based: bool,
}

impl DisplayAs for BedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "BedExec: projection=[{columns}]")
    }
}

impl ExecutionPlan for BedExec {
    fn name(&self) -> &str {
        "BedExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "BED scan does not accept children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid BED partition {partition}; expected 0"
            )));
        }
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let file_path = self.file_path.clone();
        let options = self.object_storage_options.clone().unwrap_or_default();
        let projection = self
            .projection
            .clone()
            .unwrap_or_else(|| (0..self.bed_fields.count()).collect());
        let fields = self.bed_fields;
        let zero_based = self.coordinate_system_zero_based;
        let limit = self.limit;
        let output_schema = schema.clone();
        let stream = try_stream! {
            if limit != Some(0) {
                let records = open_records(file_path.clone(), options);
                let batches = batches_from_records(records, schema, fields, projection, batch_size, limit, zero_based);
                futures::pin_mut!(batches);
                while let Some(batch) = batches.next().await {
                    yield batch.map_err(|e| DataFusionError::Execution(format!("BED {}: {e}", sanitize_location(&file_path))))?;
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

fn open_records(
    path: String,
    options: ObjectStorageOptions,
) -> impl Stream<Item = io::Result<Record<3>>> {
    // Parse the three required fields in every output mode. Columns::push
    // decodes optional fields and supplies nulls when they are absent.
    try_stream! {
        match get_storage_type(path.clone()) {
            StorageType::LOCAL => {
                let mut reader = BedLocalReader::<3>::with_options(path, options).await?;
                let records = reader.read_records();
                futures::pin_mut!(records);
                while let Some(record) = records.next().await { yield record?; }
            }
            StorageType::GCS | StorageType::S3 | StorageType::AZBLOB | StorageType::HTTP => {
                let mut reader = BedRemoteReader::<3>::new(path, options).await?;
                let mut records = reader.read_records().await;
                while let Some(record) = records.next().await { yield record?; }
            }
        }
    }
}

/// All backends share conversion, validation, batching and projection.
#[allow(clippy::too_many_arguments)]
fn batches_from_records(
    records: impl Stream<Item = io::Result<Record<3>>>,
    schema: SchemaRef,
    fields: BEDFields,
    projection: Vec<usize>,
    batch_size: usize,
    limit: Option<usize>,
    zero_based: bool,
) -> impl Stream<Item = Result<RecordBatch>> {
    try_stream! {
        futures::pin_mut!(records);
        let mut columns = Columns::default();
        let mut count = 0;
        while let Some(record) = records.next().await {
            count += 1;
            columns.push(record?, fields, zero_based)
                .map_err(|e| invalid_data(format!("BED record {count}: {e}")))?;
            let at_limit = limit.is_some_and(|limit| count >= limit);
            if columns.chrom.len() >= batch_size || at_limit {
                yield std::mem::take(&mut columns).finish(schema.clone(), fields, &projection)?;
            }
            if at_limit { break; }
        }
        if !columns.chrom.is_empty() {
            yield columns.finish(schema, fields, &projection)?;
        }
    }
}

#[derive(Default)]
struct Columns {
    chrom: Vec<String>,
    start: Vec<u32>,
    end: Vec<u32>,
    name: Vec<Option<String>>,
    score: Vec<Option<u16>>,
    strand: Vec<Option<String>>,
}

impl Columns {
    fn push(&mut self, record: Record<3>, fields: BEDFields, zero_based: bool) -> io::Result<()> {
        // Noodles represents BED start as 1-based and end=0 as None. Preserve
        // zero-length intervals and convert before narrowing to Arrow UInt32.
        let start = record.feature_start()?.get();
        let start = if zero_based { start - 1 } else { start };
        self.start.push(
            u32::try_from(start).map_err(|_| {
                invalid_data("start exceeds UInt32 range after coordinate conversion")
            })?,
        );
        let end = record
            .feature_end()
            .transpose()?
            .map_or(0, |position| position.get());
        self.end
            .push(u32::try_from(end).map_err(|_| invalid_data("end exceeds UInt32 range"))?);
        self.chrom
            .push(record.reference_sequence_name().to_string());
        let other = record.other_fields();
        if fields.count() >= 4 {
            self.name.push(
                other
                    .get(0)
                    .filter(|value| *value != ".")
                    .map(|value| value.to_string()),
            );
        }
        if fields.count() >= 5 {
            let score = other
                .get(1)
                .filter(|value| *value != ".")
                .map(|value| {
                    let value = value.to_string();
                    if value.is_empty() {
                        return Err(invalid_data(
                            "score must not be empty; use '.' for a missing score",
                        ));
                    }
                    if !value.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(invalid_data("score must be an integer between 0 and 1000"));
                    }
                    let score: u16 = value
                        .parse()
                        .map_err(|_| invalid_data("score must be between 0 and 1000"))?;
                    if score > 1000 {
                        return Err(invalid_data("score must be between 0 and 1000"));
                    }
                    Ok(score)
                })
                .transpose()?;
            self.score.push(score);
        }
        if fields.count() >= 6 {
            let strand = other
                .get(2)
                .filter(|value| *value != ".")
                .map(|value| {
                    if value != "+" && value != "-" {
                        return Err(invalid_data("strand must be +, - or ."));
                    }
                    Ok(value.to_string())
                })
                .transpose()?;
            self.strand.push(strand);
        }
        Ok(())
    }

    fn finish(
        self,
        schema: SchemaRef,
        fields: BEDFields,
        projection: &[usize],
    ) -> Result<RecordBatch> {
        let row_count = self.chrom.len();
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.chrom)),
            Arc::new(UInt32Array::from(self.start)),
            Arc::new(UInt32Array::from(self.end)),
        ];
        if fields.count() >= 4 {
            arrays.push(Arc::new(StringArray::from(self.name)));
        }
        if fields.count() >= 5 {
            arrays.push(Arc::new(UInt16Array::from(self.score)));
        }
        if fields.count() >= 6 {
            arrays.push(Arc::new(StringArray::from(self.strand)));
        }
        let projected = projection
            .iter()
            .map(|&index| {
                arrays.get(index).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!("invalid BED projection index {index}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // A genuine zero-column batch carries its row count for COUNT(*).
        Ok(RecordBatch::try_new_with_options(
            schema,
            projected,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?)
    }
}
