use crate::storage::{BedLocalReader, BedRemoteReader};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, NullArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use futures_util::{StreamExt, TryStreamExt};
use log::{debug, info};

use crate::table_provider::BEDFields;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for scanning BED files
///
/// This struct implements DataFusion's [`ExecutionPlan`] trait to handle
/// the actual execution of BED file scans. It manages file I/O, record parsing,
/// and record batch construction.
#[allow(dead_code)]
pub struct BedExec {
    /// Path to the BED file
    pub(crate) file_path: String,
    /// BED format variant (BED3, BED4, BED5, BED6)
    pub(crate) bed_fields: BEDFields,
    /// Output schema for the execution plan
    pub(crate) schema: SchemaRef,
    /// Optional column projection indices
    pub(crate) projection: Option<Vec<usize>>,
    /// Plan properties for optimization
    pub(crate) cache: PlanProperties,
    /// Optional maximum number of rows to return
    pub(crate) limit: Option<usize>,
    /// Optional cloud storage configuration
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
}

impl Debug for BedExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let proj_str = match &self.projection {
            Some(_) => {
                let col_names: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                col_names.join(", ")
            }
            None => "*".to_string(),
        };
        write!(f, "BedExec: projection=[{}]", proj_str)
    }
}

impl ExecutionPlan for BedExec {
    /// Returns the name of this execution plan
    fn name(&self) -> &str {
        "BedExec"
    }

    /// Returns `self` as `Any` for dynamic type casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the properties (schema, partitioning, etc.) of this plan
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    /// Returns child execution plans (none for BED scanning)
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Returns a new plan with updated children
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Executes the BED file scan and returns a record batch stream
    ///
    /// # Arguments
    ///
    /// * `_partition` - Partition index (not used)
    /// * `context` - Task execution context
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let proj_cols = match &self.projection {
            Some(_) => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };
        info!(
            "{}: executing partition={} with projection=[{}]",
            self.name(),
            partition,
            proj_cols
        );
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let fut = get_stream(
            self.file_path.clone(),
            self.bed_fields.clone(),
            schema.clone(),
            batch_size,
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Reads BED records from remote storage and produces record batches
///
/// # Arguments
///
/// * `file_path` - Remote file path (GCS, S3, Azure URL)
/// * `bed_fields` - BED format variant
/// * `schema` - Output schema
/// * `batch_size` - Number of records per batch
/// * `projection` - Optional column projection
/// * `object_storage_options` - Cloud storage configuration
/// * `coordinate_system_zero_based` - If true, output 0-based coordinates; if false, 1-based
async fn get_remote_bed_stream(
    file_path: String,
    bed_fields: BEDFields,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader = match bed_fields {
        BEDFields::BED4 => {
            BedRemoteReader::<4>::new(file_path.clone(), object_storage_options.unwrap()).await
        }
        _ => unimplemented!("Unsupported BED fields: {:?}", bed_fields),
    };

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
        let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
        let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
        let mut name: Vec<Option<String>> =  Vec::with_capacity(batch_size);



        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            // BED files are natively 0-based in noodles. feature_start().get() returns 1-based.
            // For 0-based output: subtract 1 to get back to 0-based
            // For 1-based output: use as-is (noodles already returns 1-based)
            let start_pos = record.feature_start()?.get() as u32;
            poss.push(if coordinate_system_zero_based { start_pos - 1 } else { start_pos });
            // End position: noodles returns 1-based exclusive end
            // For 0-based half-open: subtract 1 to get 0-based exclusive end
            // For 1-based closed: use as-is
            let end_pos = record.feature_end().unwrap()?.get() as u32;
            pose.push(if coordinate_system_zero_based { end_pos - 1 } else { end_pos });
            name.push(record.name().map(|n| n.to_string()));

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &name,
                    projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                name.clear();

            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !chroms.is_empty() {
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
                &chroms,
                &poss,
                &pose,
                &name,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

/// Reads BED records from local storage and produces record batches
///
/// # Arguments
///
/// * `file_path` - Local file path
/// * `bed_fields` - BED format variant
/// * `schema` - Output schema
/// * `batch_size` - Number of records per batch
/// * `projection` - Optional column projection
/// * `coordinate_system_zero_based` - If true, output 0-based coordinates; if false, 1-based
async fn get_local_bed(
    file_path: String,
    bed_fields: BEDFields,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut reader = match bed_fields {
        BEDFields::BED4 => BedLocalReader::<4>::new(file_path.clone()).await?,
        _ => unimplemented!("Unsupported BED fields: {:?}", bed_fields),
    };

    let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
    let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
    let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
    let mut name: Vec<Option<String>> = Vec::with_capacity(batch_size);

    let mut record_num = 0;
    let mut batch_num = 0;

    let stream = try_stream! {

        let mut records = reader.read_records();
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            // BED files are natively 0-based in noodles. feature_start().get() returns 1-based.
            // For 0-based output: subtract 1 to get back to 0-based
            // For 1-based output: use as-is (noodles already returns 1-based)
            let start_pos = record.feature_start()?.get() as u32;
            poss.push(if coordinate_system_zero_based { start_pos - 1 } else { start_pos });
            // End position: noodles returns 1-based exclusive end
            // For 0-based half-open: subtract 1 to get 0-based exclusive end
            // For 1-based closed: use as-is
            let end_pos = record.feature_end().unwrap()?.get() as u32;
            pose.push(if coordinate_system_zero_based { end_pos - 1 } else { end_pos });
            name.push(record.name().map(|n| n.to_string()));

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &name,
                   projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                name.clear();

            }
        }
        // If there are remaining records that don't fill a complete batch,
        // yield them as well.
        if !chroms.is_empty() {
            let batch = build_record_batch(
                Arc::clone(&schema.clone()),
                &chroms,
                &poss,
                &pose,
                &name,
                projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

/// Constructs a DataFusion record batch from BED record data
///
/// # Arguments
///
/// * `schema` - Output schema
/// * `chroms` - Chromosome names
/// * `poss` - Feature start positions
/// * `pose` - Feature end positions
/// * `name` - Feature names (optional)
/// * `projection` - Optional column indices to include
fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    name: &[Option<String>],
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<RecordBatch> {
    let chrom_array = Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let pos_start_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let pos_end_array = Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>;
    let name_array = Arc::new(StringArray::from(name.to_vec())) as Arc<dyn Array>;
    let chrom_len = chrom_array.len();
    let arrays = match projection {
        None => {
            let arrays: Vec<Arc<dyn Array>> =
                vec![chrom_array, pos_start_array, pos_end_array, name_array];
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(chrom_len);
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(chrom_array.len())) as Arc<dyn Array>);
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(chrom_array.clone()),
                        1 => arrays.push(pos_start_array.clone()),
                        2 => arrays.push(pos_end_array.clone()),
                        3 => arrays.push(name_array.clone()),
                        _ => arrays.push(Arc::new(NullArray::new(chrom_len)) as Arc<dyn Array>),
                    }
                }
            }
            arrays
        }
    };
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}

/// Routes to appropriate reader based on storage backend and creates a record batch stream
///
/// # Arguments
///
/// * `file_path` - Path to BED file (local or remote)
/// * `bed_fields` - BED format variant
/// * `schema_ref` - Output schema
/// * `batch_size` - Number of records per batch
/// * `projection` - Optional column projection
/// * `object_storage_options` - Cloud storage configuration
/// * `coordinate_system_zero_based` - If true, output 0-based coordinates; if false, 1-based
#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    bed_fields: BEDFields,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed BED file.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_bed(
                file_path.clone(),
                bed_fields.clone(),
                schema.clone(),
                batch_size,
                projection,
                coordinate_system_zero_based,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_bed_stream(
                file_path.clone(),
                bed_fields.clone(),
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
                coordinate_system_zero_based,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}
