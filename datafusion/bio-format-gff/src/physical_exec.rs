use crate::storage::{GffLocalReader, GffRemoteReader};
use crate::table_provider::get_attribute_names_and_types;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{
    Array, Float32Builder, NullArray, RecordBatch, StringArray, UInt32Array, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::{
    object_storage::{ObjectStorageOptions, StorageType, get_storage_type},
    table_utils::{Attribute, OptionalField, builders_to_arrays},
};
use futures_util::{StreamExt, TryStreamExt};
use log::debug;
use noodles_gff::feature::RecordBuf;
use noodles_gff::feature::record::{Phase, Strand};
use noodles_gff::feature::record_buf::attributes::field::Value;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[allow(dead_code)]
pub struct GffExec {
    pub(crate) file_path: String,
    pub(crate) attr_fields: Option<Vec<String>>,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
}

impl Debug for GffExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for GffExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for GffExec {
    fn name(&self) -> &str {
        "GffExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        debug!("GffExec::execute");
        debug!("Projection: {:?}", self.projection);
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let fut = get_stream(
            self.file_path.clone(),
            self.attr_fields.clone(),
            schema.clone(),
            batch_size,
            self.thread_num,
            self.projection.clone(),
            self.object_storage_options.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn set_attribute_builders(
    batch_size: usize,
    attributes_names_and_types: (Vec<String>, Vec<DataType>),
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) {
    let (attribute_names, attribute_types) = attributes_names_and_types;
    for (name, data_type) in attribute_names.into_iter().zip(attribute_types.into_iter()) {
        let field = OptionalField::new(&data_type, batch_size).unwrap();
        attribute_builders.0.push(name);
        attribute_builders.1.push(data_type);
        attribute_builders.2.push(field);
    }
}

fn load_attributes_unnest(
    record: RecordBuf,
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) -> Result<(), datafusion::arrow::error::ArrowError> {
    for i in 0..attribute_builders.2.len() {
        let name = &attribute_builders.0[i];
        let builder = &mut attribute_builders.2[i];
        let attributes = record.attributes();
        let value = attributes.get(name.as_ref());

        match value {
            Some(v) => match v {
                Value::String(v) => {
                    builder.append_string(&*v.to_string())?;
                }
                Value::Array(v) => {
                    builder.append_array_string(v.iter().map(|v| v.to_string()).collect())?;
                }
            },
            None => builder.append_null()?,
        }
    }
    Ok(())
}

fn load_attributes(
    record: RecordBuf,
    builder: &mut Vec<OptionalField>,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let attributes = record.attributes();
    let mut vec_attributes: Vec<Attribute> = Vec::new();
    for (tag, value) in attributes.as_ref().iter() {
        debug!("Loading attribute: {} with value: {:?}", tag, value);
        match value {
            Value::String(v) => vec_attributes.push(Attribute {
                tag: tag.to_string(),
                value: Some(v.to_string()),
            }),
            Value::Array(v) => {
                let val = &*v
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                vec_attributes.push(Attribute {
                    tag: tag.to_string(),
                    value: Some(val.to_string()),
                })
            }
        }
    }
    builder[0].append_array_struct(vec_attributes)?;
    Ok(())
}

fn standardize_strand(strand: Strand) -> String {
    match strand {
        Strand::Forward => "+".to_string(),
        Strand::Reverse => "-".to_string(),
        Strand::Unknown => "?".to_string(),
        Strand::None => ".".to_string(),
    }
}

fn standardize_phase(phase: Phase) -> u32 {
    match phase {
        Phase::Zero => 0,
        Phase::One => 1,
        Phase::Two => 2,
    }
}
async fn get_remote_gff_stream(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader =
        GffRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await?;

    //unnest builder
    let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());

    let unnest_enable = match attr_fields {
        Some(attr_fields) => {
            let attribute_names_and_types = get_attribute_names_and_types(attr_fields.clone());
            set_attribute_builders(
                batch_size,
                attribute_names_and_types,
                &mut attribute_builders,
            );
            true
        }
        _ => false,
    };
    //nested builder
    let mut builder = vec![OptionalField::new(
        &DataType::List(FieldRef::new(Field::new(
            "attribute",
            DataType::Struct(Fields::from(vec![
                Field::new("tag", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ))),
        batch_size,
    )?];

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
        let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
        let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
        let mut ty: Vec<String> = Vec::with_capacity(batch_size);
        let mut source: Vec<String> = Vec::with_capacity(batch_size);
        let mut scores: Vec<Option<f32>> = Vec::with_capacity(batch_size);
        let mut strand: Vec<String> = Vec::with_capacity(batch_size);
        let mut phase: Vec<Option<u32>> = Vec::with_capacity(batch_size);

        let mut record_num = 0;
        let mut batch_num = 0;

        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.start().get() as u32);
            pose.push(record.end().get() as u32);
            ty.push(record.ty().to_string());
            source.push(record.source().to_string());
            scores.push(record.score());
            strand.push(standardize_strand(record.strand()));
            phase.push(record.phase().map(|p| standardize_phase(p)) );
            if unnest_enable {
                load_attributes_unnest(record, &mut attribute_builders)?
            }
            else {
                load_attributes(record, &mut builder)?
            }

            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ty,
                    &source,
                    &scores,
                    &strand,
                    &phase,
                    Some(&builders_to_arrays(
                        if unnest_enable {
                            &mut attribute_builders.2
                        } else {
                            &mut builder
                        })), projection.clone(),


                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                ty.clear();
                source.clear();
                scores.clear();
                strand.clear();
                phase.clear();

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
                &ty,
                &source,
                &scores,
                &strand,
                &phase,
                 Some(&builders_to_arrays(
                        if unnest_enable {
                            &mut attribute_builders.2
                        } else {
                            &mut builder
                        })), projection.clone(),
                // if infos.is_empty() { None } else { Some(&infos) },
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_local_gff(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
    let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
    let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
    let mut ty: Vec<String> = Vec::with_capacity(batch_size);
    let mut source: Vec<String> = Vec::with_capacity(batch_size);
    let mut scores: Vec<Option<f32>> = Vec::with_capacity(batch_size);
    let mut strand: Vec<String> = Vec::with_capacity(batch_size);
    let mut phase: Vec<Option<u32>> = Vec::with_capacity(batch_size);

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = GffLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await?;
    //unnest builder
    let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());

    let unnest_enable = match attr_fields {
        Some(attr_fields) => {
            let attribute_names_and_types = get_attribute_names_and_types(attr_fields.clone());
            set_attribute_builders(
                batch_size,
                attribute_names_and_types,
                &mut attribute_builders,
            );
            true
        }
        _ => false,
    };
    //nested builder
    let mut builder = vec![OptionalField::new(
        &DataType::List(FieldRef::new(Field::new(
            "attribute",
            DataType::Struct(Fields::from(vec![
                Field::new("tag", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ))),
        batch_size,
    )?];
    let mut record_num = 0;

    let stream = try_stream! {

        let mut records = reader.read_records().await;
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.start().get() as u32);
            pose.push(record.end().get() as u32);
            ty.push(record.ty().to_string());
            source.push(record.source().to_string());
            scores.push(record.score());
            strand.push(standardize_strand(record.strand())) ;
            phase.push(record.phase().map(|p|  standardize_phase(p)) );
            if unnest_enable {
                load_attributes_unnest(record, &mut attribute_builders)?
            }
            else {
                load_attributes(record, &mut builder)?
            }
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ty,
                    &source,
                    &scores,
                    &strand,
                    &phase,
                   Some(&builders_to_arrays(
                        if unnest_enable {
                            &mut attribute_builders.2
                        } else {
                            &mut builder
                        })), projection.clone(),
                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                ty.clear();
                source.clear();
                scores.clear();
                strand.clear();
                phase.clear();

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
                &ty,
                &source,
                &scores,
                &strand,
                &phase,
                 Some(&builders_to_arrays(
                        if unnest_enable {
                            &mut attribute_builders.2
                        } else {
                            &mut builder
                        })), projection.clone(),
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ty: &[String],
    source: &[String],
    score: &[Option<f32>],
    strand: &[String],
    phase: &[Option<u32>],
    attributes: Option<&Vec<Arc<dyn Array>>>,
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<RecordBatch> {
    let chrom_array = Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let pos_start_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let pos_end_array = Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>;
    let ty_array = Arc::new(StringArray::from(ty.to_vec())) as Arc<dyn Array>;
    let source_array = Arc::new(StringArray::from(source.to_vec())) as Arc<dyn Array>;
    let score_array = Arc::new({
        let mut builder = Float32Builder::new();
        for s in score {
            builder.append_option(*s);
        }
        builder.finish()
    }) as Arc<dyn Array>;
    let strand_array = Arc::new(StringArray::from(strand.to_vec())) as Arc<dyn Array>;
    let phase_array = Arc::new({
        let mut builder = UInt32Builder::new();
        for s in phase {
            builder.append_option(*s);
        }
        builder.finish()
    }) as Arc<dyn Array>;
    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                chrom_array,
                pos_start_array,
                pos_end_array,
                ty_array,
                source_array,
                score_array,
                strand_array,
                phase_array,
            ];
            arrays.append(&mut attributes.unwrap().clone());
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(chroms.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(chrom_array.len())) as Arc<dyn Array>);
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(chrom_array.clone()),
                        1 => arrays.push(pos_start_array.clone()),
                        2 => arrays.push(pos_end_array.clone()),
                        3 => arrays.push(ty_array.clone()),
                        4 => arrays.push(source_array.clone()),
                        5 => arrays.push(score_array.clone()),
                        6 => arrays.push(strand_array.clone()),
                        7 => arrays.push(phase_array.clone()),
                        _ => arrays.push(attributes.unwrap()[i - 8].clone()),
                    }
                }
            }
            arrays
        }
    };
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}

async fn get_stream(
    file_path: String,
    attr_fields: Option<Vec<String>>,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_gff(
                file_path.clone(),
                attr_fields.clone(),
                schema.clone(),
                batch_size,
                thread_num,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_gff_stream(
                file_path.clone(),
                attr_fields.clone(),
                schema.clone(),
                batch_size,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}
