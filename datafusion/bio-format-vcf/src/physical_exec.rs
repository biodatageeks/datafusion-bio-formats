use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::storage::{VcfLocalReader, VcfRemoteReader};
use crate::table_provider::{OptionalField, info_to_arrow_type};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, Float64Array, NullArray, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::get_storage_type;
use datafusion_bio_format_core::object_storage::{ObjectStorageOptions, StorageType};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{StreamExt, TryStreamExt};
use log::debug;
use noodles::vcf::Header;
use noodles::vcf::header::Infos;
use noodles::vcf::variant::Record;
use noodles::vcf::variant::record::info::field::{Value, value::Array as ValueArray};
use noodles::vcf::variant::record::{AlternateBases, Filters, Ids, ReferenceBases};
use std::str;

fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ids: &[String],
    refs: &[String],
    alts: &[String],
    quals: &[f64],
    filters: &[String],
    infos: Option<&Vec<Arc<dyn Array>>>,
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<RecordBatch> {
    let chrom_array = Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>;
    let pos_start_array = Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>;
    let pos_end_array = Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>;
    let id_array = Arc::new(StringArray::from(ids.to_vec())) as Arc<dyn Array>;
    let ref_array = Arc::new(StringArray::from(refs.to_vec())) as Arc<dyn Array>;
    let alt_array = Arc::new(StringArray::from(alts.to_vec())) as Arc<dyn Array>;
    let qual_array = Arc::new(Float64Array::from(quals.to_vec())) as Arc<dyn Array>;
    let filter_array = Arc::new(StringArray::from(filters.to_vec())) as Arc<dyn Array>;
    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = vec![
                chrom_array,
                pos_start_array,
                pos_end_array,
                id_array,
                ref_array,
                alt_array,
                qual_array,
                filter_array,
            ];
            arrays.append(&mut infos.unwrap().clone());
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(ids.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(chrom_array.len())) as Arc<dyn Array>);
            } else {
                for i in proj_ids.clone() {
                    match i {
                        0 => arrays.push(chrom_array.clone()),
                        1 => arrays.push(pos_start_array.clone()),
                        2 => arrays.push(pos_end_array.clone()),
                        3 => arrays.push(id_array.clone()),
                        4 => arrays.push(ref_array.clone()),
                        5 => arrays.push(alt_array.clone()),
                        6 => arrays.push(qual_array.clone()),
                        7 => arrays.push(filter_array.clone()),
                        _ => arrays.push(infos.unwrap()[i - 8].clone()),
                    }
                }
            }
            arrays
        }
    };
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {:?}", e)))
}

fn load_infos(
    record: Box<dyn Record>,
    header: &Header,
    info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) -> Result<(), datafusion::arrow::error::ArrowError> {
    for i in 0..info_builders.2.len() {
        let name = &info_builders.0[i];
        let data_type = &info_builders.1[i];
        let builder = &mut info_builders.2[i];
        let info = record.info();
        let value = info.get(header, name);

        match value {
            Some(Ok(v)) => match v {
                Some(Value::Integer(v)) => {
                    builder.append_int(v)?;
                }
                Some(Value::Array(ValueArray::Integer(values))) => {
                    builder
                        .append_array_int(values.iter().map(|v| v.unwrap().unwrap()).collect())?;
                }
                Some(Value::Array(ValueArray::Float(values))) => {
                    builder
                        .append_array_float(values.iter().map(|v| v.unwrap().unwrap()).collect())?;
                }
                Some(Value::Float(v)) => {
                    builder.append_float(v)?;
                }
                Some(Value::String(v)) => {
                    builder.append_string(&v)?;
                }
                Some(Value::Array(ValueArray::String(values))) => {
                    builder.append_array_string(
                        values
                            .iter()
                            .map(|v| v.unwrap().unwrap().to_string())
                            .collect(),
                    )?;
                }
                Some(Value::Flag) => {
                    builder.append_boolean(true)?;
                }
                None => {
                    if data_type == &DataType::Boolean {
                        builder.append_boolean(false)?;
                    } else {
                        builder.append_null()?;
                    }
                }
                _ => panic!("Unsupported value type"),
            },

            _ => {
                if data_type == &DataType::Boolean {
                    builder.append_boolean(false)?;
                } else {
                    builder.append_null()?;
                }
            }
        }
    }
    Ok(())
}

fn builders_to_arrays(builders: &mut Vec<OptionalField>) -> Vec<Arc<dyn Array>> {
    builders
        .iter_mut()
        .map(|f| f.finish())
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn get_variant_end(record: &dyn Record, header: &Header) -> u32 {
    let ref_len = record.reference_bases().len();
    let alt_len = record.alternate_bases().len();
    //check if all are single base ACTG
    if ref_len == 1
        && alt_len == 1
        && record
            .reference_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c == b'A' || c == b'C' || c == b'G' || c == b'T')
        && record
            .alternate_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c.eq("A") || c.eq("C") || c.eq("G") || c.eq("T"))
    {
        record.variant_start().unwrap().unwrap().get() as u32
    } else {
        record.variant_end(header).unwrap().get() as u32
    }
}

async fn get_local_vcf(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    info_fields: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
    let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
    let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
    let mut ids: Vec<String> = Vec::with_capacity(batch_size);
    let mut refs: Vec<String> = Vec::with_capacity(batch_size);
    let mut alts: Vec<String> = Vec::with_capacity(batch_size);
    let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
    let mut filters: Vec<String> = Vec::with_capacity(batch_size);

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = VcfLocalReader::new(file_path.clone(), thread_num).await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let mut record_num = 0;
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, infos, &mut info_builders);

    let stream = try_stream! {

        let mut records = reader.read_records();
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.variant_start().unwrap()?.get() as u32);
            pose.push(get_variant_end(&record, &header));
            ids.push(record.ids().iter().map(|v| v.to_string()).collect::<Vec<String>>().join(";"));
            refs.push(record.reference_bases().to_string());
            alts.push(record.alternate_bases().iter().map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join("|"));
            quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
            filters.push(record.filters().iter(&header).map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join(";"));
            load_infos(Box::new(record), &header, &mut info_builders)?;
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
                    // if infos.is_empty() { None } else { Some(&infos) },

                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                ids.clear();
                refs.clear();
                alts.clear();
                quals.clear();
                filters.clear();

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
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
                // if infos.is_empty() { None } else { Some(&infos) },
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

async fn get_remote_vcf_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader = VcfRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields, infos, &mut info_builders);

    let stream = try_stream! {
        // Create vectors for accumulating record data.
        let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
        let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
        let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
        let mut ids: Vec<String> = Vec::with_capacity(batch_size);
        let mut refs: Vec<String> = Vec::with_capacity(batch_size);
        let mut alts: Vec<String> = Vec::with_capacity(batch_size);
        let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
        let mut filters: Vec<String> = Vec::with_capacity(batch_size);
        // add infos fields vector of vectors of different types

        debug!("Info fields: {:?}", info_builders);

        let mut record_num = 0;
        let mut batch_num = 0;


        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any
            chroms.push(record.reference_sequence_name().to_string());
            poss.push(record.variant_start().unwrap()?.get() as u32);
            pose.push(get_variant_end(&record, &header));
            ids.push(record.ids().iter().map(|v| v.to_string()).collect::<Vec<String>>().join(";"));
            refs.push(record.reference_bases().to_string());
            alts.push(record.alternate_bases().iter().map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join("|"));
            quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
            filters.push(record.filters().iter(&header).map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join(";"));
            load_infos(Box::new(record), &header, &mut info_builders)?;
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
                    // if infos.is_empty() { None } else { Some(&infos) },

                )?;
                batch_num += 1;
                debug!("Batch number: {}", batch_num);
                yield batch;
                // Clear vectors for the next batch.
                chroms.clear();
                poss.clear();
                pose.clear();
                ids.clear();
                refs.clear();
                alts.clear();
                quals.clear();
                filters.clear();

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
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
                // if infos.is_empty() { None } else { Some(&infos) },
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

fn set_info_builders(
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    infos: &Infos,
    info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) {
    for f in info_fields.unwrap_or_default() {
        let data_type = info_to_arrow_type(infos, &f);
        let field = OptionalField::new(&data_type, batch_size).unwrap();
        info_builders.0.push(f);
        info_builders.1.push(data_type);
        info_builders.2.push(field);
    }
}

async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    thread_num: Option<usize>,
    info_fields: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let stream = get_local_vcf(
                file_path.clone(),
                schema.clone(),
                batch_size,
                thread_num,
                info_fields,
                projection,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB | StorageType::HTTP => {
            let stream = get_remote_vcf_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                info_fields,
                projection,
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
    }
}

#[allow(dead_code)]
pub struct VcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Option<Vec<String>>,
    pub(crate) format_fields: Option<Vec<String>>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) thread_num: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
}

impl Debug for VcfExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for VcfExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for VcfExec {
    fn name(&self) -> &str {
        "VCFExec"
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
        debug!("VcfExec::execute");
        debug!("Projection: {:?}", self.projection);
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.thread_num,
            self.info_fields.clone(),
            self.projection.clone(),
            self.object_storage_options.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
