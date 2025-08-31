use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::storage::{VcfLocalReader, VcfRemoteReader};
use crate::table_provider::info_to_arrow_type;
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, Float64Array, NullArray, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::{
    object_storage::{ObjectStorageOptions, StorageType, get_storage_type},
    table_utils::{OptionalField, builders_to_arrays},
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{StreamExt, TryStreamExt};
use log::debug;
use noodles_vcf::Header;
use noodles_vcf::header::Infos;
use noodles_vcf::variant::Record;
use noodles_vcf::variant::record::info::field::{Value, value::Array as ValueArray};
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids, ReferenceBases};
use std::str;

fn build_record_batch(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ids: &[String],
    refs: &[String],
    alts: &[String],
    quals: &[Option<f64>],
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

fn build_record_batch_optimized(
    schema: SchemaRef,
    chroms: &[String],
    poss: &[u32],
    pose: &[u32],
    ids: &[String],
    refs: &[String],
    alts: &[String],
    quals: &[Option<f64>],
    filters: &[String],
    infos: Option<&Vec<Arc<dyn Array>>>,
    projection: Option<Vec<usize>>,
    needs_chrom: bool,
    needs_start: bool,
    needs_end: bool,
    needs_id: bool,
    needs_ref: bool,
    needs_alt: bool,
    needs_qual: bool,
    needs_filter: bool,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    // Only create arrays for fields that are actually needed
    let chrom_array = if needs_chrom {
        Arc::new(StringArray::from(chroms.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let pos_start_array = if needs_start {
        Arc::new(UInt32Array::from(poss.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(UInt32Array::from(vec![0u32; record_count])) as Arc<dyn Array>
    };

    let pos_end_array = if needs_end {
        Arc::new(UInt32Array::from(pose.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(UInt32Array::from(vec![0u32; record_count])) as Arc<dyn Array>
    };

    let id_array = if needs_id {
        Arc::new(StringArray::from(ids.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let ref_array = if needs_ref {
        Arc::new(StringArray::from(refs.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let alt_array = if needs_alt {
        Arc::new(StringArray::from(alts.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

    let qual_array = if needs_qual {
        Arc::new(Float64Array::from(quals.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(Float64Array::from(vec![None::<f64>; record_count])) as Arc<dyn Array>
    };

    let filter_array = if needs_filter {
        Arc::new(StringArray::from(filters.to_vec())) as Arc<dyn Array>
    } else {
        Arc::new(StringArray::from(vec![String::new(); record_count])) as Arc<dyn Array>
    };

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
            if let Some(info_arrays) = infos {
                arrays.append(&mut info_arrays.clone());
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            if proj_ids.is_empty() {
                debug!("Empty projection creating a dummy field");
                arrays.push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>);
            } else {
                for i in proj_ids {
                    match i {
                        0 => arrays.push(chrom_array.clone()),
                        1 => arrays.push(pos_start_array.clone()),
                        2 => arrays.push(pos_end_array.clone()),
                        3 => arrays.push(id_array.clone()),
                        4 => arrays.push(ref_array.clone()),
                        5 => arrays.push(alt_array.clone()),
                        6 => arrays.push(qual_array.clone()),
                        7 => arrays.push(filter_array.clone()),
                        _ => {
                            if let Some(info_arrays) = infos {
                                // Map schema field index to actual INFO array index
                                // The schema field index i maps to INFO field (i - 8)
                                // But our INFO arrays may be sparse due to projection optimization
                                let schema_info_idx = i - 8;
                                if schema_info_idx < info_arrays.len() {
                                    arrays.push(info_arrays[schema_info_idx].clone());
                                } else {
                                    arrays
                                        .push(Arc::new(NullArray::new(record_count))
                                            as Arc<dyn Array>);
                                }
                            } else {
                                arrays
                                    .push(Arc::new(NullArray::new(record_count)) as Arc<dyn Array>);
                            }
                        }
                    }
                }
            }
            arrays
        }
    };

    RecordBatch::try_new(schema, arrays).map_err(|e| {
        DataFusionError::Execution(format!("Error creating optimized VCF batch: {:?}", e))
    })
}

fn load_infos(
    record: Box<dyn Record>,
    header: &Header,
    info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    _projection: Option<Vec<usize>>,
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
    object_storage_options: Option<ObjectStorageOptions>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    // Determine which core VCF fields we need to parse based on projection
    let needs_chrom = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_start = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_end = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_id = projection.as_ref().map_or(true, |proj| proj.contains(&3));
    let needs_ref = projection.as_ref().map_or(true, |proj| proj.contains(&4));
    let needs_alt = projection.as_ref().map_or(true, |proj| proj.contains(&5));
    let needs_qual = projection.as_ref().map_or(true, |proj| proj.contains(&6));
    let needs_filter = projection.as_ref().map_or(true, |proj| proj.contains(&7));

    let mut chroms: Vec<String> = if needs_chrom {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut poss: Vec<u32> = if needs_start {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut pose: Vec<u32> = if needs_end {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut ids: Vec<String> = if needs_id {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut refs: Vec<String> = if needs_ref {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut alts: Vec<String> = if needs_alt {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut quals: Vec<Option<f64>> = if needs_qual {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };
    let mut filters: Vec<String> = if needs_filter {
        Vec::with_capacity(batch_size)
    } else {
        Vec::new()
    };

    // let mut count: usize = 0;
    let mut batch_num = 0;
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();
    let thread_num = thread_num.unwrap_or(1);
    let mut reader = VcfLocalReader::new(
        file_path.clone(),
        thread_num,
        object_storage_options.unwrap(),
    )
    .await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let mut record_num = 0;
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(
        batch_size,
        info_fields,
        infos,
        &mut info_builders,
        projection.clone(),
    );

    let stream = try_stream! {

        let mut records = reader.read_records();
        // let iter_start_time = Instant::now();
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_chrom {
                chroms.push(record.reference_sequence_name().to_string());
            }
            if needs_start {
                poss.push(record.variant_start().unwrap()?.get() as u32);
            }
            if needs_end {
                pose.push(get_variant_end(&record, &header));
            }
            if needs_id {
                ids.push(record.ids().iter().map(|v| v.to_string()).collect::<Vec<String>>().join(";"));
            }
            if needs_ref {
                refs.push(record.reference_bases().to_string());
            }
            if needs_alt {
                alts.push(record.alternate_bases().iter().map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join("|"));
            }
            if needs_qual {
                quals.push(record.quality_score().transpose()?.map(|v| v as f64));
            }
            if needs_filter {
                filters.push(record.filters().iter(&header).map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join(";"));
            }

            // Only load info fields if any info fields are needed
            if !info_builders.0.is_empty() {
                load_infos(Box::new(record), &header, &mut info_builders, projection.clone())?;
            }
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)),
                    projection.clone(),
                    needs_chrom,
                    needs_start,
                    needs_end,
                    needs_id,
                    needs_ref,
                    needs_alt,
                    needs_qual,
                    needs_filter,
                    batch_size,
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
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &chroms,
                &poss,
                &pose,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                Some(&builders_to_arrays(&mut info_builders.2)),
                projection.clone(),
                needs_chrom,
                needs_start,
                needs_end,
                needs_id,
                needs_ref,
                needs_alt,
                needs_qual,
                needs_filter,
                chroms.len(),
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
    set_info_builders(
        batch_size,
        info_fields,
        infos,
        &mut info_builders,
        projection.clone(),
    );

    // Determine which core VCF fields we need to parse based on projection
    let needs_chrom = projection.as_ref().map_or(true, |proj| proj.contains(&0));
    let needs_start = projection.as_ref().map_or(true, |proj| proj.contains(&1));
    let needs_end = projection.as_ref().map_or(true, |proj| proj.contains(&2));
    let needs_id = projection.as_ref().map_or(true, |proj| proj.contains(&3));
    let needs_ref = projection.as_ref().map_or(true, |proj| proj.contains(&4));
    let needs_alt = projection.as_ref().map_or(true, |proj| proj.contains(&5));
    let needs_qual = projection.as_ref().map_or(true, |proj| proj.contains(&6));
    let needs_filter = projection.as_ref().map_or(true, |proj| proj.contains(&7));

    let stream = try_stream! {
        // Create vectors for accumulating record data only for needed fields.
        let mut chroms: Vec<String> = if needs_chrom { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut poss: Vec<u32> = if needs_start { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut pose: Vec<u32> = if needs_end { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut ids: Vec<String> = if needs_id { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut refs: Vec<String> = if needs_ref { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut alts: Vec<String> = if needs_alt { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut quals: Vec<Option<f64>> = if needs_qual { Vec::with_capacity(batch_size) } else { Vec::new() };
        let mut filters: Vec<String> = if needs_filter { Vec::with_capacity(batch_size) } else { Vec::new() };
        // add infos fields vector of vectors of different types

        debug!("Info fields: {:?}", info_builders);

        let mut record_num = 0;
        let mut batch_num = 0;


        // Process records one by one.

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;  // propagate errors if any

            // Only parse and store fields that are needed
            if needs_chrom {
                chroms.push(record.reference_sequence_name().to_string());
            }
            if needs_start {
                poss.push(record.variant_start().unwrap()?.get() as u32);
            }
            if needs_end {
                pose.push(get_variant_end(&record, &header));
            }
            if needs_id {
                ids.push(record.ids().iter().map(|v| v.to_string()).collect::<Vec<String>>().join(";"));
            }
            if needs_ref {
                refs.push(record.reference_bases().to_string());
            }
            if needs_alt {
                alts.push(record.alternate_bases().iter().map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join("|"));
            }
            if needs_qual {
                quals.push(record.quality_score().transpose()?.map(|v| v as f64));
            }
            if needs_filter {
                filters.push(record.filters().iter(&header).map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join(";"));
            }

            // Only load info fields if any info fields are needed
            if !info_builders.0.is_empty() {
                load_infos(Box::new(record), &header, &mut info_builders, projection.clone())?;
            }
            record_num += 1;
            // Once the batch size is reached, build and yield a record batch.
            if record_num % batch_size == 0 {
                debug!("Record number: {}", record_num);
                let batch = build_record_batch_optimized(
                    Arc::clone(&schema.clone()),
                    &chroms,
                    &poss,
                    &pose,
                    &ids,
                    &refs,
                    &alts,
                    &quals,
                    &filters,
                    Some(&builders_to_arrays(&mut info_builders.2)),
                    projection.clone(),
                    needs_chrom,
                    needs_start,
                    needs_end,
                    needs_id,
                    needs_ref,
                    needs_alt,
                    needs_qual,
                    needs_filter,
                    batch_size,
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
            let batch = build_record_batch_optimized(
                Arc::clone(&schema.clone()),
                &chroms,
                &poss,
                &pose,
                &ids,
                &refs,
                &alts,
                &quals,
                &filters,
                Some(&builders_to_arrays(&mut info_builders.2)),
                projection.clone(),
                needs_chrom,
                needs_start,
                needs_end,
                needs_id,
                needs_ref,
                needs_alt,
                needs_qual,
                needs_filter,
                chroms.len(),
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
    projection: Option<Vec<usize>>,
) {
    let available_info_fields = info_fields.unwrap_or_default();

    // Always create builders for all requested INFO fields to maintain schema consistency
    for field_name in available_info_fields {
        let data_type = info_to_arrow_type(infos, &field_name);
        let field = OptionalField::new(&data_type, batch_size).unwrap();
        info_builders.0.push(field_name);
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
                object_storage_options,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
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
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
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
